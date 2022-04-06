import concurrent
import datetime
import logging
import multiprocessing
import queue
import random
import sys
import threading
import time
from enum import Enum

from concurrent.futures import ProcessPoolExecutor

from esrally import (
    PROGRAM_NAME,
    client,
    config,
    driver,
    exceptions,
    metrics,
    paths,
    racecontrol,
    telemetry,
    track,
)
from esrally.driver import runner, scheduler
from esrally.driver.core import (
    AsyncIoAdapter,
    JoinPoint,
    SamplePostprocessor,
    Sampler,
    UpdateSamples,
    WorkerTask,
    num_cores,
    select_challenge,
)
from esrally.track import TrackProcessorRegistry, load_track, load_track_plugins
from esrally.utils import console, convert, net


class TaskExecutor:
    def __init__(self, task_queue):
        self.task_queue = task_queue

    def do_task(self):
        while True:
            task = self.task_queue.get()
            # Poison pill
            if task is None:
                self.task_queue.task_done()
                break
            try:
                for t in task:
                    t.func(**t.params)
            finally:
                self.task_queue.task_done()
        return


class TrackPreparationWorker:
    def __init__(self, cfg, track, registry, task_queue, total_processes):
        self.cfg = cfg
        self.track = track
        self.registry = registry
        self.register_track = self.registry.register_track_processor
        self.task_queue = task_queue
        self.total_processes = total_processes
        self.data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
        self.executor_processes = None

    def start_task_executors(self):
        executors = [TaskExecutor(self.task_queue) for _ in range(self.total_processes)]
        processes = [multiprocessing.Process(target=w.do_task) for w in executors]
        self.executor_processes = processes

        for p in processes:
            p.start()

    def parallel_on_prepare(self):
        self.start_task_executors()

        for processor in self.registry.processors:
            on_prepare = processor.on_prepare_track(self.track, self.data_root_dir)
            tasks = [WorkerTask(func, params) for func, params in on_prepare]
            self.task_queue.put(tasks)

        # Signals worker processes to break out of their loops
        for i in range(self.total_processes):
            self.task_queue.put(None)

        self.task_queue.join()
        self.stop_task_executors()

    def stop_task_executors(self):
        for p in self.executor_processes:
            p.kill()

    def prepare_track(self):
        load_track(self.cfg)
        load_track_plugins(self.cfg, self.track.name, register_track_processor=self.register_track, force_update=True)
        self.parallel_on_prepare()

def initialize_worker(cfg, track, cancel, complete, client_queue):
    execute_task.cfg = cfg
    execute_task.track = track
    execute_task.cancel = cancel
    execute_task.complete = complete
    execute_task.client_queue = client_queue
    execute_task.on_error = cfg.opts("driver", "on.error")
    execute_task.sample_queue_size = int(cfg.opts("reporting", "sample.queue.size", mandatory=False, default_value=1 << 20))


def execute_task(inputs):
    worker_id, tasks = inputs
    f = execute_task
    print(f"worker_{worker_id}: executing {tasks}")
#    print(f"worker_{worker_id}: client_ids: {[t.client_id for t in tasks]}")
    if tasks:
        sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=f.sample_queue_size)
        AsyncIoAdapter(f.cfg, f.track, tasks, sampler, f.cancel, f.complete, f.on_error, f.client_queue).__call__()
        samples = sampler.samples
        return (worker_id, time.perf_counter, samples)


class SingleNodeDriver:
    class Status(Enum):
        INITIALIZING = "initializing"
        PROCESSOR_RUNNING = "processor running"
        PROCESSOR_COMPLETE = "processor complete"

    def __init__(self, cfg, es_client_factory_class=client.EsClientFactory):
        self.logger = logging.getLogger(__name__)
        self.cfg = cfg
        self.es_client_factory = es_client_factory_class
        self.benchmark_coordinator = racecontrol.BenchmarkCoordinator(self.cfg)
        self.cores = num_cores(self.cfg)
        self.data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
        self.quiet = self.cfg.opts("system", "quiet.mode", mandatory=False, default_value=False)

        self.track = None
        self.track_name = None
        self.challenge = None
        self.metrics_store = None

        self.load_driver_hosts = []

        self.manager = multiprocessing.Manager()
        self.track_preparation_queue = self.manager.JoinableQueue()
        self.sample_queue = self.manager.Queue()
        self.sample_post_processor = None

        self.allocations = None
        self.client_allocations_per_worker = {}

        self.cluster_details = {}

        self.telemetry = None
        self.status = self.Status.INITIALIZING

        self.complete = self.manager.Event()
        self.cancel = self.manager.Event()
        self.client_queue = self.manager.JoinableQueue()

    def create_es_clients(self):
        all_hosts = self.cfg.opts("client", "hosts").all_hosts
        es = {}
        for cluster_name, cluster_hosts in all_hosts.items():
            all_client_options = self.cfg.opts("client", "options").all_client_options
            cluster_client_options = dict(all_client_options[cluster_name])
            # Use retries to avoid aborts on long living connections for telemetry devices
            cluster_client_options["retry-on-timeout"] = True
            es[cluster_name] = self.es_client_factory(cluster_hosts, cluster_client_options).create()
        return es

    def wait_for_rest_api(self, es):
        es_default = es["default"]
        self.logger.info("Checking if REST API is available.")
        if client.wait_for_rest_layer(es_default, max_attempts=40):
            self.logger.info("REST API is available.")
        else:
            self.logger.error("REST API layer is not yet available. Stopping benchmark.")
            raise exceptions.SystemSetupError("Elasticsearch REST API layer is not available.")

    def retrieve_cluster_info(self, es):
        try:
            return es["default"].info()
        except BaseException:
            self.logger.exception("Could not retrieve cluster info on benchmark start")
            return None

    def reset_relative_time(self):
        self.logger.debug("Resetting relative time of request metrics store.")
        self.metrics_store.reset_relative_time()

    def prepare_telemetry(self, es, enable):
        enabled_devices = self.cfg.opts("telemetry", "devices")
        telemetry_params = self.cfg.opts("telemetry", "params")
        log_root = paths.race_root(self.cfg)

        es_default = es["default"]

        if enable:
            devices = [
                telemetry.NodeStats(telemetry_params, es, self.metrics_store),
                telemetry.ExternalEnvironmentInfo(es_default, self.metrics_store),
                telemetry.ClusterEnvironmentInfo(es_default, self.metrics_store),
                telemetry.JvmStatsSummary(es_default, self.metrics_store),
                telemetry.IndexStats(es_default, self.metrics_store),
                telemetry.MlBucketProcessingTime(es_default, self.metrics_store),
                telemetry.MasterNodeStats(telemetry_params, es_default, self.metrics_store),
                telemetry.SegmentStats(log_root, es_default),
                telemetry.CcrStats(telemetry_params, es, self.metrics_store),
                telemetry.RecoveryStats(telemetry_params, es, self.metrics_store),
                telemetry.ShardStats(telemetry_params, es, self.metrics_store),
                telemetry.TransformStats(telemetry_params, es, self.metrics_store),
                telemetry.SearchableSnapshotsStats(telemetry_params, es, self.metrics_store),
                telemetry.DataStreamStats(telemetry_params, es, self.metrics_store),
            ]
        else:
            devices = []
        self.telemetry = telemetry.Telemetry(enabled_devices, devices=devices)

    def prepare_benchmark(self):
        self.track = load_track(self.cfg)
        self.track_name = self.track.name
        self.challenge = select_challenge(self.cfg, self.track)

        downsample_factor = int(self.cfg.opts("reporting", "metrics.request.downsample.factor", mandatory=False, default_value=1))
        self.metrics_store = metrics.metrics_store(cfg=self.cfg, track=self.track.name, challenge=self.challenge.name, read_only=False)
        self.sample_post_processor = SamplePostprocessor(
            self.metrics_store, downsample_factor, self.track.meta_data, self.challenge.meta_data
        )

        es_clients = self.create_es_clients()

        skip_rest_api_check = self.cfg.opts("mechanic", "skip.rest.api.check")
        uses_static_responses = self.cfg.opts("client", "options").uses_static_responses
        if skip_rest_api_check:
            self.logger.info("Skipping REST API check as requested explicitly.")
        elif uses_static_responses:
            self.logger.info("Skipping REST API check as static responses are used.")
        else:
            self.wait_for_rest_api(es_clients)

        self.cluster_details = self.retrieve_cluster_info(es_clients)

        # Avoid issuing any requests to the target cluster when static responses are enabled. The results
        # are not useful and attempts to connect to a non-existing cluster just lead to exception traces in logs.
        self.prepare_telemetry(es_clients, enable= not uses_static_responses)

        track.set_absolute_data_path(self.cfg, self.track)
        runner.register_default_runners()

        for host in self.cfg.opts("driver", "load_driver_hosts"):
            host_config = {
                # for simplicity we assume that all benchmark machines have the same specs
                "cores": num_cores(self.cfg)
            }
            if host != "localhost":
                host_config["host"] = net.resolve(host)
            else:
                host_config["host"] = host

            self.load_driver_hosts.append(host_config)

    def start_telemetry_devices(self):
        self.logger.info("Attaching cluster-level telemetry devices.")
        self.telemetry.on_benchmark_start()
        self.logger.info("Cluster-level telemetry devices are now attached.")

    def allocate_workers(self):
        allocator = driver.driver.Allocator(self.challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.tasks_per_join_point = allocator.tasks_per_joinpoint
        self.completing_join_points = None

        self.logger.info("Benchmark consists of [%d] steps executed by [%d] clients.", self.number_of_steps, len(self.allocations))
        # avoid flooding the log if there are too many clients
        if allocator.clients < 128:
            self.logger.info("Allocation matrix:\n%s", "\n".join([str(a) for a in self.allocations]))

        worker_assignments = driver.driver.calculate_worker_assignments(self.load_driver_hosts, allocator.clients)
        worker_id = 0
        for assignment in worker_assignments:
            host = assignment["host"]
            for clients in assignment["workers"]:
                # don't assign workers without any clients
                if len(clients) > 0:
                    self.logger.info(f"Allocating worker {worker_id} on {host} with {len(clients)} clients.")
                    client_allocations = driver.driver.ClientAllocations()
                    for client_id in clients:
                        client_allocations.add(client_id, self.allocations[client_id])
                    self.client_allocations_per_worker[worker_id] = client_allocations
                    worker_id += 1

    def prepare_track(self, cfg, track):
        registry = TrackProcessorRegistry(cfg)
        track_preparator = TrackPreparationWorker(cfg, track, registry, self.track_preparation_queue, self.cores)
        track_preparator.prepare_track()

    def complete_task(self, samples):
        self.sample_post_processor(samples)
        self.reset_relative_time()
        m = self.metrics_store.to_externalizable(clear=True)
        self.benchmark_coordinator.on_task_finished(m)

    def run(self):
        def complete_benchmark():
            print(f"coordinator: All steps completed.")
            m = self.metrics_store.to_externalizable(clear=True)
            print(f"coordinator: Closing metrics store...")
            self.metrics_store.close()
            # immediately clear as we don't need it anymore and it can consume a significant amount of memory
            self.metrics_store = None
            print(f"coordinator: Sending results to benchmark coordinator...")
            self.benchmark_coordinator.on_benchmark_complete(m)

        def complete_task(samples, step):
            print(f"coordinator: all tasks complete for step {step}")
            self.sample_post_processor(samples)
            self.reset_relative_time()
            self.cancel.clear()
            self.complete.clear()
            m = self.metrics_store.to_externalizable(clear=True)
            self.benchmark_coordinator.on_task_finished(m)

        def run_task_loops(allocations):
            steps = self.number_of_steps * 2 + 1
            tasks_per_step = []
            completing_joinpoints = {}

            task_idx = 0
            for step in range(1, steps, 2):
                tasks = []

                for worker_id, allocation in self.client_allocations_per_worker.items():
                    if not allocation.is_joinpoint(step):
                        tasks.append((worker_id, allocation.tasks(step)))
                        # Next step will be a joinpoint corresponding to this task index
                        joinpoint = allocation.tasks(step + 1)[0].task
                        if joinpoint.preceding_task_completes_parent or joinpoint.any_task_completes_parent:
                            completing_joinpoints[task_idx] = joinpoint
                        else:
                            completing_joinpoints[task_idx] = None
                tasks_per_step.append(tasks)
                task_idx += 1

            pool = ProcessPoolExecutor(initializer=initialize_worker,
                                       initargs=(self.cfg,
                                                 self.track,
                                                 self.cancel,
                                                 self.complete,
                                                 self.client_queue))

            with pool:
                for step, tasks in enumerate(tasks_per_step):
                    inputs = [(worker, task) for worker, task in tasks if task]
                    exit_condition = completing_joinpoints[step]
                    # Submit tasks asynchronously to the process pool
                    results = pool.map(execute_task, inputs)
                    # We're now at a virtual "join point"
                    if exit_condition:
                        if exit_condition.preceding_task_completes_parent:
                            print(f"coordinator: Specific task completes. Waiting for all clients of that task to complete.")
                            pending = exit_condition.clients_executing_completing_task
                            finished = []
                            while not all(client in finished for client in pending):
                                print(f"Waiting on {len(set(pending) ^ set(finished))} pending clients.")
                                finished.append(self.client_queue.get())
                            print(f"coordinator: All clients complete. Cancelling in-flight tasks.")
                            self.cancel.set()
                        elif exit_condition.any_task_completes_parent:
                            print(f"coordinator: Any task completes. Waiting for first task to complete.")
                            self.complete.wait()
                            print(f"coordinator: Fist task complete. Cancelling remaining tasks.")
                            self.cancel.set()
                    raw_samples = []
                    for worker_id, worker_timestamp, sample in results:
                        raw_samples += sample
                    complete_task(raw_samples, step)
                complete_benchmark()

            print(completing_joinpoints)

        run_task_loops(self.client_allocations_per_worker)

    def execute(self):
        self.benchmark_coordinator.setup()
        self.prepare_benchmark()
        self.prepare_track(self.cfg, self.track)
        # Hard-code these for now, since we're not hooked up to the racecontrol machinery yet
        self.benchmark_coordinator.on_preparation_complete("basic", "7.17.0", "master")
        self.reset_relative_time()
        self.start_telemetry_devices()
        self.allocate_workers()
        self.run()


def race(cfg):
    number_of_drivers = len(cfg.opts("driver", "load_driver_hosts")) == 1
    if number_of_drivers == 1:
        coordinator = SingleNodeDriver(cfg)
        coordinator.execute()
    else:
        msg = f"Race configured with {number_of_drivers} load drivers, but --actorless currently only supports one."
        raise (exceptions.RallyError(msg))
