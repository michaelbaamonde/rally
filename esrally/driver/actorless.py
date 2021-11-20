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

    def start_task_executors(self):
        executors = [TaskExecutor(self.task_queue) for _ in range(self.total_processes)]
        processes = [multiprocessing.Process(target=w.do_task) for w in executors]

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

    def prepare_track(self):
        load_track(self.cfg)
        load_track_plugins(self.cfg, self.track.name, register_track_processor=self.register_track, force_update=True)
        self.parallel_on_prepare()


class LoadGenerationWorker:
    """
    The actual worker that applies load against the cluster(s).

    It will also regularly send measurements to the coordinator node so it can consolidate them.
    """

    def __init__(self, coordinator, cfg, worker_id, track, task_queue, sample_queue, complete_event, complete_queue):
        self.logger = logging.getLogger(__name__)
        self.coordinator = coordinator
        self.worker_id = worker_id
        self.cfg = cfg
        self.track = track
        self.sample_queue_size = int(self.cfg.opts("reporting", "sample.queue.size", mandatory=False, default_value=1 << 20))
        self.on_error = self.cfg.opts("driver", "on.error")
        # cancellation via future does not work, hence we use our own mechanism with a shared variable and polling
        self.cancel = threading.Event()
        # used to indicate that we want to prematurely consider this completed. This is *not* due to cancellation
        # but a regular event in a benchmark and used to model task dependency of parallel tasks.
        self.complete = complete_event
        self.complete_queue = complete_queue

        self.poison_pill = "STOP"
        self.task_queue = task_queue
        self.sample_queue = sample_queue
        self.sampler = None

    def initialize(self):
        track.set_absolute_data_path(self.cfg, self.track)
        self.cancel.clear()
        runner.register_default_runners()

    def complete_task(self):
        while True:
            try:
                msg = self.complete_queue.get(block=True, timeout=0.05)
                if msg == "COMPLETE":
                    print(f"cancellation_worker_{self.worker_id}: completing current task")
                    self.complete.set()
            except queue.Empty:
                continue
        return

    def drive(self):
        def handle_poison_pill():
            print(f"worker_{self.worker_id}: Shutting down.")
            self.task_queue.task_done()

        def execute_tasks(step, tasks):
            print(f"worker_{self.worker_id}: Executing tasks: {tasks}")
            self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)
            AsyncIoAdapter(self.cfg, self.track, tasks, self.sampler, self.cancel, self.complete, self.on_error).__call__()

        def task_finished():
            self.task_queue.task_done()

        def at_joinpoint(step):
            print(f"worker_{self.worker_id}: Reached join point at step {step}.")
            self.send_samples()
            self.cancel.clear()
            self.sampler = None
            self.task_queue.task_done()

        while True:
            msg = self.task_queue.get()
            if msg == self.poison_pill:
                handle_poison_pill()
                break
            elif not msg[1]:
                self.task_queue.task_done()
            else:
                step, ta = msg
                task = ta[0].task
                if isinstance(task, driver.driver.JoinPoint):
                    at_joinpoint(step)
                else:
                    execute_tasks(step, ta)
                    task_finished()
        return

    def send_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            print(f"worker_{self.worker_id}: sending samples {samples}")
            self.sample_queue.put(UpdateSamples(self.worker_id, samples))
            return samples
        else:
            self.sample_queue.put([None])


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
        self.workers = []
        self.cancellation_workers = None
        self.client_allocations_per_worker = {}

        self.cluster_details = {}

        self.telemetry = None
        self.status = self.Status.INITIALIZING

    def create_worker(self, cfg, worker_id, track):
        task_queue = self.manager.JoinableQueue()
        complete_queue = self.manager.Queue()
        complete_event = self.manager.Event()
        worker = LoadGenerationWorker(self, cfg, worker_id, track, task_queue, self.sample_queue, complete_event, complete_queue)
        worker.initialize()
        return worker

    def worker_process(self, worker):
        process_name = f"worker_{worker.worker_id}"
        return multiprocessing.Process(name=process_name, target=worker.drive)

    def start_worker_processes(self):
        processes = [self.worker_process(worker) for _, worker in enumerate(self.workers)]

        for p in processes:
            p.start()

    def worker_cancellation_process(self, worker):
        process_name = f"cancel_{worker.worker_id}"
        return multiprocessing.Process(name=process_name, target=worker.complete_task)

    def start_worker_cancellation_processes(self):
        self.cancellation_workers = [self.worker_cancellation_process(worker) for _, worker in enumerate(self.workers)]

        for p in self.cancellation_workers:
            p.start()

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
        self.prepare_telemetry(es_clients, enable=not uses_static_responses)

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
                    worker = self.create_worker(self.cfg, worker_id, self.track)
                    self.workers.append(worker)
                    self.client_allocations_per_worker[worker_id] = client_allocations
                    worker_id += 1

    def prepare_track(self, cfg, track):
        registry = TrackProcessorRegistry(cfg)
        track_preparator = TrackPreparationWorker(cfg, track, registry, self.track_preparation_queue, self.cores)
        track_preparator.prepare_track()

    def run(self):
        def complete_step(step, steps):
            print(f"coordinator: All workers at join point {step} of {steps - 1}")
            print(f"coordinator: Postprocessing samples for step {step} of {steps - 1}")
            raw_samples = self.update_samples()
            if raw_samples is not None:
                self.sample_post_processor(raw_samples)
            print(f"coordinator: Done postprocessing samples for step {step} of {steps - 1}")
            self.reset_relative_time()
            m = self.metrics_store.to_externalizable(clear=True)
            self.benchmark_coordinator.on_task_finished(m)

        def complete_benchmark():
            print(f"coordinator: All steps completed.")
            m = self.metrics_store.to_externalizable(clear=True)
            print(f"coordinator: Closing metrics store...")
            self.metrics_store.close()
            # immediately clear as we don't need it anymore and it can consume a significant amount of memory
            self.metrics_store = None
            print(f"coordinator: Sending results to benchmark coordinator...")
            self.benchmark_coordinator.on_benchmark_complete(m)

        # def may_complete_current_task(task_allocations):
        #     any_joinpoints_completing_parent = [a for a in task_allocations if a.task.any_task_completes_parent]
        #     joinpoints_completing_parent = [a for a in task_allocations if a.task.preceding_task_completes_parent]

        #     if not any_joinpoints_completing_parent:
        #         for worker in self.workers:
        #             worker.complete.set()

            # elif len(joinpoints_completing_parent) > 0:
            #     current_join_point = joinpoints_completing_parent[0].task
            #     pending_client_ids = []
            #     for client_id in current_join_point.clients_executing_completing_task:
            #         # We assume that all clients have finished if their corresponding worker has finished
            #         worker_id = self.clients_per_worker[client_id]
            #         if worker_id not in self.workers_completed_current_step:
            #             pending_client_ids.append(client_id)

            #     if len(pending_client_ids) == 0:
            #         self.complete_current_task_sent = True
            #         for worker in self.workers:
            #             self.target.complete_current_task(worker)
            #     else:
            #         if len(pending_client_ids) > 32:
            #             self.logger.info("[%d] clients did not yet finish.", len(pending_client_ids))
            #         else:
            #             self.logger.info("Client id(s) [%s] did not yet finish.", ",".join(map(str, pending_client_ids)))

        def run_task_loops(workers, allocations):
            steps = self.number_of_steps * 2 + 1
            raw_allocations = self.allocations

            for step in range(steps):
                queues = []
                for worker in workers:
                    worker_id = worker.worker_id
                    task_queue = worker.task_queue
                    ta = allocations[worker_id]
                    tasks = ta.tasks(step)
                    queues.append((worker_id, task_queue))
                    print(f"coordinator: Queueing tasks {tasks} for worker_{worker} at step {step} of {steps - 1}")
                    task_queue.put((step, tasks))
                    if step == 31:
                        worker.complete_queue.put("COMPLETE")
                print(f"coordinator: Waiting for workers to complete step {step} of {steps - 1}")
                for worker, queue in queues:
                    queue.join()
                    print(f"coordinator: worker_{worker} at join point {step} of {steps - 1}")
                complete_step(step, steps)
            complete_benchmark()

        def shutdown_workers(workers):
            qs = []
            print(f"coordinator: Telling all workers to shut down.")
            for worker in workers:
                task_queue = worker.task_queue
                qs.append(task_queue)
                task_queue.put("STOP")
            print(f"coordinator: Waiting for all workers to shut down.")
            for queue in qs:
                queue.join()

        def shutdown_cancellation_workers(workers):
            print(f"coordinator: Closing all cancellation worker queues.")
            for process in self.cancellation_workers:
                process.terminate()

        self.start_worker_processes()
        self.start_worker_cancellation_processes()
        run_task_loops(self.workers, self.client_allocations_per_worker)
        shutdown_workers(self.workers)
        shutdown_cancellation_workers(self.cancellation_workers)

    def drain_sample_queue(self):
        results = []
        while not self.sample_queue.empty():
            results.append(self.sample_queue.get())
        return results

    def update_samples(self):
        res = self.drain_sample_queue()
        raw_samples = []
        if res:
            for r in res:
                if isinstance(r, UpdateSamples):
                    samples = r.samples
                    if len(samples) > 0:
                        raw_samples += samples
        return raw_samples

    def drive_at(self, worker, worker_start_timestamp):
        pass

    def execute(self):
        self.benchmark_coordinator.setup()
        self.prepare_benchmark()
        self.prepare_track(self.cfg, self.track)
        # Hard-code these for now, since we're not hooked up to the racecontrol machinery yet
        self.benchmark_coordinator.on_preparation_complete("basic", "7.15.1", "master")
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
