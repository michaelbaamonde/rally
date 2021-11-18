import concurrent
import datetime
import logging
import multiprocessing
import queue
import random
import sys
import threading
import time
import zmq
from dataclasses import dataclass
from enum import Enum
from typing import Callable

from zmq.decorators import context, socket
from esrally import (
    PROGRAM_NAME,
    client,
    config,
    driver,
    exceptions,
    metrics,
    paths,
    telemetry,
    track,
)
from esrally.driver import runner, scheduler
from esrally.track import TrackProcessorRegistry, load_track, load_track_plugins
from esrally.utils import console, convert, net
from esrally.driver.driver import AsyncIoAdapter, JoinPoint, JoinPointReached, Sampler, TaskFinished, UpdateSamples


def load_local_config(coordinator_config):
    cfg = config.auto_load_local_config(
        coordinator_config,
        additional_sections=[
            # only copy the relevant bits
            "track",
            "driver",
            "client",
            # due to distribution version...
            "mechanic",
            "telemetry",
        ],
    )
    # set root path (normally done by the main entry point)
    cfg.add(config.Scope.application, "node", "rally.root", paths.rally_root())
    return cfg


def num_cores(cfg):
    return int(cfg.opts("system", "available.cores", mandatory=False, default_value=multiprocessing.cpu_count()))


def select_challenge(config, current_track):
    challenge_name = config.opts("track", "challenge.name")
    selected_challenge = current_track.find_challenge_or_default(challenge_name)

    if not selected_challenge:
        raise exceptions.SystemSetupError(
            "Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
            "challenges with %s list tracks." % (challenge_name, current_track.name, PROGRAM_NAME)
        )
    return selected_challenge


@dataclass(frozen=True)
class WorkerTask:
    """
    Unit of work that should be completed by the low-level TaskExecutor
    """

    func: Callable
    params: dict


class TaskExecutor:
    def __init__(self, task_queue):
        self.task_queue = task_queue

    def do_task(self):
        while True:
            task = self.task_queue.get()
            if task is None:
                self.task_queue.task_done()
                break
            try:
                for t in task:
                    t.func(**t.params)
            finally:
                self.task_queue.task_done()
        return


class Worker:
    """
    The actual worker that applies load against the cluster(s).

    TODO: Figure this out. Enquueue results locally, push to socket remotely
    It will also regularly send measurements to the coordinator node so it can consolidate them.
    """

    WAKEUP_INTERVAL_SECONDS = 5

    def __init__(self, coordinator, cfg, worker_id, track, client_allocations):
        self.logger = logging.getLogger(__name__)
        self.coordinator = coordinator
        self.worker_id = worker_id
        self.cfg = cfg
        self.track = track
        self.client_allocations = client_allocations
        self.task_queue = None
        self.result_queue = None

        self.current_task_index = 0
        self.next_task_index = 0
        self.sample_queue_size = int(self.cfg.opts("reporting", "sample.queue.size", mandatory=False, default_value=1 << 20))
        self.on_error = self.cfg.opts("driver", "on.error")
        if self.cfg.opts("track", "test.mode.enabled"):
            self.wakeup_interval = 0.5
        else:
            self.wakeup_interval = Worker.WAKEUP_INTERVAL_SECONDS

        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        # cancellation via future does not work, hence we use our own mechanism with a shared variable and polling
        self.cancel = threading.Event()
        # used to indicate that we want to prematurely consider this completed. This is *not* due to cancellation
        # but a regular event in a benchmark and used to model task dependency of parallel tasks.
        self.complete = threading.Event()
        self.executor_future = None
        self.sampler = None
        self.start_driving = False
        self.wakeup_interval = Worker.WAKEUP_INTERVAL_SECONDS

        self.result_queue = None

    def start(self):
        self.logger.info("Worker[%d] is about to start.", self.worker_id)
        track.set_absolute_data_path(self.cfg, self.track)
        self.cancel.clear()
        runner.register_default_runners()

    def do_task(self, task_queue):
        self.task_queue = task_queue
        while True:
            msg = self.task_queue.get()
            if msg == "STOP":
                print(f"worker_{self.worker_id}: Received message: {msg}. Shutting down.")
                self.task_queue.task_done()
                break
            else:
                step, tasks = msg
                if not tasks:
                    self.task_queue.task_done()
                elif isinstance(tasks, JoinPoint):
                    print(f"worker_{self.worker_id}: Reached join point at step {step}.")
                    self.send_samples()
                    self.cancel.clear()
                    self.sampler = None
                    self.task_queue.task_done()
                else:
                    print(f"worker_{self.worker_id}: Executing tasks: {tasks}")
                    self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)
                    AsyncIoAdapter(self.cfg, self.track, tasks, self.sampler, self.cancel, self.complete, self.on_error).__call__()
                    task_queue.task_done()
        return

    def send_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            self.result_queue.put(UpdateSamples(self.worker_id, samples))
            return samples
        else:
            self.result_queue.put([None])


class SingleNodeCoordinator:
    RESET_RELATIVE_TIME_MARKER = "reset_relative_time"

    WAKEUP_INTERVAL_SECONDS = 1

    # post-process request metrics every N seconds and send it to the metrics store
    POST_PROCESS_INTERVAL_SECONDS = 30

    def __init__(self, cfg):
        self.logger = logging.getLogger(__name__)
        self.status = "init"
        self.cluster_details = {}
        self.cfg = cfg
        self.cores = num_cores(self.cfg)

        self.manager = multiprocessing.Manager()
        self.result_queue = self.manager.Queue()
        self.raw_samples = []
        self.most_recent_sample_per_client = {}
        self.sample_post_processor = None

        self.driver = Driver(self, cfg, self.raw_samples, self.most_recent_sample_per_client)
        self.client_allocations_per_worker = {}

        self.post_process_timer = 0

    def prepare_benchmark(self):
        self.driver.prepare_benchmark()

    def start_local_task_executors(self, task_queue):
        workers = [TaskExecutor(task_queue) for i in range(self.cores)]
        processes = [multiprocessing.Process(target=w.do_task) for w in workers]

        for p in processes:
            p.start()

    def parallel_on_prepare(self, track, registry, data_root_dir):
        m = multiprocessing.Manager()
        q = m.JoinableQueue()
        self.start_local_task_executors(q)

        for processor in registry.processors:
            on_prepare = processor.on_prepare_track(track, data_root_dir)
            tasks = [WorkerTask(func, params) for func, params in on_prepare]
            q.put(tasks)

        # Signals worker processes to break out of their loops
        for i in range(self.cores):
            q.put(None)

        q.join()

    def prepare_track(self, cfg, track):
        self.track = track
        self.logger.info("Preparing track [%s]", track.name)
        # Not necessary in the single-load-driver case?
        self.logger.info("Reloading track [%s] to ensure plugins are up-to-date.", track.name)
        registry = TrackProcessorRegistry(cfg)
        loaded_track = load_track(self.cfg)
        track_plugins = load_track_plugins(cfg, track.name, register_track_processor=registry.register_track_processor, force_update=True)
        self.parallel_on_prepare(track, registry, self.driver.data_root_dir)

        return {"track": loaded_track.description, "plugins": track_plugins}

    def create_client(self, cfg, worker_id, track, client_allocations):
        worker = Worker(self, cfg, worker_id, track, client_allocations)
        worker.result_queue = self.result_queue
        return worker

    def start_worker(self, worker_process):
        worker_process.start()

    def run(self):
        def worker_allocations(worker_id, allocations):
            return allocations[worker_id].allocations

        def create_worker_process(worker, task_queue):
            process_name = f"worker_{worker.worker_id}"
            return multiprocessing.Process(name=process_name, target=worker.do_task, args=(task_queue,))

        def worker_context(worker, manager):
            return {"worker": worker, "queue": manager.JoinableQueue()}

        def run_task_loops(workers, allocations):
            steps = self.driver.number_of_steps

            for step in range(steps):
                queues = []
                for worker, ctx in workers.items():
                    task_queue = ctx["queue"]
                    ta = allocations[worker]
                    if ta.is_joinpoint(step):
                        tasks = JoinPoint(step)
                    else:
                        tasks = ta.tasks(step)
                    queues.append((worker, task_queue))
                    print(f"coordinator: Queueing tasks for worker_{worker} at step {step} of {steps - 1}")
                    task_queue.put((step, tasks))
                print(f"coordinator: Waiting for workers to complete step {step} of {steps - 1}")
                for worker, queue in queues:
                    queue.join()
                    print(f"coordinator: worker_{worker} at join point {step} of {steps - 1}")
                print(f"coordinator: All workers at join point {step} of {steps - 1}")
                print(f"coordinator: Postprocessing samples for step {step} of {steps - 1}")
                raw_samples = self.update_samples()
                print(f"coordinator: raw_samples = {raw_samples}")
                if raw_samples is not None:
                    self.sample_post_processor(raw_samples)
                # sample_post_processor = multiprocessing.Process(name="sample_processor", target=self.driver.post_process_samples())
                # sample_post_processor.start()
                # sample_post_processor.join()
                print(f"coordinator: Done postprocessing samples for step {step} of {steps - 1}")
                # self.driver.update_progress_message(task_finished=True)


        def shutdown_workers(workers):
            qs = []
            print(f"coordinator: Telling all workers to shut down.")
            for worker, ctx in workers.items():
                task_queue = ctx["queue"]
                qs.append(task_queue)
                task_queue.put("STOP")
            print(f"coordinator: Waiting for all workers to shut down.")
            for queue in qs:
                queue.join()

        def start_worker_processes(workers):
            processes = [create_worker_process(ctx["worker"], ctx["queue"]) for _, ctx in workers.items()]

            for p in processes:
                p.start()

        def initialize_worker_context(workers):
            manager = multiprocessing.Manager()
            return {worker.worker_id: worker_context(worker, manager) for worker in workers}

        workers = initialize_worker_context(self.driver.workers)
        start_worker_processes(workers)
        run_task_loops(workers, self.client_allocations_per_worker)
        shutdown_workers(workers)

    def on_task_finished(self, metrics, next_task_scheduled_in):
        if next_task_scheduled_in > 0:
            time.sleep(datetime.timedelta(seconds=next_task_scheduled_in))
            self.driver.reset_relative_time()
        else:
            self.driver.reset_relative_time()
        # This is sent to racecontrol? cf racecontrol.py::135
        # self.send(self.start_sender, TaskFinished(metrics, next_task_scheduled_in))

    def drain_result_queue(self):
        results = []
        while not self.result_queue.empty():
            results.append(self.result_queue.get())

    def update_samples(self):
        res = self.result_queue.get()
        if isinstance(res, UpdateSamples):
            samples = res.samples
            raw_samples = []
            if len(samples) > 0:
                raw_samples += samples
                return raw_samples
        else:
            return None

    def start_benchmark(self):
        return self.driver.start_benchmark()

    def drive_at(self, worker, worker_start_timestamp):
        # Instruct the process to do stuff, somehow
        pass

    def execute(self):
        self.prepare_benchmark()
        self.prepare_track(self.cfg, self.driver.track)
        self.start_benchmark()
        self.run()


class MultiNodeCoordinator:
    def __init__(self, cfg, driver):
        self.logger = logging.getLogger(__name__)
        self.status = "init"
        self.cluster_details = {}
        self.driver = driver

    @context
    @socket(zmq.PULL)
    @socket(zmq.PUSH)
    def prepare_benchmark(self, ctx, task_socket, result_socket):
        pass

    @context
    @socket(zmq.PULL)
    @socket(zmq.PUSH)
    def prepare_track(self):
        # Broadcast prepare_track message to workers
        # coordinator.send_pyobj(cfg)
        # self.sink.send_pyobj({"track": loaded_track.name, "plugins_loaded": track_plugins})
        pass

    @context
    @socket(zmq.PULL)
    @socket(zmq.PUSH)
    def start_benchmark(self, ctx, task_socket, result_socket):
        pass


class Driver:
    class Status(Enum):
        INITIALIZING = "initializing"
        PROCESSOR_RUNNING = "processor running"
        PROCESSOR_COMPLETE = "processor complete"

    def __init__(self, target, cfg, coord_raw_samples, coord_most_recent_sample_per_client, es_client_factory_class=client.EsClientFactory):
        self.logger = logging.getLogger(__name__)
        self.target = target
        self.cfg = cfg
        self.es_client_factory = es_client_factory_class

        self.data_root_dir = None
        self.track = None
        self.track_name = None
        self.challenge = None
        self.metrics_store = None

        self.load_driver_hosts = []
        self.cores = None
        self.workers = []
        # which client ids are assigned to which workers?
        self.clients_per_worker = {}

        self.progress_reporter = console.progress()
        self.progress_counter = 0
        self.quiet = False
        self.allocations = None
        self.raw_samples = coord_raw_samples
        self.most_recent_sample_per_client = coord_most_recent_sample_per_client
        self.sample_post_processor = None

        self.number_of_steps = 0
        self.currently_completed = 0
        self.workers_completed_current_step = multiprocessing.Manager().dict()
        self.current_step = -1
        self.tasks_per_join_point = None
        self.complete_current_task_sent = False

        self.telemetry = None

        self.status = self.Status.INITIALIZING

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
        self.cfg = load_local_config(self.cfg)
        self.cores = num_cores(self.cfg)

        # Does this *have* to be done on the coordinator?
        self.track = load_track(self.cfg)
        self.track_name = self.track.name

        self.data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
        self.challenge = select_challenge(self.cfg, self.track)
        self.quiet = self.cfg.opts("system", "quiet.mode", mandatory=False, default_value=False)

        downsample_factor = int(self.cfg.opts("reporting", "metrics.request.downsample.factor", mandatory=False, default_value=1))
        self.metrics_store = metrics.metrics_store(cfg=self.cfg, track=self.track.name, challenge=self.challenge.name, read_only=False)

        self.target.sample_post_processor = driver.driver.SamplePostprocessor(
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
            self.target.cluster_details = self.retrieve_cluster_info(es_clients)

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

    def start_benchmark(self):
        self.logger.info("Benchmark is about to start.")
        # ensure relative time starts when the benchmark starts.
        self.reset_relative_time()
        self.logger.info("Attaching cluster-level telemetry devices.")
        # self.telemetry.on_benchmark_start()
        self.logger.info("Cluster-level telemetry devices are now attached.")

        allocator = driver.driver.Allocator(self.challenge.schedule)
        self.allocations = allocator.allocations
        self.number_of_steps = len(allocator.join_points) - 1
        self.tasks_per_join_point = allocator.tasks_per_joinpoint

        self.target.allocations = allocator.allocations
        self.target.join_points = allocator.join_points
        self.target.number_of_steps = len(allocator.join_points) - 1
        self.target.tasks_per_join_point = allocator.tasks_per_joinpoint

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
                        self.clients_per_worker[client_id] = worker_id
                    worker = self.target.create_client(self.cfg, worker_id, self.track, client_allocations)
                    self.target.start_worker(worker)
                    self.workers.append(worker)
                    self.target.client_allocations_per_worker[worker_id] = client_allocations
                    worker_id += 1

@context()
@socket(zmq.PUSH)
@socket(zmq.PULL)
def drive_old(cfg, ctx, coordinator, sink):
    coordinator.bind("tcp://*:5557")
    sink.bind("tcp://*:5558")

    # start main driver
    driver = Driver("tcp://localhost:5557", "tcp://localhost:5558")
    driver_process = multiprocessing.Process(target=driver.prepare_benchmark)
    driver_process.start()

    # Broadcast the config
    coordinator.send_pyobj(cfg)

    driver_process.join()

    # See what we got back
    x = sink.recv_pyobj()
    print(repr(x))


def drive(cfg):
#    multiprocessing.log_to_stderr(logging.DEBUG)
    single_driver = len(cfg.opts("driver", "load_driver_hosts")) == 1
    coordinator = SingleNodeCoordinator(cfg) if single_driver else MultiNodeCoordinator(cfg)
    coordinator.execute()
