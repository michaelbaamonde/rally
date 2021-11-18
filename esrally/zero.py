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
                # TODO: Distinguish between JoinPoint and None
                if not tasks:
                    self.task_queue.task_done()
                else:
                    print(f"worker_{self.worker_id}: Executing tasks: {tasks}")
                    self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)
                    AsyncIoAdapter(self.cfg, self.track, tasks, self.sampler, self.cancel, self.complete, self.on_error).__call__()
                    task_queue.task_done()
        return

    def drive(self):
        task_allocations = self.current_tasks_and_advance()
        # skip non-tasks in the task list
        while len(task_allocations) == 0:
            task_allocations = self.current_tasks_and_advance()

        if self.at_joinpoint():
            self.logger.info("Worker[%d] reached join point at index [%d].", self.worker_id, self.current_task_index)
            # clients that don't execute tasks don't need to care about waiting
            if self.executor_future is not None:
                self.executor_future.result()
            self.send_samples()
            self.cancel.clear()
            self.complete.clear()
            self.executor_future = None
            self.sampler = None
            self.coordinator.joinpoint_reached(JoinPointReached(self.worker_id, task_allocations))
        else:
            # There may be a situation where there are more (parallel) tasks than workers. If we were asked to complete all tasks, we not
            # only need to complete actively running tasks but actually all scheduled tasks until we reach the next join point.
            if self.complete.is_set():
                self.logger.info(
                    "Worker[%d] skips tasks at index [%d] because it has been asked to complete all tasks until next join point.",
                    self.worker_id,
                    self.current_task_index,
                )
            else:
                self.logger.info("Worker[%d] is executing tasks at index [%d].", self.worker_id, self.current_task_index)
                self.sampler = Sampler(start_timestamp=time.perf_counter(), buffer_size=self.sample_queue_size)
                executor = AsyncIoAdapter(
                    self.cfg, self.track, task_allocations, self.sampler, self.cancel, self.complete, self.on_error
                )

                self.executor_future = self.pool.submit(executor)
                time.sleep(datetime.timedelta(seconds=self.wakeup_interval))
                if self.executor_future.done():
                    print("Done")
                    self.task_queue.task_done()


    def wakeup_after(self, interval):
        time.sleep(interval)

        if self.start_driving:
            self.start_driving = False
            self.drive()
        else:
            current_samples = self.send_samples()
            if self.cancel.is_set():
                msg = f"Worker[{str(self.worker_id)}] has detected that benchmark has been cancelled. Notifying coordinator..."
                self.logger.info(msg)
                # TODO: better error handling
                raise exceptions.RallyError(msg)
            elif self.executor_future is not None and self.executor_future.done():
                e = self.executor_future.exception(timeout=0)
                if e:
                    self.logger.exception(
                        "Worker[%s] has detected a benchmark failure. Notifying master...", str(self.worker_id), exc_info=e
                    )
                    # the exception might be user-defined and not be on the load path of the master driver. Hence, it cannot be
                    # deserialized on the receiver so we convert it here to a plain string.
                    # TODO: Catch these upstream in the coordinator in single-node case. Figure out error handling in multi-node case.
                    raise exceptions.RallyError(f"Error in load generator [{self.worker_id, str(e)}]")
                else:
                    self.logger.info("Worker[%s] is ready for the next task.", str(self.worker_id))
                    self.executor_future = None
                    self.drive()
            else:
                if current_samples and len(current_samples) > 0:
                    most_recent_sample = current_samples[-1]
                    if most_recent_sample.percent_completed is not None:
                        self.logger.debug(
                            "Worker[%s] is executing [%s] (%.2f%% complete).",
                            str(self.worker_id),
                            most_recent_sample.task,
                            most_recent_sample.percent_completed * 100.0,
                        )
                    else:
                        # TODO: This could be misleading given that one worker could execute more than one task...
                        self.logger.debug(
                            "Worker[%s] is executing [%s] (dependent eternal task).", str(self.worker_id), most_recent_sample.task
                        )
                else:
                    self.logger.debug("Worker[%s] is executing (no samples).", str(self.worker_id))
                self.wakeup_after(datetime.timedelta(seconds=self.wakeup_interval))


    def drive_at(self, client_start_timestamp):
        sleep_time = datetime.timedelta(seconds=client_start_timestamp - time.perf_counter())
        self.logger.info(
            "Worker[%d] is continuing its work at task index [%d] on [%f], that is in [%s].",
            self.worker_id,
            self.current_task_index,
            client_start_timestamp,
            sleep_time,
        )
        self.start_driving = True
        self.wakeup_after(sleep_time)

    def send_samples(self):
        if self.sampler:
            samples = self.sampler.samples
            if len(samples) > 0:
                self.coordinator.update_samples(UpdateSamples(self.worker_id, samples))
            return samples
        return None

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
        self.driver = Driver(self, cfg)
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
        return worker

    def start_worker(self, worker_process):
        worker_process.start()

    def run(self):
        def filter_noop_tasks(tasks):
            return [task for task in tasks if isinstance(task, driver.driver.TaskAllocation)]

        def filtered_allocations(allocations):
            for a in allocations:
                tasks = filter_noop_tasks(a["tasks"])
                a.update({"tasks": tasks})
            return allocations

        def worker_allocations(worker_id, allocations):
            return allocations[worker_id].allocations

        def create_worker_process(worker, task_queue):
            process_name = f"worker_{worker.worker_id}"
            return multiprocessing.Process(name=process_name, target=worker.do_task, args=(task_queue, ))

        def worker_context(worker, manager, allocations):
            return {"worker": worker, "queue": manager.JoinableQueue(), "allocations": worker_allocations(worker.worker_id, allocations)}

        allocations = self.client_allocations_per_worker
        manager = multiprocessing.Manager()
        workers = {worker.worker_id: worker_context(worker, manager, allocations) for worker in self.driver.workers}

        processes = [create_worker_process(worker_context["worker"], worker_context["queue"]) for _, worker_context in workers.items()]

        for p in processes:
            p.start()

        steps = self.driver.number_of_steps

        for step in range(steps):
            queues = []
            for worker, ctx in workers.items():
                ta = allocations[worker]
                if ta.is_joinpoint(step):
                    tasks = []
                else:
                    tasks = ta.tasks(step)
                task_queue = ctx["queue"]
                queues.append(task_queue)
                print(f"coordinator: Queueing tasks for worker_{worker} at step {step} of {steps - 1}")
                task_queue.put((step, tasks))
            print(f"coordinator: Waiting for workers to complete step {step} of {steps - 1}")
            for queue in queues:
                queue.join()
            print(f"coordinator: All workers at join point {step} of {steps - 1}")

        qs = []
        print(f"coordinator: Telling all workers to shut down.")
        for worker, ctx in workers.items():
            task_queue = ctx["queue"]
            qs.append(task_queue)
            task_queue.put("STOP")
        print(f"coordinator: Waiting for all workers to shut down.")
        for queue in qs:
            queue.join()

    def on_task_finished(self, metrics, next_task_scheduled_in):
        if next_task_scheduled_in > 0:
            time.sleep(datetime.timedelta(seconds=next_task_scheduled_in))
            self.driver.reset_relative_time()
        else:
            self.driver.reset_relative_time()
        # This is sent to racecontrol? cf racecontrol.py::135
        # self.send(self.start_sender, TaskFinished(metrics, next_task_scheduled_in))

    def update_samples(self, samples):
        self.driver.update_samples(samples.samples)

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

    def __init__(self, target, cfg, es_client_factory_class=client.EsClientFactory):
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
        self.raw_samples = []
        self.most_recent_sample_per_client = {}
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

        self.sample_post_processor = driver.driver.SamplePostprocessor(
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

#        self.update_progress_message()

    def update_progress_message(self, task_finished=False):
        if not self.quiet and self.current_step >= 0:
            tasks = ",".join([t.name for t in self.tasks_per_join_point[self.current_step]])

            if task_finished:
                total_progress = 1.0
            else:
                # we only count clients which actually contribute to progress. If clients are executing tasks eternally in a parallel
                # structure, we should not count them. The reason is that progress depends entirely on the client(s) that execute the
                # task that is completing the parallel structure.
                progress_per_client = [
                    s.percent_completed for s in self.most_recent_sample_per_client.values() if s.percent_completed is not None
                ]

                num_clients = max(len(progress_per_client), 1)
                total_progress = sum(progress_per_client) / num_clients
            self.progress_reporter.print("Running %s" % tasks, "[%3d%% done]" % (round(total_progress * 100)))
            if task_finished:
                self.progress_reporter.finish()

    def may_complete_current_task(self, task_allocations):
        any_joinpoints_completing_parent = [a for a in task_allocations if a.task.any_task_completes_parent]
        joinpoints_completing_parent = [a for a in task_allocations if a.task.preceding_task_completes_parent]

        # If 'completed-by' is set to 'any', then we *do* want to check for completion by
        # any client and *not* wait until the remaining runner has completed. This way the 'parallel' task will exit
        # on the completion of _any_ client for any task, i.e. given a contrived track with two tasks to execute inside
        # a parallel block:
        #   * parallel:
        #     * bulk-1, with clients 8
        #     * bulk-2, with clients: 8
        #
        # 1. Both 'bulk-1' and 'bulk-2' execute in parallel
        # 2. 'bulk-1' client[0]'s runner is first to complete and reach the next joinpoint successfully
        # 3. 'bulk-1' will now cause the parent task to complete and _not_ wait for all 8 clients' runner to complete,
        # or for 'bulk-2' at all
        #
        # The reasoning for the distinction between 'any_joinpoints_completing_parent' & 'joinpoints_completing_parent'
        # is to simplify logic, otherwise we'd need to implement some form of state machine involving actor-to-actor
        # communication.

        if len(any_joinpoints_completing_parent) > 0 and not self.complete_current_task_sent:
            self.logger.info(
                "Any task before join point [%s] is able to complete the parent structure. Telling all clients to exit immediately.",
                any_joinpoints_completing_parent[0].task,
            )

            self.complete_current_task_sent = True
            for worker in self.workers:
                self.target.complete_current_task(worker)

        # If we have a specific 'completed-by' task specified, then we want to make sure that all clients for that task
        # are able to complete their runners as expected before completing the parent
        elif len(joinpoints_completing_parent) > 0 and not self.complete_current_task_sent:
            # while this list could contain multiple items, it should always be the same task (but multiple
            # different clients) so any item is sufficient.
            current_join_point = joinpoints_completing_parent[0].task
            self.logger.info(
                "Tasks before join point [%s] are able to complete the parent structure. Checking "
                "if all [%d] clients have finished yet.",
                current_join_point,
                len(current_join_point.clients_executing_completing_task),
            )
            pending_client_ids = []
            for client_id in current_join_point.clients_executing_completing_task:
                # We assume that all clients have finished if their corresponding worker has finished
                worker_id = self.clients_per_worker[client_id]
                if worker_id not in self.workers_completed_current_step:
                    pending_client_ids.append(client_id)

            # are all clients executing said task already done? if so we need to notify the remaining clients
            if len(pending_client_ids) == 0:
                # As we are waiting for other clients to finish, we would send this message over and over again.
                # Hence we need to memorize whether we have already sent it for the current step.
                self.complete_current_task_sent = True
                self.logger.info("All affected clients have finished. Notifying all clients to complete their current tasks.")
                for worker in self.workers:
                    self.target.complete_current_task(worker)
            else:
                if len(pending_client_ids) > 32:
                    self.logger.info("[%d] clients did not yet finish.", len(pending_client_ids))
                else:
                    self.logger.info("Client id(s) [%s] did not yet finish.", ",".join(map(str, pending_client_ids)))


    def joinpoint_reached(self, worker_id, worker_local_timestamp, task_allocations):
        self.currently_completed += 1
        # This isn't thread-safe
        self.workers_completed_current_step[worker_id] = (worker_local_timestamp, time.perf_counter())
        self.logger.info(
            "[%d/%d] workers reached join point [%d/%d].",
            self.currently_completed,
            len(self.workers),
            self.current_step + 1,
            self.number_of_steps,
        )
        if self.currently_completed == len(self.workers):
            self.logger.info("All workers completed their tasks until join point [%d/%d].", self.current_step + 1, self.number_of_steps)
            # we can go on to the next step
            self.currently_completed = 0
            self.complete_current_task_sent = False
            # make a copy and reset early to avoid any race conditions from clients that reach a join point already while we are sending...
            workers_curr_step = self.workers_completed_current_step
            self.workers_completed_current_step = {}
            self.update_progress_message(task_finished=True)
            # clear per step
            self.most_recent_sample_per_client = {}
            self.current_step += 1

            self.logger.debug("Postprocessing samples...")
            self.post_process_samples()
            if self.finished():
                #self.telemetry.on_benchmark_stop()
                self.logger.info("All steps completed.")
                # Some metrics store implementations return None because no external representation is required.
                # pylint: disable=assignment-from-none
                m = self.metrics_store.to_externalizable(clear=True)
                self.logger.debug("Closing metrics store...")
                self.metrics_store.close()
                # immediately clear as we don't need it anymore and it can consume a significant amount of memory
                self.metrics_store = None
                self.logger.debug("Sending benchmark results...")
                self.target.on_benchmark_complete(m)
            else:
                self.move_to_next_task(workers_curr_step)
        else:
            self.may_complete_current_task(task_allocations)

    def update_samples(self, samples):
        if len(samples) > 0:
            self.raw_samples += samples
            # We need to check all samples, they will be from different clients
            for s in samples:
                self.most_recent_sample_per_client[s.client_id] = s

    def post_process_samples(self):
        # we do *not* do this here to avoid concurrent updates (actors are single-threaded) but rather to make it clear that we use
        # only a snapshot and that new data will go to a new sample set.
        raw_samples = self.raw_samples
        self.raw_samples = []
        self.sample_post_processor(raw_samples)

    def finished(self):
        return self.current_step == self.number_of_steps


    def move_to_next_task(self, workers_curr_step):
#        print(f"workers_curr_step: {workers_curr_step}")
        if self.cfg.opts("track", "test.mode.enabled"):
            # don't wait if test mode is enabled and start the next task immediately.
            waiting_period = 0
        else:
            # start the next task in one second (relative to master's timestamp)
            #
            # Assumption: We don't have a lot of clock skew between reaching the join point and sending the next task
            #             (it doesn't matter too much if we're a few ms off).
            waiting_period = 1.0
        # Some metrics store implementations return None because no external representation is required.
        # pylint: disable=assignment-from-none
        m = self.metrics_store.to_externalizable(clear=True)
        self.target.on_task_finished(m, waiting_period)
        # Using a perf_counter here is fine also in the distributed case as we subtract it from `master_received_msg_at` making it
        # a relative instead of an absolute value.
        start_next_task = time.perf_counter() + waiting_period
        for worker_id, worker in enumerate(self.workers):
            worker_ended_task_at, master_received_msg_at = workers_curr_step[worker_id]
            worker_start_timestamp = worker_ended_task_at + (start_next_task - master_received_msg_at)
            self.logger.info(
                "Scheduling next task for worker id [%d] at their timestamp [%f] (master timestamp [%f])",
                worker_id,
                worker_start_timestamp,
                start_next_task,
            )
            self.target.drive_at(worker, worker_start_timestamp)



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
    #multiprocessing.log_to_stderr(logging.DEBUG)
    single_driver = len(cfg.opts("driver", "load_driver_hosts")) == 1
    coordinator = SingleNodeCoordinator(cfg) if single_driver else MultiNodeCoordinator(cfg)
    coordinator.execute()
