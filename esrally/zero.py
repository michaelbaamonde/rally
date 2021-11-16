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
    config,
    driver,
    paths,
    track,
)
from esrally.track import TrackProcessorRegistry, load_track, load_track_plugins

CONTEXT = zmq.Context()

# Workers pull from this
class Coordinator:
    def __init__(self, sink, worker_urls=None):
        self.cfg = None
        self.url = "tcp://localhost:5557"
        # Basically the work queue
        self.push = CONTEXT.socket(zmq.PUSH)
        self.push.bind("tcp://*:5557")
        # Clients should do this
        self.pull = CONTEXT.socket(zmq.PULL)
        # May not need this
        self.sink = sink.push
        self.sink.connect(sink.url)

    def signal_start(self):
        print("Coordinator: signalling start to Sink")
        self.sink.send(b"0")


# Coordinator pushes to synchronize start of the batch
# Workers push to signal completion of tasks
# Coordinator pulls to gather results
class Sink:
    def __init__(self):
        self.cfg = None
        self.url = "tcp://localhost:5558"
        # Clients should do this
        self.push = CONTEXT.socket(zmq.PUSH)
        self.pull = CONTEXT.socket(zmq.PULL)
        self.pull.bind("tcp://*:5558")

    def is_ready(self):
        start = self.pull.recv()
        return start == b"0"


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


@dataclass(frozen=True)
class WorkerTask:
    """
    Unit of work that should be completed by the low-level TaskExecutor
    """

    func: Callable
    params: dict


class TaskExecutor():
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


class TrackPreparationWorker:
    class Status(Enum):
        INITIALIZING = "initializing"
        PROCESSOR_RUNNING = "processor running"
        PROCESSOR_COMPLETE = "processor complete"

    def __init__(self, coordinator_addr, sink_addr):
        self.logger = logging.getLogger(__name__)
        self.coordinator_addr = coordinator_addr
        self.sink_addr = sink_addr
        self.killswitch_addr = None

        self.cores = None
        self.status = self.Status.INITIALIZING
        self.children = []
        self.tasks = []
        self.cfg = None
        self.data_root_dir = None
        self.track = None
        self.track_name = None

    def start_task_executors(self, task_queue):
        workers = [TaskExecutor(task_queue) for i in range(self.cores)]
        processes = [multiprocessing.Process(target=w.do_task) for w in workers]

        for p in processes:
            p.start()

    def parallel_prep(self, tpr):
        m = multiprocessing.Manager()
        q = m.JoinableQueue()
        self.start_task_executors(q)

        for processor in tpr.processors:
            on_prepare = processor.on_prepare_track(self.track, self.data_root_dir)
            tasks = [WorkerTask(func, params) for func, params in on_prepare]
            q.put(tasks)

        for i in range(self.cores):
            q.put(None)

        q.join()

    # def prepare_benchmark(self):
    #     self.challenge = select_challenge(self.config, self.track)
    #     self.quiet = self.config.opts("system", "quiet.mode", mandatory=False, default_value=False)
    #     downsample_factor = int(self.config.opts("reporting", "metrics.request.downsample.factor", mandatory=False, default_value=1))
    #     self.metrics_store = metrics.metrics_store(cfg=self.config, track=self.track.name, challenge=self.challenge.name, read_only=False)

    #     self.sample_post_processor = SamplePostprocessor(
    #         self.metrics_store, downsample_factor, self.track.meta_data, self.challenge.meta_data
    #     )

    #     es_clients = self.create_es_clients()

    #     skip_rest_api_check = self.config.opts("mechanic", "skip.rest.api.check")
    #     uses_static_responses = self.config.opts("client", "options").uses_static_responses
    #     if skip_rest_api_check:
    #         self.logger.info("Skipping REST API check as requested explicitly.")
    #     elif uses_static_responses:
    #         self.logger.info("Skipping REST API check as static responses are used.")
    #     else:
    #         self.wait_for_rest_api(es_clients)
    #         self.target.cluster_details = self.retrieve_cluster_info(es_clients)

    #     # Avoid issuing any requests to the target cluster when static responses are enabled. The results
    #     # are not useful and attempts to connect to a non-existing cluster just lead to exception traces in logs.
    #     self.prepare_telemetry(es_clients, enable=not uses_static_responses)

    #     for host in self.config.opts("driver", "load_driver_hosts"):
    #         host_config = {
    #             # for simplicity we assume that all benchmark machines have the same specs
    #             "cores": num_cores(self.config)
    #         }
    #         if host != "localhost":
    #             host_config["host"] = net.resolve(host)
    #         else:
    #             host_config["host"] = host

    #         self.load_driver_hosts.append(host_config)


    @socket(zmq.PULL)
    @socket(zmq.PUSH)
    def prepare_track(self, coordinator, sink):
        self.coordinator = coordinator
        self.sink = sink

        self.coordinator.connect(self.coordinator_addr)
        self.sink.connect(self.sink_addr)

        coordinator_cfg = coordinator.recv_pyobj()
        self.cfg = load_local_config(coordinator_cfg)
        self.cores = num_cores(self.cfg)

        self.data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
        tpr = TrackProcessorRegistry(self.cfg)
        # Does this *have* to be done on the coordinator?
        self.track = load_track(self.cfg)
        self.track_name = self.track.name
        self.logger.info("Preparing track [%s]", self.track_name)
        self.logger.info("Reloading track [%s] to ensure plugins are up-to-date.", self.track.name)
        # the track might have been loaded on a different machine (the coordinator machine) so we force a track
        # update to ensure we use the latest version of plugins.
        loaded_track = load_track(self.cfg)
        track_plugins = load_track_plugins(
            self.cfg, self.track.name, register_track_processor=tpr.register_track_processor, force_update=True
        )
        self.status = self.Status.PROCESSOR_RUNNING
        self.parallel_prep(tpr)
        # self.send_to_children_and_transition(
        #     self, driver.driver.StartTaskLoop(self.track.name, self.cfg), self.Status.INITIALIZING, self.Status.PROCESSOR_RUNNING
        # )
        self.status = self.Status.PROCESSOR_COMPLETE
        print("Sending update")
        self.sink.send_pyobj({"track": loaded_track.name, "plugins_loaded": track_plugins})


@context()
@socket(zmq.PUSH)
@socket(zmq.PULL)
def drive(cfg, ctx, coordinator, sink):
    coordinator.bind("tcp://*:5557")
    sink.bind("tcp://*:5558")

    # start worker
    worker = TrackPreparationWorker("tcp://localhost:5557", "tcp://localhost:5558")
    worker_process = multiprocessing.Process(target=worker.prepare_track)
    worker_process.start()

    # Broadcast the config
    coordinator.send_pyobj(cfg)

    worker_process.join()

    # See what we got back
    x = sink.recv_pyobj()
    print(repr(x))
