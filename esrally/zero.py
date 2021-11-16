import logging
import multiprocessing
import random
import sys
import threading
import time
import zmq
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
class Coordinator():
    def __init__(self, sink, worker_urls=None):
        self.cfg = None
        self.url = ("tcp://localhost:5557")
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
        self.sink.send(b'0')

# Coordinator pushes to synchronize start of the batch
# Workers push to signal completion of tasks
# Coordinator pulls to gather results
class Sink():
    def __init__(self):
        self.cfg = None
        self.url = "tcp://localhost:5558"
        # Clients should do this
        self.push = CONTEXT.socket(zmq.PUSH)
        self.pull = CONTEXT.socket(zmq.PULL)
        self.pull.bind("tcp://*:5558")

    def is_ready(self):
        start = self.pull.recv()
        return start == b'0'

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

class TrackPreparationWorker():
    def __init__(self, coordinator_addr, sink_addr):
        self.logger = logging.getLogger(__name__)
        self.coordinator_addr = coordinator_addr
        self.sink_addr = sink_addr
        self.killswitch_addr = None

    @socket(zmq.PULL)
    @socket(zmq.PUSH)
    def prepare_track(self, coordinator, sink):
        self.coordinator = coordinator
        self.sink = sink

        self.coordinator.connect(self.coordinator_addr)
        self.sink.connect(self.sink_addr)

        coordinator_cfg = coordinator.recv_pyobj()
        self.cfg = load_local_config(coordinator_cfg)

        self.data_root_dir = self.cfg.opts("benchmarks", "local.dataset.cache")
        tpr = TrackProcessorRegistry(self.cfg)
        # Does this *have* to be done on the coordinator?
        self.track = load_track(self.cfg)
        self.logger.info("Preparing track [%s]", self.track.name)
        self.logger.info("Reloading track [%s] to ensure plugins are up-to-date.", self.track.name)
        # the track might have been loaded on a different machine (the coordinator machine) so we force a track
        # update to ensure we use the latest version of plugins.
        loaded_track = load_track(self.cfg)
        track_plugins = load_track_plugins(self.cfg, self.track.name, register_track_processor=tpr.register_track_processor, force_update=True)
        # we expect on_prepare_track can take a long time. seed a queue of tasks and delegate to child workers
        # self.children = [self._create_task_executor() for _ in range(num_cores(self.cfg))]
        # for processor in tpr.processors:
        #     self.processors.put(processor)
        # self._seed_tasks(self.processors.get())
        # self.send_to_children_and_transition(
        #     self, StartTaskLoop(self.track.name, self.cfg), self.Status.INITIALIZING, self.Status.PROCESSOR_RUNNING
        # )
        print("Sending update")
        self.sink.send_pyobj({"track": loaded_track, "plugins_loaded": track_plugins})

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
