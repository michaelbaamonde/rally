# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import asyncio
import collections
import concurrent.futures
import datetime
import itertools
import logging
import math
import multiprocessing
import queue
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable

import thespian.actors

from esrally import (
    PROGRAM_NAME,
    actor,
    client,
    config,
    exceptions,
    metrics,
    paths,
    telemetry,
    track,
)
from esrally.driver import runner, scheduler
from esrally.track import TrackProcessorRegistry, load_track, load_track_plugins
from esrally.utils import console, convert, net


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


class UpdateSamples:
    """
    Used to send samples from a load generator node to the master.
    """

    def __init__(self, client_id, samples):
        self.client_id = client_id
        self.samples = samples


class SamplePostprocessor:
    def __init__(self, metrics_store, downsample_factor, track_meta_data, challenge_meta_data):
        self.logger = logging.getLogger(__name__)
        self.metrics_store = metrics_store
        self.track_meta_data = track_meta_data
        self.challenge_meta_data = challenge_meta_data
        self.throughput_calculator = ThroughputCalculator()
        self.downsample_factor = downsample_factor

    def __call__(self, raw_samples):
        if len(raw_samples) == 0:
            return
        total_start = time.perf_counter()
        start = total_start
        final_sample_count = 0
        for idx, sample in enumerate(raw_samples):
            if idx % self.downsample_factor == 0:
                final_sample_count += 1
                meta_data = self.merge(
                    self.track_meta_data,
                    self.challenge_meta_data,
                    sample.operation_meta_data,
                    sample.task.meta_data,
                    sample.request_meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="latency",
                    value=convert.seconds_to_ms(sample.latency),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="service_time",
                    value=convert.seconds_to_ms(sample.service_time),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                self.metrics_store.put_value_cluster_level(
                    name="processing_time",
                    value=convert.seconds_to_ms(sample.processing_time),
                    unit="ms",
                    task=sample.task.name,
                    operation=sample.operation_name,
                    operation_type=sample.operation_type,
                    sample_type=sample.sample_type,
                    absolute_time=sample.absolute_time,
                    relative_time=sample.relative_time,
                    meta_data=meta_data,
                )

                for timing in sample.dependent_timings:
                    self.metrics_store.put_value_cluster_level(
                        name="service_time",
                        value=convert.seconds_to_ms(timing.service_time),
                        unit="ms",
                        task=timing.task.name,
                        operation=timing.operation_name,
                        operation_type=timing.operation_type,
                        sample_type=timing.sample_type,
                        absolute_time=timing.absolute_time,
                        relative_time=timing.relative_time,
                        meta_data=meta_data,
                    )

        end = time.perf_counter()
        self.logger.debug("Storing latency and service time took [%f] seconds.", (end - start))
        start = end
        aggregates = self.throughput_calculator.calculate(raw_samples)
        end = time.perf_counter()
        self.logger.debug("Calculating throughput took [%f] seconds.", (end - start))
        start = end
        for task, samples in aggregates.items():
            meta_data = self.merge(self.track_meta_data, self.challenge_meta_data, task.operation.meta_data, task.meta_data)
            for absolute_time, relative_time, sample_type, throughput, throughput_unit in samples:
                self.metrics_store.put_value_cluster_level(
                    name="throughput",
                    value=throughput,
                    unit=throughput_unit,
                    task=task.name,
                    operation=task.operation.name,
                    operation_type=task.operation.type,
                    sample_type=sample_type,
                    absolute_time=absolute_time,
                    relative_time=relative_time,
                    meta_data=meta_data,
                )
        end = time.perf_counter()
        self.logger.debug("Storing throughput took [%f] seconds.", (end - start))
        start = end
        # this will be a noop for the in-memory metrics store. If we use an ES metrics store however, this will ensure that we already send
        # the data and also clear the in-memory buffer. This allows users to see data already while running the benchmark. In cases where
        # it does not matter (i.e. in-memory) we will still defer this step until the end.
        #
        # Don't force refresh here in the interest of short processing times. We don't need to query immediately afterwards so there is
        # no need for frequent refreshes.
        self.metrics_store.flush(refresh=False)
        end = time.perf_counter()
        self.logger.debug("Flushing the metrics store took [%f] seconds.", (end - start))
        self.logger.debug(
            "Postprocessing [%d] raw samples (downsampled to [%d] samples) took [%f] seconds in total.",
            len(raw_samples),
            final_sample_count,
            (end - total_start),
        )

    def merge(self, *args):
        result = {}
        for arg in args:
            if arg is not None:
                result.update(arg)
        return result


def calculate_worker_assignments(host_configs, client_count):
    """
    Assigns clients to workers on the provided hosts.

    :param host_configs: A list of dicts where each dict contains the host name (key: ``host``) and the number of
                         available CPU cores (key: ``cores``).
    :param client_count: The number of clients that should be used at most.
    :return: A list of dicts containing the host (key: ``host``) and a list of workers (key ``workers``). Each entry
             in that list contains another list with the clients that should be assigned to these workers.
    """
    assignments = []
    client_idx = 0
    host_count = len(host_configs)
    clients_per_host = math.ceil(client_count / host_count)
    remaining_clients = client_count
    for host_config in host_configs:
        # the last host might not need to simulate as many clients as the rest of the hosts as we eagerly
        # assign clients to hosts.
        clients_on_this_host = min(clients_per_host, remaining_clients)
        assignment = {
            "host": host_config["host"],
            "workers": [],
        }
        assignments.append(assignment)

        workers_on_this_host = host_config["cores"]
        clients_per_worker = [0] * workers_on_this_host

        # determine how many clients each worker should simulate
        for c in range(clients_on_this_host):
            clients_per_worker[c % workers_on_this_host] += 1

        # assign client ids to workers
        for client_count_for_worker in clients_per_worker:
            worker_assignment = []
            assignment["workers"].append(worker_assignment)
            for c in range(client_idx, client_idx + client_count_for_worker):
                worker_assignment.append(c)
            client_idx += client_count_for_worker

        remaining_clients -= clients_on_this_host

    assert remaining_clients == 0

    return assignments


ClientAllocation = collections.namedtuple("ClientAllocation", ["client_id", "task"])


class ClientAllocations:
    def __init__(self):
        self.allocations = []

    def add(self, client_id, tasks):
        self.allocations.append({"client_id": client_id, "tasks": tasks})

    def is_joinpoint(self, task_index):
        return all(isinstance(t.task, JoinPoint) for t in self.tasks(task_index))

    def tasks(self, task_index, remove_empty=True):
        current_tasks = []
        for allocation in self.allocations:
            tasks_at_index = allocation["tasks"][task_index]
            if remove_empty and tasks_at_index is not None:
                current_tasks.append(ClientAllocation(allocation["client_id"], tasks_at_index))
        return current_tasks


class Sampler:
    """
    Encapsulates management of gathered samples.
    """

    def __init__(self, start_timestamp, buffer_size=16384):
        self.start_timestamp = start_timestamp
        self.q = queue.Queue(maxsize=buffer_size)
        self.logger = logging.getLogger(__name__)

    def add(
        self,
        task,
        client_id,
        sample_type,
        meta_data,
        absolute_time,
        request_start,
        latency,
        service_time,
        processing_time,
        throughput,
        ops,
        ops_unit,
        time_period,
        percent_completed,
        dependent_timing=None,
    ):
        try:
            self.q.put_nowait(
                Sample(
                    client_id,
                    absolute_time,
                    request_start,
                    self.start_timestamp,
                    task,
                    sample_type,
                    meta_data,
                    latency,
                    service_time,
                    processing_time,
                    throughput,
                    ops,
                    ops_unit,
                    time_period,
                    percent_completed,
                    dependent_timing,
                )
            )
        except queue.Full:
            self.logger.warning("Dropping sample for [%s] due to a full sampling queue.", task.operation.name)

    @property
    def samples(self):
        samples = []
        try:
            while True:
                samples.append(self.q.get_nowait())
        except queue.Empty:
            pass
        return samples


class Sample:
    def __init__(
        self,
        client_id,
        absolute_time,
        request_start,
        task_start,
        task,
        sample_type,
        request_meta_data,
        latency,
        service_time,
        processing_time,
        throughput,
        total_ops,
        total_ops_unit,
        time_period,
        percent_completed,
        dependent_timing=None,
        operation_name=None,
        operation_type=None,
    ):
        self.client_id = client_id
        self.absolute_time = absolute_time
        self.request_start = request_start
        self.task_start = task_start
        self.task = task
        self.sample_type = sample_type
        self.request_meta_data = request_meta_data
        self.latency = latency
        self.service_time = service_time
        self.processing_time = processing_time
        self.throughput = throughput
        self.total_ops = total_ops
        self.total_ops_unit = total_ops_unit
        self.time_period = time_period
        self._dependent_timing = dependent_timing
        self._operation_name = operation_name
        self._operation_type = operation_type
        # may be None for eternal tasks!
        self.percent_completed = percent_completed

    @property
    def operation_name(self):
        return self._operation_name if self._operation_name else self.task.operation.name

    @property
    def operation_type(self):
        return self._operation_type if self._operation_type else self.task.operation.type

    @property
    def operation_meta_data(self):
        return self.task.operation.meta_data

    @property
    def relative_time(self):
        return self.request_start - self.task_start

    @property
    def dependent_timings(self):
        if self._dependent_timing:
            for t in self._dependent_timing:
                yield Sample(
                    self.client_id,
                    t["absolute_time"],
                    t["request_start"],
                    self.task_start,
                    self.task,
                    self.sample_type,
                    self.request_meta_data,
                    0,
                    t["service_time"],
                    0,
                    0,
                    self.total_ops,
                    self.total_ops_unit,
                    self.time_period,
                    self.percent_completed,
                    None,
                    t["operation"],
                    t["operation-type"],
                )

    def __repr__(self, *args, **kwargs):
        return (
            f"[{self.absolute_time}; {self.relative_time}] [client [{self.client_id}]] [{self.task}] "
            f"[{self.sample_type}]: [{self.latency}s] request latency, [{self.service_time}s] service time, "
            f"[{self.total_ops} {self.total_ops_unit}]"
        )


def select_challenge(config, t):
    challenge_name = config.opts("track", "challenge.name")
    selected_challenge = t.find_challenge_or_default(challenge_name)

    if not selected_challenge:
        raise exceptions.SystemSetupError(
            "Unknown challenge [%s] for track [%s]. You can list the available tracks and their "
            "challenges with %s list tracks." % (challenge_name, t.name, PROGRAM_NAME)
        )
    return selected_challenge


class ThroughputCalculator:
    class TaskStats:
        """
        Stores per task numbers needed for throughput calculation in between multiple calculations.
        """

        def __init__(self, bucket_interval, sample_type, start_time):
            self.unprocessed = []
            self.total_count = 0
            self.interval = 0
            self.bucket_interval = bucket_interval
            # the first bucket is complete after one bucket interval is over
            self.bucket = bucket_interval
            self.sample_type = sample_type
            self.has_samples_in_sample_type = False
            # start relative to the beginning of our (calculation) time slice.
            self.start_time = start_time

        @property
        def throughput(self):
            return self.total_count / self.interval

        def maybe_update_sample_type(self, current_sample_type):
            if self.sample_type < current_sample_type:
                self.sample_type = current_sample_type
                self.has_samples_in_sample_type = False

        def update_interval(self, absolute_sample_time):
            self.interval = max(absolute_sample_time - self.start_time, self.interval)

        def can_calculate_throughput(self):
            return self.interval > 0 and self.interval >= self.bucket

        def can_add_final_throughput_sample(self):
            return self.interval > 0 and not self.has_samples_in_sample_type

        def finish_bucket(self, new_total):
            self.unprocessed = []
            self.total_count = new_total
            self.has_samples_in_sample_type = True
            self.bucket = int(self.interval) + self.bucket_interval

    def __init__(self):
        self.task_stats = {}

    def calculate(self, samples, bucket_interval_secs=1):
        """
        Calculates global throughput based on samples gathered from multiple load generators.

        :param samples: A list containing all samples from all load generators.
        :param bucket_interval_secs: The bucket interval for aggregations.
        :return: A global view of throughput samples.
        """

        samples_per_task = {}
        # first we group all samples by task (operation).
        for sample in samples:
            k = sample.task
            if k not in samples_per_task:
                samples_per_task[k] = []
            samples_per_task[k].append(sample)

        global_throughput = {}
        # with open("raw_samples_new.csv", "a") as sample_log:
        # print("client_id,absolute_time,relative_time,operation,sample_type,total_ops,time_period", file=sample_log)
        for k, v in samples_per_task.items():
            task = k
            if task not in global_throughput:
                global_throughput[task] = []
            # sort all samples by time
            if task in self.task_stats:
                samples = itertools.chain(v, self.task_stats[task].unprocessed)
            else:
                samples = v
            current_samples = sorted(samples, key=lambda s: s.absolute_time)

            # Calculate throughput based on service time if the runner does not provide one, otherwise use it as is and
            # only transform the values into the expected structure.
            first_sample = current_samples[0]
            if first_sample.throughput is None:
                task_throughput = self.calculate_task_throughput(task, current_samples, bucket_interval_secs)
            else:
                task_throughput = self.map_task_throughput(current_samples)
            global_throughput[task].extend(task_throughput)

        return global_throughput

    def calculate_task_throughput(self, task, current_samples, bucket_interval_secs):
        task_throughput = []

        if task not in self.task_stats:
            first_sample = current_samples[0]
            self.task_stats[task] = ThroughputCalculator.TaskStats(
                bucket_interval=bucket_interval_secs,
                sample_type=first_sample.sample_type,
                start_time=first_sample.absolute_time - first_sample.time_period,
            )
        current = self.task_stats[task]
        count = current.total_count
        last_sample = None
        for sample in current_samples:
            last_sample = sample
            # print("%d,%f,%f,%s,%s,%d,%f" %
            #       (sample.client_id, sample.absolute_time, sample.relative_time, sample.operation, sample.sample_type,
            #        sample.total_ops, sample.time_period), file=sample_log)

            # once we have seen a new sample type, we stick to it.
            current.maybe_update_sample_type(sample.sample_type)

            # we need to store the total count separately and cannot update `current.total_count` immediately here
            # because we would count all raw samples in `unprocessed` twice. Hence, we'll only update
            # `current.total_count` when we have calculated a new throughput sample.
            count += sample.total_ops
            current.update_interval(sample.absolute_time)

            if current.can_calculate_throughput():
                current.finish_bucket(count)
                task_throughput.append(
                    (
                        sample.absolute_time,
                        sample.relative_time,
                        current.sample_type,
                        current.throughput,
                        # we calculate throughput per second
                        f"{sample.total_ops_unit}/s",
                    )
                )
            else:
                current.unprocessed.append(sample)

        # also include the last sample if we don't have one for the current sample type, even if it is below the bucket
        # interval (mainly needed to ensure we show throughput data in test mode)
        if last_sample is not None and current.can_add_final_throughput_sample():
            current.finish_bucket(count)
            task_throughput.append(
                (
                    last_sample.absolute_time,
                    last_sample.relative_time,
                    current.sample_type,
                    current.throughput,
                    f"{last_sample.total_ops_unit}/s",
                )
            )

        return task_throughput

    def map_task_throughput(self, current_samples):
        throughput = []
        for sample in current_samples:
            throughput.append(
                (
                    sample.absolute_time,
                    sample.relative_time,
                    sample.sample_type,
                    sample.throughput,
                    f"{sample.total_ops_unit}/s",
                )
            )
        return throughput


class AsyncIoAdapter:
    def __init__(self, cfg, track, task_allocations, sampler, cancel, complete, abort_on_error):
        self.cfg = cfg
        self.track = track
        self.task_allocations = task_allocations
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.abort_on_error = abort_on_error
        self.profiling_enabled = self.cfg.opts("driver", "profiling")
        self.assertions_enabled = self.cfg.opts("driver", "assertions")
        self.debug_event_loop = self.cfg.opts("system", "async.debug", mandatory=False, default_value=False)
        self.logger = logging.getLogger(__name__)

    def __call__(self, *args, **kwargs):
        # only possible in Python 3.7+ (has introduced get_running_loop)
        # try:
        #     loop = asyncio.get_running_loop()
        # except RuntimeError:
        #     loop = asyncio.new_event_loop()
        #     asyncio.set_event_loop(loop)
        loop = asyncio.new_event_loop()
        loop.set_debug(self.debug_event_loop)
        loop.set_exception_handler(self._logging_exception_handler)
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self.run())
        finally:
            loop.close()

    def _logging_exception_handler(self, loop, context):
        self.logger.error("Uncaught exception in event loop: %s", context)

    async def run(self):
        def es_clients(all_hosts, all_client_options):
            es = {}
            for cluster_name, cluster_hosts in all_hosts.items():
                es[cluster_name] = client.EsClientFactory(cluster_hosts, all_client_options[cluster_name]).create_async()
            return es

        # Properly size the internal connection pool to match the number of expected clients but allow the user
        # to override it if needed.
        client_count = len(self.task_allocations)
        es = es_clients(self.cfg.opts("client", "hosts").all_hosts, self.cfg.opts("client", "options").with_max_connections(client_count))

        self.logger.info("Task assertions enabled: %s", str(self.assertions_enabled))
        runner.enable_assertions(self.assertions_enabled)

        aws = []
        # A parameter source should only be created once per task - it is partitioned later on per client.
        params_per_task = {}
        for client_id, task_allocation in self.task_allocations:
            task = task_allocation.task
            if task not in params_per_task:
                param_source = track.operation_parameters(self.track, task)
                params_per_task[task] = param_source
            schedule = schedule_for(task_allocation, params_per_task[task])
            async_executor = AsyncExecutor(
                client_id, task, schedule, es, self.sampler, self.cancel, self.complete, task.error_behavior(self.abort_on_error)
            )
            final_executor = AsyncProfiler(async_executor) if self.profiling_enabled else async_executor
            aws.append(final_executor())
        run_start = time.perf_counter()
        try:
            _ = await asyncio.gather(*aws)
        finally:
            run_end = time.perf_counter()
            self.logger.info("Total run duration: %f seconds.", (run_end - run_start))
            await asyncio.get_event_loop().shutdown_asyncgens()
            shutdown_asyncgens_end = time.perf_counter()
            self.logger.info("Total time to shutdown asyncgens: %f seconds.", (shutdown_asyncgens_end - run_end))
            for e in es.values():
                await e.transport.close()
            transport_close_end = time.perf_counter()
            self.logger.info("Total time to close transports: %f seconds.", (shutdown_asyncgens_end - transport_close_end))


class AsyncProfiler:
    def __init__(self, target):
        """
        :param target: The actual executor which should be profiled.
        """
        self.target = target
        self.profile_logger = logging.getLogger("rally.profile")

    async def __call__(self, *args, **kwargs):
        # initialize lazily, we don't need it in the majority of cases
        # pylint: disable=import-outside-toplevel
        import io as python_io

        import yappi

        yappi.start()
        try:
            return await self.target(*args, **kwargs)
        finally:
            yappi.stop()
            s = python_io.StringIO()
            yappi.get_func_stats().print_all(
                out=s, columns={0: ("name", 140), 1: ("ncall", 8), 2: ("tsub", 8), 3: ("ttot", 8), 4: ("tavg", 8)}
            )

            profile = "\n=== Profile START ===\n"
            profile += s.getvalue()
            profile += "=== Profile END ==="
            self.profile_logger.info(profile)


class AsyncExecutor:
    def __init__(self, client_id, task, schedule, es, sampler, cancel, complete, on_error):
        """
        Executes tasks according to the schedule for a given operation.

        :param task: The task that is executed.
        :param schedule: The schedule for this task.
        :param es: Elasticsearch client that will be used to execute the operation.
        :param sampler: A container to store raw samples.
        :param cancel: A shared boolean that indicates we need to cancel execution.
        :param complete: A shared boolean that indicates we need to prematurely complete execution.
        :param on_error: A string specifying how the load generator should behave on errors.
        """
        self.client_id = client_id
        self.task = task
        self.op = task.operation
        self.schedule_handle = schedule
        self.es = es
        self.sampler = sampler
        self.cancel = cancel
        self.complete = complete
        self.on_error = on_error
        self.logger = logging.getLogger(__name__)

    async def __call__(self, *args, **kwargs):
        any_task_completes_parent = self.task.any_completes_parent
        task_completes_parent = self.task.completes_parent
        total_start = time.perf_counter()
        # lazily initialize the schedule
        self.logger.debug("Initializing schedule for client id [%s].", self.client_id)
        schedule = self.schedule_handle()
        # Start the schedule's timer early so the warmup period is independent of any deferred start due to ramp-up
        self.schedule_handle.start()
        rampup_wait_time = self.schedule_handle.ramp_up_wait_time
        if rampup_wait_time:
            self.logger.debug("client id [%s] waiting [%.2f]s for ramp-up.", self.client_id, rampup_wait_time)
            await asyncio.sleep(rampup_wait_time)

        self.logger.debug("Entering main loop for client id [%s].", self.client_id)
        # noinspection PyBroadException
        try:
            async for expected_scheduled_time, sample_type, percent_completed, runner, params in schedule:
                if self.cancel.is_set():
                    self.logger.info("User cancelled execution.")
                    break
                absolute_expected_schedule_time = total_start + expected_scheduled_time
                throughput_throttled = expected_scheduled_time > 0
                if throughput_throttled:
                    rest = absolute_expected_schedule_time - time.perf_counter()
                    if rest > 0:
                        await asyncio.sleep(rest)

                absolute_processing_start = time.time()
                processing_start = time.perf_counter()
                self.schedule_handle.before_request(processing_start)
                async with self.es["default"].new_request_context() as request_context:
                    total_ops, total_ops_unit, request_meta_data = await execute_single(runner, self.es, params, self.on_error)
                    request_start = request_context.request_start
                    request_end = request_context.request_end

                processing_end = time.perf_counter()
                service_time = request_end - request_start
                processing_time = processing_end - processing_start
                time_period = request_end - total_start
                self.schedule_handle.after_request(processing_end, total_ops, total_ops_unit, request_meta_data)
                # Allow runners to override the throughput calculation in very specific circumstances. Usually, Rally
                # assumes that throughput is the "amount of work" (determined by the "weight") per unit of time
                # (determined by the elapsed time period). However, in certain cases (e.g. shard recovery or other
                # long running operations where there is a dedicated stats API to determine progress), it is
                # advantageous if the runner calculates throughput directly. The following restrictions apply:
                #
                # * Only one client must call that runner (when throughput is calculated, it is aggregated across
                #   all clients but if the runner provides it, we take the value as is).
                # * The runner should be rate-limited as each runner call will result in one throughput sample.
                #
                throughput = request_meta_data.pop("throughput", None)
                # Do not calculate latency separately when we run unthrottled. This metric is just confusing then.
                latency = request_end - absolute_expected_schedule_time if throughput_throttled else service_time
                # If this task completes the parent task we should *not* check for completion by another client but
                # instead continue until our own runner has completed. We need to do this because the current
                # worker (process) could run multiple clients that execute the same task. We do not want all clients to
                # finish this task as soon as the first of these clients has finished but rather continue until the last
                # client has finished that task.
                if task_completes_parent:
                    completed = runner.completed
                else:
                    completed = self.complete.is_set() or runner.completed
                # last sample should bump progress to 100% if externally completed.
                if completed:
                    progress = 1.0
                elif runner.percent_completed:
                    progress = runner.percent_completed
                else:
                    progress = percent_completed

                self.sampler.add(
                    self.task,
                    self.client_id,
                    sample_type,
                    request_meta_data,
                    absolute_processing_start,
                    request_start,
                    latency,
                    service_time,
                    processing_time,
                    throughput,
                    total_ops,
                    total_ops_unit,
                    time_period,
                    progress,
                    request_meta_data.pop("dependent_timing", None),
                )

                if completed:
                    self.logger.info("Task [%s] is considered completed due to external event.", self.task)
                    break
        except BaseException as e:
            self.logger.exception("Could not execute schedule")
            raise exceptions.RallyError(f"Cannot run task [{self.task}]: {e}") from None
        finally:
            # Actively set it if this task completes its parent
            if task_completes_parent:
                self.logger.info(
                    "Task [%s] completes parent. Client id [%s] is finished executing it and signals completion.",
                    self.task,
                    self.client_id,
                )
                self.complete.set()
            elif any_task_completes_parent:
                self.logger.info(
                    "Task [%s] completes parent. Client id [%s] is finished executing it and signals completion of all "
                    "remaining clients, immediately.",
                    self.task,
                    self.client_id,
                )
                self.complete.set()


async def execute_single(runner, es, params, on_error):
    """
    Invokes the given runner once and provides the runner's return value in a uniform structure.

    :return: a triple of: total number of operations, unit of operations, a dict of request meta data (may be None).
    """
    # pylint: disable=import-outside-toplevel
    import elasticsearch

    fatal_error = False
    try:
        async with runner:
            return_value = await runner(es, params)
        if isinstance(return_value, tuple) and len(return_value) == 2:
            total_ops, total_ops_unit = return_value
            request_meta_data = {"success": True}
        elif isinstance(return_value, dict):
            total_ops = return_value.pop("weight", 1)
            total_ops_unit = return_value.pop("unit", "ops")
            request_meta_data = return_value
            if "success" not in request_meta_data:
                request_meta_data["success"] = True
        else:
            total_ops = 1
            total_ops_unit = "ops"
            request_meta_data = {"success": True}
    except elasticsearch.TransportError as e:
        # we *specifically* want to distinguish connection refused (a node died?) from connection timeouts
        # pylint: disable=unidiomatic-typecheck
        if type(e) is elasticsearch.ConnectionError:
            fatal_error = True

        total_ops = 0
        total_ops_unit = "ops"
        request_meta_data = {"success": False, "error-type": "transport"}
        # The ES client will sometimes return string like "N/A" or "TIMEOUT" for connection errors.
        if isinstance(e.status_code, int):
            request_meta_data["http-status"] = e.status_code
        # connection timeout errors don't provide a helpful description
        if isinstance(e, elasticsearch.ConnectionTimeout):
            request_meta_data["error-description"] = "network connection timed out"
        elif e.info:
            request_meta_data["error-description"] = f"{e.error} ({e.info})"
        else:
            if isinstance(e.error, bytes):
                error_description = e.error.decode("utf-8")
            else:
                error_description = str(e.error)
            request_meta_data["error-description"] = error_description
    except KeyError as e:
        logging.getLogger(__name__).exception("Cannot execute runner [%s]; most likely due to missing parameters.", str(runner))
        msg = "Cannot execute [%s]. Provided parameters are: %s. Error: [%s]." % (str(runner), list(params.keys()), str(e))
        raise exceptions.SystemSetupError(msg)

    if not request_meta_data["success"]:
        if on_error == "abort" or fatal_error:
            msg = "Request returned an error. Error type: %s" % request_meta_data.get("error-type", "Unknown")
            description = request_meta_data.get("error-description")
            if description:
                msg += ", Description: %s" % description
            raise exceptions.RallyAssertionError(msg)
    return total_ops, total_ops_unit, request_meta_data


class JoinPoint:
    def __init__(self, id, clients_executing_completing_task=None, any_task_completes_parent=None):
        """

        :param id: The join point's id.
        :param clients_executing_completing_task: An array of client indices which execute a task that can prematurely complete its parent
        element. Provide 'None' or an empty array if no task satisfies this predicate.
        """
        if clients_executing_completing_task is None:
            clients_executing_completing_task = []
        if any_task_completes_parent is None:
            any_task_completes_parent = []
        self.id = id
        self.any_task_completes_parent = any_task_completes_parent
        self.clients_executing_completing_task = clients_executing_completing_task
        self.num_clients_executing_completing_task = len(clients_executing_completing_task)
        self.preceding_task_completes_parent = self.num_clients_executing_completing_task > 0

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.id == other.id

    def __repr__(self, *args, **kwargs):
        return "JoinPoint(%s)" % self.id


class TaskAllocation:
    def __init__(self, task, client_index_in_task, global_client_index, total_clients):
        """

        :param task: The current task which is always a leaf task.
        :param client_index_in_task: The task-specific index for the allocated client.
        :param global_client_index:  The globally unique index for the allocated client across
                                     all concurrently executed tasks.
        :param total_clients: The total number of clients executing tasks concurrently.
        """
        self.task = task
        self.client_index_in_task = client_index_in_task
        self.global_client_index = global_client_index
        self.total_clients = total_clients

    def __hash__(self):
        return hash(self.task) ^ hash(self.global_client_index)

    def __eq__(self, other):
        return isinstance(other, type(self)) and self.task == other.task and self.global_client_index == other.global_client_index

    def __repr__(self, *args, **kwargs):
        return (
            f"TaskAllocation [{self.client_index_in_task}/{self.task.clients}] for {self.task} "
            f"and [{self.global_client_index}/{self.total_clients}] in total"
        )


class Allocator:
    """
    Decides which operations runs on which client and how to partition them.
    """

    def __init__(self, schedule):
        self.schedule = schedule

    @property
    def allocations(self):
        """
        Calculates an allocation matrix consisting of two dimensions. The first dimension is the client. The second dimension are the task
         this client needs to run. The matrix shape is rectangular (i.e. it is not ragged). There are three types of entries in the matrix:

          1. Normal tasks: They need to be executed by a client.
          2. Join points: They are used as global coordination points which all clients need to reach until the benchmark can go on. They
                          indicate that a client has to wait until the master signals it can go on.
          3. `None`: These are inserted by the allocator to keep the allocation matrix rectangular. Clients have to skip `None` entries
                     until one of the other entry types are encountered.

        :return: An allocation matrix with the structure described above.
        """
        max_clients = self.clients
        allocations = [None] * max_clients
        for client_index in range(max_clients):
            allocations[client_index] = []
        join_point_id = 0
        # start with an artificial join point to allow master to coordinate that all clients start at the same time
        next_join_point = JoinPoint(join_point_id)
        for client_index in range(max_clients):
            allocations[client_index].append(next_join_point)
        join_point_id += 1

        for task in self.schedule:
            start_client_index = 0
            clients_executing_completing_task = []
            any_task_completes_parent = []
            for sub_task in task:
                for client_index in range(start_client_index, start_client_index + sub_task.clients):
                    # this is the actual client that will execute the task. It may differ from the logical one in case we over-commit (i.e.
                    # more tasks than actually available clients)
                    physical_client_index = client_index % max_clients
                    if sub_task.completes_parent:
                        clients_executing_completing_task.append(physical_client_index)
                    elif sub_task.any_completes_parent:
                        any_task_completes_parent.append(physical_client_index)

                    ta = TaskAllocation(
                        task=sub_task,
                        client_index_in_task=client_index - start_client_index,
                        global_client_index=client_index,
                        # if task represents a parallel structure this is the total number of clients
                        # executing sub-tasks concurrently.
                        total_clients=task.clients,
                    )
                    allocations[physical_client_index].append(ta)
                start_client_index += sub_task.clients
            # uneven distribution between tasks and clients, e.g. there are 5 (parallel) tasks but only 2 clients. Then, one of them
            # executes three tasks, the other one only two. So we need to fill in a `None` for the second one.
            if start_client_index % max_clients > 0:
                # pin the index range to [0, max_clients). This simplifies the code below.
                start_client_index = start_client_index % max_clients
                for client_index in range(start_client_index, max_clients):
                    allocations[client_index].append(None)

            # let all clients join after each task, then we go on
            next_join_point = JoinPoint(join_point_id, clients_executing_completing_task, any_task_completes_parent)
            for client_index in range(max_clients):
                allocations[client_index].append(next_join_point)
            join_point_id += 1
        return allocations

    @property
    def join_points(self):
        """
        :return: A list of all join points for this allocations.
        """
        return [allocation for allocation in self.allocations[0] if isinstance(allocation, JoinPoint)]

    @property
    def tasks_per_joinpoint(self):
        """

        Calculates a flat list of all tasks that are run in between join points.

        Consider the following schedule (2 clients):

        1. task1 and task2 run by both clients in parallel
        2. join point
        3. task3 run by client 1
        4. join point

        The results in: [{task1, task2}, {task3}]

        :return: A list of sets containing all tasks.
        """
        tasks = []
        current_tasks = set()

        allocs = self.allocations
        # assumption: the shape of allocs is rectangular (i.e. each client contains the same number of elements)
        for idx in range(0, len(allocs[0])):
            for client in range(0, self.clients):
                allocation = allocs[client][idx]
                if isinstance(allocation, TaskAllocation):
                    current_tasks.add(allocation.task)
                elif isinstance(allocation, JoinPoint) and len(current_tasks) > 0:
                    tasks.append(current_tasks)
                    current_tasks = set()

        return tasks

    @property
    def clients(self):
        """
        :return: The maximum number of clients involved in executing the given schedule.
        """
        max_clients = 1
        for task in self.schedule:
            max_clients = max(max_clients, task.clients)
        return max_clients


#######################################
#
# Scheduler related stuff
#
#######################################


# Runs a concrete schedule on one worker client
# Needs to determine the runners and concrete iterations per client.
def schedule_for(task_allocation, parameter_source):
    """
    Calculates a client's schedule for a given task.

    :param task_allocation: The task allocation that should be executed by this schedule.
    :param parameter_source: The parameter source that should be used for this task.
    :return: A generator for the operations the given client needs to perform for this task.
    """
    logger = logging.getLogger(__name__)
    task = task_allocation.task
    op = task.operation
    sched = scheduler.scheduler_for(task)

    # We cannot use the global client index here because we need to support parallel execution of tasks
    # with multiple clients. Consider the following scenario:
    #
    # * Clients 0-3 bulk index into indexA
    # * Clients 4-7 bulk index into indexB
    #
    # Now we need to ensure that we start partitioning parameters correctly in both cases. And that means we
    # need to start from (client) index 0 in both cases instead of 0 for indexA and 4 for indexB.
    client_index = task_allocation.client_index_in_task

    # guard all logging statements with the client index and only emit them for the first client. This information is
    # repetitive and may cause issues in thespian with many clients (an excessive number of actor messages is sent).
    if client_index == 0:
        logger.info("Choosing [%s] for [%s].", sched, task)
    runner_for_op = runner.runner_for(op.type)
    params_for_op = parameter_source.partition(client_index, task.clients)
    if hasattr(sched, "parameter_source"):
        if client_index == 0:
            logger.debug("Setting parameter source [%s] for scheduler [%s]", params_for_op, sched)
        sched.parameter_source = params_for_op

    if requires_time_period_schedule(task, runner_for_op, params_for_op):
        warmup_time_period = task.warmup_time_period if task.warmup_time_period else 0
        if client_index == 0:
            logger.info(
                "Creating time-period based schedule with [%s] distribution for [%s] with a warmup period of [%s] "
                "seconds and a time period of [%s] seconds.",
                task.schedule,
                task.name,
                str(warmup_time_period),
                str(task.time_period),
            )
        loop_control = TimePeriodBased(warmup_time_period, task.time_period)
    else:
        warmup_iterations = task.warmup_iterations if task.warmup_iterations else 0
        if task.iterations:
            iterations = task.iterations
        elif params_for_op.infinite:
            # this is usually the case if the parameter source provides a constant
            iterations = 1
        else:
            iterations = None
        if client_index == 0:
            logger.info(
                "Creating iteration-count based schedule with [%s] distribution for [%s] with [%s] warmup "
                "iterations and [%s] iterations.",
                task.schedule,
                task.name,
                str(warmup_iterations),
                str(iterations),
            )
        loop_control = IterationBased(warmup_iterations, iterations)

    if client_index == 0:
        if loop_control.infinite:
            logger.info("Parameter source will determine when the schedule for [%s] terminates.", task.name)
        else:
            logger.info("%s schedule will determine when the schedule for [%s] terminates.", str(loop_control), task.name)

    return ScheduleHandle(task_allocation, sched, loop_control, runner_for_op, params_for_op)


def requires_time_period_schedule(task, task_runner, params):
    if task.warmup_time_period is not None or task.time_period is not None:
        return True
    # user has explicitly requested iterations
    if task.warmup_iterations is not None or task.iterations is not None:
        return False
    # the runner determines completion
    if task_runner.completed is not None:
        return True
    # If the parameter source ends after a finite amount of iterations, we will run with a time-based schedule
    return not params.infinite


class ScheduleHandle:
    def __init__(self, task_allocation, sched, task_progress_control, runner, params):
        """
        Creates a generator that will yield individual task invocations for the provided schedule.

        :param task_allocation: The task allocation for which the schedule is generated.
        :param sched: The scheduler for this task.
        :param task_progress_control: Controls how and how often this generator will loop.
        :param runner: The runner for a given operation.
        :param params: The parameter source for a given operation.
        :return: A generator for the corresponding parameters.
        """
        self.task_allocation = task_allocation
        self.operation_type = task_allocation.task.operation.type
        self.sched = sched
        self.task_progress_control = task_progress_control
        self.runner = runner
        self.params = params
        # TODO: Can we offload the parameter source execution to a different thread / process? Is this too heavy-weight?
        # from concurrent.futures import ThreadPoolExecutor
        # import asyncio
        # self.io_pool_exc = ThreadPoolExecutor(max_workers=1)
        # self.loop = asyncio.get_event_loop()

    @property
    def ramp_up_wait_time(self):
        """
        :return: the number of seconds to wait until this client should start so load can gradually ramp-up.
        """
        ramp_up_time_period = self.task_allocation.task.ramp_up_time_period
        if ramp_up_time_period:
            return ramp_up_time_period * (self.task_allocation.global_client_index / self.task_allocation.total_clients)
        else:
            return 0

    def start(self):
        self.task_progress_control.start()

    def before_request(self, now):
        self.sched.before_request(now)

    def after_request(self, now, weight, unit, request_meta_data):
        self.sched.after_request(now, weight, unit, request_meta_data)

    def params_with_operation_type(self):
        p = self.params.params()
        p.update({"operation-type": self.operation_type})
        return p

    async def __call__(self):
        next_scheduled = 0
        if self.task_progress_control.infinite:
            param_source_knows_progress = hasattr(self.params, "percent_completed")
            while True:
                try:
                    next_scheduled = self.sched.next(next_scheduled)
                    # does not contribute at all to completion. Hence, we cannot define completion.
                    percent_completed = self.params.percent_completed if param_source_knows_progress else None
                    # current_params = await self.loop.run_in_executor(self.io_pool_exc, self.params.params)
                    yield (
                        next_scheduled,
                        self.task_progress_control.sample_type,
                        percent_completed,
                        self.runner,
                        self.params_with_operation_type(),
                    )
                    self.task_progress_control.next()
                except StopIteration:
                    return
        else:
            while not self.task_progress_control.completed:
                try:
                    next_scheduled = self.sched.next(next_scheduled)
                    # current_params = await self.loop.run_in_executor(self.io_pool_exc, self.params.params)
                    yield (
                        next_scheduled,
                        self.task_progress_control.sample_type,
                        self.task_progress_control.percent_completed,
                        self.runner,
                        self.params_with_operation_type(),
                    )
                    self.task_progress_control.next()
                except StopIteration:
                    return


class TimePeriodBased:
    def __init__(self, warmup_time_period, time_period):
        self._warmup_time_period = warmup_time_period
        self._time_period = time_period
        if warmup_time_period is not None and time_period is not None:
            self._duration = self._warmup_time_period + self._time_period
        else:
            self._duration = None
        self._start = None
        self._now = None

    def start(self):
        self._now = time.perf_counter()
        self._start = self._now

    @property
    def _elapsed(self):
        return self._now - self._start

    @property
    def sample_type(self):
        return metrics.SampleType.Warmup if self._elapsed < self._warmup_time_period else metrics.SampleType.Normal

    @property
    def infinite(self):
        return self._time_period is None

    @property
    def percent_completed(self):
        return self._elapsed / self._duration

    @property
    def completed(self):
        return self._now >= (self._start + self._duration)

    def next(self):
        self._now = time.perf_counter()

    def __str__(self):
        return "time-period-based"


class IterationBased:
    def __init__(self, warmup_iterations, iterations):
        self._warmup_iterations = warmup_iterations
        self._iterations = iterations
        if warmup_iterations is not None and iterations is not None:
            self._total_iterations = self._warmup_iterations + self._iterations
            if self._total_iterations == 0:
                raise exceptions.RallyAssertionError("Operation must run at least for one iteration.")
        else:
            self._total_iterations = None
        self._it = None

    def start(self):
        self._it = 0

    @property
    def sample_type(self):
        return metrics.SampleType.Warmup if self._it < self._warmup_iterations else metrics.SampleType.Normal

    @property
    def infinite(self):
        return self._iterations is None

    @property
    def percent_completed(self):
        return (self._it + 1) / self._total_iterations

    @property
    def completed(self):
        return self._it >= self._total_iterations

    def next(self):
        self._it += 1

    def __str__(self):
        return "iteration-count-based"


@dataclass(frozen=True)
class WorkerTask:
    """
    Unit of work that should be completed by the low-level TaskExecutor
    """

    func: Callable
    params: dict