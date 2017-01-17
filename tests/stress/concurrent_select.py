#!/usr/bin/env impala-python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# This module is used to stress test Impala by running queries concurrently. Only SELECT
# queries are used.
#
# Stress test outline (and notes):
#  1) Get a set of queries as requested by the user from the CLI options.
#  2) For each query, run it individually to find:
#      a) Minimum mem limit to avoid spilling
#      b) Minimum mem limit to successfully run the query (spilling allowed)
#      c) Runtime when no mem was spilled
#      d) Runtime when mem was spilled
#      e) A row order independent hash of the result set.
#     This is a slow process so the results will be written to disk for reuse.
#  3) Find the memory available to Impalad. This will be done by finding the minimum
#     memory available across all impalads (-mem_limit startup option). Ideally, for
#     maximum stress, all impalads will have the same memory configuration but this is
#     not required.
#  4) Optionally, set an amount of memory that can be overcommitted. Overcommitting
#     memory can increase memory pressure which can result in memory being spilled to
#     disk.
#  5) Start submitting queries. There are two modes for throttling the number of
#     concurrent queries:
#      a) Submit queries until all available memory (as determined by items 3 and 4) is
#         used. Before running the query a query mem limit is set between 2a and 2b.
#         (There is a runtime option to increase the likelihood that a query will be
#         given the full 2a limit to avoid spilling.)
#      b) TODO: Use admission control.
#  6) Randomly cancel queries to test cancellation. There is a runtime option to control
#     the likelihood that a query will be randomly canceled.
#  7) If a query errored, verify that memory was overcommitted during execution and the
#     error is a mem limit exceeded error. There is no other reason a query should error
#     and any such error will cause the stress test to stop.
#  8) Verify the result set hash of successful queries if there are no DML queries in the
#     current run.

from __future__ import print_function

import json
import logging
import os
import re
import signal
import sys
import threading
import traceback
from Queue import Empty   # Must be before Queue below
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
from multiprocessing import Lock, Process, Queue, Value
from random import choice, random, randrange
from sys import exit, maxint
from tempfile import gettempdir
from textwrap import dedent
from threading import current_thread, Thread
from time import sleep, time

import tests.util.test_file_parser as test_file_parser
from tests.comparison.cluster import Timeout
from tests.comparison.db_types import Int, TinyInt, SmallInt, BigInt
from tests.comparison.model_translator import SqlWriter
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import DefaultProfile
from tests.util.parse_util import parse_mem_to_mb
from tests.util.thrift_util import op_handle_to_query_id

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

# Used to short circuit a binary search of the min mem limit. Values will be considered
# equal if they are within this ratio or absolute amount of each other.
MEM_LIMIT_EQ_THRESHOLD_PC = 0.975
MEM_LIMIT_EQ_THRESHOLD_MB = 50

# Regex to extract the estimated memory from an explain plan.
MEM_ESTIMATE_PATTERN = re.compile(r"Estimated.*Memory=(\d+.?\d*)(T|G|M|K)?B")

# The version of the file format containing the collected query runtime info.
RUNTIME_INFO_FILE_VERSION = 3


def create_and_start_daemon_thread(fn, name):
  thread = Thread(target=fn, name=name)
  thread.error = None
  thread.daemon = True
  thread.start()
  return thread


def increment(counter):
  with counter.get_lock():
    counter.value += 1


def print_stacks(*_):
  """Print the stacks of all threads from this script to stderr."""
  thread_names = dict([(t.ident, t.name) for t in threading.enumerate()])
  stacks = list()
  for thread_id, stack in sys._current_frames().items():
    stacks.append(
        "\n# Thread: %s(%d)"
        % (thread_names.get(thread_id, "No name"), thread_id))
    for filename, lineno, name, line in traceback.extract_stack(stack):
      stacks.append('File: "%s", line %d, in %s' % (filename, lineno, name))
      if line:
        stacks.append("  %s" % (line.strip(), ))
  print("\n".join(stacks), file=sys.stderr)


# To help debug hangs, the stacks of all threads can be printed by sending signal USR1
# to each process.
signal.signal(signal.SIGUSR1, print_stacks)


def print_crash_info_if_exists(impala, start_time):
  """If any impalads are found not running, they will assumed to have crashed and an
  error message will be printed to stderr for each stopped impalad. Returns a value
  that evaluates to True if any impalads are stopped.
  """
  max_attempts = 5
  for remaining_attempts in xrange(max_attempts - 1, -1, -1):
    try:
      crashed_impalads = impala.find_crashed_impalads(start_time)
      break
    except Timeout as e:
      LOG.info(
          "Timeout checking if impalads crashed: %s."
          % e + (" Will retry." if remaining_attempts else ""))
  else:
    LOG.error(
        "Aborting after %s failed attempts to check if impalads crashed", max_attempts)
    raise e
  for message in crashed_impalads.itervalues():
    print(message, file=sys.stderr)
  return crashed_impalads


class QueryReport(object):
  """Holds information about a single query run."""

  def __init__(self):
    self.result_hash = None
    self.runtime_secs = None
    self.mem_was_spilled = False
    self.mem_limit_exceeded = False
    self.non_mem_limit_error = None
    self.timed_out = False
    self.was_cancelled = False
    self.profile = None


class MemBroker(object):
  """Provides memory usage coordination for clients running in different processes.
  The broker fulfills reservation requests by blocking as needed so total memory
  used by clients never exceeds the total available memory (including an
  'overcommitable' amount).

  The lock built in to _available is also used to protect access to other members.

  The state stored in this class is actually an encapsulation of part of the state
  of the StressRunner class below. The state here is separated for clarity.
  """

  def __init__(self, real_mem_mb, overcommitable_mem_mb):
    """'real_mem_mb' memory should be the amount of memory that each impalad is able
    to use. 'overcommitable_mem_mb' is the amount of memory that will be dispensed
    over the 'real' amount.
    """
    self._total_mem_mb = real_mem_mb + overcommitable_mem_mb
    self._available = Value("i", self._total_mem_mb)
    self._max_overcommitment = overcommitable_mem_mb

    # Each reservation will be assigned an id. Ids are monotonically increasing. When
    # a reservation crosses the overcommitment threshold, the corresponding reservation
    # id will be stored in '_last_overcommitted_reservation_id' so clients can check
    # to see if memory was overcommitted since their reservation was made (this is a race
    # but an incorrect result will be on the conservative side).
    self._next_reservation_id = Value("L", 0)
    self._last_overcommitted_reservation_id = Value("L", 0)

  @property
  def total_mem_mb(self):
    return self._total_mem_mb

  @property
  def overcommitted_mem_mb(self):
    return max(self._max_overcommitment - self._available.value, 0)

  @property
  def available_mem_mb(self):
    return self._available.value

  @property
  def last_overcommitted_reservation_id(self):
    return self._last_overcommitted_reservation_id.value

  @contextmanager
  def reserve_mem_mb(self, mem_mb):
    """Blocks until the requested amount of memory is available and taken for the caller.
    This function should be used in a 'with' block. The taken memory will
    automatically be released when the 'with' context exits. A numeric id is returned
    so clients can compare against 'last_overcommitted_reservation_id' to see if
    memory was overcommitted since the reservation was obtained.

    with broker.reserve_mem_mb(100) as reservation_id:
      # Run query using 100 MB of memory
      if <query failed>:
        # Immediately check broker.was_overcommitted(reservation_id) to see if
        # memory was overcommitted.
    """
    reservation_id = self._wait_until_reserved(mem_mb)
    try:
      yield reservation_id
    finally:
      self._release(mem_mb)

  def _wait_until_reserved(self, req):
    while True:
      with self._available.get_lock():
        if req <= self._available.value:
          self._available.value -= req
          LOG.debug(
              "Reserved %s MB; %s MB available; %s MB overcommitted",
              req, self._available.value, self.overcommitted_mem_mb)
          reservation_id = self._next_reservation_id.value
          increment(self._next_reservation_id)
          if self.overcommitted_mem_mb > 0:
            self._last_overcommitted_reservation_id.value = reservation_id
          return reservation_id
      sleep(0.1)

  def _release(self, req):
    with self._available.get_lock():
      self._available.value += req
      LOG.debug(
          "Released %s MB; %s MB available; %s MB overcommitted",
          req, self._available.value, self.overcommitted_mem_mb)

  def was_overcommitted(self, reservation_id):
    """Returns True if memory was overcommitted since the given reservation was made.
    For an accurate return value, this should be called just after the query ends
    or while the query is still running.
    """
    return reservation_id <= self._last_overcommitted_reservation_id.value


class StressRunner(object):
  """This class contains functionality related to producing/consuming queries for the
  purpose of stress testing Impala.

  Queries will be executed in separate processes since python threading is limited
  to the use of a single CPU.
  """

  # This is the point at which the work queue will block because it is full.
  WORK_QUEUE_CAPACITY = 10

  def __init__(self):
    self.use_kerberos = False
    self.common_query_options = {}
    self._mem_broker = None
    self._verify_results = True
    self._select_probability = None

    # Synchronized blocking work queue for producer/consumers.
    self._query_queue = Queue(self.WORK_QUEUE_CAPACITY)

    # The Value class provides cross-process shared memory.
    self._mem_mb_needed_for_next_query = Value("i", 0)

    # This lock provides a way to stop new queries from running. This lock must be
    # acquired before writing to _num_queries_started. Before query submission
    # _num_queries_started must be incremented. Reading _num_queries_started is allowed
    # without taking this lock.
    self._submit_query_lock = Lock()

    self.leak_check_interval_mins = None
    self._next_leak_check_unix_time = Value("i", 0)
    self._max_mem_mb_reported_usage = Value("i", -1)   # -1 => Unknown
    self._max_mem_mb_usage = Value("i", -1)   # -1 => Unknown

    # All values below are cumulative.
    self._num_queries_dequeued = Value("i", 0)
    self._num_queries_started = Value("i", 0)
    self._num_queries_finished = Value("i", 0)
    self._num_queries_exceeded_mem_limit = Value("i", 0)
    self._num_queries_cancelled = Value("i", 0)
    self._num_queries_timedout = Value("i", 0)
    self._num_result_mismatches = Value("i", 0)
    self._num_other_errors = Value("i", 0)

    self.cancel_probability = 0
    self.spill_probability = 0

    self.startup_queries_per_sec = 1.0
    self.num_successive_errors_needed_to_abort = 1
    self._num_successive_errors = Value("i", 0)
    self.result_hash_log_dir = gettempdir()

    self._status_headers = [
        " Done", "Running", "Mem Lmt Ex", "Time Out", "Cancel",
        "Err", "Next Qry Mem Lmt", "Tot Qry Mem Lmt", "Tracked Mem", "RSS Mem"]

    self._num_queries_to_run = None
    self._query_producer_thread = None
    self._query_runners = list()
    self._query_consumer_thread = None
    self._mem_polling_thread = None

  def run_queries(
      self, queries, impala, num_queries_to_run, mem_overcommit_pct, should_print_status,
      verify_results, select_probability
  ):
    """Runs queries randomly chosen from 'queries' and stops after 'num_queries_to_run'
    queries have completed. 'select_probability' should be float between 0 and 1, it
    determines the likelihood of choosing a select query (as opposed to a DML query,
    for example).

    Before a query is run, a mem limit will be chosen. 'spill_probability' determines
    the likelihood of choosing a mem limit that will cause spilling. To induce
    spilling, a value is randomly chosen below the min memory needed to avoid spilling
    but above the min memory needed with spilling. So the min/max query memory
    requirements must be determined before calling this method.

    If 'mem_overcommit_pct' is zero, an exception will be raised if any queries
    fail for any reason other than cancellation (controlled by the 'cancel_probability'
    property), since each query should have enough memory to run successfully. If
    non-zero, failures due to insufficient memory will be ignored if memory was
    overcommitted at any time during execution.

    If a query completes without error, the result will be verified if 'verify_results'
    is True. An error will be raised upon a result mismatch. 'verify_results' should be
    false for the case where the expected results are not known in advance, if we are
    running DML queries, for example.
    """
    # TODO: The state from a previous run should be cleared out. This isn't really a
    #       problem now because the one caller (main()) never calls a second time.

    if self.startup_queries_per_sec <= 0:
      raise Exception("Startup queries per second must be positive")
    if self.leak_check_interval_mins is not None and self.leak_check_interval_mins <= 0:
      raise Exception("Memory leak check interval must be positive")

    # If there is a crash, start looking for errors starting from this time.
    start_time = datetime.now()

    self._mem_broker = MemBroker(
        impala.min_impalad_mem_mb,
        int(impala.min_impalad_mem_mb * mem_overcommit_pct / 100))

    self._verify_results = verify_results
    self._select_probability = select_probability

    # Print the status to show the state before starting.
    if should_print_status:
      self._print_status_header()
      self._print_status()
      lines_printed = 1
      last_report_secs = 0

    self._num_queries_to_run = num_queries_to_run
    self._start_polling_mem_usage(impala)
    self._start_producing_queries(queries)
    self._start_consuming_queries(impala)

    # Wait for everything to finish.
    sleep_secs = 0.1
    while (
        self._query_producer_thread.is_alive() or
        self._query_consumer_thread.is_alive() or
        self._query_runners
    ):
      if self._query_producer_thread.error or self._query_consumer_thread.error:
        # This is bad enough to abort early. A failure here probably means there's a
        # bug in this script. The mem poller could be checked for an error too. It is
        # not critical so is ignored.
        LOG.error("Aborting due to error in producer/consumer")
        sys.exit(1)
      checked_for_crashes = False
      for idx, runner in enumerate(self._query_runners):
        if runner.exitcode is not None:
          if runner.exitcode != 0:
            if not checked_for_crashes:
              LOG.info("Checking for crashes")
              if print_crash_info_if_exists(impala, start_time):
                sys.exit(runner.exitcode)
              LOG.info("No crashes detected")
              checked_for_crashes = True
            if (
                self._num_successive_errors.value >=
                self.num_successive_errors_needed_to_abort
            ):
              print(
                  "Aborting due to %s successive errors encountered"
                  % self._num_successive_errors.value, file=sys.stderr)
              sys.exit(1)
          del self._query_runners[idx]
      sleep(sleep_secs)
      if should_print_status:
        last_report_secs += sleep_secs
        if last_report_secs > 5:
          if (
              not self._query_producer_thread.is_alive() or
              not self._query_consumer_thread.is_alive() or
              not self._query_runners
          ):
            LOG.debug("Producer is alive: %s" % self._query_producer_thread.is_alive())
            LOG.debug("Consumer is alive: %s" % self._query_consumer_thread.is_alive())
            LOG.debug("Queue size: %s" % self._query_queue.qsize())
            LOG.debug("Runners: %s" % len(self._query_runners))
          last_report_secs = 0
          lines_printed %= 50
          if lines_printed == 0:
            self._print_status_header()
          self._print_status()
          lines_printed += 1

    # And print the final state.
    if should_print_status:
      self._print_status()

  def _start_producing_queries(self, queries):
    def enqueue_queries():
      # Generate a dict(query type -> list of queries).
      queries_by_type = {}
      for query in queries:
        if query.query_type not in queries_by_type:
          queries_by_type[query.query_type] = []
        queries_by_type[query.query_type].append(query)
      try:
        for _ in xrange(self._num_queries_to_run):
          # First randomly determine a query type, then choose a random query of that
          # type.
          if (
              QueryType.SELECT in queries_by_type and
              (len(queries_by_type.keys()) == 1 or random() < self._select_probability)
          ):
            result = choice(queries_by_type[QueryType.SELECT])
          else:
            query_type = choice([
                key for key in queries_by_type if key != QueryType.SELECT])
            result = choice(queries_by_type[query_type])
          self._query_queue.put(result)
      except Exception as e:
        LOG.error("Error producing queries: %s", e)
        current_thread().error = e
        raise e
    self._query_producer_thread = create_and_start_daemon_thread(
        enqueue_queries, "Query Producer")

  def _start_consuming_queries(self, impala):
    def start_additional_runners_if_needed():
      try:
        while self._num_queries_started.value < self._num_queries_to_run:
          sleep(1.0 / self.startup_queries_per_sec)
          # Remember num dequeued/started are cumulative.
          with self._submit_query_lock:
            if self._num_queries_dequeued.value != self._num_queries_started.value:
              # Assume dequeued queries are stuck waiting for cluster resources so there
              # is no point in starting an additional runner.
              continue
          impalad = impala.impalads[len(self._query_runners) % len(impala.impalads)]
          runner = Process(target=self._start_single_runner, args=(impalad, ))
          runner.daemon = True
          self._query_runners.append(runner)
          runner.start()
      except Exception as e:
        LOG.error("Error consuming queries: %s", e)
        current_thread().error = e
        raise e
    self._query_consumer_thread = create_and_start_daemon_thread(
        start_additional_runners_if_needed, "Query Consumer")

  def _start_polling_mem_usage(self, impala):
    def poll_mem_usage():
      if self.leak_check_interval_mins:
        self._next_leak_check_unix_time.value = int(
            time() + 60 * self.leak_check_interval_mins)
      query_sumbission_is_locked = False

      # Query submission will be unlocked after a memory report has been collected twice
      # while no queries were running.
      ready_to_unlock = None
      try:
        while self._num_queries_started.value < self._num_queries_to_run:
          if ready_to_unlock:
            assert query_sumbission_is_locked, "Query submission not yet locked"
            assert not self._num_queries_running, "Queries are still running"
            LOG.debug("Resuming query submission")
            self._next_leak_check_unix_time.value = int(
                time() + 60 * self.leak_check_interval_mins)
            self._submit_query_lock.release()
            query_sumbission_is_locked = False
            ready_to_unlock = None

          if (
              not query_sumbission_is_locked and
              self.leak_check_interval_mins and
              time() > self._next_leak_check_unix_time.value
          ):
            assert self._num_queries_running <= len(self._query_runners), \
                "Each running query should belong to a runner"
            LOG.debug("Stopping query submission")
            self._submit_query_lock.acquire()
            query_sumbission_is_locked = True

          max_reported, max_actual = self._get_mem_usage_values()
          if max_reported != -1 and max_actual != -1:
            # Value were already retrieved but haven't been used yet. Assume newer
            # values aren't wanted and check again later.
            sleep(1)
            continue

          try:
            max_reported = max(impala.find_impalad_mem_mb_reported_usage())
          except Timeout:
            LOG.debug("Timeout collecting reported mem usage")
            max_reported = -1
          try:
            max_actual = max(impala.find_impalad_mem_mb_actual_usage())
          except Timeout:
            LOG.debug("Timeout collecting reported actual usage")
            max_actual = -1
          self._set_mem_usage_values(max_reported, max_actual)

          if query_sumbission_is_locked and not self._num_queries_running:
            if ready_to_unlock is None:
              ready_to_unlock = False
            else:
              ready_to_unlock = True
      except Exception:
        LOG.debug("Error collecting impalad mem usage", exc_info=True)
        if query_sumbission_is_locked:
          LOG.debug("Resuming query submission")
          self._submit_query_lock.release()
    self._mem_polling_thread = create_and_start_daemon_thread(
        poll_mem_usage, "Mem Usage Poller")

  def _get_mem_usage_values(self, reset=False):
    reported = None
    actual = None
    with self._max_mem_mb_reported_usage.get_lock():
      with self._max_mem_mb_usage.get_lock():
        reported = self._max_mem_mb_reported_usage.value
        actual = self._max_mem_mb_usage.value
        if reset:
          self._max_mem_mb_reported_usage.value = -1
          self._max_mem_mb_usage.value = -1
    return reported, actual

  def _set_mem_usage_values(self, reported, actual):
    with self._max_mem_mb_reported_usage.get_lock():
      with self._max_mem_mb_usage.get_lock():
        self._max_mem_mb_reported_usage.value = reported
        self._max_mem_mb_usage.value = actual

  @property
  def _num_queries_running(self):
    num_running = self._num_queries_started.value - self._num_queries_finished.value
    assert num_running >= 0, "The number of running queries is negative"
    return num_running

  def _start_single_runner(self, impalad):
    """Consumer function to take a query of the queue and run it. This is intended to
    run in a separate process so validating the result set can use a full CPU.
    """
    LOG.debug("New query runner started")
    runner = QueryRunner()
    runner.impalad = impalad
    runner.result_hash_log_dir = self.result_hash_log_dir
    runner.use_kerberos = self.use_kerberos
    runner.common_query_options = self.common_query_options
    runner.connect()

    while not self._query_queue.empty():
      try:
        query = self._query_queue.get(True, 1)
      except Empty:
        continue
      except EOFError:
        LOG.debug("Query running aborting due to closed query queue")
        break
      LOG.debug("Getting query_idx")
      with self._num_queries_dequeued.get_lock():
        query_idx = self._num_queries_dequeued.value
        self._num_queries_dequeued.value += 1

      if not query.required_mem_mb_without_spilling:
        mem_limit = query.required_mem_mb_with_spilling
        solo_runtime = query.solo_runtime_secs_with_spilling
      elif self.spill_probability < random():
        mem_limit = query.required_mem_mb_without_spilling
        solo_runtime = query.solo_runtime_secs_without_spilling
      else:
        mem_limit = randrange(
            query.required_mem_mb_with_spilling,
            query.required_mem_mb_without_spilling + 1)
        solo_runtime = query.solo_runtime_secs_with_spilling

      LOG.debug("Waiting for other query runners to start their queries")
      while query_idx > self._num_queries_started.value:
        sleep(0.1)

      self._mem_mb_needed_for_next_query.value = mem_limit

      LOG.debug("Requesting memory reservation")
      with self._mem_broker.reserve_mem_mb(mem_limit) as reservation_id:
        LOG.debug("Received memory reservation")
        with self._submit_query_lock:
          increment(self._num_queries_started)
        should_cancel = self.cancel_probability > random()
        if should_cancel:
          timeout = randrange(1, max(int(solo_runtime), 2))
        else:
          timeout = solo_runtime * max(
              10, self._num_queries_started.value - self._num_queries_finished.value)
        report = runner.run_query(query, timeout, mem_limit)
        LOG.debug("Got execution report for query")
        if report.timed_out and should_cancel:
          report.was_cancelled = True
        self._update_from_query_report(report)
        if report.non_mem_limit_error:
          error_msg = str(report.non_mem_limit_error)
          # There is a possible race during cancellation. If a fetch request fails (for
          # example due to hitting a mem limit), just before the cancellation request, the
          # server may have already unregistered the query as part of the fetch failure.
          # In that case the server gives an error response saying the handle is invalid.
          if "Invalid query handle" in error_msg and report.timed_out:
            self._num_successive_errors.value = 0
            continue
          # Occasionally the network connection will fail, and depending on when the
          # failure occurred during run_query(), an attempt to get the profile may be
          # made which results in "Invalid session id" since the server destroyed the
          # session upon disconnect.
          if "Invalid session id" in error_msg:
            self._num_successive_errors.value = 0
            continue
          # The server may fail to respond to clients if the load is high. An error
          # message with "connect()...Connection timed out" comes from the impalad so
          # that will not be ignored.
          if (
              ("Connection timed out" in error_msg and "connect()" not in error_msg) or
              "ECONNRESET" in error_msg or
              "couldn't get a client" in error_msg or
              "timeout: timed out" in error_msg
          ):
            self._num_successive_errors.value = 0
            continue
          increment(self._num_successive_errors)
          increment(self._num_other_errors)
          raise Exception("Query failed: %s" % str(report.non_mem_limit_error))
        if (
            report.mem_limit_exceeded and
            not self._mem_broker.was_overcommitted(reservation_id)
        ):
          increment(self._num_successive_errors)
          raise Exception(
              "Unexpected mem limit exceeded; mem was not overcommitted\n"
              "Profile: %s" % report.profile)
        if (
            not report.mem_limit_exceeded and
            not report.timed_out and
            (self._verify_results and report.result_hash != query.result_hash)
        ):
          increment(self._num_successive_errors)
          increment(self._num_result_mismatches)
          raise Exception(
              "Result hash mismatch; expected %s, got %s\nQuery: %s"
              % (query.result_hash, report.result_hash, query.sql))
        self._num_successive_errors.value = 0

  def _print_status_header(self):
    print(" | ".join(self._status_headers))

  def _print_status(self):
    reported_mem, actual_mem = self._get_mem_usage_values(reset=True)
    status_format = " | ".join(["%%%ss" % len(header) for header in self._status_headers])
    print(status_format % (
        self._num_queries_finished.value,
        self._num_queries_started.value - self._num_queries_finished.value,
        self._num_queries_exceeded_mem_limit.value,
        self._num_queries_timedout.value - self._num_queries_cancelled.value,
        self._num_queries_cancelled.value,
        self._num_other_errors.value,
        self._mem_mb_needed_for_next_query.value,
        self._mem_broker.total_mem_mb - self._mem_broker.available_mem_mb,
        "" if reported_mem == -1 else reported_mem,
        "" if actual_mem == -1 else actual_mem))

  def _update_from_query_report(self, report):
    LOG.debug("Updating runtime stats")
    increment(self._num_queries_finished)
    if report.mem_limit_exceeded:
      increment(self._num_queries_exceeded_mem_limit)
    if report.was_cancelled:
      increment(self._num_queries_cancelled)
    if report.timed_out:
      increment(self._num_queries_timedout)


class QueryTimeout(Exception):
  pass


class QueryType(object):
  COMPUTE_STATS, DELETE, INSERT, SELECT, UPDATE, UPSERT = range(6)


class Query(object):
  """Contains a SQL statement along with expected runtime information."""

  def __init__(self):
    self.name = None
    self.sql = None
    # In order to be able to make good estimates for DML queries in the binary search,
    # we need to bring the table to a good initial state before excuting the sql. Running
    # set_up_sql accomplishes this task.
    self.set_up_sql = None
    self.db_name = None
    self.result_hash = None
    self.required_mem_mb_with_spilling = None
    self.required_mem_mb_without_spilling = None
    self.solo_runtime_secs_with_spilling = None
    self.solo_runtime_secs_without_spilling = None
    # Query options to set before running the query.
    self.options = {}
    # Determines the order in which we will populate query runtime info. Queries with the
    # lowest population_order property will be handled first.
    self.population_order = 0
    # Type of query. Can have the following values: SELECT, COMPUTE_STATS, INSERT, UPDATE,
    # UPSERT, DELETE.
    self.query_type = QueryType.SELECT

  def __repr__(self):
    return dedent("""
        <Query
        Mem: %(required_mem_mb_with_spilling)s
        Mem no-spilling: %(required_mem_mb_without_spilling)s
        Solo Runtime: %(solo_runtime_secs_with_spilling)s
        Solo Runtime no-spilling: %(solo_runtime_secs_without_spilling)s
        DB: %(db_name)s
        Options: %(options)s
        Set up SQL: %(set_up_sql)s>
        SQL: %(sql)s>
        Population order: %(population_order)r>
        """.strip() % self.__dict__)


class QueryRunner(object):
  """Encapsulates functionality to run a query and provide a runtime report."""

  SPILLED_PATTERNS = [re.compile("ExecOption:.*Spilled"), re.compile("SpilledRuns: [^0]")]
  BATCH_SIZE = 1024

  def __init__(self):
    self.impalad = None
    self.impalad_conn = None
    self.use_kerberos = False
    self.result_hash_log_dir = gettempdir()
    self.check_if_mem_was_spilled = False
    self.common_query_options = {}

  def connect(self):
    self.impalad_conn = self.impalad.impala.connect(impalad=self.impalad)

  def disconnect(self):
    if self.impalad_conn:
      self.impalad_conn.close()
      self.impalad_conn = None

  def run_query(self, query, timeout_secs, mem_limit_mb, run_set_up=False):
    """Run a query and return an execution report. If 'run_set_up' is True, set up sql
    will be executed before the main query. This should be the case during the binary
    search phase of the stress test.
    """
    if not self.impalad_conn:
      raise Exception("connect() must first be called")

    timeout_unix_time = time() + timeout_secs
    report = QueryReport()
    try:
      with self.impalad_conn.cursor() as cursor:
        start_time = time()
        if query.db_name:
          LOG.debug("Using %s database", query.db_name)
          cursor.execute("USE %s" % query.db_name)
        if run_set_up and query.set_up_sql:
          LOG.debug("Running set up query:\n%s", self.set_up_sql)
          cursor.execute(query.set_up_sql)
        for query_option, value in self.common_query_options.iteritems():
          cursor.execute(
              "SET {query_option}={value}".format(query_option=query_option, value=value))
        for query_option, value in query.options.iteritems():
          cursor.execute(
              "SET {query_option}={value}".format(query_option=query_option, value=value))
        cursor.execute("SET ABORT_ON_ERROR=1")
        LOG.debug("Setting mem limit to %s MB", mem_limit_mb)
        cursor.execute("SET MEM_LIMIT=%sM" % mem_limit_mb)
        LOG.debug(
            "Running query with %s MB mem limit at %s with timeout secs %s:\n%s",
            mem_limit_mb, self.impalad.host_name, timeout_secs, query.sql)
        error = None
        try:
          cursor.execute_async(
              "/* Mem: %s MB. Coordinator: %s. */\n"
              % (mem_limit_mb, self.impalad.host_name) + query.sql)
          LOG.debug(
              "Query id is %s", op_handle_to_query_id(cursor._last_operation.handle if
                                                      cursor._last_operation else None))
          sleep_secs = 0.1
          secs_since_log = 0
          while cursor.is_executing():
            if time() > timeout_unix_time:
              self._cancel(cursor, report)
              return report
            if secs_since_log > 5:
              secs_since_log = 0
              LOG.debug("Waiting for query to execute")
            sleep(sleep_secs)
            secs_since_log += sleep_secs
          if query.query_type == QueryType.SELECT:
            try:
              report.result_hash = self._hash_result(cursor, timeout_unix_time, query)
            except QueryTimeout:
              self._cancel(cursor, report)
              return report
          else:
            # If query is in error state, this will raise an exception
            cursor._wait_to_finish()
        except Exception as error:
          LOG.debug(
              "Error running query with id %s: %s",
              op_handle_to_query_id(cursor._last_operation.handle if
                                    cursor._last_operation else None), error)
          self._check_for_mem_limit_exceeded(report, cursor, error)
        if report.non_mem_limit_error or report.mem_limit_exceeded:
          return report
        report.runtime_secs = time() - start_time
        if cursor.execution_failed() or self.check_if_mem_was_spilled:
          # Producing a query profile can be somewhat expensive. A v-tune profile of
          # impalad showed 10% of cpu time spent generating query profiles.
          report.profile = cursor.get_profile()
          report.mem_was_spilled = any([
              pattern.search(report.profile) is not None
              for pattern in QueryRunner.SPILLED_PATTERNS])
          report.mem_limit_exceeded = "Memory limit exceeded" in report.profile
    except Exception as error:
      # A mem limit error would have been caught above, no need to check for that here.
      report.non_mem_limit_error = error
    return report

  def _cancel(self, cursor, report):
    report.timed_out = True

    # Copy the operation handle in case another thread causes the handle to be reset.
    operation_handle = cursor._last_operation.handle if cursor._last_operation else None
    if not operation_handle:
      return

    query_id = op_handle_to_query_id(operation_handle)
    try:
      LOG.debug("Attempting cancellation of query with id %s", query_id)
      cursor.cancel_operation()
      LOG.debug("Sent cancellation request for query with id %s", query_id)
    except Exception as e:
      LOG.debug("Error cancelling query with id %s: %s", query_id, e)
      try:
        LOG.debug("Attempting to cancel query through the web server.")
        self.impalad.cancel_query(query_id)
      except Exception as e:
        LOG.debug("Error cancelling query %s through the web server: %s", query_id, e)

  def _check_for_mem_limit_exceeded(self, report, cursor, caught_exception):
    """To be called after a query failure to check for signs of failed due to a
    mem limit. The report will be updated accordingly.
    """
    if cursor._last_operation:
      try:
        report.profile = cursor.get_profile()
      except Exception as e:
        LOG.debug(
            "Error getting profile for query with id %s: %s",
            op_handle_to_query_id(cursor._last_operation.handle), e)
    caught_msg = str(caught_exception).lower().strip()

    # Exceeding a mem limit may result in the message "cancelled".
    # https://issues.cloudera.org/browse/IMPALA-2234
    if "memory limit exceeded" in caught_msg or \
       "repartitioning did not reduce the size of a spilled partition" in caught_msg or \
       caught_msg == "cancelled":
      report.mem_limit_exceeded = True
      return

    # If the mem limit is very low and abort_on_error is enabled, the message from
    # exceeding the mem_limit could be something like:
    #   Metadata states that in group hdfs://<node>:8020<path> there are <X> rows,
    #   but only <Y> rows were read.
    if (
        "metadata states that in group" in caught_msg and
        "rows were read" in caught_msg
    ):
      report.mem_limit_exceeded = True
      return

    LOG.debug(
        "Non-mem limit error for query with id %s: %s",
        op_handle_to_query_id(
            cursor._last_operation.handle if cursor._last_operation else None),
        caught_exception,
        exc_info=True)
    report.non_mem_limit_error = caught_exception

  def _hash_result(self, cursor, timeout_unix_time, query):
    """Returns a hash that is independent of row order. 'query' is only used for debug
    logging purposes (if the result is not as expected a log file will be left for
    investigations).
    """
    query_id = op_handle_to_query_id(cursor._last_operation.handle if
                                     cursor._last_operation else None)

    # A value of 1 indicates that the hash thread should continue to work.
    should_continue = Value("i", 1)

    def hash_result_impl():
      result_log = None
      try:
        file_name = query_id.replace(":", "_")
        if query.result_hash is None:
          file_name += "_initial"
        file_name += "_results.txt"
        result_log = open(os.path.join(self.result_hash_log_dir, file_name), "w")
        result_log.write(query.sql)
        result_log.write("\n")
        current_thread().result = 1
        while should_continue.value:
          LOG.debug(
              "Fetching result for query with id %s",
              op_handle_to_query_id(
                  cursor._last_operation.handle if cursor._last_operation else None))
          rows = cursor.fetchmany(self.BATCH_SIZE)
          if not rows:
            LOG.debug(
                "No more results for query with id %s",
                op_handle_to_query_id(
                    cursor._last_operation.handle if cursor._last_operation else None))
            return
          for row in rows:
            for idx, val in enumerate(row):
              if val is None:
                # The hash() of None can change from run to run since it's based on
                # a memory address. A chosen value will be used instead.
                val = 38463209
              elif isinstance(val, float):
                # Floats returned by Impala may not be deterministic, the ending
                # insignificant digits may differ. Only the first 6 digits will be used
                # after rounding.
                sval = "%f" % val
                dot_idx = sval.find(".")
                val = round(val, 6 - dot_idx)
              current_thread().result += (idx + 1) * hash(val)
              # Modulo the result to keep it "small" otherwise the math ops can be slow
              # since python does infinite precision math.
              current_thread().result %= maxint
              if result_log:
                result_log.write(str(val))
                result_log.write("\t")
                result_log.write(str(current_thread().result))
                result_log.write("\n")
      except Exception as e:
        current_thread().error = e
      finally:
        if result_log is not None:
          result_log.close()
          if (
              current_thread().error is not None and
              current_thread().result == query.result_hash
          ):
            os.remove(result_log.name)

    hash_thread = create_and_start_daemon_thread(
        hash_result_impl, "Fetch Results %s" % query_id)
    hash_thread.join(max(timeout_unix_time - time(), 0))
    if hash_thread.is_alive():
      should_continue.value = 0
      raise QueryTimeout()
    if hash_thread.error:
      raise hash_thread.error
    return hash_thread.result


def load_tpc_queries(workload, load_in_kudu=False):
  """Returns a list of TPC queries. 'workload' should either be 'tpch' or 'tpcds'.
  If 'load_in_kudu' is True, it loads only queries specified for the Kudu storage
  engine.
  """
  LOG.info("Loading %s queries", workload)
  queries = list()
  query_dir = os.path.join(
      os.path.dirname(__file__), "..", "..", "testdata", "workloads", workload, "queries")
  engine = 'kudu-' if load_in_kudu else ''
  file_name_pattern = re.compile(r"%s-%s(q\d+).test$" % (workload, engine))
  for query_file in os.listdir(query_dir):
    match = file_name_pattern.search(query_file)
    if not match:
      continue
    file_path = os.path.join(query_dir, query_file)
    file_queries = load_queries_from_test_file(file_path)
    if len(file_queries) != 1:
      raise Exception(
          "Expected exactly 1 query to be in file %s but got %s"
          % (file_path, len(file_queries)))
    query = file_queries[0]
    query.name = match.group(1)
    queries.append(query)
  return queries


def load_queries_from_test_file(file_path, db_name=None):
  LOG.debug("Loading queries from %s", file_path)
  test_cases = test_file_parser.parse_query_test_file(file_path)
  queries = list()
  for test_case in test_cases:
    query = Query()
    query.sql = test_file_parser.remove_comments(test_case["QUERY"])
    query.db_name = db_name
    queries.append(query)
  return queries


def load_random_queries_and_populate_runtime_info(
    query_generator, model_translator, tables, db_name, impala, use_kerberos, query_count,
    query_timeout_secs, result_hash_log_dir
):
  """Returns a list of random queries. Each query will also have its runtime info
  populated. The runtime info population also serves to validate the query.
  """
  LOG.info("Generating random queries")

  def generate_candidates():
    while True:
      query_model = query_generator.generate_statement(tables)
      sql = model_translator.write_query(query_model)
      query = Query()
      query.sql = sql
      query.db_name = db_name
      yield query
  return populate_runtime_info_for_random_queries(
      impala, use_kerberos, generate_candidates(), query_count, query_timeout_secs,
      result_hash_log_dir)


def populate_runtime_info_for_random_queries(
    impala, use_kerberos, candidate_queries,
    query_count, query_timeout_secs, result_hash_log_dir
):
  """Returns a list of random queries. Each query will also have its runtime info
  populated. The runtime info population also serves to validate the query.
  """
  start_time = datetime.now()
  queries = list()
  # TODO(IMPALA-4632): Consider running reset_databases() here if we want to extend DML
  #                    functionality to random stress queries as well.
  for query in candidate_queries:
    try:
      populate_runtime_info(
          query, impala, use_kerberos, result_hash_log_dir,
          timeout_secs=query_timeout_secs)
      queries.append(query)
    except Exception as e:
      # Ignore any non-fatal errors. These could be query timeouts or bad queries (
      # query generator bugs).
      if print_crash_info_if_exists(impala, start_time):
        raise e
      LOG.warn(
          "Error running query (the test will continue)\n%s\n%s",
          e, query.sql, exc_info=True)
    if len(queries) == query_count:
      break
  return queries


def populate_runtime_info(
    query, impala, use_kerberos, result_hash_log_dir,
    timeout_secs=maxint, samples=1, max_conflicting_samples=0
):
  """Runs the given query by itself repeatedly until the minimum memory is determined
  with and without spilling. Potentially all fields in the Query class (except
  'sql') will be populated by this method. 'required_mem_mb_without_spilling' and
  the corresponding runtime field may still be None if the query could not be run
  without spilling.

  'samples' and 'max_conflicting_samples' control the reliability of the collected
  information. The problem is that memory spilling or usage may differ (by a large
  amount) from run to run due to races during execution. The parameters provide a way
  to express "X out of Y runs must have resulted in the same outcome". Increasing the
  number of samples and decreasing the tolerance (max conflicts) increases confidence
  but also increases the time to collect the data.
  """
  LOG.info("Collecting runtime info for query %s: \n%s", query.name, query.sql)
  runner = QueryRunner()
  runner.check_if_mem_was_spilled = True
  runner.impalad = impala.impalads[0]
  runner.result_hash_log_dir = result_hash_log_dir
  runner.use_kerberos = use_kerberos
  runner.connect()
  limit_exceeded_mem = 0
  non_spill_mem = None
  spill_mem = None

  report = None
  mem_limit = None

  old_required_mem_mb_without_spilling = query.required_mem_mb_without_spilling
  old_required_mem_mb_with_spilling = query.required_mem_mb_with_spilling

  # TODO: This method is complicated enough now that breaking it out into a class may be
  # helpful to understand the structure.

  def update_runtime_info():
    required_mem = min(mem_limit, impala.min_impalad_mem_mb)
    if report.mem_was_spilled:
      if (
          query.required_mem_mb_with_spilling is None or
          required_mem < query.required_mem_mb_with_spilling
      ):
        query.required_mem_mb_with_spilling = required_mem
        query.solo_runtime_secs_with_spilling = report.runtime_secs
    elif (
        query.required_mem_mb_without_spilling is None or
        required_mem < query.required_mem_mb_without_spilling
    ):
      query.required_mem_mb_without_spilling = required_mem
      query.solo_runtime_secs_without_spilling = report.runtime_secs

  def get_report(desired_outcome=None):
    reports_by_outcome = defaultdict(list)
    leading_outcome = None
    for remaining_samples in xrange(samples - 1, -1, -1):
      report = runner.run_query(query, timeout_secs, mem_limit, run_set_up=True)
      if report.timed_out:
        raise QueryTimeout()
      if report.non_mem_limit_error:
        raise report.non_mem_limit_error
      LOG.debug("Spilled: %s" % report.mem_was_spilled)
      if not report.mem_limit_exceeded:
        if query.result_hash is None:
          query.result_hash = report.result_hash
        elif query.result_hash != report.result_hash:
          raise Exception(
              "Result hash mismatch; expected %s, got %s" %
              (query.result_hash, report.result_hash))

      if report.mem_limit_exceeded:
        outcome = "EXCEEDED"
      elif report.mem_was_spilled:
        outcome = "SPILLED"
      else:
        outcome = "NOT_SPILLED"
      reports_by_outcome[outcome].append(report)
      if not leading_outcome:
        leading_outcome = outcome
        continue
      if len(reports_by_outcome[outcome]) > len(reports_by_outcome[leading_outcome]):
        leading_outcome = outcome
      if len(reports_by_outcome[leading_outcome]) + max_conflicting_samples == samples:
        break
      if (
          len(reports_by_outcome[leading_outcome]) + remaining_samples <
          samples - max_conflicting_samples
      ):
        return
      if desired_outcome \
          and len(reports_by_outcome[desired_outcome]) + remaining_samples \
              < samples - max_conflicting_samples:
        return
    reports = reports_by_outcome[leading_outcome]
    reports.sort(key=lambda r: r.runtime_secs)
    return reports[len(reports) / 2]

  if not any((old_required_mem_mb_with_spilling, old_required_mem_mb_without_spilling)):
    mem_estimate = estimate_query_mem_mb_usage(query, runner)
    LOG.info("Finding a starting point for binary search")
    mem_limit = min(mem_estimate, impala.min_impalad_mem_mb) or impala.min_impalad_mem_mb
    while True:
      report = get_report()
      if not report or report.mem_limit_exceeded:
        if report and report.mem_limit_exceeded:
          limit_exceeded_mem = mem_limit
        if mem_limit == impala.min_impalad_mem_mb:
          LOG.warn(
              "Query couldn't be run even when using all available memory\n%s", query.sql)
          return
        mem_limit = min(2 * mem_limit, impala.min_impalad_mem_mb)
        continue
      update_runtime_info()
      if report.mem_was_spilled:
        spill_mem = mem_limit
      else:
        non_spill_mem = mem_limit
      break

  LOG.info("Finding minimum memory required to avoid spilling")
  lower_bound = max(limit_exceeded_mem, spill_mem)
  upper_bound = min(non_spill_mem or maxint, impala.min_impalad_mem_mb)
  while True:
    if old_required_mem_mb_without_spilling:
      mem_limit = old_required_mem_mb_without_spilling
      old_required_mem_mb_without_spilling = None
    else:
      mem_limit = (lower_bound + upper_bound) / 2
    should_break = mem_limit / float(upper_bound) > MEM_LIMIT_EQ_THRESHOLD_PC or \
        upper_bound - mem_limit < MEM_LIMIT_EQ_THRESHOLD_MB
    report = get_report(desired_outcome=("NOT_SPILLED" if spill_mem else None))
    if not report:
      lower_bound = mem_limit
    elif report.mem_limit_exceeded:
      lower_bound = mem_limit
      limit_exceeded_mem = mem_limit
    else:
      update_runtime_info()
      if report.mem_was_spilled:
        lower_bound = mem_limit
        spill_mem = min(spill_mem, mem_limit)
      else:
        upper_bound = mem_limit
        non_spill_mem = mem_limit
    if mem_limit == impala.min_impalad_mem_mb:
      break
    if should_break:
      if non_spill_mem:
        break
      lower_bound = upper_bound = impala.min_impalad_mem_mb
  # This value may be updated during the search for the absolute minimum.
  LOG.info(
      "Minimum memory to avoid spilling: %s MB" % query.required_mem_mb_without_spilling)

  LOG.info("Finding absolute minimum memory required")
  lower_bound = limit_exceeded_mem
  upper_bound = min(
      spill_mem or maxint, non_spill_mem or maxint, impala.min_impalad_mem_mb)
  while True:
    if old_required_mem_mb_with_spilling:
      mem_limit = old_required_mem_mb_with_spilling
      old_required_mem_mb_with_spilling = None
    else:
      mem_limit = (lower_bound + upper_bound) / 2
    should_break = mem_limit / float(upper_bound) > MEM_LIMIT_EQ_THRESHOLD_PC \
        or upper_bound - mem_limit < MEM_LIMIT_EQ_THRESHOLD_MB
    report = get_report(desired_outcome="SPILLED")
    if not report or report.mem_limit_exceeded:
      lower_bound = mem_limit
    else:
      update_runtime_info()
      upper_bound = mem_limit
    if should_break:
      if not query.required_mem_mb_with_spilling:
        query.required_mem_mb_with_spilling = query.required_mem_mb_without_spilling
        query.solo_runtime_secs_with_spilling = query.solo_runtime_secs_without_spilling
      break
  LOG.info("Minimum memory is %s MB" % query.required_mem_mb_with_spilling)
  if (
      query.required_mem_mb_without_spilling is not None and
      query.required_mem_mb_without_spilling is not None and
      query.required_mem_mb_without_spilling < query.required_mem_mb_with_spilling
  ):
    # Query execution is not deterministic and sometimes a query will run without spilling
    # at a lower mem limit than it did with spilling. In that case, just use the lower
    # value.
    LOG.info(
        "A lower memory limit to avoid spilling was found while searching for"
        " the absolute minimum memory.")
    query.required_mem_mb_with_spilling = query.required_mem_mb_without_spilling
    query.solo_runtime_secs_with_spilling = query.solo_runtime_secs_without_spilling
  LOG.debug("Query after populating runtime info: %s", query)


def estimate_query_mem_mb_usage(query, query_runner):
  """Runs an explain plan then extracts and returns the estimated memory needed to run
  the query.
  """
  with query_runner.impalad_conn.cursor() as cursor:
    LOG.debug("Using %s database", query.db_name)
    if query.db_name:
      cursor.execute('USE ' + query.db_name)
    if query.query_type == QueryType.COMPUTE_STATS:
      # Running "explain" on compute stats is not supported by Impala.
      return
    LOG.debug("Explaining query\n%s", query.sql)
    cursor.execute('EXPLAIN ' + query.sql)
    first_val = cursor.fetchone()[0]
    regex_result = MEM_ESTIMATE_PATTERN.search(first_val)
    if not regex_result:
      return
    mem_limit, units = regex_result.groups()
    return parse_mem_to_mb(mem_limit, units)


def save_runtime_info(path, query, impala):
  """Updates the file at 'path' with the given query information."""
  store = None
  if os.path.exists(path):
    with open(path) as file:
      store = json.load(file)
    _check_store_version(store)
  if not store:
    store = {
        "host_names": list(), "db_names": dict(), "version": RUNTIME_INFO_FILE_VERSION}
  with open(path, "w+") as file:
    store["host_names"] = sorted([i.host_name for i in impala.impalads])
    queries = store["db_names"].get(query.db_name, dict())
    query_by_options = queries.get(query.sql, dict())
    query_by_options[str(sorted(query.options.items()))] = query
    queries[query.sql] = query_by_options
    store["db_names"][query.db_name] = queries

    class JsonEncoder(json.JSONEncoder):
      def default(self, obj):
        data = dict(obj.__dict__)
        # Queries are stored by sql, so remove the duplicate data.
        if "sql" in data:
          del data["sql"]
        return data
    json.dump(
        store, file, cls=JsonEncoder, sort_keys=True, indent=2, separators=(',', ': '))


def load_runtime_info(path, impala=None):
  """Reads the query runtime information at 'path' and returns a
  dict<db_name, dict<sql, Query>>. Returns an empty dict if the hosts in the 'impala'
  instance do not match the data in 'path'.
  """
  queries_by_db_and_sql = defaultdict(lambda: defaultdict(dict))
  if not os.path.exists(path):
    return queries_by_db_and_sql
  with open(path) as file:
    store = json.load(file)
    _check_store_version(store)
    if (
        impala and
        store.get("host_names") != sorted([i.host_name for i in impala.impalads])
    ):
      return queries_by_db_and_sql
    for db_name, queries_by_sql in store["db_names"].iteritems():
      for sql, queries_by_options in queries_by_sql.iteritems():
        for options, json_query in queries_by_options.iteritems():
          query = Query()
          query.__dict__.update(json_query)
          query.sql = sql
          queries_by_db_and_sql[db_name][sql][options] = query
  return queries_by_db_and_sql


def _check_store_version(store):
  """Clears 'store' if the version is too old or raises an error if the version is too
  new.
  """
  if store["version"] < RUNTIME_INFO_FILE_VERSION:
    LOG.warn("Runtime file info version is old and will be ignored")
    store.clear()
  elif store["version"] > RUNTIME_INFO_FILE_VERSION:
    raise Exception(
        "Unexpected runtime file info version %s expected %s"
        % (store["version"], RUNTIME_INFO_FILE_VERSION))


def print_runtime_info_comparison(old_runtime_info, new_runtime_info):
  # TODO: Provide a way to call this from the CLI. This was hard coded to run from main()
  #       when it was used.
  print(",".join([
      "Database", "Query",
      "Old Mem MB w/Spilling",
      "New Mem MB w/Spilling",
      "Diff %",
      "Old Runtime w/Spilling",
      "New Runtime w/Spilling",
      "Diff %",
      "Old Mem MB wout/Spilling",
      "New Mem MB wout/Spilling",
      "Diff %",
      "Old Runtime wout/Spilling",
      "New Runtime wout/Spilling",
      "Diff %"]))
  for db_name, old_queries in old_runtime_info.iteritems():
    new_queries = new_runtime_info.get(db_name)
    if not new_queries:
      continue
    for sql, old_query in old_queries.iteritems():
      new_query = new_queries.get(sql)
      if not new_query:
        continue
      sys.stdout.write(old_query["db_name"])
      sys.stdout.write(",")
      sys.stdout.write(old_query["name"])
      sys.stdout.write(",")
      for attr in [
          "required_mem_mb_with_spilling", "solo_runtime_secs_with_spilling",
          "required_mem_mb_without_spilling", "solo_runtime_secs_without_spilling"
      ]:
        old_value = old_query[attr]
        sys.stdout.write(str(old_value))
        sys.stdout.write(",")
        new_value = new_query[attr]
        sys.stdout.write(str(new_value))
        sys.stdout.write(",")
        if old_value and new_value is not None:
          sys.stdout.write("%0.2f%%" % (100 * float(new_value - old_value) / old_value))
        else:
          sys.stdout.write("N/A")
        sys.stdout.write(",")
      print()


def generate_DML_queries(cursor, dml_mod_values):
  """Generate insert, upsert, update, delete DML statements.

  For each table in the database that cursor is connected to, create 4 DML queries
  (insert, upsert, update, delete) for each mod value in 'dml_mod_values'. This value
  controls which rows will be affected. The generated queries assume that for each table
  in the database, there exists a table with a '_original' suffix that is never modified.

  This function has some limitations:
  1. Only generates DML statements against Kudu tables, and ignores non-Kudu tables.
  2. Requires that the type of the first column of the primary key is an integer type.
  """
  LOG.info("Generating DML queries")
  tables = [cursor.describe_table(t) for t in cursor.list_table_names()
            if not t.endswith("_original")]
  result = []
  for table in tables:
    if not table.primary_keys:
      # Skip non-Kudu tables. If a table has no primary keys, then it cannot be a Kudu
      # table.
      LOG.debug("Skipping table '{0}' because it has no primary keys.".format(table.name))
      continue
    if len(table.primary_keys) > 1:
      # TODO(IMPALA-4665): Add support for tables with multiple primary keys.
      LOG.debug("Skipping table '{0}' because it has more than "
                "1 primary key column.".format(table.name))
      continue
    primary_key = table.primary_keys[0]
    if primary_key.exact_type not in (Int, TinyInt, SmallInt, BigInt):
      # We want to be able to apply the modulo operation on the primary key. If the
      # the first primary key column happens to not be an integer, we will skip
      # generating queries for this table
      LOG.debug("Skipping table '{0}' because the first column '{1}' in the "
                "primary key is not an integer.".format(table.name, primary_key.name))
      continue
    for mod_value in dml_mod_values:
      # Insert
      insert_query = Query()
      # Populate runtime info for Insert and Upsert queries before Update and Delete
      # queries because tables remain in original state after running the Insert and
      # Upsert queries. During the binary search in runtime info population for the
      # Insert query, we first delete some rows and then reinsert them, so the table
      # remains in the original state. For the delete, the order is reversed, so the table
      # is not in the original state after running the the delete (or update) query. This
      # is why population_order is smaller for Insert and Upsert queries.
      insert_query.population_order = 1
      insert_query.query_type = QueryType.INSERT
      insert_query.name = "insert_{0}".format(table.name)
      insert_query.db_name = cursor.db_name
      insert_query.sql = (
          "INSERT INTO TABLE {0} SELECT * FROM {0}_original "
          "WHERE {1} % {2} = 0").format(table.name, primary_key.name, mod_value)
      # Upsert
      upsert_query = Query()
      upsert_query.population_order = 1
      upsert_query.query_type = QueryType.UPSERT
      upsert_query.name = "upsert_{0}".format(table.name)
      upsert_query.db_name = cursor.db_name
      upsert_query.sql = (
          "UPSERT INTO TABLE {0} SELECT * "
          "FROM {0}_original WHERE {1} % {2} = 0").format(
              table.name, primary_key.name, mod_value)
      # Update
      update_query = Query()
      update_query.population_order = 2
      update_query.query_type = QueryType.UPDATE
      update_query.name = "update_{0}".format(table.name)
      update_query.db_name = cursor.db_name
      update_list = ', '.join(
          'a.{0} = b.{0}'.format(col.name)
          for col in table.cols if not col.is_primary_key)
      update_query.sql = (
          "UPDATE a SET {update_list} FROM {table_name} a JOIN {table_name}_original b "
          "ON a.{pk} = b.{pk} + 1 WHERE a.{pk} % {mod_value} = 0").format(
              table_name=table.name, pk=primary_key.name, mod_value=mod_value,
              update_list=update_list)
      # Delete
      delete_query = Query()
      delete_query.population_order = 2
      delete_query.query_type = QueryType.DELETE
      delete_query.name = "delete_{0}".format(table.name)
      delete_query.db_name = cursor.db_name
      delete_query.sql = ("DELETE FROM {0} WHERE {1} % {2} = 0").format(
          table.name, primary_key.name, mod_value)

      if table.name + "_original" in set(table.name for table in tables):
        insert_query.set_up_sql = "DELETE FROM {0} WHERE {1} % {2} = 0".format(
            table.name, primary_key.name, mod_value)
        upsert_query.set_up_sql = insert_query.set_up_sql
        update_query.set_up_sql = (
            "UPSERT INTO TABLE {0} SELECT * FROM {0}_original "
            "WHERE {1} % {2} = 0").format(table.name, primary_key.name, mod_value)
        delete_query.set_up_sql = update_query.set_up_sql

      result.append(insert_query)
      LOG.debug("Added insert query: {0}".format(insert_query))
      result.append(update_query)
      LOG.debug("Added update query: {0}".format(update_query))
      result.append(upsert_query)
      LOG.debug("Added upsert query: {0}".format(upsert_query))
      result.append(delete_query)
      LOG.debug("Added delete query: {0}".format(delete_query))
  assert len(result) > 0, "No DML queries were added."
  return result


def generate_compute_stats_queries(cursor):
  """For each table in the database that cursor is connected to, generate several compute
  stats queries. Each query will have a different value for the MT_DOP query option.
  """
  LOG.info("Generating Compute Stats queries")
  tables = [cursor.describe_table(t) for t in cursor.list_table_names()
            if not t.endswith("_original")]
  result = []
  mt_dop_values = [str(2**k) for k in range(5)]
  for table in tables:
    for mt_dop_value in mt_dop_values:
      compute_query = Query()
      compute_query.population_order = 1
      compute_query.query_type = QueryType.COMPUTE_STATS
      compute_query.sql = "COMPUTE STATS {0}".format(table.name)
      compute_query.options["MT_DOP"] = mt_dop_value
      compute_query.db_name = cursor.db_name
      compute_query.name = "compute_stats_{0}_mt_dop_{1}".format(
          table.name, compute_query.options["MT_DOP"])
      result.append(compute_query)
      LOG.debug("Added compute stats query: {0}".format(compute_query))
  return result


def prepare_database(cursor):
  """For each table in the database that cursor is connected to, create an identical copy
  with '_original' suffix. This function is idempotent.

  Note: At this time we only support Kudu tables with a simple hash partitioning based on
  the primary key. (SHOW CREATE TABLE would not work otherwise.)
  """
  tables = {t: cursor.describe_table(t) for t in cursor.list_table_names()}
  for table_name in tables:
    if not table_name.endswith("_original") and table_name + "_original" not in tables:
      LOG.debug("Creating original table: {0}".format(table_name))
      cursor.execute("SHOW CREATE TABLE " + table_name)
      create_sql = cursor.fetchone()[0]
      search_pattern = r"CREATE TABLE (\w*)\.(.*) \("
      replacement = "CREATE TABLE {tbl} (".format(tbl=table_name + "_original")
      create_original_sql = re.sub(
          search_pattern, replacement, create_sql, count=1)
      LOG.debug("Create original SQL:\n{0}".format(create_original_sql))
      cursor.execute(create_original_sql)
      cursor.execute("INSERT INTO {0}_original SELECT * FROM {0}".format(table_name))
      cursor.execute("COMPUTE STATS {0}".format(table_name + "_original"))


def reset_databases(cursor):
  """Reset the database to the initial state. This is done by overwriting tables which
  don't have the _original suffix with data from tables with the _original suffix.

  Note: At this time we only support Kudu tables with a simple hash partitioning based on
  the primary key. (SHOW CREATE TABLE would not work otherwise.)
  """
  LOG.info("Resetting {0} database".format(cursor.db_name))
  tables = {t: cursor.describe_table(t) for t in cursor.list_table_names()}
  for table_name in tables:
    if not table_name.endswith("_original"):
      if table_name + "_original" in tables:
        cursor.execute("SHOW CREATE TABLE " + table_name)
        create_table_command = cursor.fetchone()[0]
        cursor.execute("DROP TABLE {0}".format(table_name))
        cursor.execute(create_table_command)
        cursor.execute("INSERT INTO {0} SELECT * FROM {0}_original".format(table_name))
        cursor.execute("COMPUTE STATS {0}".format(table_name))
      else:
        LOG.debug("Table '{0}' cannot be reset because '{0}_original' does not"
                  " exist in '{1}' database.".format(table_name, cursor.db_name))


def populate_all_queries(queries, impala, args, runtime_info_path,
                         queries_with_runtime_info_by_db_sql_and_options):
  """Populate runtime info for all queries, ordered by the population_order property."""
  result = []
  queries_by_order = {}
  for query in queries:
    if query.population_order not in queries_by_order:
      queries_by_order[query.population_order] = []
    queries_by_order[query.population_order].append(query)
  for population_order in sorted(queries_by_order.keys()):
    for query in queries_by_order[population_order]:
      if (
          query.sql in
          queries_with_runtime_info_by_db_sql_and_options[query.db_name] and
          str(sorted(query.options.items())) in
          queries_with_runtime_info_by_db_sql_and_options[query.db_name][query.sql]
      ):
        LOG.debug("Reusing previous runtime data for query: " + query.sql)
        result.append(queries_with_runtime_info_by_db_sql_and_options[
            query.db_name][query.sql][str(sorted(query.options.items()))])
      else:
        populate_runtime_info(
            query, impala, args.use_kerberos, args.result_hash_log_dir,
            samples=args.samples, max_conflicting_samples=args.max_conflicting_samples)
        save_runtime_info(runtime_info_path, query, impala)
        result.append(query)
  return result


def main():
  from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
  from random import shuffle
  import tests.comparison.cli_options as cli_options

  parser = ArgumentParser(
      epilog=dedent("""
      Before running this script a CM cluster must be setup and any needed data
      such as TPC-H/DS must be loaded. The first time this script is run it will
      find memory limits and runtimes for each query and save the data to disk (since
      collecting the data is slow) at --runtime-info-path then run the stress test.
      Later runs will reuse the saved memory limits and timings. If the cluster changes
      significantly the memory limits should be re-measured (deleting the file at
      --runtime-info-path will cause re-measuring to happen).""").strip(),
      formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_cluster_options(parser)
  cli_options.add_kerberos_options(parser)
  parser.add_argument(
      "--runtime-info-path",
      default=os.path.join(gettempdir(), "{cm_host}_query_runtime_info.json"),
      help="The path to store query runtime info at. '{cm_host}' will be replaced with"
      " the actual host name from --cm-host.")
  parser.add_argument(
      "--samples", default=1, type=int,
      help='Used when collecting "runtime info" - the number of samples to collect when'
      ' testing a particular mem limit value.')
  parser.add_argument(
      "--max-conflicting-samples", default=0, type=int,
      help='Used when collecting "runtime info" - the number of samples outcomes that'
      ' can disagree when deciding to accept a particular mem limit. Ex, when trying to'
      ' determine the mem limit that avoids spilling with samples=5 and'
      ' max-conflicting-samples=1, then 4/5 queries must not spill at a particular mem'
      ' limit.')
  parser.add_argument(
      "--result-hash-log-dir", default=gettempdir(),
      help="If query results do not match, a log file will be left in this dir. The log"
      " file is also created during the first run when runtime info is collected for"
      " each query.")
  parser.add_argument(
      "--no-status", action="store_true", help="Do not print the status table.")
  parser.add_argument(
      "--cancel-current-queries", action="store_true",
      help="Cancel any queries running on the cluster before beginning.")
  parser.add_argument(
      "--filter-query-mem-ratio", type=float, default=0.333,
      help="Queries that require this ratio of total available memory will be filtered.")
  parser.add_argument(
      "--startup-queries-per-second", type=float, default=2.0,
      help="Adjust this depending on the cluster size and workload. This determines"
      " the minimum amount of time between successive query submissions when"
      " the workload is initially ramping up.")
  parser.add_argument(
      "--fail-upon-successive-errors", type=int, default=1,
      help="Continue running until N query errors are encountered in a row. Set"
      " this to a high number to only stop when something catastrophic happens. A"
      " value of 1 stops upon the first error.")
  parser.add_argument(
      "--mem-limit-padding-pct", type=int, default=25,
      help="Pad query mem limits found by solo execution with this percentage when"
      " running concurrently. After padding queries will not be expected to fail"
      " due to mem limit exceeded.")
  parser.add_argument(
      "--mem-limit-padding-abs", type=int, default=0,
      help="Pad query mem limits found by solo execution with this value (in megabytes)"
      " running concurrently. After padding queries will not be expected to fail"
      " due to mem limit exceeded. This is useful if we want to be able to add the same"
      " amount of memory to smaller queries as to the big ones.")
  parser.add_argument(
      "--timeout-multiplier", type=float, default=1.0,
      help="Query timeouts will be multiplied by this value.")
  parser.add_argument("--max-queries", type=int, default=100)
  parser.add_argument(
      "--reset-databases-before-binary-search", action="store_true",
      help="If True, databases will be reset to their original state before the binary"
      " search.")
  parser.add_argument(
      "--reset-databases-after-binary-search", action="store_true",
      help="If True, databases will be reset to their original state after the binary"
      " search and before starting the stress test. The primary intent of this option is"
      " to undo the changes made to the databases by the binary search. This option can"
      " also be used to reset the databases before running other (non stress) tests on"
      " the same data.")
  parser.add_argument(
      "--generate-dml-queries", action="store_true",
      help="If True, DML queries will be generated for Kudu databases.")
  parser.add_argument(
      "--dml-mod-values", nargs="+", type=int, default=[11],
      help="List of mod values to use for the DML queries. There will be 4 DML (delete,"
      " insert, update, upsert) queries generated per mod value per table. The smaller"
      " the value, the more rows the DML query would touch (the query should touch about"
      " 1/mod_value rows.)")
  parser.add_argument(
      "--generate-compute-stats-queries", action="store_true",
      help="If True, Compute Stats queries will be generated.")
  parser.add_argument(
      "--select-probability", type=float, default=0.5,
      help="Probability of choosing a select query (as opposed to a DML query).")
  parser.add_argument("--tpcds-db", help="If provided, TPC-DS queries will be used.")
  parser.add_argument("--tpch-db", help="If provided, TPC-H queries will be used.")
  parser.add_argument(
      "--tpch-nested-db", help="If provided, nested TPC-H queries will be used.")
  parser.add_argument(
      "--tpch-kudu-db", help="If provided, TPC-H queries for Kudu will be used.")
  parser.add_argument(
      "--tpcds-kudu-db", help="If provided, TPC-DS queries for Kudu will be used.")
  parser.add_argument(
      "--random-db", help="If provided, random queries will be used.")
  parser.add_argument(
      "--random-query-count", type=int, default=50,
      help="The number of random queries to generate.")
  parser.add_argument(
      "--random-query-timeout-seconds", type=int, default=(5 * 60),
      help="A random query that runs longer than this time when running alone will"
      " be discarded.")
  parser.add_argument(
      "--query-file-path", help="Use queries in the given file. The file"
      " format must be the same as standard test case format. Queries are expected to "
      " be randomly generated and will be validated before running in stress mode.")
  parser.add_argument(
      "--query-file-db",
      help="The name of the database to use with the queries from --query-file-path.")
  parser.add_argument("--mem-overcommit-pct", type=float, default=0)
  parser.add_argument(
      "--mem-spill-probability", type=float, default=0.33, dest="spill_probability",
      help="The probability that a mem limit will be set low enough to induce spilling.")
  parser.add_argument(
      "--mem-leak-check-interval-mins", type=int, default=None,
      help="Periodically stop query execution and check that memory levels have reset.")
  parser.add_argument(
      "--cancel-probability", type=float, default=0.1,
      help="The probability a query will be cancelled.")
  parser.add_argument(
      "--nlj-filter", choices=("in", "out", None),
      help="'in' means only nested-loop queries will be used, 'out' means no NLJ queries"
      " will be used. The default is to not filter either way.")
  parser.add_argument(
      "--common-query-options", default=None, nargs="*",
      help="Space-delimited string of query options and values. This is a freeform "
      "string with little regard to whether you've spelled the query options correctly "
      "or set valid values. Example: --common-query-options "
      "DISABLE_CODEGEN=true RUNTIME_FILTER_MODE=1")
  args = parser.parse_args()

  cli_options.configure_logging(
      args.log_level, debug_log_file=args.debug_log_file, log_thread_name=True,
      log_process_id=True)
  LOG.debug("CLI args: %s" % (args, ))

  if (
      not args.tpcds_db and not args.tpch_db and not args.random_db and not
      args.tpch_nested_db and not args.tpch_kudu_db and not
      args.tpcds_kudu_db and not args.query_file_path
  ):
    raise Exception(
        "At least one of --tpcds-db, --tpch-db, --tpch-kudu-db,"
        "--tpcds-kudu-db, --tpch-nested-db, --random-db, --query-file-path is required")

  # The stress test sets these, so callers cannot override them.
  IGNORE_QUERY_OPTIONS = frozenset([
      'ABORT_ON_ERROR',
      'MEM_LIMIT',
  ])

  common_query_options = {}
  if args.common_query_options is not None:
    for query_option_and_value in args.common_query_options:
      try:
        query_option, value = query_option_and_value.split('=')
      except ValueError:
        LOG.error(
            "Could not parse --common-query-options: '{common_query_options}'".format(
                common_query_options=args.common_query_options))
        exit(1)
      query_option = query_option.upper()
      if query_option in common_query_options:
        LOG.error(
            "Query option '{query_option}' already defined in --common-query-options: "
            "'{common_query_options}'".format(
                query_option=query_option,
                common_query_options=args.common_query_options))
        exit(1)
      elif query_option in IGNORE_QUERY_OPTIONS:
        LOG.warn(
            "Ignoring '{query_option}' in common query options: '{opt}': "
            "The stress test algorithm needs control of this option.".format(
                query_option=query_option, opt=args.common_query_options))
      else:
        common_query_options[query_option] = value
        LOG.debug("Common query option '{query_option}' set to '{value}'".format(
            query_option=query_option, value=value))

  cluster = cli_options.create_cluster(args)
  cluster.is_kerberized = args.use_kerberos
  impala = cluster.impala
  if impala.find_stopped_impalads():
    impala.restart()
  impala.find_and_set_path_to_running_impalad_binary()
  if args.cancel_current_queries and impala.queries_are_running():
    impala.cancel_queries()
    sleep(10)
  if impala.queries_are_running():
    raise Exception("Queries are currently running on the cluster")
  impala.min_impalad_mem_mb = min(impala.find_impalad_mem_mb_limit())

  runtime_info_path = args.runtime_info_path
  if "{cm_host}" in runtime_info_path:
    runtime_info_path = runtime_info_path.format(cm_host=args.cm_host)
  queries_with_runtime_info_by_db_sql_and_options = load_runtime_info(
      runtime_info_path, impala)

  # Start loading the test queries.
  queries = list()

  # If random queries were requested, those will be handled later. Unlike random queries,
  # the TPC queries are expected to always complete successfully.
  if args.tpcds_db:
    tpcds_queries = load_tpc_queries("tpcds")
    for query in tpcds_queries:
      query.db_name = args.tpcds_db
    queries.extend(tpcds_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpcds_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_db:
    tpch_queries = load_tpc_queries("tpch")
    for query in tpch_queries:
      query.db_name = args.tpch_db
    queries.extend(tpch_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpch_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_nested_db:
    tpch_nested_queries = load_tpc_queries("tpch_nested")
    for query in tpch_nested_queries:
      query.db_name = args.tpch_nested_db
    queries.extend(tpch_nested_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpch_nested_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_kudu_db:
    tpch_kudu_queries = load_tpc_queries("tpch", load_in_kudu=True)
    for query in tpch_kudu_queries:
      query.db_name = args.tpch_kudu_db
    queries.extend(tpch_kudu_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpch_kudu_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
    if args.generate_dml_queries:
      with impala.cursor(db_name=args.tpch_kudu_db) as cursor:
        prepare_database(cursor)
        queries.extend(generate_DML_queries(cursor, args.dml_mod_values))
  if args.tpcds_kudu_db:
    tpcds_kudu_queries = load_tpc_queries("tpcds", load_in_kudu=True)
    for query in tpcds_kudu_queries:
      query.db_name = args.tpcds_kudu_db
    queries.extend(tpcds_kudu_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpcds_kudu_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
    if args.generate_dml_queries:
      with impala.cursor(db_name=args.tpcds_kudu_db) as cursor:
        prepare_database(cursor)
        queries.extend(generate_DML_queries(cursor, args.dml_mod_values))

  if args.reset_databases_before_binary_search:
    for database in set(query.db_name for query in queries):
      with impala.cursor(db_name=database) as cursor:
        reset_databases(cursor)

  queries = populate_all_queries(queries, impala, args, runtime_info_path,
                                 queries_with_runtime_info_by_db_sql_and_options)

  # A particular random query may either fail (due to a generator or Impala bug) or
  # take a really long time to complete. So the queries needs to be validated. Since the
  # runtime info also needs to be collected, that will serve as validation.
  if args.random_db:
    query_generator = QueryGenerator(DefaultProfile())
    with impala.cursor(db_name=args.random_db) as cursor:
      tables = [cursor.describe_table(t) for t in cursor.list_table_names()]
    queries.extend(load_random_queries_and_populate_runtime_info(
        query_generator, SqlWriter.create(), tables, args.random_db, impala,
        args.use_kerberos, args.random_query_count, args.random_query_timeout_seconds,
        args.result_hash_log_dir))

  if args.query_file_path:
    file_queries = load_queries_from_test_file(
        args.query_file_path, db_name=args.query_file_db)
    shuffle(file_queries)
    queries.extend(populate_runtime_info_for_random_queries(
        impala, args.use_kerberos, file_queries, args.random_query_count,
        args.random_query_timeout_seconds, args.result_hash_log_dir))

  # Apply tweaks to the query's runtime info as requested by CLI options.
  for idx in xrange(len(queries) - 1, -1, -1):
    query = queries[idx]
    if query.required_mem_mb_with_spilling:
      query.required_mem_mb_with_spilling += int(
          query.required_mem_mb_with_spilling * args.mem_limit_padding_pct / 100.0) + \
          args.mem_limit_padding_abs
    if query.required_mem_mb_without_spilling:
      query.required_mem_mb_without_spilling += int(
          query.required_mem_mb_without_spilling * args.mem_limit_padding_pct / 100.0) + \
          args.mem_limit_padding_abs
    if query.solo_runtime_secs_with_spilling:
      query.solo_runtime_secs_with_spilling *= args.timeout_multiplier
    if query.solo_runtime_secs_without_spilling:
      query.solo_runtime_secs_without_spilling *= args.timeout_multiplier

    # Remove any queries that would use "too many" resources. This way a larger number
    # of queries will run concurrently.
    if query.required_mem_mb_with_spilling is None \
        or query.required_mem_mb_with_spilling / impala.min_impalad_mem_mb \
            > args.filter_query_mem_ratio:
      LOG.debug("Filtered query due to mem ratio option: " + query.sql)
      del queries[idx]

  # Remove queries that have a nested loop join in the plan.
  if args.nlj_filter:
    with impala.cursor(db_name=args.random_db) as cursor:
      for idx in xrange(len(queries) - 1, -1, -1):
        query = queries[idx]
        if query.db_name:
          cursor.execute("USE %s" % query.db_name)
        cursor.execute("EXPLAIN " + query.sql)
        for row in cursor.fetchall():
          found_nlj = False
          for col in row:
            col = str(col).lower()
            if "nested loop join" in col:
              found_nlj = True
              if args.nlj_filter == "out":
                del queries[idx]
              break
          if found_nlj:
            break
        else:
          if args.nlj_filter == "in":
            del queries[idx]

  if len(queries) == 0:
    raise Exception("All queries were filtered")
  print("Using %s queries" % len(queries))

  # After the binary search phase finishes, it may be a good idea to reset the database
  # again to start the stress test from a clean state.
  if args.reset_databases_after_binary_search:
    for database in set(query.db_name for query in queries):
      with impala.cursor(db_name=database) as cursor:
        reset_databases(cursor)

  LOG.info("Number of queries in the list: {0}".format(len(queries)))

  stress_runner = StressRunner()
  stress_runner.result_hash_log_dir = args.result_hash_log_dir
  stress_runner.startup_queries_per_sec = args.startup_queries_per_second
  stress_runner.num_successive_errors_needed_to_abort = args.fail_upon_successive_errors
  stress_runner.use_kerberos = args.use_kerberos
  stress_runner.cancel_probability = args.cancel_probability
  stress_runner.spill_probability = args.spill_probability
  stress_runner.leak_check_interval_mins = args.mem_leak_check_interval_mins
  stress_runner.common_query_options = common_query_options
  stress_runner.run_queries(
      queries, impala, args.max_queries, args.mem_overcommit_pct,
      should_print_status=not args.no_status,
      verify_results=not args.generate_dml_queries,
      select_probability=args.select_probability)


if __name__ == "__main__":
  main()
