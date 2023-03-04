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

# This module is used to stress test Impala by running queries concurrently.
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
#     disk or queries failing with out-of-memory.
#  5) Start submitting queries. There are two modes for throttling the number of
#     concurrent queries, depending on --test-admission-control.
#      a) test-admission-control=false: Submit queries until all available memory (as
#         determined by items 3 and 4) is used. Before running the query a query mem
#         limit is set between 2a and 2b. (There is a runtime option to increase the
#         likelihood that a query will be given the full 2a limit to avoid spilling.)
#      b) test-admission-control=true: Submit enough queries to achieve the desired
#         level of overcommit, but expect that Impala's admission control will throttle
#         queries. In this mode mem_limit is not set per query.
#  6) Randomly cancel queries to test cancellation. There is a runtime option to control
#     the likelihood that a query will be randomly canceled.
#  7) If a query errored, verify that the error is expected. Errors are expected in the
#     following cases:
#      a) Memory-based admission control is not being tested (i.e.
#        --test-admission-control=false), the error is an out-of-memory error and memory
#        on the cluster is overcommitted.
#      b) The error is an admission control rejection or timeout.
#  8) Verify the result set hash of successful queries if there are no DML queries in the
#     current run.

from __future__ import absolute_import, division, print_function

from builtins import range
import logging
import os
import re
import signal
import sys
import threading
from queue import Empty   # Must be before Queue below
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace, SUPPRESS
from collections import defaultdict
from copy import copy
from datetime import datetime
from multiprocessing import Lock, Process, Queue, Value
from random import choice, random, randrange, shuffle
from sys import exit, maxsize
from tempfile import gettempdir
from textwrap import dedent
from threading import current_thread
from time import sleep, time

import tests.comparison.cli_options as cli_options
from tests.comparison.cluster import Timeout
from tests.comparison.db_types import Int, TinyInt, SmallInt, BigInt
from tests.stress.mem_broker import MemBroker
from tests.stress.runtime_info import save_runtime_info, load_runtime_info
from tests.stress.queries import (QueryType, generate_compute_stats_queries,
    generate_DML_queries, generate_random_queries, load_tpc_queries,
    load_queries_from_test_file, estimate_query_mem_mb_usage)
from tests.stress.query_runner import (QueryRunner, QueryTimeout,
    NUM_QUERIES_DEQUEUED, NUM_QUERIES_SUBMITTED, NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED,
    NUM_QUERIES_FINISHED, NUM_QUERIES_EXCEEDED_MEM_LIMIT, NUM_QUERIES_AC_REJECTED,
    NUM_QUERIES_AC_TIMEDOUT, NUM_QUERIES_CANCELLED, NUM_RESULT_MISMATCHES,
    NUM_OTHER_ERRORS, RESULT_HASHES_DIR, CancelMechanism)
from tests.stress.util import create_and_start_daemon_thread, increment, print_stacks
from tests.util.parse_util import (
    EXPECTED_TPCDS_QUERIES_COUNT, EXPECTED_TPCH_NESTED_QUERIES_COUNT,
    EXPECTED_TPCH_STRESS_QUERIES_COUNT)

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

PROFILES_DIR = "profiles"


class StressArgConverter(object):
  def __init__(self, args):
    """
    Convert arguments as returned from from argparse parse_args() into internal forms.

    The purpose of this object is to do any conversions needed from the type given by
    parge_args() into internal forms. For example, if a commandline option takes in a
    complicated string that needs to be converted into a list or dictionary, this is the
    place to do it. Access works the same as on the object returned by parse_args(),
    i.e., object.option_attribute.

    In most cases, simple arguments needn't be converted, because argparse handles the
    type conversion already, and in most cases, type conversion (e.g., "8" <str> to 8
    <int>) is all that's needed. If a property getter below doesn't exist, it means the
    argument value is just passed along unconverted.

    Params:
      args: argparse.Namespace object (from argparse.ArgumentParser().parse_args())
    """
    assert isinstance(args, Namespace), "expected Namespace, got " + str(type(args))
    self._args = args
    self._common_query_options = None

  def __getattr__(self, attr):
    # This "proxies through" all the attributes from the Namespace object that are not
    # defined in this object via property getters below.
    return getattr(self._args, attr)

  @property
  def common_query_options(self):
    # Memoize this, as the integrity checking of --common-query-options need only
    # happen once.
    if self._common_query_options is not None:
      return self._common_query_options
    # The stress test sets these, so callers cannot override them.
    IGNORE_QUERY_OPTIONS = frozenset([
        'ABORT_ON_ERROR',
        'MEM_LIMIT',
    ])
    common_query_options = {}
    if self._args.common_query_options is not None:
      for query_option_and_value in self._args.common_query_options:
        try:
          query_option, value = query_option_and_value.split('=')
        except ValueError:
          LOG.error(
              "Could not parse --common-query-options: '{common_query_options}'".format(
                  common_query_options=self._args.common_query_options))
          exit(1)
        query_option = query_option.upper()
        if query_option in common_query_options:
          LOG.error(
              "Query option '{query_option}' already defined in --common-query-options: "
              "'{common_query_options}'".format(
                  query_option=query_option,
                  common_query_options=self._args.common_query_options))
          exit(1)
        elif query_option in IGNORE_QUERY_OPTIONS:
          LOG.warn(
              "Ignoring '{query_option}' in common query options: '{opt}': "
              "The stress test algorithm needs control of this option.".format(
                  query_option=query_option, opt=self._args.common_query_options))
        else:
          common_query_options[query_option] = value
          LOG.debug("Common query option '{query_option}' set to '{value}'".format(
              query_option=query_option, value=value))
    self._common_query_options = common_query_options
    return self._common_query_options

  @property
  def runtime_info_path(self):
    runtime_info_path = self._args.runtime_info_path
    if "{cm_host}" in runtime_info_path:
      runtime_info_path = runtime_info_path.format(cm_host=self._args.cm_host)
    return runtime_info_path


# To help debug hangs, the stacks of all threads can be printed by sending signal USR1
# to each process.
signal.signal(signal.SIGUSR1, print_stacks)


def print_crash_info_if_exists(impala, start_time):
  """If any impalads are found not running, they will assumed to have crashed and an
  error message will be printed to stderr for each stopped impalad. Returns a value
  that evaluates to True if any impalads are stopped.
  """
  max_attempts = 5
  for remaining_attempts in range(max_attempts - 1, -1, -1):
    try:
      crashed_impalads = impala.find_crashed_impalads(start_time)
      break
    except Timeout as e:
      LOG.info(
          "Timeout checking if impalads crashed: %s."
          % e + (" Will retry." if remaining_attempts else ""))
      if not remaining_attempts:
        LOG.error(
            "Aborting after %s failed attempts to check if impalads crashed",
            max_attempts)
        raise e
  for message in crashed_impalads.values():
    print(message, file=sys.stderr)
  return crashed_impalads


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
    self.test_admission_control = False
    self._mem_broker = None
    self._verify_results = True
    self._select_probability = None

    # Synchronized blocking work queue for producer/consumers.
    self._query_queue = Queue(self.WORK_QUEUE_CAPACITY)

    # The Value class provides cross-process shared memory.
    self._mem_mb_needed_for_next_query = Value("i", 0)

    # This lock provides a way to stop new queries from running. This lock must be
    # acquired before writing to the NUM_QUERIES_SUBMITTED metric for the query_runner,
    # which is incremented before every query submission.Reading NUM_QUERIES_SUBMITTED is
    # allowed without taking this lock.
    self._submit_query_lock = Lock()

    self.leak_check_interval_mins = None
    self._next_leak_check_unix_time = Value("i", 0)
    self._max_mem_mb_reported_usage = Value("i", -1)   # -1 => Unknown
    self._max_mem_mb_usage = Value("i", -1)   # -1 => Unknown

    self.cancel_probability = 0
    self.spill_probability = 0

    self.startup_queries_per_sec = 1.0
    self.num_successive_errors_needed_to_abort = 1
    self._num_successive_errors = Value("i", 0)
    self.results_dir = gettempdir()

    self._status_headers = [
        "Done", "Active", "Executing", "Mem Lmt Ex", "AC Reject", "AC Timeout",
        "Cancel", "Err", "Incorrect", "Next Qry Mem Lmt",
        "Tot Qry Mem Lmt", "Tracked Mem", "RSS Mem"]

    self._num_queries_to_run = None
    self._query_producer_thread = None

    # This lock is used to synchronize access to the '_query_runners' list and also to all
    # the '_past_runners*' members.
    self._query_runners_lock = Lock()
    self._query_runners = []

    # These are the cumulative values of all the queries that have started/finished/-
    # dequeued, etc. on runners that have already died. Every time we notice that a query
    # runner has died, we update these values.
    self._past_runner_metrics = defaultdict(lambda: Value("i", 0))

    self._query_consumer_thread = None
    self._mem_polling_thread = None

  def _record_runner_metrics_before_evict(self, query_runner):
    """ Before removing 'query_runner' from the self._query_runners list, record its
        metrics. Must only be called if 'query_runner' is to be removed from the list.
        MUST hold '_query_runners_lock' before calling.
    """
    for key, val in query_runner.get_metric_vals():
      self._past_runner_metrics[key].value += val

  def _calc_total_runner_metrics(self):
    """ Calculate the total of metrics across past and active query runners. """
    totals = defaultdict(lambda: 0)
    with self._query_runners_lock:
      for key in self._past_runner_metrics:
        totals[key] = self._past_runner_metrics[key].value
      for query_runner in self._query_runners:
        for key, val in query_runner.get_metric_vals():
          totals[key] += val
    return totals

  def _calc_total_runner_metric(self, key):
    """ Calculate the total of metric 'key' across past and active query runners. """
    with self._query_runners_lock:
      return self._calc_total_runner_metric_no_lock(key)

  def _calc_total_runner_metric_no_lock(self, key):
    """ TODO: Get rid of this function after reformatting how we obtain query indices.
        _query_runners_lock MUST be taken before calling this function.
    """
    total = self._past_runner_metrics[key].value
    for runner in self._query_runners:
      total += runner.get_metric_val(key)
    return total

  def _total_num_queries_submitted(self):
    return self._calc_total_runner_metric(NUM_QUERIES_SUBMITTED)

  def _total_num_queries_active(self):
    """The number of queries that are currently active (i.e. submitted to a query runner
    and haven't yet completed)."""
    metrics = self._calc_total_runner_metrics()
    num_running = metrics[NUM_QUERIES_SUBMITTED] - metrics[NUM_QUERIES_FINISHED]
    assert num_running >= 0, "The number of running queries is negative"
    return num_running

  def _num_runners_remaining(self):
    return len(self._query_runners)

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
    self.start_time = datetime.now()

    self._mem_broker = MemBroker(
        impala.min_impalad_mem_mb,
        int(impala.min_impalad_mem_mb * mem_overcommit_pct / 100))

    self._verify_results = verify_results
    self._select_probability = select_probability

    # Print the status to show the state before starting.
    if should_print_status:
      self._print_status(print_header=True)

    self._num_queries_to_run = num_queries_to_run
    self._start_polling_mem_usage(impala)
    self._start_producing_queries(queries)
    self._start_consuming_queries(impala)

    # Wait for everything to finish.
    self._wait_for_test_to_finish(impala, should_print_status)

    # And print the final state.
    if should_print_status:
      self._print_status()

    self._check_for_test_failure()
    self.print_duration()

  def _start_producing_queries(self, queries):
    def enqueue_queries():
      # Generate a dict(query type -> list of queries).
      queries_by_type = {}
      for query in queries:
        if query.query_type not in queries_by_type:
          queries_by_type[query.query_type] = []
        queries_by_type[query.query_type].append(query)
      try:
        for _ in range(self._num_queries_to_run):
          # First randomly determine a query type, then choose a random query of that
          # type.
          if (
              QueryType.SELECT in queries_by_type
              and (len(list(queries_by_type.keys())) == 1
                   or random() < self._select_probability)
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
      LOG.info("Producing thread completed job. Exiting...")
    self._query_producer_thread = create_and_start_daemon_thread(
        enqueue_queries, "Query Producer")

  def _start_consuming_queries(self, impala):
    def start_additional_runners_if_needed():
      try:
        while self._total_num_queries_submitted() < self._num_queries_to_run:
          # TODO: sleeping for the below amount leads to slower submission than the goal,
          # because it does not factor in the time spent by this thread outside of the
          # sleep() call.
          sleep(1.0 / self.startup_queries_per_sec)
          # Remember num dequeued/started are cumulative.
          with self._submit_query_lock:
            metrics = self._calc_total_runner_metrics()
            num_dequeued = metrics[NUM_QUERIES_DEQUEUED]
            num_submitted = metrics[NUM_QUERIES_SUBMITTED]
            LOG.debug("Submitted {0} queries. Dequeued {1} queries".format(
                num_submitted, num_dequeued))
            if num_dequeued != num_submitted:
              # Assume dequeued queries are stuck waiting for cluster resources so there
              # is no point in starting an additional runner.
              continue
          num_coordinators = len(impala.impalads)
          if self.max_coordinators > 0:
            num_coordinators = min(num_coordinators, self.max_coordinators)
          impalad = impala.impalads[len(self._query_runners) % num_coordinators]

          query_runner = QueryRunner(impalad=impalad, results_dir=self.results_dir,
              use_kerberos=self.use_kerberos,
              common_query_options=self.common_query_options,
              test_admission_control=self.test_admission_control)
          query_runner.proc = \
              Process(target=self._start_single_runner, args=(query_runner, ))
          query_runner.proc.daemon = True
          with self._query_runners_lock:
            self._query_runners.append(query_runner)
          query_runner.proc.start()

        LOG.info("Consuming thread completed job. Exiting...")
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
      query_submission_is_locked = False

      # Query submission will be unlocked after a memory report has been collected twice
      # while no queries were running.
      ready_to_unlock = None
      try:
        while self._total_num_queries_submitted() < self._num_queries_to_run:
          if ready_to_unlock:
            assert query_submission_is_locked, "Query submission not yet locked"
            assert not self._total_num_queries_active(), "Queries are still running"
            LOG.debug("Resuming query submission")
            self._next_leak_check_unix_time.value = int(
                time() + 60 * self.leak_check_interval_mins)
            self._submit_query_lock.release()
            query_submission_is_locked = False
            ready_to_unlock = None

          if (
              not query_submission_is_locked and
              self.leak_check_interval_mins and
              time() > self._next_leak_check_unix_time.value
          ):
            assert self._total_num_queries_active() <= self._num_runners_remaining(), \
                "Each running query should belong to a runner"
            LOG.debug("Stopping query submission")
            self._submit_query_lock.acquire()
            query_submission_is_locked = True

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

          if query_submission_is_locked and not self._total_num_queries_active():
            if ready_to_unlock is None:
              ready_to_unlock = False
            else:
              ready_to_unlock = True
      except Exception:
        LOG.debug("Error collecting impalad mem usage", exc_info=True)
        if query_submission_is_locked:
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

  def _start_single_runner(self, query_runner):
    """Consumer function to take a query of the queue and run it. This is intended to
    run in a separate process so validating the result set can use a full CPU.
    """
    LOG.debug("New query runner started")

    # The query runner should already be set up. We just need to connect() before using
    # the runner.
    query_runner.connect()

    while not self._query_queue.empty():
      try:
        query = self._query_queue.get(True, 1)
      except Empty:
        continue
      except EOFError:
        LOG.debug("Query running aborting due to closed query queue")
        break
      LOG.debug("Getting query_idx")
      with self._query_runners_lock:
        query_idx = self._calc_total_runner_metric_no_lock(NUM_QUERIES_DEQUEUED)
        query_runner.increment_metric(NUM_QUERIES_DEQUEUED)
        LOG.debug("Query_idx: {0} | PID: {1}".format(query_idx, query_runner.proc.pid))

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
      while query_idx > self._total_num_queries_submitted():
        sleep(0.1)

      self._mem_mb_needed_for_next_query.value = mem_limit

      LOG.debug("Requesting memory reservation")
      with self._mem_broker.reserve_mem_mb(mem_limit) as reservation_id:
        LOG.debug("Received memory reservation")
        with self._submit_query_lock:
          query_runner.increment_metric(NUM_QUERIES_SUBMITTED)

        cancel_mech = None
        if self.cancel_probability > random():
          # Exercise both timeout mechanisms.
          if random() > 0.5:
            cancel_mech = CancelMechanism.VIA_CLIENT
          else:
            cancel_mech = CancelMechanism.VIA_OPTION
          timeout = randrange(1, max(int(solo_runtime), 2))
        else:
          # Let the query run as long as necessary - it is nearly impossible to pick a
          # good value that won't have false positives under load - see IMPALA-8222.
          timeout = maxsize
        report = query_runner.run_query(query, mem_limit, timeout_secs=timeout,
            cancel_mech=cancel_mech)
        LOG.debug("Got execution report for query")
        if report.timed_out and cancel_mech:
          report.was_cancelled = True
        query_runner.update_from_query_report(report)
        if report.other_error:
          error_msg = str(report.other_error)
          # There is a possible race during cancellation. If a fetch request fails (for
          # example due to hitting a mem limit), just before the cancellation request, the
          # server may have already unregistered the query as part of the fetch failure.
          # In that case the server gives an error response saying the handle is invalid.
          if "Invalid or unknown query handle" in error_msg and report.timed_out:
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
          query_runner.increment_metric(NUM_OTHER_ERRORS)
          self._write_query_profile(report, PROFILES_DIR, prefix='error')
          raise Exception("Query {query} ID {id} failed: {mesg}".format(
              query=query.logical_query_id,
              id=report.query_id,
              mesg=error_msg))
        if (
            report.not_enough_memory and (self.test_admission_control or
            not self._mem_broker.was_overcommitted(reservation_id))
        ):
          increment(self._num_successive_errors)
          self._write_query_profile(
              report, PROFILES_DIR, prefix='unexpected_mem_exceeded')
          raise Exception("Unexpected mem limit exceeded; mem was not overcommitted. "
                          "Query ID: {0}".format(report.query_id))
        if (
            not report.timed_out and not report.has_query_error() and
            (self._verify_results and report.result_hash != query.result_hash)
        ):
          increment(self._num_successive_errors)
          query_runner.increment_metric(NUM_RESULT_MISMATCHES)
          self._write_query_profile(report, PROFILES_DIR, prefix='incorrect_results')
          raise Exception(dedent("""\
                                 Result hash mismatch; expected {expected}, got {actual}
                                 Query ID: {id}
                                 Query: {query}""".format(expected=query.result_hash,
                                                          actual=report.result_hash,
                                                          id=report.query_id,
                                                          query=query.logical_query_id)))
        if report.timed_out and not cancel_mech:
          self._write_query_profile(report, PROFILES_DIR, prefix='timed_out')
          raise Exception(
              "Query {query} unexpectedly timed out. Query ID: {id}".format(
                  query=query.logical_query_id, id=report.query_id))
        self._num_successive_errors.value = 0
    LOG.debug("Query runner completed...")

  def _print_status_header(self):
    print(" | ".join(self._status_headers))

  def _print_status(self, print_header=False):
    if print_header:
      self._print_status_header()

    metrics = self._calc_total_runner_metrics()
    reported_mem, actual_mem = self._get_mem_usage_values(reset=True)
    status_format = " | ".join(["%%%ss" % len(header) for header in self._status_headers])
    print(status_format % (
        # Done
        metrics[NUM_QUERIES_FINISHED],
        # Active
        metrics[NUM_QUERIES_SUBMITTED] - metrics[NUM_QUERIES_FINISHED],
        # Executing
        metrics[NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED] -
        metrics[NUM_QUERIES_FINISHED],
        # Mem Lmt Ex
        metrics[NUM_QUERIES_EXCEEDED_MEM_LIMIT],
        # AC Rejected
        metrics[NUM_QUERIES_AC_REJECTED],
        # AC Timed Out
        metrics[NUM_QUERIES_AC_TIMEDOUT],
        # Cancel
        metrics[NUM_QUERIES_CANCELLED],
        # Err
        metrics[NUM_OTHER_ERRORS],
        # Incorrect
        metrics[NUM_RESULT_MISMATCHES],
        # Next Qry Mem Lmt
        self._mem_mb_needed_for_next_query.value,
        # Total Qry Mem Lmt
        self._mem_broker.total_mem_mb - self._mem_broker.available_mem_mb,
        # Tracked Mem
        "" if reported_mem == -1 else reported_mem,
        # RSS Mem
        "" if actual_mem == -1 else actual_mem))


  def _write_query_profile(self, report, subdir, prefix=None):
    report.write_query_profile(
        os.path.join(self.results_dir, subdir),
        prefix)

  def _check_successive_errors(self):
    if (self._num_successive_errors.value >= self.num_successive_errors_needed_to_abort):
      print(
          "Aborting due to %s successive errors encountered"
          % self._num_successive_errors.value, file=sys.stderr)
      self.print_duration()
      sys.exit(1)

  def _check_for_test_failure(self):
    metrics = self._calc_total_runner_metrics()
    if metrics[NUM_OTHER_ERRORS] > 0 or metrics[NUM_RESULT_MISMATCHES] > 0:
      LOG.error("Failing the stress test due to unexpected errors, incorrect results, or "
                "timed out queries. See the report line above for details.")
      self.print_duration()
      sys.exit(1)

  def _wait_for_test_to_finish(self, impala, should_print_status):
    last_report_secs = 0
    lines_printed = 1
    sleep_secs = 0.1

    num_runners_remaining = self._num_runners_remaining()
    while (
        self._query_producer_thread.is_alive() or
        self._query_consumer_thread.is_alive() or
        num_runners_remaining
    ):
      if self._query_producer_thread.error or self._query_consumer_thread.error:
        # This is bad enough to abort early. A failure here probably means there's a
        # bug in this script. The mem poller could be checked for an error too. It is
        # not critical so is ignored.
        LOG.error("Aborting due to error in producer/consumer")
        sys.exit(1)
      do_check_for_impala_crashes = False
      with self._query_runners_lock:
        for idx, runner in enumerate(self._query_runners):
          if runner.proc.exitcode is not None:
            if runner.proc.exitcode != 0:
              # Since at least one query runner process failed, make sure to check for
              # crashed impalads.
              do_check_for_impala_crashes = True
              # TODO: Handle case for num_queries_dequeued != num_queries_submitted
              num_submitted = runner.get_metric_val(NUM_QUERIES_SUBMITTED)
              num_started_or_cancelled = \
                  runner.get_metric_val(NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED)
              num_finished = runner.get_metric_val(NUM_QUERIES_FINISHED)
              if num_submitted != num_finished:
                # The query runner process may have crashed before updating the number
                # of finished queries but after it incremented the number of queries
                # submitted.
                assert num_submitted - num_finished == 1
                runner.increment_metric(NUM_QUERIES_FINISHED)
                if num_submitted != num_started_or_cancelled:
                  assert num_submitted - num_started_or_cancelled == 1
                  runner.increment_metric(NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED)

                # Since we know that the runner crashed while trying to run a query, we
                # count it as an 'other error'
                runner.increment_metric(NUM_OTHER_ERRORS)
              self._check_successive_errors()
            assert runner.get_metric_val(NUM_QUERIES_SUBMITTED) == \
                    runner.get_metric_val(NUM_QUERIES_FINISHED), \
                    str(runner.get_metric_vals())
            # Make sure to record all the metrics before removing this runner from the
            # list.
            print("Query runner ({0}) exited with exit code {1}".format(
                runner.proc.pid, runner.proc.exitcode))
            self._record_runner_metrics_before_evict(self._query_runners[idx])

            # Remove the query runner from the list.
            del self._query_runners[idx]

      if do_check_for_impala_crashes:
        # Since we know that at least one query runner failed, check if any of the Impala
        # daemons themselves crashed.
        LOG.info("Checking for Impala crashes")
        if print_crash_info_if_exists(impala, self.start_time):
          self.print_duration()
          sys.exit(runner.proc.exitcode)
        do_check_for_impala_crashes = False
        LOG.info("No Impala crashes detected")

      sleep(sleep_secs)
      num_runners_remaining = self._num_runners_remaining()

      if should_print_status:
        last_report_secs += sleep_secs
        if last_report_secs > 5:
          if (
              not self._query_producer_thread.is_alive() or
              not self._query_consumer_thread.is_alive() or
              not num_runners_remaining
          ):
            LOG.debug("Producer is alive: %s" % self._query_producer_thread.is_alive())
            LOG.debug("Consumer is alive: %s" % self._query_consumer_thread.is_alive())
            LOG.debug("Queue size: %s" % self._query_queue.qsize())
            LOG.debug("Runners: %s" % num_runners_remaining)
          last_report_secs = 0
          lines_printed %= 50
          self._print_status(print_header=(lines_printed == 0))
          lines_printed += 1

  def print_duration(self):
    duration = datetime.now() - self.start_time
    LOG.info("Test Duration: {0:.0f} seconds".format(duration.total_seconds()))


def load_random_queries_and_populate_runtime_info(impala, converted_args):
  """Returns a list of random queries. Each query will also have its runtime info
  populated. The runtime info population also serves to validate the query.
  """
  LOG.info("Generating random queries")
  return populate_runtime_info_for_random_queries(
      impala, generate_random_queries(impala, converted_args.random_db), converted_args)


def populate_runtime_info_for_random_queries(impala, candidate_queries, converted_args):
  """Returns a list of random queries selected from candidate queries, which should be
  a generator that will return an unlimited number of randomly generated queries.
  Each query will also have its runtime info populated. The runtime info population
  also serves to validate the query.
  """
  start_time = datetime.now()
  queries = list()
  # TODO(IMPALA-4632): Consider running reset_databases() here if we want to extend DML
  #                    functionality to random stress queries as well.
  for query in candidate_queries:
    try:
      populate_runtime_info(
          query, impala, converted_args,
          timeout_secs=converted_args.random_query_timeout_seconds)
      queries.append(query)
    except Exception as e:
      # Ignore any non-fatal errors. These could be query timeouts or bad queries (
      # query generator bugs).
      if print_crash_info_if_exists(impala, start_time):
        raise e
      LOG.warn(
          "Error running query (the test will continue)\n%s\n%s",
          e, query.sql, exc_info=True)
    if len(queries) == converted_args.random_query_count:
      break
  return queries


def populate_runtime_info(query, impala, converted_args, timeout_secs=maxsize):
  """Runs the given query by itself repeatedly until the minimum memory is determined
  with and without spilling. Potentially all fields in the Query class (except
  'sql') will be populated by this method. 'required_mem_mb_without_spilling' and
  the corresponding runtime field may still be None if the query could not be run
  without spilling.

  converted_args.samples and converted_args.max_conflicting_samples control the
  reliability of the collected information. The problem is that memory spilling or usage
  may differ (by a large amount) from run to run due to races during execution. The
  parameters provide a way to express "X out of Y runs must have resulted in the same
  outcome". Increasing the number of samples and decreasing the tolerance (max conflicts)
  increases confidence but also increases the time to collect the data.
  """
  LOG.info("Collecting runtime info for query %s: \n%s", query.name, query.sql)
  samples = converted_args.samples
  max_conflicting_samples = converted_args.max_conflicting_samples
  results_dir = converted_args.results_dir
  mem_limit_eq_threshold_mb = converted_args.mem_limit_eq_threshold_mb
  mem_limit_eq_threshold_percent = converted_args.mem_limit_eq_threshold_percent
  runner = QueryRunner(impalad=impala.impalads[0], results_dir=results_dir,
      common_query_options=converted_args.common_query_options,
      test_admission_control=converted_args.test_admission_control,
      use_kerberos=converted_args.use_kerberos, check_if_mem_was_spilled=True)
  runner.connect()
  limit_exceeded_mem = 0
  non_spill_mem = None
  spill_mem = None

  report = None
  mem_limit = None

  old_required_mem_mb_without_spilling = query.required_mem_mb_without_spilling
  old_required_mem_mb_with_spilling = query.required_mem_mb_with_spilling

  profile_error_prefix = query.logical_query_id + "_binsearch_error"

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
        query.solo_runtime_profile_with_spilling = report.profile
    elif (
        query.required_mem_mb_without_spilling is None or
        required_mem < query.required_mem_mb_without_spilling
    ):
      query.required_mem_mb_without_spilling = required_mem
      query.solo_runtime_secs_without_spilling = report.runtime_secs
      assert report.runtime_secs is not None, report
      query.solo_runtime_profile_without_spilling = report.profile

  def get_report(desired_outcome=None):
    reports_by_outcome = defaultdict(list)
    leading_outcome = None
    for remaining_samples in range(samples - 1, -1, -1):
      report = runner.run_query(query, mem_limit, run_set_up=True,
          timeout_secs=timeout_secs, retain_profile=True)
      if report.timed_out:
        report.write_query_profile(
            os.path.join(results_dir, PROFILES_DIR), profile_error_prefix)
        raise QueryTimeout(
            "query {0} timed out during binary search".format(query.logical_query_id))
      if report.other_error:
        report.write_query_profile(
            os.path.join(results_dir, PROFILES_DIR), profile_error_prefix)
        raise Exception(
            "query {0} errored during binary search: {1}".format(
                query.logical_query_id, str(report.other_error)))
      LOG.debug("Spilled: %s" % report.mem_was_spilled)
      if not report.has_query_error():
        if query.result_hash is None:
          query.result_hash = report.result_hash
        elif query.result_hash != report.result_hash:
          report.write_query_profile(
              os.path.join(results_dir, PROFILES_DIR), profile_error_prefix)
          raise Exception(
              "Result hash mismatch for query %s; expected %s, got %s" %
              (query.logical_query_id, query.result_hash, report.result_hash))

      if report.not_enough_memory:
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
    return reports[len(reports) // 2]

  if not any((old_required_mem_mb_with_spilling, old_required_mem_mb_without_spilling)):
    mem_estimate = estimate_query_mem_mb_usage(query, runner.impalad_conn)
    LOG.info("Finding a starting point for binary search")
    mem_limit = min(mem_estimate, impala.min_impalad_mem_mb) or impala.min_impalad_mem_mb
    while True:
      LOG.info("Next mem_limit: {0}".format(mem_limit))
      report = get_report()
      if not report or report.not_enough_memory:
        if report and report.not_enough_memory:
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
  upper_bound = min(non_spill_mem or maxsize, impala.min_impalad_mem_mb)
  while True:
    if old_required_mem_mb_without_spilling:
      mem_limit = old_required_mem_mb_without_spilling
      old_required_mem_mb_without_spilling = None
    else:
      mem_limit = (lower_bound + upper_bound) // 2
    LOG.info("Next mem_limit: {0}".format(mem_limit))
    should_break = mem_limit / float(upper_bound) > 1 - mem_limit_eq_threshold_percent \
        or upper_bound - mem_limit < mem_limit_eq_threshold_mb
    report = get_report(desired_outcome=("NOT_SPILLED" if spill_mem else None))
    if not report:
      lower_bound = mem_limit
    elif report.not_enough_memory:
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
      spill_mem or maxsize, non_spill_mem or maxsize, impala.min_impalad_mem_mb)
  while True:
    if old_required_mem_mb_with_spilling:
      mem_limit = old_required_mem_mb_with_spilling
      old_required_mem_mb_with_spilling = None
    else:
      mem_limit = (lower_bound + upper_bound) // 2
    LOG.info("Next mem_limit: {0}".format(mem_limit))
    should_break = mem_limit / float(upper_bound) > 1 - mem_limit_eq_threshold_percent \
        or upper_bound - mem_limit < mem_limit_eq_threshold_mb
    report = get_report(desired_outcome="SPILLED")
    if not report or report.not_enough_memory:
      lower_bound = mem_limit
    else:
      update_runtime_info()
      upper_bound = mem_limit
    if should_break:
      if not query.required_mem_mb_with_spilling:
        if upper_bound - mem_limit < mem_limit_eq_threshold_mb:
          # IMPALA-6604: A fair amount of queries go down this path.
          LOG.info(
              "Unable to find a memory limit with spilling within the threshold of {0} "
              "MB. Using the same memory limit for both.".format(
                  mem_limit_eq_threshold_mb))
        query.required_mem_mb_with_spilling = query.required_mem_mb_without_spilling
        query.solo_runtime_secs_with_spilling = query.solo_runtime_secs_without_spilling
        query.solo_runtime_profile_with_spilling = \
            query.solo_runtime_profile_without_spilling
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
    query.solo_runtime_profile_with_spilling = query.solo_runtime_profile_without_spilling
  LOG.debug("Query after populating runtime info: %s", query)


def prepare_database(cursor):
  """For each table in the database that cursor is connected to, create an identical copy
  with '_original' suffix. This function is idempotent.

  Note: At this time we only support Kudu tables with a simple hash partitioning based on
  the primary key. (SHOW CREATE TABLE would not work otherwise.)
  """
  tables = dict((t, cursor.describe_table(t)) for t in cursor.list_table_names())
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
  tables = dict((t, cursor.describe_table(t)) for t in cursor.list_table_names())
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


def populate_all_queries(
    queries, impala, converted_args, queries_with_runtime_info_by_db_sql_and_options
):
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
        populate_runtime_info(query, impala, converted_args)
        save_runtime_info(converted_args.runtime_info_path, query, impala)
        query.write_runtime_info_profiles(
            os.path.join(converted_args.results_dir, PROFILES_DIR))
        result.append(query)
  return result


def main():
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
  cli_options.add_ssl_options(parser)
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
      "--mem-limit-eq-threshold-percent", default=0.025,
      type=float, help='Used when collecting "runtime info". If the difference between'
      ' two memory limits is less than this percentage, we consider the two limits to'
      ' be equal and stop the memory binary search.')
  parser.add_argument(
      "--mem-limit-eq-threshold-mb", default=50,
      type=int, help='Used when collecting "runtime info". If the difference between'
      ' two memory limits is less than this value in MB, we consider the two limits to'
      ' be equal and stop the memory binary search.')
  parser.add_argument(
      "--results-dir", default=gettempdir(),
      help="Directory under which the profiles and result_hashes directories are created."
      " Query hash results are written in the result_hashes directory. If query results"
      " do not match, a log file will be left in that dir. The log file is also created"
      " during the first run when runtime info is collected for each query. Unexpected"
      " query timeouts, exceeded memory, failures or result mismatches will result in a"
      " profile written in the profiles directory.")
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
      help="Deprecated - has no effect.")
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
  parser.add_argument("--nlj-filter", help=SUPPRESS) # Made a no-op by IMPALA-7440.
  parser.add_argument(
      "--common-query-options", default=None, nargs="*",
      help="Space-delimited string of query options and values. This is a freeform "
      "string with little regard to whether you've spelled the query options correctly "
      "or set valid values. Example: --common-query-options "
      "DISABLE_CODEGEN=true RUNTIME_FILTER_MODE=1")
  parser.add_argument(
      "--test-admission-control", type=bool, default=False,
      help="If true, assume that the Impala cluster under test is using memory-based "
      "admission control and should not admit queries that cannot be run to completion. "
      "In this mode the stress runner does not set mem_limit on queries and "
      "out-of-memory errors are not expected in this mode so will fail the stress test "
      "if encountered. The stress runner still tracks the 'admitted' memory so that "
      "it can try to submit more queries than there is available memory for.")
  parser.add_argument(
      "--max-coordinators", default=0, type=int, metavar="max coordinators",
      help="If > 0, submit queries to at most this number of coordinators."
      "This is useful in conjunction with --test-admission-control to test behaviour "
      "with a smaller number of admission controller instances.")
  args = parser.parse_args()
  converted_args = StressArgConverter(args)

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

  result_hashes_path = os.path.join(args.results_dir, RESULT_HASHES_DIR)
  if not os.path.isdir(result_hashes_path):
    os.makedirs(result_hashes_path)
  results_dir_path = os.path.join(args.results_dir, PROFILES_DIR)
  if not os.path.isdir(results_dir_path):
    os.makedirs(results_dir_path)

  cluster = cli_options.create_cluster(args)
  impala = cluster.impala
  if impala.find_stopped_impalads():
    impala.restart()
  cluster.print_version()
  impala.find_and_set_path_to_running_impalad_binary()
  if args.cancel_current_queries and impala.queries_are_running():
    impala.cancel_queries()
    sleep(10)
  if impala.queries_are_running():
    raise Exception("Queries are currently running on the cluster")
  impala.min_impalad_mem_mb = min(impala.find_impalad_mem_mb_limit())

  queries_with_runtime_info_by_db_sql_and_options = load_runtime_info(
      converted_args.runtime_info_path, impala)

  # Start loading the test queries.
  queries = list()

  # If random queries were requested, those will be handled later. Unlike random queries,
  # the TPC queries are expected to always complete successfully.
  if args.tpcds_db:
    tpcds_queries = load_tpc_queries("tpcds")
    assert len(tpcds_queries) == EXPECTED_TPCDS_QUERIES_COUNT
    for query in tpcds_queries:
      query.db_name = args.tpcds_db
    queries.extend(tpcds_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpcds_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_db:
    tpch_queries = load_tpc_queries("tpch")
    assert len(tpch_queries) == EXPECTED_TPCH_STRESS_QUERIES_COUNT
    for query in tpch_queries:
      query.db_name = args.tpch_db
    queries.extend(tpch_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpch_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_nested_db:
    tpch_nested_queries = load_tpc_queries("tpch_nested")
    assert len(tpch_nested_queries) == EXPECTED_TPCH_NESTED_QUERIES_COUNT
    for query in tpch_nested_queries:
      query.db_name = args.tpch_nested_db
    queries.extend(tpch_nested_queries)
    if args.generate_compute_stats_queries:
      with impala.cursor(db_name=args.tpch_nested_db) as cursor:
        queries.extend(generate_compute_stats_queries(cursor))
  if args.tpch_kudu_db:
    tpch_kudu_queries = load_tpc_queries("tpch")
    assert len(tpch_kudu_queries) == EXPECTED_TPCH_STRESS_QUERIES_COUNT
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
    tpcds_kudu_queries = load_tpc_queries("tpcds")
    assert len(tpcds_kudu_queries) == EXPECTED_TPCDS_QUERIES_COUNT
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

  queries = populate_all_queries(
      queries, impala, converted_args, queries_with_runtime_info_by_db_sql_and_options)

  # A particular random query may either fail (due to a generator or Impala bug) or
  # take a really long time to complete. So the queries needs to be validated. Since the
  # runtime info also needs to be collected, that will serve as validation.
  if args.random_db:
    queries.extend(load_random_queries_and_populate_runtime_info(impala, converted_args))

  if args.query_file_path:
    file_queries = load_queries_from_test_file(
        args.query_file_path, db_name=args.query_file_db)
    shuffle(file_queries)
    queries.extend(populate_runtime_info_for_random_queries(
        impala, file_queries, converted_args))

  # Apply tweaks to the query's runtime info as requested by CLI options.
  for idx in range(len(queries) - 1, -1, -1):
    query = queries[idx]
    if query.required_mem_mb_with_spilling:
      query.required_mem_mb_with_spilling += int(
          query.required_mem_mb_with_spilling * args.mem_limit_padding_pct / 100.0) + \
          args.mem_limit_padding_abs
    if query.required_mem_mb_without_spilling:
      query.required_mem_mb_without_spilling += int(
          query.required_mem_mb_without_spilling * args.mem_limit_padding_pct / 100.0) + \
          args.mem_limit_padding_abs

    # Remove any queries that would use "too many" resources. This way a larger number
    # of queries will run concurrently.
    if query.required_mem_mb_without_spilling is not None and \
        query.required_mem_mb_without_spilling / float(impala.min_impalad_mem_mb) \
            > args.filter_query_mem_ratio:
      LOG.debug(
          "Filtering non-spilling query that exceeds "
          "--filter-query-mem-ratio: " + query.sql)
      query.required_mem_mb_without_spilling = None
    if query.required_mem_mb_with_spilling is None \
        or query.required_mem_mb_with_spilling / float(impala.min_impalad_mem_mb) \
            > args.filter_query_mem_ratio:
      LOG.debug("Filtering query that exceeds --filter-query-mem-ratio: " + query.sql)
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
  stress_runner.results_dir = args.results_dir
  stress_runner.startup_queries_per_sec = args.startup_queries_per_second
  stress_runner.num_successive_errors_needed_to_abort = args.fail_upon_successive_errors
  stress_runner.use_kerberos = args.use_kerberos
  stress_runner.cancel_probability = args.cancel_probability
  stress_runner.spill_probability = args.spill_probability
  stress_runner.leak_check_interval_mins = args.mem_leak_check_interval_mins
  stress_runner.common_query_options = converted_args.common_query_options
  stress_runner.test_admission_control = converted_args.test_admission_control
  stress_runner.max_coordinators = converted_args.max_coordinators
  stress_runner.run_queries(
      queries, impala, args.max_queries, args.mem_overcommit_pct,
      should_print_status=not args.no_status,
      verify_results=not args.generate_dml_queries,
      select_probability=args.select_probability)


if __name__ == "__main__":
  main()
