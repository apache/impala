#!/usr/bin/env impala-python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This module is used to stress test Impala by running queries concurrently. Only SELECT
# queries are used.
#
# Stress test outline (and notes):
#  1) Get a set of queries. TPCH and/or TPCDS queries will be used.
#     TODO: Add randomly generated queries.
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
#  7) Cancel long running queries. Queries that run longer than some expected time,
#     determined by the number of queries currently running, will be canceled.
#     TODO: Collect stacks of timed out queries and add reporting.
#  8) If a query errored, verify that memory was overcommitted during execution and the
#     error is a mem limit exceeded error. There is no other reason a query should error
#     and any such error will cause the stress test to stop.
#     TODO: Handle crashes -- collect core dumps and restart Impala
#     TODO: Handle client connectivity timeouts -- retry a few times
#  9) Verify the result set hash of successful queries.

import json
import logging
import os
import re
import sys
from Queue import Empty   # Must be before Queue below
from cm_api.api_client import ApiResource
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timedelta
from multiprocessing import Process, Queue, Value
from multiprocessing.pool import ThreadPool
from random import choice, random, randrange
from sys import maxint
from tempfile import gettempdir
from textwrap import dedent
from threading import Thread, current_thread
from time import sleep, time, strptime

import tests.util.test_file_parser as test_file_parser
from tests.comparison.db_connector import DbConnector, IMPALA

LOG = logging.getLogger(__name__)

# Used to short circuit a binary search of the min mem limit. Values will be considered
# equal if they are within this ratio of each other.
MEM_LIMIT_EQ_THRESHOLD = 0.975

# Regex to extract the estimated memory from an explain plan.
MEM_ESTIMATE_PATTERN = re.compile(r"Estimated.*Memory=(\d+.?\d*)(T|G|M|K)?B")

# The version of the file format containing the collected query runtime info.
RUNTIME_INFO_FILE_VERSION = 1

def create_and_start_daemon_thread(target):
  thread = Thread(target=target)
  thread.error = None
  thread.daemon = True
  thread.start()
  return thread


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
    self._available = Value("i", real_mem_mb + overcommitable_mem_mb)
    self._max_overcommitment = overcommitable_mem_mb

    # Each reservation will be assigned an id. Ids are monotonically increasing. When
    # a reservation crosses the overcommitment threshold, the corresponding reservation
    # id will be stored in '_last_overcommitted_reservation_id' so clients can check
    # to see if memory was overcommitted since their reservation was made (this is a race
    # but an incorrect result will be on the conservative side).
    self._next_reservation_id = Value("L", 0)
    self._last_overcommitted_reservation_id = Value("L", 0)

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
          LOG.debug("Reserved %s MB; %s MB available; %s MB overcommitted", req,
              self._available.value, self.overcommitted_mem_mb)
          reservation_id = self._next_reservation_id.value
          self._next_reservation_id.value += 1
          if self.overcommitted_mem_mb > 0:
            self._last_overcommitted_reservation_id.value = reservation_id
          return reservation_id
      sleep(0.1)

  def _release(self, req):
    with self._available.get_lock():
      self._available.value += req
      LOG.debug("Released %s MB; %s MB available; %s MB overcommitted", req,
          self._available.value, self.overcommitted_mem_mb)

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
    self._mem_broker = None

    # Synchronized blocking work queue for producer/consumers.
    self._query_queue = Queue(self.WORK_QUEUE_CAPACITY)

    # The Value class provides cross-process shared memory.
    self._mem_mb_needed_for_next_query = Value("i", 0)

    # All values below are cumulative.
    self._num_queries_dequeued = Value("i", 0)
    self._num_queries_started = Value("i", 0)
    self._num_queries_finished = Value("i", 0)
    self._num_queries_exceeded_mem_limit = Value("i", 0)
    self._num_queries_cancelled = Value("i", 0)
    self._num_queries_timedout = Value("i", 0)

    self.cancel_probability = 0
    self.spill_probability = 0

  def run_queries(self, queries, impala, num_queries_to_run, mem_overcommit_pct,
      should_print_status):
    """Runs queries randomly chosen from 'queries' and stops after 'num_queries_to_run'
       queries have completed.

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

       If a query completes without error, the result will be verified. An error
       will be raised upon a result mismatch.
    """
    self._mem_broker = MemBroker(impala.min_impalad_mem_mb,
        int(impala.min_impalad_mem_mb * mem_overcommit_pct / 100))

    # Print the status to show the state before starting.
    if should_print_status:
      self._print_status_header()
      self._print_status()
      lines_printed = 1
      last_report_secs = 0

    # Start producing queries.
    def enque_queries():
      try:
        for _ in xrange(num_queries_to_run):
          self._query_queue.put(choice(queries))
      except Exception as e:
        current_thread().error = e
        raise e
    enqueue_thread = create_and_start_daemon_thread(enque_queries)

    # Start a thread to check if more producers are needed. More producers are needed
    # when no queries are currently dequeued and waiting to be started.
    runners = list()
    def start_additional_runners_if_needed():
      try:
        while self._num_queries_started.value < num_queries_to_run:
          # Remember num dequeued/started are cumulative.
          if self._num_queries_dequeued.value == self._num_queries_started.value:
            impalad = impala.impalads[len(runners) % len(impala.impalads)]
            runner = Process(target=self._start_single_runner, args=(impalad, ))
            runner.daemon = True
            runners.append(runner)
            runner.start()
          sleep(1)
      except Exception as e:
        current_thread().error = e
        raise e
    runners_thread = create_and_start_daemon_thread(start_additional_runners_if_needed)

    # Wait for everything to finish but exit early if anything failed.
    sleep_secs = 0.1
    while enqueue_thread.is_alive() or runners_thread.is_alive() or runners:
      if enqueue_thread.error or runners_thread.error:
        sys.exit(1)
      for idx, runner in enumerate(runners):
        if runner.exitcode is not None:
          if runner.exitcode == 0:
            del runners[idx]
          else:
            sys.exit(runner.exitcode)
      sleep(sleep_secs)
      if should_print_status:
        last_report_secs += sleep_secs
        if last_report_secs > 5:
          last_report_secs = 0
          lines_printed %= 50
          if lines_printed == 0:
            self._print_status_header()
          self._print_status()
          lines_printed += 1

    # And print the final state.
    if should_print_status:
      self._print_status()

  def _start_single_runner(self, impalad):
    """Consumer function to take a query of the queue and run it. This is intended to
       run in a separate process so validating the result set can use a full CPU.
    """
    runner = QueryRunner()
    runner.impalad = impalad
    runner.connect()

    while not self._query_queue.empty():
      try:
        query = self._query_queue.get(True, 1)
      except Empty:
        continue
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
        mem_limit = randrange(query.required_mem_mb_with_spilling,
            query.required_mem_mb_without_spilling + 1)
        solo_runtime = query.solo_runtime_secs_with_spilling

      while query_idx > self._num_queries_started.value:
        sleep(0.1)

      self._mem_mb_needed_for_next_query.value = mem_limit

      with self._mem_broker.reserve_mem_mb(mem_limit) as reservation_id:
        self._num_queries_started.value += 1
        should_cancel = self.cancel_probability > random()
        if should_cancel:
          timeout = randrange(1, max(int(solo_runtime), 2))
        else:
          timeout = solo_runtime * max(10, self._num_queries_started.value
              - self._num_queries_finished.value)
        report = runner.run_query(query, timeout, mem_limit)
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
            continue
          # Occasionally the network connection will fail, and depending on when the
          # failure occurred during run_query(), an attempt to get the profile may be
          # made which results in "Invalid session id" since the server destroyed the
          # session upon disconnect.
          if "Invalid session id" in error_msg:
            continue
          raise Exception("Query failed: %s" % str(report.non_mem_limit_error))
        if report.mem_limit_exceeded \
            and not self._mem_broker.was_overcommitted(reservation_id):
          raise Exception("Unexpected mem limit exceeded; mem was not overcommitted\n"
              "Profile: %s" % report.profile)
        if not report.mem_limit_exceeded \
            and not report.timed_out \
            and report.result_hash != query.result_hash:
          raise Exception("Result hash mismatch; expected %s, got %s"
              % (query.result_hash, report.result_hash))

  def _print_status_header(self):
    print(" Done | Running | Mem Exceeded | Timed Out | Canceled | Mem Avail | Mem Over "
        "| Next Qry Mem")

  def _print_status(self):
    print("%5d | %7d | %12d | %9d | %8d | %9d | %8d | %12d" % (
        self._num_queries_finished.value,
        self._num_queries_started.value - self._num_queries_finished.value,
        self._num_queries_exceeded_mem_limit.value,
        self._num_queries_timedout.value - self._num_queries_cancelled.value,
        self._num_queries_cancelled.value,
        self._mem_broker.available_mem_mb,
        self._mem_broker.overcommitted_mem_mb,
        self._mem_mb_needed_for_next_query.value))

  def _update_from_query_report(self, report):
    self._num_queries_finished.value += 1
    if report.mem_limit_exceeded:
      self._num_queries_exceeded_mem_limit.value += 1
    if report.was_cancelled:
      self._num_queries_cancelled.value += 1
    if report.timed_out:
      self._num_queries_timedout.value += 1


class QueryTimeout(Exception):
  pass


class Query(object):
  """Contains a SQL statement along with expected runtime information."""

  def __init__(self):
    self.sql = None
    self.db_name = None
    self.result_hash = None
    self.required_mem_mb_with_spilling = None
    self.required_mem_mb_without_spilling = None
    self.solo_runtime_secs_with_spilling = None
    self.solo_runtime_secs_without_spilling = None


class Impalad(object):

  def __init__(self):
    self.host_name = None
    self.port = None


class Impala(object):
  """This class wraps the CM API to provide additional functionality."""

  def __init__(self, cm_service_api):
    self.cm_service = cm_service_api

    cm_impalads = cm_service_api.get_roles_by_type('IMPALAD')

    # Keep a list of impalads. The host name and port will be found later.
    self.impalads = [Impalad() for _ in cm_impalads]

    # Getting the info over the network can be slow so threads will be used.
    def set_fields_and_get_mem((impalad_idx, cm_impalad)):
      impalad = self.impalads[impalad_idx]
      config = cm_impalad.get_config(view="full")
      port_config = config["hs2_port"]
      impalad.port = int(port_config.value or port_config.default)
      impalad.host_name = cm_service_api._resource_root.get_host(
          cm_impalad.hostRef.hostId).hostname

      mem_config = config["impalad_memory_limit"]
      return int(mem_config.value or mem_config.default) / 1024 ** 2
    # Initialize strptime() to workaround https://bugs.python.org/issue7980. Apparently
    # something in the CM API uses strptime().
    strptime("2015", "%Y")
    self.min_impalad_mem_mb = min(
        ThreadPool().map(set_fields_and_get_mem, enumerate(cm_impalads)))

  def get_cm_queries(self):
    search_range = timedelta(days=365)
    api_result = self.cm_service.get_impala_queries(datetime.now() - search_range,
        datetime.now() + search_range, filter_str="executing = true")
    return api_result.queries

  def queries_are_running(self):
    return len(self.get_cm_queries())

  def cancel_queries(self):
    for cm_query in self.get_cm_queries():
      self.cm_service.cancel_impala_query(cm_query.queryId)


class QueryRunner(object):
  """Encapsulates functionality to run a query and provide a runtime report."""

  SPILLED_PATTERN = re.compile("ExecOption:.*Spilled")
  BATCH_SIZE = 1024

  def __init__(self):
    self.impalad = None
    self.impalad_conn = None

  def connect(self):
    self.impalad_conn = DbConnector(
        IMPALA, host_name=self.impalad.host_name, port=self.impalad.port
        ).create_connection()

  def disconnect(self):
    if self.impalad_conn:
      self.impalad_conn.close()
      self.impalad_conn = None

  def run_query(self, query, timeout_secs, mem_limit_mb):
    """Run a query and return an execution report."""
    if not self.impalad_conn:
      raise Exception("connect() must first be called")

    timeout_unix_time = time() + timeout_secs
    report = QueryReport()
    try:
      with self.impalad_conn.open_cursor() as cursor:
        start_time = time()
        LOG.debug("Setting mem limit to %s MB", mem_limit_mb)
        cursor.execute("SET MEM_LIMIT=%sM" % mem_limit_mb)
        LOG.debug("Using %s database", query.db_name)
        cursor.execute("USE %s" % query.db_name)
        LOG.debug("Running query with %s MB mem limit at %s with timeout secs %s:\n%s",
            mem_limit_mb, self.impalad.host_name, timeout_secs, query.sql)
        error = None
        try:
          cursor.execute_async("/* Mem: %s MB. Coordinator: %s. */\n"
              % (mem_limit_mb, self.impalad.host_name) + query.sql)
          LOG.debug("Query id is %s", cursor._last_operation_handle)
          while cursor.is_executing():
            if time() > timeout_unix_time:
              self._cancel(cursor, report)
              return report
            sleep(0.1)
          try:
            report.result_hash = self._hash_result(cursor, timeout_unix_time)
          except QueryTimeout:
            self._cancel(cursor, report)
            return report
        except Exception as error:
          LOG.debug("Error running query with id %s: %s", cursor._last_operation_handle,
              error)
          self._check_for_mem_limit_exceeded(report, cursor, error)
        if report.non_mem_limit_error or report.mem_limit_exceeded:
          return report
        report.runtime_secs = time() - start_time
        report.profile = cursor.get_profile()
        report.mem_was_spilled = \
            QueryRunner.SPILLED_PATTERN.search(report.profile) is not None
    except Exception as error:
      # A mem limit error would have been caught above, no need to check for that here.
      report.non_mem_limit_error = error
    return report


  def _cancel(self, cursor, report):
    report.timed_out = True
    if cursor._last_operation_handle:
      LOG.debug("Attempting cancellation of query with id %s",
          cursor._last_operation_handle)
      cursor.cancel_operation()

  def _check_for_mem_limit_exceeded(self, report, cursor, caught_exception):
    """To be called after a query failure to check for signs of failed due to a
       mem limit. The report will be updated accordingly.
    """
    if cursor._last_operation_handle:
      try:
        report.profile = cursor.get_profile()
      except Exception as e:
        LOG.debug("Error getting profile for query with id %s: %s",
            cursor._last_operation_handle, e)
    if "memory limit exceeded" in str(caught_exception).lower():
      report.mem_limit_exceeded = True
      return
    LOG.error("Non-mem limit error for query with id %s: %s",
        cursor._last_operation_handle, caught_exception, exc_info=True)
    report.non_mem_limit_error = caught_exception

  def _hash_result(self, cursor, timeout_unix_time):
    """Returns a hash that is independent of row order."""
    # A value of 1 indicates that the hash thread should continue to work.
    should_continue = Value("i", 1)
    def hash_result_impl():
      try:
        current_thread().result = 1
        while should_continue.value:
          LOG.debug("Fetching result for query with id %s"
              % cursor._last_operation_handle)
          rows = cursor.fetchmany(self.BATCH_SIZE)
          if not rows:
            return
          for row in rows:
            for idx, val in enumerate(row):
              # Floats returned by Impala may not be deterministic, the ending
              # insignificant digits may differ. Only the first 6 digits will be used
              # after rounding.
              if isinstance(val, float):
                sval = "%f" % val
                dot_idx = sval.find(".")
                val = round(val, 6 - dot_idx)
              current_thread().result += (idx + 1) * hash(val)
              # Modulo the result to Keep it "small" otherwise the math ops can be slow
              # since python does infinite precision math.
              current_thread().result %= maxint
      except Exception as e:
        current_thread().error = e
    hash_thread = create_and_start_daemon_thread(hash_result_impl)
    hash_thread.join(max(timeout_unix_time - time(), 0))
    if hash_thread.is_alive():
      should_continue.value = 0
      raise QueryTimeout()
    if hash_thread.error:
      raise hash_thread.error
    return hash_thread.result


def find_impala_in_cm(cm_host, cm_user, cm_password, cm_cluster_name):
  """Finds the Impala service in CM and returns an Impala instance."""
  cm = ApiResource(cm_host, username=cm_user, password=cm_password)
  cm_impalas = [service for cluster in cm.get_all_clusters()
                if cm_cluster_name is None or cm_cluster_name == cluster.name
                for service in cluster.get_all_services() if service.type == "IMPALA"]
  if len(cm_impalas) > 1:
    raise Exception("Found %s Impala services in CM;" % len(cm_impalas) +
        " use --cm-cluster-name option to specify which one to use.")
  if len(cm_impalas) == 0:
    raise Exception("No Impala services found in CM")
  return Impala(cm_impalas[0])


def load_tpc_queries(workload):
  """Returns a list of tpc queries. 'workload' should either be 'tpch' or 'tpcds'."""
  queries = list()
  query_dir = os.path.join(os.path.dirname(__file__), "..", "..",
      "testdata", "workloads", workload, "queries")
  for query_file in os.listdir(query_dir):
    if workload + "-q" not in query_file:
      continue
    test_cases = test_file_parser.parse_query_test_file(
        os.path.join(query_dir, query_file))
    for test_case in test_cases:
      query = Query()
      query.sql = test_file_parser.remove_comments(test_case["QUERY"])
      queries.append(query)
  return queries


def populate_runtime_info(query, impala):
  """Runs the given query by itself repeatedly until the minimum memory is determined
     with and without spilling. Potentially all fields in the Query class (except
     'sql') will be populated by this method. 'required_mem_mb_without_spilling' and
     the corresponding runtime field may still be None if the query could not be run
     without spilling.
  """
  LOG.info("Collecting runtime info for query: \n%s", query.sql)
  runner = QueryRunner()
  runner.impalad = impala.impalads[0]
  runner.connect()
  min_mem = 1
  max_mem = impala.min_impalad_mem_mb
  spill_mem = None
  error_mem = None

  report = None
  mem_limit = None

  def validate_result_hash():
    if query.result_hash is None:
      query.result_hash = report.result_hash
    elif query.result_hash != report.result_hash:
      raise Exception("Result hash mismatch; expected %s, got %s"
          % (query.result_hash, report.result_hash))

  def update_runtime_info():
    assert not report.non_mem_limit_error
    if report.mem_was_spilled:
      query.required_mem_mb_with_spilling = min(mem_limit, impala.min_impalad_mem_mb)
      query.solo_runtime_secs_with_spilling = report.runtime_secs
    else:
      query.required_mem_mb_without_spilling = min(mem_limit, impala.min_impalad_mem_mb)
      query.solo_runtime_secs_without_spilling = report.runtime_secs

  mem_limit = min(estimate_query_mem_mb_usage(query, runner), max_mem) or max_mem
  while True:
    report = runner.run_query(query, maxint, mem_limit)
    if report.mem_limit_exceeded:
      min_mem = mem_limit
    elif report.mem_was_spilled:
      update_runtime_info()
      validate_result_hash()
      spill_mem = mem_limit
    else:
      update_runtime_info()
      validate_result_hash()
      max_mem = mem_limit
      break
    if mem_limit == max_mem:
      LOG.warn("Query could not be run even when using all available memory\n%s",
          query.sql)
      return
    mem_limit = min(2 * mem_limit, max_mem)

  LOG.info("Finding minimum memory required to avoid spilling")
  while True:
    mem_limit = (min_mem + max_mem) / 2
    if min_mem / float(mem_limit) > MEM_LIMIT_EQ_THRESHOLD:
      break
    report = runner.run_query(query, maxint, mem_limit)
    if report.mem_limit_exceeded:
      min_mem = error_mem = mem_limit
      continue
    update_runtime_info()
    validate_result_hash()
    if report.mem_was_spilled:
      min_mem = spill_mem = mem_limit
    else:
      max_mem = mem_limit
  LOG.info("Minimum memory to avoid spilling is %s MB" % mem_limit)

  min_mem = error_mem or 1
  max_mem = spill_mem or mem_limit
  LOG.info("Finding absolute minimum memory required")
  while True:
    mem_limit = (min_mem + max_mem) / 2
    if min_mem / float(mem_limit) > MEM_LIMIT_EQ_THRESHOLD:
      if not query.required_mem_mb_with_spilling:
        query.required_mem_mb_with_spilling = query.required_mem_mb_without_spilling
        query.solo_runtime_secs_with_spilling = query.solo_runtime_secs_without_spilling
      break
    report = runner.run_query(query, maxint, mem_limit)
    if report.mem_limit_exceeded:
      min_mem = mem_limit
      continue
    update_runtime_info()
    validate_result_hash()
    max_mem = mem_limit
  LOG.info("Minimum memory is %s MB" % mem_limit)


def estimate_query_mem_mb_usage(query, query_runner):
  """Runs an explain plan then extracts and returns the estimated memory needed to run
     the query.
  """
  with query_runner.impalad_conn.open_cursor() as cursor:
    LOG.debug("Using %s database", query.db_name)
    cursor.execute('USE ' + query.db_name)
    LOG.debug("Explaining query\n%s", query.sql)
    cursor.execute('EXPLAIN ' + query.sql)
    first_val = cursor.fetchone()[0]
    regex_result = MEM_ESTIMATE_PATTERN.search(first_val)
    if not regex_result:
      return
    mem_limit, units = regex_result.groups()
    if mem_limit <= 0:
      return
    mem_limit = float(mem_limit)
    if units is None:
      mem_limit /= 10 ** 6
    elif units == "K":
      mem_limit /= 10 ** 3
    elif units == "M":
      pass
    elif units == "G":
      mem_limit *= 10 ** 3
    elif units == "T":
      mem_limit *= 10 ** 6
    else:
      raise Exception('Unexpected memory unit "%s" in "%s"' % (units, first_val))
    return int(mem_limit)


def save_runtime_info(path, query, impala):
  """Updates the file at 'path' with the given query information."""
  store = None
  if os.path.exists(path):
    with open(path) as file:
      store = json.load(file)
    _check_store_version(store)
  if not store:
    store = {"host_names": list(), "db_names": dict(),
        "version": RUNTIME_INFO_FILE_VERSION}
  with open(path, "w+") as file:
    store["host_names"] = sorted([i.host_name for i in impala.impalads])
    queries = store["db_names"].get(query.db_name, dict())
    queries[query.sql] = query
    store["db_names"][query.db_name] = queries
    class JsonEncoder(json.JSONEncoder):
      def default(self, obj):
        data = dict(obj.__dict__)
        # Queries are stored by sql, so remove the duplicate data.
        if "sql" in data:
          del data["sql"]
        return data
    json.dump(store, file, cls=JsonEncoder, sort_keys=True, indent=2,
        separators=(',', ': '))


def load_runtime_info(path, impala):
  """Reads the query runtime information at 'path' and returns a
     dict<db_name, dict<sql, Query>>. Returns an empty dict if the hosts in the 'impala'
     instance do not match the data in 'path'.
  """
  queries_by_db_and_sql = defaultdict(dict)
  if not os.path.exists(path):
    return queries_by_db_and_sql
  with open(path) as file:
    store = json.load(file)
    _check_store_version(store)
    if store.get("host_names") != sorted([i.host_name for i in impala.impalads]):
      return queries_by_db_and_sql
    for db_name, queries_by_sql in store["db_names"].iteritems():
      for sql, json_query in queries_by_sql.iteritems():
        query = Query()
        query.__dict__.update(json_query)
        query.sql = sql
        queries_by_db_and_sql[db_name][sql] = query
  return queries_by_db_and_sql


def _check_store_version(store):
  """Clears 'store' if the version is too old or raises an error if the version is too
     new.
  """
  if store["version"] < RUNTIME_INFO_FILE_VERSION:
    LOG.warn("Runtime file info version is old and will be ignored")
    store.clear()
  elif store["version"] > RUNTIME_INFO_FILE_VERSION:
    raise Exception("Unexpected runtime file info version %s expected %s"
        % (store["version"], RUNTIME_INFO_FILE_VERSION))


def main():
  from optparse import OptionParser
  import tests.comparison.cli_options as cli_options

  parser = OptionParser(epilog=dedent(
      """Before running this script a CM cluster must be setup and any needed data
         such as TPC-H/DS must be loaded. The first time this script is run it will
         find memory limits and runtimes for each query and save the data to disk (since
         collecting the data is slow) at --runtime-info-path then run the stress test.
         Later runs will reuse the saved memory limits and timings. If the cluster changes
         significantly the memory limits should be re-measured (deleting the file at
         --runtime-info-path will cause re-measuring to happen)."""))
  cli_options.add_logging_options(parser)
  cli_options.add_cm_options(parser)
  cli_options.add_db_name_option(parser)
  parser.add_option("--runtime-info-path",
      default=os.path.join(gettempdir(), "{cm_host}_query_runtime_info.json"),
      help="The path to store query runtime info at. '{cm_host}' will be replaced with"
      " the actual host name from --cm-host.")
  parser.add_option("--no-status", action="store_true",
      help="Do not print the status table.")
  parser.add_option("--cancel-current-queries", action="store_true",
      help="Cancel any queries running on the cluster before beginning.")
  parser.add_option("--filter-query-mem-ratio", type=float, default=0.333,
      help="Queries that require this ratio of total available memory will be filtered.")
  parser.add_option("--mem-limit-padding-pct", type=int, default=25,
      help="Pad query mem limits found by solo execution with this percentage when"
      " running concurrently. After padding queries will not be expected to fail"
      " due to mem limit exceeded.")
  parser.add_option("--timeout-multiplier", type=float, default=1.0,
      help="Query timeouts will be multiplied by this value.")
  parser.add_option("--max-queries", type=int, default=100)
  parser.add_option("--tpcds-db-name")
  parser.add_option("--tpch-db-name")
  parser.add_option("--mem-overcommit-pct", type=float, default=0)
  parser.add_option("--mem-spill-probability", type=float, default=0.33,
      dest="spill_probability",
      help="The probability that a mem limit will be set low enough to induce spilling.")
  parser.add_option("--cancel-probability", type=float, default=0.1,
      help="The probability a query will be cancelled.")
  cli_options.add_default_values_to_help(parser)
  opts, args = parser.parse_args()

  if not opts.tpcds_db_name and not opts.tpch_db_name:
    raise Exception("At least one of --tpcds-db-name --tpch-db-name is required")

  cli_options.configure_logging(opts.log_level, debug_log_file=opts.debug_log_file,
      log_thread_id=True, log_process_id=True)
  LOG.debug("CLI opts: %s" % (opts, ))
  LOG.debug("CLI args: %s" % (args, ))

  impala = find_impala_in_cm(
      opts.cm_host, opts.cm_user, opts.cm_password, opts.cm_cluster_name)
  if opts.cancel_current_queries:
    impala.cancel_queries()
  if impala.queries_are_running():
    raise Exception("Queries are currently running on the cluster")

  runtime_info_path = opts.runtime_info_path
  if "{cm_host}" in runtime_info_path:
    runtime_info_path = runtime_info_path.format(cm_host=opts.cm_host)
  queries_with_runtime_info_by_db_and_sql = load_runtime_info(runtime_info_path, impala)
  queries = list()
  if opts.tpcds_db_name:
    tpcds_queries = load_tpc_queries("tpcds")
    for query in tpcds_queries:
      query.db_name = opts.tpcds_db_name
    queries.extend(tpcds_queries)
  if opts.tpch_db_name:
    tpch_queries = load_tpc_queries("tpch")
    for query in tpch_queries:
      query.db_name = opts.tpch_db_name
    queries.extend(tpch_queries)
  for idx in xrange(len(queries) - 1, -1, -1):
    query = queries[idx]
    if query.sql in queries_with_runtime_info_by_db_and_sql[query.db_name]:
      query = queries_with_runtime_info_by_db_and_sql[query.db_name][query.sql]
      LOG.debug("Reusing previous runtime data for query: " + query.sql)
      queries[idx] = query
    else:
      populate_runtime_info(query, impala)
      save_runtime_info(runtime_info_path, query, impala)
    if query.required_mem_mb_with_spilling:
      query.required_mem_mb_with_spilling += int(query.required_mem_mb_with_spilling
          * opts.mem_limit_padding_pct / 100.0)
    if query.required_mem_mb_without_spilling:
      query.required_mem_mb_without_spilling += int(query.required_mem_mb_without_spilling
          * opts.mem_limit_padding_pct / 100.0)
    if query.solo_runtime_secs_with_spilling:
      query.solo_runtime_secs_with_spilling *= opts.timeout_multiplier
    if query.solo_runtime_secs_without_spilling:
      query.solo_runtime_secs_without_spilling *= opts.timeout_multiplier

    # Remove any queries that would use "too many" resources. This way a larger number
    # of queries will run concurrently.
    if query.required_mem_mb_with_spilling is None \
        or query.required_mem_mb_with_spilling / impala.min_impalad_mem_mb \
            > opts.filter_query_mem_ratio:
      LOG.debug("Filtered query due to mem ratio option: " + query.sql)
      del queries[idx]
  if len(queries) == 0:
    raise Exception("All queries were filtered")

  stress_runner = StressRunner()
  stress_runner.cancel_probability = opts.cancel_probability
  stress_runner.spill_probability = opts.spill_probability
  stress_runner.run_queries(queries, impala, opts.max_queries, opts.mem_overcommit_pct,
      not opts.no_status)

if __name__ == "__main__":
  main()
