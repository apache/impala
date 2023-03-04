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

from __future__ import absolute_import, division, print_function
from builtins import round
import logging
from multiprocessing import Value
import os
import re
from textwrap import dedent
from time import sleep, time
from sys import maxsize

from tests.stress.queries import QueryType
from tests.stress.util import create_and_start_daemon_thread, increment
from tests.util.thrift_util import op_handle_to_query_id

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

# Metrics collected during the stress running process.
NUM_QUERIES_DEQUEUED = "num_queries_dequeued"
# The number of queries that were submitted to a query runner.
NUM_QUERIES_SUBMITTED = "num_queries_submitted"
# The number of queries that have entered the RUNNING state (i.e. got through Impala's
# admission control and started executing) or were cancelled or hit an error.
NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED = "num_queries_started_running_or_cancelled"
NUM_QUERIES_FINISHED = "num_queries_finished"
NUM_QUERIES_EXCEEDED_MEM_LIMIT = "num_queries_exceeded_mem_limit"
NUM_QUERIES_AC_REJECTED = "num_queries_ac_rejected"
NUM_QUERIES_AC_TIMEDOUT = "num_queries_ac_timedout"
NUM_QUERIES_CANCELLED = "num_queries_cancelled"
NUM_RESULT_MISMATCHES = "num_result_mismatches"
NUM_OTHER_ERRORS = "num_other_errors"


class CancelMechanism:
  """Pseudo-enum to specify how a query should be cancelled."""
  # Cancel via the stress test client issuing an RPC to the server.
  VIA_CLIENT = "CANCEL_VIA_CLIENT"
  # Cancel by setting the EXEC_TIME_LIMIT_S query option.
  VIA_OPTION = "CANCEL_VIA_OPTION"

  ALL_MECHS = [VIA_CLIENT, VIA_OPTION]

RESULT_HASHES_DIR = "result_hashes"


class QueryTimeout(Exception):
  pass


class QueryRunner(object):
  """Encapsulates functionality to run a query and provide a runtime report."""

  SPILLED_PATTERNS = [re.compile("ExecOption:.*Spilled"), re.compile("SpilledRuns: [^0]")]
  BATCH_SIZE = 1024

  def __init__(self, impalad, results_dir, use_kerberos, common_query_options,
               test_admission_control, check_if_mem_was_spilled=False):
    """Creates a new instance, but does not start the process. """
    self.impalad = impalad
    self.use_kerberos = use_kerberos
    self.results_dir = results_dir
    self.check_if_mem_was_spilled = check_if_mem_was_spilled
    self.common_query_options = common_query_options
    self.test_admission_control = test_admission_control
    # proc is filled out by caller
    self.proc = None
    # impalad_conn is initialised in connect()
    self.impalad_conn = None

    # All these values are shared values between processes. We want these to be accessible
    # by the parent process that started this QueryRunner, for operational purposes.
    self._metrics = {
        NUM_QUERIES_DEQUEUED: Value("i", 0),
        NUM_QUERIES_SUBMITTED: Value("i", 0),
        NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED: Value("i", 0),
        NUM_QUERIES_FINISHED: Value("i", 0),
        NUM_QUERIES_EXCEEDED_MEM_LIMIT: Value("i", 0),
        NUM_QUERIES_AC_REJECTED: Value("i", 0),
        NUM_QUERIES_AC_TIMEDOUT: Value("i", 0),
        NUM_QUERIES_CANCELLED: Value("i", 0),
        NUM_RESULT_MISMATCHES: Value("i", 0),
        NUM_OTHER_ERRORS: Value("i", 0)}

  def connect(self):
    """Connect to the server and start the query runner thread."""
    self.impalad_conn = self.impalad.impala.connect(impalad=self.impalad)

  def run_query(self, query, mem_limit_mb, run_set_up=False,
                timeout_secs=maxsize, cancel_mech=None, retain_profile=False):
    """Run a query and return an execution report. If 'run_set_up' is True, set up sql
    will be executed before the main query. This should be the case during the binary
    search phase of the stress test. 'cancel_mech' is optionally a CancelMechanism
    value that should be used to cancel the query after timeout_secs.
    If 'cancel_mech' is provided, don't get the query profile for timed out queries
    because the query was purposely cancelled, rather than having some problem that needs
    investigation.
    """
    assert self.impalad_conn, "connect() must be called before run_query()"
    assert cancel_mech is None or cancel_mech in CancelMechanism.ALL_MECHS

    timeout_unix_time = time() + timeout_secs
    report = QueryReport(query)
    try:
      with self.impalad_conn.cursor() as cursor:
        start_time = time()
        self._set_db_and_options(cursor, query, run_set_up, mem_limit_mb, timeout_secs,
            cancel_mech)
        error = None
        try:
          cursor.execute_async(
              "/* Mem: %s MB. Coordinator: %s. */\n"
              % (mem_limit_mb, self.impalad.host_name) + query.sql)
          report.query_id = op_handle_to_query_id(cursor._last_operation.handle if
                                                  cursor._last_operation else None)
          LOG.debug("Query id is %s", report.query_id)
          self._wait_until_fetchable(cursor, report, timeout_unix_time)
          if query.query_type == QueryType.SELECT:
            report.result_hash = self._fetch_and_hash_result(cursor, timeout_unix_time,
                                                             query)
          else:
            # If query is in error state, this will raise an exception
            cursor._wait_to_finish()
          if (retain_profile or
             query.result_hash and report.result_hash != query.result_hash):
            fetch_and_set_profile(cursor, report)
        except QueryTimeout:
          if not cancel_mech:
            fetch_and_set_profile(cursor, report)
          # Cancel from this client if a) we hit the timeout unexpectedly or b) we're
          # deliberately cancelling the query via the client.
          if not cancel_mech or cancel_mech == CancelMechanism.VIA_CLIENT:
            self._cancel(cursor, report)
          else:
            # Wait until the query is cancelled by a different mechanism.
            self._wait_until_cancelled(cursor, report.query_id)
          report.timed_out = True
          return report
        except Exception as error:
          report.query_id = op_handle_to_query_id(cursor._last_operation.handle if
                                                  cursor._last_operation else None)
          LOG.debug("Error running query with id %s: %s", report.query_id, error)
          if (cancel_mech == CancelMechanism.VIA_OPTION and
                is_exec_time_limit_error(error)):
            # Timeout via query option counts as a timeout.
            report.timed_out = True
          else:
            self._check_for_memory_errors(report, cursor, error)
        if report.has_query_error():
          return report
        report.runtime_secs = time() - start_time
        if cursor.execution_failed() or self.check_if_mem_was_spilled:
          fetch_and_set_profile(cursor, report)
          report.mem_was_spilled = any([
              pattern.search(report.profile) is not None
              for pattern in QueryRunner.SPILLED_PATTERNS])
          # TODO: is this needed? Memory errors are generally caught by the try/except.
          report.not_enough_memory = "Memory limit exceeded" in report.profile
    except Exception as error:
      # A mem limit error would have been caught above, no need to check for that here.
      LOG.debug("Caught error", error)
      report.other_error = error
    return report

  def _set_db_and_options(self, cursor, query, run_set_up, mem_limit_mb, timeout_secs,
      cancel_mech):
    """Set up a new cursor for running a query by switching to the correct database and
    setting query options."""
    if query.db_name:
      LOG.debug("Using %s database", query.db_name)
      cursor.execute("USE %s" % query.db_name)
    if run_set_up and query.set_up_sql:
      LOG.debug("Running set up query:\n%s", query.set_up_sql)
      cursor.execute(query.set_up_sql)
    for query_option, value in self.common_query_options.items():
      cursor.execute(
          "SET {query_option}={value}".format(query_option=query_option, value=value))
    for query_option, value in query.options.items():
      cursor.execute(
          "SET {query_option}={value}".format(query_option=query_option, value=value))
    # Set a time limit if it is the expected method of cancellation, or as an additional
    # method of cancellation if the query runs for too long. This is useful if, e.g.
    # if the client is blocked in a fetch() call and can't initiate the cancellation.
    if not cancel_mech or cancel_mech == CancelMechanism.VIA_OPTION:
      # EXEC_TIME_LIMIT_S is an int32, so we need to cap the value.
      cursor.execute("SET EXEC_TIME_LIMIT_S={timeout_secs}".format(
          timeout_secs=min(timeout_secs, 2**31 - 1)))
    cursor.execute("SET ABORT_ON_ERROR=1")
    if self.test_admission_control:
      LOG.debug(
          "Running query without mem limit at %s with timeout secs %s:\n%s",
          self.impalad.host_name, timeout_secs, query.sql)
    else:
      LOG.debug("Setting mem limit to %s MB", mem_limit_mb)
      cursor.execute("SET MEM_LIMIT=%sM" % mem_limit_mb)
      LOG.debug(
          "Running query with %s MB mem limit at %s with timeout secs %s:\n%s",
          mem_limit_mb, self.impalad.host_name, timeout_secs, query.sql)

  def _wait_until_fetchable(self, cursor, report, timeout_unix_time):
    """Wait up until timeout_unix_time until the query results can be fetched (if it's
    a SELECT query) or until it has finished executing (if it's a different query type
    like DML). If the timeout expires raises a QueryTimeout exception."""
    # Loop until the query gets to the right state or a timeout expires.
    sleep_secs = 0.1
    secs_since_log = 0
    # True if we incremented num_queries_started_running_or_cancelled for this query.
    started_running_or_cancelled = False
    while True:
      query_state = cursor.status()
      # Check if the query got past the PENDING/INITIALIZED states, either because
      # it's executing or hit an error.
      if (not started_running_or_cancelled and query_state not in ('PENDING_STATE',
                                                      'INITIALIZED_STATE')):
        started_running_or_cancelled = True
        increment(self._metrics[NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED])
      # Return if we're ready to fetch results (in the FINISHED state) or we are in
      # another terminal state like EXCEPTION.
      if query_state not in ('PENDING_STATE', 'INITIALIZED_STATE', 'RUNNING_STATE'):
        return

      if time() > timeout_unix_time:
        if not started_running_or_cancelled:
          increment(self._metrics[NUM_QUERIES_STARTED_RUNNING_OR_CANCELLED])
        raise QueryTimeout()
      if secs_since_log > 5:
        secs_since_log = 0
        LOG.debug("Waiting for query to execute")
      sleep(sleep_secs)
      secs_since_log += sleep_secs

  def _wait_until_cancelled(self, cursor, query_id):
    """Wait until 'cursor' is in a CANCELED or ERROR status."""
    last_log_time = 0
    status = cursor.status()
    while status not in ["CANCELED_STATE", "ERROR_STATE"]:
      # Work around IMPALA-7561: queries don't transition out of FINISHED state once they
      # hit eos.
      if status == "FINISHED_STATE" and "Last row fetched" in cursor.get_profile():
        return
      if time() - last_log_time > 5:
        LOG.debug("Waiting for query with id {query_id} to be cancelled".format(
                  query_id=query_id))
        last_log_time = time()
      sleep(0.1)
      status = cursor.status()

  def update_from_query_report(self, report):
    LOG.debug("Updating runtime stats (Query Runner PID: {0})".format(self.proc.pid))
    increment(self._metrics[NUM_QUERIES_FINISHED])
    if report.not_enough_memory:
      increment(self._metrics[NUM_QUERIES_EXCEEDED_MEM_LIMIT])
    if report.ac_rejected:
      increment(self._metrics[NUM_QUERIES_AC_REJECTED])
    if report.ac_timedout:
      increment(self._metrics[NUM_QUERIES_AC_TIMEDOUT])
    if report.was_cancelled:
      increment(self._metrics[NUM_QUERIES_CANCELLED])

  def _cancel(self, cursor, report):
    if not report.query_id:
      return

    try:
      LOG.debug("Attempting cancellation of query with id %s", report.query_id)
      cursor.cancel_operation()
      LOG.debug("Sent cancellation request for query with id %s", report.query_id)
    except Exception as e:
      LOG.debug("Error cancelling query with id %s: %s", report.query_id, e)
      try:
        LOG.debug("Attempting to cancel query through the web server.")
        self.impalad.cancel_query(report.query_id)
      except Exception as e:
        LOG.debug("Error cancelling query %s through the web server: %s",
                  report.query_id, e)

  def _check_for_memory_errors(self, report, cursor, caught_exception):
    """To be called after a query failure to check for signs of failed due to a
    mem limit or admission control rejection/timeout. The report will be updated
    accordingly.
    """
    fetch_and_set_profile(cursor, report)
    caught_msg = str(caught_exception).lower().strip()
    # Distinguish error conditions based on string fragments. The AC rejection and
    # out-of-memory conditions actually overlap (since some memory checks happen in
    # admission control) so check the out-of-memory conditions first.
    if "memory limit exceeded" in caught_msg or \
       "repartitioning did not reduce the size of a spilled partition" in caught_msg or \
       "failed to get minimum memory reservation" in caught_msg or \
       "minimum memory reservation is greater than" in caught_msg or \
       "minimum memory reservation needed is greater than" in caught_msg:
      report.not_enough_memory = True
      return
    if "rejected query from pool" in caught_msg:
      report.ac_rejected = True
      return
    if "admission for query exceeded timeout" in caught_msg:
      report.ac_timedout = True
      return

    LOG.debug("Non-mem limit error for query with id %s: %s", report.query_id,
              caught_exception, exc_info=True)
    report.other_error = caught_exception

  def _fetch_and_hash_result(self, cursor, timeout_unix_time, query):
    """Fetches results from 'cursor' and returns a hash that is independent of row order.
    Raises QueryTimeout() if we couldn't fetch all rows from the query before time()
    reaches 'timeout_unix_time'.
    'query' is only used for debug logging purposes (if the result is not as expected a
    log file will be left in RESULTS_DIR for investigation).
    """
    query_id = op_handle_to_query_id(cursor._last_operation.handle if
                                     cursor._last_operation else None)

    result_log_filename = None
    curr_hash = 1
    try:
      with self._open_result_file(query, query_id) as result_log:
        result_log.write(query.sql)
        result_log.write("\n")
        result_log_filename = result_log.name
        while True:
          # Check for timeout before each fetch call. Fetching can block indefinitely,
          # e.g. if the query is a selective scan, so we may miss the timeout, but we
          # will live with that limitation for now.
          # TODO: IMPALA-8289: use a non-blocking fetch when support is added
          if time() >= timeout_unix_time:
            LOG.debug("Hit timeout before fetch for query with id {0} {1} >= {2}".format(
                query_id, time(), timeout_unix_time))
            raise QueryTimeout()
          LOG.debug("Fetching result for query with id {0}".format(query_id))
          rows = cursor.fetchmany(self.BATCH_SIZE)
          if not rows:
            LOG.debug("No more results for query with id {0}".format(query_id))
            break
          for row in rows:
            curr_hash = _add_row_to_hash(row, curr_hash)
            if result_log:
              result_log.write(",".join([str(val) for val in row]))
              result_log.write("\thash=")
              result_log.write(str(curr_hash))
              result_log.write("\n")
    except Exception:
      # Don't retain result files for failed queries.
      if result_log_filename is not None:
        os.remove(result_log_filename)
      raise

    # Only retain result files with incorrect results.
    if result_log_filename is not None and curr_hash == query.result_hash:
      os.remove(result_log_filename)
    return curr_hash

  def _open_result_file(self, query, query_id):
    """Opens and returns a file under RESULT_HASHES_DIR to write query results to."""
    file_name = '_'.join([query.logical_query_id, query_id.replace(":", "_")])
    if query.result_hash is None:
      file_name += "_initial"
    file_name += "_results.txt"
    result_log = open(os.path.join(self.results_dir, RESULT_HASHES_DIR, file_name),
                      "w")
    return result_log

  def get_metric_val(self, name):
    """Get the current value of the metric called 'name'."""
    return self._metrics[name].value

  def get_metric_vals(self):
    """Get the current values of the all metrics as a list of (k, v) pairs."""
    return [(k, v.value) for k, v in self._metrics.items()]

  def increment_metric(self, name):
    """Increment the current value of the metric called 'name'."""
    increment(self._metrics[name])


class QueryReport(object):
  """Holds information about a single query run."""

  def __init__(self, query):
    self.query = query

    self.result_hash = None
    self.runtime_secs = None
    self.mem_was_spilled = False
    # not_enough_memory includes conditions like "Memory limit exceeded", admission
    # control rejecting because not enough memory, etc.
    self.not_enough_memory = False
    # ac_rejected is true if the query was rejected by admission control.
    # It is mutually exclusive with not_enough_memory - if the query is rejected by
    # admission control because the memory limit is too low, it is counted as
    # not_enough_memory.
    # TODO: reconsider whether they should be mutually exclusive
    self.ac_rejected = False
    self.ac_timedout = False
    self.other_error = None
    self.timed_out = False
    self.was_cancelled = False
    self.profile = None
    self.query_id = None

  def __str__(self):
    return dedent("""
        <QueryReport
        result_hash: %(result_hash)s
        runtime_secs: %(runtime_secs)s
        mem_was_spilled: %(mem_was_spilled)s
        not_enough_memory: %(not_enough_memory)s
        ac_rejected: %(ac_rejected)s
        ac_timedout: %(ac_timedout)s
        other_error: %(other_error)s
        timed_out: %(timed_out)s
        was_cancelled: %(was_cancelled)s
        query_id: %(query_id)s
        >
        """.strip() % self.__dict__)

  def has_query_error(self):
    """Return true if any kind of error status was returned from the query (i.e.
    the query didn't run to completion, time out or get cancelled)."""
    return (self.not_enough_memory or self.ac_rejected or self.ac_timedout
            or self.other_error)

  def write_query_profile(self, directory, prefix=None):
    """
    Write out the query profile bound to this object to a given directory.

    The file name is generated and will contain the query ID. Use the optional prefix
    parameter to set a prefix on the filename.

    Example return:
      tpcds_300_decimal_parquet_q21_00000001_a38c8331_profile.txt

    Parameters:
      directory (str): Directory to write profile.
      prefix (str): Prefix for filename.
    """
    if not (self.profile and self.query_id):
      return
    if prefix is not None:
      file_name = prefix + '_'
    else:
      file_name = ''
    file_name += self.query.logical_query_id + '_'
    file_name += self.query_id.replace(":", "_") + "_profile.txt"
    profile_log_path = os.path.join(directory, file_name)
    with open(profile_log_path, "w") as profile_log:
      profile_log.write(self.profile)


def _add_row_to_hash(row, curr_hash):
  """Hashes row and accumulates it with 'curr_hash'. The hash is invariant of the
  order in which rows are added to the hash, since many queries we run through
  the stress test do not have a canonical order in which they return rows."""
  for idx, val in enumerate(row):
    curr_hash += _hash_val(idx, val)
    # Modulo the result to keep it "small" otherwise the math ops can be slow
    # since python does infinite precision math.
    curr_hash %= maxsize
  return curr_hash


def _hash_val(col_idx, val):
  """Compute a numeric hash of 'val', which is at column index 'col_idx'."""
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
  return (col_idx + 1) * hash(val)


def fetch_and_set_profile(cursor, report):
  """Set the report's query profile using the given cursor.
  Producing a query profile can be somewhat expensive. A v-tune profile of
  impalad showed 10% of cpu time spent generating query profiles.
  """
  if not report.profile and cursor._last_operation:
    try:
      report.profile = cursor.get_profile()
    except Exception as e:
      LOG.debug("Error getting profile for query with id %s: %s", report.query_id, e)


def is_exec_time_limit_error(error):
  """Return true if this is an error hit as a result of hitting EXEC_TIME_LIMIT_S."""
  return "expired due to execution time limit" in str(error)
