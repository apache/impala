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

# Tests admission control

import itertools
import logging
import os
import pytest
import re
import sys
import threading
from time import sleep, time

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import specific_build_type_timeout, IMPALAD_BUILD
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.test_vector import ImpalaTestDimension
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from ImpalaService import ImpalaHiveServer2Service
from TCLIService import TCLIService

LOG = logging.getLogger('admission_test')

# The query used for testing. It is important that this query returns many rows
# while keeping fragments active on all backends. This allows a thread to keep
# the query active and consuming resources by fetching one row at a time. The
# where clause is for debugging purposes; each thread will insert its id so
# that running queries can be correlated with the thread that submitted them.
QUERY = """
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
union all
select * from alltypesagg where id != {0}
"""

# The statestore heartbeat and topic update frequency (ms). Set low for testing.
STATESTORE_RPC_FREQUENCY_MS = 100

# Time to sleep (in milliseconds) between issuing queries. When the delay is at least
# the statestore heartbeat frequency, all state should be visible by every impalad by
# the time the next query is submitted. Otherwise the different impalads will see stale
# state for some admission decisions.
SUBMISSION_DELAY_MS = \
    [0, STATESTORE_RPC_FREQUENCY_MS / 2, STATESTORE_RPC_FREQUENCY_MS * 3 / 2]

# The number of queries to submit. The test does not support fewer queries than
# MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES to keep some validation logic
# simple.
NUM_QUERIES = [15, 30, 50]

# Whether we will submit queries to all available impalads (in a round-robin fashion)
ROUND_ROBIN_SUBMISSION = [True, False]

# The query pool to use. The impalads should be configured to recognize this
# pool with the parameters below.
POOL_NAME = "default-pool"

# Stress test timeout (seconds). The timeout needs to be significantly higher for
# slow builds like code coverage and ASAN (IMPALA-3790, IMPALA-6241).
STRESS_TIMEOUT = specific_build_type_timeout(60, slow_build_timeout=600)

# The number of queries that can execute concurrently in the pool POOL_NAME.
MAX_NUM_CONCURRENT_QUERIES = 5

# The number of queries that can be queued in the pool POOL_NAME
MAX_NUM_QUEUED_QUERIES = 10

# Mem limit (bytes) used in the mem limit test
MEM_TEST_LIMIT = 12 * 1024 * 1024 * 1024

_STATESTORED_ARGS = ("-statestore_heartbeat_frequency_ms={freq_ms} "
                     "-statestore_priority_update_frequency_ms={freq_ms}").format(
                    freq_ms=STATESTORE_RPC_FREQUENCY_MS)

# Name of the subscriber metric tracking the admission control update interval.
REQUEST_QUEUE_UPDATE_INTERVAL =\
    'statestore-subscriber.topic-impala-request-queue.update-interval'

# Key in the query profile for the query options.
PROFILE_QUERY_OPTIONS_KEY = "Query Options (set by configuration): "

# The different ways that a query thread can end its query.
QUERY_END_BEHAVIORS = ['EOS', 'CLIENT_CANCEL', 'QUERY_TIMEOUT', 'CLIENT_CLOSE']

# The timeout used for the QUERY_TIMEOUT end behaviour
QUERY_END_TIMEOUT_S = 1

def impalad_admission_ctrl_flags(max_requests, max_queued, pool_max_mem,
    proc_mem_limit = None, queue_wait_timeout_ms=None):
  extra_flags = ""
  if proc_mem_limit is not None:
    extra_flags += " -mem_limit={0}".format(proc_mem_limit)
  if queue_wait_timeout_ms is not None:
    extra_flags += " -queue_wait_timeout_ms={0}".format(queue_wait_timeout_ms)
  return ("-vmodule admission-controller=3 -default_pool_max_requests {0} "
      "-default_pool_max_queued {1} -default_pool_mem_limit {2} "
      "-disable_admission_control=false {3}".format(
      max_requests, max_queued, pool_max_mem, extra_flags))


def impalad_admission_ctrl_config_args(additional_args=""):
  impalad_home = os.environ['IMPALA_HOME']
  resources_dir = os.path.join(impalad_home, "fe", "src", "test", "resources")
  fs_allocation_path = os.path.join(resources_dir, "fair-scheduler-test2.xml")
  llama_site_path = os.path.join(resources_dir, "llama-site-test2.xml")
  return ("-vmodule admission-controller=3 -fair_scheduler_allocation_path %s "
        "-llama_site_path %s -disable_admission_control=false %s" %\
        (fs_allocation_path, llama_site_path, additional_args))

def log_metrics(log_prefix, metrics, log_level=logging.DEBUG):
  LOG.log(log_level, "%sadmitted=%s, queued=%s, dequeued=%s, rejected=%s, "\
      "released=%s, timed-out=%s", log_prefix, metrics['admitted'], metrics['queued'],
      metrics['dequeued'], metrics['rejected'], metrics['released'],
      metrics['timed-out'])

def compute_metric_deltas(m2, m1):
  """Returns a dictionary of the differences of metrics in m2 and m1 (m2 - m1)"""
  return dict((n, m2.get(n, 0) - m1.get(n, 0)) for n in m2.keys())

def metric_key(pool_name, metric_name):
  """Helper method to construct the admission controller metric keys"""
  return "admission-controller.%s.%s" % (metric_name, pool_name)

class TestAdmissionControllerBase(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerBase, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # There's no reason to test this on other file formats/compression codecs right now
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

class TestAdmissionController(TestAdmissionControllerBase, HS2TestSuite):
  def __check_pool_rejected(self, client, pool, expected_error_re):
    try:
      client.set_configuration({'request_pool': pool})
      client.execute("select 1")
      assert False, "Query should return error"
    except ImpalaBeeswaxException as e:
      assert re.search(expected_error_re, str(e))

  def __check_query_options(self, profile, expected_query_options):
    """Validate that the expected per-pool query options were set on the specified
    profile. expected_query_options is a list of "KEY=VALUE" strings, e.g.
    ["MEM_LIMIT=1", ...]"""
    confs = []
    for line in profile.split("\n"):
      if PROFILE_QUERY_OPTIONS_KEY in line:
        rhs = re.split(": ", line)[1]
        confs = re.split(",", rhs)
        break
    assert len(confs) == len(expected_query_options)
    confs = map(str.lower, confs)
    for expected in expected_query_options:
      if expected.lower() not in confs:
        expected = ",".join(sorted(expected_query_options))
        actual = ",".join(sorted(confs))
        assert False, "Expected query options %s, got %s." % (expected, actual)

  def __check_hs2_query_opts(self, pool_name, mem_limit=None, expected_options=None):
    """ Submits a query via HS2 (optionally with a mem_limit in the confOverlay)
        into pool_name and checks that the expected_query_options are set in the
        profile."""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.confOverlay = {'request_pool': pool_name}
    if mem_limit is not None: execute_statement_req.confOverlay['mem_limit'] = mem_limit
    execute_statement_req.statement = "select 1";
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    HS2TestSuite.check_response(self.hs2_client.CloseOperation(close_operation_req))

    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    HS2TestSuite.check_response(get_profile_resp)
    self.__check_query_options(get_profile_resp.profile, expected_options)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(\
          "-default_query_options=mem_limit=200000000"),
      statestored_args=_STATESTORED_ARGS)
  @needs_session(conf_overlay={'batch_size': '100'})
  def test_set_request_pool(self):
    """Tests setting the REQUEST_POOL with the pool placement policy configured
    to require a specific pool, and validate that the per-pool configurations were
    applied."""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    # Expected default mem limit for queueA, used in several tests below
    queueA_mem_limit = "MEM_LIMIT=%s" % (128*1024*1024)
    try:
      for pool in ['', 'not_a_pool_name']:
        expected_error =\
            "No mapping found for request from user '\S+' with requested pool '%s'"\
            % (pool)
        self.__check_pool_rejected(client, pool, expected_error)

      # Check rejected if user does not have access.
      expected_error = "Request from user '\S+' with requested pool 'root.queueC' "\
          "denied access to assigned pool 'root.queueC'"
      self.__check_pool_rejected(client, 'root.queueC', expected_error)

      # Also try setting a valid pool
      client.set_configuration({'request_pool': 'root.queueB'})
      result = client.execute("select 1")
      # Query should execute in queueB which doesn't have a default mem limit set in the
      # llama-site.xml, so it should inherit the value from the default process query
      # options.
      self.__check_query_options(result.runtime_profile,\
          ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB'])

      # Try setting the pool for a queue with a very low queue timeout.
      # queueA allows only 1 running query and has a queue timeout of 50ms, so the
      # second concurrent query should time out quickly.
      client.set_configuration({'request_pool': 'root.queueA'})
      handle = client.execute_async("select sleep(1000)")
      self.__check_pool_rejected(client, 'root.queueA', "exceeded timeout")
      assert client.get_state(handle) == client.QUERY_STATES['FINISHED']
      # queueA has default query options mem_limit=128m,query_timeout_s=5
      self.__check_query_options(client.get_runtime_profile(handle),\
          [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA'])
      client.close_query(handle)

      # Should be able to set query options via the set command (overriding defaults if
      # applicable). mem_limit overrides the pool default. abort_on_error has no
      # proc/pool default.
      client.execute("set mem_limit=31337")
      client.execute("set abort_on_error=1")
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,\
          ['MEM_LIMIT=31337', 'ABORT_ON_ERROR=1', 'QUERY_TIMEOUT_S=5',\
           'REQUEST_POOL=root.queueA'])

      # Should be able to set query options (overriding defaults if applicable) with the
      # config overlay sent with the query RPC. mem_limit is a pool-level override and
      # max_io_buffers has no proc/pool default.
      client.set_configuration({'request_pool': 'root.queueA', 'mem_limit': '12345'})
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,\
          ['MEM_LIMIT=12345', 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA',\
           'ABORT_ON_ERROR=1'])

      # Once options are reset to their defaults, the queue
      # configuration should kick back in. We'll see the
      # queue-configured mem_limit, and we won't see
      # abort on error, because it's back to being the default.
      client.execute('set mem_limit=""')
      client.execute('set abort_on_error=""')
      client.set_configuration({ 'request_pool': 'root.queueA' })
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,
            [queueA_mem_limit, 'REQUEST_POOL=root.queueA', 'QUERY_TIMEOUT_S=5'])

    finally:
      client.close()

    # HS2 tests:
    # batch_size is set in the HS2 OpenSession() call via the requires_session() test
    # decorator, so that is included in all test cases below.
    batch_size = "BATCH_SIZE=100"

    # Check HS2 query in queueA gets the correct query options for the pool.
    self.__check_hs2_query_opts("root.queueA", None,\
        [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check overriding the mem limit sent in the confOverlay with the query.
    self.__check_hs2_query_opts("root.queueA", '12345',\
        ['MEM_LIMIT=12345', 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check HS2 query in queueB gets the process-wide default query options
    self.__check_hs2_query_opts("root.queueB", None,\
        ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB', batch_size])

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args("-require_username"),
      statestored_args=_STATESTORED_ARGS)
  def test_require_user(self):
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = ""
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestAdmissionController.check_response(open_session_resp)

    try:
      execute_statement_req = TCLIService.TExecuteStatementReq()
      execute_statement_req.sessionHandle = open_session_resp.sessionHandle
      execute_statement_req.statement = "select count(1) from functional.alltypes"
      execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
      TestAdmissionController.check_response(execute_statement_resp,
          TCLIService.TStatusCode.ERROR_STATUS, "User must be specified")
    finally:
      close_req = TCLIService.TCloseSessionReq()
      close_req.sessionHandle = open_session_resp.sessionHandle
      TestAdmissionController.check_response(self.hs2_client.CloseSession(close_req))


  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=10 * 1024 * 1024, proc_mem_limit=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_trivial_coord_query_limits(self):
    """Tests that trivial coordinator only queries have negligible resource requirements.
    """
    # Queries with only constant exprs or limit 0 should be admitted.
    self.execute_query_expect_success(self.client, "select 1")
    self.execute_query_expect_success(self.client,
        "select * from functional.alltypes limit 0")

    non_trivial_queries = [
        "select * from functional.alltypesagg limit 1",
        "select * from functional.alltypestiny"]
    for query in non_trivial_queries:
      ex = self.execute_query_expect_failure(self.client, query)
      assert re.search("Rejected query from pool default-pool: request memory needed "
          ".* is greater than pool max mem resources 10.00 MB", str(ex))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=10 * 1024 * 1024, proc_mem_limit=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_reject_min_reservation(self, vector):
    """Test that the query will be rejected by admission control if:
       a) the largest per-backend min buffer reservation is larger than the query mem
          limit
       b) the largest per-backend min buffer reservation is larger than the
          buffer_pool_limit query option
       c) the cluster-wide min-buffer reservation size is larger than the pool memory
          resources.
    """
    self.run_test_case('QueryTest/admission-reject-min-reservation', vector)

  # Process mem_limit used in test_mem_limit_upper_bound
  PROC_MEM_TEST_LIMIT = 1024 * 1024 * 1024

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=10 * PROC_MEM_TEST_LIMIT, proc_mem_limit=PROC_MEM_TEST_LIMIT))
  def test_mem_limit_upper_bound(self, vector):
    """ Test to ensure that a query is admitted if the requested memory is equal to the
    process mem limit"""
    query = "select * from functional.alltypesagg limit 1"
    exec_options = vector.get_value('exec_option')
    # Setting requested memory equal to process memory limit
    exec_options['mem_limit'] = self.PROC_MEM_TEST_LIMIT
    self.execute_query_expect_success(self.client, query, exec_options)

class TestAdmissionControllerStress(TestAdmissionControllerBase):
  """Submits a number of queries (parameterized) with some delay between submissions
  (parameterized) and the ability to submit to one impalad or many in a round-robin
  fashion. Each query is submitted on a separate thread. After admission, the query
  thread will block with the query open and wait for the main thread to notify it to
  end its query. The query thread can end its query by fetching to the end, cancelling
  itself, closing itself, or waiting for the query timeout to take effect. Depending
  on the test parameters a varying number of queries will be admitted, queued, and
  rejected. After the queries are admitted, the main thread will request each admitted
  query thread to end its query and allow queued queries to be admitted.

  The test tracks the state of the admission controller using the metrics from each
  impalad to do the following:
  (1) After submitting all queries, the change in metrics for the number of admitted,
      queued, and rejected requests should sum to the number of queries and that the
      values are reasonable given the test parameters.
  (2) While there are running queries:
      * Request the currently running queries to end and wait for the queries to end.
        Verify the metric for the number of completed queries. The threads that
        submitted those queries will keep their connections open until the entire test
        completes. This verifies that admission control is tied to the end of the query
        and does not depend on closing the connection.
      * Check that queued requests are then dequeued and verify using the metric for the
        number of dequeued requests. The threads that were waiting to submit the query
        should then insert themselves into a list of currently running queries and then
        wait for a notification from the main thread.
  (3) After all queries have completed, check that the final number of admitted,
      queued, and rejected requests are reasonable given the test parameters. When
      submitting to a single impalad, we know exactly what the values should be,
      otherwise we just check that they are within reasonable bounds.
  """
  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerStress, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('num_queries', *NUM_QUERIES))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('round_robin_submission', *ROUND_ROBIN_SUBMISSION))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('submission_delay_ms', *SUBMISSION_DELAY_MS))

    # Additional constraints for code coverage jobs and core.
    num_queries = None
    if IMPALAD_BUILD.has_code_coverage():
      # Code coverage builds can't handle the increased concurrency.
      num_queries = 15
    elif cls.exploration_strategy() == 'core':
      num_queries = 30
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('submission_delay_ms') == 0)
      cls.ImpalaTestMatrix.add_constraint(\
          lambda v: v.get_value('round_robin_submission') == True)

    if num_queries is not None:
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('num_queries') == num_queries)

  def setup(self):
    # All threads are stored in this list and it's used just to make sure we clean up
    # properly in teardown.
    self.all_threads = list()
    # Each submission thread will append() itself to this list if the query begins
    # execution.  The main thread will access this list to determine which threads are
    # executing queries that can be cancelled (it will pop() elements from the front of
    # the list). The individual operations on the list are atomic and thread-safe thanks
    # to the GIL.
    self.executing_threads = list()

  def teardown(self):
    # Set shutdown for all threads (cancel if needed)
    for thread in self.all_threads:
      try:
        thread.lock.acquire()
        thread.shutdown = True
        if thread.query_handle is not None:
          LOG.debug("Attempt to clean up thread executing query %s (state %s)",
              thread.query_num, thread.query_state)
          client = thread.impalad.service.create_beeswax_client()
          try:
            client.cancel(thread.query_handle)
          finally:
            client.close()
      finally:
        thread.lock.release()

    # Wait for all threads to exit
    for thread in self.all_threads:
      thread.join(5)
      LOG.debug("Join thread for query num %s %s", thread.query_num,
          "TIMED OUT" if thread.isAlive() else "")

  def get_admission_metrics(self):
    """
    Returns a map of the admission metrics, aggregated across all of the impalads.

    The metrics names are shortened for brevity: 'admitted', 'queued', 'dequeued',
    'rejected', 'released', and 'timed-out'.
    """
    metrics = {'admitted': 0, 'queued': 0, 'dequeued': 0, 'rejected' : 0,
        'released': 0, 'timed-out': 0}
    for impalad in self.impalads:
      for short_name in metrics.keys():
        metrics[short_name] += impalad.service.get_metric_value(\
            metric_key(self.pool_name, 'total-%s' % short_name), 0)
    return metrics

  def wait_for_metric_changes(self, metric_names, initial, expected_delta):
    """
    Waits for the sum of metrics in metric_names to change by at least expected_delta.

    This is similar to ImpalaService.wait_for_metric_value(), but it uses one or more
    metrics aggregated across all impalads, e.g. we want to wait for the total number of
    admitted, queued, and rejected metrics to change some amount in total, but we don't
    know exactly how the metrics will change individually.
    'metric_names' is a list of the keys returned by get_admission_metrics() which are
    expected to change.
    'initial' is the initial set of metrics returned by get_admission_metrics() to
    compare against.
    'expected_delta' is the total change expected across all impalads for the specified
    metrics.
    """
    log_metrics("wait_for_metric_changes, initial=", initial)
    current = initial
    start_time = time()
    while True:
      current = self.get_admission_metrics()
      log_metrics("wait_for_metric_changes, current=", current)
      deltas = compute_metric_deltas(current, initial)
      delta_sum = sum([ deltas[x] for x in metric_names ])
      LOG.debug("DeltaSum=%s Deltas=%s (Expected=%s for metrics=%s)",\
          delta_sum, deltas, expected_delta, metric_names)
      if delta_sum >= expected_delta:
        LOG.debug("Found all %s metrics after %s seconds", delta_sum,
            round(time() - start_time, 1))
        return (deltas, current)
      assert (time() - start_time < STRESS_TIMEOUT),\
          "Timed out waiting {0} seconds for metrics {1} delta {2} "\
          "current {3} initial {4}" .format(
              STRESS_TIMEOUT, ','.join(metric_names), expected_delta, str(current), str(initial))
      sleep(1)

  def wait_for_statestore_updates(self, heartbeats):
    """Waits for a number of admission control statestore updates from all impalads."""
    start_time = time()
    num_impalads = len(self.impalads)
    init = dict()
    curr = dict()
    for impalad in self.impalads:
      init[impalad] = impalad.service.get_metric_value(
          REQUEST_QUEUE_UPDATE_INTERVAL)['count']
      curr[impalad] = init[impalad]

    while True:
      LOG.debug("wait_for_statestore_updates: curr=%s, init=%s, d=%s", curr.values(),
          init.values(), [curr[i] - init[i] for i in self.impalads])
      if all([curr[i] - init[i] >= heartbeats for i in self.impalads]): break
      for impalad in self.impalads:
        curr[impalad] = impalad.service.get_metric_value(
            REQUEST_QUEUE_UPDATE_INTERVAL)['count']
      assert (time() - start_time < STRESS_TIMEOUT),\
          "Timed out waiting %s seconds for heartbeats" % (STRESS_TIMEOUT,)
      sleep(STATESTORE_RPC_FREQUENCY_MS / float(1000))
    LOG.debug("Waited %s for %s heartbeats", round(time() - start_time, 1), heartbeats)

  def wait_for_admitted_threads(self, num_threads):
    """
    Wait for query submission threads to update after being admitted, as determined
    by observing metric changes. This is necessary because the metrics may change
    before the execute_async() calls on the query threads return and add themselves
    to self.executing_threads.
    """
    start_time = time()
    LOG.debug("Waiting for %s threads to begin execution", num_threads)
    # All individual list operations are thread-safe, so we don't need to use a
    # lock to synchronize before checking the list length (on which another thread
    # may call append() concurrently).
    while len(self.executing_threads) < num_threads:
      assert (time() - start_time < STRESS_TIMEOUT), ("Timed out waiting %s seconds for "
          "%s admitted client rpcs to return. Only %s executing " % (
          STRESS_TIMEOUT, num_threads, len(self.executing_threads)))
      sleep(0.1)
    LOG.debug("Found all %s admitted threads after %s seconds", num_threads,
        round(time() - start_time, 1))

  def end_admitted_queries(self, num_queries):
    """
    Requests each admitted query to end its query.
    """
    assert len(self.executing_threads) >= num_queries
    LOG.debug("Requesting {0} clients to end queries".format(num_queries))

    # Request admitted clients to end their queries
    current_executing_queries = []
    for i in xrange(num_queries):
      # pop() is thread-safe, it's OK if another thread is appending concurrently.
      thread = self.executing_threads.pop(0)
      LOG.debug("Cancelling query %s", thread.query_num)
      assert thread.query_state == 'ADMITTED'
      current_executing_queries.append(thread)
      thread.query_state = 'REQUEST_QUERY_END'

    # Wait for the queries to end
    start_time = time()
    while True:
      all_done = True
      for thread in self.all_threads:
        if thread.query_state == 'REQUEST_QUERY_END':
          all_done = False
      if all_done:
        break
      assert (time() - start_time < STRESS_TIMEOUT),\
        "Timed out waiting %s seconds for query end" % (STRESS_TIMEOUT,)
      sleep(1)

  class SubmitQueryThread(threading.Thread):
    def __init__(self, impalad, additional_query_options, vector, query_num,
        query_end_behavior, executing_threads):
      """
      executing_threads must be provided so that this thread can add itself when the
      query is admitted and begins execution.
      """
      super(self.__class__, self).__init__()
      self.executing_threads = executing_threads
      self.vector = vector
      self.additional_query_options = additional_query_options
      self.query_num = query_num
      self.query_end_behavior = query_end_behavior
      self.impalad = impalad
      self.error = None
      # query_state is defined and used only by the test code, not a property exposed by
      # the server
      self.query_state = 'NOT_SUBMITTED'

      # lock protects query_handle and shutdown, used by the main thread in teardown()
      self.lock = threading.RLock()
      self.query_handle = None
      self.shutdown = False # Set by the main thread when tearing down

    def run(self):
      client = None
      try:
        try:
          # Take the lock while query_handle is being created to avoid an unlikely race
          # condition with teardown() (i.e. if an error occurs on the main thread), and
          # check if the test is already shut down.
          self.lock.acquire()
          if self.shutdown:
            return

          exec_options = self.vector.get_value('exec_option')
          exec_options.update(self.additional_query_options)
          query = QUERY.format(self.query_num)
          self.query_state = 'SUBMITTING'
          client = self.impalad.service.create_beeswax_client()
          ImpalaTestSuite.change_database(client, self.vector.get_value('table_format'))
          client.set_configuration(exec_options)

          if self.query_end_behavior == 'QUERY_TIMEOUT':
            client.execute("SET QUERY_TIMEOUT_S={0}".format(QUERY_END_TIMEOUT_S))

          LOG.debug("Submitting query %s", self.query_num)
          self.query_handle = client.execute_async(query)
        except ImpalaBeeswaxException as e:
          if re.search("Rejected.*queue full", str(e)):
            LOG.debug("Rejected query %s", self.query_num)
            self.query_state = 'REJECTED'
            return
          elif "exceeded timeout" in str(e):
            LOG.debug("Query %s timed out", self.query_num)
            self.query_state = 'TIMED OUT'
            return
          else:
            raise e
        finally:
          self.lock.release()
        LOG.debug("Admitted query %s", self.query_num)
        self.query_state = 'ADMITTED'
        # The thread becomes visible to the main thread when it is added to the
        # shared list of executing_threads. append() is atomic and thread-safe.
        self.executing_threads.append(self)

        # Synchronize with the main thread. At this point, the thread is executing a
        # query. It needs to wait until the main thread requests it to end its query.
        while not self.shutdown:
          # The QUERY_TIMEOUT needs to stay active until the main thread requests it
          # to end. Otherwise, the query may get cancelled early. Fetch rows 5 times
          # per QUERY_TIMEOUT interval to keep the query active.
          if self.query_end_behavior == 'QUERY_TIMEOUT' and \
             self.query_state != 'COMPLETED':
            fetch_result = client.fetch(query, self.query_handle, 1)
            assert len(fetch_result.data) == 1, str(fetch_result)
          if self.query_state == 'REQUEST_QUERY_END':
            self._end_query(client, query)
            # The query has released admission control resources
            self.query_state = 'COMPLETED'
            self.query_handle = None
          sleep(QUERY_END_TIMEOUT_S * 0.2)
      except Exception as e:
        LOG.exception(e)
        # Unknown errors will be raised later
        self.error = e
        self.query_state = 'ERROR'
      finally:
        LOG.debug("Thread terminating in state=%s", self.query_state)
        if client is not None:
          client.close()

    def _end_query(self, client, query):
      """Bring the query to the appropriate end state defined by self.query_end_behaviour.
      Returns once the query has reached that state."""
      LOG.debug("Ending query %s by %s",
          str(self.query_handle.get_handle()), self.query_end_behavior)
      if self.query_end_behavior == 'QUERY_TIMEOUT':
        # Sleep and wait for the query to be cancelled. The cancellation will
        # set the state to EXCEPTION.
        start_time = time()
        while (client.get_state(self.query_handle) != \
               client.QUERY_STATES['EXCEPTION']):
          assert (time() - start_time < STRESS_TIMEOUT),\
            "Timed out waiting %s seconds for query cancel" % (STRESS_TIMEOUT,)
          sleep(1)
      elif self.query_end_behavior == 'EOS':
        # Fetch all rows so we hit eos.
        client.fetch(query, self.query_handle)
      elif self.query_end_behavior == 'CLIENT_CANCEL':
        client.cancel(self.query_handle)
      else:
        assert self.query_end_behavior == 'CLIENT_CLOSE'
        client.close_query(self.query_handle)

  def _check_queries_page_resource_pools(self):
    """Checks that all queries in the '/queries' webpage json have the correct resource
    pool (this is called after all queries have been admitted, queued, or rejected, so
    they should already have the pool set), or no pool for queries that don't go through
    admission control."""
    for impalad in self.impalads:
      queries_json = impalad.service.get_debug_webpage_json('/queries')
      for query in itertools.chain(queries_json['in_flight_queries'], \
          queries_json['completed_queries']):
        if query['stmt_type'] == 'QUERY' or query['stmt_type'] == 'DML':
          assert query['last_event'] != 'Registered' and \
              query['last_event'] != 'Planning finished'
          assert query['resource_pool'] == self.pool_name
        else:
          assert query['resource_pool'] == ''

  def _get_queries_page_num_queued(self):
    """Returns the number of queries currently in the 'queued' state from the '/queries'
    webpage json"""
    num_queued = 0
    for impalad in self.impalads:
      queries_json = impalad.service.get_debug_webpage_json('/queries')
      for query in queries_json['in_flight_queries']:
        if query['last_event'] == 'Queued':
          num_queued += 1
    return num_queued

  def run_admission_test(self, vector, additional_query_options):
    LOG.debug("Starting test case with parameters: %s", vector)
    self.impalads = self.cluster.impalads
    round_robin_submission = vector.get_value('round_robin_submission')
    submission_delay_ms = vector.get_value('submission_delay_ms')
    if not round_robin_submission:
      self.impalads = [self.impalads[0]]

    num_queries = vector.get_value('num_queries')
    assert num_queries >= MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
    initial_metrics = self.get_admission_metrics();
    log_metrics("Initial metrics: ", initial_metrics);

    for query_num in xrange(num_queries):
      impalad = self.impalads[query_num % len(self.impalads)]
      query_end_behavior = QUERY_END_BEHAVIORS[query_num % len(QUERY_END_BEHAVIORS)]
      thread = self.SubmitQueryThread(impalad, additional_query_options, vector,
          query_num, query_end_behavior, self.executing_threads)
      thread.start()
      self.all_threads.append(thread)
      sleep(submission_delay_ms / 1000.0)

    # Wait for the admission control to make the initial admission decision for all of
    # the queries. They should either be admitted immediately, queued, or rejected.
    # The test query is chosen that it with remain active on all backends until the test
    # ends the query. This prevents queued queries from being dequeued in the background
    # without this thread explicitly ending them, so that the test can admit queries in
    # discrete waves.
    LOG.debug("Wait for initial admission decisions")
    (metric_deltas, curr_metrics) = self.wait_for_metric_changes(\
        ['admitted', 'queued', 'rejected'], initial_metrics, num_queries)
    # Also wait for the test threads that submitted the queries to start executing.
    self.wait_for_admitted_threads(metric_deltas['admitted'])

    # Check that the admission decisions are reasonable given the test parameters
    # The number of admitted and queued requests should be at least the configured limits
    # but less than or equal to those limits times the number of impalads.
    assert metric_deltas['dequeued'] == 0,\
        "Queued queries should not run until others are made to finish"
    assert metric_deltas['admitted'] >= MAX_NUM_CONCURRENT_QUERIES,\
        "Admitted fewer than expected queries"
    assert metric_deltas['admitted'] <= MAX_NUM_CONCURRENT_QUERIES * len(self.impalads),\
        "Admitted more than expected queries: at least one daemon over-admitted"
    assert metric_deltas['queued'] >=\
        min(num_queries - metric_deltas['admitted'], MAX_NUM_QUEUED_QUERIES),\
        "Should have queued more queries before rejecting them"
    assert metric_deltas['queued'] <= MAX_NUM_QUEUED_QUERIES * len(self.impalads),\
        "Queued too many queries: at least one daemon queued too many"
    assert metric_deltas['rejected'] + metric_deltas['admitted'] +\
        metric_deltas['queued'] == num_queries ,\
        "Initial admission decisions don't add up to {0}: {1}".format(
        num_queries, str(metric_deltas))
    initial_metric_deltas = metric_deltas

    # Like above, check that the count from the queries webpage json is reasonable.
    queries_page_num_queued = self._get_queries_page_num_queued()
    assert queries_page_num_queued >=\
        min(num_queries - metric_deltas['admitted'], MAX_NUM_QUEUED_QUERIES)
    assert queries_page_num_queued <= MAX_NUM_QUEUED_QUERIES * len(self.impalads)
    self._check_queries_page_resource_pools()

    # Admit queries in waves until all queries are done. A new wave of admission
    # is started by killing some of the running queries.
    while len(self.executing_threads) > 0:
      curr_metrics = self.get_admission_metrics();
      log_metrics("Main loop, curr_metrics: ", curr_metrics);
      num_to_end = len(self.executing_threads)
      LOG.debug("Main loop, will request %s queries to end", num_to_end)
      self.end_admitted_queries(num_to_end)
      self.wait_for_metric_changes(['released'], curr_metrics, num_to_end)

      num_queued_remaining =\
          curr_metrics['queued'] - curr_metrics['dequeued'] - curr_metrics['timed-out']
      expected_admitted = min(num_queued_remaining, MAX_NUM_CONCURRENT_QUERIES)
      (metric_deltas, _) = self.wait_for_metric_changes(
          ['admitted', 'timed-out'], curr_metrics, expected_admitted)

      # The queue timeout is set high for these tests, so we don't expect any queries to
      # time out.
      assert metric_deltas['admitted'] >= expected_admitted
      assert metric_deltas['timed-out'] == 0
      self.wait_for_admitted_threads(metric_deltas['admitted'])
      # Wait a few topic updates to ensure the admission controllers have reached a steady
      # state or we may find an impalad dequeue more requests after we capture metrics.
      self.wait_for_statestore_updates(10)

    final_metrics = self.get_admission_metrics();
    log_metrics("Final metrics: ", final_metrics, logging.INFO);
    metric_deltas = compute_metric_deltas(final_metrics, initial_metrics)
    assert metric_deltas['timed-out'] == 0

    if round_robin_submission:
      min_expected_admitted = MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['admitted'] >= min_expected_admitted
      assert metric_deltas['admitted'] <= min_expected_admitted * len(self.impalads)
      assert metric_deltas['admitted'] ==\
          initial_metric_deltas['admitted'] + initial_metric_deltas['queued']
      assert metric_deltas['queued'] == initial_metric_deltas['queued']
      assert metric_deltas['rejected'] == initial_metric_deltas['rejected']
    else:
      # We shouldn't go over the max number of queries or queue size so we can compute
      # the expected number of queries that should have been admitted (which includes the
      # number queued as they eventually get admitted as well), queued, and rejected
      expected_admitted = MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['admitted'] == expected_admitted
      assert metric_deltas['queued'] == MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['rejected'] == num_queries - expected_admitted

    # All queries should be completed by now.
    queries_page_num_queued = self._get_queries_page_num_queued()
    assert queries_page_num_queued == 0
    self._check_queries_page_resource_pools()

    for thread in self.all_threads:
      if thread.error is not None:
        raise thread.error

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=MAX_NUM_CONCURRENT_QUERIES,
        max_queued=MAX_NUM_QUEUED_QUERIES, pool_max_mem=-1, queue_wait_timeout_ms=600000),
      statestored_args=_STATESTORED_ARGS)
  def test_admission_controller_with_flags(self, vector):
    self.pool_name = 'default-pool'
    # The pool has no mem resources set, so submitting queries with huge mem_limits
    # should be fine. This exercises the code that does the per-pool memory
    # accounting (see MemTracker::GetPoolMemReserved()) without actually being throttled.
    self.run_admission_test(vector, {'request_pool': self.pool_name,
      'mem_limit': sys.maxint})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(),
      statestored_args=_STATESTORED_ARGS)
  def test_admission_controller_with_configs(self, vector):
    self.pool_name = 'root.queueB'
    self.run_admission_test(vector, {'request_pool': self.pool_name})

  def get_proc_limit(self):
    """Gets the process mem limit as reported by the impalad's mem-tracker metric.
       Raises an assertion if not all impalads have the same value."""
    limit_metrics = []
    for impalad in self.cluster.impalads:
      limit_metrics.append(impalad.service.get_metric_value("mem-tracker.process.limit"))
      assert limit_metrics[0] == limit_metrics[-1],\
          "Not all impalads have the same process limit: %s" % (limit_metrics,)
    assert limit_metrics[0] is not None
    return limit_metrics[0]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(
        max_requests=MAX_NUM_CONCURRENT_QUERIES * 30, max_queued=MAX_NUM_QUEUED_QUERIES,
        pool_max_mem=MEM_TEST_LIMIT, proc_mem_limit=MEM_TEST_LIMIT,
        queue_wait_timeout_ms=600000),
      statestored_args=_STATESTORED_ARGS)
  def test_mem_limit(self, vector):
    # Impala may set the proc mem limit lower than we think depending on the overcommit
    # settings of the OS. It should be fine to continue anyway.
    proc_limit = self.get_proc_limit()
    if proc_limit != MEM_TEST_LIMIT:
      LOG.info("Warning: Process mem limit %s is not expected val %s", limit_val,
          MEM_TEST_LIMIT)

    self.pool_name = 'default-pool'
    # Each query mem limit (set the query option to override the per-host memory
    # estimate) should use a bit less than (total pool mem limit) / #queries so that
    # once #queries are running, the total pool mem usage is about at the limit and
    # additional incoming requests will be rejected. The actual pool limit on the number
    # of running requests is very high so that requests are only queued/rejected due to
    # the mem limit.
    num_impalads = len(self.cluster.impalads)
    query_mem_limit = (proc_limit / MAX_NUM_CONCURRENT_QUERIES / num_impalads) - 1
    self.run_admission_test(vector,
        {'request_pool': self.pool_name, 'mem_limit': query_mem_limit})
