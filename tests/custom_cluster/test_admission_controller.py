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

from __future__ import absolute_import, division, print_function
from copy import deepcopy
import itertools
import logging
import os
import re
import signal
import subprocess
import sys
import threading
from time import sleep, time

from builtins import int, range, round
import pytest

from impala_thrift_gen.ImpalaService import ImpalaHiveServer2Service
from impala_thrift_gen.TCLIService import TCLIService
from tests.common.cluster_config import (
    impalad_admission_ctrl_config_args,
    impalad_admission_ctrl_flags,
    RESOURCES_DIR,
)
from tests.common.custom_cluster_test_suite import (
    ADMISSIOND_ARGS,
    CustomClusterTestSuite,
    IMPALAD_ARGS,
    START_ARGS,
    WORKLOAD_MGMT_IMPALAD_FLAGS,
)
from tests.common.environ import build_flavor_timeout, ImpalaTestClusterProperties
from tests.common.impala_connection import (
    ERROR,
    FINISHED,
    IMPALA_CONNECTION_EXCEPTION,
    RUNNING,
)
from tests.common.resource_pool_config import ResourcePoolConfig
from tests.common.skip import SkipIfEC, SkipIfFS, SkipIfNotHdfsMinicluster
from tests.common.test_dimensions import (
    add_mandatory_exec_option,
    create_exec_option_dimension,
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension,
    HS2,
)
from tests.common.test_vector import ImpalaTestDimension
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from tests.util.web_pages_util import (
    get_mem_admitted_backends_debug_page,
    get_num_completed_backends,
)
from tests.util.workload_management import QUERY_TBL_LIVE
from tests.verifiers.mem_usage_verifier import MemUsageVerifier
from tests.verifiers.metric_verifier import MetricVerifier

LOG = logging.getLogger('admission_test')

# The query used for testing. It is important that this query returns many rows
# while keeping fragments active on all backends. This allows a thread to keep
# the query active and consuming resources by fetching one row at a time. The
# where clause is for debugging purposes; each thread will insert its id so
# that running queries can be correlated with the thread that submitted them.
# This query returns 329970 rows.
QUERY = " union all ".join(["select * from functional.alltypesagg where id != {0}"] * 30)

SLOW_QUERY = "select count(*) from functional.alltypes where int_col = sleep(20000)"

# Same query but with additional unpartitioned non-coordinator fragments.
# The unpartitioned fragments are both interior fragments that consume input
# from a scan fragment and non-interior fragments with a constant UNION.
QUERY_WITH_UNPARTITIONED_FRAGMENTS = """
    select *, (select count(distinct int_col) from functional.alltypestiny) subquery1,
           (select count(distinct int_col) from functional.alltypes) subquery2,
           (select 1234) subquery3
    from (""" + QUERY + """) v"""

# The statestore heartbeat and topic update frequency (ms). Set low for testing.
STATESTORE_RPC_FREQUENCY_MS = 100

# Time to sleep (in milliseconds) between issuing queries. When the delay is at least
# the statestore heartbeat frequency, all state should be visible by every impalad by
# the time the next query is submitted. Otherwise, the different impalads will see stale
# state for some admission decisions.
SUBMISSION_DELAY_MS = \
    [0, STATESTORE_RPC_FREQUENCY_MS // 2, STATESTORE_RPC_FREQUENCY_MS * 3 // 2]

# Whether we will submit queries to all available impalads (in a round-robin fashion)
ROUND_ROBIN_SUBMISSION = [True, False]

# The query pool to use. The impalads should be configured to recognize this
# pool with the parameters below.
POOL_NAME = "default-pool"

# Stress test timeout (seconds). The timeout needs to be significantly higher for
# slow builds like code coverage and ASAN (IMPALA-3790, IMPALA-6241).
STRESS_TIMEOUT = build_flavor_timeout(90, slow_build_timeout=600)

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
QUERY_END_TIMEOUT_S = 3
FETCH_INTERVAL = 0.5
assert FETCH_INTERVAL < QUERY_END_TIMEOUT_S

# How long to wait for admission control status. This assumes a worst case of 40 queries
# admitted serially, with a 3s inactivity timeout.
ADMIT_TIMEOUT_S = 120

# Value used for --admission_control_stale_topic_threshold_ms in tests.
STALE_TOPIC_THRESHOLD_MS = 500

# Regex that matches the first part of the profile info string added when a query is
# queued.
INITIAL_QUEUE_REASON_REGEX = \
    "Initial admission queue reason: waited [0-9]* ms, reason: .*"

# SQL statement that selects all records for the active queries table.
ACTIVE_SQL = "select * from {}".format(QUERY_TBL_LIVE)


def log_metrics(log_prefix, metrics):
  LOG.info("%sadmitted=%s, queued=%s, dequeued=%s, rejected=%s, "
      "released=%s, timed-out=%s", log_prefix, metrics['admitted'], metrics['queued'],
      metrics['dequeued'], metrics['rejected'], metrics['released'],
      metrics['timed-out'])


def compute_metric_deltas(m2, m1):
  """Returns a dictionary of the differences of metrics in m2 and m1 (m2 - m1)"""
  return dict((n, m2.get(n, 0) - m1.get(n, 0)) for n in m2.keys())


def metric_key(pool_name, metric_name):
  """Helper method to construct the admission controller metric keys"""
  return "admission-controller.%s.%s" % (metric_name, pool_name)


def wait_statestore_heartbeat(num_heartbeat=1):
  """Wait for state sync across impalads."""
  assert num_heartbeat > 0
  sleep(STATESTORE_RPC_FREQUENCY_MS / 1000.0 * num_heartbeat)


class TestAdmissionControllerBase(CustomClusterTestSuite):

  @classmethod
  def default_test_protocol(cls):
    # Do not change this. Multiple test method has been hardcoded under this assumption.
    return HS2

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerBase, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # There's no reason to test this on other file formats/compression codecs right now
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  def enable_admission_service(self, method):
    """Inject argument to enable admission control service.
    Must be called at setup_method() and before calling setup_method() of superclass."""
    start_args = "--enable_admission_service"
    if START_ARGS in method.__dict__:
      start_args = method.__dict__[START_ARGS] + " " + start_args
    method.__dict__[START_ARGS] = start_args
    if IMPALAD_ARGS in method.__dict__:
      method.__dict__[ADMISSIOND_ARGS] = method.__dict__[IMPALAD_ARGS]


class TestAdmissionControllerRawHS2(TestAdmissionControllerBase, HS2TestSuite):

  def __check_pool_rejected(self, client, pool, expected_error_re):
    try:
      client.set_configuration({'request_pool': pool})
      client.execute("select 1")
      assert False, "Query should return error"
    except IMPALA_CONNECTION_EXCEPTION as e:
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
    expected_set = set([x.lower() for x in expected_query_options])
    confs_set = set([x.lower() for x in confs])
    assert expected_set.issubset(confs_set)

  def __check_hs2_query_opts(self, pool_name, mem_limit=None, spool_query_results=None,
      expected_options=None):
    """ Submits a query via HS2 (optionally with a mem_limit in the confOverlay)
        into pool_name and checks that the expected_query_options are set in the
        profile."""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.confOverlay = {'request_pool': pool_name}
    if mem_limit is not None: execute_statement_req.confOverlay['mem_limit'] = mem_limit
    if spool_query_results is not None:
      execute_statement_req.confOverlay['spool_query_results'] = spool_query_results
    execute_statement_req.statement = "select 1"
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

  def get_ac_process(self):
    """Returns the Process that is running the admission control service."""
    return self.cluster.impalads[0]

  def get_ac_log_name(self):
    """Returns the prefix of the log files for the admission control process."""
    return "impalad"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-test2.xml",
        llama_site_file="llama-site-test2.xml"),
      default_query_options=[('mem_limit', 200000000)],
      statestored_args=_STATESTORED_ARGS)
  @needs_session(conf_overlay={'batch_size': '100'})
  def test_set_request_pool(self):
    """Tests setting the REQUEST_POOL with the pool placement policy configured
    to require a specific pool, and validate that the per-pool configurations were
    applied."""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_hs2_client()
    # Expected default mem limit for queueA, used in several tests below
    queueA_mem_limit = "MEM_LIMIT=%s" % (128 * 1024 * 1024)
    try:
      for pool in ['', 'not_a_pool_name']:
        expected_error = re.compile(r"Request from user '\S+' with requested pool "
            "'%s' denied access to assigned pool" % (pool))
        self.__check_pool_rejected(client, pool, expected_error)

      # Check rejected if user does not have access.
      expected_error = re.compile(r"Request from user '\S+' with requested pool "
          "'root.queueC' denied access to assigned pool 'root.queueC'")
      self.__check_pool_rejected(client, 'root.queueC', expected_error)

      # Also try setting a valid pool
      client.set_configuration({'request_pool': 'root.queueB'})
      client.execute('set enable_trivial_query_for_admission=false')
      result = client.execute("select 1")
      # Query should execute in queueB which doesn't have a default mem limit set in the
      # llama-site.xml, so it should inherit the value from the default process query
      # options.
      self.__check_query_options(result.runtime_profile,
          ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB'])

      # Try setting the pool for a queue with a very low queue timeout.
      # queueA allows only 1 running query and has a queue timeout of 50ms, so the
      # second concurrent query should time out quickly.
      client.set_configuration({'request_pool': 'root.queueA'})
      client.execute('set enable_trivial_query_for_admission=false')
      handle = client.execute_async("select sleep(1000)")
      # Wait for query to clear admission control and get accounted for
      client.wait_for_admission_control(handle)
      self.__check_pool_rejected(client, 'root.queueA', "exceeded timeout")
      assert client.is_finished(handle)
      # queueA has default query options mem_limit=128m,query_timeout_s=5
      self.__check_query_options(client.get_runtime_profile(handle),
          [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA'])
      client.close_query(handle)

      # IMPALA-9856: We disable query result spooling so that this test can run queries
      # with low mem_limit.
      client.execute("set spool_query_results=0")

      # Should be able to set query options via the set command (overriding defaults if
      # applicable). mem_limit overrides the pool default. abort_on_error has no
      # proc/pool default.
      client.execute("set mem_limit=31337")
      client.execute("set abort_on_error=1")
      client.execute('set enable_trivial_query_for_admission=false')
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,
          ['MEM_LIMIT=31337', 'ABORT_ON_ERROR=1', 'QUERY_TIMEOUT_S=5',
           'REQUEST_POOL=root.queueA'])

      # Should be able to set query options (overriding defaults if applicable) with the
      # config overlay sent with the query RPC. mem_limit is a pool-level override and
      # max_io_buffers has no proc/pool default.
      client.set_configuration({'request_pool': 'root.queueA', 'mem_limit': '12345'})
      client.execute('set enable_trivial_query_for_admission=false')
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,
          ['MEM_LIMIT=12345', 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA',
           'ABORT_ON_ERROR=1'])

      # Once options are reset to their defaults, the queue
      # configuration should kick back in. We'll see the
      # queue-configured mem_limit, and we won't see
      # abort on error, because it's back to being the default.
      client.execute('set mem_limit=""')
      client.execute('set abort_on_error=""')
      client.execute('set enable_trivial_query_for_admission=false')
      client.set_configuration({'request_pool': 'root.queueA'})
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
    self.__check_hs2_query_opts("root.queueA", None, 'false',
        [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check overriding the mem limit sent in the confOverlay with the query.
    self.__check_hs2_query_opts("root.queueA", '12345', 'false',
        ['MEM_LIMIT=12345', 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check HS2 query in queueB gets the process-wide default query options
    self.__check_hs2_query_opts("root.queueB", None, 'false',
        ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB', batch_size])

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-test2.xml",
        llama_site_file="llama-site-test2.xml",
        additional_args="-require_username -anonymous_user_name="),
      statestored_args=_STATESTORED_ARGS)
  def test_require_user(self):
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = ""
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestAdmissionControllerRawHS2.check_response(open_session_resp)

    try:
      execute_statement_req = TCLIService.TExecuteStatementReq()
      execute_statement_req.sessionHandle = open_session_resp.sessionHandle
      execute_statement_req.statement = "select count(1) from functional.alltypes"
      execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
      self.wait_for_operation_state(execute_statement_resp.operationHandle,
                                    TCLIService.TOperationState.ERROR_STATE)
      get_operation_status_resp = self.get_operation_status(
          execute_statement_resp.operationHandle)
      assert "User must be specified" in get_operation_status_resp.errorMessage
    finally:
      close_req = TCLIService.TCloseSessionReq()
      close_req.sessionHandle = open_session_resp.sessionHandle
      TestAdmissionControllerRawHS2.check_response(
        self.hs2_client.CloseSession(close_req))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
          pool_max_mem=1024 * 1024 * 1024))
  @needs_session()
  def test_queuing_status_through_query_log_and_exec_summary(self):
    """Test to verify that the HS2 client's GetLog() call and the ExecSummary expose
    the query's queuing status, that is, whether the query was queued and what was the
    latest queuing reason."""
    # Start a long-running query.
    long_query_resp = self.execute_statement("select sleep(10000)")
    # Ensure that the query has started executing.
    self.wait_for_admission_control(long_query_resp.operationHandle)
    # Submit another query.
    queued_query_resp = self.execute_statement("select sleep(1)")
    # Wait until the query is queued.
    self.wait_for_operation_state(queued_query_resp.operationHandle,
            TCLIService.TOperationState.PENDING_STATE)
    # Check whether the query log message correctly exposes the queuing status.
    log = self.wait_for_log_message(
        queued_query_resp.operationHandle, "Admission result :")
    assert "Admission result : Queued" in log, log
    assert "Latest admission queue reason : number of running queries 1 is at or over "
    "limit 1" in log, log
    # Now check the same for ExecSummary.
    summary_req = ImpalaHiveServer2Service.TGetExecSummaryReq()
    summary_req.operationHandle = queued_query_resp.operationHandle
    summary_req.sessionHandle = self.session_handle
    exec_summary_resp = self.hs2_client.GetExecSummary(summary_req)
    assert exec_summary_resp.summary.is_queued
    assert "number of running queries 1 is at or over limit 1" in \
           exec_summary_resp.summary.queued_reason, \
      exec_summary_resp.summary.queued_reason
    # Close the running query.
    self.close(long_query_resp.operationHandle)
    # Close the queued query.
    self.close(queued_query_resp.operationHandle)


class TestAdmissionControllerRawHS2WithACService(TestAdmissionControllerRawHS2):
  """Runs all of the tests from TestAdmissionControllerRawHS2 but with the second
  impalad in the minicluster configured to perform all admission control."""

  def get_ac_process(self):
    return self.cluster.admissiond

  def get_ac_log_name(self):
    return "admissiond"

  def setup_method(self, method):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.enable_admission_service(method)
    super(TestAdmissionControllerRawHS2, self).setup_method(method)


class TestAdmissionController(TestAdmissionControllerBase):

  def get_ac_process(self):
    """Returns the Process that is running the admission control service."""
    return self.cluster.impalads[0]

  def get_ac_log_name(self):
    """Returns the prefix of the log files for the admission control process."""
    return "impalad"

  def setup_method(self, method):
    """All tests in this class is non-destructive. Therefore, we can afford
    resetting clients at every setup_method."""
    super(TestAdmissionController, self).setup_method(method)
    self._reset_impala_clients()

  def _execute_and_collect_profiles(self, queries, timeout_s, config_options={},
      allow_query_failure=False):
    """Submit the query statements in 'queries' in parallel to the first impalad in
    the cluster. After submission, the results are fetched from the queries in
    sequence and their profiles are collected. Wait for up to timeout_s for
    each query to finish. If 'allow_query_failure' is True, succeeds if the query
    completes successfully or ends up in the EXCEPTION state. Otherwise expects the
    queries to complete successfully.
    Returns the profile strings."""
    client = self.cluster.impalads[0].service.create_hs2_client()
    expected_states = [FINISHED]
    if allow_query_failure:
      expected_states.append(ERROR)
    try:
      handles = []
      profiles = []
      client.set_configuration(config_options)
      for query in queries:
        handles.append(client.execute_async(query))
      for query, handle in zip(queries, handles):
        state = client.wait_for_any_impala_state(handle, expected_states, timeout_s)
        if state == FINISHED:
          self.client.fetch(query, handle)
        profiles.append(client.get_runtime_profile(handle))
      return profiles
    finally:
      for handle in handles:
        client.close_query(handle)
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=10 * 1024 * 1024, proc_mem_limit=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_trivial_coord_query_limits(self):
    """Tests that trivial coordinator only queries have negligible resource requirements.
    """
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
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

  @SkipIfFS.hdfs_block_size
  @SkipIfEC.parquet_file_size
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=40 * 1024 * 1024, proc_mem_limit=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_memory_rejection(self, vector):
    """Test that rejection of queries based on reservation and estimates works as
    expected. The test depends on scanner memory estimates, which different on remote
    filesystems with different (synthetic) block sizes."""
    # Test that the query will be rejected by admission control if:
    # a) the largest per-backend min buffer reservation is larger than the query mem limit
    # b) the largest per-backend min buffer reservation is larger than the
    #    buffer_pool_limit query option
    # c) the cluster-wide min-buffer reservation size is larger than the pool memory
    #    resources.
    self.run_test_case('QueryTest/admission-reject-min-reservation', vector)

    # Test that queries are rejected based on memory estimates. Set num_nodes=1 to
    # avoid unpredictability from scheduling on different backends.
    exec_options = vector.get_value('exec_option')
    exec_options['num_nodes'] = 1
    self.run_test_case('QueryTest/admission-reject-mem-estimate', vector)

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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
      pool_max_mem=10 * PROC_MEM_TEST_LIMIT, proc_mem_limit=PROC_MEM_TEST_LIMIT)
      + " -clamp_query_mem_limit_backend_mem_limit=false",
      num_exclusive_coordinators=1)
  def test_mem_limit_dedicated_coordinator(self, vector):
    """Regression test for IMPALA-8469: coordinator fragment should be admitted on
    dedicated coordinator"""
    query = "select * from functional.alltypesagg limit 1"
    exec_options = vector.get_value('exec_option')
    # Test both single-node and distributed plans
    for num_nodes in [0, 1]:
      # Memory just fits in memory limits
      exec_options['mem_limit'] = self.PROC_MEM_TEST_LIMIT
      exec_options['num_nodes'] = num_nodes
      self.execute_query_expect_success(self.client, query, exec_options)

      # A bit too much memory to run on coordinator.
      exec_options['mem_limit'] = int(self.PROC_MEM_TEST_LIMIT * 1.1)
      ex = self.execute_query_expect_failure(self.client, query, exec_options)
      assert ("Rejected query from pool default-pool: request memory needed "
              "1.10 GB is greater than memory available for admission 1.00 GB" in
              str(ex)), str(ex)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
      pool_max_mem=10 * PROC_MEM_TEST_LIMIT, proc_mem_limit=PROC_MEM_TEST_LIMIT)
      + " -clamp_query_mem_limit_backend_mem_limit=true",
      num_exclusive_coordinators=1,
      cluster_size=2)
  def test_clamp_query_mem_limit_backend_mem_limit_flag(self, vector):
    """If a query requests more memory than backend's memory limit for admission, the
    query gets admitted with the max memory for admission on backend."""
    query = "select * from functional.alltypesagg limit 10"
    exec_options = vector.get_value('exec_option')
    # Requested mem_limit is more than the memory limit for admission on backends.
    # mem_limit will be clamped to the mem limit for admission on backends.
    exec_options['mem_limit'] = int(self.PROC_MEM_TEST_LIMIT * 1.1)
    result = self.execute_query_expect_success(self.client, query, exec_options)
    assert "Cluster Memory Admitted: 2.00 GB" in str(result.runtime_profile), \
           str(result.runtime_profile)
    # Request mem_limit more than memory limit for admission on executors. Executor's
    # memory limit will be clamped to the mem limit for admission on executor.
    exec_options['mem_limit'] = 0
    exec_options['mem_limit_executors'] = int(self.PROC_MEM_TEST_LIMIT * 1.1)
    result = self.execute_query_expect_success(self.client, query, exec_options)
    assert "Cluster Memory Admitted: 1.10 GB" in str(result.runtime_profile), \
           str(result.runtime_profile)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml"), num_exclusive_coordinators=1,
    cluster_size=2)
  def test_dedicated_coordinator_mem_accounting(self, vector):
    """Verify that when using dedicated coordinators, the memory admitted for and the
    mem limit applied to the query fragments running on the coordinator is different from
    the ones on executors."""
    self.__verify_mem_accounting(vector, using_dedicated_coord_estimates=True)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml")
    + " -use_dedicated_coordinator_estimates false",
    num_exclusive_coordinators=1,
    cluster_size=2)
  def test_dedicated_coordinator_legacy_mem_accounting(self, vector):
    """Verify that when using dedicated coordinators with specialized dedicated coord
    estimates turned off using a hidden startup param, the memory admitted for and the
    mem limit applied to the query fragments running on the coordinator is the same
    (as expected from legacy behavior)."""
    self.__verify_mem_accounting(vector, using_dedicated_coord_estimates=False)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml"), num_exclusive_coordinators=1,
    cluster_size=2)
  def test_sanity_checks_dedicated_coordinator(self, vector, unique_database):
    """Sanity tests for verifying targeted dedicated coordinator memory estimations and
    behavior."""
    self.client.set_configuration_option('request_pool', "root.regularPool")
    exec_options = vector.get_value('exec_option')
    # Make sure query option MAX_MEM_ESTIMATE_FOR_ADMISSION is enforced on the dedicated
    # coord estimates. Without this query option the estimate would be > 100MB.
    expected_mem = 60 * (1 << 20)  # 60MB
    exec_options['MAX_MEM_ESTIMATE_FOR_ADMISSION'] = expected_mem
    self.client.set_configuration(exec_options)
    handle = self.client.execute_async(QUERY.format(1))
    self.client.wait_for_finished_timeout(handle, 1000)
    mem_to_admit = self.__get_mem_limits_admission_debug_page()
    assert abs(mem_to_admit['coordinator'] - expected_mem) < 0.0001,\
      "mem_to_admit:" + str(mem_to_admit)
    assert abs(mem_to_admit['executor'] - expected_mem) < 0.0001, \
      "mem_to_admit:" + str(mem_to_admit)
    self.client.close_query(handle)

    # If the query is only scheduled on the coordinator then the mem to admit on executor
    # should be zero.
    exec_options['NUM_NODES'] = 1
    self.client.set_configuration(exec_options)
    handle = self.client.execute_async(QUERY.format(1))
    self.client.wait_for_finished_timeout(handle, 1000)
    mem_to_admit = self.__get_mem_limits_admission_debug_page()
    assert abs(mem_to_admit['coordinator'] - expected_mem) < 0.0001, \
      "mem_to_admit:" + str(mem_to_admit)
    assert abs(mem_to_admit['executor'] - 0) < 0.0001, \
      "mem_to_admit:" + str(mem_to_admit)
    self.client.close_query(handle)

    # Make sure query execution works perfectly for a query that does not have any
    # fragments scheduled on the coordinator, but has runtime-filters that need to be
    # aggregated at the coordinator.
    exec_options = vector.get_value('exec_option')
    exec_options['RUNTIME_FILTER_WAIT_TIME_MS'] = 30000
    query = """CREATE TABLE {0}.temp_tbl AS SELECT STRAIGHT_JOIN o_orderkey
    FROM tpch_parquet.lineitem INNER JOIN [SHUFFLE] tpch_parquet.orders
    ON o_orderkey = l_orderkey GROUP BY 1""".format(unique_database)
    result = self.execute_query_expect_success(self.client, query, exec_options)
    assert "Runtime filters: All filters arrived" in result.runtime_profile

  def __verify_mem_accounting(self, vector, using_dedicated_coord_estimates):
    """Helper method used by test_dedicated_coordinator_*_mem_accounting that verifies
    the actual vs expected values for mem admitted and mem limit for both coord and
    executor. Also verifies that those memory values are different if
    'using_dedicated_coord_estimates' is true."""
    vector.set_exec_option('request_pool', 'root.regularPool')
    self.client.set_configuration(vector.get_exec_option_dict())
    # Use a test query that has unpartitioned non-coordinator fragments to make
    # sure those are handled correctly (IMPALA-10036).
    for query in [QUERY, QUERY_WITH_UNPARTITIONED_FRAGMENTS]:
      handle = self.client.execute_async(query.format(1))
      self.client.wait_for_finished_timeout(handle, 1000)
      expected_mem_limits = self.__get_mem_limits_admission_debug_page()
      actual_mem_limits = self.__get_mem_limits_memz_debug_page(
        self.client.handle_id(handle))
      mem_admitted =\
          get_mem_admitted_backends_debug_page(self.cluster, self.get_ac_process())
      debug_string = " expected_mem_limits:" + str(
        expected_mem_limits) + " actual_mem_limits:" + str(
        actual_mem_limits) + " mem_admitted:" + str(mem_admitted)
      MB = 1 << 20
      # Easiest way to check float in-equality.
      assert abs(expected_mem_limits['coordinator'] - expected_mem_limits[
        'executor']) > 0.0001 or not using_dedicated_coord_estimates, debug_string
      # There may be some rounding errors so keep a margin of 5MB when verifying
      assert abs(actual_mem_limits['coordinator'] - expected_mem_limits[
        'coordinator']) < 5 * MB, debug_string
      assert abs(actual_mem_limits['executor'] - expected_mem_limits[
        'executor']) < 5 * MB, debug_string
      assert abs(mem_admitted['coordinator'] - expected_mem_limits[
        'coordinator']) < 5 * MB, debug_string
      assert abs(
        mem_admitted['executor'][0] - expected_mem_limits['executor']) < 5 * MB, \
          debug_string
      # Ensure all fragments finish executing before running next query.
      self.client.fetch(query, handle)
      self.client.close_query(handle)

  def __get_mem_limits_admission_debug_page(self):
    """Helper method assumes a 2 node cluster using a dedicated coordinator. Returns the
    mem_limit calculated by the admission controller from the impala admission debug page
    of the coordinator impala daemon. Returns a dictionary with the keys 'coordinator'
    and 'executor' and their respective mem values in bytes."""
    # Based on how the cluster is setup, the first impalad in the cluster is the
    # coordinator.
    response_json = self.get_ac_process().service.get_debug_webpage_json("admission")
    assert 'resource_pools' in response_json
    assert len(response_json['resource_pools']) == 1
    assert response_json['resource_pools'][0]['running_queries']
    assert len(response_json['resource_pools'][0]['running_queries']) == 1
    query_info = response_json['resource_pools'][0]['running_queries'][0]
    return {'coordinator': float(query_info["coord_mem_to_admit"]),
            'executor': float(query_info["mem_limit"])}

  def __get_mem_limits_memz_debug_page(self, query_id):
    """Helper method assumes a 2 node cluster using a dedicated coordinator. Returns the
    mem limits enforced on the query (identified by the 'query_id') extracted from
    mem-tracker's output on the memz debug page of the dedicated coordinator and the
    executor impala daemons. Returns a dictionary with the keys 'coordinator' and
    'executor' and their respective mem values in bytes."""
    metric_name = "Query({0})".format(query_id)
    # Based on how the cluster is setup, the first impalad in the cluster is the
    # coordinator.
    mem_trackers = [MemUsageVerifier(i.service).get_mem_usage_values(metric_name) for i in
                    self.cluster.impalads]
    return {'coordinator': float(mem_trackers[0]['limit']),
            'executor': float(mem_trackers[1]['limit'])}

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1)
  def test_dedicated_coordinator_planner_estimates(self, vector, unique_database):
    """Planner tests to add coverage for coordinator estimates when using dedicated
    coordinators. Also includes coverage for verifying cluster memory admitted."""
    vector_copy = deepcopy(vector)
    exec_options = vector_copy.get_value('exec_option')
    # Remove num_nodes from the options to allow test case runner to set it in one of
    # the test cases.
    del exec_options['num_nodes']
    # Do not turn the default cluster into 2-group one
    exec_options['test_replan'] = 0
    exec_options['num_scanner_threads'] = 1  # To make estimates consistently reproducible
    self.run_test_case('QueryTest/dedicated-coord-mem-estimates', vector_copy,
                       unique_database)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1, cluster_size=2)
  def test_mem_limit_executors(self):
    """Verify that the query option mem_limit_executors is only enforced on the
    executors."""
    expected_exec_mem_limit = "999999999"
    self.client.set_configuration({"MEM_LIMIT_EXECUTORS": expected_exec_mem_limit})
    handle = self.client.execute_async(QUERY.format(1))
    self.client.wait_for_finished_timeout(handle, 1000)
    expected_mem_limits = self.__get_mem_limits_admission_debug_page()
    assert expected_mem_limits['executor'] > expected_mem_limits[
      'coordinator'], expected_mem_limits
    assert expected_mem_limits['executor'] == float(
      expected_exec_mem_limit), expected_mem_limits

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1, cluster_size=2,
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=2 * 1024 * 1024 * 1024, proc_mem_limit=3 * 1024 * 1024 * 1024))
  def test_mem_limit_coordinators(self):
    """Verify that the query option mem_limit_coordinators is only enforced on the
    coordinators."""
    expected_exec_mem_limit = "999999999"
    expected_coord_mem_limit = "111111111"
    self.client.set_configuration({"MEM_LIMIT_EXECUTORS": expected_exec_mem_limit,
        "MEM_LIMIT_COORDINATORS": expected_coord_mem_limit})
    handle = self.client.execute_async(QUERY.format(1))
    self.client.wait_for_finished_timeout(handle, 1000)
    expected_mem_limits = self.__get_mem_limits_admission_debug_page()
    assert expected_mem_limits['executor'] == float(
      expected_exec_mem_limit), expected_mem_limits
    assert expected_mem_limits['coordinator'] == float(
      expected_coord_mem_limit), expected_mem_limits

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1, cluster_size=2,
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
          pool_max_mem=2 * 1024 * 1024 * 1024, proc_mem_limit=3 * 1024 * 1024 * 1024))
  def test_mem_limits(self):
    """Verify that the query option mem_limit_coordinators and mem_limit_executors are
    ignored when mem_limit is set."""
    exec_mem_limit = "999999999"
    coord_mem_limit = "111111111"
    mem_limit = "888888888"
    self.client.set_configuration({"MEM_LIMIT_EXECUTORS": exec_mem_limit,
        "MEM_LIMIT_COORDINATORS": coord_mem_limit, "MEM_LIMIT": mem_limit})
    handle = self.client.execute_async(QUERY.format(1))
    self.client.wait_for_finished_timeout(handle, 1000)
    expected_mem_limits = self.__get_mem_limits_admission_debug_page()
    assert expected_mem_limits['executor'] == float(mem_limit), expected_mem_limits
    assert expected_mem_limits['coordinator'] == float(mem_limit), expected_mem_limits

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=2, max_queued=1,
      pool_max_mem=10 * PROC_MEM_TEST_LIMIT,
      queue_wait_timeout_ms=2 * STATESTORE_RPC_FREQUENCY_MS)
      + " -clamp_query_mem_limit_backend_mem_limit=false",
      start_args="--per_impalad_args=-mem_limit=3G;-mem_limit=3G;-mem_limit=2G;",
      statestored_args=_STATESTORED_ARGS)
  def test_heterogeneous_proc_mem_limit(self, vector):
    """ Test to ensure that the admission controller takes into account the actual proc
    mem limits of each impalad. Starts a cluster where the last impalad has a smaller
    proc mem limit than other impalads and runs queries where admission/rejection decision
    depends on the coordinator knowing the other impalad's mem limits.
    The queue_wait_timeout_ms has been set to be more than the prioritized statestore
    update time, so that the queries don't time out before receiving updates to pool
    stats"""
    # Choose a query that runs on all 3 backends.
    query = "select * from functional.alltypesagg, (select 1) B limit 1"
    # Successfully run a query with mem limit equal to the lowest process memory among
    # impalads
    exec_options = deepcopy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "2G"
    self.execute_query_expect_success(self.client, query, exec_options)
    # Test that a query scheduled to run on a single node and submitted to the impalad
    # with higher proc mem limit succeeds.
    exec_options = deepcopy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "3G"
    exec_options['num_nodes'] = "1"
    self.execute_query_expect_success(self.client, query, exec_options)
    # Exercise rejection checks in admission controller.
    exec_options = deepcopy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "3G"
    ex = self.execute_query_expect_failure(self.client, query, exec_options)
    assert ("Rejected query from pool default-pool: request memory needed "
            "3.00 GB is greater than memory available for admission 2.00 GB" in
            str(ex)), str(ex)
    # Exercise queuing checks in admission controller.
    try:
      # Wait for previous queries to finish to avoid flakiness.
      for impalad in self.cluster.impalads:
        impalad.service.wait_for_metric_value("impala-server.num-fragments-in-flight", 0)
      impalad_with_2g_mem = self.cluster.impalads[2].service.create_client_from_vector(
        vector)
      impalad_with_2g_mem.set_configuration_option('mem_limit', '1G')
      impalad_with_2g_mem.execute_async("select sleep(1000)")
      # Wait for statestore update to update the mem admitted in each node.
      wait_statestore_heartbeat()
      exec_options = deepcopy(vector.get_value('exec_option'))
      exec_options['mem_limit'] = "2G"
      # Since Queuing is synchronous, and we can't close the previous query till this
      # returns, we wait for this to timeout instead.
      self.execute_query(query, exec_options)
    except IMPALA_CONNECTION_EXCEPTION as e:
      assert re.search(r"Queued reason: Not enough memory available on host \S+.Needed "
          r"2.00 GB but only 1.00 GB out of 2.00 GB was available.", str(e)), str(e)
    finally:
      if impalad_with_2g_mem is not None:
        impalad_with_2g_mem.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--logbuflevel=-1 " + impalad_admission_ctrl_flags(max_requests=1,
        max_queued=1, pool_max_mem=PROC_MEM_TEST_LIMIT),
    statestored_args=_STATESTORED_ARGS,
    disable_log_buffering=True)
  def test_cancellation(self, vector):
    """ Test to confirm that all Async cancellation windows are hit and are able to
    successfully cancel the query"""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_client_from_vector(vector)
    try:
      client.set_configuration_option("debug_action", "AC_BEFORE_ADMISSION:SLEEP@2000")
      client.set_configuration_option("mem_limit", self.PROC_MEM_TEST_LIMIT + 1)
      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      handle = client.execute_async("select 1")
      sleep(1)
      client.close_query(handle)
      self.assert_log_contains(self.get_ac_log_name(), 'INFO',
          "Ready to be Rejected but already cancelled, query id=")
      client.clear_configuration()

      client.set_configuration_option("debug_action", "AC_BEFORE_ADMISSION:SLEEP@2000")
      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      handle = client.execute_async("select 2")
      sleep(1)
      client.close_query(handle)
      self.assert_log_contains(self.get_ac_log_name(), 'INFO',
          "Ready to be Admitted immediately but already cancelled, query id=")

      client.set_configuration_option("debug_action",
          "CRS_BEFORE_COORD_STARTS:SLEEP@2000")
      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      handle = client.execute_async("select 3")
      sleep(1)
      client.close_query(handle)
      self.assert_impalad_log_contains('INFO',
          "Cancelled right after starting the coordinator query id=")

      client.set_configuration_option("debug_action", "CRS_AFTER_COORD_STARTS:SLEEP@2000")
      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      handle = client.execute_async("select 4")
      sleep(1)
      client.close_query(handle)
      self.assert_impalad_log_contains('INFO',
          "Cancelled right after starting the coordinator query id=", 2)

      client.clear_configuration()
      handle = client.execute_async("select sleep(10000)")
      client.set_configuration_option("debug_action",
          "AC_AFTER_ADMISSION_OUTCOME:SLEEP@2000")
      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      queued_query_handle = client.execute_async("select 5")
      sleep(1)
      assert client.is_pending(queued_query_handle)
      assert "Admission result: Queued" in client.get_runtime_profile(
        queued_query_handle)
      # Only cancel the queued query, because close will wait till it unregisters, this
      # gives us a chance to close the running query and allow the dequeue thread to
      # dequeue the queue query
      client.cancel(queued_query_handle)
      client.close_query(handle)
      queued_profile = client.close_query(queued_query_handle,
                                          fetch_profile_after_close=True)
      assert "Admission result: Cancelled (queued)" in queued_profile, queued_profile
      self.assert_log_contains(
          self.get_ac_log_name(), 'INFO', "Dequeued cancelled query=")
      client.clear_configuration()

      client.set_configuration_option('enable_trivial_query_for_admission', 'false')
      handle = client.execute_async("select sleep(10000)")
      queued_query_handle = client.execute_async("select 6")
      sleep(1)
      assert client.is_pending(queued_query_handle)
      assert "Admission result: Queued" in client.get_runtime_profile(
        queued_query_handle)
      queued_profile = client.close_query(queued_query_handle,
                                          fetch_profile_after_close=True)
      client.close_query(handle)
      assert "Admission result: Cancelled (queued)" in queued_profile
      for i in self.cluster.impalads:
        i.service.wait_for_metric_value(
            "impala-server.num-fragments-in-flight", 0, timeout=20)
      assert self.get_ac_process().service.get_metric_value(
        "admission-controller.agg-num-running.default-pool") == 0
      assert self.get_ac_process().service.get_metric_value(
        "admission-controller.total-admitted.default-pool") == 4
      assert self.get_ac_process().service.get_metric_value(
        "admission-controller.total-queued.default-pool") == 2
    finally:
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=2, max_queued=1,
          pool_max_mem=1024 * 1024 * 1024), statestored_args=_STATESTORED_ARGS)
  def test_concurrent_queries(self):
    """Test that the number of running queries appears in the profile when the query is
    successfully admitted."""
    # A trivial coordinator only query is scheduled on the empty group which does not
    # exist in the cluster.
    result = self.execute_query_expect_success(self.client, "select 1")
    assert "Executor Group: empty group (using coordinator only)" \
        in result.runtime_profile
    assert "Number of running queries in designated executor group when admitted: 0" \
        in result.runtime_profile
    # Two queries run concurrently in the default pool.
    sleep_query = "select * from functional.alltypesagg where id < sleep(1000)"
    query = "select * from functional.alltypesagg"
    sleep_query_handle = self.client.execute_async(sleep_query)
    self.client.wait_for_admission_control(sleep_query_handle)
    self._wait_for_change_to_profile(sleep_query_handle,
        "Admission result: Admitted immediately")
    result = self.execute_query_expect_success(self.client, query)
    assert "Executor Group: default" in result.runtime_profile
    assert "Number of running queries in designated executor group when admitted: 2" \
        in result.runtime_profile

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
          pool_max_mem=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_queue_reasons_num_queries(self):
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')

    """Test that queue details appear in the profile when queued based on num_queries."""
    # Run a bunch of queries - one should get admitted immediately, the rest should
    # be dequeued one-by-one.
    STMT = "select sleep(1000)"
    TIMEOUT_S = 60
    EXPECTED_REASON = \
        "Latest admission queue reason: number of running queries 1 is at or over limit 1"
    NUM_QUERIES = 5
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        TIMEOUT_S)

    num_reasons = len([profile for profile in profiles if EXPECTED_REASON in profile])
    assert num_reasons == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    init_queue_reasons = self.__extract_init_queue_reasons(profiles)
    assert len(init_queue_reasons) == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    over_limit_details = [detail
        for detail in init_queue_reasons if 'number of running queries' in detail]
    assert len(over_limit_details) == 1, \
        "One query initially queued because of num_queries: " + '\n===\n'.join(profiles)
    queue_not_empty_details = [detail
        for detail in init_queue_reasons if 'queue is not empty' in detail]
    assert len(queue_not_empty_details) == NUM_QUERIES - 2, \
        "Others queued because of non-empty queue: " + '\n===\n'.join(profiles)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=10, max_queued=10,
          pool_max_mem=10 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_queue_reasons_memory(self):
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')

    """Test that queue details appear in the profile when queued based on memory."""
    # Run a bunch of queries with mem_limit set so that only one can be admitted at a
    # time- one should get admitted immediately, the rest should be dequeued one-by-one.
    STMT = "select sleep(100)"
    TIMEOUT_S = 60
    EXPECTED_REASON = "Latest admission queue reason: Not enough aggregate memory " +\
        "available in pool default-pool with max mem resources 10.00 MB. Needed 9.00 MB" \
        " but only 1.00 MB was available."
    NUM_QUERIES = 5
    # IMPALA-9856: Disable query result spooling so that we can run queries with low
    # mem_limit.
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        TIMEOUT_S, {'mem_limit': '9mb', 'spool_query_results': '0'})

    num_reasons = len([profile for profile in profiles if EXPECTED_REASON in profile])
    assert num_reasons == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    init_queue_reasons = self.__extract_init_queue_reasons(profiles)
    assert len(init_queue_reasons) == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    over_limit_details = [detail for detail in init_queue_reasons
        if 'Not enough aggregate memory available' in detail]
    assert len(over_limit_details) == 1, \
        "One query initially queued because of memory: " + '\n===\n'.join(profiles)
    queue_not_empty_details = [detail
        for detail in init_queue_reasons if 'queue is not empty' in detail]
    assert len(queue_not_empty_details) == NUM_QUERIES - 2, \
        "Others queued because of non-empty queue: " + '\n===\n'.join(profiles)

  def __extract_init_queue_reasons(self, profiles):
    """Return a list of the 'Admission Queue details' strings found in 'profiles'"""
    matches = [re.search(INITIAL_QUEUE_REASON_REGEX, profile) for profile in profiles]
    return [match.group(0) for match in matches if match is not None]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=10, max_queued=10,
        pool_max_mem=10 * 1024 * 1024, proc_mem_limit=2 * 1024 * 1024,
        queue_wait_timeout_ms=1000)
      + " --enable_admission_service_mem_safeguard=false",
      statestored_args=_STATESTORED_ARGS)
  def test_timeout_reason_host_memory(self):
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')

    """Test that queue details appear in the profile when queued and then timed out
    due to a small 2MB host memory limit configuration."""
    # Run a bunch of queries with mem_limit set so that only one can be admitted
    # immediately. The rest should be queued and dequeued (timeout) due to host memory
    # pressure.
    STMT = "select sleep(1000)"
    TIMEOUT_S = 20
    NUM_QUERIES = 5
    # IMPALA-9856: Disable query result spooling so that we can run queries with low
    # mem_limit.
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        TIMEOUT_S, {'mem_limit': '2mb', 'spool_query_results': '0'}, True)

    EXPECTED_REASON = """.*Admission for query exceeded timeout 1000ms in pool """\
             """default-pool.*"""\
             """Not enough memory available on host.*"""\
             """Stats for host.*"""\
             """topN_query_stats.*"""\
             """all_query_stats:.*"""
    num_reasons = len([profile for profile in profiles
         if re.search(EXPECTED_REASON, profile, re.DOTALL)])
    assert num_reasons >= 1, \
        "At least one query should have been timed out with topN query details: " +\
        '\n===\n'.join(profiles)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=10, max_queued=10,
        pool_max_mem=2 * 1024 * 1024, proc_mem_limit=20 * 1024 * 1024,
        queue_wait_timeout_ms=1000)
      + " --enable_admission_service_mem_safeguard=false",
      statestored_args=_STATESTORED_ARGS)
  def test_timeout_reason_pool_memory(self):
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')

    """Test that queue details appear in the profile when queued and then timed out
    due to a small 2MB pool memory limit configuration."""
    # Run a bunch of queries with mem_limit set so that only one can be admitted
    # immediately. The rest should be queued and dequeued (timeout) due to pool memory
    # pressure.
    STMT = "select sleep(1000)"
    TIMEOUT_S = 20
    NUM_QUERIES = 5
    # IMPALA-9856: Disable query result spooling so that we can run queries with low
    # mem_limit.
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        TIMEOUT_S, {'mem_limit': '2mb', 'spool_query_results': '0'}, True)

    EXPECTED_REASON = """.*Admission for query exceeded timeout 1000ms in pool """\
            """default-pool.*"""\
            """Not enough aggregate memory available in pool default-pool.*"""\
            """Aggregated stats for pool.*"""\
            """topN_query_stats.*"""
    num_reasons = len([profile for profile in profiles
         if re.search(EXPECTED_REASON, profile, re.DOTALL)])
    assert num_reasons >= 1, \
        "At least one query should have been timed out with topN query details: " +\
        '\n===\n'.join(profiles)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=100, max_queued=10,
          pool_max_mem=-1, admission_control_slots=2,
          executor_groups="default-pool-group1"),
      statestored_args=_STATESTORED_ARGS)
  def test_queue_reasons_slots(self):
    """Test that queue details appear in the profile when queued based on number of
    slots."""
    # Run a bunch of queries - one should get admitted immediately, the rest should
    # be dequeued one-by-one. This is achieved by running 3 Aggregation queries in
    # parallel with MT_DOP option equals to available number of slots in each Impalad
    # executor. Each ScanNode instance read one partition of store_sales.
    cluster_size = len(self.cluster.impalads)
    mt_dop = 2
    num_part = cluster_size * mt_dop
    part_begin = 2450816
    part_end = part_begin + num_part - 1
    # This query runs for roughly 2s in normal build.
    STMT = ("select min(ss_wholesale_cost) from tpcds_parquet.store_sales "
            "where ss_sold_date_sk between {} and {}").format(part_begin, part_end)
    EXPECTED_REASON = "Latest admission queue reason: Not enough admission control " +\
                      "slots available on host"
    NUM_QUERIES = 3
    coordinator_limited_metric = \
      "admission-controller.total-dequeue-failed-coordinator-limited"
    original_metric_value = self.get_ac_process().service.get_metric_value(
        coordinator_limited_metric)
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        STRESS_TIMEOUT, config_options={"mt_dop": mt_dop})

    num_reasons = len([profile for profile in profiles if EXPECTED_REASON in profile])
    assert num_reasons == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    init_queue_reasons = self.__extract_init_queue_reasons(profiles)
    assert len(init_queue_reasons) == NUM_QUERIES - 1, \
        "All queries except first should have been queued: " + '\n===\n'.join(profiles)
    over_limit_details = [detail
        for detail in init_queue_reasons
        if "Not enough admission control slots available on host" in detail]
    assert len(over_limit_details) == 1, \
        "One query initially queued because of slots: " + '\n===\n'.join(profiles)
    queue_not_empty_details = [detail
        for detail in init_queue_reasons if 'queue is not empty' in detail]
    assert len(queue_not_empty_details) == NUM_QUERIES - 2, \
        "Others queued because of non-empty queue: " + '\n===\n'.join(profiles)

    # Confirm that the cluster quiesces and all metrics return to zero.
    for impalad in self.cluster.impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_backend_admission_control_state()

    # The number of admission control slots on the coordinator is limited
    # so the failures to dequeue should trigger a bump in the coordinator_limited_metric.
    later_metric_value = self.get_ac_process().service.get_metric_value(
        coordinator_limited_metric)
    assert later_metric_value > original_metric_value, \
      "Metric %s did not change" % coordinator_limited_metric

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
        pool_max_mem=1024 * 1024 * 1024),
    statestored_args=_STATESTORED_ARGS)
  def test_query_locations_correctness(self, vector):
    """Regression test for IMPALA-7516: Test to make sure query locations and in-flight
    queries are correct for different admission results that can affect it."""
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    # Choose a query that runs on all 3 backends.
    query = "select * from functional.alltypesagg A, (select sleep(10000)) B limit 1"
    # Case 1: When a query runs succesfully.
    handle = self.client.execute_async(query)
    self.__assert_num_queries_accounted(1)
    self.close_query(handle)
    self.__assert_num_queries_accounted(0)
    # Case 2: When a query is queued then cancelled
    handle_running = self.client.execute_async(query)
    self.client.wait_for_admission_control(handle_running)
    handle_queued = self.client.execute_async(query)
    self.client.wait_for_admission_control(handle_queued)
    self.get_ac_process().service.wait_for_metric_value(
      "admission-controller.total-queued.default-pool", 1)
    # Queued queries don't show up on backends
    self.__assert_num_queries_accounted(1, 1)
    # First close the queued query
    self.close_query(handle_queued)
    self.close_query(handle_running)
    self.__assert_num_queries_accounted(0)
    # Case 3: When a query gets rejected
    exec_options = deepcopy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "1b"
    self.execute_query_expect_failure(self.client, query, exec_options)
    self.__assert_num_queries_accounted(0)

  def __assert_num_queries_accounted(self, num_running, num_queued=0):
    """Checks if the num of queries accounted by query_locations and in-flight are as
    expected"""
    # Wait for queries to start/un-register.
    num_inflight = num_running + num_queued
    assert self.impalad_test_service.wait_for_num_in_flight_queries(num_inflight)
    query_locations = self.impalad_test_service.get_query_locations()
    for host, num_q in query_locations.items():
      assert num_q == num_running, "There should be {0} running queries on either " \
                                   "impalads: {0}".format(query_locations)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml"),
    statestored_args=_STATESTORED_ARGS)
  def test_pool_mem_limit_configs(self, vector):
    """Runs functional tests for the max/min_query_mem_limit pool config attributes"""
    exec_options = vector.get_value('exec_option')
    # Set this to the default.
    exec_options['exec_single_node_rows_threshold'] = 100
    # Set num_nodes to 1 since its easier to see one-to-one mapping of per_host and
    # per_cluster values used in the test.
    exec_options['num_nodes'] = 1
    self.run_test_case('QueryTest/admission-max-min-mem-limits', vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-test2.xml",
      llama_site_file="llama-site-test2.xml"),
    statestored_args=_STATESTORED_ARGS)
  def test_user_loads_propagate(self):
    """Test that user loads are propagated between impalads by checking
    metric values"""
    LOG.info("Exploration Strategy {0}".format(self.exploration_strategy()))
    self.check_user_loads(user_loads_present=True, pool='root.queueB')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-3-groups.xml",
      llama_site_file="llama-site-3-groups.xml"),
    statestored_args=_STATESTORED_ARGS)
  def test_user_loads_do_not_propagate(self):
    """Test that user loads are not propagated between impalads if user
    quotas are not configured. There are no user quotas configured in
    fair-scheduler-3-groups.xml."""
    self.check_user_loads(user_loads_present=False, pool="root.tiny")

  def check_user_loads(self, user_loads_present, pool):
    """Fetches the metrics for user loads from the webui and checks they are as
    expected."""
    USER_ROOT = 'root'
    USER_C = 'userC'
    impalad1 = self.cluster.impalads[0]
    impalad2 = self.cluster.impalads[1]
    query1 = self.execute_async_and_wait_for_running(impalad1, SLOW_QUERY, USER_C,
                                                     pool=pool)
    query2 = self.execute_async_and_wait_for_running(impalad2, SLOW_QUERY, USER_ROOT,
                                                     pool=pool)
    wait_statestore_heartbeat(num_heartbeat=3)
    keys = [
      "admission-controller.agg-current-users.root.queueB",
      "admission-controller.local-current-users.root.queueB",
    ]
    # Order matter, since impalad1 run the query ahead of impalad2.
    # This give slightly longer time for impalad1 to hear about query in impalad2.
    values2 = impalad2.service.get_metric_values(keys)
    values1 = impalad1.service.get_metric_values(keys)

    if self.get_ac_log_name() == 'impalad':
      if user_loads_present:
        # The aggregate users are the same on either server.
        assert values1[0] == [USER_ROOT, USER_C]
        assert values2[0] == [USER_ROOT, USER_C]
        # The local users differ.
        assert values1[1] == [USER_C]
        assert values2[1] == [USER_ROOT]
      else:
        # No user quotas configured means no metrics.
        assert values1[0] is None
        assert values2[0] is None
        assert values1[1] is None
        assert values2[1] is None
    else:
      # In exhaustive mode, running with AdmissionD.
      assert self.get_ac_log_name() == 'admissiond'
      admissiond = self.cluster.admissiond
      valuesA = admissiond.service.get_metric_values(keys)
      if user_loads_present:
        # In this case the metrics are the same everywhere
        assert values1[0] == [USER_ROOT, USER_C]
        assert values2[0] == [USER_ROOT, USER_C]
        assert values1[1] == []
        assert values2[1] == []
        assert valuesA[0] == [USER_ROOT, USER_C]
        assert valuesA[1] == [USER_ROOT, USER_C]
      else:
        # No user quotas configured means no metrics.
        assert values1[0] is None
        assert values2[0] is None
        assert values1[1] is None
        assert values2[1] is None
        assert valuesA[0] is None
        assert valuesA[1] is None

    query1.close()
    query2.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-test2.xml",
      llama_site_file="llama-site-test2.xml",
      additional_args="--injected_group_members_debug_only=group1:userB,userC"
    ),
    statestored_args=_STATESTORED_ARGS)
  def test_user_loads_rules(self):
    """Test that rules for user loads are followed for new queries.
    Note that some detailed checking of rule semantics is done at the unit test level in
    admission-controller-test.cc"""

    # The per-pool limit for userA is 3 in root.queueE.
    self.check_user_load_limits('userA', 'root.queueE', 3, "user")
    # In queueE the wildcard limit is 1
    self.check_user_load_limits('random_user', 'root.queueE', 1, "wildcard")

    # userB is in the injected group1, so the limit is 2.
    self.check_user_load_limits('userB', 'root.queueE', 2, "group", group_name="group1")

    # userD had a limit at the pool level, run it in queueD which has no wildcard limit.
    self.check_user_load_limits('userD', 'root.queueD', 2, "user", pool_to_fail="root")

  def check_user_load_limits(self, user, pool, limit, err_type, group_name="",
                             pool_to_fail=None):
    query_handles = []
    type = "group" if group_name else "user"
    group_description = " in group '" + group_name + "'" if group_name else ""
    pool_that_fails = pool_to_fail if pool_to_fail else pool
    for i in range(limit):
      impalad = self.cluster.impalads[i % 2]
      query_handle = self.execute_async_and_wait_for_running(impalad, SLOW_QUERY, user,
                                                             pool=pool)
      query_handles.append(query_handle)

    # Let state sync across impalads.
    wait_statestore_heartbeat(num_heartbeat=3)

    # Another query should be rejected
    impalad = self.cluster.impalads[limit % 2]
    client = impalad.service.create_hs2_client(user=user)
    client.set_configuration({'request_pool': pool})
    try:
      client.execute('select count(*) from functional.alltypes')
      assert False, "query should fail"
    except IMPALA_CONNECTION_EXCEPTION as e:
      # Construct the expected error message.
      expected = ("Rejected query from pool {pool}: current per-{type} load {limit} for "
                  "user '{user}'{group_description} is at or above the {err_type} limit "
                  "{limit} in pool '{pool_that_fails}'".
                  format(pool=pool, type=type, limit=limit, user=user,
                         group_description=group_description, err_type=err_type,
                         pool_that_fails=pool_that_fails))
      assert expected in str(e)

    for query_handle in query_handles:
      query_handle.close()

  class ClientAndHandle:
    """Holder class for a client and query handle"""
    def __init__(self, client, handle):
      self.client = client
      self.handle = handle

    def close(self):
      """close the query"""
      self.client.close_query(self.handle)

  def execute_async_and_wait_for_running(self, impalad, query, user, pool):
    # Execute a query asynchronously, and wait for it to be running.
    client = impalad.service.create_hs2_client(user=user)
    client.set_configuration({'request_pool': pool})
    handle = client.execute_async(query)
    timeout_s = 10
    # Make sure the query has been admitted and is running.
    client.wait_for_impala_state(handle, RUNNING, timeout_s)
    return self.ClientAndHandle(client, handle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml",
      additional_args=("-clamp_query_mem_limit_backend_mem_limit=false "
                       "-default_pool_max_requests 1"),
      make_copy=True),
    statestored_args=_STATESTORED_ARGS)
  def test_pool_config_change_while_queued(self):
    """Tests that the invalid checks work even if the query is queued. Makes sure that a
    queued query is dequeued and rejected if the config is invalid."""
    # IMPALA-9856: This test modify request pool max-query-mem-limit. Therefore, we
    # disable query result spooling so that min reservation of queries being run stay low
    # by not involving BufferedPlanRootSink.
    self.client.set_configuration_option('spool_query_results', 'false')

    # Instantiate ResourcePoolConfig modifier and initialize 'max-query-mem-limit' to 0
    # (unclamped).
    pool_name = "invalidTestPool"
    config_str = "max-query-mem-limit"
    llama_site_path = os.path.join(RESOURCES_DIR, "copy-mem-limit-test-llama-site.xml")
    config = ResourcePoolConfig(
        self.cluster.impalads[0].service, self.get_ac_process().service, llama_site_path)
    config.set_config_value(pool_name, config_str, 0)

    self.client.set_configuration_option('request_pool', pool_name)
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')
    # Setup to queue a query.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    self._wait_for_change_to_profile(sleep_query_handle,
                                      "Admission result: Admitted immediately")
    queued_query_handle = self.client.execute_async("select 2")
    self._wait_for_change_to_profile(queued_query_handle, "Admission result: Queued")

    # Change config to be invalid (max-query-mem-limit < min-query-mem-limit).
    # impala.admission-control.min-query-mem-limit.root.invalidTestPool is set to
    # 25MB in mem-limit-test-llama-site.xml.
    config.set_config_value(pool_name, config_str, 1)
    # Close running query so the queued one gets a chance.
    self.client.close_query(sleep_query_handle)

    # Observe that the queued query fails.
    self.client.wait_for_impala_state(queued_query_handle, ERROR, 20),
    self.client.close_query(queued_query_handle)

    # Change the config back to a valid value
    config.set_config_value(pool_name, config_str, 0)

    # Now do the same thing for change to pool.max-query-mem-limit such that it can no
    # longer accommodate the largest min_reservation.
    # Setup to queue a query.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    queued_query_handle = self.client.execute_async(
      "select * from functional_parquet.alltypes limit 1")
    self._wait_for_change_to_profile(queued_query_handle, "Admission result: Queued")
    # Change config to something less than what is required to accommodate the
    # largest min_reservation (which in this case is 32.09 MB).
    # Setting max-query-mem-limit = min-query-mem-limit = 25MB is sufficient.
    config.set_config_value(pool_name, config_str, 25 * 1024 * 1024)
    # Close running query so the queued one gets a chance.
    self.client.close_query(sleep_query_handle)

    # Observe that the queued query fails.
    self.client.wait_for_impala_state(queued_query_handle, ERROR, 20),
    self.client.close_query(queued_query_handle)

    # Change the config back to a valid value
    config.set_config_value(pool_name, config_str, 0)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
      pool_max_mem=1024 * 1024 * 1024),
    statestored_args=_STATESTORED_ARGS)
  def test_trivial_query(self):
    self.client.set_configuration_option("enable_trivial_query_for_admission", "false")

    # Test the second request does need to queue when trivial query is disabled.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    self._wait_for_change_to_profile(sleep_query_handle,
                                      "Admission result: Admitted immediately")
    trivial_query_handle = self.client.execute_async("select 2")
    self._wait_for_change_to_profile(trivial_query_handle, "Admission result: Queued")
    self.client.close_query(sleep_query_handle)
    self.client.close_query(trivial_query_handle)

    self.client.set_configuration_option("enable_trivial_query_for_admission", "true")
    # Test when trivial query is enabled, all trivial queries should be
    # admitted immediately.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    self._wait_for_change_to_profile(sleep_query_handle,
                                      "Admission result: Admitted immediately")
    # Test the trivial queries.
    self._test_trivial_queries_suc()
    # Test the queries that are not trivial.
    self._test_trivial_queries_negative()
    self.client.close_query(sleep_query_handle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=1,
      pool_max_mem=1),
    statestored_args=_STATESTORED_ARGS)
  def test_trivial_query_low_mem(self):
    # Test whether it will fail for a normal query.
    failed_query_handle = self.client.execute_async(
            "select * from functional_parquet.alltypes limit 100")
    self.client.wait_for_impala_state(failed_query_handle, ERROR, 20)
    self.client.close_query(failed_query_handle)
    # Test it should pass all the trivial queries.
    self._test_trivial_queries_suc()

  class MultiTrivialRunThread(threading.Thread):
    def __init__(self, admit_obj, sql, expect_err=False):
        super(self.__class__, self).__init__()
        self.admit_obj = admit_obj
        self.sql = sql
        self.error = None
        self.expect_err = expect_err

    def run(self):
        try:
          self._test_multi_trivial_query_runs()
        except Exception as e:
          LOG.exception(e)
          self.error = e
          raise e

    def _test_multi_trivial_query_runs(self):
      timeout = 10
      admit_obj = self.admit_obj
      client = admit_obj.cluster.impalads[0].service.create_hs2_client()
      for i in range(100):
        handle = client.execute_async(self.sql)
        if not self.expect_err:
          assert client.wait_for_finished_timeout(handle, timeout)
        else:
          if not client.wait_for_finished_timeout(handle, timeout):
            self.error = Exception("Wait timeout " + str(timeout) + " seconds.")
            break
        result = client.fetch(self.sql, handle)
        assert result.success
        client.close_query(handle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=100000,
      pool_max_mem=1024 * 1024 * 1024),
    statestored_args=_STATESTORED_ARGS)
  def test_trivial_query_multi_runs(self):
    threads = []
    # Test mixed trivial and non-trivial queries workload, and should successfully run
    # for all.
    # Test the case when the number of trivial queries is over the maximum parallelism,
    # which is three.
    for i in range(5):
      thread_instance = self.MultiTrivialRunThread(self, "select 1")
      threads.append(thread_instance)
    # Runs non-trivial queries below.
    for i in range(2):
      thread_instance = self.MultiTrivialRunThread(self, "select sleep(1)")
      threads.append(thread_instance)
    for thread_instance in threads:
      thread_instance.start()
    for thread_instance in threads:
      thread_instance.join()
    for thread_instance in threads:
      if thread_instance.error is not None:
        raise thread_instance.error

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=100000,
      pool_max_mem=1024 * 1024 * 1024),
    statestored_args=_STATESTORED_ARGS)
  def test_trivial_query_multi_runs_fallback(self):
    threads = []
    # Test the case when the number of trivial queries is over the maximum parallelism,
    # which is three, other trivial queries should fall back to normal process and
    # blocked by the long sleep query in our testcase, then leads to a timeout error.
    long_query_handle = self.client.execute_async("select sleep(100000)")
    for i in range(5):
      thread_instance = self.MultiTrivialRunThread(self, "select 1", True)
      threads.append(thread_instance)
    for thread_instance in threads:
      thread_instance.start()
    for thread_instance in threads:
      thread_instance.join()
    has_error = False
    for thread_instance in threads:
      if thread_instance.error is not None:
        assert "Wait timeout" in str(thread_instance.error)
        has_error = True
    assert has_error
    self.client.close_query(long_query_handle)

  def _test_trivial_queries_suc(self):
    self._test_trivial_queries_helper("select 1")
    self._test_trivial_queries_helper(
            "select * from functional_parquet.alltypes limit 0")
    self._test_trivial_queries_helper("select 1, (2 + 3)")
    self._test_trivial_queries_helper(
            "select id from functional_parquet.alltypes limit 0 union all select 1")
    self._test_trivial_queries_helper(
            "select 1 union all select id from functional_parquet.alltypes limit 0")

  # Test the cases that do not fit for trivial queries.
  def _test_trivial_queries_negative(self):
    self._test_trivial_queries_helper("select 1 union all select 2", False)
    self._test_trivial_queries_helper(
            "select * from functional_parquet.alltypes limit 1", False)

    # Cases when the query contains function sleep().
    self._test_trivial_queries_helper(
            "select 1 union all select sleep(1)", False)
    self._test_trivial_queries_helper(
            "select 1 from functional.alltypes limit 0 union all select sleep(1)",
            False)
    self._test_trivial_queries_helper(
            "select a from (select 1 a, sleep(1)) s", False)
    self._test_trivial_queries_helper("select sleep(1)", False)
    self._test_trivial_queries_helper("select ISTRUE(sleep(1))", False)
    self._test_trivial_queries_helper(
            "select 1 from functional.alltypes limit 0 "
            "union all select ISTRUE(sleep(1))",
            False)

  def _test_trivial_queries_helper(self, sql, expect_trivial=True):
    trivial_query_handle = self.client.execute_async(sql)
    if expect_trivial:
      expect_msg = "Admission result: Admitted as a trivial query"
    else:
      expect_msg = "Admission result: Queued"
    self._wait_for_change_to_profile(trivial_query_handle, expect_msg)
    self.client.close_query(trivial_query_handle)

  def _wait_for_change_to_profile(
      self, query_handle, search_string, timeout=20, client=None):
    if client is None:
      client = self.client
    for _ in range(timeout * 10):
      profile = client.get_runtime_profile(query_handle)
      if search_string in profile:
        return
      sleep(0.1)
    assert False, "Timed out waiting for change to profile\nSearch " \
                  "String: {0}\nProfile:\n{1}".format(search_string, str(profile))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=(impalad_admission_ctrl_flags(
        max_requests=1, max_queued=3, pool_max_mem=1024 * 1024 * 1024)
        + " --admission_control_stale_topic_threshold_ms={0}".format(
          STALE_TOPIC_THRESHOLD_MS)),
      statestored_args=_STATESTORED_ARGS)
  def test_statestore_outage(self):
    self.client.set_configuration_option('enable_trivial_query_for_admission', 'false')

    """Test behaviour with a failed statestore. Queries should continue to be admitted
    but we should generate diagnostics about the stale topic."""
    self.cluster.statestored.kill()
    impalad = self.get_ac_process()
    # Sleep until the update should be definitely stale.
    sleep(STALE_TOPIC_THRESHOLD_MS / 1000. * 1.5)
    ac_json = impalad.service.get_debug_webpage_json('/admission')
    ms_since_update = ac_json["statestore_admission_control_time_since_last_update_ms"]
    assert ms_since_update > STALE_TOPIC_THRESHOLD_MS
    assert ("Warning: admission control information from statestore is stale:" in
        ac_json["statestore_update_staleness_detail"])

    # Submit a batch of queries. One should get to run, one will be rejected because
    # of the full queue, and the others will run after being queued.
    STMT = "select sleep(100)"
    TIMEOUT_S = 60
    NUM_QUERIES = 5
    profiles = self._execute_and_collect_profiles([STMT for i in range(NUM_QUERIES)],
        TIMEOUT_S, allow_query_failure=True)
    ADMITTED_STALENESS_WARNING = \
        "Warning: admission control information from statestore is stale"
    ADMITTED_STALENESS_PROFILE_ENTRY = \
        "Admission control state staleness: " + ADMITTED_STALENESS_WARNING

    num_queued = 0
    num_admitted_immediately = 0
    num_rejected = 0
    for profile in profiles:
      if "Admission result: Admitted immediately" in profile:
        assert ADMITTED_STALENESS_PROFILE_ENTRY in profile, profile
        num_admitted_immediately += 1
      elif "Admission result: Rejected" in profile:
        num_rejected += 1
        # Check that the rejection error returned to the client contains a warning.
        query_statuses = [line for line in profile.split("\n")
                          if "Query Status:" in line]
        assert len(query_statuses) == 1, profile
        assert ADMITTED_STALENESS_WARNING in query_statuses[0]
      else:
        assert "Admission result: Admitted (queued)" in profile, profile
        assert ADMITTED_STALENESS_PROFILE_ENTRY in profile, profile

        # Check that the queued reason contains a warning.
        queued_reasons = [line for line in profile.split("\n")
                         if "Initial admission queue reason:" in line]
        assert len(queued_reasons) == 1, profile
        assert ADMITTED_STALENESS_WARNING in queued_reasons[0]
        num_queued += 1
    assert num_admitted_immediately == 1
    assert num_queued == 3
    assert num_rejected == NUM_QUERIES - num_admitted_immediately - num_queued

  @pytest.mark.execute_serially
  def test_impala_server_startup_delay(self):
    """This test verifies that queries get queued when the coordinator has already started
    accepting client connections during startup, but the local backend descriptor is not
    yet available."""
    server_start_delay_s = 20
    # We need to start the cluster here instead of during setup_method() so we can launch
    # it from a separate thread.

    def start_cluster():
      LOG.info("Starting cluster")
      impalad_args = "--debug_actions=IMPALA_SERVER_END_OF_START:SLEEP@%s" % (
          1000 * server_start_delay_s)
      self._start_impala_cluster(['--impalad_args=%s' % impalad_args])

    # Initiate the cluster start
    start_cluster_thread = threading.Thread(target=start_cluster)
    start_cluster_thread.start()

    # Wait some time to arrive at IMPALA_SERVER_END_OF_START
    sleep(server_start_delay_s)

    # With a new client, execute a query and observe that it gets queued and ultimately
    # succeeds.
    client = self.create_impala_client()
    result = self.execute_query_expect_success(client, "select sleep(1)")
    start_cluster_thread.join()
    profile = result.runtime_profile
    reasons = self.__extract_init_queue_reasons([profile])
    assert len(reasons) == 1
    assert "Coordinator not registered with the statestore." in reasons[0]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1)
  def test_release_backends(self, vector):
    """Test that executor backends are shutdown when they complete, that completed
    executor backends release their admitted memory, and that
    NumCompletedBackends is updated each time an executor backend completes."""
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')

    # Craft a query where part of the executor backends completes, while the rest remain
    # running indefinitely. The query forces the 'lineitem' table to be treated as the
    # small table even though it is bigger than the 'customer' table. This forces the
    # small table scan ('lineitem' scan) to run on two nodes and the big table scan
    # ('customers' scan) to run on a single node. By using debug actions to force the
    # big table scan to hang indefinitely, the small table scan should finish quickly.
    # This causes one executor backend to complete quickly, and causes the other one to
    # hang.
    vector.get_value('exec_option')['debug_action'] = '0:GETNEXT:WAIT'
    query = "select STRAIGHT_JOIN * from tpch.customer JOIN /* +BROADCAST */ " \
            "tpch.lineitem where customer.c_custkey = lineitem.l_orderkey limit 100"

    # Amount of time to wait for the query to reach the running state before throwing a
    # Timeout exception.
    timeout = 10

    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    try:
      # Wait for the query to reach the running state (it should never reach the finished
      # state because of the 'WAIT' debug action), wait for the 'lineitem' scan to
      # complete, and then validate that one of the executor backends shutdowns and
      # releases its admitted memory.
      self.client.wait_for_impala_state(handle, RUNNING, timeout)
      # Once the 'lineitem' scan completes, NumCompletedBackends should be 1.
      self.assert_eventually(60, 1, lambda: "NumCompletedBackends: 1 (1)"
          in self.client.get_runtime_profile(handle))
      get_num_completed_backends(self.cluster.impalads[0].service,
        self.client.handle_id(handle)) == 1
      mem_admitted =\
          get_mem_admitted_backends_debug_page(self.cluster, self.get_ac_process())
      num_executor_zero_admitted = 0
      for executor_mem_admitted in mem_admitted['executor']:
        if executor_mem_admitted == 0:
          num_executor_zero_admitted += 1
      assert num_executor_zero_admitted == 1
    finally:
      # Once the query is closed, validate that all backends have shutdown.
      self.client.close_query(handle)
      mem_admitted = get_mem_admitted_backends_debug_page(self.cluster)
      assert mem_admitted['coordinator'] == 0
      for executor_mem_admitted in mem_admitted['executor']:
        assert executor_mem_admitted == 0

  def __assert_systables_query(self, profile, expected_coords=None,
      expected_frag_counts=None):
    """Asserts the per-host fragment instances are correct in the provided profile."""

    if expected_coords is None:
      expected_coords = self.cluster.get_all_coordinators()

    populate_frag_count = False
    if expected_frag_counts is None:
      populate_frag_count = True
      expected_frag_counts = []

    expected = []
    for i, val in enumerate(expected_coords):
      if populate_frag_count:
        if i == 0:
          expected_frag_counts.append(2)
        else:
          expected_frag_counts.append(1)

      expected.append("{0}:{1}({2})".format(val.service.hostname, val.service.krpc_port,
          expected_frag_counts[i]))

    # Assert the correct request pool was used.
    req_pool = re.search(r'\n\s+Request Pool:\s+(.*?)\n', profile)
    assert req_pool, "Did not find request pool in query profile"
    assert req_pool.group(1) == "root.onlycoords"

    # Assert the fragment instances only ran on the coordinators.
    perhost_frags = re.search(r'\n\s+Per Host Number of Fragment Instances:\s+(.*?)\n',
        profile)
    assert perhost_frags
    sorted_hosts = " ".join(sorted(perhost_frags.group(1).split(" ")))
    assert sorted_hosts
    assert sorted_hosts == " ".join(expected)

    # Assert the frontend selected the first executor group.
    expected_verdict = "Assign to first group because only coordinators request pool " \
        "specified"
    fe_verdict = re.search(r'\n\s+Executor group 1:\n\s+Verdict: (.*?)\n', profile)
    assert fe_verdict, "No frontend executor group verdict found."
    assert fe_verdict.group(1) == expected_verdict, "Incorrect verdict found"

  def __run_assert_systables_query(self, vector, expected_coords=None,
        expected_frag_counts=None, query=ACTIVE_SQL):
    """Runs a query using an only coordinators request pool and asserts the per-host
        fragment instances are correct. This function can only be called from tests that
        configured the cluster to use 'fair-scheduler-onlycoords.xml' and
        'llama-site-onlycoords.xml'."""

    vector.set_exec_option('request_pool', 'onlycoords')
    with self.create_impala_client(protocol=vector.get_value('protocol')) as client:
      result = self.execute_query_using_client(client, query, vector)
      assert result.success

    self.__assert_systables_query(result.runtime_profile, expected_coords,
      expected_frag_counts)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=3, cluster_size=5,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_happy_path(self, vector):
    """Asserts queries set to use an only coordinators request pool run all the fragment
       instances on all coordinators and no executors even if the query includes
       non-system tables."""
    self.wait_for_wm_init_complete()

    # Execute a query that only selects from a system table using a request pool that is
    # only coordinators.
    self.__run_assert_systables_query(vector)

    # Execute a query that joins a non-system table with a system table using a request
    # pool that is only coordinators. All fragment instances will run on the coordinators
    # without running any on the executors.
    self.__run_assert_systables_query(
        vector=vector,
        expected_frag_counts=[4, 2, 2],
        query="select a.test_name, b.db_user from functional.jointbl a inner join "
              "{} b on a.test_name = b.db_name".format(QUERY_TBL_LIVE)),

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=3, cluster_size=3,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_no_executors(self, vector):
    """Asserts queries that only select from the active queries table run even if no
       executors are running."""
    self.wait_for_wm_init_complete()
    self.__run_assert_systables_query(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=3, cluster_size=5,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_one_coord_quiescing(self, vector):
    """Asserts quiescing coordinators do not run fragment instances for queries that only
       select from the active queries table."""
    self.wait_for_wm_init_complete()

    # Quiesce the second coordinator.
    all_coords = self.cluster.get_all_coordinators()
    coord_to_quiesce = all_coords[1]
    self.execute_query_expect_success(self.client, ": shutdown('{}:{}')".format(
        coord_to_quiesce.service.hostname, coord_to_quiesce.service.krpc_port))

    # Ensure only two coordinators process a system tables query.
    self.__run_assert_systables_query(
        vector=vector,
        expected_coords=[all_coords[0], all_coords[2]])
    # Wait until quiescing coordinator exit.
    coord_to_quiesce.wait_for_exit()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=3, cluster_size=5,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_one_coord_terminate(self, vector):
    """Asserts a force terminated coordinator is eventually removed from the list of
       active coordinators."""
    self.wait_for_wm_init_complete()

    # Abruptly end the third coordinator.
    all_coords = self.cluster.get_all_coordinators()
    coord_to_term = all_coords[2]
    coord_to_term.kill()

    vector.set_exec_option('request_pool', 'onlycoords')

    done_waiting = False
    iterations = 0
    while not done_waiting and iterations < 20:
      try:
        result = self.execute_query_using_client(self.client, ACTIVE_SQL, vector)
        assert result.success
        done_waiting = True
      except Exception as e:
        # Since the coordinator was not gracefully shut down, it never had a change to
        # send a quiescing message. Thus, the statestore will take some time to detect
        # that coordinator is gone. During that time, queries again system tables will
        # fail as the now terminated coordinator will still be sent rpcs.
        if re.search(r"Exec\(\) rpc failed: Network error: "
            r"Client connection negotiation failed: client connection to .*?:{}: "
            r"connect: Connection refused".format(coord_to_term.service.krpc_port),
            str(e)):
          # Expected error, coordinator down not yet detected.
          iterations += 1
          sleep(3)
        else:
          raise e

    assert done_waiting
    self.__assert_systables_query(result.runtime_profile, [all_coords[0], all_coords[1]])
    # Ensure that coord_to_term has truly exit.
    coord_to_term.wait_for_exit()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=3, cluster_size=5,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_add_coord(self, vector):
    self.wait_for_wm_init_complete()

    # Add a coordinator to the cluster.
    cluster_size = len(self.cluster.impalads)
    self._start_impala_cluster(
        options=[
            "--impalad_args={0} {1}".format(
              WORKLOAD_MGMT_IMPALAD_FLAGS,
              impalad_admission_ctrl_config_args(
                fs_allocation_file="fair-scheduler-onlycoords.xml",
                llama_site_file="llama-site-onlycoords.xml"))],
        add_impalads=True,
        cluster_size=6,
        num_coordinators=1,
        use_exclusive_coordinators=True,
        wait_for_backends=False)

    self.assert_log_contains("impalad_node" + str(cluster_size), "INFO",
        "join Impala Service pool")

    # Assert the new coordinator ran a fragment instance.
    self.__run_assert_systables_query(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1, cluster_size=1,
      workload_mgmt=True, impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-onlycoords.xml",
        llama_site_file="llama-site-onlycoords.xml",
        additional_args="--expected_executor_group_sets=root.group-set-small:1,"
                        "root.group-set-large:2 "
                        "--num_expected_executors=2 --executor_groups=coordinator"),
      statestored_args=_STATESTORED_ARGS)
  def test_coord_only_pool_exec_groups(self, vector):
    """Asserts queries using only coordinators request pools can run successfully when
       executor groups are configured."""
    self.wait_for_wm_init_complete()
    executor_flags = '--shutdown_grace_period_s=0 --shutdown_deadline_s=60 '

    # Assert queries can be run when no executors are started.
    self.__run_assert_systables_query(vector)
    # If not using admissiond, there should be 2 statestore subscribers now
    # (1 impalad and 1 catalogd). Otherwise, admissiond is the 3rd statestore subscriber.
    expected_subscribers = 3 if self.get_ac_log_name() == 'admissiond' else 2
    expected_num_impalads = 1

    # Add a single executor for the small executor group set.
    expected_subscribers += 1
    expected_num_impalads += 1
    self._start_impala_cluster(
        options=[
            "--impalad_args=--executor_groups=root.group-set-small-group-000:1 "
            + executor_flags],
        add_executors=True,
        cluster_size=1,
        expected_subscribers=expected_subscribers,
        expected_num_impalads=expected_num_impalads)
    self.__run_assert_systables_query(vector)

    # Add two executors for the large executor group set.
    expected_subscribers += 2
    expected_num_impalads += 2
    self._start_impala_cluster(
        options=[
            "--impalad_args=--executor_groups=root.group-set-large-group-000:2 "
            + executor_flags],
        add_executors=True,
        cluster_size=2,
        expected_subscribers=expected_subscribers,
        expected_num_impalads=expected_num_impalads)
    self.__run_assert_systables_query(vector)


class TestAdmissionControllerWithACService(TestAdmissionController):
  """Runs all of the tests from TestAdmissionController but with the second impalad in the
  minicluster configured to perform all admission control."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerWithACService, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('admission_control_rpc_compress_threshold_bytes', 0, 1))

  def get_ac_process(self):
    return self.cluster.admissiond

  def get_ac_log_name(self):
    return "admissiond"

  def setup_method(self, method):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.enable_admission_service(method)
    super(TestAdmissionControllerWithACService, self).setup_method(method)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--admission_max_retry_time_s=5")
  def test_admit_query_retry(self):
    """Tests that if the AdmitQuery rpc fails with a network error, either before or after
    reaching the admissiond and being processed, it will be retried and the query will
    eventually succeed."""
    # Query designed to run for a few seconds.
    query = "select count(*) from functional.alltypes where int_col = sleep(10)"
    # Run the query with a debug action that will sometimes return errors from AdmitQuery
    # even though the admissiond started scheduling successfully. Tests the path where the
    # admissiond received multiple AdmitQuery rpcs with the same query id.
    before_kill_handle = self.execute_query_async(
        query, {"DEBUG_ACTION": "ADMIT_QUERY_NETWORK_ERROR:FAIL@0.5"})
    timeout_s = 10
    # Make sure the query is through admission control before killing the admissiond. It
    # should be unaffected and finish successfully.
    self.client.wait_for_impala_state(before_kill_handle, RUNNING, timeout_s)
    self.cluster.admissiond.kill()
    result = self.client.fetch(query, before_kill_handle)
    assert result.data == ["730"]

    # Run another query and sleep briefly before starting the admissiond again. It should
    # retry until the admissiond is available again and then succeed.
    after_kill_handle = self.execute_query_async(query)
    sleep(1)
    self.cluster.admissiond.start()
    result = self.client.fetch(query, after_kill_handle)
    assert result.data == ["730"]

    # Kill the admissiond again and don't restart it this time. The query should
    # eventually time out on retrying and fail.
    self.cluster.admissiond.kill()
    no_restart_handle = self.execute_query_async(query)
    try:
      result = self.client.fetch(query, no_restart_handle)
      assert False, "Query should have failed"
    except IMPALA_CONNECTION_EXCEPTION as e:
      assert "Failed to admit query after waiting " in str(e)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--vmodule admission-controller=3 --default_pool_max_requests=1 "
      "--debug_actions=IMPALA_SERVICE_POOL:127.0.0.1:29500:ReleaseQuery:FAIL@1.0")
  def test_release_query_failed(self):
    """Tests that if the ReleaseQuery rpc fails, the query's resources will eventually be
    cleaned up. Uses the --debug_action flag to simulate rpc failures, and sets max
    requests for the default pool as the number of requests per pool is decremented when
    the entire query is released."""
    # Query designed to run for a few minutes.
    query = "select count(*) from functional.alltypes where int_col = sleep(10000)"
    handle1 = self.execute_query_async(query)
    timeout_s = 10
    # Make sure the first query has been admitted.
    self.client.wait_for_impala_state(handle1, RUNNING, timeout_s)

    # Run another query. This query should be queued because only 1 query is allowed in
    # the default pool.
    handle2 = self.client.execute_async(query)
    self._wait_for_change_to_profile(handle2, "Admission result: Queued")

    # Cancel the first query. It's resources should be released and the second query
    # should be admitted.
    self.client.cancel(handle1)
    self.client.close_query(handle1)
    self.client.wait_for_impala_state(handle2, RUNNING, timeout_s)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--vmodule admission-controller=3 --default_pool_max_requests=1 ",
      disable_log_buffering=True)
  def test_coord_not_registered_in_ac(self):
    """Regression test for IMPALA-12057. Verifies that no excessive logs are
    generated when a query is queued in the  admission controller and the coordinator
    hosting the admitted query goes down. Prior to IMPALA-12057, such a scenario could
    cause excessive logging during dequeue attempts. After the fix, such logging should
    no longer occur and the queued query should be rejected."""
    # Query designed to run for a few minutes.
    query = "select count(*) from functional.alltypes where int_col = sleep(10000)"
    timeout_s = 10
    keys = [
      "admission-controller.total-admitted.default-pool",
      "admission-controller.total-queued.default-pool",
      "admission-controller.total-dequeued.default-pool",
      "admission-controller.total-rejected.default-pool",
    ]

    def get_ac_metrics(service, keys, default=0):
      return service.get_metric_values(keys, [default] * len(keys))
    for i in range(1, 4):
      handle1 = self.client.execute_async(query)
      # Make sure the first query has been admitted.
      self.client.wait_for_impala_state(handle1, RUNNING, timeout_s)

      # Run another query. This query should be queued because only 1 query is allowed in
      # the default pool.
      handle2 = self.client.execute_async(query)
      self._wait_for_change_to_profile(handle2, "Admission result: Queued")
      # Kill the first coordinator.
      all_coords = self.cluster.get_all_coordinators()
      all_coords[0].kill()
      # Wait briefly to allow the potential excessive logging to occur.
      sleep(3)
      self.assert_log_contains(self.get_ac_log_name(), 'INFO',
          "Coordinator not registered with the statestore", expected_count=0)
      # Verify the metrics.
      cur_admission_metrics = get_ac_metrics(self.cluster.admissiond.service, keys)
      assert cur_admission_metrics == [i, i, i, i]
      all_coords[0].start()
    self.assert_log_contains_multiline(self.get_ac_log_name(), 'INFO',
        "The coordinator no longer exists")

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--vmodule admission-controller=3 --default_pool_max_requests=1 "
    "--queue_wait_timeout_ms=60000 ")
  def test_kill_statestore_with_queries_running(self):
    long_query = "select count(*), sleep(10000) from functional.alltypes limit 1"
    short_query = "select count(*) from functional.alltypes limit 1"
    timeout_s = 60

    handle1 = self.client.execute_async(long_query)
    # Make sure the first query has been admitted.
    self.client.wait_for_impala_state(handle1, RUNNING, timeout_s)

    # Run another query. This query should be queued because only 1 query is allowed in
    # the default pool.
    handle2 = self.client.execute_async(short_query)
    self._wait_for_change_to_profile(handle2, "Admission result: Queued")

    # Restart the statestore while queries are running/queued.
    statestore = self.cluster.statestored
    statestore.kill()
    statestore.start()

    # Verify that both queries eventually complete.
    self.client.wait_for_impala_state(handle1, FINISHED, timeout_s)
    self.client.close_query(handle1)
    self.client.wait_for_impala_state(handle2, FINISHED, timeout_s)
    self.client.close_query(handle2)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--vmodule admission-controller=3 --default_pool_max_requests=1 "
    "--queue_wait_timeout_ms=60000 ", disable_log_buffering=True)
  def test_kill_coord_with_queries_running(self):
    long_query = "select count(*), sleep(1000000000) from functional.alltypes limit 1"
    short_query = "select count(*) from functional.alltypes limit 1"
    timeout_s = 10

    all_coords = self.cluster.get_all_coordinators()
    assert len(all_coords) >= 2, "Test requires at least two coordinators"
    coord1 = all_coords[0]
    coord2 = all_coords[1]

    # Make sure the first query has been admitted.
    client1 = coord1.service.create_hs2_client()
    handle1 = client1.execute_async(long_query)
    client1.wait_for_impala_state(handle1, RUNNING, timeout_s)
    query_id1 = client1.handle_id(handle1)

    # Run another query. This query should be queued because only 1 query is allowed in
    # the default pool.
    client2 = coord2.service.create_hs2_client()
    handle2 = client2.execute_async(short_query)
    self._wait_for_change_to_profile(handle2, "Admission result: Queued", client=client2)

    # Kill the coordinator handling the running query.
    coord1.kill()
    try:
      client1.close_query(handle1)
    except Exception:
      pass

    # The first query should be canceled after coord1 is killed,
    # allowing the queued query to run.
    admissiond_log = self.get_ac_log_name()
    self.assert_log_contains(admissiond_log, 'INFO',
      "Released query id={}".format(query_id1), expected_count=1)
    client2.wait_for_impala_state(handle2, FINISHED, timeout_s)
    client2.close_query(handle2)

    # Cleanup.
    client1.close()
    client2.close()

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--vmodule admission-controller=3 --mem_limit=10MB ")
  def test_admission_service_low_mem_limit(self):
    EXPECTED_REASON = "Admission rejected due to memory pressure"
    # Test whether it will fail for a normal query.
    failed_query_handle = self.client.execute_async(
            "select * from functional_parquet.alltypes limit 100")
    self.client.wait_for_impala_state(failed_query_handle, ERROR, 20)
    profile = self.client.get_runtime_profile(failed_query_handle)
    assert EXPECTED_REASON in profile, \
      "Expected reason '{0}' not found in profile: {1}".format(EXPECTED_REASON, profile)
    self.client.close_query(failed_query_handle)
    # Test it should pass all the trivial queries.
    self._test_trivial_queries_suc()

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  def test_retained_removed_coords_size(self):
    # Use a flag value below the hard cap (1000). Expect the value to be accepted.
    self._start_impala_cluster([
      '--impalad_args=--vmodule admission-controller=3',
      '--impalad_args=--cluster_membership_retained_removed_coords=10',
      'disable_log_buffering=True'])
    self.assert_log_contains(self.get_ac_log_name(), 'INFO',
      "Using cluster membership removed coords size 10", expected_count=1)

    # Use invalid values. Expect the cluster to fail to start.
    try:
      self._start_impala_cluster([
        '--impalad_args=--vmodule admission-controller=3',
        '--impalad_args=--cluster_membership_retained_removed_coords=10000',
        'disable_log_buffering=True'])
      self.fail("Expected CalledProcessError was not raised.")
    except subprocess.CalledProcessError as e:
      assert "cluster_membership_retained_removed_coords" in str(e)

    try:
      self._start_impala_cluster([
        '--impalad_args=--vmodule admission-controller=3',
        '--impalad_args=--cluster_membership_retained_removed_coords=0',
        'disable_log_buffering=True'])
      self.fail("Expected CalledProcessError was not raised.")
    except subprocess.CalledProcessError as e:
      assert "cluster_membership_retained_removed_coords" in str(e)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=("--vmodule=admission-control-service=3 --default_pool_max_requests=1 "
      "--queue_wait_timeout_ms=1"),
      disable_log_buffering=True)
  def test_admission_state_map_mem_leak(self):
    """
    Regression test to reproduce IMPALA-14276.
    Steps:
    1. Submit a long-running query to coord1 and let it run.
    2. Repeatedly submit short queries to coord2 that get queued and time out due to
       admission limits.
    3. Get memory usage before and after to check for possible memory leak in
       admissiond.
    """

    # Long-running query that blocks a request slot.
    long_query = "select count(*) from functional.alltypes where int_col = sleep(10000)"
    # Simple short query used to trigger queuing and timeout.
    short_query = "select count(*) from functional.alltypes limit 1"

    # Max timeout for waiting on query state transitions.
    timeout_s = 10

    ac = self.cluster.admissiond.service
    all_coords = self.cluster.get_all_coordinators()
    assert len(all_coords) >= 2, "Test requires at least two coordinators"

    coord1, coord2 = all_coords[0], all_coords[1]

    # Submit long query to coord1 to occupy the admission slot.
    client1 = coord1.service.create_hs2_client()
    handle1 = client1.execute_async(long_query)
    client1.wait_for_impala_state(handle1, RUNNING, timeout_s)

    ac.wait_for_metric_value("admission-control-service.num-queries", 1)
    # Capture memory usage before stressing the system.
    old_total_bytes = ac.get_metric_value("tcmalloc.bytes-in-use")
    assert old_total_bytes != 0

    # Submit short queries to coord2 which will be queued and time out.
    client2 = coord2.service.create_hs2_client()
    number_of_iterations = 500
    for i in range(number_of_iterations):
        handle2 = client2.execute_async(short_query)
        self._wait_for_change_to_profile(
            handle2,
            "Query Status: Admission for query exceeded timeout",
            client=client2,
            timeout=timeout_s)
        client2.close_query(handle2)

    # Capture memory usage after the test.
    new_total_bytes = ac.get_metric_value("tcmalloc.bytes-in-use")

    # Ensure memory usage has not grown more than 10%, indicating no leak.
    assert new_total_bytes < old_total_bytes * 1.1
    # Check if the admission state map size stays 1 all the time, which is
    # the long running query.
    admission_state_size = ac.get_metric_value("admission-control-service.num-queries")
    assert admission_state_size == 1

    # Cleanup clients.
    client1.close()
    client2.close()

    # Verify num queries return to 0.
    ac.wait_for_metric_value(
      "admission-control-service.num-queries", 0)
    num_queries_hwm = \
      ac.get_metric_value("admission-control-service.num-queries-high-water-mark")
    assert num_queries_hwm > 1

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=("--debug_actions=INBOUND_GETQUERYSTATUS_REJECT:FAIL@0.2 "
                    "--vmodule=admission-control-service=3 "
                    "--default_pool_max_requests=1 --queue_wait_timeout_ms=1"),
      disable_log_buffering=True
  )
  def test_admission_state_map_leak_in_dequeue(self):
    """
    Regression test for IMPALA-14605.
    We verify that cancellations of dequeued queries when there are dropped due to
    backpressure errors do not leak memory in the admission_state_map.
    """

    # Log patterns to verify.
    LOG_PATTERN_LEAK_FIX = "Dequeued cancelled query"
    LOG_PATTERN_INJECTION = "dropped due to backpressure"

    ac_service = self.cluster.admissiond.service

    # The workload for concurrent clients.
    def run_client_workload(client_index):
        impalad = self.cluster.impalads[client_index % len(self.cluster.impalads)]
        client = impalad.service.create_hs2_client()
        query = "select sleep(1000)"
        # Run multiple iterations to maximize race condition probability.
        for i in range(10):
          try:
            handle = client.execute_async(query)
            client.wait_for_finished_timeout(handle, 3000)
            client.close_query(handle)
          except Exception:
            pass
        try:
          client.close()
        except Exception:
          pass
    threads = []
    # Use multiple clients to do the queries concurrently.
    num_clients = 5
    for i in range(num_clients):
        t = threading.Thread(target=run_client_workload, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # Verify the logs and metrics.
    ac_service.wait_for_metric_value(
      "admission-control-service.num-queries", 0)
    num_queries_hwm = \
      ac_service.get_metric_value("admission-control-service.num-queries-high-water-mark")
    assert num_queries_hwm > 1
    self.assert_log_contains_multiline("admissiond", "INFO", LOG_PATTERN_INJECTION)
    self.assert_log_contains_multiline("admissiond", "INFO", LOG_PATTERN_LEAK_FIX)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--vmodule admission-controller=3 "
      "--debug_actions=IMPALA_SERVICE_POOL:127.0.0.1:29500:ReleaseQueryBackends:FAIL@1.0 "
      "--admission_control_slots=1 --executor_groups=default-pool-group1")
  def test_release_query_backends_failed(self):
    """Tests that if the ReleaseQueryBackends rpc fails, the query's resources will
    eventually be cleaned up. Uses the --debug_action flag to simulate rpc failures, and
    sets the number of slots for a single pool as slot usage per executor is decremented
    when releasing individual backends."""
    # Query designed to run for a few minutes.
    query = "select count(*) from functional.alltypes where int_col = sleep(10000)"
    handle1 = self.execute_query_async(query)
    timeout_s = 10
    # Make sure the first query has been admitted.
    self.client.wait_for_impala_state(handle1, RUNNING, timeout_s)

    # Run another query. This query should be queued because the executor group only has 1
    # slot.
    handle2 = self.client.execute_async(query)
    self._wait_for_change_to_profile(handle2, "Admission result: Queued")

    # Cancel the first query. It's resources should be released and the second query
    # should be admitted.
    self.client.cancel(handle1)
    self.client.close_query(handle1)
    self.client.wait_for_impala_state(handle2, RUNNING, timeout_s)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--vmodule admission-controller=3 --default_pool_max_requests=1")
  def test_coordinator_failed(self):
    """Tests that if a coordinator fails, the resources for queries running at that
    coordinator are eventually released."""
    # Query designed to run for a few minutes.
    query = "select count(*) from functional.alltypes where int_col = sleep(10000)"
    impalad1 = self.cluster.impalads[0]
    client1 = impalad1.service.create_hs2_client()
    handle1 = client1.execute_async(query)
    timeout_s = 10
    # Make sure the first query has been admitted.
    client1.wait_for_impala_state(handle1, RUNNING, timeout_s)

    # Run another query with a different coordinator. This query should be queued because
    # only 1 query is allowed in the default pool.
    impalad2 = self.cluster.impalads[1]
    client2 = impalad2.service.create_hs2_client()
    handle2 = client2.execute_async(query)
    self._wait_for_change_to_profile(handle2, "Admission result: Queued", client=client2)

    # Kill the coordinator for the first query. The resources for the query should get
    # cleaned up and the second query should be admitted.
    impalad1.kill()
    client2.wait_for_impala_state(handle2, RUNNING, timeout_s)


class TestAdmissionControllerStress(TestAdmissionControllerBase):
  """Submits a number of queries (parameterized) with some delay between submissions
  (parameterized) and the ability to submit to one impalad or many in a round-robin
  fashion. Each query is submitted on a separate thread. After admission, the query
  thread will fetch rows slowly and wait for the main thread to notify it to
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

  BATCH_SIZE = 100

  @classmethod
  def add_test_dimensions(cls):
    super(TestAdmissionControllerStress, cls).add_test_dimensions()
    # Slow down test query by setting low batch_size.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
      cluster_sizes=[0], disable_codegen_options=[False],
      batch_sizes=[cls.BATCH_SIZE]))
    # Turning off result spooling allows us to better control query execution by
    # controlling the number of rows fetched. This allows us to maintain resource
    # usage among backends.
    add_mandatory_exec_option(cls, 'spool_query_results', 0)
    # Set 100ms long poling time to get faster initial response.
    add_mandatory_exec_option(cls, 'long_polling_time_ms', 100)
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('round_robin_submission', *ROUND_ROBIN_SUBMISSION))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('submission_delay_ms', *SUBMISSION_DELAY_MS))

    # The number of queries to submit. The test does not support fewer queries than
    # MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES to keep some validation logic
    # simple.
    num_queries = 40
    if ImpalaTestClusterProperties.get_instance().has_code_coverage():
      # Code coverage builds can't handle the increased concurrency.
      num_queries = 15
    elif cls.exploration_strategy() == 'core':
      num_queries = 30
    assert num_queries >= MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('num_queries', num_queries))

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Override default constraint from CustomClusterTestSuite
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('submission_delay_ms') == 0)
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('round_robin_submission'))

  def setup_method(self, method):
    super(TestAdmissionControllerStress, self).setup_method(method)
    # All threads are stored in this list and it's used just to make sure we clean up
    # properly in teardown.
    self.all_threads = list()
    # Each submission thread will append() itself to this list if the query begins
    # execution.  The main thread will access this list to determine which threads are
    # executing queries that can be cancelled (it will pop() elements from the front of
    # the list). The individual operations on the list are atomic and thread-safe thanks
    # to the GIL.
    self.executing_threads = list()
    # Exit event to break any sleep/wait.
    self.exit = threading.Event()

    def quit(signum, _frame):
      signame = signal.Signals(signum).name
      LOG.fatal('Signal handler called with signal {} ({}): {}'.format(
        signum, signame, _frame))
      self.exit.set()

    signal.signal(signal.SIGTERM, quit)
    signal.signal(signal.SIGINT, quit)
    signal.signal(signal.SIGHUP, quit)

  def teardown_method(self, method):
    # Set shutdown for all threads (cancel if needed)
    self.exit.set()

    # Wait for all threads to exit
    for thread in self.all_threads:
      thread.join(5)
      LOG.info("Join thread for query num %s %s", thread.query_num,
          "TIMED OUT" if thread.is_alive() else "")
    super(TestAdmissionControllerStress, self).teardown_method(method)

  def should_run(self):
    return not self.exit.is_set()

  def get_ac_processes(self):
    """Returns a list of all Processes which may be used to perform admission control. If
    round-robin submission is not being used, only the first Process in this list will
    perform admission control."""
    return self.cluster.impalads

  def get_admission_metrics(self):
    """
    Returns a map of the admission metrics, aggregated across all of the impalads.

    The metrics names are shortened for brevity: 'admitted', 'queued', 'dequeued',
    'rejected', 'released', and 'timed-out'.
    """
    metrics = {'admitted': 0, 'queued': 0, 'dequeued': 0, 'rejected': 0,
               'released': 0, 'timed-out': 0}
    for impalad in self.ac_processes:
      keys = [metric_key(self.pool_name, 'total-%s' % short_name)
              for short_name in metrics.keys()]
      values = impalad.service.get_metric_values(keys, [0] * len(keys))
      for short_name, value in zip(metrics.keys(), values):
        metrics[short_name] += value
    return metrics

  def get_consistent_admission_metrics(self, num_submitted):
    """Same as get_admission_metrics() except retries until it gets consistent metrics for
    num_submitted queries. See IMPALA-6227 for an example of problems with inconsistent
    metrics where a dequeued query is reflected in dequeued but not admitted."""
    ATTEMPTS = 5
    for i in range(ATTEMPTS):
      metrics = self.get_admission_metrics()
      admitted_immediately = num_submitted - metrics['queued'] - metrics['rejected']
      if admitted_immediately + metrics['dequeued'] == metrics['admitted']:
        return metrics
      LOG.info("Got inconsistent metrics {0}".format(metrics))
    assert False, "Could not get consistent metrics for {0} queries after {1} attempts: "\
        "{2}".format(num_submitted, ATTEMPTS,
                     metrics)

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
    while self.should_run():
      current = self.get_admission_metrics()
      log_metrics("wait_for_metric_changes, current=", current)
      deltas = compute_metric_deltas(current, initial)
      delta_sum = sum([deltas[x] for x in metric_names])
      LOG.info("DeltaSum=%s Deltas=%s (Expected=%s for metrics=%s)",
          delta_sum, deltas, expected_delta, metric_names)
      if delta_sum >= expected_delta:
        LOG.info("Found all %s metrics after %s seconds", delta_sum,
            round(time() - start_time, 1))
        return (deltas, current)
      assert (time() - start_time < STRESS_TIMEOUT),\
          "Timed out waiting {0} seconds for metrics {1} delta {2} "\
          "current {3} initial {4}" .format(
              STRESS_TIMEOUT, ','.join(metric_names), expected_delta, str(current),
              str(initial))
      sleep(1)

  def wait_for_statestore_updates(self, heartbeats):
    """Waits for a number of admission control statestore updates from all impalads."""
    start_time = time()
    init = dict()
    curr = dict()
    for impalad in self.impalads:
      init[impalad] = impalad.service.get_metric_value(
          REQUEST_QUEUE_UPDATE_INTERVAL)['count']
      curr[impalad] = init[impalad]

    while self.should_run():
      LOG.debug("wait_for_statestore_updates: curr=%s, init=%s, d=%s",
          list(curr.values()), list(init.values()),
          [curr[i] - init[i] for i in self.impalads])
      if all([curr[i] - init[i] >= heartbeats for i in self.impalads]): break
      for impalad in self.impalads:
        curr[impalad] = impalad.service.get_metric_value(
            REQUEST_QUEUE_UPDATE_INTERVAL)['count']
      assert (time() - start_time < STRESS_TIMEOUT),\
          "Timed out waiting %s seconds for heartbeats" % (STRESS_TIMEOUT,)
      wait_statestore_heartbeat()
    LOG.info("Waited %s for %s heartbeats", round(time() - start_time, 1), heartbeats)

  def wait_for_admitted_threads(self, num_threads):
    """
    Wait for query submission threads to update after being admitted, as determined
    by observing metric changes. This is necessary because the metrics may change
    before the execute_async() calls on the query threads return and add themselves
    to self.executing_threads.
    """
    start_time = time()
    LOG.info("Waiting for %s threads to begin execution", num_threads)
    # All individual list operations are thread-safe, so we don't need to use a
    # lock to synchronize before checking the list length (on which another thread
    # may call append() concurrently).
    while self.should_run() and len(self.executing_threads) < num_threads:
      assert (time() - start_time < STRESS_TIMEOUT), ("Timed out waiting %s seconds for "
          "%s admitted client rpcs to return. Only %s executing " % (
            STRESS_TIMEOUT, num_threads, len(self.executing_threads)))
      sleep(0.1)
    LOG.info("Found all %s admitted threads after %s seconds", num_threads,
        round(time() - start_time, 1))

  def end_admitted_queries(self, num_queries):
    """
    Requests each admitted query to end its query.
    """
    assert len(self.executing_threads) >= num_queries
    LOG.info("Requesting {0} clients to end queries".format(num_queries))

    # Request admitted clients to end their queries
    current_executing_queries = []
    for i in range(num_queries):
      # pop() is thread-safe, it's OK if another thread is appending concurrently.
      thread = self.executing_threads.pop(0)
      LOG.info("Ending query {}".format(thread.query_num))
      assert thread.query_state == 'ADMITTED'
      current_executing_queries.append(thread)
      thread.query_state = 'REQUEST_QUERY_END'

    # Wait for the queries to end
    start_time = time()
    while self.should_run():
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
    def __init__(self, impalad, vector, query_num,
        query_end_behavior, executing_threads, exit_signal):
      """
      executing_threads must be provided so that this thread can add itself when the
      query is admitted and begins execution.
      """
      super(self.__class__, self).__init__()
      self.executing_threads = executing_threads
      # Make vector local to this thread, because run() will modify it later.
      self.vector = deepcopy(vector)
      self.query_num = query_num
      self.query_end_behavior = query_end_behavior
      self.impalad = impalad
      self.error = None
      self.num_rows_fetched = 0
      # query_state is defined and used only by the test code, not a property exposed by
      # the server
      self.query_state = 'NOT_SUBMITTED'
      # Set by the main thread when tearing down
      self.exit_signal = exit_signal
      self.setDaemon(True)
      # Determine how many rows to fetch per interval.
      self.rows_per_fetch = TestAdmissionControllerStress.BATCH_SIZE

    def thread_should_run(self):
      return not self.exit_signal.is_set()

    def run(self):
      # Scope of client and query_handle must be local within this run() method.
      client = None
      query_handle = None
      try:
        try:
          if not self.thread_should_run():
            return

          if self.query_end_behavior == 'QUERY_TIMEOUT':
            self.vector.set_exec_option('query_timeout_s', QUERY_END_TIMEOUT_S)
          query = QUERY.format(self.query_num)
          self.query_state = 'SUBMITTING'
          assert self.vector.get_protocol() == HS2, "Must use hs2 protocol"
          client = self.impalad.service.create_client_from_vector(self.vector)

          LOG.info("Submitting query %s with ending behavior %s",
                   self.query_num, self.query_end_behavior)
          query_handle = client.execute_async(query)
          admitted = client.wait_for_admission_control(
            query_handle, timeout_s=ADMIT_TIMEOUT_S)
          if not admitted:
            msg = "Query {} failed to pass admission control within {} seconds".format(
                self.query_num, ADMIT_TIMEOUT_S)
            self.log_handle(client, query_handle, msg)
            self.query_state = 'ADMIT_TIMEOUT'
            return
          admission_result = client.get_admission_result(query_handle)
          assert len(admission_result) > 0
          if "Rejected" in admission_result:
            msg = "Rejected query {}".format(self.query_num)
            self.log_handle(client, query_handle, msg)
            self.query_state = 'REJECTED'
            return
          elif "Timed out" in admission_result:
            msg = "Query {} timed out".format(self.query_num)
            self.log_handle(client, query_handle, msg)
            self.query_state = 'TIMED OUT'
            return
          msg = "Admission result for query {} : {}".format(
            self.query_num, admission_result)
          self.log_handle(client, query_handle, msg)
        except IMPALA_CONNECTION_EXCEPTION as e:
          LOG.exception(e)
          raise e

        msg = "Admitted query {}".format(self.query_num)
        self.log_handle(client, query_handle, msg)
        self.query_state = 'ADMITTED'
        # The thread becomes visible to the main thread when it is added to the
        # shared list of executing_threads. append() is atomic and thread-safe.
        self.executing_threads.append(self)

        # Synchronize with the main thread. At this point, the thread is executing a
        # query. It needs to wait until the main thread requests it to end its query.
        while self.thread_should_run() and self.query_state != 'COMPLETED':
          # The query needs to stay active until the main thread requests it to end.
          # Otherwise, the query may get cancelled early. Fetch self.rows_per_fetch row
          # every FETCH_INTERVAL to keep the query active.
          fetch_result = client.fetch(query, query_handle, self.rows_per_fetch)
          assert len(fetch_result.data) == self.rows_per_fetch, str(fetch_result)
          self.num_rows_fetched += len(fetch_result.data)
          if self.query_state == 'REQUEST_QUERY_END':
            self._end_query(client, query, query_handle)
            # The query has released admission control resources
            self.query_state = 'COMPLETED'
          sleep(FETCH_INTERVAL)
      except Exception as e:
        LOG.exception(e)
        # Unknown errors will be raised later
        self.error = e
        self.query_state = 'ERROR'
      finally:
        self.print_termination_log()
        if client is not None:
          # Closing the client closes the query as well
          client.close()

    def print_termination_log(self):
      msg = ("Thread for query {} terminating in state {}. "
             "rows_fetched={} end_behavior={}").format(
          self.query_num, self.query_state, self.num_rows_fetched,
          self.query_end_behavior)
      LOG.info(msg)

    def _end_query(self, client, query, query_handle):
      """Bring the query to the appropriate end state defined by self.query_end_behaviour.
      Returns once the query has reached that state."""
      msg = "Ending query {} by {}".format(self.query_num, self.query_end_behavior)
      self.log_handle(client, query_handle, msg)
      if self.query_end_behavior == 'QUERY_TIMEOUT':
        # Sleep and wait for the query to be cancelled. The cancellation will
        # set the state to EXCEPTION.
        start_time = time()
        while self.thread_should_run() and not client.is_error(query_handle):
          assert (time() - start_time < STRESS_TIMEOUT),\
            "Timed out waiting %s seconds for query cancel" % (STRESS_TIMEOUT,)
          sleep(1)
        try:
          # try fetch and confirm from exception message that query was timed out.
          client.fetch(query, query_handle, discard_results=True)
          assert False
        except Exception as e:
          assert 'expired due to client inactivity' in str(e)
      elif self.query_end_behavior == 'EOS':
        # Fetch all rows so we hit eos.
        client.fetch(query, query_handle, discard_results=True)
      elif self.query_end_behavior == 'CLIENT_CANCEL':
        client.cancel(query_handle)
      else:
        assert self.query_end_behavior == 'CLIENT_CLOSE'
        client.close_query(query_handle)

    def log_handle(self, client, query_handle, msg):
      """Log ourself here rather than using client.log_handle() to display
      log timestamp."""
      handle_id = client.handle_id(query_handle)
      LOG.info("{}: {}".format(handle_id, msg))

  def _check_queries_page_resource_pools(self):
    """Checks that all queries in the '/queries' webpage json have the correct resource
    pool (this is called after all queries have been admitted, queued, or rejected, so
    they should already have the pool set), or no pool for queries that don't go through
    admission control."""
    for impalad in self.impalads:
      queries_json = impalad.service.get_debug_webpage_json('/queries')
      for query in itertools.chain(queries_json['in_flight_queries'],
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

  def wait_on_queries_page_num_queued(self, min_queued, max_queued):
    start_time = time()
    LOG.info("Waiting for %s <= queued queries <= %s" % (min_queued, max_queued))
    actual_queued = self._get_queries_page_num_queued()
    while self.should_run() and (
      actual_queued < min_queued or actual_queued > max_queued):
      assert (time() - start_time < STRESS_TIMEOUT), ("Timed out waiting %s seconds for "
          "%s <= queued queries <= %s, %s currently queued.",
            STRESS_TIMEOUT, min_queued, max_queued, actual_queued)
      sleep(0.1)
      actual_queued = self._get_queries_page_num_queued()
    LOG.info("Found %s queued queries after %s seconds", actual_queued,
        round(time() - start_time, 1))

  def run_admission_test(self, vector, check_user_aggregates=False):
    LOG.info("Starting test case with parameters: %s", vector)
    self.impalads = self.cluster.impalads
    self.ac_processes = self.get_ac_processes()
    round_robin_submission = vector.get_value('round_robin_submission')
    submission_delay_ms = vector.get_value('submission_delay_ms')
    if not round_robin_submission:
      self.impalads = [self.impalads[0]]
      self.ac_processes = [self.ac_processes[0]]

    num_queries = vector.get_value('num_queries')
    assert num_queries >= MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
    initial_metrics = self.get_consistent_admission_metrics(0)
    log_metrics("Initial metrics: ", initial_metrics)

    # This is the query submission loop.
    for query_num in range(num_queries):
      if not self.should_run():
        break
      if submission_delay_ms > 0:
        sleep(submission_delay_ms / 1000.0)
      impalad = self.impalads[query_num % len(self.impalads)]
      query_end_behavior = QUERY_END_BEHAVIORS[query_num % len(QUERY_END_BEHAVIORS)]
      thread = self.SubmitQueryThread(impalad, vector, query_num, query_end_behavior,
                                      self.executing_threads, self.exit)
      thread.start()
      self.all_threads.append(thread)

    # Wait for the admission control to make the initial admission decision for all
    # the queries. They should either be admitted immediately, queued, or rejected.
    # The test query is chosen that it with remain active on all backends until the test
    # ends the query. This prevents queued queries from being dequeued in the background
    # without this thread explicitly ending them, so that the test can admit queries in
    # discrete waves.
    LOG.info("Wait for initial admission decisions")
    (metric_deltas, curr_metrics) = self.wait_for_metric_changes(
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
        metric_deltas['queued'] == num_queries,\
        "Initial admission decisions don't add up to {0}: {1}".format(
        num_queries, str(metric_deltas))
    initial_metric_deltas = metric_deltas

    # Like above, check that the count from the queries webpage json is reasonable.
    min_queued = min(num_queries - metric_deltas['admitted'], MAX_NUM_QUEUED_QUERIES)
    max_queued = MAX_NUM_QUEUED_QUERIES * len(self.impalads)
    self.wait_on_queries_page_num_queued(min_queued, max_queued)
    self._check_queries_page_resource_pools()

    # Admit queries in waves until all queries are done. A new wave of admission
    # is started by killing some of the running queries.
    while len(self.executing_threads) > 0:
      curr_metrics = self.get_consistent_admission_metrics(num_queries)
      log_metrics("Main loop, curr_metrics: ", curr_metrics)
      num_to_end = len(self.executing_threads)
      LOG.info("Main loop, will request %s queries to end", num_to_end)
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
      # state, or we may find an impalad dequeue more requests after we capture metrics.
      self.wait_for_statestore_updates(10)

    final_metrics = self.get_consistent_admission_metrics(num_queries)
    log_metrics("Final metrics: ", final_metrics)
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
      # We shouldn't go over the max number of queries or queue size, so we can compute
      # the expected number of queries that should have been admitted (which includes the
      # number queued as they eventually get admitted as well), queued, and rejected
      expected_admitted = MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['admitted'] == expected_admitted
      assert metric_deltas['queued'] == MAX_NUM_QUEUED_QUERIES
      assert metric_deltas['rejected'] == num_queries - expected_admitted

    # All queries should be completed by now.
    self.wait_on_queries_page_num_queued(0, 0)
    self._check_queries_page_resource_pools()

    if check_user_aggregates:
      # Check that metrics tracking running users are empty as queries have finished.
      # These metrics are only present if user quotas are configured.
      keys = [
        "admission-controller.agg-current-users.root.queueF",
        "admission-controller.local-current-users.root.queueF",
      ]
      for impalad in self.ac_processes:
        values = impalad.service.get_metric_values(keys)
        assert values[0] == []
        assert values[1] == []

    for thread in self.all_threads:
      if thread.error is not None:
        raise thread.error

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=MAX_NUM_CONCURRENT_QUERIES,
        max_queued=MAX_NUM_QUEUED_QUERIES, pool_max_mem=-1, queue_wait_timeout_ms=600000),
      statestored_args=_STATESTORED_ARGS, force_restart=True)
  def test_admission_controller_with_flags(self, vector):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.pool_name = 'default-pool'
    vector.set_exec_option('request_pool', self.pool_name)
    vector.set_exec_option('mem_limit', sys.maxsize)
    # The pool has no mem resources set, so submitting queries with huge mem_limits
    # should be fine. This exercises the code that does the per-pool memory
    # accounting (see MemTracker::GetPoolMemReserved()) without actually being throttled.
    self.run_admission_test(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-test2.xml",
      llama_site_file="llama-site-test2.xml"),
    statestored_args=_STATESTORED_ARGS, force_restart=True)
  def test_admission_controller_with_configs(self, vector):
    self.pool_name = 'root.queueB'
    vector.set_exec_option('request_pool', self.pool_name)
    self.run_admission_test(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-test2.xml",
      llama_site_file="llama-site-test2.xml"),
    statestored_args=_STATESTORED_ARGS, force_restart=True)
  def test_admission_controller_with_quota_configs(self, vector):
    """Run a workload with a variety of outcomes in a pool that has user quotas
    configured. Note the user quotas will not prevent any queries from running, but this
    allows verification that metrics about users are consistent after queries end"""
    if (not vector.get_value('round_robin_submission')
        or not vector.get_value('submission_delay_ms') == 0):
      # Save time by running only 1 out of 6 vector combination.
      pytest.skip('Only run with round_robin_submission=True and submission_delay_ms=0.')
    self.pool_name = 'root.queueF'
    vector.set_exec_option('request_pool', self.pool_name)
    self.run_admission_test(vector, check_user_aggregates=True)

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
      statestored_args=_STATESTORED_ARGS, force_restart=True)
  def test_mem_limit(self, vector):
    # Impala may set the proc mem limit lower than we think depending on the overcommit
    # settings of the OS. It should be fine to continue anyway.
    proc_limit = self.get_proc_limit()
    if proc_limit != MEM_TEST_LIMIT:
      LOG.info("Warning: Process mem limit %s is not expected val %s", proc_limit,
          MEM_TEST_LIMIT)

    self.pool_name = 'default-pool'
    # Each query mem limit (set the query option to override the per-host memory
    # estimate) should use a bit less than (total pool mem limit) / #queries so that
    # once #queries are running, the total pool mem usage is about at the limit and
    # additional incoming requests will be rejected. The actual pool limit on the number
    # of running requests is very high so that requests are only queued/rejected due to
    # the mem limit.
    num_impalads = len(self.cluster.impalads)
    query_mem_limit = (proc_limit // MAX_NUM_CONCURRENT_QUERIES // num_impalads) - 1
    vector.set_exec_option('request_pool', self.pool_name)
    vector.set_exec_option('mem_limit', query_mem_limit)
    self.run_admission_test(vector)


class TestAdmissionControllerStressWithACService(TestAdmissionControllerStress):
  """Runs all of the tests from TestAdmissionControllerStress but with the second impalad
  in the minicluster configured to perform all admission control."""

  def get_ac_processes(self):
    return [self.cluster.admissiond]

  def get_ac_log_name(self):
    return "admissiond"

  def setup_method(self, method):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.enable_admission_service(method)
    super(TestAdmissionControllerStressWithACService, self).setup_method(method)
