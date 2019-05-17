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
import math
import os
import pytest
import re
import shutil
import sys
import threading
from copy import copy
from time import sleep, time

from beeswaxd.BeeswaxService import QueryState

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout, IMPALA_TEST_CLUSTER_PROPERTIES
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.resource_pool_config import ResourcePoolConfig
from tests.common.skip import (
    SkipIfS3,
    SkipIfABFS,
    SkipIfADLS,
    SkipIfEC,
    SkipIfNotHdfsMinicluster)
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
QUERY_END_TIMEOUT_S = 1

# Value used for --admission_control_stale_topic_threshold_ms in tests.
STALE_TOPIC_THRESHOLD_MS = 500

# Regex that matches the first part of the profile info string added when a query is
# queued.
INITIAL_QUEUE_REASON_REGEX = \
    "Initial admission queue reason: waited [0-9]* ms, reason: .*"

# The path to resources directory which contains the admission control config files.
RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test", "resources")


def impalad_admission_ctrl_flags(max_requests, max_queued, pool_max_mem,
                                 proc_mem_limit=None, queue_wait_timeout_ms=None):
  extra_flags = ""
  if proc_mem_limit is not None:
    extra_flags += " -mem_limit={0}".format(proc_mem_limit)
  if queue_wait_timeout_ms is not None:
    extra_flags += " -queue_wait_timeout_ms={0}".format(queue_wait_timeout_ms)
  return ("-vmodule admission-controller=3 -default_pool_max_requests {0} "
          "-default_pool_max_queued {1} -default_pool_mem_limit {2} {3}".format(
            max_requests, max_queued, pool_max_mem, extra_flags))


def impalad_admission_ctrl_config_args(fs_allocation_file, llama_site_file,
                                        additional_args="", make_copy=False):
  fs_allocation_path = os.path.join(RESOURCES_DIR, fs_allocation_file)
  llama_site_path = os.path.join(RESOURCES_DIR, llama_site_file)
  if make_copy:
    copy_fs_allocation_path = os.path.join(RESOURCES_DIR, "copy-" + fs_allocation_file)
    copy_llama_site_path = os.path.join(RESOURCES_DIR, "copy-" + llama_site_file)
    shutil.copy2(fs_allocation_path, copy_fs_allocation_path)
    shutil.copy2(llama_site_path, copy_llama_site_path)
    fs_allocation_path = copy_fs_allocation_path
    llama_site_path = copy_llama_site_path
  return ("-vmodule admission-controller=3 -fair_scheduler_allocation_path %s "
          "-llama_site_path %s %s" % (fs_allocation_path, llama_site_path,
                                      additional_args))


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
    expected_set = set([x.lower() for x in expected_query_options])
    confs_set = set([x.lower() for x in confs])
    assert expected_set.issubset(confs_set)

  def __check_hs2_query_opts(self, pool_name, mem_limit=None, expected_options=None):
    """ Submits a query via HS2 (optionally with a mem_limit in the confOverlay)
        into pool_name and checks that the expected_query_options are set in the
        profile."""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.confOverlay = {'request_pool': pool_name}
    if mem_limit is not None: execute_statement_req.confOverlay['mem_limit'] = mem_limit
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

  def _execute_and_collect_profiles(self, queries, timeout_s, config_options={},
      allow_query_failure=False):
    """Submit the query statements in 'queries' in parallel to the first impalad in
    the cluster. After submission, the results are fetched from the queries in
    sequence and their profiles are collected. Wait for up to timeout_s for
    each query to finish. If 'allow_query_failure' is True, succeeds if the query
    completes successfully or ends up in the EXCEPTION state. Otherwise expects the
    queries to complete successfully.
    Returns the profile strings."""
    client = self.cluster.impalads[0].service.create_beeswax_client()
    expected_states = [client.QUERY_STATES['FINISHED']]
    if allow_query_failure:
      expected_states.append(client.QUERY_STATES['EXCEPTION'])
    try:
      handles = []
      profiles = []
      client.set_configuration(config_options)
      for query in queries:
        handles.append(client.execute_async(query))
      for query, handle in zip(queries, handles):
        state = self.wait_for_any_state(handle, expected_states, timeout_s)
        if state == client.QUERY_STATES['FINISHED']:
          self.client.fetch(query, handle)
        profiles.append(self.client.get_runtime_profile(handle))
      return profiles
    finally:
      client.close()

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
    client = impalad.service.create_beeswax_client()
    # Expected default mem limit for queueA, used in several tests below
    queueA_mem_limit = "MEM_LIMIT=%s" % (128 * 1024 * 1024)
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
      self.__check_query_options(result.runtime_profile,
          ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB'])

      # Try setting the pool for a queue with a very low queue timeout.
      # queueA allows only 1 running query and has a queue timeout of 50ms, so the
      # second concurrent query should time out quickly.
      client.set_configuration({'request_pool': 'root.queueA'})
      handle = client.execute_async("select sleep(1000)")
      self.__check_pool_rejected(client, 'root.queueA', "exceeded timeout")
      assert client.get_state(handle) == client.QUERY_STATES['FINISHED']
      # queueA has default query options mem_limit=128m,query_timeout_s=5
      self.__check_query_options(client.get_runtime_profile(handle),
          [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA'])
      client.close_query(handle)

      # Should be able to set query options via the set command (overriding defaults if
      # applicable). mem_limit overrides the pool default. abort_on_error has no
      # proc/pool default.
      client.execute("set mem_limit=31337")
      client.execute("set abort_on_error=1")
      result = client.execute("select 1")
      self.__check_query_options(result.runtime_profile,
          ['MEM_LIMIT=31337', 'ABORT_ON_ERROR=1', 'QUERY_TIMEOUT_S=5',
           'REQUEST_POOL=root.queueA'])

      # Should be able to set query options (overriding defaults if applicable) with the
      # config overlay sent with the query RPC. mem_limit is a pool-level override and
      # max_io_buffers has no proc/pool default.
      client.set_configuration({'request_pool': 'root.queueA', 'mem_limit': '12345'})
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
    self.__check_hs2_query_opts("root.queueA", None,
        [queueA_mem_limit, 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check overriding the mem limit sent in the confOverlay with the query.
    self.__check_hs2_query_opts("root.queueA", '12345',
        ['MEM_LIMIT=12345', 'QUERY_TIMEOUT_S=5', 'REQUEST_POOL=root.queueA', batch_size])
    # Check HS2 query in queueB gets the process-wide default query options
    self.__check_hs2_query_opts("root.queueB", None,
        ['MEM_LIMIT=200000000', 'REQUEST_POOL=root.queueB', batch_size])

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_config_args(
        fs_allocation_file="fair-scheduler-test2.xml",
        llama_site_file="llama-site-test2.xml",
        additional_args="-require_username"),
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
                       ".* is greater than pool max mem resources 10.00 MB \(configured "
                       "statically\)", str(ex))

  @SkipIfS3.hdfs_block_size
  @SkipIfABFS.hdfs_block_size
  @SkipIfADLS.hdfs_block_size
  @SkipIfEC.fix_later
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
          pool_max_mem=10 * PROC_MEM_TEST_LIMIT, proc_mem_limit=PROC_MEM_TEST_LIMIT),
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
      exec_options['mem_limit'] = long(self.PROC_MEM_TEST_LIMIT * 1.1)
      ex = self.execute_query_expect_failure(self.client, query, exec_options)
      assert ("Rejected query from pool default-pool: request memory needed "
              "1.10 GB per node is greater than memory available for admission 1.00 GB" in
              str(ex)), str(ex)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=2, max_queued=1,
      pool_max_mem=10 * PROC_MEM_TEST_LIMIT,
      queue_wait_timeout_ms=2 * STATESTORE_RPC_FREQUENCY_MS),
      start_args="--per_impalad_args=-mem_limit=3G;-mem_limit=3G;-mem_limit=2G",
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
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "2G"
    self.execute_query_expect_success(self.client, query, exec_options)
    # Test that a query scheduled to run on a single node and submitted to the impalad
    # with higher proc mem limit succeeds.
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "3G"
    exec_options['num_nodes'] = "1"
    self.execute_query_expect_success(self.client, query, exec_options)
    # Exercise rejection checks in admission controller.
    try:
      exec_options = copy(vector.get_value('exec_option'))
      exec_options['mem_limit'] = "3G"
      self.execute_query(query, exec_options)
    except ImpalaBeeswaxException as e:
      assert re.search("Rejected query from pool \S+: request memory needed 3.00 GB per "
          "node is greater than memory available for admission 2.00 GB of \S+", str(e)), \
          str(e)
    # Exercise queuing checks in admission controller.
    try:
      # Wait for previous queries to finish to avoid flakiness.
      for impalad in self.cluster.impalads:
        impalad.service.wait_for_metric_value("impala-server.num-fragments-in-flight", 0)
      impalad_with_2g_mem = self.cluster.impalads[2].service.create_beeswax_client()
      impalad_with_2g_mem.set_configuration_option('mem_limit', '1G')
      impalad_with_2g_mem.execute_async("select sleep(1000)")
      # Wait for statestore update to update the mem admitted in each node.
      sleep(STATESTORE_RPC_FREQUENCY_MS / 1000)
      exec_options = copy(vector.get_value('exec_option'))
      exec_options['mem_limit'] = "2G"
      # Since Queuing is synchronous and we can't close the previous query till this
      # returns, we wait for this to timeout instead.
      self.execute_query(query, exec_options)
    except ImpalaBeeswaxException as e:
      assert re.search("Queued reason: Not enough memory available on host \S+.Needed "
          "2.00 GB but only 1.00 GB out of 2.00 GB was available.", str(e)), str(e)
    finally:
      if impalad_with_2g_mem is not None:
        impalad_with_2g_mem.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--logbuflevel=-1 " + impalad_admission_ctrl_flags(max_requests=1,
        max_queued=1, pool_max_mem=PROC_MEM_TEST_LIMIT),
    statestored_args=_STATESTORED_ARGS)
  def test_cancellation(self):
    """ Test to confirm that all Async cancellation windows are hit and are able to
    succesfully cancel the query"""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    try:
      client.set_configuration_option("debug_action", "CRS_BEFORE_ADMISSION:SLEEP@2000")
      client.set_configuration_option("mem_limit", self.PROC_MEM_TEST_LIMIT + 1)
      handle = client.execute_async("select 1")
      sleep(1)
      client.close_query(handle)
      self.assert_impalad_log_contains('INFO',
          "Ready to be Rejected but already cancelled, query id=")
      client.clear_configuration()

      client.set_configuration_option("debug_action", "CRS_BEFORE_ADMISSION:SLEEP@2000")
      handle = client.execute_async("select 1")
      sleep(1)
      client.close_query(handle)
      self.assert_impalad_log_contains('INFO',
          "Ready to be Admitted immediately but already cancelled, query id=")

      client.set_configuration_option("debug_action",
          "CRS_AFTER_COORD_STARTS:SLEEP@2000")
      handle = client.execute_async("select 1")
      sleep(1)
      client.close_query(handle)
      self.assert_impalad_log_contains('INFO',
          "Cancelled right after starting the coordinator query id=")

      client.clear_configuration()
      handle = client.execute_async("select sleep(10000)")
      client.set_configuration_option("debug_action",
          "AC_AFTER_ADMISSION_OUTCOME:SLEEP@2000")
      queued_query_handle = client.execute_async("select 1")
      sleep(1)
      assert client.get_state(queued_query_handle) == QueryState.COMPILED
      assert "Admission result: Queued" in client.get_runtime_profile(queued_query_handle)
      # Only cancel the queued query, because close will wait till it unregisters, this
      # gives us a chance to close the running query and allow the dequeue thread to
      # dequeue the queue query
      client.cancel(queued_query_handle)
      client.close_query(handle)
      client.close_query(queued_query_handle)
      queued_profile = client.get_runtime_profile(queued_query_handle)
      assert "Admission result: Cancelled (queued)" in queued_profile
      self.assert_impalad_log_contains('INFO', "Dequeued cancelled query=")
      client.clear_configuration()

      handle = client.execute_async("select sleep(10000)")
      queued_query_handle = client.execute_async("select 1")
      sleep(1)
      assert client.get_state(queued_query_handle) == QueryState.COMPILED
      assert "Admission result: Queued" in client.get_runtime_profile(queued_query_handle)
      client.close_query(queued_query_handle)
      client.close_query(handle)
      queued_profile = client.get_runtime_profile(queued_query_handle)
      assert "Admission result: Cancelled (queued)" in queued_profile
      for i in self.cluster.impalads:
        i.service.wait_for_metric_value("impala-server.num-fragments-in-flight", 0)
      assert self.cluster.impalads[0].service.get_metric_value(
        "admission-controller.agg-num-running.default-pool") == 0
      assert self.cluster.impalads[0].service.get_metric_value(
        "admission-controller.total-admitted.default-pool") == 3
      assert self.cluster.impalads[0].service.get_metric_value(
        "admission-controller.total-queued.default-pool") == 2
    finally:
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
          pool_max_mem=1024 * 1024 * 1024),
      statestored_args=_STATESTORED_ARGS)
  def test_queue_reasons_num_queries(self):
    """Test that queue details appear in the profile when queued based on num_queries."""
    # Run a bunch of queries - one should get admitted immediately, the rest should
    # be dequeued one-by-one.
    STMT = "select sleep(1000)"
    TIMEOUT_S = 60
    EXPECTED_REASON = \
        "Latest admission queue reason: number of running queries 1 is at or over limit 1"
    NUM_QUERIES = 5
    profiles = self._execute_and_collect_profiles([STMT for i in xrange(NUM_QUERIES)],
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
    """Test that queue details appear in the profile when queued based on memory."""
    # Run a bunch of queries with mem_limit set so that only one can be admitted at a
    # time- one should get admitted immediately, the rest should be dequeued one-by-one.
    STMT = "select sleep(100)"
    TIMEOUT_S = 60
    EXPECTED_REASON = "Latest admission queue reason: Not enough aggregate memory " +\
        "available in pool default-pool with max mem resources 10.00 MB (configured " \
        "statically). Needed 9.00 MB but only 1.00 MB was available."
    NUM_QUERIES = 5
    profiles = self._execute_and_collect_profiles([STMT for i in xrange(NUM_QUERIES)],
        TIMEOUT_S, {'mem_limit': '9mb'})

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
    impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
        pool_max_mem=1024 * 1024 * 1024),
    statestored_args=_STATESTORED_ARGS)
  def test_query_locations_correctness(self, vector):
    """Regression test for IMPALA-7516: Test to make sure query locations and in-flight
    queries are correct for different admission results that can affect it."""
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
    self.impalad_test_service.wait_for_metric_value(
      "admission-controller.total-queued.default-pool", 1)
    self.__assert_num_queries_accounted(2)
    # First close the queued query
    self.close_query(handle_queued)
    self.close_query(handle_running)
    self.__assert_num_queries_accounted(0)
    # Case 3: When a query gets rejected
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = "1b"
    self.execute_query_expect_failure(self.client, query, exec_options)
    self.__assert_num_queries_accounted(0)

  def __assert_num_queries_accounted(self, expected_num):
    """Checks if the num of queries accounted by query_locations and in-flight are as
    expected"""
    # Wait for queries to start/un-register.
    assert self.impalad_test_service.wait_for_num_in_flight_queries(expected_num)
    query_locations = self.impalad_test_service.get_query_locations()
    for host, num_q in query_locations.items():
      assert num_q == expected_num, "There should be {0} running queries on either " \
                                    "impalads: {0}".format(query_locations)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml", make_copy=True),
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
      fs_allocation_file="mem-limit-test-fair-scheduler.xml",
      llama_site_file="mem-limit-test-llama-site.xml",
      additional_args="-default_pool_max_requests 1", make_copy=True),
    statestored_args=_STATESTORED_ARGS)
  def test_pool_config_change_while_queued(self, vector):
    """Tests if the invalid checks work even if the query is queued. Makes sure the query
    is not dequeued if the config is invalid and is promptly dequeued when it goes back
    to being valid"""
    pool_name = "invalidTestPool"
    config_str = "max-query-mem-limit"
    self.client.set_configuration_option('request_pool', pool_name)
    # Setup to queue a query.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    self.__wait_for_change_to_profile(sleep_query_handle,
                                      "Admission result: Admitted immediately")
    queued_query_handle = self.client.execute_async("select 1")
    self.__wait_for_change_to_profile(queued_query_handle, "Admission result: Queued")

    # Change config to be invalid.
    llama_site_path = os.path.join(RESOURCES_DIR, "copy-mem-limit-test-llama-site.xml")
    config = ResourcePoolConfig(self.cluster.impalads[0].service, llama_site_path)
    config.set_config_value(pool_name, config_str, 1)
    # Close running query so the queued one gets a chance.
    self.client.close_query(sleep_query_handle)

    # Check latest queued reason changed
    queued_reason = "Latest admission queue reason: Invalid pool config: the " \
                    "min_query_mem_limit is greater than the max_query_mem_limit" \
                    " (26214400 > 1)"
    self.__wait_for_change_to_profile(queued_query_handle, queued_reason)

    # Now change the config back to valid value and make sure the query is allowed to run.
    config.set_config_value(pool_name, config_str, 0)
    self.client.wait_for_finished_timeout(queued_query_handle, 20)
    self.close_query(queued_query_handle)

    # Now do the same thing for change to pool.max-query-mem-limit such that it can no
    # longer accommodate the largest min_reservation.
    # Setup to queue a query.
    sleep_query_handle = self.client.execute_async("select sleep(10000)")
    self.client.wait_for_admission_control(sleep_query_handle)
    queued_query_handle = self.client.execute_async(
      "select * from functional_parquet.alltypes limit 1")
    self.__wait_for_change_to_profile(queued_query_handle, "Admission result: Queued")
    # Change config to something less than the what is required to accommodate the
    # largest min_reservation (which in this case is 32.09 MB.
    config.set_config_value(pool_name, config_str, 25 * 1024 * 1024)
    # Close running query so the queued one gets a chance.
    self.client.close_query(sleep_query_handle)
    # Check latest queued reason changed
    queued_reason = "minimum memory reservation is greater than memory available to " \
                    "the query for buffer reservations. Memory reservation needed given" \
                    " the current plan: 88.00 KB. Adjust either the mem_limit or the" \
                    " pool config (max-query-mem-limit, min-query-mem-limit) for the" \
                    " query to allow the query memory limit to be at least 32.09 MB."
    self.__wait_for_change_to_profile(queued_query_handle, queued_reason, 5)
    # Now change the config back to a reasonable value.
    config.set_config_value(pool_name, config_str, 0)
    self.client.wait_for_finished_timeout(queued_query_handle, 20)
    self.__wait_for_change_to_profile(queued_query_handle,
                                      "Admission result: Admitted (queued)")
    self.close_query(queued_query_handle)

  def __wait_for_change_to_profile(self, query_handle, search_string, timeout=20):
    for _ in range(timeout * 10):
      profile = self.client.get_runtime_profile(query_handle)
      if search_string in profile:
        return
      sleep(0.1)
    assert False, "Timed out waiting for change to profile\nSearch " \
                  "String: {0}\nProfile:\n{1}".format(search_string, str(profile))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=10,
          pool_max_mem=1024 * 1024 * 1024))
  @needs_session()
  def test_queuing_status_through_query_log_and_exec_summary(self):
    """Test to verify that the HS2 client's GetLog() call and the ExecSummary expose
    the query's queuing status, that is, whether the query was queued and what was the
    latest queuing reason."""
    # Start a long running query.
    long_query_resp = self.execute_statement("select sleep(10000)")
    # Ensure that the query has started executing.
    self.wait_for_admission_control(long_query_resp.operationHandle)
    # Submit another query.
    queued_query_resp = self.execute_statement("select 1")
    # Wait until the query is queued.
    self.wait_for_operation_state(queued_query_resp.operationHandle,
            TCLIService.TOperationState.PENDING_STATE)
    # Check whether the query log message correctly exposes the queuing status.
    get_log_req = TCLIService.TGetLogReq()
    get_log_req.operationHandle = queued_query_resp.operationHandle
    log = self.hs2_client.GetLog(get_log_req).log
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
           exec_summary_resp.summary.queued_reason,\
      exec_summary_resp.summary.queued_reason
    # Close the running query.
    self.close(long_query_resp.operationHandle)
    # Close the queued query.
    self.close(queued_query_resp.operationHandle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=impalad_admission_ctrl_flags(max_requests=1, max_queued=3,
          pool_max_mem=1024 * 1024 * 1024) +
      " --admission_control_stale_topic_threshold_ms={0}".format(
          STALE_TOPIC_THRESHOLD_MS),
      statestored_args=_STATESTORED_ARGS)
  def test_statestore_outage(self):
    """Test behaviour with a failed statestore. Queries should continue to be admitted
    but we should generate diagnostics about the stale topic."""
    self.cluster.statestored.kill()
    impalad = self.cluster.impalads[0]
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
    profiles = self._execute_and_collect_profiles([STMT for i in xrange(NUM_QUERIES)],
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
  @CustomClusterTestSuite.with_args(impalad_args=impalad_admission_ctrl_config_args(
    fs_allocation_file="fair-scheduler-test2.xml",
    llama_site_file="llama-site-test2.xml"),
    statestored_args=_STATESTORED_ARGS)
  def test_scalable_config(self, vector):
    """
    Test that the scalable configuration parameters scale as the cluster size changes.
    """
    # Start with 3 Impalads
    coordinator = self.cluster.impalads[0]
    impalad_1 = self.cluster.impalads[1]
    impalad_2 = self.cluster.impalads[2]
    self.__check_admission_by_counts(expected_num_impalads=3)
    # The mem_limit values that are passed to __check_admission_by_memory are based on
    # the memory used by the query when run on clusters of varying sizes, but mem_limit
    # is used in the test to make the test deterministic in the presence of changing
    # memory estimates.
    self.__check_admission_by_memory(True, '85M')

    # Kill an Impalad, now there are 2.
    impalad_1.kill_and_wait_for_exit()
    coordinator.service.wait_for_num_known_live_backends(2)
    self.__check_admission_by_counts(expected_num_impalads=2)
    self.__check_admission_by_memory(False, '125M',
                                   'is greater than pool max mem resources 200.00 MB'
                                   ' (calculated as 2 backends each with 100.00 MB)')

    # Restart an Impalad, now there are 3 again.
    impalad_1.start(wait_until_ready=True)
    coordinator.service.wait_for_num_known_live_backends(3)
    self.__check_admission_by_counts(expected_num_impalads=3)
    self.__check_admission_by_memory(True, '85M')

    # Kill 2 Impalads, now there are 1.
    impalad_1.kill_and_wait_for_exit()
    impalad_2.kill_and_wait_for_exit()
    coordinator.service.wait_for_num_known_live_backends(1)
    self.__check_admission_by_counts(expected_num_impalads=1)
    self.__check_admission_by_memory(False, '135M',
                                   'is greater than pool max mem resources 100.00 MB'
                                   ' (calculated as 1 backends each with 100.00 MB)')

    # Restart 2 Impalads, now there are 3 again.
    impalad_1.start(wait_until_ready=True)
    impalad_2.start(wait_until_ready=True)
    coordinator.service.wait_for_num_known_live_backends(3)
    self.__check_admission_by_counts(expected_num_impalads=3)
    self.__check_admission_by_memory(True, '85M')

  def __check_admission_by_memory(self, expected_admission, mem_limit,
                                  expected_rejection_reason=None):
    """
    Test if a query can run against the current cluster.
    :param mem_limit set in the client configuration to limit query memory.
    :param expected_admission: True if admission is expected.
    :param expected_rejection_reason: a string expected to be in the reason for rejection.
    """
    query = "select * from functional.alltypesagg  order by int_col limit 1"
    profiles = self._execute_and_collect_profiles([query], timeout_s=60,
        allow_query_failure=True, config_options={'request_pool': 'root.queueE',
                                                  'mem_limit': mem_limit})
    assert len(profiles) == 1
    profile = profiles[0]
    if "Admission result: Admitted immediately" in profile:
      did_admit = True
    elif "Admission result: Rejected" in profile:
      did_admit = False
      num_rejected, rejected_reasons = self.parse_profiles_rejected(profiles)
      assert num_rejected == 1
      assert expected_rejection_reason is not None, rejected_reasons[0]
      assert expected_rejection_reason in rejected_reasons[0], profile
    else:
      assert "Admission result: Admitted (queued)" in profile, profile
      assert 0, "should not queue based on memory"
    assert did_admit == expected_admission, profile

  def __check_admission_by_counts(self, expected_num_impalads):
    """
    Run some queries, find how many were admitted, queued or rejected, and check that
    AdmissionController correctly enforces query count limits based in the configuration
    in llama-site-test2.xml.
    """
    NUM_QUERIES = 6
    # Set expected values based on expected_num_impalads.
    # We can run 1 query per backend for queueE from llama-site-test2.xml.
    expected_num_admitted = expected_num_impalads
    # The value of max-queued-queries-multiple for queueE from llama-site-test2.xml.
    QUERIES_MULTIPLE = 0.6
    expected_num_queued = int(math.ceil(expected_num_impalads * QUERIES_MULTIPLE))
    expected_num_rejected = NUM_QUERIES - (expected_num_admitted + expected_num_queued)

    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    client.set_configuration({'request_pool': 'root.queueE'})
    result = client.execute("select 1")
    # Query should execute in queueE.
    self.__check_query_options(result.runtime_profile, ['REQUEST_POOL=root.queueE'])
    STMT = "select sleep(1000)"
    TIMEOUT_S = 60
    profiles = self._execute_and_collect_profiles([STMT for i in xrange(NUM_QUERIES)],
            TIMEOUT_S, allow_query_failure=True,
            config_options={'request_pool': 'root.queueE'})
    # Check admitted queries
    num_admitted_immediately = self.parse_profiles_admitted(profiles)
    assert num_admitted_immediately == expected_num_admitted

    # Check queued queries.
    num_queued, queued_reasons = self.parse_profiles_queued(profiles)
    assert num_queued == expected_num_queued
    assert len(queued_reasons) == num_queued
    expected_queue_reason = (
      "number of running queries {0} is at or over limit {1}".format(
        expected_num_admitted, expected_num_admitted)
    )
    # The first query to get queued sees expected_queue_reason.
    assert len([s for s in queued_reasons if
                expected_queue_reason in s]) == 1
    # Subsequent queries that are queued see that the queue is not empty.
    expected_see_non_empty_queue = max(expected_num_queued - 1, 0)
    assert len([s for s in queued_reasons if
                "queue is not empty" in s]) == expected_see_non_empty_queue

    # Check rejected queries
    num_rejected, rejected_reasons = self.parse_profiles_rejected(profiles)
    assert num_rejected == expected_num_rejected
    expected_rejection_reason = (
      "Rejected query from pool root.queueE: queue full, "
      "limit={0} (calculated as {1} backends each with 0.6 queries)".format(
        expected_num_queued, expected_num_impalads)
    )
    assert len([s for s in rejected_reasons if
                expected_rejection_reason in s]) == expected_num_rejected

  def parse_profiles_queued(self, profiles):
    """
    Parse a list of Profile strings and sum the counts of queries queued.
    :param profiles: a list of query profiles to parse.
    :return: The number queued.
    """
    num_queued = 0
    queued_reasons_return = []
    for profile in profiles:
      if "Admission result: Admitted (queued)" in profile:
        queued_reasons = [line for line in profile.split("\n")
                          if "Initial admission queue reason:" in line]
        assert len(queued_reasons) == 1, profile
        num_queued += 1
        queued_reasons_return.append(queued_reasons[0])
    return num_queued, queued_reasons_return

  def parse_profiles_admitted(self, profiles):
    """
    Parse a list of Profile strings and sum the counts of queries admitted immediately.
    :param profiles: a list of query profiles to parse.
    :return: The number admitted immediately.
    """
    num_admitted_immediately = 0
    for profile in profiles:
      if "Admission result: Admitted immediately" in profile:
        num_admitted_immediately += 1
    return num_admitted_immediately

  def parse_profiles_rejected(self, profiles):
    """
    Parse a list of Profile strings and sum the counts of queries rejected.
    :param profiles: a list of query profiles to parse.
    :return: The number rejected.
    """
    num_rejected = 0
    rejected_reasons_return = []
    for profile in profiles:
      if "Admission result: Rejected" in profile:
        num_rejected += 1
        query_statuses = [line for line in profile.split("\n")
                          if "Query Status:" in line]
        assert len(query_statuses) == 1, profile
        rejected_reasons_return.append(query_statuses[0])
    return num_rejected, rejected_reasons_return


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
    if IMPALA_TEST_CLUSTER_PROPERTIES.has_code_coverage():
      # Code coverage builds can't handle the increased concurrency.
      num_queries = 15
    elif cls.exploration_strategy() == 'core':
      num_queries = 30
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('submission_delay_ms') == 0)
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('round_robin_submission'))

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
    metrics = {'admitted': 0, 'queued': 0, 'dequeued': 0, 'rejected': 0,
               'released': 0, 'timed-out': 0}
    for impalad in self.impalads:
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
    for i in xrange(ATTEMPTS):
      metrics = self.get_admission_metrics()
      admitted_immediately = num_submitted - metrics['queued'] - metrics['rejected']
      if admitted_immediately + metrics['dequeued'] == metrics['admitted']:
        return metrics
      LOG.info("Got inconsistent metrics {0}".format(metrics))
    assert False, "Could not get consistent metrics for {0} queries after {1} attempts: "\
        "{2}".format(num_submitted, ATTEMPTS, metrics)

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
    while len(self.executing_threads) < num_threads:
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
    for i in xrange(num_queries):
      # pop() is thread-safe, it's OK if another thread is appending concurrently.
      thread = self.executing_threads.pop(0)
      LOG.info("Cancelling query %s", thread.query_num)
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
      self.shutdown = False  # Set by the main thread when tearing down

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

          LOG.info("Submitting query %s", self.query_num)
          self.query_handle = client.execute_async(query)
          client.wait_for_admission_control(self.query_handle)
          admission_result = client.get_admission_result(self.query_handle)
          assert len(admission_result) > 0
          if "Rejected" in admission_result:
            LOG.info("Rejected query %s", self.query_num)
            self.query_state = 'REJECTED'
            self.query_handle = None
            return
          elif "Timed out" in admission_result:
            LOG.info("Query %s timed out", self.query_num)
            self.query_state = 'TIMED OUT'
            self.query_handle = None
            return
          LOG.info("Admission result for query %s : %s", self.query_num, admission_result)
        except ImpalaBeeswaxException as e:
          LOG.exception(e)
          raise e
        finally:
          self.lock.release()
        LOG.info("Admitted query %s", self.query_num)
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
        LOG.info("Thread terminating in state=%s", self.query_state)
        if client is not None:
          client.close()

    def _end_query(self, client, query):
      """Bring the query to the appropriate end state defined by self.query_end_behaviour.
      Returns once the query has reached that state."""
      LOG.info("Ending query %s by %s",
          str(self.query_handle.get_handle()), self.query_end_behavior)
      if self.query_end_behavior == 'QUERY_TIMEOUT':
        # Sleep and wait for the query to be cancelled. The cancellation will
        # set the state to EXCEPTION.
        start_time = time()
        while (client.get_state(self.query_handle) !=
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

  def run_admission_test(self, vector, additional_query_options):
    LOG.info("Starting test case with parameters: %s", vector)
    self.impalads = self.cluster.impalads
    round_robin_submission = vector.get_value('round_robin_submission')
    submission_delay_ms = vector.get_value('submission_delay_ms')
    if not round_robin_submission:
      self.impalads = [self.impalads[0]]

    num_queries = vector.get_value('num_queries')
    assert num_queries >= MAX_NUM_CONCURRENT_QUERIES + MAX_NUM_QUEUED_QUERIES
    initial_metrics = self.get_admission_metrics()
    log_metrics("Initial metrics: ", initial_metrics)

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
    queries_page_num_queued = self._get_queries_page_num_queued()
    assert queries_page_num_queued >=\
        min(num_queries - metric_deltas['admitted'], MAX_NUM_QUEUED_QUERIES)
    assert queries_page_num_queued <= MAX_NUM_QUEUED_QUERIES * len(self.impalads)
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
      # state or we may find an impalad dequeue more requests after we capture metrics.
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
    impalad_args=impalad_admission_ctrl_config_args(
      fs_allocation_file="fair-scheduler-test2.xml",
      llama_site_file="llama-site-test2.xml"),
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
    query_mem_limit = (proc_limit / MAX_NUM_CONCURRENT_QUERIES / num_impalads) - 1
    self.run_admission_test(vector,
        {'request_pool': self.pool_name, 'mem_limit': query_mem_limit})
