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
from builtins import range
import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIfBuildType, SkipIfFS
from tests.verifiers.metric_verifier import MetricVerifier

# The BE krpc port of the impalad to simulate rpc errors in tests.
FAILED_KRPC_PORT = 27001


def _get_rpc_fail_action(port):
  return "IMPALA_SERVICE_POOL:127.0.0.1:{port}:ExecQueryFInstances:FAIL" \
      .format(port=port)

@SkipIfBuildType.not_dev_build
class TestRPCTimeout(CustomClusterTestSuite):
  """Tests for every Impala RPC timeout handling, query should not hang and
     resource should be all released."""
  TEST_QUERY = "select count(c2.string_col) from \
     functional.alltypestiny join functional.alltypessmall c2"
  # Designed to take approx. 30s.
  SLOW_TEST_QUERY = TEST_QUERY + " where c2.int_col = sleep(1000)"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestRPCTimeout, cls).setup_class()

  def execute_query_verify_metrics(self, query, query_options=None, repeat=1,
      expected_exception=None):
    for i in range(repeat):
      try:
        self.execute_query(query, query_options)
        assert expected_exception is None
      except ImpalaBeeswaxException as e:
        if expected_exception is not None:
          assert expected_exception in str(e)
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]

    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0)
      v.verify_num_unused_buffers()

  def execute_query_then_cancel(self, query, vector, repeat = 1):
    for _ in range(repeat):
      handle = self.execute_query_async(query, vector.get_value('exec_option'))
      self.client.fetch(query, handle)
      try:
        self.client.cancel(handle)
      except ImpalaBeeswaxException:
        pass
      finally:
        self.client.close_query(handle)
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]

    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0)
      v.verify_num_unused_buffers()

  def execute_runtime_filter_query(self):
    query = "select STRAIGHT_JOIN * from functional_avro.alltypes a join \
            [SHUFFLE] functional_avro.alltypes b on a.month = b.id \
            and b.int_col = -3"
    self.client.execute("SET RUNTIME_FILTER_MODE=GLOBAL")
    self.client.execute("SET RUNTIME_FILTER_WAIT_TIME_MS=10000")
    self.client.execute("SET MAX_SCAN_RANGE_LENGTH=1024")
    self.execute_query_verify_metrics(query)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --debug_actions=EXEC_QUERY_FINSTANCES_DELAY:SLEEP@1000"
      " --datastream_sender_timeout_ms=30000")
  def test_execqueryfinstances_race(self, vector):
    """ Test for IMPALA-7464, where the rpc times out while the rpc handler continues to
        run simultaneously."""
    self.execute_query_verify_metrics(self.TEST_QUERY)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --debug_actions=EXEC_QUERY_FINSTANCES_DELAY:SLEEP@3000"
      " --datastream_sender_timeout_ms=30000")
  def test_execqueryfinstances_timeout(self, vector):
    for i in range(3):
      ex= self.execute_query_expect_failure(self.client, self.TEST_QUERY)
      assert "Exec() rpc failed: Timed out" in str(ex)
    verifiers = [MetricVerifier(i.service) for i in
                 ImpalaCluster.get_e2e_test_cluster().impalads]

    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0)
      v.verify_num_unused_buffers()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --debug_actions=CANCEL_QUERY_FINSTANCES_DELAY:SLEEP@3000"
      " --datastream_sender_timeout_ms=30000")
  def test_cancelplanfragment_timeout(self, vector):
    query = "select * from tpch.lineitem limit 5000"
    self.execute_query_then_cancel(query, vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --debug_actions=PUBLISH_FILTER_DELAY:SLEEP@3000")
  def test_publishfilter_timeout(self, vector):
    self.execute_runtime_filter_query()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --debug_actions=UPDATE_FILTER_DELAY:SLEEP@3000")
  def test_updatefilter_timeout(self, vector):
    self.execute_runtime_filter_query()

  all_rpcs = ["EXEC_QUERY_FINSTANCES", "CANCEL_QUERY_FINSTANCES", "PUBLISH_FILTER",
      "UPDATE_FILTER", "TRANSMIT_DATA", "END_DATA_STREAM", "REMOTE_SHUTDOWN"]
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=1000"
      " --datastream_sender_timeout_ms=30000 --debug_actions=%s" %
      "|".join(["%s_DELAY:JITTER@3000@0.1" % rpc for rpc in all_rpcs]))
  def test_random_rpc_timeout(self, vector):
    self.execute_query_verify_metrics(self.TEST_QUERY, None, 10)

  # Inject jitter into the RPC handler of ReportExecStatus() to trigger RPC timeout.
  # Useful for triggering IMPALA-8274.
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--status_report_interval_ms=100"
      " --backend_client_rpc_timeout_ms=100"
      " --debug_actions=REPORT_EXEC_STATUS_DELAY:JITTER@110@0.7")
  def test_reportexecstatus_jitter(self, vector):
    LONG_RUNNING_QUERY = "with v as (select t1.ss_hdemo_sk as xk " +\
       "from tpcds_parquet.store_sales t1, tpcds_parquet.store_sales t2 " +\
       "where t1.ss_hdemo_sk = t2.ss_hdemo_sk) " +\
       "select count(*) from v, tpcds_parquet.household_demographics t3 " +\
       "where v.xk = t3.hd_demo_sk"
    self.execute_query_verify_metrics(LONG_RUNNING_QUERY, None, 1)

  # Use a small service queue memory limit and a single service thread to exercise
  # the retry paths in the ReportExecStatus() RPC
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--status_report_interval_ms=100"
      " --control_service_queue_mem_limit=1"
      " --control_service_queue_mem_limit_floor_bytes=1"
      " --control_service_num_svc_threads=1")
  def test_reportexecstatus_retry(self, vector):
    self.execute_query_verify_metrics(self.TEST_QUERY, None, 10)

  # Inject artificial failure during thrift profile serialization / deserialization.
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--status_report_interval_ms=10")
  def test_reportexecstatus_profile_fail(self):
    query_options = {'debug_action': 'REPORT_EXEC_STATUS_PROFILE:FAIL@0.8'}
    self.execute_query_verify_metrics(self.TEST_QUERY, query_options, 10)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--backend_client_rpc_timeout_ms=100"
      " --status_report_interval_ms=1000 --status_report_max_retry_s=1000"
      " --debug_actions=REPORT_EXEC_STATUS_DELAY:SLEEP@1000")
  def test_reportexecstatus_retries(self, unique_database):
    tbl = "%s.kudu_test" % unique_database
    self.execute_query("create table %s ("
        "a bigint primary key, b bigint not null) stored as kudu" % tbl)
    # Since the sleep time (1000ms) is much longer than the rpc timeout (100ms), all
    # reports will appear to fail. The query is designed to result in many intermediate
    # status reports but fewer than the max allowed failures, so the query should succeed.
    result = self.execute_query("insert into %s select c_custkey, "
        "case when c_custkey > 1 then null else c_custkey end "
        "from tpch.customer order by c_custkey limit 100000" % tbl)
    assert result.success, str(result)
    # Ensure that the error log was tracked correctly - all but the first row inserted
    # should result in a 'Row with null value violates nullability constraint on table'
    # insert error.
    assert "(1 of 99999 similar)" in result.log, str(result)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--status_report_interval_ms=100000 "
      "--status_report_max_retry_s=1 --abort_on_config_error=false")
  def test_unresponsive_backend(self, unique_database):
    """Test the UnresponsiveBackendThread by setting a status report retry time that is
    much lower than the status report interval, ensuring that the coordinator will
    conclude that the backend is unresponsive."""
    self.execute_query_verify_metrics(self.SLOW_TEST_QUERY,
        expected_exception="cancelled due to unresponsive backend")

  @SkipIfFS.shutdown_idle_fails
  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--backend_client_rpc_timeout_ms=1000 --debug_actions=" +
      _get_rpc_fail_action(FAILED_KRPC_PORT),
      statestored_args="--statestore_heartbeat_frequency_ms=1000 \
          --statestore_max_missed_heartbeats=2")
  def test_miss_complete_cb(self, unique_database):
    """Test verify cancellation should not be blocked if the callback of ExecComplate
    are missing."""

    rpc_not_accessible_impalad = self.cluster.impalads[1]
    assert rpc_not_accessible_impalad.service.krpc_port == FAILED_KRPC_PORT

    # The 2nd node cannot be accessible through KRPC so that it's added to blacklist
    # and the query should be aborted without hanging.
    query = "select count(*) from tpch_parquet.lineitem where l_orderkey < 50"
    debug_action = 'IMPALA_MISS_EXEC_COMPLETE_CB:FAIL@1.0'
    ex = self.execute_query_expect_failure(self.client, query,
        query_options={'retry_failed_queries': 'false', 'debug_action': debug_action})
    assert "Query aborted" in str(ex)

class TestCatalogRPCTimeout(CustomClusterTestSuite):
  """"Tests RPC timeout and retry handling for catalogd operations."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestCatalogRPCTimeout, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--catalog_client_rpc_timeout_ms=10 "
                 "--catalog_client_rpc_retry_interval_ms=1 "
                 "--catalog_client_connection_num_retries=1",
    catalogd_args="--debug_actions=RESET_METADATA_DELAY:SLEEP@1000")
  def test_catalog_rpc_timeout(self):
    """Tests that catalog_client_rpc_timeout_ms enforces a timeout on catalogd
    operations. The debug action causes a delay of 1 second for refresh table
    commands. The RPC timeout is 10 ms, so all refresh table commands should
    fail with an RPC timeout exception."""
    try:
      self.execute_query("refresh functional.alltypes")
    except ImpalaBeeswaxException as e:
      assert "RPC recv timed out" in str(e)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--catalog_client_rpc_timeout_ms=5000 "
                 "--catalog_client_rpc_retry_interval_ms=0 "
                 "--catalog_client_connection_num_retries=10",
    catalogd_args="--debug_actions=RESET_METADATA_DELAY:JITTER@10000@0.75")
  def test_catalog_rpc_retries(self):
    """Tests that catalogd operations are retried. The debug action should randomly
    cause refresh table commands to fail. However, the catalogd client will retry
    the command 10 times, so eventually the refresh attempt should succeed. The debug
    action will add 10 seconds of delay to refresh table operations. The delay will only
    be triggered 75% of the time.
    """
    self.execute_query("refresh functional.alltypes")
