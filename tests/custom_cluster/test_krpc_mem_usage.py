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

import pytest
import time
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIf, SkipIfBuildType
from tests.verifiers.mem_usage_verifier import MemUsageVerifier

DATA_STREAM_MGR_METRIC = "Data Stream Manager Deferred RPCs"
DATA_STREAM_SVC_METRIC = "Data Stream Service Queue"
ALL_METRICS = [ DATA_STREAM_MGR_METRIC, DATA_STREAM_SVC_METRIC ]

@SkipIf.not_krpc
class TestKrpcMemUsage(CustomClusterTestSuite):
  """Test for memory usage tracking when using KRPC."""
  TEST_QUERY = "select count(c2.string_col) from \
     functional.alltypestiny join functional.alltypessmall c2"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestKrpcMemUsage, cls).setup_class()

  def execute_query_verify_mem_usage(self, query, non_zero_peak_metrics):
    """Executes 'query' and makes sure that the memory used by KRPC is returned to the
    memtrackers. It also verifies that metrics in 'non_zero_peak_metrics' have a peak
    value > 0.
    """
    self.client.execute(query)
    self.verify_mem_usage(non_zero_peak_metrics)

  def verify_mem_usage(self, non_zero_peak_metrics):
    """Verifies that the memory used by KRPC is returned to the memtrackers and that
    metrics in 'non_zero_peak_metrics' have a peak value > 0.
    """
    verifiers = [ MemUsageVerifier(i.service) for i in ImpalaCluster().impalads ]
    for verifier in verifiers:
      for metric_name in ALL_METRICS:
        usage = verifier.get_mem_usage_values(metric_name)
        assert usage["total"] == 0
        if metric_name in non_zero_peak_metrics:
          assert usage["peak"] > 0, metric_name

  @pytest.mark.execute_serially
  def test_krpc_unqueued_memory_usage(self, vector):
    """Executes a simple query and checks that the data stream service consumed some
    memory.
    """
    # The data stream manager may not need to track memory in any queue if the receivers
    # show up in time.
    self.execute_query_verify_mem_usage(self.TEST_QUERY, [DATA_STREAM_SVC_METRIC])

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_datastream_recvr_delay_ms=1000")
  def test_krpc_deferred_memory_usage(self, vector):
    """Executes a simple query. The cluster is started with delayed receiver creation to
    trigger RPC queueing.
    """
    self.execute_query_verify_mem_usage(self.TEST_QUERY, ALL_METRICS)

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_datastream_recvr_delay_ms=1000")
  def test_krpc_deferred_memory_cancellation(self, vector):
    """Executes a query and cancels it while RPCs are still queued up. This exercises the
    code to flush the deferred RPC queue in the receiver.
    """
    query = "select count(*) from tpch_parquet.lineitem l1 join tpch_parquet.lineitem l2 \
            where l1.l_orderkey = l2.l_orderkey"
    # Warm up metadata
    self.client.execute(query)
    # Execute and cancel query
    handle = self.client.execute_async(query)
    # Sleep to allow RPCs to arrive.
    time.sleep(0.5)
    self.client.cancel(handle)
    self.client.close()
    self.verify_mem_usage(ALL_METRICS)
