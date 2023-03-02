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
import pytest
import time
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.common.skip import SkipIf, SkipIfBuildType
from tests.verifiers.mem_usage_verifier import MemUsageVerifier

class TestKrpcMetrics(CustomClusterTestSuite):
  """Test for KRPC metrics that require special arguments during cluster startup."""
  RPCZ_URL = 'http://localhost:25000/rpcz?json'
  TEST_QUERY = 'select count(*) from tpch_parquet.lineitem l1 \
      join tpch_parquet.lineitem l2 where l1.l_orderkey = l2.l_orderkey;'

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestKrpcMetrics, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('-datastream_service_queue_mem_limit=1B \
                                     -datastream_service_num_svc_threads=1')
  def test_krpc_queue_overflow_rpcz(self, vector):
    """Test that rejected RPCs show up on the /rpcz debug web page.
    """
    def get_rpc_overflows():
      rpcz = self.get_debug_page(self.RPCZ_URL)
      assert len(rpcz['services']) > 0
      for s in rpcz['services']:
        if s['service_name'] == 'impala.DataStreamService':
          return int(s['rpcs_queue_overflow'])
      assert False, "Could not find DataStreamService metrics"

    before = get_rpc_overflows()
    assert before == 0
    self.client.execute(self.TEST_QUERY)
    after = get_rpc_overflows()

    assert before < after

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('-datastream_service_queue_mem_limit=1B \
                                     -datastream_service_num_svc_threads=1')
  def test_krpc_queue_overflow_metrics(self, vector):
    """Test that rejected RPCs show up on the /metrics debug web page.
    """
    metric_name = 'rpc.impala.DataStreamService.rpcs_queue_overflow'
    before = self.get_metric(metric_name)
    assert before == 0

    self.client.execute(self.TEST_QUERY)
    after = self.get_metric(metric_name)
    assert before < after

  @pytest.mark.execute_serially
  def test_krpc_service_queue_metrics(self, vector):
    """Test that memory usage metrics for the data stream service queue show up on the
    /metrics debug web page.
    """
    self.client.execute(self.TEST_QUERY)
    assert self.get_metric('mem-tracker.DataStreamService.current_usage_bytes') >= 0
    assert self.get_metric('mem-tracker.DataStreamService.peak_usage_bytes') > 0
