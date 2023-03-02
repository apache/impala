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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.verifiers.metric_verifier import MetricVerifier


class TestExchangeEos(CustomClusterTestSuite):
  """ Test to verify that the senders' fragments get unblocked and run to completion
  after exchange node hits eos"""

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=9, num_exclusive_coordinators=1)
  def test_exchange_eos(self, vector):
    """ Test IMPALA-8845: runs with result spooling enabled and defers the fetching
    of results until all non-coordinator fragments have completed. It aims to verify
    that once the coordinator fragment reaches eos, the rest of the fragments will
    get unblocked. Using a cluster of size 9 which can reliably reproduce the hang of
    some non-coordinator fragments without the fix of IMPALA-8845.
    """

    cluster = ImpalaCluster.get_e2e_test_cluster()
    coordinator = cluster.get_first_impalad()
    client = coordinator.service.create_beeswax_client()

    vector.get_value('exec_option')['spool_query_results'] = 'true'
    for query in ["select * from tpch.lineitem order by l_orderkey limit 10000",
                  "select * from tpch.lineitem limit 10000"]:
      handle = self.execute_query_async_using_client(client, query, vector)
      for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
        verifier = MetricVerifier(impalad.service)
        if impalad.get_webserver_port() == coordinator.get_webserver_port():
          num_fragments = 1
        else:
          num_fragments = 0
        verifier.wait_for_metric("impala-server.num-fragments-in-flight", num_fragments)
      results = client.fetch(query, handle)
      assert results.success
      assert len(results.data) == 10000
    client.close()
