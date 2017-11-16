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

from time import sleep
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType

@SkipIfBuildType.not_dev_build
class TestCatalogWait(CustomClusterTestSuite):
  """Impalad coordinators must wait for their local replica of the catalog to be
     initialized from the statestore prior to opening up client ports.
     This test simulates a failed or slow catalog on impalad startup."""

  def expect_connection(self, impalad):
    impalad.service.create_beeswax_client()
    impalad.service.create_hs2_client()

  def expect_no_connection(self, impalad):
    with pytest.raises(Exception) as e:
      impalad.service.create_beeswax_client()
      assert 'Could not connect to' in str(e.value)

    with pytest.raises(Exception) as e:
      impalad.service.create_hs2_client()
      assert 'Could not connect to' in str(e.value)

  @pytest.mark.execute_serially
  def test_delayed_catalog(self):
    """ Tests client interactions with the cluster when one of the daemons,
        impalad[2], is delayed in initializing its local catalog replica."""

    # On startup, expect only two executors to be registered.
    self._start_impala_cluster(["--catalog_init_delays=0,0,200000"],
                               expected_num_executors=2)

    # Expect that impalad[2] is not ready.
    self.cluster.impalads[2].service.wait_for_metric_value('impala-server.ready', 0);

    # Expect that impalad[0,1] are both ready and with initialized catalog.
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.ready', 1);
    self.cluster.impalads[0].service.wait_for_metric_value('catalog.ready', 1);
    self.cluster.impalads[1].service.wait_for_metric_value('impala-server.ready', 1);
    self.cluster.impalads[1].service.wait_for_metric_value('catalog.ready', 1);

    # Expect that connections can be made to impalads[0,1], but not to impalads[2].
    self.expect_connection(self.cluster.impalads[0])
    self.expect_connection(self.cluster.impalads[1])
    self.expect_no_connection(self.cluster.impalads[2])

    # Issues a query to check that impalad[2] does not evaluate any fragments
    # and does not prematurely register itself as an executor. The former is
    # verified via query fragment metrics and the latter would fail if registered
    # but unable to process fragments.
    client0 = self.cluster.impalads[0].service.create_beeswax_client()
    client1 = self.cluster.impalads[1].service.create_beeswax_client()

    self.execute_query_expect_success(client0, "select * from functional.alltypes");
    self.execute_query_expect_success(client1, "select * from functional.alltypes");

    # Check that fragments were run on impalad[0,1] and none on impalad[2].
    # Each ready impalad runs a fragment per query and one coordinator fragment. With
    # two queries, one coordinated per ready impalad, that should be 3 total fragments.
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.num-fragments', 3);
    self.cluster.impalads[1].service.wait_for_metric_value('impala-server.num-fragments', 3);
    self.cluster.impalads[2].service.wait_for_metric_value('impala-server.num-fragments', 0);
