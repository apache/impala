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

from impala.error import HiveServer2Error
from tests.common.environ import specific_build_type_timeout
from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite



class TestRestart(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_restart_statestore(self, cursor):
    """ Regression test of IMPALA-6973. After the statestore restarts, the metadata should
        eventually recover after being cleared by the new statestore.
    """

    self.cluster.statestored.restart()
    # We need to wait for the impalad to register to the new statestored and for a
    # non-empty catalog update from the new statestored. It cannot be expressed with the
    # existing metrics yet so we wait for some time here.
    wait_time_s = specific_build_type_timeout(60, slow_build_timeout=100)
    sleep(wait_time_s)
    for retry in xrange(wait_time_s):
      try:
        cursor.execute("describe database functional")
        return
      except HiveServer2Error, e:
        assert "AnalysisException: Database does not exist: functional" in e.message,\
               "Unexpected exception: " + e.message
        sleep(1)
    assert False, "Coordinator never received non-empty metadata from the restarted " \
           "statestore after {0} seconds".format(wait_time_s)

  @pytest.mark.execute_serially
  def test_restart_impala(self):
      """ This test aims to restart Impalad executor nodes between queries to exercise
      the cluster membership callback which removes stale connections to the restarted
      nodes."""

      self._start_impala_cluster([], num_coordinators=1, cluster_size=3)
      assert len(self.cluster.impalads) == 3

      client = self.cluster.impalads[0].service.create_beeswax_client()
      assert client is not None

      for i in xrange(5):
        self.execute_query_expect_success(client, "select * from functional.alltypes")
        node_to_restart = 1 + (i % 2)
        self.cluster.impalads[node_to_restart].restart()
        # Sleep for a bit for the statestore change in membership to propagate. The min
        # update frequency for statestore is 100ms but using a larger sleep time here
        # as certain builds (e.g. ASAN) can be really slow.
        sleep(3)

      client.close()
