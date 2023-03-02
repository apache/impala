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
#
# Tests for local and remote disk scheduling.

from __future__ import absolute_import, division, print_function
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.network import get_external_ip
from tests.common.skip import SkipIfNotHdfsMinicluster


LOCAL_ASSIGNMENTS_METRIC = "simple-scheduler.local-assignments.total"
TOTAL_ASSIGNMENTS_METRIC = "simple-scheduler.assignments.total"


@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestSchedulerLocality(CustomClusterTestSuite):
  """Tests for local and remote disk scheduling."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      impalad_args='--hostname=localhost', cluster_size=1)
  def test_local_assignment(self, vector):
    self.client.execute('select count(1) from functional.alltypes')
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_metric_value(LOCAL_ASSIGNMENTS_METRIC, 1,
          allow_greater=True)
      assignments, local_assignments = impalad.service.get_metric_values([
          TOTAL_ASSIGNMENTS_METRIC, LOCAL_ASSIGNMENTS_METRIC])
      assert assignments == local_assignments

  @CustomClusterTestSuite.with_args(
      impalad_args='--hostname=' + get_external_ip(), cluster_size=1)
  def test_remote_assignment(self, vector):
    self.client.execute('select count(1) from functional.alltypes')
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_metric_value(TOTAL_ASSIGNMENTS_METRIC, 1,
          allow_greater=True)
      assert impalad.service.get_metric_value(LOCAL_ASSIGNMENTS_METRIC) == 0
