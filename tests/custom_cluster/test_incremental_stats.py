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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestIncrementalStatistics(CustomClusterTestSuite):
  """Tests flag that controls pulling directly from catalogd using the
  --pull_incremental_statistics flag."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--pull_incremental_statistics=false",
                                    catalogd_args="--pull_incremental_statistics=false")
  def test_push_stats(self, vector, unique_database):
    """
    Tests compute incremental stats when incremental stats are pushed via statestore.
    """
    self.run_test_case('QueryTest/compute-stats-incremental', vector, unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--pull_incremental_statistics=true --use_local_catalog=true",
    catalogd_args="--pull_incremental_statistics=true --catalog_topic_mode=minimal")
  def test_with_local_catalog(self, vector, unique_database):
    """
    Tests that when local catalog is used, the pull incremental stats flag has no effect.
    """
    self.run_test_case('QueryTest/compute-stats-incremental', vector, unique_database)
