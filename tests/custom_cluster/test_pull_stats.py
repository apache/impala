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

class TestPullStatistics(CustomClusterTestSuite):
  """Tests incremental statistics when pulling directly from catalogd
  using the --pull_incremental_statistics flag."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--pull_incremental_statistics=true",
                                    catalogd_args="--pull_incremental_statistics=true")
  def test_pull_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/compute-stats-incremental', vector, unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--pull_incremental_statistics=true",
                                    catalogd_args="--pull_incremental_statistics=true")
  def test_pull_stats_profile(self, vector, unique_database):
    """Checks that the frontend profile includes metrics when computing
       incremental statistics.
    """
    try:
      client = self.cluster.impalads[0].service.create_beeswax_client()
      create = "create table test like functional.alltypes"
      load = "insert into test partition(year, month) select * from functional.alltypes"
      insert = """insert into test partition(year=2009, month=1) values
                  (29349999, true, 4, 4, 4, 40,4.400000095367432,40.4,
                  "10/21/09","4","2009-10-21 03:24:09.600000000")"""
      stats_all = "compute incremental stats test"
      stats_part = "compute incremental stats test partition (year=2009,month=1)"

      # Checks that profile does not have metrics for incremental stats when
      # the operation is not 'compute incremental stats'.
      self.execute_query_expect_success(client, "use %s" % unique_database)
      profile = self.execute_query_expect_success(client, create).runtime_profile
      assert profile.count("StatsFetch") == 0
      # Checks that incremental stats metrics are present when 'compute incremental
      # stats' is run. Since the table has no stats, expect that no bytes are fetched.
      self.execute_query_expect_success(client, load)
      profile = self.execute_query_expect_success(client, stats_all).runtime_profile
      assert profile.count("StatsFetch") > 1
      assert profile.count("StatsFetch.CompressedBytes: 0") == 1
      # Checks that bytes fetched is non-zero since incremental stats are present now
      # and should have been fetched.
      self.execute_query_expect_success(client, insert)
      profile = self.execute_query_expect_success(client, stats_part).runtime_profile
      assert profile.count("StatsFetch") > 1
      assert profile.count("StatsFetch.CompressedBytes") == 1
      assert profile.count("StatsFetch.CompressedBytes: 0") == 0
      # Adds a partition, computes stats, and checks that the metrics in the profile
      # reflect the operation.
      alter = "alter table test add partition(year=2011, month=1)"
      insert_new_partition = """
          insert into test partition(year=2011, month=1) values
          (29349999, true, 4, 4, 4, 40,4.400000095367432,40.4,
          "10/21/09","4","2009-10-21 03:24:09.600000000")
          """
      self.execute_query_expect_success(client, alter)
      self.execute_query_expect_success(client, insert_new_partition)
      profile = self.execute_query_expect_success(client, stats_all).runtime_profile
      assert profile.count("StatsFetch.TotalPartitions: 25") == 1
      assert profile.count("StatsFetch.NumPartitionsWithStats: 24") == 1
    finally:
      client.close()
