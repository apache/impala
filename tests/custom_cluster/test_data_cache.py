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


class TestDataCache(CustomClusterTestSuite):
  """ This test enables the data cache and verfies that cache hit and miss counts
  in the runtime profile and metrics are as expected.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--always_use_data_cache=true --data_cache_write_concurrency=64",
      start_args="--data_cache_dir=/tmp --data_cache_size=500MB", cluster_size=1)
  def test_data_cache(self, vector, unique_database):
      """ This test creates a temporary table from another table, overwrites it with
      some other data and verifies that no stale data is read from the cache. Runs
      with a single node to make it easier to verify the runtime profile.  """
      self.run_test_case('QueryTest/data-cache', vector, unique_database)
      assert self.get_metric('impala-server.io-mgr.remote-data-cache-dropped-bytes') >= 0
      assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-bytes') > 0
      assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-bytes') > 0
      assert self.get_metric('impala-server.io-mgr.remote-data-cache-total-bytes') > 0
