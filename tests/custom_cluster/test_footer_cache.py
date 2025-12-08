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
from tests.common.skip import SkipIf, SkipIfNotHdfsMinicluster


@SkipIfNotHdfsMinicluster.scheduling
class TestFooterCache(CustomClusterTestSuite):
  """ This test enables the footer cache and verifies that cache hit and miss counts
  in the runtime profile and metrics are as expected. Run on non-EC HDFS only as
  this test checks the number of footer cache hit counts, which implicitly relies
  on the scheduler's behavior and number of HDFS blocks.
  """
  @classmethod
  def setup_class(cls):
    super(TestFooterCache, cls).setup_class()

  def get_footer_cache_metric(self, suffix):
    """Helper method to get footer cache metrics."""
    return self.get_metric('impala-server.io-mgr.footer-cache.' + suffix)

  def __test_footer_cache_deterministic(self, vector, unique_database):
    """ This test creates a temporary table from another table, overwrites it with
    some other data and verifies that no stale footer is read from the cache. Runs with
    a single node to make it easier to verify the runtime profile.
    """
    self.run_test_case('QueryTest/footer-cache', vector, unique_database)
    assert self.get_footer_cache_metric('hits') > 0
    assert self.get_footer_cache_metric('misses') > 0
    assert self.get_footer_cache_metric('entries-in-use') > 0
    assert self.get_footer_cache_metric('entries-in-use-bytes') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_footer_cache_deterministic(self, vector, unique_database):
    """Test footer cache with deterministic queries."""
    self.__test_footer_cache_deterministic(vector, unique_database)

  def __test_footer_cache(self):
    """ This test scans the same table twice and verifies the footer cache hit count
    metrics are correct.
    """
    QUERY = "select * from tpch_parquet.lineitem"
    # Do a first run to warm up the cache. Expect no hits.
    self.execute_query(QUERY)
    assert self.get_footer_cache_metric('hits') == 0
    assert self.get_footer_cache_metric('misses') > 0
    assert self.get_footer_cache_metric('entries-in-use') > 0
    assert self.get_footer_cache_metric('entries-in-use-bytes') > 0

    # Do a second run. Expect some hits.
    self.execute_query(QUERY)
    assert self.get_footer_cache_metric('hits') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1, force_restart=True)
  def test_footer_cache(self):
    """Test basic footer cache functionality."""
    self.__test_footer_cache()

  def __test_footer_cache_disablement(self, vector):
    """Verifies that the cache metrics are all zero when footer cache is disabled."""
    assert self.get_footer_cache_metric('hits') == 0
    assert self.get_footer_cache_metric('misses') == 0
    assert self.get_footer_cache_metric('entries-in-use') == 0
    assert self.get_footer_cache_metric('entries-in-use-bytes') == 0

    # Runs a query against parquet format.
    # Verifies that the metrics stay at zero when the cache is disabled.
    QUERY = "select * from functional_parquet.alltypes"
    self.execute_query(QUERY, vector.get_value('exec_option'))
    assert self.get_footer_cache_metric('hits') == 0
    assert self.get_footer_cache_metric('misses') == 0
    assert self.get_footer_cache_metric('entries-in-use') == 0
    assert self.get_footer_cache_metric('entries-in-use-bytes') == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--footer_cache_capacity=0",
      cluster_size=1)
  def test_footer_cache_disablement(self, vector):
    """Test that footer cache can be disabled."""
    self.__test_footer_cache_disablement(vector)

  def __test_footer_cache_with_orc(self):
    """ This test scans ORC tables and verifies the footer cache hit count
    metrics are correct for ORC format.
    """
    QUERY = "select * from functional_orc_def.alltypes"
    # Do a first run to warm up the cache. Expect no hits.
    self.execute_query(QUERY)
    assert self.get_footer_cache_metric('hits') == 0
    assert self.get_footer_cache_metric('misses') > 0
    assert self.get_footer_cache_metric('entries-in-use') > 0
    assert self.get_footer_cache_metric('entries-in-use-bytes') > 0

    # Do a second run. Expect some hits.
    self.execute_query(QUERY)
    assert self.get_footer_cache_metric('hits') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_footer_cache_with_orc(self):
    """Test footer cache with ORC format."""
    self.__test_footer_cache_with_orc()

  def __test_footer_cache_capacity(self):
    """ This test verifies that footer cache respects the capacity limit.
    Uses a small cache capacity to trigger evictions.
    """
    # Query multiple tables to fill up the cache
    queries = [
        "select * from tpch_parquet.lineitem limit 1",
        "select * from tpch_parquet.orders limit 1",
        "select * from tpch_parquet.customer limit 1",
        "select * from tpch_parquet.part limit 1",
        "select * from tpch_parquet.supplier limit 1",
        "select * from tpch_parquet.partsupp limit 1",
        "select * from tpch_parquet.nation limit 1",
        "select * from tpch_parquet.region limit 1"
    ]
    
    for query in queries:
      self.execute_query(query)

    # Verify that cache metrics are being tracked
    assert self.get_footer_cache_metric('misses') > 0

    # With small cache, some entries might have been evicted
    assert self.get_footer_cache_metric('entries-evicted') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--footer_cache_capacity=10KB",
      cluster_size=1)
  def test_footer_cache_capacity(self):
    """Test footer cache with limited capacity."""
    self.__test_footer_cache_capacity()
