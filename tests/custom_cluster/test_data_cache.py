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
from tests.common.skip import SkipIf, SkipIfNotHdfsMinicluster


@SkipIf.is_buggy_el6_kernel
@SkipIfNotHdfsMinicluster.scheduling
class TestDataCache(CustomClusterTestSuite):
  """ This test enables the data cache and verfies that cache hit and miss counts
  in the runtime profile and metrics are as expected. Run on non-EC HDFS only as
  this test checks the number of data cache hit counts, which implicitly relies
  on the scheduler's behavior and number of HDFS blocks.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestDataCache, cls).setup_class()

  def get_impalad_args(eviction_policy, high_write_concurrency=True,
                       force_single_shard=True):
    impalad_args = ["--always_use_data_cache=true"]
    if (high_write_concurrency):
      impalad_args.append("--data_cache_write_concurrency=64")
    if (force_single_shard):
      impalad_args.append("--cache_force_single_shard")
    impalad_args.append("--data_cache_eviction_policy={0}".format(eviction_policy))
    return " ".join(impalad_args)

  CACHE_START_ARGS = "--data_cache_dir=/tmp --data_cache_size=500MB"

  def __test_data_cache_deterministic(self, vector, unique_database):
    """ This test creates a temporary table from another table, overwrites it with
    some other data and verifies that no stale data is read from the cache. Runs with
    a single node to make it easier to verify the runtime profile. Also enables higher
    write concurrency and uses a single shard to avoid non-determinism.
    """
    opened_file_handles_metric = 'impala-server.io.mgr.cached-file-handles-miss-count'
    self.run_test_case('QueryTest/data-cache', vector, unique_database)
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-dropped-bytes') >= 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-dropped-entries') >= 0
    assert \
        self.get_metric('impala-server.io-mgr.remote-data-cache-instant-evictions') >= 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-count') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-count') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-total-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-entries') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-writes') > 0

    # Expect all cache hits results in no opened files.
    opened_file_handles_metric = 'impala-server.io.mgr.cached-file-handles-miss-count'
    baseline = self.get_metric(opened_file_handles_metric)
    self.execute_query("select count(distinct l_orderkey) from test_parquet")
    assert self.get_metric(opened_file_handles_metric) == baseline

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU"),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_deterministic_lru(self, vector, unique_database):
    self.__test_data_cache_deterministic(vector, unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS"),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_deterministic_lirs(self, vector, unique_database):
    self.__test_data_cache_deterministic(vector, unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU") + " --max_cached_file_handles=0",
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_deterministic_no_file_handle_cache(self, vector, unique_database):
    self.__test_data_cache_deterministic(vector, unique_database)

  def __test_data_cache(self, vector):
    """ This test scans the same table twice and verifies the cache hit count metrics
    are correct. The exact number of bytes hit is non-deterministic between runs due
    to different mtime of files and multiple shards in the cache.
    """
    QUERY = "select * from tpch_parquet.lineitem"
    # Do a first run to warm up the cache. Expect no hits.
    self.execute_query(QUERY)
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-bytes') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-count') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-count') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-total-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-entries') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-writes') > 0

    # Do a second run. Expect some hits.
    self.execute_query(QUERY)
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-bytes') > 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-count') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU", high_write_concurrency=False,
                                    force_single_shard=False),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_lru(self, vector):
    self.__test_data_cache(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS", high_write_concurrency=False,
                                    force_single_shard=False),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_lirs(self, vector):
    self.__test_data_cache(vector)

  def __test_data_cache_disablement(self, vector):
    # Verifies that the cache metrics are all zero.
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-bytes') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-hit-count') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-bytes') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-miss-count') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-total-bytes') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-entries') == 0
    assert self.get_metric('impala-server.io-mgr.remote-data-cache-num-writes') == 0

    # Runs a query with the cache disabled and then enabled against multiple file formats.
    # Verifies that the metrics stay at zero when the cache is disabled.
    for disable_cache in [True, False]:
      vector.get_value('exec_option')['disable_data_cache'] = int(disable_cache)
      for file_format in ['text_gzip', 'parquet', 'avro', 'seq', 'rc']:
        QUERY = "select * from functional_{0}.alltypes".format(file_format)
        self.execute_query(QUERY, vector.get_value('exec_option'))
        assert disable_cache ==\
            (self.get_metric('impala-server.io-mgr.remote-data-cache-miss-bytes') == 0)
        assert disable_cache ==\
            (self.get_metric('impala-server.io-mgr.remote-data-cache-miss-count') == 0)
        assert disable_cache ==\
            (self.get_metric('impala-server.io-mgr.remote-data-cache-total-bytes') == 0)
        assert disable_cache ==\
            (self.get_metric('impala-server.io-mgr.remote-data-cache-num-entries') == 0)
        assert disable_cache ==\
            (self.get_metric('impala-server.io-mgr.remote-data-cache-num-writes') == 0)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU"),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_disablement_lru(self, vector):
    self.__test_data_cache_disablement(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS"),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_disablement_lirs(self, vector):
    self.__test_data_cache_disablement(vector)
