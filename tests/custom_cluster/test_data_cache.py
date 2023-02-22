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
from signal import SIGRTMIN
from time import sleep

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
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestDataCache, cls).setup_class()

  def get_impalad_args(eviction_policy, high_write_concurrency=True,
                       force_single_shard=True, keep_across_restarts=False):
    impalad_args = ["--always_use_data_cache=true"]
    if (high_write_concurrency):
      impalad_args.append("--data_cache_write_concurrency=64")
    if (force_single_shard):
      impalad_args.append("--cache_force_single_shard")
    if (keep_across_restarts):
      impalad_args.append("--data_cache_keep_across_restarts=true")
      impalad_args.append("--shutdown_grace_period_s=1")
    impalad_args.append("--data_cache_eviction_policy={0}".format(eviction_policy))
    return " ".join(impalad_args)

  def get_data_cache_metric(self, suffix):
    return self.get_metric('impala-server.io-mgr.remote-data-cache-' + suffix)

  CACHE_START_ARGS = "--data_cache_dir=/tmp --data_cache_size=500MB"

  def __test_data_cache_deterministic(self, vector, unique_database):
    """ This test creates a temporary table from another table, overwrites it with
    some other data and verifies that no stale data is read from the cache. Runs with
    a single node to make it easier to verify the runtime profile. Also enables higher
    write concurrency and uses a single shard to avoid non-determinism.
    """
    self.run_test_case('QueryTest/data-cache', vector, unique_database)
    assert self.get_data_cache_metric('dropped-bytes') >= 0
    assert self.get_data_cache_metric('dropped-entries') >= 0
    assert self.get_data_cache_metric('instant-evictions') >= 0
    assert self.get_data_cache_metric('hit-bytes') > 0
    assert self.get_data_cache_metric('hit-count') > 0
    assert self.get_data_cache_metric('miss-bytes') > 0
    assert self.get_data_cache_metric('miss-count') > 0
    assert self.get_data_cache_metric('total-bytes') > 0
    assert self.get_data_cache_metric('num-entries') > 0
    assert self.get_data_cache_metric('num-writes') > 0

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
    assert self.get_data_cache_metric('hit-bytes') == 0
    assert self.get_data_cache_metric('hit-count') == 0
    assert self.get_data_cache_metric('miss-bytes') > 0
    assert self.get_data_cache_metric('miss-count') > 0
    assert self.get_data_cache_metric('total-bytes') > 0
    assert self.get_data_cache_metric('num-entries') > 0
    assert self.get_data_cache_metric('num-writes') > 0

    # Do a second run. Expect some hits.
    self.execute_query(QUERY)
    assert self.get_data_cache_metric('hit-bytes') > 0
    assert self.get_data_cache_metric('hit-count') > 0

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
    assert self.get_data_cache_metric('hit-bytes') == 0
    assert self.get_data_cache_metric('hit-count') == 0
    assert self.get_data_cache_metric('miss-bytes') == 0
    assert self.get_data_cache_metric('miss-count') == 0
    assert self.get_data_cache_metric('total-bytes') == 0
    assert self.get_data_cache_metric('num-entries') == 0
    assert self.get_data_cache_metric('num-writes') == 0

    # Runs a query with the cache disabled and then enabled against multiple file formats.
    # Verifies that the metrics stay at zero when the cache is disabled.
    for disable_cache in [True, False]:
      vector.get_value('exec_option')['disable_data_cache'] = int(disable_cache)
      for file_format in ['text_gzip', 'parquet', 'avro', 'seq', 'rc']:
        QUERY = "select * from functional_{0}.alltypes".format(file_format)
        self.execute_query(QUERY, vector.get_value('exec_option'))
        assert disable_cache == (self.get_data_cache_metric('miss-bytes') == 0)
        assert disable_cache == (self.get_data_cache_metric('miss-count') == 0)
        assert disable_cache == (self.get_data_cache_metric('total-bytes') == 0)
        assert disable_cache == (self.get_data_cache_metric('num-entries') == 0)
        assert disable_cache == (self.get_data_cache_metric('num-writes') == 0)

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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS", high_write_concurrency=False),
      start_args="--data_cache_dir=/tmp --data_cache_size=9MB",
      cluster_size=1)
  def test_data_cache_lirs_instant_evictions(self, vector):
    # The setup for this test is intricate. For Allocate() to succeed, the request
    # needs to be smaller than the protected size (95% of the cache). For Insert() to
    # fail, the request needs to be larger than the unprotected size (5% of the cache).
    # So, for an 8MB cache store to fail, the cache needs to be > 8.4MB (8MB / 0.95)
    # and less than 160MB (8MB / 0.05). This sets it to 9MB, which should result in
    # 8MB cache inserts to be instantly evicted.
    QUERY = "select count(*) from tpch.lineitem"
    self.execute_query(QUERY)
    assert self.get_data_cache_metric('miss-bytes') > 0
    assert self.get_data_cache_metric('miss-count') > 0
    assert self.get_data_cache_metric('total-bytes') >= 0
    assert self.get_data_cache_metric('num-entries') >= 0
    assert self.get_data_cache_metric('num-writes') >= 0
    assert self.get_data_cache_metric('instant-evictions') > 0

    # Run the query multiple times and verify that none of the counters go negative
    instant_evictions_before = \
        self.get_data_cache_metric('instant-evictions')
    for i in range(10):
      self.execute_query(QUERY)
    instant_evictions_after = \
        self.get_data_cache_metric('instant-evictions')
    assert instant_evictions_after - instant_evictions_before > 0

    # All the counters remain positive
    assert self.get_data_cache_metric('num-entries') >= 0
    assert self.get_data_cache_metric('num-writes') >= 0
    assert self.get_data_cache_metric('total-bytes') >= 0

  def __test_data_cache_keep_across_restarts(self, vector, test_reduce_size=False):
    QUERY = "select * from tpch_parquet.lineitem"
    # Execute a query, record the total bytes and the number of entries of cache before
    # cache dump.
    self.execute_query(QUERY)
    assert self.get_data_cache_metric('hit-bytes') == 0
    assert self.get_data_cache_metric('hit-count') == 0
    total_bytes = self.get_data_cache_metric('total-bytes')
    num_entries = self.get_data_cache_metric('num-entries')

    # Do graceful restart and, if necessary, reduce the cache size by 1/5.
    impalad = self.cluster.impalads[0]
    impalad.kill_and_wait_for_exit(SIGRTMIN)
    new_size = 4 * total_bytes // 5
    if test_reduce_size:
      impalad.modify_argument('-data_cache', '/tmp/impala-datacache-0:' + str(new_size))
    impalad.start()
    impalad.service.wait_for_num_known_live_backends(1)

    # After the restart, we expect the cache to have the same total bytes
    # and number of entries as before the restart, and if the cache size is reduced,
    # the metrics should be reduced accordingly.
    if test_reduce_size:
      assert self.get_data_cache_metric('total-bytes') <= new_size
      assert self.get_data_cache_metric('num-entries') < num_entries
    else:
      assert self.get_data_cache_metric('total-bytes') == total_bytes
      assert self.get_data_cache_metric('num-entries') == num_entries

    # Reconnect to the service and execute the query, expecting some cache hits.
    self.client.connect()
    self.execute_query(QUERY)
    assert self.get_data_cache_metric('hit-bytes') > 0
    assert self.get_data_cache_metric('hit-count') > 0
    if test_reduce_size:
      assert self.get_data_cache_metric('miss-bytes') > 0
      assert self.get_data_cache_metric('miss-count') > 0
    else:
      assert self.get_data_cache_metric('miss-bytes') == 0
      assert self.get_data_cache_metric('miss-count') == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_keep_across_restarts_lru(self, vector):
    self.__test_data_cache_keep_across_restarts(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_keep_across_restarts_lirs(self, vector):
    self.__test_data_cache_keep_across_restarts(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_reduce_size_restarts_lru(self, vector):
    self.__test_data_cache_keep_across_restarts(vector, test_reduce_size=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_reduce_size_restarts_lirs(self, vector):
    self.__test_data_cache_keep_across_restarts(vector, test_reduce_size=True)

  def __test_data_cache_readonly(self, vector):
    QUERY = "select * from tpch_parquet.lineitem"
    # Execute the query asynchronously, wait a short while, and do gracefully shutdown
    # immediately to test the race between cache writes and setting cache read-only.
    handle = self.execute_query_async(QUERY)
    sleep(1)
    impalad = self.cluster.impalads[0]
    impalad.kill(SIGRTMIN)
    self.client.fetch(QUERY, handle)
    self.client.close_query(handle)
    impalad.wait_for_exit()
    impalad.start()
    impalad.service.wait_for_num_known_live_backends(1)

    # We hope that in this case, the cache is still properly dumped and loaded,
    # and then the same query is executed to expect some cache hits.
    self.assert_impalad_log_contains('INFO', 'Partition 0 load successfully.')
    self.client.connect()
    self.execute_query(QUERY)
    assert self.get_data_cache_metric('hit-bytes') > 0
    assert self.get_data_cache_metric('hit-count') > 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LRU", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_readonly_lru(self, vector):
    self.__test_data_cache_readonly(vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args=get_impalad_args("LIRS", keep_across_restarts=True),
      start_args=CACHE_START_ARGS, cluster_size=1)
  def test_data_cache_readonly_lirs(self, vector):
    self.__test_data_cache_readonly(vector)
