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
from builtins import range
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.network import get_external_ip
from tests.common.skip import SkipIfLocal
from tests.util.filesystem_utils import (
    IS_ISILON,
    IS_ADLS,
    IS_GCS,
    IS_COS,
    IS_OSS)
from time import sleep


@SkipIfLocal.hdfs_fd_caching
class TestHdfsFdCaching(CustomClusterTestSuite):
  """Tests that if HDFS file handle caching is enabled, file handles are actually cached
  and the associated metrics return valid results. In addition, tests that the upper bound
  of cached file handles is respected."""

  NUM_ROWS = 100
  INSERT_TPL = "insert into cachefd.simple values"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def create_n_files(self, n):
    """Creates 'n' files by performing 'n' inserts with NUM_ROWS rows."""
    values = ", ".join(["({0},{0},{0})".format(x) for x in range(self.NUM_ROWS)])
    for _ in range(n):
      self.client.execute(self.INSERT_TPL + values)

  def setup_method(self, method):
    super(TestHdfsFdCaching, self).setup_method(method)
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()

    self.client = client
    client.execute("drop database if exists cachefd cascade")
    client.execute("create database cachefd")
    client.execute("create table cachefd.simple(id int, col1 int, col2 int) "
                   "stored as parquet")
    self.create_n_files(1)

  def teardown_method(self, method):
    super(TestHdfsFdCaching, self).teardown_method(method)
    self.client.execute("drop database if exists cachefd cascade")

  def run_fd_caching_test(self, vector, caching_expected, cache_capacity,
      eviction_timeout_secs):
    """
    Tests that HDFS file handles are cached as expected. This is used both
    for the positive and negative test cases. If caching_expected is true,
    this verifies that the cache adheres to the specified capacity. Also,
    repeated queries across the same files reuse the file handles.
    If caching_expected is false, it verifies that the cache does not
    change in size while running queries.
    """

    # Maximum number of file handles cached (applies whether caching expected
    # or not)
    assert self.max_cached_handles() <= cache_capacity

    num_handles_start = self.cached_handles()
    # The table has one file. If caching is expected, there should be one more
    # handle cached after the first select. If caching is not expected, the
    # number of handles should not change from the initial number.
    self.execute_query("select * from cachefd.simple", vector=vector)
    num_handles_after = self.cached_handles()
    assert self.max_cached_handles() <= cache_capacity

    if caching_expected:
      assert num_handles_after == (num_handles_start + 1)
    else:
      assert num_handles_after == num_handles_start

    # No open handles if scanning is finished
    assert self.outstanding_handles() == 0

    # No change when reading the table again
    for x in range(10):
      self.execute_query("select * from cachefd.simple", vector=vector)
      assert self.cached_handles() == num_handles_after
      assert self.max_cached_handles() <= cache_capacity
      assert self.outstanding_handles() == 0

    # Create more files. This means there are more files than the cache size.
    # The cache size should still be enforced.
    self.create_n_files(cache_capacity + 100)

    # Read all the files of the table and make sure no FD leak
    for x in range(10):
      self.execute_query("select count(*) from cachefd.simple;", vector=vector)
      assert self.max_cached_handles() <= cache_capacity
      if not caching_expected:
        assert self.cached_handles() == num_handles_start
    assert self.outstanding_handles() == 0

    if caching_expected and eviction_timeout_secs is not None:
      # To test unused file handle eviction, sleep for longer than the timeout.
      # All the cached handles should be evicted.
      sleep(eviction_timeout_secs + 5)
      assert self.cached_handles() == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--max_cached_file_handles=16"
                   " --unused_file_handle_timeout_sec=18446744073709551600",
      catalogd_args="--load_catalog_in_background=false")
  def test_caching_enabled(self, vector):
    """
    Test of the HDFS file handle cache with the parameter specified and a very
    large file handle timeout
    """
    cache_capacity = 16

    # Caching applies to HDFS, Ozone, S3, and ABFS files. If this is HDFS, Ozone, S3, or
    # ABFS, then verify that caching works. Otherwise, verify that file handles are not
    # cached.
    if IS_ADLS or IS_ISILON or IS_GCS or IS_COS or IS_OSS:
      caching_expected = False
    else:
      caching_expected = True
    self.run_fd_caching_test(vector, caching_expected, cache_capacity, None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--max_cached_file_handles=16 --unused_file_handle_timeout_sec=5",
      catalogd_args="--load_catalog_in_background=false")
  def test_caching_with_eviction(self, vector):
    """Test of the HDFS file handle cache with unused file handle eviction enabled"""
    cache_capacity = 16
    handle_timeout = 5

    # Only test eviction on platforms where caching is enabled.
    if IS_ADLS or IS_ISILON or IS_GCS or IS_COS or IS_OSS:
      return
    caching_expected = True
    self.run_fd_caching_test(vector, caching_expected, cache_capacity, handle_timeout)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--max_cached_file_handles=0",
      catalogd_args="--load_catalog_in_background=false")
  def test_caching_disabled_by_param(self, vector):
    """Test that the HDFS file handle cache is disabled when the parameter is zero"""
    cache_capacity = 0
    caching_expected = False
    self.run_fd_caching_test(vector, caching_expected, cache_capacity, None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--cache_remote_file_handles=false --cache_s3_file_handles=false "
                   "--cache_abfs_file_handles=false --cache_ozone_file_handles=false "
                   "--hostname=" + get_external_ip(),
      catalogd_args="--load_catalog_in_background=false")
  def test_remote_caching_disabled_by_param(self, vector):
    """Test that the file handle cache is disabled for remote files when disabled"""
    cache_capacity = 0
    caching_expected = False
    self.run_fd_caching_test(vector, caching_expected, cache_capacity, None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--max_cached_file_handles=0 --hostname=" + get_external_ip(),
      catalogd_args="--load_catalog_in_background=false")
  def test_remote_caching_disabled_by_global_param(self, vector):
    """Test that the file handle cache is disabled for remote files when all caching is
    disabled"""
    cache_capacity = 0
    caching_expected = False
    self.run_fd_caching_test(vector, caching_expected, cache_capacity, None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--max_cached_file_handles=16 --unused_file_handle_timeout_sec=5 "
                   "--always_use_data_cache=true",
      start_args="--data_cache_dir=/tmp --data_cache_size=500MB",
      catalogd_args="--load_catalog_in_background=false")
  def test_no_fd_caching_on_cached_data(self, vector):
    """IMPALA-10147: Test that no file handle should be opened nor cached again if data
    is being read from data cache."""
    cache_capacity = 16
    eviction_timeout_secs = 5

    # Only test eviction on platforms where caching is enabled.
    if IS_ADLS or IS_ISILON or IS_GCS or IS_COS or IS_OSS:
      return

    # Maximum number of file handles cached.
    assert self.max_cached_handles() <= cache_capacity

    num_handles_start = self.cached_handles()
    # The table has one file. If caching is expected, there should be one more
    # handle cached after the first select. If caching is not expected, the
    # number of handles should not change from the initial number.
    # Read 5 times to make sure the data cache is fully warmed up.
    for x in range(5):
      self.execute_query("select * from cachefd.simple", vector=vector)
      num_handles_after = self.cached_handles()
      assert self.max_cached_handles() <= cache_capacity
      assert num_handles_after == (num_handles_start + 1)

    # No open handles if scanning is finished.
    assert self.outstanding_handles() == 0

    # To test unused file handle eviction, sleep for longer than the timeout.
    # All the cached handles should be evicted.
    sleep(eviction_timeout_secs + 5)
    assert self.cached_handles() == 0

    # Reread from data cache. Expect that no handle should be opened nor cached again.
    for x in range(10):
      self.execute_query("select * from cachefd.simple", vector=vector)
      assert self.cached_handles() == 0
      assert self.max_cached_handles() <= cache_capacity
      assert self.outstanding_handles() == 0

  def cached_handles(self):
    return self.get_agg_metric("impala-server.io.mgr.num-cached-file-handles")

  def outstanding_handles(self):
    return self.get_agg_metric("impala-server.io.mgr.num-file-handles-outstanding")

  def max_cached_handles(self):
    return self.get_agg_metric("impala-server.io.mgr.num-cached-file-handles", max)

  def get_agg_metric(self, key, fun=sum):
    cluster = self.cluster
    return fun([s.service.get_metric_value(key) for s in cluster.impalads])
