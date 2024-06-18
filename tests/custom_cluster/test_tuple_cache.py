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
import random
import string

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestDimension

TABLE_LAYOUT = 'name STRING, age INT, address STRING'
CACHE_START_ARGS = "--tuple_cache_dir=/tmp --log_level=2"


# Generates a random table entry of at least 15 bytes.
def table_value(seed):
  r = random.Random(seed)
  name = "".join([r.choice(string.ascii_letters) for _ in range(r.randint(5, 20))])
  age = r.randint(1, 90)
  address = "{0} {1}".format(r.randint(1, 9999),
      "".join([r.choice(string.ascii_letters) for _ in range(r.randint(4, 12))]))
  return '"{0}", {1}, "{2}"'.format(name, age, address)


def assertCounters(profile, num_hits, num_halted, num_skipped):
  assert "NumTupleCacheHits: {0} ".format(num_hits) in profile
  assert "NumTupleCacheHalted: {0} ".format(num_halted) in profile
  assert "NumTupleCacheSkipped: {0} ".format(num_skipped) in profile


def get_cache_keys(profile):
  cache_keys = []
  for line in profile.splitlines():
    if "Combined Key:" in line:
      key = line.split(":")[1].strip()
      cache_keys.append(key)
  return cache_keys


def assert_deterministic_scan(profile):
  assert "deterministic scan range assignment: true" in profile


class TestTupleCacheBase(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def cached_query(self, query, mt_dop=1):
    return self.execute_query(query,
        {"ENABLE_TUPLE_CACHE": "TRUE", "MT_DOP": str(mt_dop)})

  def cached_query_w_debugaction(self, query, debugaction):
    query_opts = {
      "ENABLE_TUPLE_CACHE": "TRUE",
      "MT_DOP": "1",
      "DEBUG_ACTION": debugaction
    }
    return self.execute_query(query, query_opts)

  # Generates a table containing at least <scale> KB of data.
  def create_table(self, fq_table, scale=1):
    self.cached_query("CREATE TABLE {0} ({1})".format(fq_table, TABLE_LAYOUT))
    # To make the rows distinct, we keep using a different seed for table_value
    global_index = 0
    for _ in range(scale):
      values = [table_value(i) for i in range(global_index, global_index + 70)]
      self.cached_query("INSERT INTO {0} VALUES ({1})".format(
          fq_table, "), (".join(values)))
      global_index += 70

  # Helper function to get a tuple cache metric from a single impalad.
  def get_tuple_cache_metric(self, impalaservice, suffix):
    return impalaservice.get_metric_value('impala.tuple-cache.' + suffix)


class TestTupleCache(TestTupleCacheBase):

  @CustomClusterTestSuite.with_args(cluster_size=1)
  @pytest.mark.execute_serially
  def test_cache_disabled(self, vector, unique_database):
    fq_table = "{0}.cache_disabled".format(unique_database)
    self.create_table(fq_table)
    result1 = self.cached_query("SELECT * from {0}".format(fq_table))
    result2 = self.cached_query("SELECT * from {0}".format(fq_table))

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)
    assertCounters(result2.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

  @CustomClusterTestSuite.with_args(
      start_args=CACHE_START_ARGS, cluster_size=1)
  @pytest.mark.execute_serially
  def test_create_and_select(self, vector, unique_database):
    fq_table = "{0}.create_and_select".format(unique_database)
    self.create_table(fq_table)
    result1 = self.cached_query("SELECT * from {0}".format(fq_table))
    result2 = self.cached_query("SELECT * from {0}".format(fq_table))

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(result2.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)

  @CustomClusterTestSuite.with_args(
      start_args=CACHE_START_ARGS + " --tuple_cache_capacity=64MB", cluster_size=1,
      impalad_args="--cache_force_single_shard")
  @pytest.mark.execute_serially
  def test_cache_halted_select(self, vector, unique_database):
    # The cache is set to the minimum cache size, so run a SQL that produces enough
    # data to exceed the cache size and halt caching.
    big_enough_query = "SELECT o_comment from tpch.orders"
    result1 = self.cached_query(big_enough_query)
    result2 = self.cached_query(big_enough_query)

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=1, num_skipped=0)
    assertCounters(result2.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

  @CustomClusterTestSuite.with_args(
    start_args=CACHE_START_ARGS, cluster_size=1)
  @pytest.mark.execute_serially
  def test_failpoints(self, vector, unique_database):
    fq_table = "{0}.create_and_select".format(unique_database)
    # Scale 20 gets us enough rows to force multiple RowBatches (needed for the
    # the reader GetNext() cases).
    self.create_table(fq_table, scale=20)
    query = "SELECT * from {0}".format(fq_table)

    # Fail when writing cache entry. All of these are handled and will not fail the
    # query.
    # Case 1: fail during Open()
    result = self.cached_query_w_debugaction(query, "TUPLE_FILE_WRITER_OPEN:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

    # Case 2: fail during Write()
    result = self.cached_query_w_debugaction(query, "TUPLE_FILE_WRITER_WRITE:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Case 3: fail during Commit()
    result = self.cached_query_w_debugaction(query, "TUPLE_FILE_WRITER_COMMIT:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Now, successfully add a cache entry
    result1 = self.cached_query(query)
    assert result1.success
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Fail when reading a cache entry
    # Case 1: fail during Open()
    result = self.cached_query_w_debugaction(query, "TUPLE_FILE_READER_OPEN:FAIL@1.0")
    assert result.success
    # Do an unordered compare (the rows are unique)
    assert set(result.data) == set(result1.data)
    # Not a hit
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

    # Case 2: fail during the first GetNext() call
    result = self.cached_query_w_debugaction(query,
        "TUPLE_FILE_READER_FIRST_GETNEXT:FAIL@1.0")
    assert result.success
    # Do an unordered compare (the rows are unique)
    assert set(result.data) == set(result1.data)
    # Technically, this is a hit
    assertCounters(result.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)

    # Case 3: fail during the second GetNext() call
    # This one must fail for correctness, as it cannot fall back to the child if it
    # has already returned cached rows
    hit_error = False
    try:
      result = self.cached_query_w_debugaction(query,
          "TUPLE_FILE_READER_SECOND_GETNEXT:FAIL@1.0")
    except Exception:
      hit_error = True

    assert hit_error


class TestTupleCacheRuntimeKeys(TestTupleCacheBase):

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheRuntimeKeys, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *[0, 1, 2]))

  @CustomClusterTestSuite.with_args(
    start_args=CACHE_START_ARGS, cluster_size=1)
  @pytest.mark.execute_serially
  def test_scan_range_basics(self, vector, unique_database):
    """
    This tests that adding/removing files to a table results in different keys.
    This runs on a single node with mt_dop=0 or mt_dop=1, so it is the simplest
    test.
    """
    mt_dop = vector.get_value('mt_dop')
    # To keep this simple, we skip mt_dop > 1.
    if mt_dop > 1:
      pytest.skip()
    fq_table = "{0}.scan_range_basics_mtdop{1}".format(unique_database, mt_dop)
    query = "SELECT * from {0}".format(fq_table)

    # Create an empty table
    self.create_table(fq_table, scale=0)

    # When there are no scan ranges, then fragment instance key is 0. This is
    # somewhat of a toy case and we probably want to avoid caching in this
    # case. Nonetheless, it is a good sanity check.
    empty_result = self.cached_query(query, mt_dop=mt_dop)
    cache_keys = get_cache_keys(empty_result.runtime_profile)
    assert len(cache_keys) == 1
    empty_table_compile_key, empty_table_finst_key = cache_keys[0].split("_")
    assert empty_table_finst_key == "0"
    assert len(empty_result.data) == 0
    if mt_dop > 0:
      assert_deterministic_scan(empty_result.runtime_profile)

    # Insert a row, which creates a file / scan range
    self.cached_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(0)))

    # Now, there is a scan range, so the fragment instance key should be non-zero.
    one_file_result = self.cached_query(query, mt_dop=mt_dop)
    cache_keys = get_cache_keys(one_file_result.runtime_profile)
    assert len(cache_keys) == 1
    one_file_compile_key, one_file_finst_key = cache_keys[0].split("_")
    assert one_file_finst_key != "0"
    # This should be a cache miss
    assertCounters(one_file_result.runtime_profile, 0, 0, 0)
    assert len(one_file_result.data) == 1
    if mt_dop > 0:
      assert_deterministic_scan(one_file_result.runtime_profile)

    # The new scan range did not change the compile-time key
    assert empty_table_compile_key == one_file_compile_key

    # Insert another row, which creates a file / scan range
    self.cached_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(1)))

    # There is a second scan range, so the fragment instance key should change again
    two_files_result = self.cached_query(query, mt_dop=mt_dop)
    cache_keys = get_cache_keys(two_files_result.runtime_profile)
    assert len(cache_keys) == 1
    two_files_compile_key, two_files_finst_key = cache_keys[0].split("_")
    assert two_files_finst_key != "0"
    assertCounters(two_files_result.runtime_profile, 0, 0, 0)
    assert len(two_files_result.data) == 2
    assert one_file_finst_key != two_files_finst_key
    overlapping_rows = set(one_file_result.data).intersection(set(two_files_result.data))
    assert len(overlapping_rows) == 1
    if mt_dop > 0:
      assert_deterministic_scan(two_files_result.runtime_profile)

    # The new scan range did not change the compile-time key
    assert one_file_compile_key == two_files_compile_key

    # Invalidate metadata and rerun the last query. The keys should stay the same.
    self.cached_query("invalidate metadata")
    rerun_two_files_result = self.cached_query(query, mt_dop=mt_dop)
    # Verify that this is a cache hit
    assertCounters(rerun_two_files_result.runtime_profile, 1, 0, 0)
    cache_keys = get_cache_keys(rerun_two_files_result.runtime_profile)
    assert len(cache_keys) == 1
    rerun_two_files_compile_key, rerun_two_files_finst_key = cache_keys[0].split("_")
    assert rerun_two_files_finst_key == two_files_finst_key
    assert rerun_two_files_compile_key == two_files_compile_key
    assert rerun_two_files_result.data == two_files_result.data

  @CustomClusterTestSuite.with_args(
    start_args=CACHE_START_ARGS, cluster_size=1)
  @pytest.mark.execute_serially
  def test_scan_range_partitioned(self, vector, unique_database):
    """
    This tests a basic partitioned case where the query is identical except that
    it operates on different partitions (and thus different scan ranges)
    This runs on a single node with mt_dop=0 or mt_dop=1 to keep it simple.
    """
    mt_dop = vector.get_value('mt_dop')
    # To keep this simple, we skip mt_dop > 1.
    if mt_dop > 1:
      pytest.skip()
    year2009_result = self.cached_query(
        "select * from functional.alltypes where year=2009", mt_dop=mt_dop)
    cache_keys = get_cache_keys(year2009_result.runtime_profile)
    assert len(cache_keys) == 1
    year2009_compile_key, year2009_finst_key = cache_keys[0].split("_")

    year2010_result = self.cached_query(
        "select * from functional.alltypes where year=2010", mt_dop=mt_dop)
    cache_keys = get_cache_keys(year2010_result.runtime_profile)
    assert len(cache_keys) == 1
    year2010_compile_key, year2010_finst_key = cache_keys[0].split("_")
    # This should be a cache miss
    assertCounters(year2010_result.runtime_profile, 0, 0, 0)

    # The year=X predicate is on a partition column, so it is enforced by pruning
    # partitions and doesn't carry through to execution. The compile keys for
    # the two queries are the same, but the fragment instance keys are different due
    # to the different scan ranges from different partitions.
    assert year2009_compile_key == year2010_compile_key
    assert year2009_finst_key != year2010_finst_key
    # Verify that the results are completely different
    year2009_result_set = set(year2009_result.data)
    year2010_result_set = set(year2010_result.data)
    overlapping_rows = year2009_result_set.intersection(year2010_result_set)
    assert len(overlapping_rows) == 0
    assert year2009_result.data[0].find("2009") != -1
    assert year2009_result.data[0].find("2010") == -1
    assert year2010_result.data[0].find("2010") != -1
    assert year2010_result.data[0].find("2009") == -1

  @CustomClusterTestSuite.with_args(start_args=CACHE_START_ARGS)
  @pytest.mark.execute_serially
  def test_scan_range_distributed(self, vector, unique_database):
    """
    This tests the distributed case where there are multiple fragment instances
    processing different scan ranges. Each fragment instance should have a
    distinct cache key. When adding a scan range, at least one fragment instance
    cache key should change.
    """

    mt_dop = vector.get_value('mt_dop')
    fq_table = "{0}.scan_range_basics_mtdop{1}".format(unique_database, mt_dop)
    query = "SELECT * from {0}".format(fq_table)

    # Create a table with several files so that we always have enough work for multiple
    # fragment instances
    self.create_table(fq_table, scale=20)

    # We run a simple select. This is running with multiple impalads, so there are
    # always multiple fragment instances
    before_result = self.cached_query(query, mt_dop=mt_dop)
    cache_keys = get_cache_keys(before_result.runtime_profile)
    expected_num_keys = 3 * max(mt_dop, 1)
    assert len(cache_keys) == expected_num_keys
    # Every cache key should be distinct, as the fragment instances are processing
    # different data
    unique_cache_keys = set(cache_keys)
    assert len(unique_cache_keys) == expected_num_keys
    # Every cache key has the same compile key
    unique_compile_keys = set([key.split("_")[0] for key in unique_cache_keys])
    assert len(unique_compile_keys) == 1
    # Verify the cache metrics for each impalad. Since we started from scratch, the
    # number of entries in the cache should be the same as the number of cache keys.
    for impalad in self.cluster.impalads:
      entries_in_use = self.get_tuple_cache_metric(impalad.service, "entries-in-use")
      assert entries_in_use == max(mt_dop, 1)
    if mt_dop > 0:
      assert_deterministic_scan(before_result.runtime_profile)

    # Insert another row, which creates a file / scan range
    # This uses a very large seed for table_value() to get a unique row that isn't
    # already in the table.
    self.cached_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(1000000)))

    # Rerun the query with the extra scan range
    after_insert_result = self.cached_query(query, mt_dop=mt_dop)
    cache_keys = get_cache_keys(after_insert_result.runtime_profile)
    expected_num_keys = 3 * max(mt_dop, 1)
    assert len(cache_keys) == expected_num_keys
    # Every cache key should be distinct, as the fragment instances are processing
    # different data
    after_insert_unique_cache_keys = set(cache_keys)
    assert len(after_insert_unique_cache_keys) == expected_num_keys
    # Every cache key has the same compile key
    unique_compile_keys = \
        set([key.split("_")[0] for key in after_insert_unique_cache_keys])
    assert len(unique_compile_keys) == 1
    # Verify the cache metrics. We can do a more exact bound by looking at the total
    # across all impalads. The lower bound for this is the number of unique cache
    # keys across both queries we ran. The upper bound for the number of entries is
    # double the expected number from the first run of the query.
    #
    # This is not the exact number, because cache key X could have run on executor 1
    # for the first query and on executor 2 for the second query. Even though it would
    # appear as a single unique cache key, it is two different cache entries in different
    # executors.
    all_cache_keys = unique_cache_keys.union(after_insert_unique_cache_keys)
    total_entries_in_use = 0
    for impalad in self.cluster.impalads:
      entries_in_use = self.get_tuple_cache_metric(impalad.service, "entries-in-use")
      assert entries_in_use >= max(mt_dop, 1)
      assert entries_in_use <= (2 * max(mt_dop, 1))
      total_entries_in_use += entries_in_use
    assert total_entries_in_use >= len(all_cache_keys)
    if mt_dop > 0:
      assert_deterministic_scan(after_insert_result.runtime_profile)

    # The extra scan range means that at least one fragment instance key changed
    # Since scheduling can change completely with the addition of a single scan range,
    # we can't assert that only one cache key changes.
    changed_cache_keys = unique_cache_keys.symmetric_difference(
        after_insert_unique_cache_keys)
    assert len(changed_cache_keys) != 0

    # Each row is distinct, so that makes it easy to verify that the results overlap
    # except the second result contains one more row than the first result.
    before_result_set = set(before_result.data)
    after_insert_result_set = set(after_insert_result.data)
    assert len(before_result_set) == 70 * 20
    assert len(before_result_set) + 1 == len(after_insert_result_set)
    different_rows = before_result_set.symmetric_difference(after_insert_result_set)
    assert len(different_rows) == 1
