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

import os
import pytest
import random
import re
import string

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (
    add_exec_option_dimension, add_mandatory_exec_option)
from tests.util.parse_util import (
    match_memory_estimate, parse_mem_to_mb, match_cache_key)

TABLE_LAYOUT = 'name STRING, age INT, address STRING'
CACHE_START_ARGS = "--tuple_cache_dir=/tmp --log_level=2"
NUM_HITS = 'NumTupleCacheHits'
NUM_HALTED = 'NumTupleCacheHalted'
NUM_SKIPPED = 'NumTupleCacheSkipped'
# Indenation used for TUPLE_CACHE_NODE in specific fragments (not averaged fragment).
NODE_INDENT = '           - '


# Generates a random table entry of at least 15 bytes.
def table_value(seed):
  r = random.Random(seed)
  name = "".join([r.choice(string.ascii_letters) for _ in range(r.randint(5, 20))])
  age = r.randint(1, 90)
  address = "{0} {1}".format(r.randint(1, 9999),
      "".join([r.choice(string.ascii_letters) for _ in range(r.randint(4, 12))]))
  return '"{0}", {1}, "{2}"'.format(name, age, address)


def getCounterValues(profile, key):
  # This matches lines like these:
  #     NumTupleCacheHits: 1 (1)
  #     TupleCacheBytesWritten: 123.00 B (123)
  # The regex extracts the value inside the parenthesis to get a simple numeric value
  # rather than a pretty print of the same value.
  counter_str_list = re.findall(r"{0}{1}: .* \((.*)\)".format(NODE_INDENT, key), profile)
  return [int(v) for v in counter_str_list]


def assertCounterOrder(profile, key, vals):
  values = getCounterValues(profile, key)
  assert values == vals, values

def assertCounter(profile, key, val, num_matches):
  if not isinstance(num_matches, list):
    num_matches = [num_matches]
  values = getCounterValues(profile, key)
  assert len([v for v in values if v == val]) in num_matches, values


def assertCounters(profile, num_hits, num_halted, num_skipped, num_matches=1):
  assertCounter(profile, NUM_HITS, num_hits, num_matches)
  assertCounter(profile, NUM_HALTED, num_halted, num_matches)
  assertCounter(profile, NUM_SKIPPED, num_skipped, num_matches)


def get_cache_keys(profile):
  cache_keys = {}
  last_node_id = -1
  matcher = re.compile(r'TUPLE_CACHE_NODE \(id=([0-9]*)\)')
  for line in profile.splitlines():
    if "Combined Key:" in line:
      key = line.split(":")[1].strip()
      cache_keys[last_node_id].append(key)
      continue

    match = matcher.search(line)
    if match:
      last_node_id = int(match.group(1))
      if last_node_id not in cache_keys:
        cache_keys[last_node_id] = []

  # Sort cache keys: with multiple nodes, order in the profile may change.
  for _, val in cache_keys.items():
    val.sort()

  return next(iter(cache_keys.values())) if len(cache_keys) == 1 else cache_keys


def assert_deterministic_scan(vector, profile):
  if vector.get_value('exec_option')['mt_dop'] > 0:
    assert "deterministic scan range assignment: true" in profile


class TestTupleCacheBase(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    super(TestTupleCacheBase, cls).setup_class()
    # Unset this environment variable to ensure it doesn't affect
    # the test like test_cache_disabled.
    cls.org_tuple_cache_dir = os.getenv("TUPLE_CACHE_DIR")
    if cls.org_tuple_cache_dir is not None:
      os.unsetenv("TUPLE_CACHE_DIR")

  @classmethod
  def teardown_class(cls):
    if cls.org_tuple_cache_dir is not None:
      os.environ["TUPLE_CACHE_DIR"] = cls.org_tuple_cache_dir
    super(TestTupleCacheBase, cls).teardown_class()

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheBase, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'enable_tuple_cache', 'true')

  # Generates a table containing at least <scale> KB of data.
  def create_table(self, fq_table, scale=1):
    self.execute_query("CREATE TABLE {0} ({1})".format(fq_table, TABLE_LAYOUT))
    # To make the rows distinct, we keep using a different seed for table_value
    global_index = 0
    for _ in range(scale):
      values = [table_value(i) for i in range(global_index, global_index + 70)]
      self.execute_query("INSERT INTO {0} VALUES ({1})".format(
          fq_table, "), (".join(values)))
      global_index += 70

  # Helper function to get a tuple cache metric from a single impalad.
  def get_tuple_cache_metric(self, impalaservice, suffix):
    return impalaservice.get_metric_value('impala.tuple-cache.' + suffix)


class TestTupleCacheOptions(TestTupleCacheBase):
  """Tests Impala with different tuple cache startup options."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheOptions, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'mt_dop', 1)

  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_cache_disabled(self, vector, unique_database):
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.cache_disabled".format(unique_database)
    self.create_table(fq_table)
    result1 = self.execute_query("SELECT * from {0}".format(fq_table))
    result2 = self.execute_query("SELECT * from {0}".format(fq_table))

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)
    assertCounters(result2.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

  @CustomClusterTestSuite.with_args(
      start_args=CACHE_START_ARGS + " --tuple_cache_capacity=64MB", cluster_size=1,
      impalad_args="--cache_force_single_shard")
  def test_cache_halted_select(self, vector):
    # The cache is set to the minimum cache size, so run a SQL that produces enough
    # data to exceed the cache size and halt caching.
    self.client.set_configuration(vector.get_value('exec_option'))
    big_enough_query = "SELECT o_comment from tpch.orders"
    result1 = self.execute_query(big_enough_query)
    result2 = self.execute_query(big_enough_query)

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=1, num_skipped=0)
    bytes_written = getCounterValues(result1.runtime_profile, "TupleCacheBytesWritten")
    # This is running on a single node, so there should be a single location where
    # TupleCacheBytesWritten exceeds 0.
    assert len([v for v in bytes_written if v > 0]) == 1
    assertCounters(result2.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

  @CustomClusterTestSuite.with_args(
    start_args=CACHE_START_ARGS, cluster_size=1,
    impalad_args="--tuple_cache_ignore_query_options=true")
  def test_failpoints(self, vector, unique_database):
    fq_table = "{0}.failpoints".format(unique_database)
    # Scale 20 gets us enough rows to force multiple RowBatches (needed for the
    # the reader GetNext() cases).
    self.create_table(fq_table, scale=20)
    query = "SELECT * from {0}".format(fq_table)

    def execute_debug(query, action):
      exec_options = dict(vector.get_value('exec_option'))
      exec_options['debug_action'] = action
      return self.execute_query(query, exec_options)

    # Fail when writing cache entry. All of these are handled and will not fail the
    # query.
    # Case 1: fail during Open()
    result = execute_debug(query, "TUPLE_FILE_WRITER_OPEN:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

    # Case 2: fail during Write()
    result = execute_debug(query, "TUPLE_FILE_WRITER_WRITE:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Case 3: fail during Commit()
    result = execute_debug(query, "TUPLE_FILE_WRITER_COMMIT:FAIL@1.0")
    assert result.success
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Now, successfully add a cache entry
    result1 = self.execute_query(query, vector.get_value('exec_option'))
    assert result1.success
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)

    # Fail when reading a cache entry
    # Case 1: fail during Open()
    result = execute_debug(query, "TUPLE_FILE_READER_OPEN:FAIL@1.0")
    assert result.success
    # Do an unordered compare (the rows are unique)
    assert set(result.data) == set(result1.data)
    # Not a hit
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

    # Case 2: fail during the first GetNext() call
    result = execute_debug(query, "TUPLE_FILE_READER_FIRST_GETNEXT:FAIL@1.0")
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
      result = execute_debug(query, "TUPLE_FILE_READER_SECOND_GETNEXT:FAIL@1.0")
    except Exception:
      hit_error = True

    assert hit_error

  @CustomClusterTestSuite.with_args(
      start_args=CACHE_START_ARGS, cluster_size=1,
      impalad_args='--tuple_cache_exempt_query_options=max_errors,exec_time_limit_s')
  def test_custom_exempt_query_options(self, vector, unique_database):
    """Custom list of exempt query options share cache entry"""
    fq_table = "{0}.query_options".format(unique_database)
    self.create_table(fq_table)
    query = "SELECT * from {0}".format(fq_table)

    errors_10 = dict(vector.get_value('exec_option'))
    errors_10['max_errors'] = '10'
    exec_time_limit = dict(vector.get_value('exec_option'))
    exec_time_limit['exec_time_limit_s'] = '30'

    exempt1 = self.execute_query(query, query_options=errors_10)
    exempt2 = self.execute_query(query, query_options=exec_time_limit)
    exempt3 = self.execute_query(query, query_options=vector.get_value('exec_option'))
    assert exempt1.success
    assert exempt2.success
    assert exempt1.data == exempt2.data
    assert exempt1.data == exempt3.data
    assertCounters(exempt1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(exempt2.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)
    assertCounters(exempt3.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)


@CustomClusterTestSuite.with_args(start_args=CACHE_START_ARGS, cluster_size=1)
class TestTupleCacheSingle(TestTupleCacheBase):
  """Tests Impala with a single executor and mt_dop=1."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheSingle, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'mt_dop', 1)

  def test_create_and_select(self, vector, unique_database):
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.create_and_select".format(unique_database)
    self.create_table(fq_table)
    result1 = self.execute_query("SELECT * from {0}".format(fq_table))
    result2 = self.execute_query("SELECT * from {0}".format(fq_table))

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(result2.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)
    # Verify that the bytes written by the first profile are the same as the bytes
    # read by the second profile.
    bytes_written = getCounterValues(result1.runtime_profile, "TupleCacheBytesWritten")
    bytes_read = getCounterValues(result2.runtime_profile, "TupleCacheBytesRead")
    assert sorted(bytes_written) == sorted(bytes_read)

  def test_non_exempt_query_options(self, vector, unique_database):
    """Non-exempt query options result in different cache entries"""
    fq_table = "{0}.query_options".format(unique_database)
    self.create_table(fq_table)
    query = "SELECT * from {0}".format(fq_table)

    strict_true = dict(vector.get_value('exec_option'))
    strict_true['strict_mode'] = 'true'
    strict_false = dict(vector.get_value('exec_option'))
    strict_false['strict_mode'] = 'false'

    noexempt1 = self.execute_query(query, query_options=strict_false)
    noexempt2 = self.execute_query(query, query_options=strict_true)
    noexempt3 = self.execute_query(query, query_options=strict_false)
    noexempt4 = self.execute_query(query, query_options=strict_true)
    noexempt5 = self.execute_query(query, query_options=vector.get_value('exec_option'))

    assert noexempt1.success
    assert noexempt2.success
    assert noexempt3.success
    assert noexempt4.success
    assert noexempt5.success
    assert noexempt1.data == noexempt2.data
    assert noexempt1.data == noexempt3.data
    assert noexempt1.data == noexempt4.data
    assert noexempt1.data == noexempt5.data
    assertCounters(noexempt1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(noexempt2.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(noexempt3.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)
    assertCounters(noexempt4.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)
    assertCounters(noexempt5.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)

  def test_exempt_query_options(self, vector, unique_database):
    """Exempt query options share cache entry"""
    fq_table = "{0}.query_options".format(unique_database)
    self.create_table(fq_table)
    query = "SELECT * from {0}".format(fq_table)

    codegen_false = dict(vector.get_value('exec_option'))
    codegen_false['disable_codegen'] = 'true'
    codegen_true = dict(vector.get_value('exec_option'))
    codegen_true['disable_codegen'] = 'false'

    exempt1 = self.execute_query(query, query_options=codegen_true)
    exempt2 = self.execute_query(query, query_options=codegen_false)
    exempt3 = self.execute_query(query, query_options=vector.get_value('exec_option'))
    assert exempt1.success
    assert exempt2.success
    assert exempt1.data == exempt2.data
    assert exempt1.data == exempt3.data
    assertCounters(exempt1.runtime_profile, num_hits=0, num_halted=0, num_skipped=0)
    assertCounters(exempt2.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)
    assertCounters(exempt3.runtime_profile, num_hits=1, num_halted=0, num_skipped=0)

  def test_aggregate(self, vector, unique_database):
    """Simple aggregation can be cached"""
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.agg".format(unique_database)
    self.create_table(fq_table)

    result1 = self.execute_query("SELECT sum(age) FROM {0}".format(fq_table))
    result2 = self.execute_query("SELECT sum(age) FROM {0}".format(fq_table))

    assert result1.success
    assert result2.success
    assert result1.data == result2.data
    assertCounters(result1.runtime_profile, 0, 0, 0, num_matches=2)
    # Aggregate should hit, and scan node below it will miss.
    assertCounterOrder(result2.runtime_profile, NUM_HITS, [1, 0])
    assertCounter(result2.runtime_profile, NUM_HALTED, 0, num_matches=2)
    assertCounter(result2.runtime_profile, NUM_SKIPPED, 0, num_matches=2)
    # Verify that the bytes written by the first profile are the same as the bytes
    # read by the second profile.
    bytes_written = getCounterValues(result1.runtime_profile, "TupleCacheBytesWritten")
    bytes_read = getCounterValues(result2.runtime_profile, "TupleCacheBytesRead")
    assert len(bytes_written) == 2
    assert len(bytes_read) == 1
    assert bytes_written[0] == bytes_read[0]

  def test_aggregate_reuse(self, vector):
    """Cached aggregation can be re-used"""
    self.client.set_configuration(vector.get_value('exec_option'))

    result = self.execute_query("SELECT sum(int_col) FROM functional.alltypes")
    assert result.success
    assertCounters(result.runtime_profile, 0, 0, 0, num_matches=2)

    result_scan = self.execute_query("SELECT avg(int_col) FROM functional.alltypes")
    assert result_scan.success
    assertCounterOrder(result_scan.runtime_profile, NUM_HITS, [0, 1])

    result_agg = self.execute_query(
        "SELECT avg(a) FROM (SELECT sum(int_col) as a FROM functional.alltypes) b")
    assert result_agg.success
    assertCounterOrder(result_agg.runtime_profile, NUM_HITS, [1, 0])


@CustomClusterTestSuite.with_args(start_args=CACHE_START_ARGS)
class TestTupleCacheCluster(TestTupleCacheBase):
  """Tests Impala with 3 executors and mt_dop=1."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheCluster, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'mt_dop', 1)

  def test_runtime_filters(self, vector, unique_database):
    """
    This tests that adding files to a table results in different runtime filter keys.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.runtime_filters".format(unique_database)
    # A query containing multiple runtime filters
    # - scan of A receives runtime filters from B and C, so it depends on contents of B/C
    # - scan of B receives runtime filter from C, so it depends on contents of C
    query = "select straight_join a.id from functional.alltypes a, functional.alltypes" \
        " b, {0} c where a.id = b.id and a.id = c.age order by a.id".format(fq_table)
    query_a_id = 10
    query_b_id = 11
    query_c_id = 12

    # Create an empty table
    self.create_table(fq_table, scale=0)

    # Establish a baseline
    empty_result = self.execute_query(query)
    empty_cache_keys = get_cache_keys(empty_result.runtime_profile)
    # Tables a and b have multiple files, so they are distributed across all 3 nodes.
    # Table c has one file, so it has a single entry.
    assert len(empty_cache_keys) == 3
    assert len(empty_cache_keys[query_c_id]) == 1
    empty_c_compile_key, empty_c_finst_key = empty_cache_keys[query_c_id][0].split("_")
    assert empty_c_finst_key == "0"
    assert len(empty_result.data) == 0

    # Insert a row, which creates a file / scan range
    self.execute_query("INSERT INTO {0} VALUES ({1})".format(fq_table, table_value(0)))

    # Now, there is a scan range, so the fragment instance key should be non-zero.
    one_file_result = self.execute_query(query)
    one_cache_keys = get_cache_keys(one_file_result.runtime_profile)
    assert len(one_cache_keys) == 3
    assert len(empty_cache_keys[query_c_id]) == 1
    one_c_compile_key, one_c_finst_key = one_cache_keys[query_c_id][0].split("_")
    assert one_c_finst_key != "0"
    # This should be a cache miss
    assertCounters(one_file_result.runtime_profile, 0, 0, 0, 7)
    assert len(one_file_result.data) == 1

    # The new scan range did not change the compile-time key, but did change the runtime
    # filter keys.
    for id in [query_a_id, query_b_id]:
      assert len(empty_cache_keys[id]) == len(one_cache_keys[id])
      for empty, one in zip(empty_cache_keys[id], one_cache_keys[id]):
        assert empty != one
    assert empty_c_compile_key == one_c_compile_key

    # Insert another row, which creates a file / scan range
    self.execute_query("INSERT INTO {0} VALUES ({1})".format(fq_table, table_value(1)))

    # There is a second scan range, so the fragment instance key should change again
    two_files_result = self.execute_query(query)
    two_cache_keys = get_cache_keys(two_files_result.runtime_profile)
    assert len(two_cache_keys) == 3
    assert len(two_cache_keys[query_c_id]) == 2
    two_c1_compile_key, two_c1_finst_key = two_cache_keys[query_c_id][0].split("_")
    two_c2_compile_key, two_c2_finst_key = two_cache_keys[query_c_id][1].split("_")
    assert two_c1_finst_key != "0"
    assert two_c2_finst_key != "0"
    # There may be a cache hit for the prior "c" scan range (if scheduled to the same
    # instance), and the rest cache misses.
    assertCounter(two_files_result.runtime_profile, NUM_HITS, 0, num_matches=[7, 8])
    assertCounter(two_files_result.runtime_profile, NUM_HITS, 1, num_matches=[0, 1])
    assertCounter(two_files_result.runtime_profile, NUM_HALTED, 0, num_matches=8)
    assertCounter(two_files_result.runtime_profile, NUM_SKIPPED, 0, num_matches=8)
    assert len(two_files_result.data) == 2
    # Ordering can vary by environment. Ensure one matches and one differs.
    assert one_c_finst_key == two_c1_finst_key or one_c_finst_key == two_c2_finst_key
    assert one_c_finst_key != two_c1_finst_key or one_c_finst_key != two_c2_finst_key
    overlapping_rows = set(one_file_result.data).intersection(set(two_files_result.data))
    assert len(overlapping_rows) == 1

    # The new scan range did not change the compile-time key, but did change the runtime
    # filter keys.
    for id in [query_a_id, query_b_id]:
      assert len(empty_cache_keys[id]) == len(one_cache_keys[id])
      for empty, one in zip(empty_cache_keys[id], one_cache_keys[id]):
        assert empty != one
    assert one_c_compile_key == two_c1_compile_key
    assert one_c_compile_key == two_c2_compile_key

    # Invalidate metadata and rerun the last query. The keys should stay the same.
    self.execute_query("invalidate metadata")
    rerun_two_files_result = self.execute_query(query)
    # Verify that this is a cache hit
    assertCounters(rerun_two_files_result.runtime_profile, 1, 0, 0, num_matches=8)
    rerun_cache_keys = get_cache_keys(rerun_two_files_result.runtime_profile)
    assert rerun_cache_keys == two_cache_keys
    assert rerun_two_files_result.data == two_files_result.data

  def test_runtime_filter_reload(self, vector, unique_database):
    """
    This tests that reloading files to a table results in matching runtime filter keys.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.runtime_filter_genspec".format(unique_database)
    # Query where fq_table generates a runtime filter.
    query = "select straight_join a.id from functional.alltypes a, {0} b " \
        "where a.id = b.age order by a.id".format(fq_table)

    # Create a partitioned table with 3 partitions
    self.execute_query("CREATE EXTERNAL TABLE {0} (name STRING) "
                       "PARTITIONED BY (age INT)".format(fq_table))
    self.execute_query(
        "INSERT INTO {0} PARTITION(age=4) VALUES (\"Vanessa\")".format(fq_table))
    self.execute_query(
        "INSERT INTO {0} PARTITION(age=5) VALUES (\"Carl\")".format(fq_table))
    self.execute_query(
        "INSERT INTO {0} PARTITION(age=6) VALUES (\"Cleopatra\")".format(fq_table))

    # Prime the cache
    base_result = self.execute_query(query)
    base_cache_keys = get_cache_keys(base_result.runtime_profile)
    assert len(base_cache_keys) == 2

    # Drop and reload the table
    self.execute_query("DROP TABLE {0}".format(fq_table))
    self.execute_query("CREATE EXTERNAL TABLE {0} (name STRING, address STRING) "
                       "PARTITIONED BY (age INT)".format(fq_table))
    self.execute_query("ALTER TABLE {0} RECOVER PARTITIONS".format(fq_table))

    # Verify we reuse the cache
    reload_result = self.execute_query(query)
    reload_cache_keys = get_cache_keys(reload_result.runtime_profile)
    assert base_result.data == reload_result.data
    assert base_cache_keys == reload_cache_keys
    # Skips verifying cache hits as fragments may not be assigned to the same nodes.


@CustomClusterTestSuite.with_args(start_args=CACHE_START_ARGS, cluster_size=1)
class TestTupleCacheRuntimeKeysBasic(TestTupleCacheBase):
  """Simpler tests that run on a single node with mt_dop=0 or mt_dop=1."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheRuntimeKeysBasic, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'mt_dop', [0, 1])

  def test_scan_range_basics(self, vector, unique_database):
    """
    This tests that adding/removing files to a table results in different keys.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.scan_range_basics".format(unique_database)
    query = "SELECT * from {0}".format(fq_table)

    # Create an empty table
    self.create_table(fq_table, scale=0)

    # When there are no scan ranges, then fragment instance key is 0. This is
    # somewhat of a toy case and we probably want to avoid caching in this
    # case. Nonetheless, it is a good sanity check.
    empty_result = self.execute_query(query)
    cache_keys = get_cache_keys(empty_result.runtime_profile)
    assert len(cache_keys) == 1
    empty_table_compile_key, empty_table_finst_key = cache_keys[0].split("_")
    assert empty_table_finst_key == "0"
    assert len(empty_result.data) == 0
    assert_deterministic_scan(vector, empty_result.runtime_profile)

    # Insert a row, which creates a file / scan range
    self.execute_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(0)))

    # Now, there is a scan range, so the fragment instance key should be non-zero.
    one_file_result = self.execute_query(query)
    cache_keys = get_cache_keys(one_file_result.runtime_profile)
    assert len(cache_keys) == 1
    one_file_compile_key, one_file_finst_key = cache_keys[0].split("_")
    assert one_file_finst_key != "0"
    # This should be a cache miss
    assertCounters(one_file_result.runtime_profile, 0, 0, 0)
    assert len(one_file_result.data) == 1
    assert_deterministic_scan(vector, one_file_result.runtime_profile)

    # The new scan range did not change the compile-time key
    assert empty_table_compile_key == one_file_compile_key

    # Insert another row, which creates a file / scan range
    self.execute_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(1)))

    # There is a second scan range, so the fragment instance key should change again
    two_files_result = self.execute_query(query)
    cache_keys = get_cache_keys(two_files_result.runtime_profile)
    assert len(cache_keys) == 1
    two_files_compile_key, two_files_finst_key = cache_keys[0].split("_")
    assert two_files_finst_key != "0"
    assertCounters(two_files_result.runtime_profile, 0, 0, 0)
    assert len(two_files_result.data) == 2
    assert one_file_finst_key != two_files_finst_key
    overlapping_rows = set(one_file_result.data).intersection(set(two_files_result.data))
    assert len(overlapping_rows) == 1
    assert_deterministic_scan(vector, two_files_result.runtime_profile)

    # The new scan range did not change the compile-time key
    assert one_file_compile_key == two_files_compile_key

    # Invalidate metadata and rerun the last query. The keys should stay the same.
    self.execute_query("invalidate metadata")
    rerun_two_files_result = self.execute_query(query)
    # Verify that this is a cache hit
    assertCounters(rerun_two_files_result.runtime_profile, 1, 0, 0)
    cache_keys = get_cache_keys(rerun_two_files_result.runtime_profile)
    assert len(cache_keys) == 1
    rerun_two_files_compile_key, rerun_two_files_finst_key = cache_keys[0].split("_")
    assert rerun_two_files_finst_key == two_files_finst_key
    assert rerun_two_files_compile_key == two_files_compile_key
    assert rerun_two_files_result.data == two_files_result.data

  def test_scan_range_partitioned(self, vector):
    """
    This tests a basic partitioned case where the query is identical except that
    it operates on different partitions (and thus different scan ranges).
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    year2009_result = self.execute_query(
        "select * from functional.alltypes where year=2009")
    cache_keys = get_cache_keys(year2009_result.runtime_profile)
    assert len(cache_keys) == 1
    year2009_compile_key, year2009_finst_key = cache_keys[0].split("_")

    year2010_result = self.execute_query(
        "select * from functional.alltypes where year=2010")
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
class TestTupleCacheFullCluster(TestTupleCacheBase):
  """Test with 3 executors and a range of mt_dop values."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheFullCluster, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'mt_dop', [0, 1, 2])

  def test_scan_range_distributed(self, vector, unique_database):
    """
    This tests the distributed case where there are multiple fragment instances
    processing different scan ranges. Each fragment instance should have a
    distinct cache key. When adding a scan range, at least one fragment instance
    cache key should change.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    mt_dop = vector.get_value('exec_option')['mt_dop']
    fq_table = "{0}.scan_range_distributed".format(unique_database)
    query = "SELECT * from {0}".format(fq_table)

    entries_baseline = {
      impalad: self.get_tuple_cache_metric(impalad.service, "entries-in-use")
      for impalad in self.cluster.impalads}

    # Create a table with several files so that we always have enough work for multiple
    # fragment instances
    self.create_table(fq_table, scale=20)

    # We run a simple select. This is running with multiple impalads, so there are
    # always multiple fragment instances
    before_result = self.execute_query(query)
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
    # Verify the cache metrics for each impalad. Determine number of new cache entries,
    # which should be the same as the number of cache keys.
    for impalad in self.cluster.impalads:
      entries_in_use = self.get_tuple_cache_metric(impalad.service, "entries-in-use")
      assert entries_in_use - entries_baseline[impalad] == max(mt_dop, 1)
    assert_deterministic_scan(vector, before_result.runtime_profile)

    # Insert another row, which creates a file / scan range
    # This uses a very large seed for table_value() to get a unique row that isn't
    # already in the table.
    self.execute_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(1000000)))

    # Rerun the query with the extra scan range
    after_insert_result = self.execute_query(query)
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
      new_entries_in_use = self.get_tuple_cache_metric(impalad.service, "entries-in-use")
      new_entries_in_use -= entries_baseline[impalad]
      assert new_entries_in_use >= max(mt_dop, 1)
      assert new_entries_in_use <= (2 * max(mt_dop, 1))
      total_entries_in_use += new_entries_in_use
    assert total_entries_in_use >= len(all_cache_keys)
    assert_deterministic_scan(vector, after_insert_result.runtime_profile)

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


@CustomClusterTestSuite.with_args(start_args=CACHE_START_ARGS, cluster_size=1)
class TestTupleCacheMtdop(TestTupleCacheBase):
  """Test with single executor and mt_dop=0 or 2."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestTupleCacheMtdop, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'mt_dop', [0, 2])

  def test_tuple_cache_count_star(self, vector, unique_database):
    """
    This test is a regression test for IMPALA-13411 to see whether it hits
    the DCHECK.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.tuple_cache_count_star".format(unique_database)

    # Create a table.
    self.create_table(fq_table, scale=1)

    # Run twice and see if it hits the DCHECK.
    query = "select count(*) from {0}".format(fq_table)
    result1 = self.execute_query(query)
    result2 = self.execute_query(query)
    assert result1.success and result2.success

  def test_tuple_cache_key_with_stats(self, vector, unique_database):
    """
    This test verifies if compute stats affect the tuple cache key.
    """
    self.client.set_configuration(vector.get_value('exec_option'))
    fq_table = "{0}.tuple_cache_stats_test".format(unique_database)

    # Create a table.
    self.create_table(fq_table, scale=1)

    # Get the explain text for a simple query.
    query = "explain select * from {0}".format(fq_table)
    result1 = self.execute_query(query)

    # Insert rows to make the stats different.
    for i in range(10):
      self.execute_query("INSERT INTO {0} VALUES ({1})".format(
        fq_table, table_value(i)))

    # Run compute stats and get the explain text again for the same query.
    self.client.execute("COMPUTE STATS {0}".format(fq_table))
    result2 = self.execute_query(query)

    # Verify memory estimations are different, while the cache keys are identical.
    assert result1.success and result2.success
    mem_limit1, units1 = match_memory_estimate(result1.data)
    mem_limit1 = parse_mem_to_mb(mem_limit1, units1)
    mem_limit2, units2 = match_memory_estimate(result2.data)
    mem_limit2 = parse_mem_to_mb(mem_limit2, units2)
    assert mem_limit1 != mem_limit2
    cache_key1 = match_cache_key(result1.data)
    cache_key2 = match_cache_key(result2.data)
    assert cache_key1 is not None and cache_key1 == cache_key2
