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

TABLE_LAYOUT = 'name STRING, age INT, address STRING'


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


class TestTupleCache(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  CACHE_START_ARGS = "--tuple_cache_dir=/tmp --log_level=2"

  def cached_query(self, query):
    return self.execute_query(query, {"ENABLE_TUPLE_CACHE": "TRUE", "MT_DOP": "1"})

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
    for _ in range(scale):
      values = [table_value(i) for i in range(70)]
      self.cached_query("INSERT INTO {0} VALUES ({1})".format(
          fq_table, "), (".join(values)))

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
    assert result.data == result1.data
    # Not a hit
    assertCounters(result.runtime_profile, num_hits=0, num_halted=0, num_skipped=1)

    # Case 2: fail during the first GetNext() call
    result = self.cached_query_w_debugaction(query,
        "TUPLE_FILE_READER_FIRST_GETNEXT:FAIL@1.0")
    assert result.success
    assert result.data == result1.data
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
