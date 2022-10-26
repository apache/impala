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
from copy import copy
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf, SkipIfNotHdfsMinicluster
from tests.common.test_result_verifier import assert_codegen_cache_hit
from tests.util.filesystem_utils import get_fs_path


@SkipIf.not_hdfs
@SkipIfNotHdfsMinicluster.scheduling
class TestCodegenCache(CustomClusterTestSuite):
  """ This test enables the codegen cache and verfies that cache hit and miss counts
  in the runtime profile and metrics are as expected.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestCodegenCache, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache(self, vector):
    self._test_codegen_cache(vector,
            ("select * from (select * from functional.alltypes "
             + "limit 1000000) t1 where int_col > 10 limit 10"))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_int_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where int_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_tinyint_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where tinyint_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_bool_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where bool_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_bigint_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where bigint_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_float_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where float_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_double_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where double_col > 0")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_date_string_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where date_string_col != ''")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_string_col(self, vector):
    self._test_codegen_cache(vector,
      "select * from functional.alltypes where string_col != ''")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_poly_func_string_col(self, vector):
    self._test_codegen_cache(vector,
      ("select * from functional.alltypes where "
      + "CHAR_LENGTH(string_col) > 0"))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_poly_func_date_string_col(self, vector):
    self._test_codegen_cache(vector,
      ("select * from functional.alltypes where "
      + "CHAR_LENGTH(date_string_col) > 0"))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  # Test native uda is missed in the codegen cache, as it is disabled.
  def test_codegen_cache_uda_miss(self, vector):
    database = "test_codegen_cache_uda_miss"
    self._load_functions(database)
    self._test_codegen_cache(vector,
      "select test_count(int_col) from functional.alltypestiny", False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  # Test native udf is missed in the codegen cache, as it is disabled.
  def test_codegen_cache_udf_miss(self, vector):
    database = "test_codegen_cache_udf_miss"
    self._load_functions(database)
    self._test_codegen_cache(vector,
      "select sum(identity(bigint_col)) from functional.alltypes", False)

  def _check_metric_expect_init(self):
    # Verifies that the cache metrics are all zero.
    assert self.get_metric('impala.codegen-cache.entries-evicted') == 0
    assert self.get_metric('impala.codegen-cache.entries-in-use') == 0
    assert self.get_metric('impala.codegen-cache.entries-in-use-bytes') == 0
    assert self.get_metric('impala.codegen-cache.hits') == 0
    assert self.get_metric('impala.codegen-cache.misses') == 0

  def _test_codegen_cache(self, vector, sql, expect_hit=True, expect_num_frag=2):
    # Do not disable codegen.
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['exec_single_node_rows_threshold'] = 0
    self._check_metric_expect_init()
    result = self.execute_query(sql, exec_options)
    assert_codegen_cache_hit(result.runtime_profile, False)
    # expect_num_cache_miss_fragment is 1 iff expect_hit is False, and expect only
    # one fragment codegen cache missing for the case if expect_hit is False.
    expect_num_cache_miss_fragment = 1
    if expect_hit:
        expect_num_cache_miss_fragment = 0
    expect_num_cache_hit = expect_num_frag - expect_num_cache_miss_fragment

    # Verifies that the cache misses > 0, because the look up fails in an empty
    # brandnew cache, then a new entry should be stored successfully, so the in-use
    # entry number and bytes should be larger than 0.
    assert self.get_metric('impala.codegen-cache.entries-evicted') == 0
    assert self.get_metric('impala.codegen-cache.entries-in-use') == expect_num_cache_hit
    assert self.get_metric('impala.codegen-cache.entries-in-use-bytes') > 0
    assert self.get_metric('impala.codegen-cache.hits') == 0
    assert self.get_metric('impala.codegen-cache.misses') == expect_num_cache_hit

    result = self.execute_query(sql, exec_options)
    # Verify again, the expected cache hit should be reflected.
    if expect_hit:
      assert_codegen_cache_hit(result.runtime_profile, True)
    else:
      assert_codegen_cache_hit(result.runtime_profile, False)
    assert self.get_metric('impala.codegen-cache.entries-evicted') == 0
    assert self.get_metric('impala.codegen-cache.entries-in-use') == expect_num_cache_hit
    assert self.get_metric('impala.codegen-cache.entries-in-use-bytes') > 0
    assert self.get_metric('impala.codegen-cache.hits') == expect_num_cache_hit
    assert self.get_metric('impala.codegen-cache.misses') == expect_num_cache_hit

  def _load_functions(self, database):
    create_func_template = """
    use default;
    drop database if exists {database} CASCADE;
    create database {database};
    create aggregate function {database}.test_count(int) returns bigint
    location '{location_uda}' update_fn='CountUpdate';
    create function {database}.identity(boolean) returns boolean
    location '{location_udf}' symbol='Identity';
    create function {database}.identity(bigint) returns bigint
    location '{location_udf}' symbol='Identity';
    use {database};
    """
    location_uda = get_fs_path('/test-warehouse/libudasample.so')
    location_udf = get_fs_path('/test-warehouse/libTestUdfs.so')
    queries = create_func_template.format(database=database,
            location_uda=location_uda, location_udf=location_udf)
    queries = [q for q in queries.split(';') if q.strip()]
    for query in queries:
      if query.strip() == '': continue
      result = self.execute_query_expect_success(self.client, query)
      assert result is not None

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=3)
  def test_codegen_cache_udf_crash(self, vector):
      # The testcase would crash if we don't disable the native udf for codegen cache.
      database = "test_codegen_cache_udf_crash"
      self._load_functions(database)
      self.run_test_case('QueryTest/codegen-cache-udf', vector, use_db=database)
      # Even the udf is disabled and the queries are using the udf, there could be
      # other fragments stored to the codegen cache, so we check whether the codegen
      # cache is enabled to other cases.
      assert self.get_metric('impala.codegen-cache.entries-in-use') > 0
      assert self.get_metric('impala.codegen-cache.entries-in-use-bytes') > 0

      # Run multiple times, recreate the udfs, would crash if the udf is reused from
      # the codegen cache.
      for i in range(3):
        # Make the database different
        database = database + "diff"
        self._load_functions(database)
        self.run_test_case('QueryTest/codegen-cache-udf', vector, use_db=database)

  def _test_codegen_cache_timezone_crash_helper(self, database):
    create_db_template = """
    use default;
    drop database if exists {database} CASCADE;
    create database {database};
    create table {database}.alltimezones as select * from functional.alltimezones;
    use {database};
    """
    queries = create_db_template.format(database=database)
    queries = [q for q in queries.split(';') if q.strip()]
    query = "select timezone, utctime, localtime,\
            from_utc_timestamp(utctime,timezone) as\
            impalaresult from alltimezones where\
            localtime != from_utc_timestamp(utctime,timezone)"
    queries.append(query)
    for query in queries:
      if query.strip() == '': continue
      result = self.execute_query_expect_success(self.client, query)
      assert result is not None

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_codegen_cache_timezone_crash(self, vector):
      # The testcase tests whether it would crash using the broken builtin function
      # from_utc_timestamp from the codegen cache.
      database = "test_codegen_cache_timezone_crash"
      # Run multiple times, recreate the database each time. Except for the first run,
      # other runs should all hit the cache.
      # Expect won't crash.
      for i in range(5):
        # Make the database different
        self._test_codegen_cache_timezone_crash_helper(database + str(i))
        # During the table creation, there will be one fragment involved, for the
        # query we are going to test, will be two fragments, so totally three
        # fragments involved, should all be cached.
        assert self.get_metric('impala.codegen-cache.entries-in-use') == 3
        assert self.get_metric('impala.codegen-cache.entries-in-use-bytes') > 0
        assert self.get_metric('impala.codegen-cache.hits') == i * 3
        assert self.get_metric('impala.codegen-cache.misses') == 3
