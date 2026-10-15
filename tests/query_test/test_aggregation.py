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

# Validates all aggregate functions across all datatypes
#
import pytest

from testdata.common import widetable
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    add_exec_option_dimension,
    ALL_CLUSTER_SIZES,
    create_exec_option_dimension,
    create_exec_option_dimension_from_dict,
    create_uncompressed_text_dimension)
from tests.common.test_result_verifier import (
    assert_codegen_enabled,
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension

# Test dimensions for TestAggregation.
AGG_FUNCTIONS = ['sum', 'count', 'min', 'max', 'avg', 'ndv']
DATA_TYPES = ['int', 'bool', 'double', 'bigint', 'tinyint',
              'smallint', 'float', 'timestamp', 'string']

# Lookup table for TestAggregation results.
result_lut = {
  # tinyint
  'sum-tinyint': 45000, 'avg-tinyint': 5, 'count-tinyint': 9000,
  'min-tinyint': 1, 'max-tinyint': 9, 'ndv-tinyint': 9,
  # smallint
  'sum-smallint': 495000, 'avg-smallint': 50, 'count-smallint': 9900,
  'min-smallint': 1, 'max-smallint': 99, 'ndv-smallint': 99,
  # int
  'sum-int': 4995000, 'avg-int': 500, 'count-int': 9990,
  'min-int': 1, 'max-int': 999, 'ndv-int': 999,
  # bigint
  'sum-bigint': 49950000, 'avg-bigint': 5000, 'count-bigint': 9990,
  'min-bigint': 10, 'max-bigint': 9990, 'ndv-bigint': 999,
  # bool
  'sum-bool': 5000, 'count-bool': 10000, 'min-bool': 'false',
  'max-bool': 'true', 'avg-bool': 0.5, 'ndv-bool': 2,
  # double
  'sum-double': 50449500.0, 'count-double': 9990, 'min-double': 10.1,
  'max-double': 10089.9, 'avg-double': 5050.0, 'ndv-double': 999,
  # float
  'sum-float': 5494500.0, 'count-float': 9990, 'min-float': 1.10,
  'max-float': 1098.9, 'avg-float': 550.0, 'ndv-float': 999,
  # timestamp
  'count-timestamp': 10000, 'min-timestamp': '2010-01-01 00:00:00',
  'max-timestamp': '2010-01-10 18:02:05.100000000',
  'avg-timestamp': '2010-01-05 20:47:11.705080000', 'ndv-timestamp': 10000,
  # string
  'count-string': 10000, 'min-string': '0', 'max-string': '999', 'ndv-string': 999,
  # distinct-tinyint
  'sum-distinct-tinyint': 45, 'count-distinct-tinyint': 9, 'min-distinct-tinyint': 1,
  'max-distinct-tinyint': 9, 'avg-distinct-tinyint': 5, 'ndv-distinct-tinyint': 9,
  # distinct-smallint
  'sum-distinct-smallint': 4950, 'count-distinct-smallint': 99,
  'min-distinct-smallint': 1, 'max-distinct-smallint': 99,
  'avg-distinct-smallint': 50, 'ndv-distinct-smallint': 99,
  # distinct-int
  'sum-distinct-int': 499500, 'count-distinct-int': 999, 'min-distinct-int': 1,
  'max-distinct-int': 999, 'avg-distinct-int': 500, 'ndv-distinct-int': 999,
  # distinct-bigint
  'sum-distinct-bigint': 4995000, 'count-distinct-bigint': 999, 'min-distinct-bigint': 10,
  'max-distinct-bigint': 9990, 'avg-distinct-bigint': 5000,
  'ndv-distinct-bigint': 999,
  # distinct-bool
  'sum-distinct-bool': 1, 'count-distinct-bool': 2, 'min-distinct-bool': 'false',
  'max-distinct-bool': 'true', 'avg-distinct-bool': 0.5, 'ndv-distinct-bool': 2,
  # distinct-double
  'sum-distinct-double': 5044950.0, 'count-distinct-double': 999,
  'min-distinct-double': 10.1, 'max-distinct-double': 10089.9,
  'avg-distinct-double': 5050.0, 'ndv-distinct-double': 999,
  # distinct-float
  'sum-distinct-float': 549450.0, 'count-distinct-float': 999, 'min-distinct-float': 1.1,
  'max-distinct-float': 1098.9, 'avg-distinct-float': 550.0,
  'ndv-distinct-float': 999,
  # distinct-timestamp
  'count-distinct-timestamp': 10000, 'min-distinct-timestamp': '2010-01-01 00:00:00',
  'max-distinct-timestamp': '2010-01-10 18:02:05.100000000',
  'avg-distinct-timestamp': '2010-01-05 20:47:11.705080000',
  'ndv-distinct-timestamp': 10000,
  # distinct-string
  'count-distinct-string': 1000, 'min-distinct-string': '0',
  'max-distinct-string': '999', 'ndv-distinct-string': 999,
}


class TestAggregation(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregation, cls).add_test_dimensions()
    # IMPALA-12383: Exercise with both num_nodes=0 (distributed plan) and num_nodes=1
    # (single node plan).
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(cluster_sizes=ALL_CLUSTER_SIZES)
    )

    # Add two more dimensions
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('agg_func', *AGG_FUNCTIONS))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('data_type', *DATA_TYPES))
    cls.ImpalaTestMatrix.add_constraint(lambda v: cls.is_valid_vector(v))

  @classmethod
  def is_valid_vector(cls, vector):
    data_type, agg_func = vector.get_value('data_type'), vector.get_value('agg_func')
    file_format = vector.get_value('table_format').file_format
    if file_format not in ['parquet']: return False

    if cls.exploration_strategy() == 'core':
      # Reduce execution time when exploration strategy is 'core'
      if vector.get_value('exec_option')['batch_size'] != 0: return False

    # Avro doesn't have timestamp type
    non_numeric = data_type in ['bool', 'string']
    if file_format == 'avro' and data_type == 'timestamp':
      return False
    elif non_numeric and agg_func not in ['min', 'max', 'count', 'ndv']:
      return False
    elif agg_func == 'sum' and data_type == 'timestamp':
      return False
    return True

  def test_aggregation(self, vector):
    exec_option = vector.get_value('exec_option')
    disable_codegen = exec_option['disable_codegen']
    data_type, agg_func = (vector.get_value('data_type'), vector.get_value('agg_func'))
    db_name = self.get_db_name_from_format(vector.get_table_format())

    query = 'select {0}({1}_col) from {2}.alltypesagg where day is not null'.format(
      agg_func, data_type, db_name)
    result = self.execute_query(query, exec_option)
    assert len(result.data) == 1
    self.verify_agg_result(agg_func, data_type, False, result.data[0])

    if not disable_codegen:
      # Verify codegen was enabled for the preaggregation.
      # It is deliberately disabled for the merge aggregation.
      assert_codegen_enabled(result.runtime_profile, [1])

    query = ('select {0}(DISTINCT({1}_col)) from {2}.alltypesagg '
             'where day is not null').format(agg_func, data_type, db_name)
    result = self.execute_query(query, exec_option)
    assert len(result.data) == 1
    self.verify_agg_result(agg_func, data_type, True, result.data[0])

    if not disable_codegen:
      # Verify codegen was enabled for all stages of the aggregation.
      assert_codegen_enabled(result.runtime_profile, [1, 2, 4, 6])

  def verify_agg_result(self, agg_func, data_type, distinct, actual_string):
    key = '%s-%s%s' % (agg_func, 'distinct-' if distinct else '', data_type)

    if agg_func == 'ndv':
      # NDV is inherently approximate. Compare with some tolerance.
      err = abs(result_lut[key] - int(actual_string))
      rel_err = err / float(result_lut[key])
      print(key, result_lut[key], actual_string,
            abs(result_lut[key] - int(actual_string)))
      assert err <= 1 or rel_err < 0.05
    elif data_type in ('float', 'double') and agg_func != 'count':
      # Compare with a margin of error.
      delta = 1e6 if data_type == 'double' else 1e3
      assert abs(result_lut[key] - float(actual_string)) < delta
    elif data_type == 'timestamp' and agg_func != 'count':
      # Strip off everything past 10s of microseconds.
      ignore_digits = 4
      assert result_lut[key][:-ignore_digits] == actual_string[:-ignore_digits]
    else:
      assert str(result_lut[key]) == actual_string


class TestAggregationQueries(ImpalaTestSuite):
  """Run the aggregation test suite, with codegen enabled and disabled, to exercise our
  non-codegen code"""
  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregationQueries, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(
          create_uncompressed_text_dimension(cls.get_workload()))

  def test_non_codegen_tinyint_grouping(self, vector, unique_database):
    # Regression for IMPALA-901. The test includes an INSERT statement, so can only be run
    # on INSERT-able formats - text only in this case, since the bug doesn't depend on the
    # file format.
    if vector.get_value('table_format').file_format == 'text' \
        and vector.get_value('table_format').compression_codec == 'none':
      self.client.execute("create table %s.imp_901 (col tinyint)" % unique_database)
      self.run_test_case('QueryTest/aggregation_no_codegen_only', vector,
          unique_database)

  def test_aggregation(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    self.run_test_case('QueryTest/aggregation', vector)

  def test_group_concat(self, vector):
    """group_concat distinct tests
       Required to run directly in python because the order in which results will be
       merged at the final, single-node aggregation step is non-deterministic (if the
       first phase is running on multiple nodes). Need to pull the result apart and
   compare the actual items)"""
    exec_option = vector.get_value('exec_option')
    disable_codegen = exec_option['disable_codegen']
    table_format = vector.get_value('table_format')
    # Test group_concat distinct with other aggregate function and groupings.
    # expected result is the row: 2010,'1, 2, 3, 4','1-2-3-4','2|3|1|4',40,4
    query = """select year, group_concat(distinct string_col),
    group_concat(distinct string_col, '-'), group_concat(distinct string_col, '|'),
    count(string_col), count(distinct string_col)
    from alltypesagg where int_col < 5 and year = 2010 group by year"""
    result = self.execute_query(query, exec_option, table_format=table_format)
    row = (result.data)[0].split("\t")
    assert(len(row) == 6)
    assert(row[0] == '2010')
    delimiter = [', ', '-', '|']
    for i in range(1, 4):
      assert(set(row[i].split(delimiter[i - 1])) == set(['1', '2', '3', '4']))
    assert(row[4] == '40')
    assert(row[5] == '4')
    if not disable_codegen:
      # Verify codegen was enabled for all three stages of the aggregation.
      assert_codegen_enabled(result.runtime_profile, [1, 2, 4])

    # Test group_concat distinct with arrow delimiter, with multiple rows
    query = """select day, group_concat(distinct string_col, "->")
    from (select * from alltypesagg where id % 100 = day order by id limit 99999) a
    group by day order by day"""
    result = self.execute_query(query, exec_option, table_format=table_format)
    string_col = [
      set(['1', '101', '201', '301', '401', '501', '601', '701', '801', '901']),
      set(['2', '102', '202', '302', '402', '502', '602', '702', '802', '902']),
      set(['3', '103', '203', '303', '403', '503', '603', '703', '803', '903']),
      set(['4', '104', '204', '304', '404', '504', '604', '704', '804', '904']),
      set(['5', '105', '205', '305', '405', '505', '605', '705', '805', '905']),
      set(['6', '106', '206', '306', '406', '506', '606', '706', '806', '906']),
      set(['7', '107', '207', '307', '407', '507', '607', '707', '807', '907']),
      set(['8', '108', '208', '308', '408', '508', '608', '708', '808', '908']),
      set(['9', '109', '209', '309', '409', '509', '609', '709', '809', '909']),
      set(['10', '110', '210', '310', '410', '510', '610', '710', '810', '910'])
    ]
    assert(len(result.data) == 10)
    for i in range(10):
      row = (result.data)[i].split("\t")
      assert(len(row) == 2)
      assert(row[0] == str(i + 1))
      assert(set(row[1].split("->")) == string_col[i])

    # Test group_concat distinct with merge node
    query = """select group_concat(distinct string_col, ' ') from alltypesagg
    where int_col < 10"""
    result = self.execute_query(query, exec_option, table_format=table_format)
    assert(set((result.data)[0].split(" ")) == set(
      ['1', '2', '3', '4', '5', '6', '7', '8', '9']))
    if not disable_codegen:
      # Verify codegen was enabled for all four stages of the aggregation.
      assert_codegen_enabled(result.runtime_profile, [1, 2, 4, 6])

  def test_ndv(self):
    """Test the version of NDV() that accepts a scale value argument against
    different column data types. The scale argument is an integer in range
    [1, 10]."""

    ndv_results = [
      [2, 9, 96, 988, 980, 1000, 944, 1030, 1020, 990, 1010, 957, 1030, 1027, 9845, 9898],
      [2, 9, 97, 957, 1008, 1016, 1005, 963, 994, 993, 1018, 1004, 963, 1014, 10210,
          10280],
      [2, 9, 98, 977, 1024, 1020, 975, 977, 1002, 991, 994, 1006, 977, 999, 10118, 9923],
      [2, 9, 99, 986, 1009, 1011, 994, 980, 997, 994, 1002, 997, 980, 988, 10148, 9987],
      [2, 9, 99, 995, 996, 1000, 998, 988, 995, 999, 997, 999, 988, 979, 9974, 9960],
      [2, 9, 99, 998, 1005, 999, 1003, 994, 1000, 993, 999, 998, 994, 992, 9899, 9941],
      [2, 9, 99, 993, 1001, 1007, 1000, 998, 1002, 997, 999, 998, 998, 999, 9923, 9931],
      [2, 9, 99, 994, 998, 1002, 1002, 999, 998, 999, 997, 1000, 999, 997, 9937, 9973],
      [2, 9, 99, 995, 997, 998, 1001, 999, 1001, 996, 997, 1000, 999, 998, 9989, 9981],
      [2, 9, 99, 998, 998, 997, 999, 998, 1000, 998, 1000, 998, 998, 1000, 10000, 10003]
    ]

    # For each possible integer value, genereate one query and test it out.
    for i in range(1, 11):
      ndv_stmt = """
        select ndv(bool_col, {0}), ndv(tinyint_col, {0}),
               ndv(smallint_col, {0}), ndv(int_col, {0}),
               ndv(bigint_col, {0}), ndv(float_col, {0}),
               ndv(double_col, {0}), ndv(string_col, {0}),
               ndv(cast(double_col as decimal(5, 0)), {0}),
               ndv(cast(double_col as decimal(10, 5)), {0}),
               ndv(cast(double_col as decimal(20, 10)), {0}),
               ndv(cast(double_col as decimal(38, 33)), {0}),
               ndv(cast(string_col as varchar(20)), {0}),
               ndv(cast(string_col as char(10)), {0}),
               ndv(timestamp_col, {0}), ndv(id, {0})
        from functional_parquet.alltypesagg""".format(i)
      ndv_result = self.execute_query(ndv_stmt)
      ndv_vals = ndv_result.data[0].split('\t')

      # Verify that each ndv() value (one per column for a total of 11) is identical
      # to the corresponding known value. Since NDV() invokes Hash64() hash function
      # with a fixed seed value, ndv() result is deterministic.
      for j in range(0, 11):
        assert(ndv_results[i - 1][j] == int(ndv_vals[j]))

  def test_grouping_sets(self, vector):
    """Tests for ROLLUP, CUBE and GROUPING SETS."""
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - HBase null handling is inconsistent")
    self.run_test_case('QueryTest/grouping-sets', vector)

  def test_aggregation_limit(self, vector):
    """Test that limits are honoured when enforced by aggregation node."""
    # 1-phase
    result = self.execute_query(
        "select distinct l_orderkey from tpch.lineitem limit 10",
        vector.get_value('exec_option'))
    assert len(result.data) == 10

    # 2-phase with transpose
    result = self.execute_query(
        "select count(distinct l_discount), group_concat(distinct l_linestatus), "
        "max(l_quantity) from tpch.lineitem group by l_tax, l_shipmode limit 10;",
        vector.get_value('exec_option'))
    assert len(result.data) == 10


class TestAggregationQueriesSingleFormat(ImpalaTestSuite):
  """Run the aggregation test suite similarly as TestAggregationQueries, but with
  stricter constraint. Each test in this class only run against one table format by
  setting uncompressed text dimension for all exploration strategy. However, they
  may not necessarily target uncompressed text table format. This also run with codegen
  enabled and disabled to exercise our non-codegen code, default batch_size,
  and batch_size=1."""
  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregationQueriesSingleFormat, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(
        batch_sizes=[0, 1], disable_codegen_options=[False, True]))

    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_parquet_count_star_optimization(self, vector, unique_database):
    self.run_test_case('QueryTest/parquet-stats-agg', vector, unique_database)

  def test_kudu_count_star_optimization(self, vector):
    self.run_test_case('QueryTest/kudu-stats-agg', vector)

  def test_orc_count_star_optimization(self, vector):
    self.run_test_case('QueryTest/orc-stats-agg', vector)

  def test_sampled_ndv(self):
    """The SAMPLED_NDV() function is inherently non-deterministic and cannot be
    reasonably made deterministic with existing options so we test it separately.
    The goal of this test is to ensure that SAMPLED_NDV() works on all data types
    and returns approximately sensible estimates. It is not the goal of this test
    to ensure tight error bounds on the NDV estimates. SAMPLED_NDV() is expected
    be inaccurate on small data sets like the ones we use in this test."""

    # NDV() is used a baseline to compare SAMPLED_NDV(). Both NDV() and SAMPLED_NDV()
    # are based on HyperLogLog so NDV() is roughly the best that SAMPLED_NDV() can do.
    # Expectations: All columns except 'id' and 'timestmap_col' have low NDVs and are
    # expected to be reasonably accurate with SAMPLED_NDV(). For the two high-NDV columns
    # SAMPLED_NDV() is expected to have high variance and error.
    ndv_stmt = """
        select ndv(bool_col), ndv(tinyint_col),
               ndv(smallint_col), ndv(int_col),
               ndv(bigint_col), ndv(float_col),
               ndv(double_col), ndv(string_col),
               ndv(cast(double_col as decimal(5, 0))),
               ndv(cast(double_col as decimal(10, 5))),
               ndv(cast(double_col as decimal(20, 10))),
               ndv(cast(double_col as decimal(38, 33))),
               ndv(cast(string_col as varchar(20))),
               ndv(cast(string_col as char(10))),
               ndv(timestamp_col), ndv(id)
        from functional_parquet.alltypesagg"""
    ndv_result = self.execute_query(ndv_stmt)
    ndv_vals = ndv_result.data[0].split('\t')

    for sample_perc in [0.1, 0.2, 0.5, 1.0]:
      sampled_ndv_stmt = """
          select sampled_ndv(bool_col, {0}), sampled_ndv(tinyint_col, {0}),
                 sampled_ndv(smallint_col, {0}), sampled_ndv(int_col, {0}),
                 sampled_ndv(bigint_col, {0}), sampled_ndv(float_col, {0}),
                 sampled_ndv(double_col, {0}), sampled_ndv(string_col, {0}),
                 sampled_ndv(cast(double_col as decimal(5, 0)), {0}),
                 sampled_ndv(cast(double_col as decimal(10, 5)), {0}),
                 sampled_ndv(cast(double_col as decimal(20, 10)), {0}),
                 sampled_ndv(cast(double_col as decimal(38, 33)), {0}),
                 sampled_ndv(cast(string_col as varchar(20)), {0}),
                 sampled_ndv(cast(string_col as char(10)), {0}),
                 sampled_ndv(timestamp_col, {0}), sampled_ndv(id, {0})
          from functional_parquet.alltypesagg""".format(sample_perc)
      sampled_ndv_result = self.execute_query(sampled_ndv_stmt)
      sampled_ndv_vals = sampled_ndv_result.data[0].split('\t')

      assert len(sampled_ndv_vals) == len(ndv_vals)
      # Low NDV columns. We expect a reasonaby accurate estimate regardless of the
      # sampling percent.
      for i in range(0, 14):
        self.appx_equals(int(sampled_ndv_vals[i]), int(ndv_vals[i]), 0.1)
      # High NDV columns. We expect the estimate to have high variance and error.
      # Since we give NDV() and SAMPLED_NDV() the same input data, i.e., we are not
      # actually sampling for SAMPLED_NDV(), we expect the result of SAMPLED_NDV() to
      # be bigger than NDV() proportional to the sampling percent.
      # For example, the column 'id' is a PK so we expect the result of SAMPLED_NDV()
      # with a sampling percent of 0.1 to be approximately 10x of the NDV().
      for i in range(14, 16):
        self.appx_equals(int(sampled_ndv_vals[i]) * sample_perc, int(ndv_vals[i]), 2.0)


class TestDistinctAggregation(ImpalaTestSuite):
  """Run the distinct aggregation test suite, with codegen and shuffle_distinct_exprs
  enabled and disabled."""
  @classmethod
  def add_test_dimensions(cls):
    super(TestDistinctAggregation, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension_from_dict({
        'disable_codegen': [False, True],
        'shuffle_distinct_exprs': [False, True]
      }))

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')

  def test_distinct(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("HBase returns columns in alphabetical order for select distinct *, "
                   "making the result verication to fail.")
    self.run_test_case('QueryTest/distinct', vector)

  def test_multiple_distinct(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("HBase returns columns in alphabetical order for select distinct *, "
                   "making the result verication to fail.")
    self.run_test_case('QueryTest/multiple-distinct-aggs', vector)


class TestWideAggregationQueries(ImpalaTestSuite):
  """Test that aggregations with many grouping columns work"""
  @classmethod
  def add_test_dimensions(cls):
    super(TestWideAggregationQueries, cls).add_test_dimensions()

    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))

    # File format doesn't matter for this test.
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_many_grouping_columns(self, vector):
    """Test that an aggregate with many grouping columns works"""
    table_format = vector.get_value('table_format')
    exec_option = vector.get_value('exec_option')
    query = "select distinct * from widetable_1000_cols"

    # Ensure codegen is enabled.
    result = self.execute_query(query, exec_option, table_format=table_format)

    # All rows should be distinct.
    expected_result = widetable.get_data(1000, 10, quote_strings=True)

    types = result.column_types
    labels = result.column_labels
    expected = QueryTestResult(expected_result, types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels,
        order_matters=False)
    assert expected == actual


class TestTPCHAggregationQueries(ImpalaTestSuite):
  # Uses the TPC-H dataset in order to have larger aggregations.

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTPCHAggregationQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_tpch_aggregations(self, vector):
    self.run_test_case('tpch-aggregations', vector)

  def test_tpch_passthrough_aggregations(self, vector):
    self.run_test_case('tpch-passthrough-aggregations', vector)

  def test_tpch_stress(self, vector):
    self.run_test_case('tpch-stress-aggregations', vector)

  def test_min_multiple_distinct(self, vector):
    self.run_test_case('min-multiple-distinct-aggs', vector)


class TestGroupingSetsChainedPreagg(ImpalaTestSuite):
  """Tests CHAIN_GROUPING_SETS_PREAGG. TPC-H is large enough to pass through rows."""

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestGroupingSetsChainedPreagg, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')
    add_exec_option_dimension(cls, 'chain_grouping_sets_preagg', ['true', 'false'])

  def test_grouping_sets_results(self, vector):
    """The results must be the same with and without chaining."""
    # The .test file sets these options in some queries.
    vector.unset_exec_option('batch_size')
    vector.unset_exec_option('num_nodes')
    self.run_test_case('grouping-sets-chained-preagg', vector)

  def test_grouping_sets_profile(self, vector):
    """The queries set the option themselves, so only run with the default."""
    if vector.get_exec_option('chain_grouping_sets_preagg') != 'true':
      pytest.skip("Only runs with the default value of the query option")
    vector.unset_exec_option('chain_grouping_sets_preagg')
    vector.unset_exec_option('num_nodes')
    self.run_test_case('grouping-sets-chained-preagg-profile', vector)


LINEITEM = "tpch_parquet.lineitem"


class GroupingSetsQuery(object):
  """A grouping sets query and a reference query with one plain GROUP BY per grouping
  set. 'keys' are (expr, alias) pairs. 'sets' are the key aliases of each grouping set,
  or None for a ROLLUP. 'aggs' are aggregate exprs, ('grouping', alias) or
  ('grouping_id', aliases or None). 'having' is the HAVING clause and the same condition
  over the reference columns, which include 'gid_' = grouping_id(). Like Impala, the
  reference returns a repeated grouping set once and no row for the empty grouping set
  over an empty input."""

  def __init__(self, keys, aggs, from_clause, sets=None, having=None):
    self.keys = keys
    self.aggs = aggs
    self.from_clause = from_clause
    self.sets = sets
    self.having = having
    self.exprs = dict((alias, expr) for expr, alias in keys)

  def columns(self):
    return [alias for _, alias in self.keys] + \
        ["a{0}".format(i) for i in range(len(self.aggs))]

  def grouping_sets(self):
    if self.sets is not None: return self.sets
    aliases = [alias for _, alias in self.keys]
    return [aliases[:n] for n in range(len(aliases), -1, -1)]

  def _grouping_args(self, agg):
    fn, args = agg
    if args is None: return [alias for _, alias in self.keys]
    return [args] if fn == 'grouping' else args

  def sql(self):
    items = ["{0} {1}".format(expr, alias) for expr, alias in self.keys]
    for i, agg in enumerate(self.aggs):
      if isinstance(agg, tuple):
        args = [] if agg[1] is None else self._grouping_args(agg)
        agg = "{0}({1})".format(agg[0], ", ".join(self.exprs[a] for a in args))
      items.append("{0} a{1}".format(agg, i))
    if self.sets is None:
      group_by = "rollup({0})".format(", ".join(expr for expr, _ in self.keys))
    else:
      group_by = "grouping sets({0})".format(", ".join(
          "({0})".format(", ".join(self.exprs[a] for a in s)) for s in self.sets))
    sql = "select {0} from {1} group by {2}".format(
        ", ".join(items), self.from_clause, group_by)
    if self.having: sql += " having " + self.having[0]
    return sql

  def reference_sql(self):
    branches = []
    grouping_sets = []
    for grouping_set in self.grouping_sets():
      if grouping_set not in grouping_sets: grouping_sets.append(grouping_set)
    for grouping_set in grouping_sets:
      def grouping_id(args):
        # The first arg is the most significant bit, 1 if the key isn't in the set.
        return sum(1 << (len(args) - 1 - i)
            for i, a in enumerate(args) if a not in grouping_set)
      items = ["{0} {1}".format(expr if alias in grouping_set else "null", alias)
          for expr, alias in self.keys]
      for i, agg in enumerate(self.aggs):
        if isinstance(agg, tuple):
          agg = str(grouping_id(self._grouping_args(agg)))
        else:
          for expr, alias in self.keys:
            agg = agg.replace("grouping({0})".format(expr), str(grouping_id([alias])))
        items.append("{0} a{1}".format(agg, i))
      items.append("{0} gid_".format(grouping_id([a for _, a in self.keys])))
      branch = "select {0} from {1}".format(", ".join(items), self.from_clause)
      if grouping_set:
        branch += " group by " + ", ".join(self.exprs[a] for a in grouping_set)
      else:
        branch += " having count(*) > 0"
      branches.append(branch)
    sql = "select {0} from ({1}) r".format(
        ", ".join(self.columns()), " union all ".join(branches))
    if self.having: sql += " where " + self.having[1]
    return sql


def lineitem(max_orderkey):
  return "{0} where l_orderkey < {1}".format(LINEITEM, max_orderkey)


# (name, queries, chained, checksum). 'chained' is whether the plan chains. 'checksum'
# compares a checksum instead of all rows.
CHAINED_PREAGG_QUERIES = [
  # Aggregate functions with fixed-size, variable-length and serialized intermediates,
  # and functions over only NULLs.
  ("agg_fns", [GroupingSetsQuery(
      [("l_returnflag", "k1"), ("l_linestatus", "k2"), ("l_shipmode", "k3")],
      ["count(*)", "count(nullif(l_shipmode, 'AIR'))", "sum(l_quantity)",
       "sum(l_extendedprice * (1 - l_discount))", "avg(l_discount)", "min(l_comment)",
       "max(l_comment)", "min(cast(l_shipdate as timestamp))",
       "max(cast(l_receiptdate as date))", "min(l_returnflag = 'R')",
       "max(cast(l_shipinstruct as char(6)))", "min(cast(l_comment as varchar(8)))",
       "ndv(l_partkey)",
       "length(group_concat(if(l_orderkey % 100 = 0, l_shipinstruct, null)))",
       "round(variance(l_quantity), 4)", "round(stddev_pop(l_discount), 6)",
       "sum(cast(null as int))", "min(cast(null as string))", ("grouping_id", None)],
      lineitem(1000000))], True, False),
  # Unique keys in the first two classes, so that they pass through most rows.
  ("unique_keys", [GroupingSetsQuery(
      [("l_orderkey", "k1"), ("l_linenumber", "k2"), ("l_suppkey", "k3")],
      ["count(*)", "sum(l_quantity)", "min(l_extendedprice)", "max(l_discount)",
       ("grouping_id", None)],
      lineitem(600000))], True, True),
  # The first class reduces the input less than the second one.
  ("mid_card", [GroupingSetsQuery(
      [("l_partkey", "k1"), ("l_suppkey", "k2")],
      ["count(*)", "sum(l_quantity)", "avg(l_extendedprice)", "max(l_tax)",
       ("grouping", "k2")],
      lineitem(3000000))], True, True),
  # Unique string keys.
  ("string_keys", [GroupingSetsQuery(
      [("l_comment", "k1"), ("l_shipmode", "k2")],
      ["count(*)", "sum(l_quantity)"], lineitem(600000))], True, True),
  # Keys with NULLs and empty strings, with grouping() and grouping_id().
  ("nulls", [GroupingSetsQuery(
      [("a", "k1"), ("b", "k2"), ("c", "k3")],
      ["count(*)", "sum(q)", ("grouping", "k1"), ("grouping", "k3"),
       ("grouping_id", ["k1", "k2"]), ("grouping_id", ["k3", "k1"]),
       ("grouping_id", None)],
      "(select nullif(l_shipmode, 'AIR') a, "
      "if(l_shipinstruct = 'NONE', '', l_shipinstruct) b, nullif(l_returnflag, 'N') c, "
      "l_quantity q from {0}) t".format(lineitem(1000000)))], True, False),
  # Seven classes.
  ("many_classes", [GroupingSetsQuery(
      [("l_returnflag", "k1"), ("l_linestatus", "k2"), ("l_shipmode", "k3"),
       ("l_shipinstruct", "k4"), ("l_linenumber", "k5"), ("year(l_shipdate)", "k6")],
      ["count(*)", "sum(l_quantity)", "max(l_comment)", ("grouping_id", None)],
      lineitem(1000000))], True, False),
  # Grouping keys of many types.
  ("key_types", [GroupingSetsQuery(
      [("cast(l_shipdate as timestamp)", "k1"), ("cast(l_commitdate as date)", "k2"),
       ("l_discount", "k3"), ("cast(l_tax as double)", "k4"),
       ("cast(l_linenumber as tinyint)", "k5"), ("l_returnflag = 'R'", "k6"),
       ("cast(l_shipmode as char(4))", "k7"),
       ("cast(l_shipinstruct as varchar(3))", "k8")],
      ["count(*)", "sum(l_quantity)", ("grouping_id", None)],
      lineitem(300000))], True, True),
  # Aggregate exprs and exprs over the grouping keys and grouping().
  ("exprs", [GroupingSetsQuery(
      [("l_suppkey % 100", "k1"), ("concat(l_shipmode, '-', l_returnflag)", "k2")],
      ["sum(l_quantity) / count(*)", "max(l_tax) - min(l_tax)",
       "count(*) * 2 + grouping(l_suppkey % 100)",
       "if(grouping(concat(l_shipmode, '-', l_returnflag)) = 1, 'all', 'one')"],
      lineitem(3000000))], True, True),
  # Nested grouping sets that aren't a rollup.
  ("nested_sets", [GroupingSetsQuery(
      [("l_partkey", "k1"), ("l_suppkey", "k2"), ("l_shipmode", "k3")],
      ["count(*)", "sum(l_quantity)", ("grouping_id", ["k1", "k2", "k3"])],
      lineitem(1000000), sets=[["k1", "k2", "k3"], ["k1", "k3"], ["k3"], []])],
   True, True),
  # The same grouping set twice, which returns its rows once. The repeated set is
  # dropped, so the rest are chained.
  ("duplicate_sets", [GroupingSetsQuery(
      [("l_partkey", "k1"), ("l_shipmode", "k2")],
      ["count(*)", "sum(l_quantity)", ("grouping_id", ["k1", "k2"])],
      lineitem(1000000), sets=[["k1", "k2"], ["k1", "k2"], ["k1"], []])],
   True, True),
  # Nested grouping sets from the smallest to the largest one, which aren't chained.
  ("ascending_sets", [GroupingSetsQuery(
      [("l_partkey", "k1"), ("l_shipmode", "k2")],
      ["count(*)", "sum(l_quantity)", ("grouping_id", ["k1", "k2"])],
      lineitem(1000000), sets=[[], ["k1"], ["k1", "k2"]])], False, True),
  # Grouping sets that aren't nested, which aren't chained.
  ("not_nested", [GroupingSetsQuery(
      [("l_returnflag", "k1"), ("l_shipmode", "k2"), ("l_linestatus", "k3")],
      ["count(*)", "sum(l_quantity)", ("grouping_id", ["k1", "k2", "k3"])],
      lineitem(1000000), sets=[["k1", "k2"], ["k3"], ["k1", "k3"]])], False, False),
  # Rollup over a join.
  ("join", [GroupingSetsQuery(
      [("o_custkey", "k1"), ("o_orderpriority", "k2"), ("l_shipmode", "k3")],
      ["count(*)", "sum(l_extendedprice)", ("grouping_id", None)],
      "{0} join tpch_parquet.orders on l_orderkey = o_orderkey "
      "where l_orderkey < 600000".format(LINEITEM))], True, True),
  # A union of two rollups with HAVING.
  ("union_having", [
    GroupingSetsQuery([("l_returnflag", "k1"), ("l_shipmode", "k2")], ["count(*)"],
        lineitem(1000000), having=("count(*) > 30000", "a0 > 30000")),
    GroupingSetsQuery([("o_orderstatus", "k1"), ("o_orderpriority", "k2")],
        ["count(*)"], "tpch_parquet.orders",
        having=("grouping_id() != 1", "gid_ != 1"))], True, False),
  # Empty input.
  ("empty", [GroupingSetsQuery(
      [("l_returnflag", "k1"), ("l_linestatus", "k2")],
      ["count(*)", "sum(l_quantity)", "min(l_comment)", ("grouping_id", None)],
      lineitem(0))], True, False),
]

# 'name=value' pairs separated by ';'. Denied reservations make the preaggregations pass
# rows through and the merge aggregations spill.
DENY_RESERVATION = "debug_action=-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@{0}"
CHAINED_PREAGG_OPTIONS_CORE = ["", DENY_RESERVATION.format("0.5")]
CHAINED_PREAGG_OPTIONS_EXHAUSTIVE = CHAINED_PREAGG_OPTIONS_CORE + [
  DENY_RESERVATION.format("1.0"), "batch_size=16", "mt_dop=2",
  "preagg_bytes_limit=1m"]


class TestGroupingSetsChainedPreaggResults(ImpalaTestSuite):
  """Compares grouping sets results with and without CHAIN_GROUPING_SETS_PREAGG to
  equivalent queries without grouping sets."""

  # The results of the reference queries by test case name.
  reference_results = {}

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestGroupingSetsChainedPreaggResults, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')
    options = CHAINED_PREAGG_OPTIONS_CORE if cls.exploration_strategy() == 'core' \
        else CHAINED_PREAGG_OPTIONS_EXHAUSTIVE
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('chained_preagg_options', *options))

  def _run(self, query, query_options):
    result = self.execute_query(query, query_options)
    assert result.success
    return sorted(result.data), result.runtime_profile

  @staticmethod
  def _checksum(query, columns):
    """Returns the row count and an order-independent hash sum of 'query'."""
    row = ", ".join("coalesce(cast({0} as string), 'NULL')".format(c) for c in columns)
    return ("select count(*), sum(abs(fnv_hash(concat_ws('|', {0}))) % 1000000007) "
            "from ({1}) v".format(row, query))

  @pytest.mark.parametrize("name,queries,chained,checksum", CHAINED_PREAGG_QUERIES,
      ids=[q[0] for q in CHAINED_PREAGG_QUERIES])
  def test_results(self, vector, name, queries, chained, checksum):
    query = " union all ".join(q.sql() for q in queries)
    reference = " union all ".join(q.reference_sql() for q in queries)
    if checksum:
      query = self._checksum(query, queries[0].columns())
      reference = self._checksum(reference, queries[0].columns())
    if name not in self.reference_results:
      # The reference result doesn't depend on the options.
      self.reference_results[name] = self._run(reference, {})[0]
    expected = self.reference_results[name]

    query_options = dict(vector.get_exec_option_dict())
    for option in vector.get_value('chained_preagg_options').split(';'):
      if option:
        option_name, value = option.split('=', 1)
        query_options[option_name] = value
    for chain in ['true', 'false']:
      query_options['chain_grouping_sets_preagg'] = chain
      rows, profile = self._run(query, query_options)
      assert ("Chained Aggregators" in profile) == (chain == 'true' and chained), \
          "chain_grouping_sets_preagg={0}".format(chain)
      assert rows == expected, "chain_grouping_sets_preagg={0}".format(chain)
