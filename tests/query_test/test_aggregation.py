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
    create_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.test_result_verifier import (
    assert_codegen_enabled,
    parse_column_types,
    parse_column_labels,
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension

# Test dimensions for TestAggregation.
AGG_FUNCTIONS = ['sum', 'count', 'min', 'max', 'avg', 'ndv']
DATA_TYPES = ['int', 'bool', 'double', 'bigint', 'tinyint',
              'smallint', 'float', 'timestamp', 'string']

# Lookup table for TestAggregation results.
result_lut = {
  'sum-tinyint': 45000, 'avg-tinyint': 5, 'count-tinyint': 9000,
      'min-tinyint': 1, 'max-tinyint': 9, 'ndv-tinyint': 9,
  'sum-smallint': 495000, 'avg-smallint': 50, 'count-smallint': 9900,
      'min-smallint': 1, 'max-smallint': 99, 'ndv-smallint': 99,
  'sum-int': 4995000, 'avg-int': 500, 'count-int': 9990,
      'min-int': 1, 'max-int': 999, 'ndv-int': 999,
  'sum-bigint': 49950000, 'avg-bigint': 5000, 'count-bigint': 9990,
      'min-bigint': 10, 'max-bigint' : 9990, 'ndv-bigint': 999,
  'sum-bool': 5000, 'count-bool': 10000, 'min-bool': 'false',
    'max-bool': 'true', 'avg-bool': 0.5, 'ndv-bool': 2,
  'sum-double': 50449500.0, 'count-double': 9990, 'min-double': 10.1,
      'max-double': 10089.9, 'avg-double': 5050.0, 'ndv-double': 999,
  'sum-float': 5494500.0, 'count-float': 9990, 'min-float': 1.10,
      'max-float': 1098.9, 'avg-float': 550.0, 'ndv-float': 999,
  'count-timestamp': 10000, 'min-timestamp': '2010-01-01 00:00:00',
      'max-timestamp': '2010-01-10 18:02:05.100000000',
      'avg-timestamp': '2010-01-05 20:47:11.705080000', 'ndv-timestamp': 10000,
  'count-string': 10000, 'min-string': '0', 'max-string': '999', 'ndv-string': 999,
  'sum-distinct-tinyint': 45, 'count-distinct-tinyint': 9, 'min-distinct-tinyint': 1,
      'max-distinct-tinyint': 9, 'avg-distinct-tinyint': 5, 'ndv-distinct-tinyint': 9,
  'sum-distinct-smallint': 4950, 'count-distinct-smallint': 99,
      'min-distinct-smallint': 1, 'max-distinct-smallint': 99,
      'avg-distinct-smallint': 50, 'ndv-distinct-smallint': 99,
  'sum-distinct-int': 499500, 'count-distinct-int': 999, 'min-distinct-int': 1,
      'max-distinct-int': 999, 'avg-distinct-int': 500, 'ndv-distinct-int': 999,
  'sum-distinct-bigint': 4995000, 'count-distinct-bigint': 999, 'min-distinct-bigint': 10,
      'max-distinct-bigint': 9990, 'avg-distinct-bigint': 5000,
      'ndv-distinct-bigint': 999,
  'sum-distinct-bool': 1, 'count-distinct-bool': 2, 'min-distinct-bool': 'false',
      'max-distinct-bool': 'true', 'avg-distinct-bool': 0.5, 'ndv-distinct-bool': 2,
  'sum-distinct-double': 5044950.0, 'count-distinct-double': 999,
      'min-distinct-double': 10.1, 'max-distinct-double': 10089.9,
      'avg-distinct-double': 5050.0, 'ndv-distinct-double': 999,
  'sum-distinct-float': 549450.0, 'count-distinct-float': 999, 'min-distinct-float': 1.1,
      'max-distinct-float': 1098.9, 'avg-distinct-float': 550.0,
      'ndv-distinct-float': 999,
  'count-distinct-timestamp': 10000, 'min-distinct-timestamp': '2010-01-01 00:00:00',
      'max-distinct-timestamp': '2010-01-10 18:02:05.100000000',
      'avg-distinct-timestamp': '2010-01-05 20:47:11.705080000',
      'ndv-distinct-timestamp': 10000,
  'count-distinct-string': 1000, 'min-distinct-string': '0',
      'max-distinct-string': '999', 'ndv-distinct-string': 999,
}

class TestAggregation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregation, cls).add_test_dimensions()

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

    query = 'select %s(%s_col) from alltypesagg where day is not null' % (agg_func,
        data_type)
    result = self.execute_query(query, exec_option,
       table_format=vector.get_value('table_format'))
    assert len(result.data) == 1
    self.verify_agg_result(agg_func, data_type, False, result.data[0]);

    if not disable_codegen:
      # Verify codegen was enabled for the preaggregation.
      # It is deliberately disabled for the merge aggregation.
      assert_codegen_enabled(result.runtime_profile, [1])

    query = 'select %s(DISTINCT(%s_col)) from alltypesagg where day is not null' % (
        agg_func, data_type)
    result = self.execute_query(query, vector.get_value('exec_option'))
    assert len(result.data) == 1
    self.verify_agg_result(agg_func, data_type, True, result.data[0]);

    if not disable_codegen:
      # Verify codegen was enabled for all stages of the aggregation.
      assert_codegen_enabled(result.runtime_profile, [1, 2, 4, 6])

  def verify_agg_result(self, agg_func, data_type, distinct, actual_string):
    key = '%s-%s%s' % (agg_func, 'distinct-' if distinct else '', data_type)

    if agg_func == 'ndv':
      # NDV is inherently approximate. Compare with some tolerance.
      err = abs(result_lut[key] - int(actual_string))
      rel_err =  err / float(result_lut[key])
      print key, result_lut[key], actual_string,abs(result_lut[key] - int(actual_string))
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
  def get_workload(self):
    return 'functional-query'

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

  def test_distinct(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("HBase returns columns in alphabetical order for select distinct *, "
                   "making the result verication to fail.")
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("IMPALA-4042: count(distinct NULL) fails on a view, needed for kudu")
    self.run_test_case('QueryTest/distinct', vector)

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
      assert(set(row[i].split(delimiter[i-1])) == set(['1', '2', '3', '4']))
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
    string_col = []
    string_col.append(set(['1','101','201','301','401','501','601','701','801','901']))
    string_col.append(set(['2','102','202','302','402','502','602','702','802','902']))
    string_col.append(set(['3','103','203','303','403','503','603','703','803','903']))
    string_col.append(set(['4','104','204','304','404','504','604','704','804','904']))
    string_col.append(set(['5','105','205','305','405','505','605','705','805','905']))
    string_col.append(set(['6','106','206','306','406','506','606','706','806','906']))
    string_col.append(set(['7','107','207','307','407','507','607','707','807','907']))
    string_col.append(set(['8','108','208','308','408','508','608','708','808','908']))
    string_col.append(set(['9','109','209','309','409','509','609','709','809','909']))
    string_col.append(set(['10','110','210','310','410','510','610','710','810','910']))
    assert(len(result.data) == 10)
    for i in range(10):
      row = (result.data)[i].split("\t")
      assert(len(row) == 2)
      assert(row[0] == str(i+1))
      assert(set(row[1].split("->")) == string_col[i])

    # Test group_concat distinct with merge node
    query = """select group_concat(distinct string_col, ' ') from alltypesagg
    where int_col < 10"""
    result = self.execute_query(query, exec_option, table_format=table_format)
    assert(set((result.data)[0].split(" ")) == set(['1','2','3','4','5','6','7','8','9']))
    if not disable_codegen:
      # Verify codegen was enabled for all four stages of the aggregation.
      assert_codegen_enabled(result.runtime_profile, [1, 2, 4, 6])

  def test_parquet_count_star_optimization(self, vector, unique_database):
    if (vector.get_value('table_format').file_format != 'text' or
        vector.get_value('table_format').compression_codec != 'none'):
      # No need to run this test on all file formats
      pytest.skip()
    self.run_test_case('QueryTest/parquet-stats-agg', vector, unique_database)
    vector.get_value('exec_option')['batch_size'] = 1
    self.run_test_case('QueryTest/parquet-stats-agg', vector, unique_database)

class TestWideAggregationQueries(ImpalaTestSuite):
  """Test that aggregations with many grouping columns work"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

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

    types = parse_column_types(result.schema)
    labels = parse_column_labels(result.schema)
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
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_tpch_aggregations(self, vector):
    self.run_test_case('tpch-aggregations', vector)

  def test_tpch_passthrough_aggregations(self, vector):
    self.run_test_case('tpch-passthrough-aggregations', vector)
