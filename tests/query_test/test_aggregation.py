# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates all aggregate functions across all datatypes
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfOldAggsJoins, SkipIfS3
from tests.util.test_file_parser import QueryTestSectionReader

agg_functions = ['sum', 'count', 'min', 'max', 'avg']

data_types = ['int', 'bool', 'double', 'bigint', 'tinyint',
              'smallint', 'float', 'timestamp']

result_lut = {
  # TODO: Add verification for other types
  'sum-tinyint': 45000, 'avg-tinyint': 5, 'count-tinyint': 9000,
      'min-tinyint': 1, 'max-tinyint': 9,
  'sum-smallint': 495000, 'avg-smallint': 50, 'count-smallint': 9900,
      'min-smallint': 1, 'max-smallint': 99,
  'sum-int': 4995000, 'avg-int': 500, 'count-int': 9990,
      'min-int': 1, 'max-int': 999,
  'sum-bigint': 49950000, 'avg-bigint': 5000, 'count-bigint': 9990,
      'min-bigint': 10, 'max-bigint': 9990,
}

class TestAggregation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregation, cls).add_test_dimensions()

    # Add two more dimensions
    cls.TestMatrix.add_dimension(TestDimension('agg_func', *agg_functions))
    cls.TestMatrix.add_dimension(TestDimension('data_type', *data_types))
    cls.TestMatrix.add_constraint(lambda v: cls.is_valid_vector(v))

  @classmethod
  def is_valid_vector(cls, vector):
    data_type, agg_func = vector.get_value('data_type'), vector.get_value('agg_func')
    file_format = vector.get_value('table_format').file_format
    if file_format not in ['parquet']: return False

    if cls.exploration_strategy() == 'core':
      # Reduce execution time when exploration strategy is 'core'
      if vector.get_value('exec_option')['batch_size'] != 0: return False

    # Avro doesn't have timestamp type
    if file_format == 'avro' and data_type == 'timestamp':
      return False
    elif agg_func not in ['min', 'max', 'count'] and data_type == 'bool':
      return False
    elif agg_func == 'sum' and data_type == 'timestamp':
      return False
    return True

  def test_aggregation(self, vector):
    data_type, agg_func = (vector.get_value('data_type'), vector.get_value('agg_func'))
    query = 'select %s(%s_col) from alltypesagg where day is not null' % (agg_func,
        data_type)
    result = self.execute_scalar(query, vector.get_value('exec_option'),
                                 table_format=vector.get_value('table_format'))
    if 'int' in data_type:
      assert result_lut['%s-%s' % (agg_func, data_type)] == int(result)

    # AVG
    if vector.get_value('data_type') == 'timestamp' and\
       vector.get_value('agg_func') == 'avg':
      return
    query = 'select %s(DISTINCT(%s_col)) from alltypesagg where day is not null' % (
        agg_func, data_type)
    result = self.execute_scalar(query, vector.get_value('exec_option'))

class TestAggregationQueries(ImpalaTestSuite):
  """Run the aggregation test suite, with codegen enabled and disabled, to exercise our
  non-codegen code"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregationQueries, cls).add_test_dimensions()

    cls.TestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))

    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfS3.insert
  @pytest.mark.execute_serially
  def test_non_codegen_tinyint_grouping(self, vector):
    # Regression for IMPALA-901. The test includes an INSERT statement, so can only be run
    # on INSERT-able formats - text only in this case, since the bug doesn't depend on the
    # file format.
    if vector.get_value('table_format').file_format == 'text' \
        and vector.get_value('table_format').compression_codec == 'none':
      self.run_test_case('QueryTest/aggregation_no_codegen_only', vector)

  def test_aggregation(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    self.run_test_case('QueryTest/aggregation', vector)

  def test_distinct(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("HBase returns columns in alphabetical order for select distinct *, "
                   "making the result verication to fail.")
    self.run_test_case('QueryTest/distinct', vector)

  def test_group_concat(self, vector):
    """group_concat distinct tests
       Required to run directly in python because the order in which results will be
       merged at the final, single-node aggregation step is non-deterministic (if the
       first phase is running on multiple nodes). Need to pull the result apart and
       compare the actual items)"""
    exec_option = vector.get_value('exec_option')
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

class TestTPCHAggregationQueries(ImpalaTestSuite):
  # Uses the TPC-H dataset in order to have larger aggregations.

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTPCHAggregationQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_tpch_aggregations(self, vector):
    self.run_test_case('tpch-aggregations', vector)

  @SkipIfOldAggsJoins.passthrough_preagg
  def test_tpch_passthrough_aggregations(self, vector):
    self.run_test_case('tpch-passthrough-aggregations', vector)
