# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates all aggregate functions across all datatypes
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfS3
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
