#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates all aggregate functions across all datatypes
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite
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
    # Reduce execution time when exploration strategy is 'core'
    if cls.exploration_strategy() == 'core':
      if vector.get_value('exec_option')['batch_size'] != 0: return False

    data_type, agg_func = vector.get_value('data_type'), vector.get_value('agg_func')
    file_format = vector.get_value('table_format').file_format

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
    query = 'select %s(%s_col) from alltypesagg' % (agg_func, data_type)
    result = self.execute_scalar(query, vector.get_value('exec_option'),
                                 table_format=vector.get_value('table_format'))
    if 'int' in data_type:
      assert result_lut['%s-%s' % (agg_func, data_type)] == int(result)

    # AVG
    if vector.get_value('data_type') == 'timestamp' and\
       vector.get_value('agg_func') == 'avg':
      return
    query = 'select %s(DISTINCT(%s_col)) from alltypesagg' % (agg_func, data_type)
    result = self.execute_scalar(query, vector.get_value('exec_option'))
