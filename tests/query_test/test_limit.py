#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Validates limit on scan nodes
#
import logging
import pytest
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import *
from tests.util.test_file_parser import QueryTestSectionReader

class TestLimit(ImpalaTestSuite):
  # TODO: add larger limits when IMP-660 is fixed
  LIMIT_VALUES = [0, 1, 2, 3, 4, 5, 10] 
  QUERIES = ["select * from tpch.lineitem$TABLE limit %d"]

  # TODO: we should be able to run count(*) in setup rather than hardcoding the values
  # but I have no idea how to do this with this framework.
  TOTAL_ROWS = 6001215

  @classmethod
  def get_dataset(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimit, cls).add_test_dimensions()

    # Add two more dimensions
    cls.TestMatrix.add_dimension(
        TestDimension('limit_value', *TestLimit.LIMIT_VALUES))
    cls.TestMatrix.add_dimension(TestDimension('query', *TestLimit.QUERIES))

  # Disable test due to IMP-660
  @pytest.mark.xfail(run=False)
  def test_limit(self, vector):
    # We can't validate the rows that are returned since that is non-deterministic.
    # This is why this is a python test rather than a .test.
    limit = vector.get_value('limit_value')
    expected_num_rows = min(limit, TestLimit.TOTAL_ROWS)
    query_string = vector.get_value('query') % limit
    query_string = QueryTestSectionReader.replace_table_suffix(
        query_string, vector.get_value('table_format'))
    result = self.execute_query(query_string, vector.get_value('exec_option'))
    assert(len(result.data) == expected_num_rows)


# Validates the default order by limit query option functionality
class TestDefaultOrderByLimitValue(ImpalaTestSuite):
  # Interesting default limit values. TODO: What about value of -2?
  DEFAULT_ORDER_BY_LIMIT_VALUES = [None, -1, 0, 1, 10, 100]

  @classmethod
  def get_dataset(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDefaultOrderByLimitValue, cls).add_test_dimensions()
    # Not interested in exploring different file formats
    cls.TestMatrix.clear_dimension('table_format')

    cls.TestMatrix.add_dimension(TestDimension('default_order_by_limit_value',
        *TestDefaultOrderByLimitValue.DEFAULT_ORDER_BY_LIMIT_VALUES))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)

  def test_default_order_by_limit_values(self, vector):
    limit_value = vector.get_value('default_order_by_limit_value')
    exec_options = copy(vector.get_value('exec_option'))
    if limit_value is not None:
      exec_options['default_order_by_limit'] = limit_value

    # Unless the default order by limit is -1 or None (not specified) we expect SELECT
    # ORDER BY without any limit specified to work properly.
    no_limit_should_succeed = limit_value not in [None, -1]

    # Validate the default order by limit option kicks on when no limit is specified.
    query_no_limit = "select * from alltypes order by int_col"
    self.exec_query_validate(query_no_limit, exec_options,
        no_limit_should_succeed, limit_value)

    # Validate that user specified limits override the default limit value.
    query_with_limit = "select * from alltypes order by int_col limit 20"
    self.exec_query_validate(query_with_limit, exec_options, True, 20)

    query_no_orderby = "select * from alltypes limit 25"
    self.exec_query_validate(query_no_orderby, exec_options, True, 25)

  def exec_query_validate(self, query, exec_options, should_succeed, expected_rows):
    """Executes a query and validates the results"""
    try:
      result = self.execute_query(query, exec_options)
      assert should_succeed, 'Query was expected to fail'
      assert len(result.data) == expected_rows,\
          'Wrong number of rows returned %d' % len(result.data)
    except ImpalaBeeswaxException as e:
      assert not should_succeed, 'Query was not expected to fail: %s' % e
      assert 'ORDER BY without LIMIT currently not supported' in str(e)
