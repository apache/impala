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
  LIMIT_VALUES = [1, 2, 3, 4, 5, 10, 100, 5000]
  QUERIES = ["select * from lineitem limit %d"]

  # TODO: we should be able to run count(*) in setup rather than hardcoding the values
  # but I have no idea how to do this with this framework.
  TOTAL_ROWS = 6001215

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimit, cls).add_test_dimensions()

    # Add two more dimensions
    cls.TestMatrix.add_dimension(
        TestDimension('limit_value', *TestLimit.LIMIT_VALUES))
    cls.TestMatrix.add_dimension(TestDimension('query', *TestLimit.QUERIES))

    # Don't run with large limits and tiny batch sizes.  This generates excessive
    # network traffic and makes the machine run very slowly.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('limit_value') < 100 or v.get_value('exec_option')['batch_size'] == 0)
    # TPCH is not generated in hbase format.
    # TODO: Add test coverage for hbase.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != "hbase")

  def test_limit(self, vector):
    # We can't validate the rows that are returned since that is non-deterministic.
    # This is why this is a python test rather than a .test.
    limit = vector.get_value('limit_value')
    expected_num_rows = min(limit, TestLimit.TOTAL_ROWS)
    query_string = vector.get_value('query') % limit
    result = self.execute_query(query_string, vector.get_value('exec_option'),
                                table_format=vector.get_value('table_format'))
    assert(len(result.data) == expected_num_rows)

# Base class for TestLimit
class TestLimitBase(ImpalaTestSuite):
  def exec_query_validate(self, query, exec_options, should_succeed, expected_rows,
                          expected_error):
    """Executes a query and validates the results"""
    try:
      result = self.execute_query(query, exec_options)
      assert should_succeed, 'Query was expected to fail'
      assert len(result.data) == expected_rows,\
          'Wrong number of rows returned %d' % len(result.data)
    except ImpalaBeeswaxException as e:
      assert not should_succeed, 'Query was not expected to fail: %s' % e
      if (expected_error not in str(e)):
        print str(e)
      assert expected_error in str(e)

# Validates the default order by limit query option functionality
class TestDefaultOrderByLimitValue(TestLimitBase):
  # Interesting default limit values. TODO: What about value of -2?
  DEFAULT_ORDER_BY_LIMIT_VALUES = [None, -1, 0, 1, 10, 100]

  @classmethod
  def get_workload(self):
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

    expected_error = 'ORDER BY without LIMIT currently not supported'

    # Unless the default order by limit is -1 or None (not specified) we expect SELECT
    # ORDER BY without any limit specified to work properly.
    no_limit_should_succeed = limit_value not in [None, -1]

    # Validate the default order by limit option kicks on when no limit is specified.
    query_no_limit = "select * from functional.alltypes order by int_col"
    self.exec_query_validate(query_no_limit, exec_options,
        no_limit_should_succeed, limit_value, expected_error)

    # Validate that user specified limits override the default limit value.
    query_with_limit = "select * from functional.alltypes order by int_col limit 20"
    self.exec_query_validate(query_with_limit, exec_options, True, 20, expected_error)

    query_no_orderby = "select * from functional.alltypes limit 25"
    self.exec_query_validate(query_no_orderby, exec_options, True, 25, expected_error)

# Validates the abort_on_default_limit_exceeded query option functionality
class TestAbortOnDefaultLimitExceeded(TestLimitBase):
  # The test vector is list of tuple of:
  #   default_order_by_limit (number)
  #   abort_on_default_limit_exceeded (boolean)
  #   query_limit (string)
  #   expected rows, only applicable if it should succeed (number)
  TEST_VECTOR = [# This will hit the default order by limit and should fail
                 (1, True, None, 100),
                 # This will hit the default order by limit but won't fail
                 (1, False, None, 1),
                 # This will not use the default order by limit and won't fail
                 (1, True, 2, 2),
                 # This will use the default order by limit but won't go beyond the limit
                 (100, True, None, 100),
                 # This will use the default order by limit but will hit the limit
                 (99, True, None, 100)]

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAbortOnDefaultLimitExceeded, cls).add_test_dimensions()
    # Not interested in exploring different file formats
    cls.TestMatrix.clear_dimension('table_format')

    cls.TestMatrix.add_dimension(TestDimension('abort_on_default_limit_exceeded',
        *TestAbortOnDefaultLimitExceeded.TEST_VECTOR))

  def test_abort_on_default_limit_exceeded(self, vector):
    #TODO: do not check the error message because it might not have arrived the coord.
    #expected_error = 'DEFAULT_ORDER_BY_LIMIT has been exceeded.'
    expected_error = ''
    exec_options = copy(vector.get_value('exec_option'))
    test_param = vector.get_value('abort_on_default_limit_exceeded')
    default_order_by_limit, abort_on_default_limit_exceeded, user_limit, expected_rows = test_param
    exec_options['default_order_by_limit'] = default_order_by_limit
    exec_options['abort_on_default_limit_exceeded'] = abort_on_default_limit_exceeded
    query_limit = ''
    if (user_limit is not None):
        query_limit = " limit " + str(user_limit)
    query = "select * from functional.alltypessmall order by int_col" + query_limit

    # The query would fail if
    # 1. abort_on_default_limit_exceeded is set and
    # 2. default_order_by_limit is set and
    # 3. no user specified limit and
    # 4. the number of rows returned exceeded the default limit
    num_rows = 100
    if (user_limit is not None):
        num_rows = min(num_rows, user_limit)
    should_fail = (abort_on_default_limit_exceeded and \
        (default_order_by_limit is not None) and (default_order_by_limit > 0) and \
        user_limit is None and \
        num_rows > default_order_by_limit)

    self.exec_query_validate(query, exec_options, not should_fail, expected_rows,
                             expected_error)

