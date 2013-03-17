#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Tests query cancellation using the ImpalaService.Cancel API
#
import pytest
from time import sleep
from tests.common.test_vector import TestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.test_file_parser import QueryTestSectionReader

# Queries to execute. Use the TPC-H dataset because tables are large so queries take some
# time to execute.
QUERIES = ['select l_returnflag from lineitem',
           'select count(l_returnflag) from lineitem',
           'select * from lineitem limit 50',
           ]

# Time to sleep between issuing query and canceling
CANCEL_DELAY_IN_SECONDS = [0, 1, 2]

# Number of times to execute/cancel each query under test
NUM_CANCELATION_ITERATIONS = 1

class TestCancellation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellation, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('query', *QUERIES))
    cls.TestMatrix.add_dimension(TestDimension('cancel_delay', *CANCEL_DELAY_IN_SECONDS))
    cls.TestMatrix.add_constraint(lambda v: v.get_value('exec_option')['batch_size'] == 0)
    if cls.exploration_strategy() != 'core':
      NUM_CANCELATION_ITERATIONS = 3

  def test_basic_cancel(self, vector):
    query = vector.get_value('query')

    # Execute the query multiple times, each time canceling it
    for i in xrange(NUM_CANCELATION_ITERATIONS):
      handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                        table_format=vector.get_value('table_format'))
      sleep(vector.get_value('cancel_delay'))
      assert self.client.get_state(handle) != self.client.query_states['EXCEPTION']
      cancel_result = self.client.cancel_query(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result

      # TODO: Add some additional verification to check to make sure the query was
      # actually canceled

    # Executing the same query without canceling should work fine. Only do this if the
    # query has a limit or aggregation
    if 'count' in query or 'limit' in query:
      self.execute_query(query, vector.get_value('exec_option'))
