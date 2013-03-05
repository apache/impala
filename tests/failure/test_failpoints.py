#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Injects failures  at specific locations in each of the plan nodes. Currently supports
# two types of failures - cancellation of the query and a failure test hook.
#
import pytest
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite, ALL_NODES_ONLY
from tests.common.test_vector import TestDimension
from tests.common.test_dimensions import create_exec_option_dimension
from tests.util.test_file_parser import QueryTestSectionReader
from time import sleep

FAILPOINT_ACTION = ['FAIL', 'CANCEL']
FAILPOINT_LOCATION = ['PREPARE', 'OPEN', 'GETNEXT', 'CLOSE']

# The goal of this query is to use all of the node types.
# TODO: This query could be simplified a bit...
QUERY = """
select a.int_col, count(b.int_col) int_sum from functional.hbasealltypesagg a
join
  (select * from alltypes
   where year=2009 and month=1 order by int_col limit 2500
   union all
   select * from alltypes
   where year=2009 and month=2 limit 3000) b
on (a.int_col = b.int_col)
group by a.int_col
order by int_sum
limit 200
"""

# This provides a map of where the node type to node id(s) for the test query. The node
# ids were found by examining the output query plan.
# TODO: Once EXPLAIN output is easier to parse, this map should be built dynamically.
NODE_ID_MAP = {'SORT_NODE': [3, 7], 'EXCHANGE_NODE': [8, 9, 12, 13],
    'HASH_JOIN_NODE': [5], 'HDFS_SCAN_NODE': [2, 4], 'MERGE_NODE': [10, 11],
    'AGGREGATION_NODE': [6, 14], 'HBASE_SCAN_NODE': [0]}

class TestFailpoints(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestFailpoints, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('location', *FAILPOINT_LOCATION))
    cls.TestMatrix.add_dimension(
        TestDimension('target_node', *(NODE_ID_MAP.items())))
    cls.TestMatrix.add_dimension(
        TestDimension('action', *FAILPOINT_ACTION))
    cls.TestMatrix.add_dimension(create_exec_option_dimension([0], [False], [0]))

    # This is an invalid test case. It causes Impala to crash, this will never happen.
    # For more info see IMPALA-55
    cls.TestMatrix.add_constraint(lambda v: not (\
        v.get_value('action') == 'FAIL' and\
        v.get_value('location') in ['CLOSE'] and\
        v.get_value('target_node')[0] == 'HASH_JOIN_NODE'))

  def test_failpoints(self, vector):
    query = QUERY
    node_type, node_ids = vector.get_value('target_node')
    action = vector.get_value('action')
    location = vector.get_value('location')

    # TODO: These vectors fail due to product bugs. Once the bugs are fixed the tests
    # can re reenabled.
    if action == 'CANCEL' and location in ['PREPARE', 'CLOSE']:
      pytest.xfail(reason='IMPALA-56 - Hangs on async query execution')
    elif node_type == 'HDFS_SCAN_NODE' and action == 'FAIL' and location == 'OPEN':
      pytest.xfail(reason='IMPALA-54 - Fails DCHECK')


    for node_id in node_ids:
      debug_action = '%d:%s:%s' % (node_id, location,
                                   'WAIT' if action == 'CANCEL' else 'FAIL')
      print 'Current dubug action string: %s' % debug_action
      vector.get_value('exec_option')['debug_action'] = debug_action

      if action == 'CANCEL':
        self.__execute_cancel_action(query, vector)
      elif action == 'FAIL':
        self.__execute_fail_action(query, vector)
      else:
        assert 0, 'Unknown action: %s' % action

    # We should be able to execute the same query successfully when no failures are
    # injected.
    del vector.get_value('exec_option')['debug_action']
    self.execute_query(query, vector.get_value('exec_option'))

  def __execute_fail_action(self, query, vector):
    try:
      self.execute_query(query, vector.get_value('exec_option'),
                         table_format=vector.get_value('table_format'))
      assert 'Expected Failure'
    except ImpalaBeeswaxException as e:
      print e

  def __execute_cancel_action(self, query, vector):
    print 'Starting async query execution'
    handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                      table_format=vector.get_value('table_format'))
    print 'Sleeping'
    sleep(3)
    print 'Issuing Cancel'
    cancel_result = self.client.cancel_query(handle)
    print 'Completed Cancel'

    assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result
