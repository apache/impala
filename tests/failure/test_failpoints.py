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

# Injects failures  at specific locations in each of the plan nodes. Currently supports
# two types of failures - cancellation of the query and a failure test hook.
#
import os
import re
from collections import defaultdict
from time import sleep

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import SkipIf, SkipIfS3, SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_vector import TestDimension

FAILPOINT_ACTION = ['FAIL', 'CANCEL', 'MEM_LIMIT_EXCEEDED']
FAILPOINT_LOCATION = ['PREPARE', 'PREPARE_SCANNER', 'OPEN', 'GETNEXT', 'CLOSE']
# Map debug actions to their corresponding query options' values.
FAILPOINT_ACTION_MAP = {'FAIL': 'FAIL', 'CANCEL': 'WAIT',
                        'MEM_LIMIT_EXCEEDED': 'MEM_LIMIT_EXCEEDED'}

# The goal of this query is to use all of the node types.
# TODO: This query could be simplified a bit...
QUERY = """
select a.int_col, count(b.int_col) int_sum, count(l.l_shipdate)
from functional_hbase.alltypesagg a, tpch_nested_parquet.customer c, c.c_orders.o_lineitems l
join
  (select * from alltypes
   where year=2009 and month=1 order by int_col limit 2500
   union all
   select * from alltypes
   where year=2009 and month=2 limit 3000) b
on (a.int_col = b.int_col) and (a.int_col = c.c_custkey)
where c.c_mktsegment = 'BUILDING'
group by a.int_col
order by int_sum
"""

# TODO: Update to include INSERT when we support failpoints in the HDFS/Hbase sinks using
# a similar pattern as test_cancellation.py
QUERY_TYPE = ["SELECT"]

@SkipIf.skip_hbase # -skip_hbase argument specified
@SkipIfS3.hbase # S3: missing coverage: failures
@SkipIfIsilon.hbase # ISILON: missing coverage: failures.
@SkipIfLocal.hbase
class TestFailpoints(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def parse_plan_nodes_from_explain_output(cls, query, use_db="default"):
    """Parses the EXPLAIN <query> output and returns a map of node_name->list(node_id)"""
    client = cls.create_impala_client()
    client.execute("use %s" % use_db)
    explain_result = client.execute("explain " + QUERY)
    # Maps plan node names to their respective node ids. Expects format of <ID>:<NAME>
    node_id_map = defaultdict(list)
    for row in explain_result.data:
      match = re.search(r'\s*(?P<node_id>\d+)\:(?P<node_type>\S+\s*\S+)', row)
      if match is not None:
        node_id_map[match.group('node_type')].append(int(match.group('node_id')))
    return node_id_map

  @classmethod
  def add_test_dimensions(cls):
    super(TestFailpoints, cls).add_test_dimensions()
    # Executing an explain on the the test query will fail in an enviornment where hbase
    # tables don't exist (s3). Since this happens before the tests are run, the skipif
    # marker won't catch it. If 's3' is detected as a file system, return immedietely.
    if os.getenv("TARGET_FILESYSTEM") in ["s3", "isilon", "local"]: return
    node_id_map = TestFailpoints.parse_plan_nodes_from_explain_output(QUERY, "functional")
    assert node_id_map
    cls.TestMatrix.add_dimension(TestDimension('location', *FAILPOINT_LOCATION))
    cls.TestMatrix.add_dimension(TestDimension('target_node', *(node_id_map.items())))
    cls.TestMatrix.add_dimension(TestDimension('action', *FAILPOINT_ACTION))
    cls.TestMatrix.add_dimension(TestDimension('query_type', *QUERY_TYPE))
    cls.TestMatrix.add_dimension(create_exec_option_dimension([0], [False], [0]))

    # These are invalid test cases.
    # For more info see IMPALA-55 and IMPALA-56.
    cls.TestMatrix.add_constraint(lambda v: not (
        v.get_value('action') == 'FAIL' and
        v.get_value('location') in ['CLOSE'] and
        v.get_value('target_node')[0] in ['AGGREGATE', 'HASH JOIN']) and
        not (v.get_value('location') in ['PREPARE'] and
             v.get_value('action') == 'CANCEL'))

    # Don't create CLOSE:WAIT debug actions to avoid leaking plan fragments (there's no
    # way to cancel a plan fragment once Close() has been called)
    cls.TestMatrix.add_constraint(
        lambda v: not (v.get_value('action') == 'CANCEL'
                     and v.get_value('location') == 'CLOSE'))

    # No need to test error in scanner preparation for non-scan nodes.
    cls.TestMatrix.add_constraint(
        lambda v: (v.get_value('location') != 'PREPARE_SCANNER' or
            v.get_value('target_node')[0] == 'SCAN HDFS'))

  def test_failpoints(self, vector):
    query = QUERY
    node_type, node_ids = vector.get_value('target_node')
    action = vector.get_value('action')
    location = vector.get_value('location')

    for node_id in node_ids:
      debug_action = '%d:%s:%s' % (node_id, location, FAILPOINT_ACTION_MAP[action])
      LOG.info('Current dubug action: SET DEBUG_ACTION=%s' % debug_action)
      vector.get_value('exec_option')['debug_action'] = debug_action

      if action == 'CANCEL':
        self.__execute_cancel_action(query, vector)
      elif action == 'FAIL' or action == 'MEM_LIMIT_EXCEEDED':
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
      LOG.debug(e)

  def __execute_cancel_action(self, query, vector):
    LOG.info('Starting async query execution')
    handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                      table_format=vector.get_value('table_format'))
    LOG.info('Sleeping')
    sleep(3)
    cancel_result = self.client.cancel(handle)
    self.client.close_query(handle)
    assert cancel_result.status_code == 0,\
        'Unexpected status code from cancel request: %s' % cancel_result
