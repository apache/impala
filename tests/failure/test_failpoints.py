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
from __future__ import absolute_import, division, print_function
from builtins import range
import pytest
import re
from time import sleep

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import SkipIf, SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.verifiers.metric_verifier import MetricVerifier

FAILPOINT_ACTIONS = ['FAIL', 'CANCEL', 'MEM_LIMIT_EXCEEDED']
# Not included:
# - SCANNER_ERROR, because it only fires if the query already hit an error.
FAILPOINT_LOCATIONS = ['PREPARE', 'PREPARE_SCANNER', 'OPEN', 'GETNEXT', 'GETNEXT_SCANNER',
                       'CLOSE']
# Map debug actions to their corresponding query options' values.
FAILPOINT_ACTION_MAP = {'FAIL': 'FAIL', 'CANCEL': 'WAIT',
                        'MEM_LIMIT_EXCEEDED': 'MEM_LIMIT_EXCEEDED'}
MT_DOP_VALUES = [0, 4]

# Queries should cover all exec nodes.
QUERIES = [
  "select * from alltypes",
  "select count(*) from alltypessmall",
  "select count(int_col) from alltypessmall group by id",
  "select 1 from alltypessmall a join alltypessmall b on a.id = b.id",
  "select 1 from alltypessmall a join alltypessmall b on a.id != b.id",
  "select 1 from alltypessmall order by id",
  "select 1 from alltypessmall order by id limit 100",
  "select * from alltypessmall union all select * from alltypessmall",
  "select row_number() over (partition by int_col order by id) from alltypessmall",
  "select c from (select id c from alltypessmall order by id limit 10) v where c = 1",
  """SELECT STRAIGHT_JOIN *
           FROM alltypes t1
                  JOIN /*+broadcast*/ alltypesagg t2 ON t1.id = t2.id
           WHERE t2.int_col < 1000"""
]


@SkipIf.skip_hbase  # -skip_hbase argument specified
@SkipIfFS.hbase  # missing coverage: failures
class TestFailpoints(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestFailpoints, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query', *QUERIES))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('action', *FAILPOINT_ACTIONS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('location', *FAILPOINT_LOCATIONS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension([0], [False], [0]))

    # Don't create PREPARE:WAIT debug actions because cancellation is not checked until
    # after the prepare phase once execution is started.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: not (v.get_value('action') == 'CANCEL'
                     and v.get_value('location') == 'PREPARE'))
    # Don't create CLOSE:WAIT debug actions to avoid leaking plan fragments (there's no
    # way to cancel a plan fragment once Close() has been called)
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: not (v.get_value('action') == 'CANCEL'
                     and v.get_value('location') == 'CLOSE'))

  # Run serially because this can create enough memory pressure to invoke the Linux OOM
  # killer on machines with 30GB RAM. This makes the test run in 4 minutes instead of 1-2.
  @pytest.mark.execute_serially
  def test_failpoints(self, vector):
    query = vector.get_value('query')
    action = vector.get_value('action')
    location = vector.get_value('location')
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')

    plan_node_ids = self.__parse_plan_nodes_from_explain(query, vector)
    for node_id in plan_node_ids:
      debug_action = '%d:%s:%s' % (node_id, location, FAILPOINT_ACTION_MAP[action])
      # IMPALA-7046: add jitter to backend startup to exercise various failure paths.
      debug_action += '|COORD_BEFORE_EXEC_RPC:JITTER@100@0.3'

      LOG.info('Current debug action: SET DEBUG_ACTION=%s' % debug_action)
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

    # Detect any hung fragments left from this test.
    for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_metric("impala-server.num-fragments-in-flight", 0)

  def __parse_plan_nodes_from_explain(self, query, vector):
    """Parses the EXPLAIN <query> output and returns a list of node ids.
    Expects format of <ID>:<NAME>"""
    explain_result =\
        self.execute_query("explain " + query, vector.get_value('exec_option'),
                           table_format=vector.get_value('table_format'))
    node_ids = []
    for row in explain_result.data:
      match = re.search(r'\s*(?P<node_id>\d+)\:(?P<node_type>\S+\s*\S+)', row)
      if match is not None:
        node_ids.append(int(match.group('node_id')))
    return node_ids

  def test_lifecycle_failures(self):
    """Test that targeted failure injections in the query lifecycle do not cause crashes
    or hangs"""
    query = "select * from tpch.lineitem limit 10000"

    # Test that the admission controller handles scheduler errors correctly.
    debug_action = "SCHEDULER_SCHEDULE:FAIL"
    result = self.execute_query_expect_failure(self.client, query,
        query_options={'debug_action': debug_action})
    assert "Error during scheduling" in str(result)

    # Fail Exec() in the coordinator.
    debug_action = 'EXEC_SERIALIZE_FRAGMENT_INFO:FAIL@1.0'
    self.execute_query_expect_failure(self.client, query,
        query_options={'debug_action': debug_action})

    # Fail the Prepare() phase of all fragment instances.
    debug_action = 'FIS_IN_PREPARE:FAIL@1.0'
    self.execute_query_expect_failure(self.client, query,
        query_options={'debug_action': debug_action})

    # Fail the Open() phase of all fragment instances.
    debug_action = 'FIS_IN_OPEN:FAIL@1.0'
    self.execute_query_expect_failure(self.client, query,
        query_options={'debug_action': debug_action})

    # Fail the ExecInternal() phase of all fragment instances.
    debug_action = 'FIS_IN_EXEC_INTERNAL:FAIL@1.0'
    self.execute_query_expect_failure(self.client, query,
        query_options={'debug_action': debug_action})

    # Fail the fragment instance thread creation with a 0.5 probability.
    debug_action = 'FIS_FAIL_THREAD_CREATION:FAIL@0.5'

    # We want to test the behavior when only some fragment instance threads fail to be
    # created, so we set the probability of fragment instance thread creation failure to
    # 0.5. Since there's only a 50% chance of fragment instance thread creation failure,
    # we attempt to catch a query failure up to a very conservative maximum of 50 tries.
    for i in range(50):
      try:
        self.execute_query(query,
            query_options={'debug_action': debug_action})
      except ImpalaBeeswaxException as e:
        assert 'Debug Action: FIS_FAIL_THREAD_CREATION:FAIL@0.5' \
            in str(e), str(e)
        break

  def __execute_fail_action(self, query, vector):
    try:
      self.execute_query(query, vector.get_value('exec_option'),
                         table_format=vector.get_value('table_format'))
      assert 'Expected Failure'
    except ImpalaBeeswaxException as e:
      LOG.debug(e)
      # IMPALA-5197: None of the debug actions should trigger corrupted file message
      assert 'Corrupt Parquet file' not in str(e)

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
