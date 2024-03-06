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

# Tests query cancellation using the ImpalaService.Cancel API
#

from __future__ import absolute_import, division, print_function
from builtins import range
import pytest
import threading
from time import sleep
from RuntimeProfile.ttypes import TRuntimeProfileFormat
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_dimensions import add_mandatory_exec_option
from tests.common.test_vector import ImpalaTestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.cancel_util import cancel_query_and_validate_state
from tests.verifiers.metric_verifier import MetricVerifier

# PRIMARY KEY for lineitem
LINEITEM_PK = 'l_orderkey, l_partkey, l_suppkey, l_linenumber'

# Queries to execute, mapped to a unique PRIMARY KEY for use in CTAS with Kudu. If None
# is specified for the PRIMARY KEY, it will not be used in a CTAS statement on Kudu.
# Use the TPC-H dataset because tables are large so queries take some time to execute.
QUERIES = {'select l_returnflag from lineitem': None,
           'select count(l_returnflag) pk from lineitem': 'pk',
           'select * from lineitem limit 50': LINEITEM_PK,
           'compute stats lineitem': None,
           'select * from lineitem order by l_orderkey': LINEITEM_PK,
           '''SELECT STRAIGHT_JOIN *
           FROM lineitem
                  JOIN /*+broadcast*/ orders ON o_orderkey = l_orderkey
                  JOIN supplier ON s_suppkey = l_suppkey
           WHERE o_orderstatus = 'F'
           ORDER BY l_orderkey
           LIMIT 10000''': LINEITEM_PK
           }

QUERY_TYPE = ["SELECT", "CTAS"]

# Time to sleep between issuing query and canceling. Favor small times since races
# are prone to occur more often when the time between RPCs is small.
CANCEL_DELAY_IN_SECONDS = [0, 0.01, 0.1, 1, 4]

# Number of times to execute/cancel each query under test
NUM_CANCELATION_ITERATIONS = 1

# Test cancellation on both running and hung queries. Node ID 0 is the scan node
WAIT_ACTIONS = [None, '0:GETNEXT:WAIT']

# Verify that failed CancelFInstances() RPCs don't lead to hung queries
FAIL_RPC_ACTIONS = [None, 'COORD_CANCEL_QUERY_FINSTANCES_RPC:FAIL']

# Test cancelling when there is a resource limit.
CPU_LIMIT_S = [0, 100000]

# Verify close rpc running concurrently with fetch rpc. The two cases verify:
# False: close and fetch rpc run concurrently.
# True: cancel rpc is enough to ensure that the fetch rpc is unblocked.
JOIN_BEFORE_CLOSE = [False, True]

# Extra dimensions to test order by without limit
SORT_QUERY = 'select * from lineitem order by l_orderkey'
SORT_CANCEL_DELAY = list(range(6, 10))
SORT_BUFFER_POOL_LIMIT = ['0', '300m'] # Test spilling and non-spilling sorts.

# Test with and without multithreading
MT_DOP_VALUES = [0, 4]

class TestCancellation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query', *QUERIES.keys()))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query_type', *QUERY_TYPE))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('cancel_delay', *CANCEL_DELAY_IN_SECONDS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('wait_action', *WAIT_ACTIONS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('fail_rpc_action', *FAIL_RPC_ACTIONS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('join_before_close', *JOIN_BEFORE_CLOSE))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('buffer_pool_limit', 0))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('cpu_limit_s', *CPU_LIMIT_S))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))

    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('query_type') != 'CTAS' or (\
            v.get_value('table_format').file_format in ['text', 'parquet', 'kudu', 'json']
            and v.get_value('table_format').compression_codec == 'none'))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('exec_option')['batch_size'] == 0)
    # Ignore 'compute stats' queries for the CTAS query type.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: not (v.get_value('query_type') == 'CTAS' and
            v.get_value('query').startswith('compute stats')))

    # Ignore CTAS on Kudu if there is no PRIMARY KEY specified.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: not (v.get_value('query_type') == 'CTAS' and
            v.get_value('table_format').file_format == 'kudu' and
            QUERIES[v.get_value('query')] is None))

    # tpch tables are not generated for hbase as the data loading takes a very long time.
    # TODO: Add cancellation tests for hbase.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'hbase')
    if cls.exploration_strategy() != 'core':
      NUM_CANCELATION_ITERATIONS = 3

  def cleanup_test_table(self, table_format):
    self.execute_query("drop table if exists ctas_cancel", table_format=table_format)

  def execute_cancel_test(self, vector):
    query = vector.get_value('query')
    query_type = vector.get_value('query_type')
    if query_type == "CTAS":
      self.cleanup_test_table(vector.get_value('table_format'))
      file_format = vector.get_value('table_format').file_format
      if file_format == 'kudu':
        assert query in QUERIES and QUERIES[query] is not None,\
            "PRIMARY KEY for query %s not specified" % query
        query = "create table ctas_cancel primary key (%s) "\
            "partition by hash partitions 3 stored as kudu as %s" %\
            (QUERIES[query], query)
      else:
        query = "create table ctas_cancel stored as %sfile as %s" %\
            (file_format, query)

    wait_action = vector.get_value('wait_action')
    fail_rpc_action = vector.get_value('fail_rpc_action')

    debug_action = "|".join([_f for _f in [wait_action, fail_rpc_action] if _f])
    vector.get_value('exec_option')['debug_action'] = debug_action

    vector.get_value('exec_option')['buffer_pool_limit'] =\
        vector.get_value('buffer_pool_limit')
    vector.get_value('exec_option')['cpu_limit_s'] = vector.get_value('cpu_limit_s')
    vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')

    # Execute the query multiple times, cancelling it each time.
    for i in range(NUM_CANCELATION_ITERATIONS):
      cancel_query_and_validate_state(self.client, query,
          vector.get_value('exec_option'), vector.get_value('table_format'),
          vector.get_value('cancel_delay'), vector.get_value('join_before_close'))

      if query_type == "CTAS":
        self.cleanup_test_table(vector.get_value('table_format'))

    # Executing the same query without canceling should work fine. Only do this if the
    # query has a limit or aggregation
    if not debug_action and ('count' in query or 'limit' in query):
      self.execute_query(query, vector.get_value('exec_option'))

  @pytest.mark.execute_serially
  def test_misformatted_profile_text(self):
    """Tests that canceled queries have no whitespace formatting errors in their profiles
    (IMPALA-2063). Executes serially because it is timing-dependent and can be flaky."""
    query = "select count(*) from functional_parquet.alltypes where bool_col = sleep(100)"
    client = self.hs2_client
    # Start query
    handle = client.execute_async(query)
    # Wait for the query to start (with a long timeout to account for admission control
    # queuing).
    WAIT_SECONDS = 60 * 30
    assert any(client.get_state(handle) == 'RUNNING_STATE' or sleep(0.1)
               for _ in range(10 * WAIT_SECONDS)), 'Query failed to start'

    client.cancel(handle)
    # Wait up to 5 seconds for the query to get cancelled
    # TODO(IMPALA-1262): This should be CANCELED_STATE
    # TODO(IMPALA-8411): Remove and assert that the query is cancelled immediately
    assert any(client.get_state(handle) == 'ERROR_STATE' or sleep(1)
               for _ in range(5)), 'Query failed to cancel'
    # Get profile and check for formatting errors
    profile = client.get_runtime_profile(handle, TRuntimeProfileFormat.THRIFT)
    for (k, v) in profile.nodes[1].info_strings.items():
      # Ensure that whitespace gets removed from values.
      assert v == v.rstrip(), \
        "Profile value contains surrounding whitespace: %s %s" % (k, v)
      # Plan text may be strangely formatted.
      assert k == 'Plan' or '\n\n' not in v, \
        "Profile contains repeating newlines: %s %s" % (k, v)

  def teardown_method(self, method):
    # For some reason it takes a little while for the query to get completely torn down
    # when the debug action is WAIT, causing TestValidateMetrics.test_metrics_are_zero to
    # fail. Introducing a small delay allows everything to quiesce.
    # TODO: Figure out a better way to address this
    sleep(1)


class TestCancellationParallel(TestCancellation):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellationParallel, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('query_type') != 'CTAS')

  def test_cancel_select(self, vector):
    self.execute_cancel_test(vector)

class TestCancellationSerial(TestCancellation):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellationSerial, cls).add_test_dimensions()
    # Only run the insert tests in this suite - they need to be serial to allow us to
    # check for file handle leaks.
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('query_type') == 'CTAS')

    # This test suite is slow because it executes serially. Restrict some of the params
    # that are not interesting for inserts.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('cpu_limit_s') == CPU_LIMIT_S[0])
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('join_before_close') == JOIN_BEFORE_CLOSE[0])
    if cls.exploration_strategy() != 'exhaustive':
      # Only run a single 'cancel_delay' option in core.
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('cancel_delay') == CANCEL_DELAY_IN_SECONDS[3])
    else:
      cls.ImpalaTestMatrix.add_constraint(
          lambda v: v.get_value('cancel_delay') != CANCEL_DELAY_IN_SECONDS[0])

  @pytest.mark.execute_serially
  def test_cancel_insert(self, vector):
    if vector.get_value('table_format').file_format == 'json':
      # Insert into json format is not supported yet.
      pytest.skip()
    self.execute_cancel_test(vector)
    metric_verifier = MetricVerifier(self.impalad_test_service)
    metric_verifier.verify_no_open_files(timeout=10)

class TestCancellationFullSort(TestCancellation):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellationFullSort, cls).add_test_dimensions()
    # Override dimensions to only execute the order-by without limit query.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query', SORT_QUERY))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query_type', 'SELECT'))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('cancel_delay', *SORT_CANCEL_DELAY))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('buffer_pool_limit', *SORT_BUFFER_POOL_LIMIT))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('fail_rpc_action') == FAIL_RPC_ACTIONS[0])
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
       v.get_value('table_format').file_format =='parquet' and\
       v.get_value('table_format').compression_codec == 'none')

  def test_cancel_sort(self, vector):
    self.execute_cancel_test(vector)


class TestCancellationFinalizeDelayed(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellationFinalizeDelayed, cls).add_test_dimensions()

    # Test with a small delay in QueryDriver::Finalize from close() to check that queries
    # that are finalized as a result of closing the session correctly return "Invalid or
    # unknown query handle" to the close() RPC.
    add_mandatory_exec_option(cls, 'debug_action', 'FINALIZE_INFLIGHT_QUERY:SLEEP@10')

    # Debug action is independent of file format, so only testing for
    # table_format=parquet/none in order to avoid a test dimension explosion.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def test_cancellation(self, vector):
    query = "select l_returnflag from tpch_parquet.lineitem"
    cancel_delay = 0
    cancel_query_and_validate_state(self.client, query,
        vector.get_value('exec_option'), vector.get_value('table_format'), cancel_delay)
