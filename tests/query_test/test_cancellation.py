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

import pytest
import threading
from time import sleep
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import ImpalaTestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.verifiers.metric_verifier import MetricVerifier

# PRIMARY KEY for lineitem
LINEITEM_PK = 'l_orderkey, l_partkey, l_suppkey, l_linenumber'

# Queries to execute, mapped to a unique PRIMARY KEY for use in CTAS with Kudu. If None
# is specified for the PRIMARY KEY, it will not be used in a CTAS statement on Kudu.
# Use the TPC-H dataset because tables are large so queries take some time to execute.
QUERIES = {'select l_returnflag from lineitem' : None,
           'select count(l_returnflag) pk from lineitem' : 'pk',
           'select * from lineitem limit 50' : LINEITEM_PK,
           'compute stats lineitem' : None,
           'select * from lineitem order by l_orderkey' : LINEITEM_PK}

QUERY_TYPE = ["SELECT", "CTAS"]

# Time to sleep between issuing query and canceling
CANCEL_DELAY_IN_SECONDS = range(5)

# Number of times to execute/cancel each query under test
NUM_CANCELATION_ITERATIONS = 1

# Test cancellation on both running and hung queries
DEBUG_ACTIONS = [None, 'WAIT']

# Extra dimensions to test order by without limit
SORT_QUERY = 'select * from lineitem order by l_orderkey'
SORT_CANCEL_DELAY = range(6, 10)
SORT_BLOCK_MGR_LIMIT = ['0', '300m'] # Test spilling and non-spilling sorts.

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
        ImpalaTestDimension('action', *DEBUG_ACTIONS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('max_block_mgr_memory', 0))

    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('query_type') != 'CTAS' or (\
            v.get_value('table_format').file_format in ['text', 'parquet', 'kudu'] and\
            v.get_value('table_format').compression_codec == 'none'))
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
        assert QUERIES.has_key(query) and QUERIES[query] is not None,\
            "PRIMARY KEY for query %s not specified" % query
        query = "create table ctas_cancel primary key (%s) "\
            "partition by hash partitions 3 stored as kudu as %s" %\
            (QUERIES[query], query)
      else:
        query = "create table ctas_cancel stored as %sfile as %s" %\
            (file_format, query)

    action = vector.get_value('action')
    # node ID 0 is the scan node
    debug_action = '0:GETNEXT:' + action if action != None else ''
    vector.get_value('exec_option')['debug_action'] = debug_action

    vector.get_value('exec_option')['max_block_mgr_memory'] =\
        vector.get_value('max_block_mgr_memory')

    # Execute the query multiple times, cancelling it each time.
    for i in xrange(NUM_CANCELATION_ITERATIONS):
      handle = self.execute_query_async(query, vector.get_value('exec_option'),
                                        table_format=vector.get_value('table_format'))

      def fetch_results():
        threading.current_thread().fetch_results_error = None
        try:
          new_client = self.create_impala_client()
          new_client.fetch(query, handle)
        except ImpalaBeeswaxException as e:
          threading.current_thread().fetch_results_error = e

      thread = threading.Thread(target=fetch_results)
      thread.start()

      sleep(vector.get_value('cancel_delay'))
      assert self.client.get_state(handle) != self.client.QUERY_STATES['EXCEPTION']
      cancel_result = self.client.cancel(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result
      thread.join()

      if thread.fetch_results_error is None:
        # If the query is cancelled while it's in the fetch rpc, it gets unregistered and
        # therefore closed. Only call close on queries that did not fail fetch.
        self.client.close_query(handle)
      elif 'Cancelled' not in str(thread.fetch_results_error):
        # If fetch failed for any reason other than cancellation, raise the error.
        raise thread.fetch_results_error

      if query_type == "CTAS":
        self.cleanup_test_table(vector.get_value('table_format'))

      # TODO: Add some additional verification to check to make sure the query was
      # actually canceled

    # Executing the same query without canceling should work fine. Only do this if the
    # query has a limit or aggregation
    if action is None and ('count' in query or 'limit' in query):
      self.execute_query(query, vector.get_value('exec_option'))

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
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('query_type') == 'CTAS' or
        v.get_value('query').startswith('compute stats'))
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('cancel_delay') != 0)
    cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('action') is None)
    # Don't run across all cancel delay options unless running in exhaustive mode
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v: v.get_value('cancel_delay') in [3])

  @pytest.mark.execute_serially
  def test_cancel_insert(self, vector):
    self.execute_cancel_test(vector)
    metric_verifier = MetricVerifier(self.impalad_test_service)
    try:
      metric_verifier.verify_no_open_files(timeout=10)
    except AssertionError:
      pytest.xfail("IMPALA-551: File handle leak for INSERT")

class TestCancellationFullSort(TestCancellation):
  @classmethod
  def add_test_dimensions(cls):
    super(TestCancellation, cls).add_test_dimensions()
    # Override dimensions to only execute the order-by without limit query.
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query', SORT_QUERY))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('query_type', 'SELECT'))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('cancel_delay', *SORT_CANCEL_DELAY))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('max_block_mgr_memory', *SORT_BLOCK_MGR_LIMIT))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('action', None))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
       v.get_value('table_format').file_format =='parquet' and\
       v.get_value('table_format').compression_codec == 'none')

  def test_cancel_sort(self, vector):
    self.execute_cancel_test(vector)
