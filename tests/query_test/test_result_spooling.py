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

from time import sleep
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.util.cancel_util import cancel_query_and_validate_state

# Queries to execute, use the TPC-H dataset because tables are large so queries take some
# time to execute.
CANCELLATION_QUERIES = ['select l_returnflag from tpch_parquet.lineitem',
                        'select * from tpch_parquet.lineitem limit 50',
                        'select * from tpch_parquet.lineitem order by l_orderkey']

# Time to sleep between issuing query and canceling.
CANCEL_DELAY_IN_SECONDS = [0, 0.01, 0.1, 1, 4]


class TestResultSpooling(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpooling, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_result_spooling(self, vector):
    self.run_test_case('QueryTest/result-spooling', vector)

  def test_multi_batches(self, vector):
    """Validates that reading multiple row batches works when result spooling is
    enabled."""
    vector.get_value('exec_option')['batch_size'] = 10
    self.validate_query("select id from alltypes order by id limit 1000",
        vector.get_value('exec_option'))

  def validate_query(self, query, exec_options):
    """Compares the results of the given query with and without result spooling
    enabled."""
    exec_options = exec_options.copy()
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run {0} when result spooling is " \
                           "disabled".format(query)
    base_data = result.data
    exec_options['spool_query_results'] = 'true'
    result = self.execute_query(query, exec_options)
    assert result.success, "Failed to run {0} when result spooling is " \
                           "enabled".format(query)
    assert len(result.data) == len(base_data), "{0} returned a different number of " \
                                               "results when result spooling was " \
                                               "enabled".format(query)
    assert result.data == base_data, "{0} returned different results when result " \
                                     "spooling was enabled".format(query)


class TestResultSpoolingCancellation(ImpalaTestSuite):
  """Test cancellation of queries when result spooling is enabled. This class heavily
  borrows from the cancellation tests in test_cancellation.py. It uses the following test
  dimensions: 'query' and 'cancel_delay'. 'query' is a list of queries to run
  asynchronously and then cancel. 'cancel_delay' controls how long a query should run
  before being cancelled.
  """

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestResultSpoolingCancellation, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('query',
        *CANCELLATION_QUERIES))
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('cancel_delay',
        *CANCEL_DELAY_IN_SECONDS))

    # Result spooling should be independent of file format, so only testing for
    # table_format=parquet/none in order to avoid a test dimension explosion.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def test_cancellation(self, vector):
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    cancel_query_and_validate_state(self.client, vector.get_value('query'),
        vector.get_value('exec_option'), vector.get_value('table_format'),
        vector.get_value('cancel_delay'))

  def test_cancel_no_fetch(self, vector):
    """Test cancelling a query before any results are fetched. Unlike the
    test_cancellation test, the query is cancelled before results are
    fetched (there is no fetch thread)."""
    vector.get_value('exec_option')['spool_query_results'] = 'true'
    handle = None
    try:
      handle = self.execute_query_async(vector.get_value('query'),
          vector.get_value('exec_option'))
      sleep(vector.get_value('cancel_delay'))
      cancel_result = self.client.cancel(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: {0}'.format(cancel_result)
    finally:
      if handle: self.client.close_query(handle)
