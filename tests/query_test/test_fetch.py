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

import re

from time import sleep
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import extend_exec_option_dimension
from tests.util.parse_util import parse_duration_string_ms


class TestFetch(ImpalaTestSuite):
  """Tests that are independent of whether result spooling is enabled or not."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestFetch, cls).add_test_dimensions()
    # Result fetching should be independent of file format, so only test against
    # Parquet files.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_rows_sent_counters(self, vector):
    """Validate that ClientFetchWaitTimer, NumRowsFetched, RowMaterializationRate,
    and RowMaterializationTimer are set to valid values in the ImpalaServer section
    of the runtime profile."""
    num_rows = 25
    query = "select sleep(100) from functional.alltypes limit {0}".format(num_rows)
    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    try:
      # Wait until the query is 'FINISHED' and results are available for fetching.
      self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 30)
      # Sleep for 2.5 seconds so that the ClientFetchWaitTimer is >= 1s.
      sleep(2.5)
      # Fetch the results so that the fetch related counters are updated.
      assert self.client.fetch(query, handle).success

      runtime_profile = self.client.get_runtime_profile(handle)
      fetch_timer = re.search("ClientFetchWaitTimer: (.*)", runtime_profile)
      assert fetch_timer and len(fetch_timer.groups()) == 1 and \
          parse_duration_string_ms(fetch_timer.group(1)) > 1000
      assert "NumRowsFetched: {0} ({0})".format(num_rows) in runtime_profile
      assert re.search("RowMaterializationRate: [1-9]", runtime_profile)
      # The query should take at least 1s to materialize all rows since it should sleep
      # for at least 1s during materialization.
      materialization_timer = re.search("RowMaterializationTimer: (.*)", runtime_profile)
      assert materialization_timer and len(materialization_timer.groups()) == 1 and \
          parse_duration_string_ms(materialization_timer.group(1)) > 1000
    finally:
      self.client.close_query(handle)


class TestFetchAndSpooling(ImpalaTestSuite):
  """Tests that apply when result spooling is enabled or disabled."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestFetchAndSpooling, cls).add_test_dimensions()
    # Result fetching should be independent of file format, so only test against
    # Parquet files.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')
    # spool_query_results is set as true by default.
    extend_exec_option_dimension(cls, 'spool_query_results', 'false')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_rows_sent_counters(self, vector):
    """Validate that RowsSent and RowsSentRate are set to valid values in
    the PLAN_ROOT_SINK section of the runtime profile."""
    num_rows = 10
    if ('spool_query_results' in vector.get_value('exec_option') and
          vector.get_value('exec_option')['spool_query_results'] == 'false'):
      vector.get_value('exec_option')['debug_action'] = "BPRS_BEFORE_ADD_ROWS:SLEEP@1000"
    else:
      vector.get_value('exec_option')['debug_action'] = "BPRS_BEFORE_ADD_BATCH:SLEEP@1000"
    result = self.execute_query("select id from functional.alltypes limit {0}"
        .format(num_rows), vector.get_value('exec_option'))
    assert "RowsSent: {0} ({0})".format(num_rows) in result.runtime_profile
    rows_sent_rate = re.search("RowsSentRate: (\d*\.?\d*)", result.runtime_profile)
    assert rows_sent_rate
    assert float(rows_sent_rate.group(1)) > 0


class TestFetchTimeout(ImpalaTestSuite):
  """A few basic tests for FETCH_ROWS_TIMEOUT_MS that are not specific to the HS2 protocol
  (e.g. in contrast to the tests in tests/hs2/test_fetch_timeout.py). These tests are
  necessary because part of the FETCH_ROWS_TIMEOUT_MS code is HS2/Beeswax specific.
  Unlike the tests in hs2/test_fetch_timeout.py, these tests do not validate that
  individual RPC calls timeout, instead they set a low value for the timeout and assert
  that the query works end-to-end."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestFetchTimeout, cls).add_test_dimensions()
    # Result fetching should be independent of file format, so only test against
    # Parquet files.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')
    extend_exec_option_dimension(cls, 'spool_query_results', 'true')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_fetch_timeout(self, vector):
    """A simple test that runs a query with a low timeout and introduces delays in
    RowBatch production. Asserts that the query succeeds and returns the expected number
    of rows."""
    num_rows = 100
    query = "select * from functional.alltypes limit {0}".format(num_rows)
    vector.get_value('exec_option')['batch_size'] = 1
    vector.get_value('exec_option')['fetch_rows_timeout_ms'] = 1
    vector.get_value('exec_option')['debug_action'] = '0:GETNEXT:DELAY'
    results = self.execute_query(query, vector.get_value('exec_option'))
    assert results.success
    assert len(results.data) == num_rows

  def test_fetch_before_finished_timeout(self, vector):
    """Tests that the FETCH_ROWS_TIMEOUT_MS timeout applies to queries that are not in
    the 'finished' state. Similar to the test tests/hs2/test_fetch_timeout.py::
    TestFetchTimeout::test_fetch_before_finished_timeout(_with_result_spooling)."""
    num_rows = 10
    query = "select * from functional.alltypes limit {0}".format(num_rows)
    vector.get_value('exec_option')['debug_action'] = 'CRS_BEFORE_COORD_STARTS:SLEEP@5000'
    vector.get_value('exec_option')['fetch_rows_timeout_ms'] = '1000'
    results = self.execute_query(query, vector.get_value('exec_option'))
    assert results.success
    assert len(results.data) == num_rows
