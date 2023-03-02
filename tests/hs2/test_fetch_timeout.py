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

from __future__ import absolute_import, division, print_function
from time import sleep
from time import time
from tests.common.errors import Timeout
from tests.hs2.hs2_test_suite import (HS2TestSuite, needs_session)
from TCLIService import TCLIService


# Tests for the query option FETCH_ROWS_TIMEOUT_MS, which is the maximum amount of
# time, in milliseconds, a fetch rows request (TFetchResultsReq) from the client should
# spend fetching results (including waiting for results to become available and
# materialize).

class TestFetchTimeout(HS2TestSuite):
  """This class contains all fetch timeout tests applicable when result spooling is
  enabled and disabled. Since the fetch timeout code changes based on whether result
  spooling is enabled, some tests are run both when result spooling is enabled and
  disabled."""

  @needs_session()
  def test_fetch_timeout(self):
    """Delegates to self.__test_fetch_timeout()."""
    self.__test_fetch_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_timeout_with_result_spooling(self):
    """Delegates to self.__test_fetch_timeout()."""
    self.__test_fetch_timeout()

  def __test_fetch_timeout(self):
    """Tests FETCH_ROWS_TIMEOUT_MS by running a query that produces RowBatches with a
    large delay. The test waits for the query to 'finish' and then fetches the first
    RowBatch, which should always be available since a query is only considered
    'finished' if rows are available. Subsequent fetches should time out because
    RowBatch production has been delayed."""
    # Construct a query where there is a large delay between RowBatch production.
    num_rows = 2
    statement = "select bool_col, avg(id) from functional.alltypes group by bool_col " \
                "having avg(id) != sleep(5000)"
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'fetch_rows_timeout_ms': '1', 'batch_size': '1', 'num_nodes': '1'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Wait for rows to be available for fetch.
    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE, timeout=30)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Assert that exactly 1 row can be fetched.
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, 1, statement)

    # Assert that the next fetch request times out while waiting for a RowBatch to be
    # produced.
    fetch_results_resp = self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(
            operationHandle=execute_statement_resp.operationHandle, maxRows=num_rows))
    HS2TestSuite.check_response(fetch_results_resp)
    num_rows_fetched = HS2TestSuite.get_num_rows(fetch_results_resp.results)
    assert num_rows_fetched == 0
    assert fetch_results_resp.hasMoreRows
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, 1, statement)

  @needs_session()
  def test_fetch_materialization_timeout(self):
    """Delegates to self.__test_fetch_materialization_timeout()."""
    self.__test_fetch_materialization_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_materialization_timeout_with_result_spooling(self):
    """Delegates to self.__test_fetch_materialization_timeout()."""
    self.__test_fetch_materialization_timeout()

  def __test_fetch_materialization_timeout(self):
    """Test the query option FETCH_ROWS_TIMEOUT_MS applies to the time taken to
    materialize rows. Runs a query with a sleep() which is evaluated during
    materialization and validates the timeout is applied appropriately."""
    num_rows = 2
    statement = "select sleep(5000) from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'batch_size': '1', 'fetch_rows_timeout_ms': '2500'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Wait for rows to be available for fetch.
    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Only one row should be returned because the timeout should be hit after
    # materializing the first row, but before materializing the second one.
    fetch_results_resp = self.hs2_client.FetchResults(
        TCLIService.TFetchResultsReq(
            operationHandle=execute_statement_resp.operationHandle, maxRows=2))
    HS2TestSuite.check_response(fetch_results_resp)
    assert HS2TestSuite.get_num_rows(fetch_results_resp.results) == 1

    # Assert that all remaining rows can be fetched.
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, num_rows - 1, statement)

  @needs_session()
  def test_fetch_before_finished_timeout(self):
    """Delegates to self.__test_fetch_before_finished_timeout()."""
    self.__test_fetch_before_finished_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_before_finished_timeout_with_result_spooling(self):
    """Delegates to self.__test_fetch_before_finished_timeout()."""
    self.__test_fetch_before_finished_timeout()

  def __test_fetch_before_finished_timeout(self):
    """Tests the query option FETCH_ROWS_TIMEOUT_MS applies to fetch requests before the
    query has 'finished'. Fetch requests issued before the query has finished, should
    wait FETCH_ROWS_TIMEOUT_MS before returning. This test runs a query with a DELAY
    DEBUG_ACTION before Coordinator starts. Fetch requests during this delay should
    return with 0 rows."""
    num_rows = 10
    statement = "select * from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'debug_action': 'CRS_BEFORE_COORD_STARTS:SLEEP@5000',
                      'fetch_rows_timeout_ms': '1000'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Assert that the first fetch request returns 0 rows.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=1024))
    HS2TestSuite.check_response(fetch_results_resp,
        expected_status_code=TCLIService.TStatusCode.STILL_EXECUTING_STATUS)
    assert fetch_results_resp.hasMoreRows
    assert not fetch_results_resp.results

    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Assert that all remaining rows can be fetched.
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, num_rows, statement)

  @needs_session()
  def test_fetch_finished_timeout(self):
    self.__test_fetch_finished_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_finished_timeout_with_result_spooling(self):
    self.__test_fetch_finished_timeout()

  def __test_fetch_finished_timeout(self):
    """Tests the query option FETCH_ROWS_TIMEOUT_MS applies to both the time spent
    waiting for a query to finish and the time spent waiting for RowBatches to be sent,
    and that the timeout it not reset for in-progress fetch requests when queries
    transition to the 'finished' state."""
    num_rows = 20
    statement = "select sleep(500) from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
            conf_overlay={'debug_action': 'CRS_BEFORE_COORD_STARTS:SLEEP@5000',
                'batch_size': '10', 'fetch_rows_timeout_ms': '7500'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Assert that the first fetch request returns 0 rows.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=1024))
    HS2TestSuite.check_response(fetch_results_resp)
    assert fetch_results_resp.hasMoreRows
    assert HS2TestSuite.get_num_rows(fetch_results_resp.results) == 10

    # Wait for rows to be available for fetch.
    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Assert that all remaining rows can be fetched.
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, num_rows - 10, statement)

  @needs_session()
  def test_fetch_no_timeout(self):
    """Delegates to self.__test_fetch_no_timeout()."""
    self.__test_fetch_no_timeout()

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_no_timeout_with_result_spooling(self):
    """Delegates to self.__test_fetch_no_timeout()."""
    self.__test_fetch_no_timeout()

  def __test_fetch_no_timeout(self):
    """Tests setting FETCH_ROWS_TIMEOUT_MS to 0, and validates that fetch requests wait
    indefinitely when the timeout is 0."""
    num_rows = 10
    statement = "select * from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'debug_action': 'CRS_BEFORE_COORD_STARTS:SLEEP@5000',
                      'fetch_rows_timeout_ms': '0'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Assert that the first fetch request returns 0 rows.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=1024))
    HS2TestSuite.check_response(fetch_results_resp)
    assert self.get_num_rows(fetch_results_resp.results) == num_rows


class TestFetchTimeoutWithResultSpooling(HS2TestSuite):
  """Tests for FETCH_ROWS_TIMEOUT_MS that are specific to result spooling. Most of these
  tests rely on the fact that when result spooling is enabled, multiple RowBatches can be
  fetched at once."""

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_fetch_multiple_batches_timeout(self):
    """Validate that FETCH_ROWS_TIMEOUT_MS applies when reading multiple RowBatches.
    This test runs a query that produces multiple RowBatches with a fixed delay, and
    asserts that a fetch request to read all rows only reads a subset of the rows (since
    the timeout should ensure that a single fetch request cannot read all RowBatches)."""
    num_rows = 500
    statement = "select id from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'batch_size': '10',
                      'debug_action': '0:GETNEXT:DELAY',
                      'fetch_rows_timeout_ms': '500'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Wait for rows to be available for fetch.
    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE)
    HS2TestSuite.check_response(get_operation_status_resp)

    # Issue a fetch request to read all rows, and validate that only a subset of the rows
    # are returned.
    fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
        operationHandle=execute_statement_resp.operationHandle, maxRows=num_rows))
    HS2TestSuite.check_response(fetch_results_resp)
    num_rows_fetched = HS2TestSuite.get_num_rows(fetch_results_resp.results)
    assert num_rows_fetched > 0 and num_rows_fetched < num_rows
    assert fetch_results_resp.hasMoreRows

    # Assert that all remaining rows can be fetched.
    FetchTimeoutUtils.fetch_num_rows(self.hs2_client,
        execute_statement_resp.operationHandle, num_rows - num_rows_fetched, statement)

  @needs_session(conf_overlay={'spool_query_results': 'true'})
  def test_multiple_fetch_multiple_batches_timeout(self):
    """Test the query option FETCH_ROWS_TIMEOUT_MS by running a query with a DELAY
    DEBUG_ACTION and a low value for the fetch timeout. This test issues fetch requests
    in a loop until all results have been returned, and validates that some of the fetch
    requests timed out. It is similar to test_fetch_multiple_batches_timeout except it
    issues multiple fetch requests that are expected to timeout."""
    num_rows = 100
    statement = "select * from functional.alltypes limit {0}".format(num_rows)
    execute_statement_resp = self.execute_statement(statement,
        conf_overlay={'batch_size': '1', 'debug_action': '0:GETNEXT:DELAY',
                      'fetch_rows_timeout_ms': '1'})
    HS2TestSuite.check_response(execute_statement_resp)

    # Wait for rows to be available for fetch.
    get_operation_status_resp = self.wait_for_operation_state(
        execute_statement_resp.operationHandle,
        TCLIService.TOperationState.FINISHED_STATE, timeout=30)
    HS2TestSuite.check_response(get_operation_status_resp)

    # The timeout to wait for fetch requests to fetch all rows.
    timeout = 30

    start_time = time()
    num_fetched = 0
    num_fetch_requests = 0

    # Fetch results until either the timeout is hit or all rows have been fetched.
    while num_fetched != num_rows and time() - start_time < timeout:
      sleep(0.5)
      fetch_results_resp = self.hs2_client.FetchResults(TCLIService.TFetchResultsReq(
          operationHandle=execute_statement_resp.operationHandle, maxRows=num_rows))
      HS2TestSuite.check_response(fetch_results_resp)
      num_fetched += HS2TestSuite.get_num_rows(fetch_results_resp.results)
      num_fetch_requests += 1
    if num_fetched != num_rows:
      raise Timeout("Query {0} did not fetch all results within the timeout {1}"
                    .format(statement, timeout))
    # The query produces 100 RowBatches, each batch was delayed 100ms before it was sent
    # to the PlanRootSink. Each fetch request requested all 100 rows, but since the
    # timeout is set to such a low value, multiple fetch requests should be necessary to
    # read all rows.
    assert num_fetch_requests >= 5


class FetchTimeoutUtils():
  """This class contains all common code used when testing fetch timeouts."""

  @staticmethod
  def fetch_num_rows(hs2_client, op_handle, num_rows, statement):
    """Fetch the specified number of rows in the given op_handle and validate that the
    number of rows returned matches the expected number of rows. If the op_handle does
    not return the expected number of rows within a timeout, an error is thrown."""
    # The timeout to wait for fetch requests to fetch all rows.
    timeout = 30

    start_time = time()
    num_fetched = 0

    # Fetch results until either the timeout is hit or all rows have been fetched.
    while num_fetched != num_rows and time() - start_time < timeout:
      sleep(0.5)
      fetch_results_resp = hs2_client.FetchResults(
          TCLIService.TFetchResultsReq(operationHandle=op_handle,
              maxRows=num_rows - num_fetched))
      HS2TestSuite.check_response(fetch_results_resp)
      num_fetched += HS2TestSuite.get_num_rows(fetch_results_resp.results)
    if num_fetched != num_rows:
      raise Timeout("Query {0} did not fetch all results within the timeout {1}"
                    .format(statement, timeout))
    assert num_fetched == num_rows
