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
#
# Client tests for Impala's HiveServer2 interface

from getpass import getuser
import json
import logging
import pytest
import time

from urllib2 import urlopen

from ImpalaService import ImpalaHiveServer2Service
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.skip import SkipIfDockerizedCluster
from tests.hs2.hs2_test_suite import (HS2TestSuite, needs_session,
    operation_id_to_query_id, create_session_handle_without_secret,
    create_op_handle_without_secret)
from TCLIService import TCLIService

LOG = logging.getLogger('test_hs2')

SQLSTATE_GENERAL_ERROR = "HY000"

class TestHS2(HS2TestSuite):
  def setup_method(self, method):
    # Keep track of extra session handles opened by _open_extra_session.
    self.__extra_sessions = []

  def teardown_method(self, method):
    for session in self.__extra_sessions:
      try:
        close_session_req = TCLIService.TCloseSessionReq(session)
        self.hs2_client.CloseSession(close_session_req)
      except Exception:
        LOG.log_exception("Error closing session.")

  def test_open_session(self):
    """Check that a session can be opened"""
    open_session_req = TCLIService.TOpenSessionReq()
    TestHS2.check_response(self.hs2_client.OpenSession(open_session_req))

  def test_open_session_query_options(self):
    """Check that OpenSession sets query options"""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.configuration = {'MAX_ERRORS': '45678',
        'NUM_NODES': '1234', 'MAX_NUM_RUNTIME_FILTERS': '333'}
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    for k, v in open_session_req.configuration.items():
      assert open_session_resp.configuration[k] == v

  def get_session_options(self, setCmd):
    """Returns dictionary of query options."""
    execute_statement_resp = self.execute_statement(setCmd)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1000
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)

    # Close the query
    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    # Results are returned in a columnar way:
    cols = fetch_results_resp.results.columns
    assert len(cols) == 3
    vals = dict(zip(cols[0].stringVal.values, cols[1].stringVal.values))
    levels = dict(zip(cols[0].stringVal.values, cols[2].stringVal.values))
    return vals, levels

  @needs_session()
  def test_session_options_via_set(self):
    """
    Tests that session options are returned by a SET
    query and can be updated by a "SET k=v" query.
    """
    vals, levels = self.get_session_options("SET")

    # No default; should be empty string.
    assert vals["COMPRESSION_CODEC"] == ""
    # Has default of 0
    assert vals["SYNC_DDL"] == "0"

    # Set an option using SET
    self.execute_statement("SET COMPRESSION_CODEC=gzip")

    vals2, levels = self.get_session_options("SET")
    assert vals2["COMPRESSION_CODEC"] == "GZIP"
    assert levels["COMPRESSION_CODEC"] == "REGULAR"
    # Should be unchanged
    assert vals2["SYNC_DDL"] == "0"

    assert "MAX_ERRORS" in vals2
    assert levels["MAX_ERRORS"] == "ADVANCED"
    # Verify that 'DEVELOPMENT' options are not returned.
    assert "DEBUG_ACTION" not in vals2

    # Removed options are not returned.
    assert "MAX_IO_BUFFERS" not in vals2

  @needs_session()
  def test_session_option_levels_via_set_all(self):
    """
    Tests the level of session options returned by a SET ALL query except DEPRECATED as we
    currently do not have any of those left.
    """
    vals, levels = self.get_session_options("SET ALL")

    assert "COMPRESSION_CODEC" in vals
    assert "SYNC_DDL" in vals
    assert "MAX_ERRORS" in vals
    assert "DEBUG_ACTION" in vals
    assert levels["COMPRESSION_CODEC"] == "REGULAR"
    assert levels["SYNC_DDL"] == "REGULAR"
    assert levels["MAX_ERRORS"] == "ADVANCED"
    assert levels["DEBUG_ACTION"] == "DEVELOPMENT"
    # Removed options are returned by "SET ALL" for the benefit of impala-shell.
    assert levels["MAX_IO_BUFFERS"] == "REMOVED"

  @SkipIfDockerizedCluster.internal_hostname
  def test_open_session_http_addr(self):
    """Check that OpenSession returns the coordinator's http address."""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    http_addr = open_session_resp.configuration['http_addr']
    resp = urlopen("http://%s/queries?json" % http_addr)
    assert resp.msg == 'OK'
    queries_json = json.loads(resp.read())
    assert 'completed_queries' in queries_json
    assert 'in_flight_queries' in queries_json

  def test_open_session_unsupported_protocol(self):
    """Test that we get the right protocol version back if we ask for one larger than the
    server supports. This test will fail as we support newer version of HS2, and should be
    updated."""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.protocol_version = \
        TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    assert open_session_resp.serverProtocolVersion == \
        TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6

  def test_open_session_empty_user(self):
    """Test that we get the expected errors back if either impala.doas.user is set but
    username is empty, or username is set but impala.doas.user is empty."""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = ""
    open_session_req.configuration = {"impala.doas.user": "do_as_user"}
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp, TCLIService.TStatusCode.ERROR_STATUS, \
        "Unable to delegate using empty proxy username.")

    open_session_req.username = "user"
    open_session_req.configuration = {"impala.doas.user": ""}
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp, TCLIService.TStatusCode.ERROR_STATUS, \
        "Unable to delegate using empty doAs username.")

  def test_close_session(self):
    """Test that an open session can be closed"""
    open_session_req = TCLIService.TOpenSessionReq()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Check that CloseSession validates session secret and acts as if the session didn't
    # exist.
    invalid_close_session_req = TCLIService.TCloseSessionReq()
    invalid_close_session_req.sessionHandle = create_session_handle_without_secret(
        resp.sessionHandle)
    TestHS2.check_invalid_session(self.hs2_client.CloseSession(invalid_close_session_req))

    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = resp.sessionHandle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

  def test_double_close_session(self):
    """Test that an already closed session cannot be closed a second time"""
    open_session_req = TCLIService.TOpenSessionReq()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = resp.sessionHandle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

    # Double close should be an error
    TestHS2.check_invalid_session(self.hs2_client.CloseSession(close_session_req))

  # This test verifies the number of open and expired sessions so avoid running
  # concurrently with other sessions.
  @pytest.mark.execute_serially
  def test_concurrent_session_mixed_idle_timeout(self):
    """Test for concurrent idle sessions' expiration with mixed timeout durations."""
    timeout_periods = [0, 5, 10]
    session_handles = []
    last_time_session_active = []

    for timeout in timeout_periods:
      open_session_req = TCLIService.TOpenSessionReq()
      open_session_req.configuration = {}
      open_session_req.configuration['idle_session_timeout'] = str(timeout)
      resp = self.hs2_client.OpenSession(open_session_req)
      TestHS2.check_response(resp)
      session_handles.append(resp.sessionHandle)

    num_open_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    num_expired_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-sessions-expired")

    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.statement = "SELECT 1+2"
    for session_handle in session_handles:
      execute_statement_req.sessionHandle = session_handle
      resp = self.hs2_client.ExecuteStatement(execute_statement_req)
      TestHS2.check_response(resp)
      last_time_session_active.append(time.time())

    assert num_open_sessions == self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    assert num_expired_sessions == self.impalad_test_service.get_metric_value(
        "impala-server.num-sessions-expired")

    for timeout in timeout_periods:
      sleep_period = timeout + 1.5
      time.sleep(sleep_period)
      for i, session_handle in enumerate(session_handles):
        if session_handle is not None:
          execute_statement_req.sessionHandle = session_handle
          resp = self.hs2_client.ExecuteStatement(execute_statement_req)
          last_exec_statement_time = time.time()
          if timeout_periods[i] == 0 or \
             timeout_periods[i] > last_exec_statement_time - last_time_session_active[i]:
            TestHS2.check_response(resp)
            last_time_session_active[i] = last_exec_statement_time
          else:
            TestHS2.check_response(resp, TCLIService.TStatusCode.ERROR_STATUS)
            close_session_req = TCLIService.TCloseSessionReq()
            close_session_req.sessionHandle = session_handles[i]
            TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))
            session_handles[i] = None
            num_open_sessions -= 1
            num_expired_sessions += 1
      assert num_open_sessions == self.impalad_test_service.get_metric_value(
          "impala-server.num-open-hiveserver2-sessions")
      assert num_expired_sessions == self.impalad_test_service.get_metric_value(
          "impala-server.num-sessions-expired")

    # Close the remaining sessions.
    for session_handle in session_handles:
      if session_handle is not None:
        close_session_req = TCLIService.TCloseSessionReq(session_handle)
        TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

  @needs_session()
  def test_get_operation_status(self):
    """Tests that GetOperationStatus returns a valid result for a running query"""
    statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    TestHS2.check_response(get_operation_status_resp)
    # If ExecuteStatement() has completed but the results haven't been fetched yet, the
    # query must have reached either PENDING or RUNNING or FINISHED.
    assert get_operation_status_resp.operationState in \
        [TCLIService.TOperationState.PENDING_STATE,
         TCLIService.TOperationState.RUNNING_STATE,
         TCLIService.TOperationState.FINISHED_STATE]

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    TestHS2.check_response(get_operation_status_resp)
    # After fetching the results, the query must be in state FINISHED.
    assert get_operation_status_resp.operationState == \
        TCLIService.TOperationState.FINISHED_STATE

    # Validate that the operation secret is checked.
    TestHS2.check_invalid_query(self.get_operation_status(
        create_op_handle_without_secret(execute_statement_resp.operationHandle)),
        expect_legacy_err=True)

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    # GetOperationState should return 'Invalid query handle' if the query has been closed.
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

  @needs_session(conf_overlay={"abort_on_error": "1"})
  def test_get_operation_status_error(self):
    """Tests that GetOperationStatus returns a valid result for a query that encountered
    an error"""
    statement = "SELECT * FROM functional.alltypeserror"
    execute_statement_resp = self.execute_statement(statement)

    get_operation_status_resp = self.wait_for_operation_state( \
        execute_statement_resp.operationHandle, TCLIService.TOperationState.ERROR_STATE)

    # Check that an error message and sql state have been set.
    assert get_operation_status_resp.errorMessage is not None and \
        get_operation_status_resp.errorMessage is not ""
    assert get_operation_status_resp.sqlState == SQLSTATE_GENERAL_ERROR

  @needs_session()
  def test_malformed_get_operation_status(self):
    """Tests that a short guid / secret returns an error (regression would be to crash
    impalad)"""
    operation_handle = TCLIService.TOperationHandle()
    operation_handle.operationId = TCLIService.THandleIdentifier()
    operation_handle.operationId.guid = "short"
    operation_handle.operationId.secret = "short_secret"
    assert len(operation_handle.operationId.guid) != 16
    assert len(operation_handle.operationId.secret) != 16
    operation_handle.operationType = TCLIService.TOperationType.EXECUTE_STATEMENT
    operation_handle.hasResultSet = False

    get_operation_status_resp = self.get_operation_status(operation_handle)
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "(guid size: %d, expected 16, secret size: %d, expected 16)" \
        % (len(operation_handle.operationId.guid),
           len(operation_handle.operationId.secret))
    assert err_msg in get_operation_status_resp.status.errorMessage

  @needs_session()
  def test_invalid_query_handle(self):
    operation_handle = TCLIService.TOperationHandle()
    operation_handle.operationId = TCLIService.THandleIdentifier()
    operation_handle.operationId.guid = "\x01\x23\x45\x67\x89\xab\xcd\xef76543210"
    operation_handle.operationId.secret = "PasswordIsPencil"
    operation_handle.operationType = TCLIService.TOperationType.EXECUTE_STATEMENT
    operation_handle.hasResultSet = False

    get_operation_status_resp = self.get_operation_status(operation_handle)
    TestHS2.check_response(get_operation_status_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "Invalid query handle: efcdab8967452301:3031323334353637"
    assert err_msg in get_operation_status_resp.status.errorMessage

    get_result_set_metadata_req = TCLIService.TGetResultSetMetadataReq()
    get_result_set_metadata_req.operationHandle = operation_handle
    get_result_set_metadata_resp = \
        self.hs2_client.GetResultSetMetadata(get_result_set_metadata_req)
    TestHS2.check_response(get_result_set_metadata_resp, \
        TCLIService.TStatusCode.ERROR_STATUS)

    err_msg = "Invalid query handle: efcdab8967452301:3031323334353637"
    assert err_msg in get_result_set_metadata_resp.status.errorMessage

  @pytest.mark.execute_serially
  def test_multiple_sessions(self):
    """Test that multiple sessions on the same socket connection are allowed"""
    num_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")
    session_ids = []
    for _ in xrange(5):
      open_session_req = TCLIService.TOpenSessionReq()
      resp = self.hs2_client.OpenSession(open_session_req)
      TestHS2.check_response(resp)
      # Check that all sessions get different IDs
      assert resp.sessionHandle not in session_ids
      session_ids.append(resp.sessionHandle)

    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions + 5)

    for session_id in session_ids:
      close_session_req = TCLIService.TCloseSessionReq(session_id)
      resp = self.hs2_client.CloseSession(close_session_req)
      TestHS2.check_response(resp)

    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions)

  @needs_session()
  def test_get_info(self):
    # Negative test for invalid session secret.
    invalid_req = TCLIService.TGetInfoReq(create_session_handle_without_secret(
        self.session_handle), TCLIService.TGetInfoType.CLI_DBMS_NAME)
    TestHS2.check_invalid_session(self.hs2_client.GetInfo(invalid_req))

    # TODO: it would be useful to add positive tests for GetInfo().

  @needs_session()
  def test_get_schemas(self):
    get_schemas_req = TCLIService.TGetSchemasReq()
    get_schemas_req.sessionHandle = self.session_handle
    get_schemas_resp = self.hs2_client.GetSchemas(get_schemas_req)
    TestHS2.check_response(get_schemas_resp)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = get_schemas_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    query_id = operation_id_to_query_id(get_schemas_resp.operationHandle.operationId)
    profile_page = self.impalad_test_service.read_query_profile_page(query_id)

    # Test fix for IMPALA-619
    assert "Sql Statement: GET_SCHEMAS" in profile_page
    assert "Query Type: DDL" in profile_page

    # Test that session secret is validated by this API.
    get_schemas_req.sessionHandle = create_session_handle_without_secret(
        self.session_handle)
    TestHS2.check_invalid_session(self.hs2_client.GetSchemas(get_schemas_req))

  @pytest.mark.execute_serially
  @needs_session()
  def test_get_tables(self):
    """Basic test for the GetTables() HS2 method. Needs to execute serially because
    the test depends on controlling whether a table is loaded or not and other
    concurrent tests loading or invalidating tables could interfere with it."""
    # TODO: unique_database would be better, but it doesn't work with @needs_session
    # at the moment.
    table = "__hs2_column_comments_test"
    self.execute_query("drop table if exists {0}".format(table))
    self.execute_query("""
        create table {0} (a int comment 'column comment')
        comment 'table comment'""".format(table))
    try:
      req = TCLIService.TGetTablesReq()
      req.sessionHandle = self.session_handle
      req.schemaName = "default"
      req.tableName = table

      # Execute the request twice, the first time with the table unloaded and the second
      # with it loaded.
      self.execute_query("invalidate metadata {0}".format(table))
      for i in range(2):
        get_tables_resp = self.hs2_client.GetTables(req)
        TestHS2.check_response(get_tables_resp)

        fetch_results_resp = self._fetch_results(get_tables_resp.operationHandle, 100)
        results = fetch_results_resp.results
        table_cat = results.columns[0].stringVal.values[0]
        table_schema = results.columns[1].stringVal.values[0]
        table_name = results.columns[2].stringVal.values[0]
        table_type = results.columns[3].stringVal.values[0]
        table_remarks = results.columns[4].stringVal.values[0]
        assert table_cat == ''
        assert table_schema == "default"
        assert table_name == table
        assert table_type == "TABLE"
        if (i == 0 and
              not ImpalaTestClusterProperties.get_instance().is_catalog_v2_cluster()):
          # IMPALA-7587: comments not returned for non-loaded tables with legacy catalog.
          assert table_remarks == ""
        else:
          assert table_remarks == "table comment"
        # Ensure the table is loaded for the second iteration.
        self.execute_query("describe {0}".format(table))

      # Test that session secret is validated by this API.
      invalid_req = TCLIService.TGetTablesReq()
      invalid_req.sessionHandle = create_session_handle_without_secret(
          self.session_handle)
      invalid_req.schemaName = "default"
      invalid_req.tableName = table
      TestHS2.check_invalid_session(self.hs2_client.GetTables(invalid_req))
    finally:
      self.execute_query("drop table {0}".format(table))

  @needs_session(conf_overlay={"idle_session_timeout": "5"})
  def test_get_operation_status_session_timeout(self):
    """Regression test for IMPALA-4488: GetOperationStatus() would not keep a session
    alive"""
    # Choose a long-running query so that it can't finish before the session timeout.
    statement = """select * from functional.alltypes a
    join functional.alltypes b join functional.alltypes c"""
    execute_statement_resp = self.execute_statement(statement)

    now = time.time()
    # Loop until the session would be timed-out if IMPALA-4488 had not been fixed.
    while time.time() - now < 10:
      get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
      # Will fail if session has timed out.
      TestHS2.check_response(get_operation_status_resp)
      time.sleep(0.1)

  def get_log(self, query_stmt):
    execute_statement_resp = self.execute_statement(query_stmt)

    # Fetch results to make sure errors are generated. Errors are only guaranteed to be
    # seen by the coordinator after FetchResults() returns eos.
    has_more_results = True
    while has_more_results:
      fetch_results_req = TCLIService.TFetchResultsReq()
      fetch_results_req.operationHandle = execute_statement_resp.operationHandle
      fetch_results_req.maxRows = 100
      fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
      TestHS2.check_response(fetch_results_resp)
      has_more_results = fetch_results_resp.hasMoreRows

    # Test that secret is validated.
    invalid_get_log_req = TCLIService.TGetLogReq()
    invalid_get_log_req.operationHandle = create_op_handle_without_secret(
        execute_statement_resp.operationHandle)
    TestHS2.check_invalid_query(self.hs2_client.GetLog(invalid_get_log_req),
        expect_legacy_err=True)

    get_log_req = TCLIService.TGetLogReq()
    get_log_req.operationHandle = execute_statement_resp.operationHandle
    get_log_resp = self.hs2_client.GetLog(get_log_req)
    TestHS2.check_response(get_log_resp)
    return get_log_resp.log

  @needs_session()
  def test_get_log(self):
    # Test query that generates BE warnings
    log = self.get_log("select * from functional.alltypeserror")
    assert "Error converting column" in log

    log = self.get_log("select base64decode('foo')")
    assert "Invalid base64 string; input length is 3, which is not a multiple of 4" in log

  @needs_session()
  def test_get_exec_summary(self):
    statement = "SELECT COUNT(1) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    exec_summary_req = ImpalaHiveServer2Service.TGetExecSummaryReq()
    exec_summary_req.operationHandle = execute_statement_resp.operationHandle
    exec_summary_req.sessionHandle = self.session_handle
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)

    # Test getting the summary while query is running. We can't verify anything
    # about the summary (depends how much progress query has made) but the call
    # should work.
    TestHS2.check_response(exec_summary_resp)

    # Wait for query to start running so we can get a non-empty ExecSummary.
    self.wait_for_admission_control(execute_statement_resp.operationHandle)
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)
    TestHS2.check_response(exec_summary_resp)
    assert len(exec_summary_resp.summary.nodes) > 0

    # Test that session secret is validated. Note that operation secret does not need to
    # be validated in addition if the session secret is valid and the operation belongs
    # to the session, because the user has full access to the session.
    TestHS2.check_invalid_session(self.hs2_client.GetExecSummary(
      ImpalaHiveServer2Service.TGetExecSummaryReq(execute_statement_resp.operationHandle,
        create_session_handle_without_secret(self.session_handle))))

    # Attempt to access query with different user should fail.
    evil_user = getuser() + "_evil_twin"
    session_handle2 = self._open_extra_session(evil_user)
    TestHS2.check_profile_access_denied(self.hs2_client.GetExecSummary(
      ImpalaHiveServer2Service.TGetExecSummaryReq(execute_statement_resp.operationHandle,
        session_handle2)), user=evil_user)

    # Now close the query and verify the exec summary is available.
    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    # Attempt to access query with different user from log should fail.
    TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
      ImpalaHiveServer2Service.TGetRuntimeProfileReq(
        execute_statement_resp.operationHandle, session_handle2)),
      user=evil_user)

    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)
    TestHS2.check_response(exec_summary_resp)
    assert len(exec_summary_resp.summary.nodes) > 0

  @needs_session()
  def test_get_profile(self):
    statement = "SELECT COUNT(2) FROM functional.alltypes"
    execute_statement_resp = self.execute_statement(statement)

    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    # If ExecuteStatement() has completed but the results haven't been fetched yet, the
    # query must have reached either COMPILED or RUNNING or FINISHED.
    assert "Query State: COMPILED" in get_profile_resp.profile or \
        "Query State: RUNNING" in get_profile_resp.profile or \
        "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)

    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    # After fetching the results, we must be in state FINISHED.
    assert "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

    # Test that session secret is validated. Note that operation secret does not need to
    # be validated in addition if the session secret is valid and the operation belongs
    # to the session, because the user has full access to the session.
    TestHS2.check_invalid_session(self.hs2_client.GetRuntimeProfile(
      ImpalaHiveServer2Service.TGetRuntimeProfileReq(
        execute_statement_resp.operationHandle,
        create_session_handle_without_secret(self.session_handle))))

    # Attempt to access query with different user should fail.
    evil_user = getuser() + "_evil_twin"
    session_handle2 = self._open_extra_session(evil_user)
    TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
      ImpalaHiveServer2Service.TGetRuntimeProfileReq(
        execute_statement_resp.operationHandle, session_handle2)),
        user=evil_user)

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    # Attempt to access query with different user from log should fail.
    TestHS2.check_profile_access_denied(self.hs2_client.GetRuntimeProfile(
      ImpalaHiveServer2Service.TGetRuntimeProfileReq(
        execute_statement_resp.operationHandle, session_handle2)),
        user=evil_user)

    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert statement in get_profile_resp.profile
    assert "Query State: FINISHED" in get_profile_resp.profile, get_profile_resp.profile

  @needs_session(conf_overlay={"use:database": "functional"})
  def test_change_default_database(self):
    statement = "SELECT 1 FROM alltypes LIMIT 1"
    # Will fail if there's no table called 'alltypes' in the database
    self.execute_statement(statement)

  @needs_session(conf_overlay={"use:database": "FUNCTIONAL"})
  def test_change_default_database_case_insensitive(self):
    statement = "SELECT 1 FROM alltypes LIMIT 1"
    # Will fail if there's no table called 'alltypes' in the database
    self.execute_statement(statement)

  @needs_session(conf_overlay={"use:database": "doesnt-exist"})
  def test_bad_default_database(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT 1 FROM alltypes LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp, TCLIService.TStatusCode.ERROR_STATUS)

  @needs_session()
  def test_get_type_info(self):
    get_type_info_req = TCLIService.TGetTypeInfoReq()
    get_type_info_req.sessionHandle = self.session_handle
    get_type_info_resp = self.hs2_client.GetTypeInfo(get_type_info_req)
    TestHS2.check_response(get_type_info_resp)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = get_type_info_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    results = fetch_results_resp.results
    types = ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'DATE',
             'TIMESTAMP', 'STRING', 'VARCHAR', 'DECIMAL', 'CHAR', 'ARRAY', 'MAP',
             'STRUCT']
    assert self.get_num_rows(results) == len(types)
    # Validate that each type description (result row) has the required 18 fields as
    # described in the DatabaseMetaData.getTypeInfo() documentation.
    assert len(results.columns) == 18
    typed_col = getattr(results.columns[0], 'stringVal')
    for colType in types:
      assert typed_col.values.count(colType) == 1

    # Test that session secret is validated by this API.
    invalid_req = TCLIService.TGetTypeInfoReq(
        create_session_handle_without_secret(self.session_handle))
    TestHS2.check_invalid_session(self.hs2_client.GetTypeInfo(invalid_req))

  def _fetch_results(self, operation_handle, max_rows):
    """Fetch results from 'operation_handle' with up to 'max_rows' rows using
    self.hs2_client, returning the TFetchResultsResp object."""
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = operation_handle
    fetch_results_req.maxRows = max_rows
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)
    return fetch_results_resp

  def _open_extra_session(self, user_name):
    """Open an extra session with the provided username that will be automatically
    closed at the end of the test. Returns the session handle."""
    resp = self.hs2_client.OpenSession(TCLIService.TOpenSessionReq(username=user_name))
    TestHS2.check_response(resp)
    return resp.sessionHandle

  def test_close_connection(self):
    """Tests that an hs2 session remains valid even after the connection is dropped."""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    self.session_handle = open_session_resp.sessionHandle
    # Ren a query, which should succeed.
    self.execute_statement("select 1")

    # Reset the connection.
    self.teardown()
    self.setup()

    # Run another query with the same session handle. It should succeed even though it's
    # on a new connection, since disconnected_session_timeout (default of 1 hour) will not
    # have been hit.
    self.execute_statement("select 2")

    # Close the session.
    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = self.session_handle
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))

    # Run another query, which should fail since the session is closed.
    self.execute_statement("select 3", expected_error_prefix="Invalid session id",
        expected_status_code=TCLIService.TStatusCode.ERROR_STATUS)
