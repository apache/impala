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

import json
import pytest
import time

from urllib2 import urlopen

from ImpalaService import ImpalaHiveServer2Service
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session, operation_id_to_query_id
from TCLIService import TCLIService

SQLSTATE_GENERAL_ERROR = "HY000"

class TestHS2(HS2TestSuite):
  def test_open_session(self):
    """Check that a session can be opened"""
    open_session_req = TCLIService.TOpenSessionReq()
    TestHS2.check_response(self.hs2_client.OpenSession(open_session_req))

  def test_open_sesssion_query_options(self):
    """Check that OpenSession sets query options"""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.configuration = {'MAX_ERRORS': '45678',
        'NUM_NODES': '1234', 'MAX_NUM_RUNTIME_FILTERS': '333'}
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(open_session_resp)
    for k, v in open_session_req.configuration.items():
      assert open_session_resp.configuration[k] == v

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
    TestHS2.check_response(self.hs2_client.CloseSession(close_session_req),
                           TCLIService.TStatusCode.ERROR_STATUS)

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
      sleep_period = timeout * 1.5
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

  @needs_session()
  def test_get_operation_status(self):
    """Tests that GetOperationStatus returns a valid result for a running query"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
    TestHS2.check_response(get_operation_status_resp)

    assert get_operation_status_resp.operationState in \
        [TCLIService.TOperationState.INITIALIZED_STATE,
         TCLIService.TOperationState.RUNNING_STATE,
         TCLIService.TOperationState.FINISHED_STATE]

  @needs_session(conf_overlay={"abort_on_error": "1"})
  def test_get_operation_status_error(self):
    """Tests that GetOperationStatus returns a valid result for a query that encountered
    an error"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT * FROM functional.alltypeserror"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

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

    print get_operation_status_resp.status.errorMessage
    err_msg = "Invalid query handle: efcdab8967452301:3031323334353637"
    assert err_msg in get_operation_status_resp.status.errorMessage

  @pytest.mark.execute_serially
  def test_socket_close_forces_session_close(self):
    """Test that closing the underlying socket forces the associated session to close.
    See IMPALA-564"""
    open_session_req = TCLIService.TOpenSessionReq()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)
    num_sessions = self.impalad_test_service.get_metric_value(
        "impala-server.num-open-hiveserver2-sessions")

    assert num_sessions > 0

    self.socket.close()
    self.socket = None
    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions - 1)

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

    self.socket.close()
    self.socket = None
    self.impalad_test_service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", num_sessions)

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

  @needs_session(conf_overlay={"idle_session_timeout": "5"})
  def test_get_operation_status_session_timeout(self):
    """Regression test for IMPALA-4488: GetOperationStatus() would not keep a session
    alive"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    # Choose a long-running query so that it can't finish before the session timeout.
    execute_statement_req.statement = """select * from functional.alltypes a
    join functional.alltypes b join functional.alltypes c"""
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    now = time.time()
    # Loop until the session would be timed-out if IMPALA-4488 had not been fixed.
    while time.time() - now < 10:
      get_operation_status_resp = \
        self.get_operation_status(execute_statement_resp.operationHandle)
      # Will fail if session has timed out.
      TestHS2.check_response(get_operation_status_resp)
      time.sleep(0.1)

  def get_log(self, query_stmt):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = query_stmt
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

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

    # Test overflow warning
    log = self.get_log("select cast(1000 as decimal(2, 1))")
    assert "Expression overflowed, returning NULL" in log

  @needs_session()
  def test_get_exec_summary(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(1) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    exec_summary_req = ImpalaHiveServer2Service.TGetExecSummaryReq()
    exec_summary_req.operationHandle = execute_statement_resp.operationHandle
    exec_summary_req.sessionHandle = self.session_handle
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)

    # Test getting the summary while query is running. We can't verify anything
    # about the summary (depends how much progress query has made) but the call
    # should work.
    TestHS2.check_response(exec_summary_resp)

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)
    TestHS2.check_response(exec_summary_resp)
    assert len(exec_summary_resp.summary.nodes) > 0

  @needs_session()
  def test_get_profile(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(2) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)
    assert execute_statement_req.statement in get_profile_resp.profile

    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = execute_statement_resp.operationHandle
    TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)
    TestHS2.check_response(get_profile_resp)

    assert execute_statement_req.statement in get_profile_resp.profile

  @needs_session(conf_overlay={"use:database": "functional"})
  def test_change_default_database(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT 1 FROM alltypes LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    # Will fail if there's no table called 'alltypes' in the database
    TestHS2.check_response(execute_statement_resp)

  @needs_session(conf_overlay={"use:database": "FUNCTIONAL"})
  def test_change_default_database_case_insensitive(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT 1 FROM alltypes LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    # Will fail if there's no table called 'alltypes' in the database
    TestHS2.check_response(execute_statement_resp)

  @needs_session(conf_overlay={"use:database": "doesnt-exist"})
  def test_bad_default_database(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT 1 FROM alltypes LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp, TCLIService.TStatusCode.ERROR_STATUS)
