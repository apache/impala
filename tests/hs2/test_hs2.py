#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Client tests for Impala's HiveServer2 interface

import os
import pytest
import json
from time import sleep
from getpass import getuser
from cli_service import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from tests.common.impala_test_suite import ImpalaTestSuite, IMPALAD_HS2_HOST_PORT

def needs_session(fn):
  """Decorator that establishes a session and sets self.session_handle. When the test is
  finished, the session is closed.
  """
  def add_session(self):
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = getuser()
    open_session_req.configuration = dict()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)
    self.session_handle = resp.sessionHandle
    try:
      fn(self)
    finally:
      close_session_req = TCLIService.TCloseSessionReq()
      close_session_req.sessionHandle = resp.sessionHandle
      TestHS2.check_response(self.hs2_client.CloseSession(close_session_req))
      self.session_handle = None
  return add_session

def operation_id_to_query_id(operation_id):
  lo, hi = operation_id.guid[:8],  operation_id.guid[8:]
  lo = ''.join(['%0.2X' % ord(c) for c in lo[::-1]])
  hi = ''.join(['%0.2X' % ord(c) for c in hi[::-1]])
  return "%s:%s" % (lo, hi)

class TestHS2(ImpalaTestSuite):
  def setup(self):
    host, port = IMPALAD_HS2_HOST_PORT.split(":")
    self.socket = TSocket(host, port)
    self.transport = TBufferedTransport(self.socket)
    self.transport.open()
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    self.hs2_client = TCLIService.Client(self.protocol)

  def teardown(self):
    if self.socket:
      self.socket.close()

  @staticmethod
  def check_response(response, expected = TCLIService.TStatusCode.SUCCESS_STATUS):
    assert response.status.statusCode == expected

  def test_open_session(self):
    """Check that a session can be opened"""
    open_session_req = TCLIService.TOpenSessionReq()
    TestHS2.check_response(self.hs2_client.OpenSession(open_session_req))

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

  @needs_session
  def test_execute_select(self):
    """Test that a simple select statement works"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestHS2.check_response(fetch_results_resp)

    assert len(fetch_results_resp.results.rows) == 1
    assert fetch_results_resp.results.startRowOffset == 0

    try:
      assert not fetch_results_resp.hasMoreRows
    except AssertionError:
      pytest.xfail("IMPALA-558")

  @needs_session
  def test_get_operation_status(self):
    """Tests that GetOperationStatus returns a valid result for a running query"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "SELECT COUNT(*) FROM functional.alltypes"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    get_operation_status_req = TCLIService.TGetOperationStatusReq()
    get_operation_status_req.operationHandle = execute_statement_resp.operationHandle

    get_operation_status_resp = \
        self.hs2_client.GetOperationStatus(get_operation_status_req)
    TestHS2.check_response(get_operation_status_resp)

    assert get_operation_status_resp.operationState in \
        [TCLIService.TOperationState.INITIALIZED_STATE,
         TCLIService.TOperationState.RUNNING_STATE,
         TCLIService.TOperationState.FINISHED_STATE]

  @needs_session
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

    get_operation_status_req = TCLIService.TGetOperationStatusReq()
    get_operation_status_req.operationHandle = operation_handle

    get_operation_status_resp = \
        self.hs2_client.GetOperationStatus(get_operation_status_req)
    TestHS2.check_response(get_operation_status_resp,
                           TCLIService.TStatusCode.ERROR_STATUS)
    err_msg = "(guid size: %d, expected 16, secret size: %d, expected 16)" \
        % (len(operation_handle.operationId.guid),
           len(operation_handle.operationId.secret))
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

  @needs_session
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
