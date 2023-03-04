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
# Superclass of all HS2 tests containing commonly used functions.

from __future__ import absolute_import, division, print_function
from builtins import range
from getpass import getuser
from TCLIService import TCLIService
from ImpalaService import ImpalaHiveServer2Service
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.impala_test_suite import ImpalaTestSuite, IMPALAD_HS2_HOST_PORT
from time import sleep, time


def add_session_helper(self, protocol_version, conf_overlay, close_session, fn):
  """Helper function used in the various needs_session decorators before to set up
  a session, call fn(), then optionally tear down the session."""
  open_session_req = TCLIService.TOpenSessionReq()
  open_session_req.username = getuser()
  open_session_req.configuration = dict()
  if conf_overlay is not None:
    open_session_req.configuration = conf_overlay
  open_session_req.client_protocol = protocol_version
  resp = self.hs2_client.OpenSession(open_session_req)
  HS2TestSuite.check_response(resp)
  self.session_handle = resp.sessionHandle
  assert protocol_version <= resp.serverProtocolVersion
  try:
    fn()
  finally:
    if close_session:
      close_session_req = TCLIService.TCloseSessionReq()
      close_session_req.sessionHandle = resp.sessionHandle
      HS2TestSuite.check_response(self.hs2_client.CloseSession(close_session_req))
    self.session_handle = None

def needs_session(protocol_version=
                  TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                  conf_overlay=None,
                  close_session=True,
                  cluster_properties=None):
  def session_decorator(fn):
    """Decorator that establishes a session and sets self.session_handle. When the test is
    finished, the session is closed.
    """
    def add_session(self):
      add_session_helper(self, protocol_version, conf_overlay, close_session,
          lambda: fn(self))
    return add_session

  return session_decorator


# same as needs_session but takes in a cluster_properties as a argument
# cluster_properties is defined as a fixture in conftest.py which allows us
# to pass it as an argument to a test. However, it does not work well with
# decorators without installing new modules.
# Ref: https://stackoverflow.com/questions/19614658
def needs_session_cluster_properties(protocol_version=
                  TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                  conf_overlay=None,
                  close_session=True):
  def session_decorator(fn):
    """Decorator that establishes a session and sets self.session_handle. When the test is
    finished, the session is closed.
    """
    def add_session(self, cluster_properties, unique_database):
      add_session_helper(self, protocol_version, conf_overlay, close_session,
          lambda: fn(self, cluster_properties, unique_database))
    return add_session

  return session_decorator

def operation_id_to_query_id(operation_id):
  lo, hi = operation_id.guid[:8],  operation_id.guid[8:]
  lo = ''.join(['%0.2X' % ord(c) for c in lo[::-1]])
  hi = ''.join(['%0.2X' % ord(c) for c in hi[::-1]])
  return "%s:%s" % (lo, hi)


def create_session_handle_without_secret(session_handle):
  """Create a HS2 session handle with the same session ID as 'session_handle' but a
  bogus secret of the right length, i.e. 16 bytes."""
  return TCLIService.TSessionHandle(TCLIService.THandleIdentifier(
      session_handle.sessionId.guid, r"xxxxxxxxxxxxxxxx"))


def create_op_handle_without_secret(op_handle):
  """Create a HS2 operation handle with same parameters as 'op_handle' but with a bogus
  secret of the right length, i.e. 16 bytes."""
  op_id = TCLIService.THandleIdentifier(op_handle.operationId.guid, r"xxxxxxxxxxxxxxxx")
  return TCLIService.TOperationHandle(
      op_id, op_handle.operationType, op_handle.hasResultSet)


class HS2TestSuite(ImpalaTestSuite):
  HS2_V6_COLUMN_TYPES = ['boolVal', 'stringVal', 'byteVal', 'i16Val', 'i32Val', 'i64Val',
                         'doubleVal', 'binaryVal']

  def setup(self):
    self.socket, self.hs2_client = self._open_hs2_connection()

  def teardown(self):
    if self.socket:
      self.socket.close()

  @staticmethod
  def _open_hs2_connection():
    """Opens a HS2 connection, returning the socket and the thrift client."""
    host, port = IMPALAD_HS2_HOST_PORT.split(":")
    socket = TSocket(host, port)
    transport = TBufferedTransport(socket)
    transport.open()
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    hs2_client = ImpalaHiveServer2Service.Client(protocol)
    return socket, hs2_client

  @staticmethod
  def check_response(response,
                     expected_status_code = TCLIService.TStatusCode.SUCCESS_STATUS,
                     expected_error_prefix = None):
    assert response.status.statusCode == expected_status_code
    if expected_status_code != TCLIService.TStatusCode.SUCCESS_STATUS\
       and expected_error_prefix is not None:
      assert response.status.errorMessage.startswith(expected_error_prefix)

  @staticmethod
  def check_invalid_session(response):
    """Checks that the HS2 API response is the correct response if the session is invalid,
    i.e. the session doesn't exist or the secret is invalid."""
    HS2TestSuite.check_response(response, TCLIService.TStatusCode.ERROR_STATUS,
                                "Invalid session id:")

  @staticmethod
  def check_invalid_query(response, expect_legacy_err=False):
    """Checks that the HS2 API response is the correct response if the query is invalid,
    i.e. the query doesn't exist, doesn't match the session provided, or the secret is
    invalid. """
    if expect_legacy_err:
      # Some operations return non-standard errors like "Query id ... not found".
      expected_err = "Query id"
    else:
      # We should standardise on this error message.
      expected_err = "Invalid or unknown query handle:"
    HS2TestSuite.check_response(response, TCLIService.TStatusCode.ERROR_STATUS,
                                expected_err)

  @staticmethod
  def check_profile_access_denied(response, user):
    """Checks that the HS2 API response is the correct response if the user is not
    authorised to access the query's profile."""
    HS2TestSuite.check_response(response, TCLIService.TStatusCode.ERROR_STATUS,
                                "User {0} is not authorized to access the runtime "
                                "profile or execution summary".format(user))

  def close(self, op_handle):
    close_op_req = TCLIService.TCloseOperationReq()
    close_op_req.operationHandle = op_handle
    close_op_resp = self.hs2_client.CloseOperation(close_op_req)
    assert close_op_resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS

  @staticmethod
  def get_num_rows(result_set):
    # rows will always be set, so the only way to tell if we should use it is to see if
    # any columns are set
    if result_set.columns is None or len(result_set.columns) == 0:
      return len(result_set.rows)

    assert result_set.columns is not None
    for col_type in HS2TestSuite.HS2_V6_COLUMN_TYPES:
      typed_col = getattr(result_set.columns[0], col_type)
      if typed_col != None:
        return len(typed_col.values)

    assert False

  def fetch(self, fetch_results_req):
    """Wrapper around ImpalaHiveServer2Service.FetchResults(fetch_results_req) that
    issues the given fetch request until the TCLIService.TStatusCode transitions from
    STILL_EXECUTING_STATUS to SUCCESS_STATUS. If a fetch response contains the
    STILL_EXECUTING_STATUS then rows are not yet available for consumption (e.g. the
    query is still running and has not produced any rows yet). This status may be
    returned to the client if the FETCH_ROWS_TIMEOUT_MS is hit."""
    fetch_results_resp = None
    while fetch_results_resp is None or \
        fetch_results_resp.status.statusCode == \
          TCLIService.TStatusCode.STILL_EXECUTING_STATUS:
      fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)
    return fetch_results_resp

  def fetch_at_most(self, handle, orientation, size, expected_num_rows = None):
    """Fetches at most size number of rows from the query identified by the given
    operation handle. Uses the given fetch orientation. Asserts that the fetch returns a
    success status, and that the number of rows returned is equal to given
    expected_num_rows (if given). It is only safe for expected_num_rows to be 0 or 1:
    Impala does not guarantee that a larger result set will be returned in one go. Use
    fetch_until() for repeated fetches."""
    assert expected_num_rows is None or expected_num_rows in (0, 1)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = handle
    fetch_results_req.orientation = orientation
    fetch_results_req.maxRows = size
    fetch_results_resp = self.fetch(fetch_results_req)
    if expected_num_rows is not None:
      assert self.get_num_rows(fetch_results_resp.results) == expected_num_rows
    return fetch_results_resp

  def fetch_until(self, handle, orientation, size, expected_num_rows = None):
    """Tries to fetch exactly 'size' rows from the given query handle, with the given
    fetch orientation, by repeatedly issuing fetch(size - num rows already fetched)
    calls. Returns fewer than 'size' rows if either a fetch() returns 0 rows (indicating
    EOS) or 'expected_num_rows' rows are returned. If 'expected_num_rows' is set to None,
    it defaults to 'size', so that the effect is to both ask for and expect the same
    number of rows."""
    assert expected_num_rows is None or (size >= expected_num_rows)
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = handle
    fetch_results_req.orientation = orientation
    fetch_results_req.maxRows = size
    fetch_results_resp = self.fetch(fetch_results_req)
    num_rows_fetched = self.get_num_rows(fetch_results_resp.results)
    if expected_num_rows is None: expected_num_rows = size
    while num_rows_fetched < expected_num_rows:
      # Always try to fetch at most 'size'
      fetch_results_req.maxRows = size - num_rows_fetched
      fetch_results_req.orientation = TCLIService.TFetchOrientation.FETCH_NEXT
      fetch_results_resp = self.fetch(fetch_results_req)
      last_fetch_size = self.get_num_rows(fetch_results_resp.results)
      assert last_fetch_size > 0
      num_rows_fetched += last_fetch_size

    assert num_rows_fetched == expected_num_rows

  def fetch_fail(self, handle, orientation, expected_error_prefix):
    """Attempts to fetch rows from the query identified by the given operation handle.
    Asserts that the fetch returns an error with an error message matching the given
    expected_error_prefix."""
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = handle
    fetch_results_req.orientation = orientation
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp, TCLIService.TStatusCode.ERROR_STATUS,
                                expected_error_prefix)
    return fetch_results_resp

  def result_metadata(self, handle):
    """ Gets the schema for the query identified by the handle """
    req = TCLIService.TGetResultSetMetadataReq()
    req.operationHandle = handle
    resp = self.hs2_client.GetResultSetMetadata(req)
    HS2TestSuite.check_response(resp)
    return resp

  def column_results_to_string(self, columns):
    """Quick-and-dirty way to get a readable string to compare the output of a
    columnar-oriented query to its expected output"""
    formatted = ""
    num_rows = 0
    # Determine the number of rows by finding the type of the first column
    for col_type in HS2TestSuite.HS2_V6_COLUMN_TYPES:
      typed_col = getattr(columns[0], col_type)
      if typed_col != None:
        num_rows = len(typed_col.values)
        break

    for i in range(num_rows):
      row = []
      for c in columns:
        for col_type in HS2TestSuite.HS2_V6_COLUMN_TYPES:
          typed_col = getattr(c, col_type)
          if typed_col != None:
            indicator = ord(typed_col.nulls[i // 8])
            if indicator & (1 << (i % 8)):
              row.append("NULL")
            else:
              row.append(str(typed_col.values[i]))
            break
      formatted += (", ".join(row) + "\n")
    return (num_rows, formatted)

  def get_operation_status(self, operation_handle):
    """Executes GetOperationStatus with the given operation handle and returns the
    TGetOperationStatusResp"""
    get_operation_status_req = TCLIService.TGetOperationStatusReq()
    get_operation_status_req.operationHandle = operation_handle
    get_operation_status_resp = \
        self.hs2_client.GetOperationStatus(get_operation_status_req)
    return get_operation_status_resp

  def wait_for_operation_state(self, operation_handle, expected_state, \
                               timeout = 10, interval = 1):
    """Waits for the operation to reach expected_state by polling GetOperationStatus every
    interval seconds, returning the TGetOperationStatusResp, or raising an assertion after
    timeout seconds."""
    start_time = time()
    while (time() - start_time < timeout):
      get_operation_status_resp = self.get_operation_status(operation_handle)
      HS2TestSuite.check_response(get_operation_status_resp)
      if get_operation_status_resp.operationState is expected_state:
        return get_operation_status_resp
      sleep(interval)
    assert False, 'Did not reach expected operation state %s in time, actual state was ' \
        '%s' % (expected_state, get_operation_status_resp.operationState)

  def wait_for_admission_control(self, operation_handle, timeout = 10):
    """Waits for the admission control processing of the query to complete by polling
      GetOperationStatus every interval seconds, returning the TGetOperationStatusResp,
      or raising an assertion after timeout seconds."""
    start_time = time()
    while (time() - start_time < timeout):
      get_operation_status_resp = self.get_operation_status(operation_handle)
      HS2TestSuite.check_response(get_operation_status_resp)
      if TCLIService.TOperationState.INITIALIZED_STATE < \
          get_operation_status_resp.operationState < \
          TCLIService.TOperationState.PENDING_STATE:
        return get_operation_status_resp
      sleep(0.05)
    assert False, 'Did not complete admission control processing in time, current ' \
        'operation state of query: %s' % (get_operation_status_resp.operationState)

  def wait_for_log_message(self, operationHandle, expected_message, timeout=10):
    start_time = time()
    while (time() - start_time < timeout):
      get_log_req = TCLIService.TGetLogReq()
      get_log_req.operationHandle = operationHandle
      log = self.hs2_client.GetLog(get_log_req).log
      if expected_message in log:
        return log
      sleep(0.05)
    assert False, "Did not find expected log message '%s' in time, latest log: '%s'" \
      % (expected_message, log)

  def execute_statement(self, statement, conf_overlay=None,
                        expected_status_code=TCLIService.TStatusCode.SUCCESS_STATUS,
                        expected_error_prefix=None):
    """Executes statement and checks if the response meets the expectations.
    If so, it returns the response."""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = statement
    if conf_overlay:
      execute_statement_req.confOverlay = conf_overlay
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp, expected_status_code,
                                expected_error_prefix)
    return execute_statement_resp
