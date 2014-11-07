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
# Superclass of all HS2 tests containing commonly used functions.

from getpass import getuser
from TCLIService import TCLIService
from ImpalaService import ImpalaHiveServer2Service
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.impala_test_suite import ImpalaTestSuite, IMPALAD_HS2_HOST_PORT

def needs_session(protocol_version=
                  TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6):
  def session_decorator(fn):
    """Decorator that establishes a session and sets self.session_handle. When the test is
    finished, the session is closed.
    """
    def add_session(self):
      open_session_req = TCLIService.TOpenSessionReq()
      open_session_req.username = getuser()
      open_session_req.configuration = dict()
      open_session_req.client_protocol = protocol_version
      resp = self.hs2_client.OpenSession(open_session_req)
      HS2TestSuite.check_response(resp)
      self.session_handle = resp.sessionHandle
      assert protocol_version <= resp.serverProtocolVersion
      try:
        fn(self)
      finally:
        close_session_req = TCLIService.TCloseSessionReq()
        close_session_req.sessionHandle = resp.sessionHandle
        HS2TestSuite.check_response(self.hs2_client.CloseSession(close_session_req))
        self.session_handle = None
    return add_session

  return session_decorator

def operation_id_to_query_id(operation_id):
  lo, hi = operation_id.guid[:8],  operation_id.guid[8:]
  lo = ''.join(['%0.2X' % ord(c) for c in lo[::-1]])
  hi = ''.join(['%0.2X' % ord(c) for c in hi[::-1]])
  return "%s:%s" % (lo, hi)

class HS2TestSuite(ImpalaTestSuite):
  TEST_DB = 'hs2_db'

  HS2_V6_COLUMN_TYPES = ['boolVal', 'stringVal', 'byteVal', 'i16Val', 'i32Val', 'i64Val',
                         'doubleVal', 'binaryVal']

  def setup(self):
    self.cleanup_db(self.TEST_DB)
    host, port = IMPALAD_HS2_HOST_PORT.split(":")
    self.socket = TSocket(host, port)
    self.transport = TBufferedTransport(self.socket)
    self.transport.open()
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    self.hs2_client = ImpalaHiveServer2Service.Client(self.protocol)

  def teardown(self):
    self.cleanup_db(self.TEST_DB)
    if self.socket:
      self.socket.close()

  @staticmethod
  def check_response(response,
                     expected_status_code = TCLIService.TStatusCode.SUCCESS_STATUS,
                     expected_error_prefix = None):
    assert response.status.statusCode == expected_status_code
    if expected_status_code != TCLIService.TStatusCode.SUCCESS_STATUS\
       and expected_error_prefix is not None:
      assert response.status.errorMessage.startswith(expected_error_prefix)

  def close(self, op_handle):
    close_op_req = TCLIService.TCloseOperationReq()
    close_op_req.operationHandle = op_handle
    close_op_resp = self.hs2_client.CloseOperation(close_op_req)
    assert close_op_resp.status.statusCode == TCLIService.TStatusCode.SUCCESS_STATUS

  def get_num_rows(self, result_set):
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

  def fetch(self, handle, orientation, size, expected_num_rows = None):
    """Fetches at most size number of rows from the query identified by the given
    operation handle. Uses the given fetch orientation. Asserts that the fetch returns
    a success status, and that the number of rows returned is equal to size, or
    equal to the given expected_num_rows (if one was given)."""
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = handle
    fetch_results_req.orientation = orientation
    fetch_results_req.maxRows = size
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)
    num_rows = size
    if expected_num_rows is not None:
      num_rows = expected_num_rows
    assert self.get_num_rows(fetch_results_resp.results) == num_rows
    return fetch_results_resp

  def fetch_until(self, handle, orientation, size):
    """Tries to fetch exactly 'size' rows from the given query handle, with the given
    fetch orientation. If fewer rows than 'size' are returned by the first fetch, repeated
    fetches are issued until either 0 rows are returned, or the number of rows fetched is
    equal to 'size'"""
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = handle
    fetch_results_req.orientation = orientation
    fetch_results_req.maxRows = size
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)
    num_rows = size
    num_rows_fetched = self.get_num_rows(fetch_results_resp.results)
    while num_rows_fetched < size:
      fetch_results_req.maxRows = size - num_rows_fetched
      fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
      HS2TestSuite.check_response(fetch_results_resp)
      last_fetch_size = self.get_num_rows(fetch_results_resp.results)
      assert last_fetch_size > 0
      num_rows_fetched += last_fetch_size

    assert num_rows_fetched == size

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
