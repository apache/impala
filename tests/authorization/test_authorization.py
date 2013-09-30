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
# Client tests for SQL statement authorization

import os
import pytest
import json
from tests.hs2.test_hs2 import *
from time import sleep
from getpass import getuser
from cli_service import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException
from tests.common.impala_test_suite import ImpalaTestSuite, IMPALAD_HS2_HOST_PORT

class TestAuthorization(TestHS2):
  def test_impersonation(self):
    """End-to-end impersonation + authorization test. Expects authorization to be
    configured before running this test"""
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = 'hue'
    open_session_req.configuration = dict()
    open_session_req.configuration['impala.doas.user'] = getuser()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Try to query a table we are not authorized to access.
    self.session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch_seq.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    assert 'User \'%s\' does not have privileges to access' % getuser() in\
        str(execute_statement_resp)

    # Now try the same operation on a table we are authorized to access.
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    # Try to impersonate as a user we are not authorized to impersonate.
    open_session_req.configuration['impala.doas.user'] = 'some_user'
    resp = self.hs2_client.OpenSession(open_session_req)
    assert 'User \'hue\' is not authorized to impersonate \'some_user\'' in str(resp)

    self.socket.close()
    self.socket = None
