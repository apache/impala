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

import pytest
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from cli_service import TCLIService

# Simple test to make sure all the HS2 types are supported.
class TestFetch(HS2TestSuite):
  @needs_session
  def test_query_stmts(self):
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle

    execute_statement_req.statement =\
        "SELECT * FROM functional.alltypessmall ORDER BY id LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch(execute_statement_resp.operationHandle,
                TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    self.close(execute_statement_resp.operationHandle)

    execute_statement_req.statement =\
        "SELECT * FROM functional.decimal_tbl ORDER BY d1 LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch(execute_statement_resp.operationHandle,
                TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1
    self.close(execute_statement_resp.operationHandle)
