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
  def __verify_result_precision_scale(self, t, precision, scale):
    # This should be DECIMAL_TYPE but how do I get that in python
    assert t.typeDesc.types[0].primitiveEntry.type == 15
    p = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers['precision']
    s = t.typeDesc.types[0].primitiveEntry.typeQualifiers.qualifiers['scale']
    assert p.i32Value == precision
    assert s.i32Value == scale

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
        "SELECT d1,d5 FROM functional.decimal_tbl ORDER BY d1 LIMIT 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)
    results = self.fetch(execute_statement_resp.operationHandle,
                TCLIService.TFetchOrientation.FETCH_NEXT, 1, 1)
    assert len(results.results.rows) == 1

    # Verify the result schema is what we expect. The result has 2 columns, the
    # first is decimal(9,0) and the second is decimal(10,5)
    metadata_resp = self.result_metadata(execute_statement_resp.operationHandle)
    column_types = metadata_resp.schema.columns
    assert len(column_types) == 2
    self.__verify_result_precision_scale(column_types[0], 9, 0)
    self.__verify_result_precision_scale(column_types[1], 10, 5)

    self.close(execute_statement_resp.operationHandle)

