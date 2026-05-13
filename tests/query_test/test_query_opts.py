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
# Tests for exercising query options that can be set in various ways.
# TODO: Add custom cluster tests for process default_query_options, but we need
#       to make it easier to handle startup failures (right now it waits 60sec to
#       timeout).

from __future__ import absolute_import, division, print_function

from impala_thrift_gen.TCLIService import TCLIService
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session


class TestQueryOptions(ImpalaTestSuite):

  def test_set_invalid_query_option(self):
    ex = self.execute_query_expect_failure(self.client, "select 1", {'foo':'bar'})
    assert "invalid query option: foo" in str(ex).lower()

class TestQueryOptionsHS2(HS2TestSuite):

  @needs_session()
  def test_set_invalid_query_option(self):
    """Tests that GetOperationStatus returns a valid result for a running query"""
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.confOverlay = {"foo":"bar"}
    execute_statement_req.statement = "select 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestQueryOptionsHS2.check_response(execute_statement_resp,
        TCLIService.TStatusCode.ERROR_STATUS, "Invalid query option: foo")
