#!/usr/bin/env python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
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
import getpass
import pytest
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from TCLIService import TCLIService, constants
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

USER_NAME = getpass.getuser()
PROXY_USER = "proxy_user_name"

class TestDelegation(CustomClusterTestSuite, HS2TestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    "--authorized_proxy_user_config=\"%s=%s\"" % (USER_NAME, PROXY_USER))
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                 conf_overlay = {"impala.doas.user": PROXY_USER})
  def test_effective_user(self):
    '''Test that the effective user is correctly set when impala.doas.user is correct, and
    that the effective_user() builtin returns the right thing'''
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.confOverlay = dict()
    execute_statement_req.statement = "SELECT effective_user(), user()";
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    HS2TestSuite.check_response(execute_statement_resp)

    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 1
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    HS2TestSuite.check_response(fetch_results_resp)

    assert self.column_results_to_string(
      fetch_results_resp.results.columns) == (1, "%s, %s\n" % (PROXY_USER, USER_NAME))
