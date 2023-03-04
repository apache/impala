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

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session
from TCLIService import TCLIService
from ImpalaService import ImpalaHiveServer2Service

class TestSetAndUnset(CustomClusterTestSuite, HS2TestSuite):
  """
  Test behavior of SET and UNSET within a session, and how
  SET/UNSET override options configured at the impalad level.
  """
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      default_query_options=[('debug_action', 'custom')])
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6)
  def test_set_and_unset(self):
    """
    Starts Impala cluster with a custom query option, and checks that option
    overlaying works correctly.

    The Beeswax API and the HiveServer2 implementations are slightly different,
    so the same test is run in both contexts.
    """
    # Beeswax API:
    result = self.execute_query_expect_success(self.client, "set all")
    assert "DEBUG_ACTION\tcustom\tDEVELOPMENT" in result.data, "baseline"
    self.execute_query_expect_success(self.client, "set debug_action=hey")
    assert "DEBUG_ACTION\they\tDEVELOPMENT" in \
        self.execute_query_expect_success(self.client, "set all").data, "session override"
    self.execute_query_expect_success(self.client, 'set debug_action=""')
    assert "DEBUG_ACTION\tcustom\tDEVELOPMENT" in \
        self.execute_query_expect_success(self.client, "set all").data, "reset"
    self.execute_query_expect_success(self.client, 'set batch_size=123')
    # Use a "request overlay" to change the option for a specific
    # request within a session. We run a real query and check its
    # runtime profile, as SET shows session options without applying
    # the request overlays to them.
    assert "BATCH_SIZE=100" in self.execute_query_expect_success(self.client, 'select 1',
            query_options=dict(batch_size="100")).runtime_profile, "request overlay"

    # Overlaying an empty string (unset) has no effect; the session option
    # takes hold and the "request overlay" is considered blank.
    assert "BATCH_SIZE=123" in self.execute_query_expect_success(self.client, 'select 1',
            query_options=dict(batch_size="")).runtime_profile, "null request overlay"

    # Same dance, but with HS2:
    assert ("DEBUG_ACTION", "custom") in self.get_set_results(), "baseline"
    self.execute_statement("set debug_action='hey'")
    assert ("DEBUG_ACTION", "hey") in self.get_set_results(), "session override"
    self.execute_statement("set debug_action=''")
    assert ("DEBUG_ACTION", "custom") in self.get_set_results(), "reset"

    # Request Overlay
    self.execute_statement("set batch_size=123")
    execute_statement_resp = self.execute_statement("select 1", conf_overlay=dict(batch_size="100"))
    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    assert "BATCH_SIZE=100" in self.hs2_client.GetRuntimeProfile(get_profile_req).profile

    # Null request overlay
    self.execute_statement("set batch_size=999")
    execute_statement_resp = self.execute_statement("select 1", conf_overlay=dict(batch_size=""))
    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    assert "BATCH_SIZE=999" in self.hs2_client.GetRuntimeProfile(get_profile_req).profile

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      default_query_options=[('debug_action', 'custom')])
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6)
  def test_unset_all(self):
    """
    Starts Impala cluster with a custom query option, and checks unset option
    works correctly.

    The Beeswax API and the HiveServer2 implementations are slightly different,
    so the same test is run in both contexts.
    """
    # Beeswax API:
    result = self.execute_query_expect_success(self.client, "set all")
    assert "DEBUG_ACTION\tcustom\tDEVELOPMENT" in result.data, "baseline"
    self.execute_query_expect_success(self.client, "set debug_action=hey")
    assert "DEBUG_ACTION\they\tDEVELOPMENT" in \
        self.execute_query_expect_success(self.client, "set all").data, "session override"
    self.execute_query_expect_success(self.client, 'unset all')
    assert "DEBUG_ACTION\tcustom\tDEVELOPMENT" in \
        self.execute_query_expect_success(self.client, "set all").data, "unset all"

    # HS2:
    assert ("DEBUG_ACTION", "custom") in self.get_set_results(), "baseline"
    self.execute_statement("set debug_action='hey'")
    assert ("DEBUG_ACTION", "hey") in self.get_set_results(), "session override"
    self.execute_statement("unset all")
    assert ("DEBUG_ACTION", "custom") in self.get_set_results(), "unset all"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--idle_session_timeout=321")
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6)
  def test_set_and_unset_session_timeout(self):
    """
    Starts Impala cluster with idle_session_timeout configured, and checks if
    the SET query displays the correct value.

    The Beeswax API and the HiveServer2 implementations are slightly different,
    so the same test is run in both contexts.
    """
    # Beeswax API:
    # Default value
    result = self.execute_query_expect_success(self.client, "set")
    assert "IDLE_SESSION_TIMEOUT\t321\tREGULAR" in result.data, "baseline"
    # Session override
    self.execute_query_expect_success(self.client, "set idle_session_timeout=123")
    assert "IDLE_SESSION_TIMEOUT\t123\tREGULAR" in \
        self.execute_query_expect_success(self.client, "set").data, "session override"
    self.execute_query_expect_success(self.client, 'set idle_session_timeout=1000')
    assert "IDLE_SESSION_TIMEOUT\t1000\tREGULAR" in \
        self.execute_query_expect_success(self.client, "set").data, "session override"
    self.execute_query_expect_success(self.client, 'set idle_session_timeout=0')
    assert "IDLE_SESSION_TIMEOUT\t0\tREGULAR" in \
        self.execute_query_expect_success(self.client, "set").data, "session override"
    # Unset
    self.execute_query_expect_success(self.client, 'set idle_session_timeout=""')
    assert "IDLE_SESSION_TIMEOUT\t321\tREGULAR" in \
        self.execute_query_expect_success(self.client, "set").data, "reset"
    assert "IDLE_SESSION_TIMEOUT\t321\tREGULAR" in \
        self.execute_query_expect_success(self.client, "set").data, "after errors"

    # Same dance, but with HS2:
    # Default value
    assert ("IDLE_SESSION_TIMEOUT", "321") in self.get_set_results(), "baseline"
    # Session override
    self.execute_statement("set idle_session_timeout=123")
    assert ("IDLE_SESSION_TIMEOUT", "123") in self.get_set_results(), "session override"
    self.execute_statement("set idle_session_timeout=1000")
    assert ("IDLE_SESSION_TIMEOUT", "1000") in self.get_set_results(), "session override"
    self.execute_statement("set idle_session_timeout=0")
    assert ("IDLE_SESSION_TIMEOUT", "0") in self.get_set_results(), "session override"
    # Unset
    self.execute_statement("set idle_session_timeout=''")
    assert ("IDLE_SESSION_TIMEOUT", "321") in self.get_set_results(), "reset"

  def get_set_results(self):
    """
    Executes a "SET" HiveServer2 query and returns a list
    of key-value tuples.
    """
    execute_statement_resp = self.execute_statement("set all")
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = execute_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    return list(zip(fetch_results_resp.results.columns[0].stringVal.values,
            fetch_results_resp.results.columns[1].stringVal.values))
