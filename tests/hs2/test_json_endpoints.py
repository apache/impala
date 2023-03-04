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
# Tests for query expiration.

from __future__ import absolute_import, division, print_function
import json
import pytest
from time import time

try:
  from urllib.request import urlopen
except ImportError:
  from urllib2 import urlopen

from tests.common.environ import IS_DOCKERIZED_TEST_CLUSTER
from tests.common.impala_cluster import ImpalaCluster
from tests.hs2.hs2_test_suite import HS2TestSuite
from TCLIService import TCLIService

class TestJsonEndpoints(HS2TestSuite):
  def _get_json_queries(self, http_addr):
    """Get the json output of the /queries page from the impalad web UI at http_addr."""
    if IS_DOCKERIZED_TEST_CLUSTER:
      # The hostnames in the dockerized cluster may not be externally reachable.
      cluster = ImpalaCluster.get_e2e_test_cluster()
      return cluster.impalads[0].service.get_debug_webpage_json("/queries")
    else:
      resp = urlopen("http://%s/queries?json" % http_addr)
      assert resp.msg == 'OK'
      return json.loads(resp.read())

  @pytest.mark.execute_serially
  def test_waiting_in_flight_queries(self):
    """Confirm that the in_flight_queries endpoint shows a query at eos as waiting"""
    open_session_req = TCLIService.TOpenSessionReq()
    default_database = "functional"
    open_session_req.configuration = {"use:database": default_database}
    open_session_resp = self.hs2_client.OpenSession(open_session_req)
    TestJsonEndpoints.check_response(open_session_resp)
    http_addr = open_session_resp.configuration['http_addr']

    # Execute a SELECT, and check that in_flight_queries shows one executing query.
    select_statement_req = TCLIService.TExecuteStatementReq()
    select_statement_req.sessionHandle = open_session_resp.sessionHandle
    select_statement_req.statement = "SELECT * FROM functional.alltypes LIMIT 0"
    select_statement_resp = self.hs2_client.ExecuteStatement(select_statement_req)
    TestJsonEndpoints.check_response(select_statement_resp)
    queries_json = self._get_json_queries(http_addr)
    assert len(queries_json["in_flight_queries"]) == 1
    assert queries_json["num_in_flight_queries"] == 1
    assert queries_json["num_executing_queries"] == 1
    assert queries_json["num_waiting_queries"] == 0
    query = queries_json["in_flight_queries"][0]
    assert query["default_db"] == default_database
    assert query["stmt"] == select_statement_req.statement
    assert query["stmt_type"] == "QUERY"
    assert query["rows_fetched"] == 0
    assert query["executing"]
    assert not query["waiting"]

    # Fetch the results, putting the query at eos, and check that in_flight_queries
    # shows one waiting query.
    fetch_results_req = TCLIService.TFetchResultsReq()
    fetch_results_req.operationHandle = select_statement_resp.operationHandle
    fetch_results_req.maxRows = 100
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestJsonEndpoints.check_response(fetch_results_resp)
    # Fetch one more time to ensure that query is at EOS (first fetch might return 0-size
    # row batch)
    fetch_results_resp = self.hs2_client.FetchResults(fetch_results_req)
    TestJsonEndpoints.check_response(fetch_results_resp)
    queries_json = self._get_json_queries(http_addr)
    assert len(queries_json["in_flight_queries"]) == 1
    assert queries_json["num_in_flight_queries"] == 1
    assert queries_json["num_executing_queries"] == 0
    assert queries_json["num_waiting_queries"] == 1
    query = queries_json["in_flight_queries"][0]
    assert not query["executing"]
    assert query["waiting"]

    # Close the query and check that in_flight_queries becomes empty when the query
    # gets unregistered.
    close_operation_req = TCLIService.TCloseOperationReq()
    close_operation_req.operationHandle = select_statement_resp.operationHandle
    close_operation_resp = self.hs2_client.CloseOperation(close_operation_req)
    TestJsonEndpoints.check_response(close_operation_resp)

    def no_inflight_queries():
      queries_json = self._get_json_queries(http_addr)
      return len(queries_json["in_flight_queries"]) == 0

    self.assert_eventually(60, 0.1, no_inflight_queries)
    queries_json = self._get_json_queries(http_addr)
    assert len(queries_json["in_flight_queries"]) == 0
    assert queries_json["num_in_flight_queries"] == 0
    assert queries_json["num_executing_queries"] == 0
    assert queries_json["num_waiting_queries"] == 0

    # Close the session so that subsequent tests that rely on an empty session list don't
    # fail.
    close_session_req = TCLIService.TCloseSessionReq()
    close_session_req.sessionHandle = open_session_resp.sessionHandle
    close_session_resp = self.hs2_client.CloseSession(close_session_req)
    TestJsonEndpoints.check_response(close_session_resp)
