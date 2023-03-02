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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

import pytest

from tests.hs2.hs2_test_suite import HS2TestSuite, operation_id_to_query_id
from time import sleep
from TCLIService import TCLIService


class TestHS2(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    super(TestHS2, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--disconnected_session_timeout=1")
  def test_disconnected_session_timeout(self):
    """Test that a session gets closed if it has no active connections for more than
    disconnected_session_timeout."""
    conn = HS2TestSuite()
    conn.setup()
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_resp = conn.hs2_client.OpenSession(open_session_req)
    HS2TestSuite.check_response(open_session_resp)
    conn.session_handle = open_session_resp.sessionHandle
    # Ren a query, which should succeed.
    conn.execute_statement("select 1")

    # Set up another connection and run a long-running query with the same session.
    conn2 = HS2TestSuite()
    conn2.setup()
    conn2.session_handle = open_session_resp.sessionHandle
    execute_resp = conn2.execute_statement("select sleep(10000)")

    # Close one connection and wait for longer than disconnected_session_timeout. The
    # session should still be available since there's still one active connection.
    conn2.teardown()
    sleep(5)
    conn.execute_statement("select 3")

    # Close the other connection and sleep again. THe session shuold now be closed.
    conn.teardown()
    sleep(5)
    conn.setup()

    # Run another query, which should fail since the session is closed.
    conn.execute_statement("select 2", expected_error_prefix="Invalid session id",
        expected_status_code=TCLIService.TStatusCode.ERROR_STATUS)

    # Check that the query was cancelled correctly.
    query_id = operation_id_to_query_id(execute_resp.operationHandle.operationId)
    status = self.cluster.get_first_impalad().service.get_query_status(query_id)
    assert status == "Session closed because it has no active connections"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=1 "
       "--disconnected_session_timeout=5 --idle_client_poll_period_s=0")
  def test_expire_disconnected_session(self):
    """Test for the interaction between idle_session_timeout and
    disconnected_session_timeout"""
    # Close the default test clients so that they don't expire while the test is running
    # and affect the metric values.
    self.close_impala_clients()
    impalad = self.cluster.get_first_impalad()

    conn = HS2TestSuite()
    conn.setup()
    # Open a session and then close the connection.
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_resp = conn.hs2_client.OpenSession(open_session_req)
    HS2TestSuite.check_response(open_session_resp)
    conn.teardown()

    # The idle session timeout should be hit first, so the session will be expired.
    impalad.service.wait_for_metric_value(
        "impala-server.num-sessions-expired", 1)
    # The session should eventually be closed by the disconnected timeout.
    impalad.service.wait_for_metric_value(
        "impala-server.num-open-hiveserver2-sessions", 0)
