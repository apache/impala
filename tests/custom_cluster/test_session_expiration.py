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
import pytest
import socket

import re
from time import sleep

from impala.dbapi import connect
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import DEFAULT_HS2_PORT
from tests.util.thrift_util import op_handle_to_query_id


class TestSessionExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""
  PROFILE_PAGE = "http://localhost:{0}/query_profile?query_id={1}&json"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=6 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration(self):
    impalad = self.cluster.get_any_impalad()
    self.close_impala_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
    num_connections = impalad.service.get_metric_value(
        "impala.thrift-server.beeswax-frontend.connections-in-use")
    client = impalad.service.create_hs2_client()
    client.execute('select 1')
    # Sleep for half the expiration time to confirm that the session is not expired early
    # (see IMPALA-838)
    sleep(3)
    assert client is not None
    assert num_expired == impalad.service.get_metric_value(
        "impala-server.num-sessions-expired")
    # Wait for session expiration. Impala will poll the session expiry queue every second
    impalad.service.wait_for_metric_value(
        "impala-server.num-sessions-expired", num_expired + 1, 20)
    # Verify that the idle connection is not closed.
    assert 1 + num_connections == impalad.service.get_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=3 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration_with_set(self):
    impalad = self.cluster.get_any_impalad()
    self.close_impala_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test if we can set a shorter timeout than the process-wide option
    client = impalad.service.create_hs2_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=1")
    sleep(2.5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

    # Test if we can set a longer timeout than the process-wide option
    client = impalad.service.create_hs2_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=10")
    sleep(5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=5 "
       "--idle_client_poll_period_s=0")
  def test_unsetting_session_expiration(self):
    impalad = self.cluster.get_any_impalad()
    self.close_impala_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test unsetting IDLE_SESSION_TIMEOUT
    client = impalad.service.create_hs2_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=1")

    # Unset to 5 sec
    client.execute('SET IDLE_SESSION_TIMEOUT=""')
    sleep(2)
    # client session should be alive at this point
    assert num_expired == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")
    sleep(5)
    # now client should have expired
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

  def _get_fast_timeout_cursor_from_hs2_client(self, connection, idle_session_timeout=3):
    """Get a fast timing out HiveServer2Cursor from a HiveServer2Connection."""
    cursor = connection.cursor()
    # Set disable the trivial query otherwise "select 1" would be admitted as a
    # trivial query.
    cursor.execute('set enable_trivial_query_for_admission=false')
    cursor.execute('set idle_session_timeout={}'.format(idle_session_timeout))
    return cursor

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--default_pool_max_requests=1 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration_with_queued_query(self):
    """Ensure that a query waiting in queue gets cancelled if the session expires."""
    # It is currently not possible to run two successive execute_async within single
    # session using ImpylaHS2Connection. Therefore, we obtain 2 HiveServer2Cursor from
    # HiveServer2Connection instead.
    impalad = self.cluster.get_any_impalad()
    with connect(port=impalad.service.hs2_port) as conn:
      timeout = 3
      debug_cursor = self._get_fast_timeout_cursor_from_hs2_client(conn, timeout)
      queued_cursor = self._get_fast_timeout_cursor_from_hs2_client(conn, timeout)
      debug_cursor.execute_async("select sleep(10000)")
      queued_cursor.execute_async("select 1")
      impalad.service.wait_for_metric_value(
          "admission-controller.local-num-queued.default-pool", 1)
      sleep(timeout)
      impalad.service.wait_for_metric_value(
          "admission-controller.local-num-queued.default-pool", 0)
      impalad.service.wait_for_metric_value(
          "admission-controller.agg-num-running.default-pool", 0)
      queued_query_id = op_handle_to_query_id(queued_cursor._last_operation.handle)
      assert queued_query_id is not None
      json_summary = self.get_debug_page(
          self.PROFILE_PAGE.format(impalad.service.webserver_port, queued_query_id))
      assert "Admission result: Cancelled (queued)" in json_summary['profile']

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--idle_session_timeout=10 "
      "--idle_client_poll_period_s=1", cluster_size=1)
  def test_closing_idle_connection(self):
    """ IMPALA-7802: verifies that connections of idle sessions are closed
    after the sessions have expired."""
    impalad = self.cluster.get_any_impalad()
    self.close_impala_clients()

    for protocol in ['beeswax', 'hiveserver2']:
      num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
      num_connections_metrics_name = \
          "impala.thrift-server.{0}-frontend.connections-in-use".format(protocol)
      num_connections = impalad.service.get_metric_value(num_connections_metrics_name)

      # Connect to Impala using either beeswax or HS2 client and verify the number of
      # opened connections.
      client = impalad.service.create_client(
          protocol=('hs2' if protocol == 'hiveserver2' else protocol))
      client.execute("select 1")
      impalad.service.wait_for_metric_value(num_connections_metrics_name,
           num_connections + 1, 20)

      # Wait till the session has expired.
      impalad.service.wait_for_metric_value("impala-server.num-sessions-expired",
           num_expired + 1, 20)
      # Wait till the idle connection is closed.
      impalad.service.wait_for_metric_value(num_connections_metrics_name,
           num_connections, 5)

    # Verify that connecting to HS2 port without establishing a session will not cause
    # the connection to be closed.
    num_hs2_connections = impalad.service.get_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use")
    sock = socket.socket()
    sock.connect((impalad._get_hostname(), DEFAULT_HS2_PORT))
    impalad.service.wait_for_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use",
        num_hs2_connections + 1, 60)
    # Sleep for some time for the frontend service thread to check for idleness.
    sleep(15)
    assert num_hs2_connections + 1 == impalad.service.get_metric_value(
        "impala.thrift-server.hiveserver2-frontend.connections-in-use")
    sock.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--max_hs2_sessions_per_user=2")
  def test_max_hs2_sessions_per_user(self):
    """Test that the --max_hs2_sessions_per_user flag restricts the total number of
    sessions per user. Also checks that the per-user count of hs2 sessions can
    be seen in the webui."""
    impalad = self.cluster.get_first_impalad()
    self.close_impala_clients()
    # Create 2 clients.
    client1 = impalad.service.create_hs2_client()
    client2 = impalad.service.create_hs2_client()
    # Run query to open session.
    client1.execute('select "client1 should succeed"')
    client2.execute('select "client2 should succeed"')
    try:
      # Trying to open a third session should fail.
      client3 = impalad.service.create_hs2_client()
      client3.execute('select "client3 should fail"')
      assert False, "should have failed"
    except Exception as e:
      assert re.match(r".*Number of sessions for user \S+ exceeds coordinator limit 2",
                      str(e), re.DOTALL), "Unexpected exception: " + str(e)

    # Test webui for hs2 sessions.
    res = impalad.service.get_debug_webpage_json("/sessions")
    assert res['num_sessions'] == 2
    assert res['users'][0]['user'] is not None
    assert res['users'][0]['session_count'] == 2
    # Let queries finish, session count should go to zero.
    sleep(6)
    client1.close()
    client2.close()
    res = impalad.service.get_debug_webpage_json("/sessions")
    assert res['num_sessions'] == 0
