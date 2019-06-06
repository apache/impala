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

import pytest
import socket
from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import DEFAULT_HS2_PORT

class TestSessionExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=6 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.__close_default_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
    num_connections = impalad.service.get_metric_value(
        "impala.thrift-server.beeswax-frontend.connections-in-use")
    client = impalad.service.create_beeswax_client()
    # Sleep for half the expiration time to confirm that the session is not expired early
    # (see IMPALA-838)
    sleep(3)
    assert num_expired == impalad.service.get_metric_value(
        "impala-server.num-sessions-expired")
    # Wait for session expiration. Impala will poll the session expiry queue every second
    impalad.service.wait_for_metric_value(
        "impala-server.num-sessions-expired", num_expired + 1, 20)
    # Verify that the idle connection is not closed.
    assert 1 + num_connections == impalad.service.get_metric_value(
        "impala.thrift-server.beeswax-frontend.connections-in-use")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=3 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration_with_set(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.__close_default_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test if we can set a shorter timeout than the process-wide option
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=1")
    sleep(2.5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")

    # Test if we can set a longer timeout than the process-wide option
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=10")
    sleep(5)
    assert num_expired + 1 == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")


  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=5 "
       "--idle_client_poll_period_s=0")
  def test_unsetting_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.__close_default_clients()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")

    # Test unsetting IDLE_SESSION_TIMEOUT
    client = impalad.service.create_beeswax_client()
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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--default_pool_max_requests=1 "
      "--idle_client_poll_period_s=0")
  def test_session_expiration_with_queued_query(self, vector):
    """Ensure that a query waiting in queue gets cancelled if the session expires."""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    client.execute("SET IDLE_SESSION_TIMEOUT=3")
    client.execute_async("select sleep(10000)")
    queued_handle = client.execute_async("select 1")
    impalad.service.wait_for_metric_value(
      "admission-controller.local-num-queued.default-pool", 1)
    sleep(3)
    impalad.service.wait_for_metric_value(
      "admission-controller.local-num-queued.default-pool", 0)
    impalad.service.wait_for_metric_value(
      "admission-controller.agg-num-running.default-pool", 0)
    queued_query_profile = impalad.service.create_beeswax_client().get_runtime_profile(
      queued_handle)
    assert "Admission result: Cancelled (queued)" in queued_query_profile

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--idle_session_timeout=10 "
      "--idle_client_poll_period_s=1", cluster_size=1)
  def test_closing_idle_connection(self, vector):
    """ IMPALA-7802: verifies that connections of idle sessions are closed
    after the sessions have expired."""
    impalad = self.cluster.get_any_impalad()
    self.__close_default_clients()

    for protocol in ['beeswax', 'hiveserver2']:
      num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
      num_connections_metrics_name = \
          "impala.thrift-server.{}-frontend.connections-in-use".format(protocol)
      num_connections = impalad.service.get_metric_value(num_connections_metrics_name)

      # Connect to Impala using either beeswax or HS2 client and verify the number of
      # opened connections.
      if protocol == 'beeswax':
        client = impalad.service.create_beeswax_client()
      else:
        client = impalad.service.create_hs2_client()
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

  def __close_default_clients(self):
    """Close the clients that were automatically created by setup_class(). These clients
    can expire during test, which results in metrics that tests depend on changing. Each
    test should create its own clients as needed."""
    self.client.close()
    self.hs2_client.close()
