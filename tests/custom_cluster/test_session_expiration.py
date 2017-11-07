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
from time import sleep

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestSessionExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=6")
  def test_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    # setup_class creates an Impala client to <hostname>:21000 after the cluster starts.
    # The client expires at the same time as the client created below. Since we choose the
    # impalad to connect to randomly, the test becomes flaky, as the metric we expect to
    # be incremented by 1 gets incremented by 2 if both clients are connected to the same
    # Impalad.
    self.client.close()
    num_expired = impalad.service.get_metric_value("impala-server.num-sessions-expired")
    client = impalad.service.create_beeswax_client()
    # Sleep for half the expiration time to confirm that the session is not expired early
    # (see IMPALA-838)
    sleep(3)
    assert num_expired == impalad.service.get_metric_value(
      "impala-server.num-sessions-expired")
    # Wait for session expiration. Impala will poll the session expiry queue every second
    impalad.service.wait_for_metric_value(
      "impala-server.num-sessions-expired", num_expired + 1, 20)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_session_timeout=3")
  def test_session_expiration_with_set(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.client.close()
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
  @CustomClusterTestSuite.with_args("--idle_session_timeout=5")
  def test_unsetting_session_expiration(self, vector):
    impalad = self.cluster.get_any_impalad()
    self.client.close()
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
