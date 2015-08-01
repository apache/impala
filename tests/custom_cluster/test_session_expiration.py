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
# Tests for query expiration.

import pytest
import threading
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.custom_cluster_test_suite import NUM_SUBSCRIBERS, CLUSTER_SIZE
from time import sleep, time
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException

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
    # Wait for session expiration. Session timeout was set for 6 seconds, so Impala
    # will poll the session expiry queue every 3 seconds. So, as long as the sleep in
    # ImpalaSever::ExpireSessions() is not late, the session will expire in at most 9
    # seconds. The test has already waited 3 seconds.
    impalad.service.wait_for_metric_value(
      "impala-server.num-sessions-expired", num_expired + 1, 20)
