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

import tests.common.cluster_config as cluster_config
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_result_verifier import error_msg_expected
from tests.util.cancel_util import (
    QueryToKill,
    assert_kill_error,
    assert_kill_ok
)


class TestKillQuery(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  def test_coordinator_unreachable(self):
    """
    The coordinator of the query to kill is unreachable.

    It is required that each impalad in the cluster is a coordinator.
    """
    protocol = 'hs2'
    with self.create_client_for_nth_impalad(0, protocol) as client, \
        QueryToKill(
            self,
            protocol,
            check_on_exit=False,
            nth_impalad=2) as query_id_to_kill:
      coordinator_to_kill = self.cluster.impalads[2]
      coordinator_to_kill.kill()
      assert_kill_error(
          client,
          "KillQuery() RPC failed: Network error:",
          query_id=query_id_to_kill,
      )

  @pytest.mark.execute_serially
  def test_another_coordinator_unreachable(self):
    """
    A coordinator other than the one of the query to kill is unreachable.

    It is required that each impalad in the cluster is a coordinator.
    """
    protocol = 'hs2'
    with self.create_client_for_nth_impalad(0, protocol) as client, \
        QueryToKill(self, protocol, nth_impalad=2) as query_id_to_kill:
      coordinator_to_kill = self.cluster.impalads[1]  # impalad 1 is between 0 and 2.
      coordinator_to_kill.kill()
      assert_kill_ok(client, query_id_to_kill)

  @pytest.mark.execute_serially
  @cluster_config.single_coordinator
  def test_single_coordinator(self):
    """
    Test when there is only one coordinator in the cluster.
    """
    protocol = 'hs2'
    with self.create_client_for_nth_impalad(0, protocol) as client:
      assert_kill_error(
          client,
          "Could not find query on any coordinator.",
          query_id='123:456')

  @pytest.mark.execute_serially
  @cluster_config.admit_one_query_at_a_time
  def test_admit_one_query_at_a_time(self):
    """
    Make sure queries can be killed when only one query is allowed to run at a time.
    """
    protocol = 'hs2'
    with self.create_client_for_nth_impalad(0, protocol) as client, \
        QueryToKill(self, protocol) as query_id_to_kill:
      assert_kill_ok(client, query_id_to_kill)

  @pytest.mark.execute_serially
  @cluster_config.admit_no_query
  def test_admit_no_query(self):
    """
    Make sure KILL QUERY statement can be executed when no query will be admitted.

    This is to show that KILL QUERY statements are not subject to admission control.
    """
    protocol = 'hs2'
    with self.create_client_for_nth_impalad(0, protocol) as client:
      try:
        client.execute("SELECT 1")
      except Exception as e:
        expected_msg = (
            "Rejected query from pool default-pool: "
            "disabled by requests limit set to 0"
        )
        assert error_msg_expected(str(e), expected_msg)
      assert_kill_error(
          client,
          "Could not find query on any coordinator",
          query_id="123:456"
      )


@cluster_config.enable_authorization
class TestKillQueryAuthorization(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  def test_kill_as_admin(self):
    # ImpylaHS2Connection does not support authentication yet.
    protocol = 'beeswax'
    with self.create_client_for_nth_impalad(0, protocol) as client, \
        QueryToKill(self, protocol, user="user1") as query_id_to_kill:
      assert_kill_ok(client, query_id_to_kill, user=cluster_config.ADMIN)

  @pytest.mark.execute_serially
  def test_kill_as_non_admin(self):
    # ImpylaHS2Connection does not support authentication yet.
    protocol = 'beeswax'
    user1, user2 = "user1", "user2"
    with self.create_client_for_nth_impalad(0, protocol) as user1_client, \
        self.create_client_for_nth_impalad(0, protocol) as user2_client, \
        QueryToKill(self, protocol, user=user1) as query_id_to_kill:
      assert_kill_error(
          user2_client,
          "User '{0}' is not authorized to kill the query.".format(user2),
          query_id=query_id_to_kill,
          user=user2,
      )
      assert_kill_ok(user1_client, query_id_to_kill, user=user1)
