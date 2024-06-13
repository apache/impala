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
import logging
import pytest
import time

from beeswaxd.BeeswaxService import QueryState
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.common.impala_cluster import (
    DEFAULT_CATALOG_SERVICE_PORT, DEFAULT_STATESTORE_SERVICE_PORT)
from tests.common.skip import SkipIfBuildType, SkipIfNotHdfsMinicluster
from time import sleep

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

import StatestoreService.StatestoreSubscriber as Subscriber
import StatestoreService.StatestoreService as Statestore

LOG = logging.getLogger('statestored_ha_test')


class TestStatestoredHA(CustomClusterTestSuite):
  """A simple wrapper class to launch a cluster with Statestored HA enabled.
  The cluster will be launched with two statestored instances as Active-Passive HA pair.
  All cluster components are started with starting flag FLAGS_enable_statestored_ha
  as true."""

  def get_workload(self):
    return 'functional-query'

  def __disable_statestored_network(self,
      service_port=DEFAULT_STATESTORE_SERVICE_PORT, disable_network=False):
    request = Subscriber.TSetStatestoreDebugActionRequest(
        protocol_version=Subscriber.StatestoreServiceVersion.V2,
        disable_network=disable_network)
    client_transport = TTransport.TBufferedTransport(
        TSocket.TSocket('localhost', service_port))
    trans_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = Statestore.Client(trans_protocol)
    client_transport.open()
    try:
      return client.SetStatestoreDebugAction(request)
    except Exception as e:
      assert False, str(e)

  # Return port of the active catalogd of statestore
  def __get_active_catalogd_port(self, statestore_service):
    active_catalogd_address = \
        statestore_service.get_metric_value("statestore.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    return int(catalog_service_port)

  # Verify port of the active catalogd of impalad is matching with the catalog
  # service port of the given catalogd service.
  def __verify_impalad_active_catalogd_port(self, impalad_index, catalogd_service):
    impalad_service = self.cluster.impalads[impalad_index].service
    active_catalogd_address = \
        impalad_service.get_metric_value("catalog.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert(int(catalog_service_port) == catalogd_service.get_catalog_service_port())

  # Verify the active statestored on impalad is matching with the current active
  # statestored.
  def __verify_impalad_active_statestored(self, impalad_index, active_statestored_index):
    impalad_service = self.cluster.impalads[impalad_index].service
    if active_statestored_index == 0:
      impalad_service.wait_for_metric_value(
          "statestore-subscriber.statestore-active-status",
          expected_value=True, timeout=5)
      assert(impalad_service.get_metric_value(
          "statestore-subscriber.statestore-active-status"))
      assert(not impalad_service.get_metric_value(
          "statestore2-subscriber.statestore-active-status"))
    else:
      impalad_service.wait_for_metric_value(
          "statestore2-subscriber.statestore-active-status",
          expected_value=True, timeout=5)
      assert(not impalad_service.get_metric_value(
          "statestore-subscriber.statestore-active-status"))
      assert(impalad_service.get_metric_value(
           "statestore2-subscriber.statestore-active-status"))

  # Verify the active statestored on catalogd is matching with the current active
  # statestored.
  def __verify_catalogd_active_statestored(
      self, catalogd_index, active_statestored_index):
    catalogds = self.cluster.catalogds()
    catalogd_service = catalogds[catalogd_index].service
    if active_statestored_index == 0:
      catalogd_service.wait_for_metric_value(
          "statestore-subscriber.statestore-active-status",
          expected_value=True, timeout=5)
      assert(catalogd_service.get_metric_value(
          "statestore-subscriber.statestore-active-status"))
      assert(not catalogd_service.get_metric_value(
          "statestore2-subscriber.statestore-active-status"))
    else:
      catalogd_service.wait_for_metric_value(
          "statestore2-subscriber.statestore-active-status",
          expected_value=True, timeout=5)
      assert(not catalogd_service.get_metric_value(
          "statestore-subscriber.statestore-active-status"))
      assert(catalogd_service.get_metric_value(
          "statestore2-subscriber.statestore-active-status"))

  def __run_simple_queries(self, sync_ddl=False):
    try:
      if sync_ddl:
        self.execute_query_expect_success(self.client, "set SYNC_DDL=1")
      self.execute_query_expect_success(
          self.client, "drop table if exists test_statestored_ha")
      self.execute_query_expect_success(
          self.client, "create table if not exists test_statestored_ha (id int)")
      self.execute_query_expect_success(
          self.client, "insert into table test_statestored_ha values(1), (2), (3)")
      self.execute_query_expect_success(
          self.client, "select count(*) from test_statestored_ha")
    finally:
      self.execute_query_expect_success(
          self.client, "drop table if exists test_statestored_ha")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true",
    start_args="--enable_statestored_ha")
  def test_statestored_ha_with_two_statestored(self):
    """The test case for cluster started with statestored HA enabled."""
    # Verify two statestored instances are created with one as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    assert(statestoreds[0].service.get_metric_value("statestore.active-status"))
    assert(not statestoreds[1].service.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Restart one coordinator. Verify it has right active statestored.
    self.cluster.impalads[0].restart()
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.ready',
        expected_value=1, timeout=30)
    self.__verify_impalad_active_statestored(0, 0)

    # Restart standby statestore. Verify that the roles are not changed.
    statestoreds[1].kill()
    statestoreds[1].start(wait_until_ready=True)
    sleep(1)
    assert(statestoreds[0].service.get_metric_value("statestore.active-status"))
    assert(not statestoreds[1].service.get_metric_value("statestore.active-status"))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--enable_statestored_ha=true "
                     "--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_preemption_wait_period_ms=200",
    catalogd_args="--enable_statestored_ha=true",
    impalad_args="--enable_statestored_ha=true")
  def test_statestored_ha_with_one_statestored(self):
    """The test case for cluster with only one statestored when statestored HA is
    enabled."""
    # Verify the statestored instances is created as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 1)
    assert(statestoreds[0].service.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true",
    start_args="--enable_statestored_ha --enable_catalogd_ha")
  def test_statestored_ha_with_catalogd_ha(self):
    """The test case for cluster started with statestored HA enabled."""
    # Verify two statestored instances are created with one as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    assert(statestoreds[0].service.get_metric_value("statestore.active-status"))
    assert(not statestoreds[1].service.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_catalogd_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

  def __test_statestored_auto_failover(
      self, catalogd_ha_enabled=False, no_catalogd_failover=True):
    """Stop active statestored and verify standby statestored becomes active.
    Restart original active statestored. Verify that it does not resume its active
    role."""
    # Verify two statestored instances are created with one as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    statestore_service_0 = statestoreds[0].service
    statestore_service_1 = statestoreds[1].service
    assert(statestore_service_0.get_metric_value("statestore.active-status"))
    assert(not statestore_service_1.get_metric_value("statestore.active-status"))

    # Get active catalogd port from active statestored
    original_active_catalogd_port = self.__get_active_catalogd_port(statestore_service_0)

    if catalogd_ha_enabled:
      self.__verify_catalogd_active_statestored(0, 0)
      self.__verify_catalogd_active_statestored(1, 0)
    else:
      self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Kill active statestored
    statestoreds[0].kill()

    # Wait for long enough for the standby statestored to detect the failure of active
    # statestored and assign itself with active role.
    statestore_service_1.wait_for_metric_value(
        "statestore.active-status", expected_value=True, timeout=120)
    assert(statestore_service_1.get_metric_value("statestore.active-status"))
    sleep(1)

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    if catalogd_ha_enabled:
      self.__verify_catalogd_active_statestored(0, 1)
      self.__verify_catalogd_active_statestored(1, 1)
    else:
      self.__verify_catalogd_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(1, 1)
    self.__verify_impalad_active_statestored(2, 1)

    # Get active catalogd port from current active statestored
    current_active_catalogd_port = self.__get_active_catalogd_port(statestore_service_1)

    if catalogd_ha_enabled:
      if no_catalogd_failover:
        assert(original_active_catalogd_port == current_active_catalogd_port)
      else:
        # Check if there is event of catalogd fail over
        if original_active_catalogd_port != current_active_catalogd_port:
          catalogds = self.cluster.catalogds()
          if current_active_catalogd_port == DEFAULT_CATALOG_SERVICE_PORT:
            catalogd_service = catalogds[0].service
          else:
            catalogd_service = catalogds[1].service
          # Wait for long enough for the current active catalogd to receive notification
          # of catalogd failover
          catalogd_service.wait_for_metric_value(
              "catalog-server.active-status", expected_value=True, timeout=60)
          # Wait catalog topics are propagated from new active catalogd through the
          # active statestored.
          sleep(2)
          self.__verify_impalad_active_catalogd_port(0, catalogd_service)
          self.__verify_impalad_active_catalogd_port(1, catalogd_service)
          self.__verify_impalad_active_catalogd_port(2, catalogd_service)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Restart original active statestored. Verify that the statestored does not resume
    # its active role to avoid flip-flop.
    statestoreds[0].start(wait_until_ready=True)
    sleep(1)
    statestore_service_0 = statestoreds[0].service
    assert(not statestore_service_0.get_metric_value("statestore.active-status"))
    assert(statestore_service_1.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    if catalogd_ha_enabled:
      self.__verify_catalogd_active_statestored(0, 1)
      self.__verify_catalogd_active_statestored(1, 1)
    else:
      self.__verify_catalogd_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(1, 1)
    self.__verify_impalad_active_statestored(2, 1)

    successful_update_statestored_rpc_num_0 = statestore_service_0.get_metric_value(
        "statestore.num-successful-update-statestored-role-rpc")
    successful_update_statestored_rpc_num_1 = statestore_service_1.get_metric_value(
        "statestore.num-successful-update-statestored-role-rpc")
    assert(successful_update_statestored_rpc_num_0 == 0)
    assert(successful_update_statestored_rpc_num_1 >= 4)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds=2",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha")
  def test_statestored_auto_failover(self):
    """Tests for Statestore Service auto fail over without failed RPCs."""
    self.__test_statestored_auto_failover()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds=2 "
                     "--debug_actions=SEND_UPDATE_STATESTORED_RPC_FIRST_ATTEMPT:FAIL@1.0",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha")
  def test_statestored_auto_failover_with_failed_rpc(self):
    """Tests for Statestore Service auto fail over with failed RPCs."""
    self.__test_statestored_auto_failover()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds=2 "
                     "--use_subscriber_id_as_catalogd_priority=true",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha --enable_catalogd_ha")
  def test_statestored_auto_failover_without_catalogd_failover(self):
    """Tests for Statestore Service auto fail over with CatalogD HA enabled.
    use_subscriber_id_as_catalogd_priority is set as true so that both statestored
    elect same catalogd as active, and there is no catalogd fail over when Statestore
    servier fail over to standby statestored.
    """
    self.__test_statestored_auto_failover(
        catalogd_ha_enabled=True, no_catalogd_failover=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds=2 ",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha --enable_catalogd_ha")
  def test_statestored_auto_failover_with_possible_catalogd_failover(self):
    """Tests for Statestore Service auto fail over with CatalogD HA enabled.
    use_subscriber_id_as_catalogd_priority is not set so that both statestored could
    elect different catalogd as active. It's possible there is catalogd fail over when
    Statestore servier fail over to standby statestored.
    """
    self.__test_statestored_auto_failover(
        catalogd_ha_enabled=True, no_catalogd_failover=False)

  def __test_statestored_manual_failover(self, second_failover=False):
    """Stop active statestored and verify standby statestored becomes active.
    Restart original active statestored with statestore_force_active as true. Verify
    that the statestored resumes its active role.
    """
    # Verify two statestored instances are created with one as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    statestore_service_0 = statestoreds[0].service
    statestore_service_1 = statestoreds[1].service
    assert(statestore_service_0.get_metric_value("statestore.active-status"))
    assert(not statestore_service_1.get_metric_value("statestore.active-status"))

    self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Kill active statestored
    statestoreds[0].kill()

    # Wait for long enough for the standby statestored to detect the failure of active
    # statestored and assign itself with active role.
    statestore_service_1.wait_for_metric_value(
        "statestore.active-status", expected_value=True, timeout=120)
    assert(statestore_service_1.get_metric_value("statestore.active-status"))
    sleep(1)

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    self.__verify_catalogd_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(1, 1)
    self.__verify_impalad_active_statestored(2, 1)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Restart original active statestored with statestore_force_active as true.
    # Verify that the statestored resumes its active role.
    statestoreds[0].start(wait_until_ready=True,
                          additional_args="--statestore_force_active=true")
    sleep(1)
    statestore_service_0 = statestoreds[0].service
    assert(statestore_service_0.get_metric_value("statestore.active-status"))
    assert(not statestore_service_1.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd is matching with the current active
    # statestored.
    catalogds = self.cluster.catalogds()
    catalogds[0].service.wait_for_metric_value(
        "statestore-subscriber.statestore-active-status", expected_value=True, timeout=60)
    self.__verify_catalogd_active_statestored(0, 0)

    successful_update_statestored_rpc_num_1 = statestore_service_1.get_metric_value(
        "statestore.num-successful-update-statestored-role-rpc")
    assert(successful_update_statestored_rpc_num_1 >= 4)

    # Trigger second fail over by disabling the network of active statestored.
    if second_failover:
      # Wait till all subscribers re-registering with the restarted statestored.
      wait_time_s = build_flavor_timeout(90, slow_build_timeout=180)
      statestore_service_0.wait_for_metric_value('statestore.live-backends',
          expected_value=4, timeout=wait_time_s)

      sleep(1)
      self.__disable_statestored_network(disable_network=True)
      # Wait for long enough for the standby statestored to detect the failure of active
      # statestored and assign itself with active role.
      statestore_service_1.wait_for_metric_value(
          "statestore.active-status", expected_value=True, timeout=120)
      assert(statestore_service_1.get_metric_value("statestore.active-status"))
      # Verify that original active statestored is in HA recovery mode and is not active.
      statestore_service_0.wait_for_metric_value(
          "statestore.in-ha-recovery-mode", expected_value=True, timeout=60)
      assert(not statestore_service_0.get_metric_value("statestore.active-status"))
      sleep(1)

      # Verify the active statestored on catalogd and impalad are matching with
      # the current active statestored.
      self.__verify_catalogd_active_statestored(0, 1)
      self.__verify_impalad_active_statestored(0, 1)
      self.__verify_impalad_active_statestored(1, 1)
      self.__verify_impalad_active_statestored(2, 1)

      # Verify simple queries are ran successfully.
      self.__run_simple_queries()
      # Verify simple queries with sync_ddl as 1.
      self.__run_simple_queries(sync_ddl=True)

      # Re-enable network for original active statestored. Verify that the statestored
      # exits HA recovery mode, and resume its active role since it was started with
      # statestore_force_active as true.
      self.__disable_statestored_network(disable_network=False)
      statestore_service_0.wait_for_metric_value(
          "statestore.in-ha-recovery-mode", expected_value=False, timeout=60)
      sleep(1)
      assert(statestore_service_0.get_metric_value("statestore.active-status"))
      assert(not statestore_service_1.get_metric_value("statestore.active-status"))

      # Verify the active statestored on catalogd and impalad are matching with
      # the current active statestored.
      self.__verify_catalogd_active_statestored(0, 0)
      self.__verify_impalad_active_statestored(0, 0)
      self.__verify_impalad_active_statestored(1, 0)
      self.__verify_impalad_active_statestored(2, 0)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=100 "
                     "--statestore_peer_timeout_seconds=2 "
                     "--debug_actions=DISABLE_STATESTORE_NETWORK",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha")
  def test_statestored_manual_failover(self):
    """Tests for Statestore Service manual fail over without failed RPCs."""
    self.__test_statestored_manual_failover(second_failover=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=100 "
                     "--statestore_peer_timeout_seconds=2 "
                     "--debug_actions=SEND_UPDATE_STATESTORED_RPC_FIRST_ATTEMPT:FAIL@1.0",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha")
  def test_statestored_manual_failover_with_failed_rpc(self):
    """Tests for Statestore Service manual fail over with failed RPCs."""
    self.__test_statestored_manual_failover(second_failover=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_force_active=true",
    start_args="--enable_statestored_ha")
  def test_two_statestored_with_force_active(self):
    """The test case for cluster started with Statestored HA enabled and
    both statestoreds started with 'statestore_force_active' as true.
    Verify that one and only one statestored is active."""
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    statestore_service_1 = statestoreds[0].service
    statestore_service_2 = statestoreds[1].service
    assert(statestore_service_1.get_metric_value("statestore.active-status")
        != statestore_service_2.get_metric_value("statestore.active-status"))

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    if statestore_service_1.get_metric_value("statestore.active-status"):
      self.__verify_catalogd_active_statestored(0, 0)
      self.__verify_impalad_active_statestored(0, 0)
      self.__verify_impalad_active_statestored(1, 0)
      self.__verify_impalad_active_statestored(2, 0)
    else:
      self.__verify_catalogd_active_statestored(0, 1)
      self.__verify_impalad_active_statestored(0, 1)
      self.__verify_impalad_active_statestored(1, 1)
      self.__verify_impalad_active_statestored(2, 1)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds=2 "
                     "--debug_actions=DISABLE_STATESTORE_NETWORK",
    impalad_args="--statestore_subscriber_timeout_seconds=2",
    catalogd_args="--statestore_subscriber_timeout_seconds=2",
    start_args="--enable_statestored_ha")
  def test_statestored_auto_failover_with_disabling_network(self):
    """Tests for Statestore Service auto fail over when active statestored lost
    connections with other nodes in the cluster.
    Disable network for active statestored and verify standby statestored becomes
    active. When original active statestored recover, it does not resume its active
    role."""
    # Verify two statestored instances are created with one as active.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    statestore_service_0 = statestoreds[0].service
    statestore_service_1 = statestoreds[1].service
    assert(statestore_service_0.get_metric_value("statestore.active-status"))
    assert(not statestore_service_1.get_metric_value("statestore.active-status"))

    self.__verify_catalogd_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(0, 0)
    self.__verify_impalad_active_statestored(1, 0)
    self.__verify_impalad_active_statestored(2, 0)

    # Disable the network of active statestored.
    self.__disable_statestored_network(disable_network=True)

    # Wait for long enough for the standby statestored to detect the failure of active
    # statestored and assign itself with active role.
    statestore_service_1.wait_for_metric_value(
        "statestore.active-status", expected_value=True, timeout=120)
    assert(statestore_service_1.get_metric_value("statestore.active-status"))
    # Verify that original active statestored is in HA recovery mode.
    statestore_service_0.wait_for_metric_value(
        "statestore.in-ha-recovery-mode", expected_value=True, timeout=60)
    assert(not statestore_service_0.get_metric_value("statestore.active-status"))
    sleep(1)

    # Verify the active statestored on catalogd and impalad are matching with
    # the current active statestored.
    self.__verify_catalogd_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(0, 1)
    self.__verify_impalad_active_statestored(1, 1)
    self.__verify_impalad_active_statestored(2, 1)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Re-enable network for original active statestored. Verify that the statestored
    # exits HA recovery mode, but does not resume its active role.
    self.__disable_statestored_network(disable_network=False)
    statestore_service_0.wait_for_metric_value(
        "statestore.in-ha-recovery-mode", expected_value=False, timeout=60)
    assert(not statestore_service_0.get_metric_value("statestore.active-status"))
    assert(statestore_service_1.get_metric_value("statestore.active-status"))

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Disable the network of current standby statestored.
    self.__disable_statestored_network(disable_network=True)
    # Verify that the standby statestored enters HA recovery mode.
    statestore_service_0.wait_for_metric_value(
        "statestore.in-ha-recovery-mode", expected_value=True, timeout=120)
    assert(not statestore_service_0.get_metric_value("statestore.active-status"))

    # Re-enable network for standby statestored. Verify that the statestored exits
    # HA recovery mode.
    self.__disable_statestored_network(disable_network=False)
    statestore_service_0.wait_for_metric_value(
        "statestore.in-ha-recovery-mode", expected_value=False, timeout=120)
    assert(not statestore_service_0.get_metric_value("statestore.active-status"))

  SUBSCRIBER_TIMEOUT_S = 2
  SS_PEER_TIMEOUT_S = 2
  RECOVERY_GRACE_PERIOD_S = 5

  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.scheduling
  @CustomClusterTestSuite.with_args(
    statestored_args="--use_network_address_as_statestore_priority=true "
                     "--statestore_ha_heartbeat_monitoring_frequency_ms=50 "
                     "--statestore_peer_timeout_seconds={timeout_s} "
                     "--use_subscriber_id_as_catalogd_priority=true"
                     .format(timeout_s=SS_PEER_TIMEOUT_S),
    impalad_args="--statestore_subscriber_timeout_seconds={timeout_s} "
                 "--statestore_subscriber_recovery_grace_period_ms={recovery_period_ms}"
                 .format(timeout_s=SUBSCRIBER_TIMEOUT_S,
                         recovery_period_ms=(RECOVERY_GRACE_PERIOD_S * 1000)),
    catalogd_args="--statestore_subscriber_timeout_seconds={timeout_s}"
                  .format(timeout_s=SUBSCRIBER_TIMEOUT_S),
    start_args="--enable_statestored_ha --enable_catalogd_ha")
  def test_statestore_failover_query_resilience(self):
    """Test that a momentary inconsistent cluster membership state after statestore
    service fail-over will not result in query cancellation. Also make sure that query
    get cancelled if a backend actually went down after recovery grace period."""
    # Verify two statestored instances are created with one in active role.
    statestoreds = self.cluster.statestoreds()
    assert (len(statestoreds) == 2)
    statestore_service_0 = statestoreds[0].service
    statestore_service_1 = statestoreds[1].service
    assert (statestore_service_0.get_metric_value("statestore.active-status")), \
        "First statestored must be active"
    assert (not statestore_service_1.get_metric_value("statestore.active-status")), \
        "Second statestored must not be active"

    slow_query = \
        "select distinct * from tpch_parquet.lineitem where l_orderkey > sleep(1000)"
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    try:
      # Run a slow query
      handle = client.execute_async(slow_query)
      # Make sure query starts running.
      self.wait_for_state(handle, QueryState.RUNNING, 120, client)
      profile = client.get_runtime_profile(handle)
      assert "NumBackends: 3" in profile, profile
      # Kill active statestored
      statestoreds[0].kill()
      # Wait for long enough for the standby statestored to detect the failure of active
      # statestored and assign itself in active role.
      statestore_service_1.wait_for_metric_value(
          "statestore.active-status", expected_value=True, timeout=120)
      assert (statestore_service_1.get_metric_value("statestore.active-status")), \
          "Second statestored must be active now"
      statestore_service_1.wait_for_live_subscribers(5)
      # Wait till the grace period ends + some buffer to verify the slow query is still
      # running.
      sleep(self.RECOVERY_GRACE_PERIOD_S + 1)
      assert client.get_state(handle) == QueryState.RUNNING, \
          "Query expected to be in running state"
      # Now kill a backend, and make sure the query fails.
      self.cluster.impalads[2].kill()
      try:
        client.wait_for_finished_timeout(handle, 100)
        assert False, "Query expected to fail"
      except ImpalaBeeswaxException as e:
        assert "Failed due to unreachable impalad" in str(e), str(e)

      # Restart original active statestored. Verify that the statestored does not resume
      # its active role.
      statestoreds[0].start(wait_until_ready=True)
      statestore_service_0.wait_for_metric_value(
          "statestore.active-status", expected_value=False, timeout=120)
      assert (not statestore_service_0.get_metric_value("statestore.active-status")), \
          "First statestored must not be active"
      assert (statestore_service_1.get_metric_value("statestore.active-status")), \
          "Second statestored must be active"
      # Run a slow query
      handle = client.execute_async(slow_query)
      # Make sure query starts running.
      self.wait_for_state(handle, QueryState.RUNNING, 120, client)
      profile = client.get_runtime_profile(handle)
      assert "NumBackends: 2" in profile, profile
      # Kill current active statestored
      start_time = time.time()
      statestoreds[1].kill()
      # Wait till the standby statestored becomes active.
      query_state = client.get_state(handle)
      assert query_state == QueryState.RUNNING
      statestore_service_0.wait_for_metric_value(
          "statestore.active-status", expected_value=True, timeout=120)
      assert (statestore_service_0.get_metric_value("statestore.active-status")), \
          "First statestored must be active now"
      # Kill one backend
      self.cluster.impalads[1].kill()
      # Verify that it has to wait longer than the RECOVERY_GRACE_PERIOD_S for the
      # query to fail. Combine failover time (SS_PEER_TIMEOUT_S) and recovery grace
      # period (RECOVERY_GRACE_PERIOD_S) to avoid flaky test.
      timeout_s = self.SS_PEER_TIMEOUT_S + self.RECOVERY_GRACE_PERIOD_S * 2
      self.wait_for_state(handle, QueryState.EXCEPTION, timeout_s, client)
      client.close_query(handle)
      elapsed_s = time.time() - start_time
      assert elapsed_s >= self.SS_PEER_TIMEOUT_S + self.RECOVERY_GRACE_PERIOD_S, \
          ("Query was canceled in %s seconds, less than failover time + grace-period"
           % (elapsed_s))
    finally:
      client.close()


class TestStatestoredHAStartupDelay(CustomClusterTestSuite):
  """This test injects a real delay in statestored startup. The impalads and catalogd are
  expected to be able to tolerate this delay with FLAGS_tolerate_statestore_startup_delay
  set as true. This is not testing anything beyond successful startup."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('Statestore startup delay tests only run in exhaustive')
    super(TestStatestoredHAStartupDelay, cls).setup_class()

  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(
    impalad_args="--tolerate_statestore_startup_delay=true",
    catalogd_args="--tolerate_statestore_startup_delay=true",
    statestored_args="--stress_statestore_startup_delay_ms=60000 "
                     "--use_network_address_as_statestore_priority=true",
    start_args="--enable_statestored_ha")
  def test_subscriber_tolerate_startup_delay(self):
    """The impalads and catalogd are expected to be able to tolerate the delay of
    statestored startup with starting flags FLAGS_tolerate_statestore_startup_delay
    set as true."""
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before the coordinator and catalogd finish
    # starting up.
    statestoreds = self.cluster.statestoreds()
    assert(len(statestoreds) == 2)
    assert(statestoreds[0].service.get_metric_value("statestore.active-status"))
    assert(not statestoreds[1].service.get_metric_value("statestore.active-status"))

    # Verify that impalad and catalogd entered recovery mode and tried to re-register
    # with statestore.
    re_register_attempt = self.cluster.impalads[0].service.get_metric_value(
        "statestore-subscriber.num-re-register-attempt")
    assert re_register_attempt > 0
    re_register_attempt = self.cluster.catalogd.service.get_metric_value(
        "statestore-subscriber.num-re-register-attempt")
    assert re_register_attempt > 0

    # Verify simple queries are ran successfully.
    self.execute_query_expect_success(
        self.client, "select count(*) from functional.alltypes")
