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
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.util.filesystem_utils import get_fs_path
from time import sleep

LOG = logging.getLogger('catalogd_ha_test')
DEFAULT_STATESTORE_SERVICE_PORT = 24000
DEFAULT_CATALOG_SERVICE_PORT = 26000


class TestCatalogdHA(CustomClusterTestSuite):
  """A simple wrapper class to launch a cluster with catalogd HA enabled.
  The cluster will be launched with two catalogd instances as Active-Passive HA pair.
  statestored and catalogds are started with starting flag FLAGS_enable_catalogd_ha
  as true. """

  def get_workload(self):
    return 'functional-query'

  # Verify port of the active catalogd of statestore is matching with the catalog
  # service port of the given catalogd service.
  def __verify_statestore_active_catalogd_port(self, catalogd_service):
    statestore_service = self.cluster.statestored.service
    active_catalogd_address = \
        statestore_service.get_metric_value("statestore.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert(int(catalog_service_port) == catalogd_service.get_catalog_service_port())

  # Verify port of the active catalogd of impalad is matching with the catalog
  # service port of the given catalogd service.
  def __verify_impalad_active_catalogd_port(self, impalad_index, catalogd_service):
    impalad_service = self.cluster.impalads[impalad_index].service
    active_catalogd_address = \
        impalad_service.get_metric_value("catalog.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert(int(catalog_service_port) == catalogd_service.get_catalog_service_port())

  def __run_simple_queries(self, sync_ddl=False):
    try:
      if sync_ddl:
        self.execute_query_expect_success(self.client, "set SYNC_DDL=1")
      self.execute_query_expect_success(
          self.client, "drop table if exists test_catalogd_ha")
      self.execute_query_expect_success(
          self.client, "create table if not exists test_catalogd_ha (id int)")
      self.execute_query_expect_success(
          self.client, "insert into table test_catalogd_ha values(1), (2), (3)")
      self.execute_query_expect_success(
          self.client, "select count(*) from test_catalogd_ha")
    finally:
      self.execute_query_expect_success(
          self.client, "drop table if exists test_catalogd_ha")

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha")
  def test_catalogd_ha_with_two_catalogd(self):
    """The test case for cluster started with catalogd HA enabled."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    # Restart one coordinator. Verify it get active catalogd address from statestore.
    self.cluster.impalads[0].restart()
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.ready',
        expected_value=1, timeout=30)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)

  @CustomClusterTestSuite.with_args(
    statestored_args="--enable_catalogd_ha=true "
                     "--use_subscriber_id_as_catalogd_priority=true "
                     "--catalogd_ha_preemption_wait_period_ms=200",
    catalogd_args="--enable_catalogd_ha=true")
  def test_catalogd_ha_with_one_catalogd(self):
    """The test case for cluster with only one catalogd when catalogd HA is enabled."""
    # Verify the catalogd instances is created as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 1)
    catalogd_service_1 = catalogds[0].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

  def __test_catalogd_auto_failover(self):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd. Verify that statestore does not resume its
    active role."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    statestore_service = self.cluster.statestored.service
    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    catalogds[0].kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0)
    assert(catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries()
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(sync_ddl=True)

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries

    # Restart original active catalogd. Verify that statestore does not resume it as
    # active to avoid flip-flop.
    catalogds[0].start(wait_until_ready=True)
    sleep(1)
    catalogd_service_1 = catalogds[0].service
    assert(not catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    start_args="--enable_catalogd_ha")
  def test_catalogd_auto_failover(self):
    """Tests for Catalog Service auto fail over without failed RPCs."""
    self.__test_catalogd_auto_failover()

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert(successful_update_catalogd_rpc_num >= 6)
    assert(failed_update_catalogd_rpc_num == 0)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000 "
                     "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:FAIL@1.0",
    start_args="--enable_catalogd_ha")
  def test_catalogd_auto_failover_with_failed_rpc(self):
    """Tests for Catalog Service auto fail over with failed RPCs."""
    self.__test_catalogd_auto_failover()

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert(successful_update_catalogd_rpc_num >= 6)
    assert(failed_update_catalogd_rpc_num == successful_update_catalogd_rpc_num)

  def __test_catalogd_manual_failover(self):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd with force_catalogd_active as true. Verify that
    statestore resume it as active.
    """
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    statestore_service = self.cluster.statestored.service
    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    catalogds[0].kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0)
    assert(catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries
    start_count_clear_topic_entries = end_count_clear_topic_entries

    # Restart original active catalogd with force_catalogd_active as true.
    # Verify that statestore resume it as active.
    catalogds[0].start(wait_until_ready=True,
                       additional_args="--force_catalogd_active=true")
    catalogd_service_1 = catalogds[0].service
    catalogd_service_1.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=15)
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    start_args="--enable_catalogd_ha")
  def test_catalogd_manual_failover(self):
    """Tests for Catalog Service manual fail over without failed RPCs."""
    self.__test_catalogd_manual_failover()

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert(successful_update_catalogd_rpc_num >= 10)
    assert(failed_update_catalogd_rpc_num == 0)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000 "
                     "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:FAIL@1.0",
    start_args="--enable_catalogd_ha")
  def test_catalogd_manual_failover_with_failed_rpc(self):
    """Tests for Catalog Service manual fail over with failed RPCs."""
    self.__test_catalogd_manual_failover()

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert(successful_update_catalogd_rpc_num >= 10)
    assert(failed_update_catalogd_rpc_num == successful_update_catalogd_rpc_num)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha")
  def test_restart_statestore(self):
    """The test case for restarting statestore after the cluster is created with
    catalogd HA enabled."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)

    # Restart statestore. Verify one catalogd is assigned as active, the other is
    # assigned as standby.
    self.cluster.statestored.restart()
    wait_time_s = build_flavor_timeout(90, slow_build_timeout=180)
    self.cluster.statestored.service.wait_for_metric_value('statestore.live-backends',
        expected_value=5, timeout=wait_time_s)
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

    unexpected_msg = re.compile("Ignore the update of active catalogd since more recent "
        "update has been processed ([0-9]+ vs [0-9]+)")
    self.assert_catalogd_log_contains("INFO", unexpected_msg, expected_count=0)
    self.assert_impalad_log_contains("INFO", unexpected_msg, expected_count=0)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--force_catalogd_active=true",
    start_args="--enable_catalogd_ha")
  def test_two_catalogd_with_force_active(self):
    """The test case for cluster started with catalogd HA enabled and
    both catalogds started with 'force_catalogd_active' as true.
    Verify that one and only one catalogd is active."""
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status")
        != catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    if catalogd_service_1.get_metric_value("catalog-server.active-status"):
      self.__verify_statestore_active_catalogd_port(catalogd_service_1)
      self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
      self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
      self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    else:
      self.__verify_statestore_active_catalogd_port(catalogd_service_2)
      self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
      self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
      self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries()

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=true",
    start_args="--enable_catalogd_ha")
  def test_metadata_after_failover(self, unique_database):
    """Verify that the metadata is correct after failover."""
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    create_func_impala = ("create function {database}.identity_tmp(bigint) "
                          "returns bigint location '{location}' symbol='Identity'")
    self.client.execute(create_func_impala.format(
        database=unique_database,
        location=get_fs_path('/test-warehouse/libTestUdfs.so')))
    self.execute_query_expect_success(
        self.client, "select %s.identity_tmp(10)" % unique_database)

    # Kill active catalogd
    catalogds[0].kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert(catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0)
    assert(catalogd_service_2.get_metric_value("catalog-server.active-status"))

    self.execute_query_expect_success(
        self.client, "select %s.identity_tmp(10)" % unique_database)
