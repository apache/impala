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
import json
import logging
import re
import requests
import time

from builtins import round
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite, IMPALA_HOME
from tests.common.environ import build_flavor_timeout
from tests.common.impala_connection import ERROR
from tests.common.parametrize import UniqueDatabase
from tests.common.test_vector import HS2
from tests.util.filesystem_utils import IS_S3, get_fs_path, FILESYSTEM_PREFIX
from time import sleep

LOG = logging.getLogger('catalogd_ha_test')
DEFAULT_STATESTORE_SERVICE_PORT = 24000
DEFAULT_CATALOG_SERVICE_PORT = 26000
SLOW_BUILD_SYNC_DDL_DELAY_S = 20
SYNC_DDL_DELAY_S = build_flavor_timeout(
    10, slow_build_timeout=SLOW_BUILD_SYNC_DDL_DELAY_S)
SS_AUTO_FAILOVER_FREQ_MS = 500
SS_AUTO_FAILOVER_ARGS = (
  "--use_subscriber_id_as_catalogd_priority=true "
  "--statestore_heartbeat_frequency_ms={0} "
  "--active_catalogd_designation_monitoring_interval_ms={0} ").format(
  SS_AUTO_FAILOVER_FREQ_MS)
# s3 can behave as a slow build.
if IS_S3:
  SYNC_DDL_DELAY_S = SLOW_BUILD_SYNC_DDL_DELAY_S


class TestCatalogdHA(CustomClusterTestSuite):
  """A simple wrapper class to launch a cluster with catalogd HA enabled.
  The cluster will be launched with two catalogd instances as Active-Passive HA pair.
  statestored and catalogds are started with starting flag FLAGS_enable_catalogd_ha
  as true. """

  VARZ_URL = "http://localhost:{0}/varz"
  CATALOG_HA_INFO_URL = "http://localhost:{0}/catalog_ha_info"
  JSON_METRICS_URL = "http://localhost:{0}/jsonmetrics"
  HEALTHZ_URL = "http://localhost:{0}/healthz"

  SS_TEST_PORT = ["25010"]

  @classmethod
  def default_test_protocol(cls):
    return HS2

  # Verify port of the active catalogd of statestore is matching with the catalog
  # service port of the given catalogd service.
  def __verify_statestore_active_catalogd_port(self, catalogd_service):
    statestore_service = self.cluster.statestored.service
    active_catalogd_address = \
        statestore_service.get_metric_value("statestore.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert int(catalog_service_port) == catalogd_service.get_catalog_service_port()

  # Verify port of the active catalogd of impalad is matching with the catalog
  # service port of the given catalogd service.
  def __verify_impalad_active_catalogd_port(self, impalad_index, catalogd_service):
    impalad_service = self.cluster.impalads[impalad_index].service
    active_catalogd_address = \
        impalad_service.get_metric_value("catalog.active-catalogd-address")
    _, catalog_service_port = active_catalogd_address.split(":")
    assert int(catalog_service_port) == catalogd_service.get_catalog_service_port()

  def __run_simple_queries(self, unique_database, sync_ddl=False):
    opts = {'sync_ddl': sync_ddl}
    self.execute_query_expect_success(self.client, "USE " + unique_database, opts)
    self.execute_query_expect_success(
        self.client, "drop table if exists test_catalogd_ha", opts)
    self.execute_query_expect_success(
        self.client, "create table if not exists test_catalogd_ha (id int)", opts)
    self.execute_query_expect_success(
        self.client, "insert into table test_catalogd_ha values(1), (2), (3)", opts)
    self.execute_query_expect_success(
        self.client, "select count(*) from test_catalogd_ha", opts)
    self.execute_query_expect_success(
        self.client, "drop table if exists test_catalogd_ha", opts)
    self.execute_query_expect_success(self.client, "USE default", opts)

  def __get_catalogds(self):
    """Return tuple of (active_catalogd, standby_catalogd)."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert len(catalogds) == 2
    # Assert that /healthz page is OK.
    for catalogd in catalogds:
      port = catalogd.get_webserver_port()
      page = requests.get(self.HEALTHZ_URL.format(port))
      LOG.info("Status of healthz page at port {}: {}".format(port, page.status_code))
      assert page.status_code == requests.codes.ok, "port {} not ready".format(port)
      page = requests.head(self.HEALTHZ_URL.format(port))
      LOG.info("Status of healthz page at port {}: {}".format(port, page.status_code))
      assert page.status_code == requests.codes.ok, "port {} not ready".format(port)
    first_impalad = self.cluster.get_first_impalad()
    page = requests.head(self.HEALTHZ_URL.format(first_impalad.get_webserver_port()))
    assert page.status_code == requests.codes.ok

    active_catalogd = catalogds[0]
    standby_catalogd = catalogds[1]
    if not active_catalogd.service.get_metric_value("catalog-server.active-status"):
      active_catalogd, standby_catalogd = standby_catalogd, active_catalogd
    assert active_catalogd.service.get_metric_value("catalog-server.active-status")
    assert not standby_catalogd.service.get_metric_value("catalog-server.active-status")
    return (active_catalogd, standby_catalogd)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha")
  def test_catalogd_ha_with_two_catalogd(self, unique_database):
    self.__test_catalogd_ha_with_two_catalogd(unique_database)

  def __test_catalogd_ha_with_two_catalogd(self, unique_database):
    """The test case for cluster started with catalogd HA enabled."""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries(unique_database)
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(unique_database, sync_ddl=True)

    # Restart one coordinator. Verify it get active catalogd address from statestore.
    self.cluster.impalads[1].restart()
    self.cluster.impalads[1].service.wait_for_metric_value('impala-server.ready',
        expected_value=1, timeout=30)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)

  @CustomClusterTestSuite.with_args(
    statestored_args="--enable_catalogd_ha=true "
                     "--use_subscriber_id_as_catalogd_priority=true "
                     "--catalogd_ha_preemption_wait_period_ms=200",
    catalogd_args="--enable_catalogd_ha=true")
  def test_catalogd_ha_with_one_catalogd(self, unique_database):
    """The test case for cluster with only one catalogd when catalogd HA is enabled."""
    # Verify the catalogd instances is created as active.
    catalogds = self.cluster.catalogds()
    assert len(catalogds) == 1
    catalogd_service_1 = catalogds[0].service
    assert catalogd_service_1.get_metric_value("catalog-server.active-status")

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries(unique_database)

  def __test_catalogd_auto_failover(self, unique_database):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd. Verify that statestore does not resume its
    active role. Run a query during failover and comfirm that it is fail."""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service
    catalogd_service_2 = standby_catalogd.service
    statestore_service = self.cluster.statestored.service

    # Assert that cluster is set up with configs needed to run this test.
    assert SS_AUTO_FAILOVER_FREQ_MS >= int(statestore_service.get_flag_current_value(
        'active_catalogd_designation_monitoring_interval_ms'))
    assert SS_AUTO_FAILOVER_FREQ_MS >= int(statestore_service.get_flag_current_value(
        'statestore_heartbeat_frequency_ms'))

    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    active_catalogd.kill()

    # Tes run a DDL query after active_catalogd killed.
    # This query should fail if coordinator has not receive update from StatestoreD about
    # the standby_catalogd becomes active.
    self.execute_query_expect_failure(
        self.client, "create table {}.table_creation_during_failover (id int)".format(
            unique_database))

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0
    assert catalogd_service_2.get_metric_value("catalog-server.active-status")

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries(unique_database)
    # Verify simple queries with sync_ddl as 1.
    self.__run_simple_queries(unique_database, sync_ddl=True)

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries

    # Restart original active catalogd. Verify that statestore does not resume it as
    # active to avoid flip-flop.
    active_catalogd.start(wait_until_ready=True)
    sleep(1)
    catalogd_service_1 = active_catalogd.service
    assert not catalogd_service_1.get_metric_value("catalog-server.active-status")
    assert catalogd_service_2.get_metric_value("catalog-server.active-status")

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

  @CustomClusterTestSuite.with_args(
    statestored_args=SS_AUTO_FAILOVER_ARGS,
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--enable_reload_events=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_catalogd_auto_failover(self, unique_database):
    """Tests for Catalog Service auto fail over without failed RPCs."""
    self.__test_catalogd_auto_failover(unique_database)

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert successful_update_catalogd_rpc_num >= 6
    assert failed_update_catalogd_rpc_num == 0

  @CustomClusterTestSuite.with_args(
    statestored_args=(
        SS_AUTO_FAILOVER_ARGS
        + "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:FAIL@1.0"),
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--enable_reload_events=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_catalogd_auto_failover_with_failed_rpc(self, unique_database):
    """Tests for Catalog Service auto fail over with failed RPCs."""
    self.__test_catalogd_auto_failover(unique_database)

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert successful_update_catalogd_rpc_num >= 6
    assert failed_update_catalogd_rpc_num == successful_update_catalogd_rpc_num

  @CustomClusterTestSuite.with_args(
    statestored_args=(
        SS_AUTO_FAILOVER_ARGS
        + "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:SLEEP@3000"),
    # minicluster has 68 Db when this test is written. So total sleep is ~3.4s.
    catalogd_args="--reset_metadata_lock_duration_ms=100 "
                  "--debug_actions=reset_metadata_loop_locked:SLEEP@50",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  @UniqueDatabase.parametrize(name_prefix='aaa_test_catalogd_auto_failover_slow_first_db')
  def test_catalogd_auto_failover_slow_first_db(self, unique_database):
    """Tests for Catalog Service auto fail over with both slow metadata reset and slow
    statestore update. Set 'aaa_' as unique_database prefix to make the database among
    the earliest in reset metadata order."""
    self.__test_catalogd_auto_failover(unique_database)

  @CustomClusterTestSuite.with_args(
    statestored_args=(
        SS_AUTO_FAILOVER_ARGS
        + "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:SLEEP@3000"),
    # minicluster has 68 Db when this test is written. So total sleep is ~3.4s.
    catalogd_args="--reset_metadata_lock_duration_ms=100 "
                  "--debug_actions=reset_metadata_loop_locked:SLEEP@50",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  @UniqueDatabase.parametrize(name_prefix='zzz_test_catalogd_auto_failover_slow_last_db')
  def test_catalogd_auto_failover_slow_last_db(self, unique_database):
    """Tests for Catalog Service auto fail over with both slow metadata reset and slow
    statestore update. Set 'zzz_' as unique_database prefix to make the database among
    the latest in reset metadata order."""
    self.__test_catalogd_auto_failover(unique_database)

  def __test_catalogd_manual_failover(self, unique_database):
    """Stop active catalogd and verify standby catalogd becomes active.
    Restart original active catalogd with force_catalogd_active as true. Verify that
    statestore resume it as active.
    """
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service
    catalogd_service_2 = standby_catalogd.service

    statestore_service = self.cluster.statestored.service
    start_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")

    # Kill active catalogd
    active_catalogd.kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0
    assert catalogd_service_2.get_metric_value("catalog-server.active-status")

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_2)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_2)

    # Verify simple queries are ran successfully.
    self.__run_simple_queries(unique_database)

    end_count_clear_topic_entries = statestore_service.get_metric_value(
        "statestore.num-clear-topic-entries-requests")
    assert end_count_clear_topic_entries > start_count_clear_topic_entries
    start_count_clear_topic_entries = end_count_clear_topic_entries

    # Restart original active catalogd with force_catalogd_active as true.
    # Verify that statestore resume it as active.
    active_catalogd.start(wait_until_ready=True,
                          additional_args="--force_catalogd_active=true")
    catalogd_service_1 = active_catalogd.service
    catalogd_service_1.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=15)
    assert catalogd_service_1.get_metric_value("catalog-server.active-status")
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    assert not catalogd_service_2.get_metric_value("catalog-server.active-status")

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
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--enable_reload_events=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_catalogd_manual_failover(self, unique_database):
    """Tests for Catalog Service manual fail over without failed RPCs."""
    self.__test_catalogd_manual_failover(unique_database)

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert successful_update_catalogd_rpc_num >= 10
    assert failed_update_catalogd_rpc_num == 0

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000 "
                     "--debug_actions=SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT:FAIL@1.0",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--enable_reload_events=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_catalogd_manual_failover_with_failed_rpc(self, unique_database):
    """Tests for Catalog Service manual fail over with failed RPCs."""
    self.__test_catalogd_manual_failover(unique_database)

    statestore_service = self.cluster.statestored.service
    successful_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    failed_update_catalogd_rpc_num = statestore_service.get_metric_value(
        "statestore.num-failed-update-catalogd-rpc")
    assert successful_update_catalogd_rpc_num >= 10
    assert failed_update_catalogd_rpc_num == successful_update_catalogd_rpc_num

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    impalad_args="--debug_actions=IGNORE_NEW_ACTIVE_CATALOGD_ADDR:FAIL@1.0",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_manual_failover_with_coord_ignore_notification(self):
    """Tests for Catalog Service manual failover with coordinators to ignore failover
    notification."""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service

    # Restart standby catalogd with force_catalogd_active as true.
    standby_catalogd.kill()
    standby_catalogd.start(wait_until_ready=True,
                       additional_args="--force_catalogd_active=true")
    # Wait until original active catalogd becomes in-active.
    catalogd_service_1.wait_for_metric_value(
        "catalog-server.active-status", expected_value=False, timeout=15)
    assert not catalogd_service_1.get_metric_value("catalog-server.active-status")

    # Run query to create a table. Coordinator still send request to catalogd_service_1
    # so that the request will be rejected.
    ddl_query = "CREATE TABLE coordinator_ignore_notification (c int)"
    ex = self.execute_query_expect_failure(self.client, ddl_query)
    assert "Request for Catalog service is rejected" in str(ex)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_restart_statestore(self, unique_database):
    """The test case for restarting statestore after the cluster is created with
    catalogd HA enabled."""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service

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
    if not active_catalogd.service.get_metric_value("catalog-server.active-status"):
      active_catalogd, standby_catalogd = standby_catalogd, active_catalogd
    assert active_catalogd.service.get_metric_value("catalog-server.active-status")
    assert not standby_catalogd.service.get_metric_value("catalog-server.active-status")
    catalogd_service_1 = active_catalogd.service

    # Verify ports of the active catalogd of statestore and impalad are matching with
    # the catalog service port of the current active catalogd.
    self.__verify_statestore_active_catalogd_port(catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(0, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(1, catalogd_service_1)
    self.__verify_impalad_active_catalogd_port(2, catalogd_service_1)
    # Verify simple queries are ran successfully.
    self.__run_simple_queries(unique_database)

    unexpected_msg = re.compile("Ignore the update of active catalogd since more recent "
        "update has been processed ([0-9]+ vs [0-9]+)")
    self.assert_catalogd_log_contains("INFO", unexpected_msg, expected_count=0)
    self.assert_impalad_log_contains("INFO", unexpected_msg, expected_count=0)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--force_catalogd_active=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_two_catalogd_with_force_active(self, unique_database):
    """The test case for cluster started with catalogd HA enabled and
    both catalogds started with 'force_catalogd_active' as true.
    Verify that one and only one catalogd is active."""
    sleep_time_s = build_flavor_timeout(2, slow_build_timeout=5)
    sleep(sleep_time_s)
    self.__test_catalogd_ha_with_two_catalogd(unique_database)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--debug_actions='catalogd_wait_sync_ddl_version_delay:SLEEP@{0}'"
                  .format(SYNC_DDL_DELAY_S * 1000),
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_catalogd_failover_with_sync_ddl(self, unique_database):
    """Tests for Catalog Service force fail-over when running DDL with SYNC_DDL
    enabled."""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_1 = active_catalogd.service

    # Run DDL with SYNC_DDL enabled.
    client = self.cluster.impalads[0].service.create_hs2_client()
    assert client is not None
    try:
      client.set_configuration_option('sync_ddl', '1')
      ddl_query = "CREATE TABLE {database}.failover_sync_ddl (c int)"
      handle = client.execute_async(ddl_query.format(database=unique_database))

      # Restart standby catalogd with force_catalogd_active as true.
      start_s = time.time()
      standby_catalogd.kill()
      standby_catalogd.start(wait_until_ready=True,
                         additional_args="--force_catalogd_active=true")
      # Wait until original active catalogd becomes in-active.
      catalogd_service_1.wait_for_metric_value(
          "catalog-server.active-status", expected_value=False, timeout=15)
      assert not catalogd_service_1.get_metric_value("catalog-server.active-status")
      elapsed_s = time.time() - start_s
      assert elapsed_s < SYNC_DDL_DELAY_S, \
          "Catalogd failover took %s seconds to complete" % (elapsed_s)
      LOG.info("Catalogd failover took %s seconds to complete" % round(elapsed_s, 1))

      # Verify that the query is failed due to the Catalogd HA fail-over.
      client.wait_for_impala_state(handle, ERROR, SYNC_DDL_DELAY_S * 2 + 10)
    finally:
      client.close()

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=true "
                  "--catalog_topic_mode=minimal",
    impalad_args="--use_local_catalog=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_metadata_after_failover(self, unique_database):
    self._test_metadata_after_failover(
        unique_database, self._create_native_fn, self._verify_native_fn)
    self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._verify_new_table)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=true "
                  "--catalog_topic_mode=minimal "
                  "--debug_actions=TRIGGER_RESET_METADATA_DELAY:SLEEP@3000",
    impalad_args="--use_local_catalog=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_metadata_after_failover_with_delayed_reset(self, unique_database):
    self._test_metadata_after_failover(
        unique_database, self._create_native_fn, self._verify_native_fn)
    self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._verify_new_table)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--catalog_topic_mode=minimal --enable_reload_events=true "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@1000",
    impalad_args="--use_local_catalog=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_metadata_after_failover_with_hms_sync(self, unique_database):
    self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._verify_new_table)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@2000 "
                  "--enable_reload_events=true --warmup_tables_config_file="
                  "{0}/test-warehouse/warmup_table_list.txt".format(FILESYSTEM_PREFIX),
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_warmed_up_metadata_after_failover(self, unique_database):
    """Verify that the metadata is warmed up in the standby catalogd."""
    for catalogd in self.__get_catalogds():
      self._test_warmed_up_tables(catalogd.service)
    # TODO: due to IMPALA-14210 the standby catalogd can't update the native function
    #  list by applying the ALTER_DATABASE event. So not testing native functions
    #  creation until IMPALA-14210 is resolved.
    active_catalogd, _ = self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._verify_new_table)
    self._test_warmed_up_tables(active_catalogd.service)
    active_catalogd, _ = self._test_metadata_after_failover(
        unique_database, self._drop_table, self._verify_table_dropped)
    self._test_warmed_up_tables(active_catalogd.service)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@3000 "
                  "--catalogd_ha_failover_catchup_timeout_s=2 "
                  "--enable_reload_events=true --warmup_tables_config_file="
                  "{0}/test-warehouse/warmup_table_list.txt".format(FILESYSTEM_PREFIX),
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_failover_catchup_timeout_and_reset(self, unique_database):
    self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._verify_new_table)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@3000 "
                  "--catalogd_ha_failover_catchup_timeout_s=2 "
                  "--catalogd_ha_reset_metadata_on_failover_catchup_timeout=false "
                  "--enable_reload_events=true --warmup_tables_config_file="
                  "{0}/test-warehouse/warmup_table_list.txt".format(FILESYSTEM_PREFIX),
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_failover_catchup_timeout_not_reset(self, unique_database):
    # Skip verifying the table existence since it's missing due to catalog not reset.
    latest_catalogd, _ = self._test_metadata_after_failover(
        unique_database, self._create_new_table, self._noop_verifier)
    # Verify tables are still loaded
    self._test_warmed_up_tables(latest_catalogd.service)
    # Run a global IM to bring up 'unique_database' in the new catalogd. Otherwise, the
    # cleanup_database step will fail.
    self.execute_query("invalidate metadata")

  def _test_warmed_up_tables(self, catalogd):
    db = "tpcds"
    tables = ["customer", "date_dim", "item", "store_sales"]
    for table in tables:
      catalogd.verify_table_metadata_loaded(db, table)
    catalogd.verify_table_metadata_loaded(db, "store", expect_loaded=False)

  @CustomClusterTestSuite.with_args(
    statestored_args="--use_subscriber_id_as_catalogd_priority=true",
    catalogd_args="--catalog_topic_mode=minimal "
                  "--catalogd_ha_reset_metadata_on_failover=false "
                  "--debug_actions=catalogd_event_processing_delay:SLEEP@1000 "
                  "--enable_reload_events=true --warmup_tables_config_file="
                  "file://%s/testdata/data/warmup_test_config.txt" % IMPALA_HOME,
    impalad_args="--catalog_client_connection_num_retries=2 "
                 "--use_local_catalog=true",
    start_args="--enable_catalogd_ha",
    disable_log_buffering=True)
  def test_warmed_up_metadata_failover_catchup(self):
    """All tables under the 'warmup_test_db' will be warmed up based on the config.
    Use local-catalog mode so coordinator needs to fetch metadata from catalogd after
    each DDL. Use a smaller catalog_client_connection_num_retries since RPC retries will
    all fail due to IMPALA-14228. We retry the query instead."""
    db = "warmup_test_db"
    self.execute_query("create database if not exists " + db)
    try:
      self._test_metadata_after_failover(
          db, self._create_new_table, self._verify_new_table)
      active_catalogd, _ = self._test_metadata_after_failover(
          db, self._add_new_partition, self._verify_new_partition)
      active_catalogd.service.verify_table_metadata_loaded(db, "tbl")
      active_catalogd, _ = self._test_metadata_after_failover(
          db, self._refresh_table, self._verify_refresh)
      active_catalogd.service.verify_table_metadata_loaded(db, "tbl")
      active_catalogd, _ = self._test_metadata_after_failover(
        db, self._drop_partition, self._verify_no_partitions)
      active_catalogd.service.verify_table_metadata_loaded(db, "tbl")
      active_catalogd, _ = self._test_metadata_after_failover(
        db, self._insert_table, self._verify_insert)
      active_catalogd.service.verify_table_metadata_loaded(db, "tbl")
      self._test_metadata_after_failover(
          db, self._drop_table, self._verify_table_dropped)
    finally:
      for i in range(2):
        try:
          self.execute_query("drop database if exists %s cascade" % db)
          break
        except Exception as e:
          # Retry in case we hit IMPALA-14228.
          LOG.warn("Ignored cleanup failure: " + str(e))

  def _create_native_fn(self, unique_database):
    create_func_impala = ("create function {database}.identity_tmp(bigint) "
                          "returns bigint location '{location}' symbol='Identity'")
    self.client.execute(create_func_impala.format(
      database=unique_database,
      location=get_fs_path('/test-warehouse/libTestUdfs.so')))
    self.execute_query_expect_success(
      self.client, "select %s.identity_tmp(10)" % unique_database)

  def _verify_native_fn(self, unique_database):
    self.execute_query_expect_success(
      self.client, "select %s.identity_tmp(10)" % unique_database)

  def _create_new_table(self, unique_database):
    table_location = get_fs_path("/test-warehouse/%s.tbl" % unique_database)
    self.client.execute("create table %s.tbl like functional.alltypes"
                        " stored as parquet location '%s'"
                        % (unique_database, table_location))

  def _verify_new_table(self, unique_database):
    self.execute_query("describe %s.tbl" % unique_database)

  def _drop_table(self, unique_database):
    self.client.execute("drop table %s.tbl" % unique_database)

  def _verify_table_dropped(self, unique_database):
    res = self.client.execute("show tables in " + unique_database)
    assert len(res.data) == 0

  def _add_new_partition(self, unique_database):
    self.execute_query("alter table %s.tbl add partition(year=2025, month=1)" %
                       unique_database)

  def _verify_new_partition(self, unique_database):
    res = self.execute_query("show partitions %s.tbl" % unique_database)
    LOG.info("partition result: {}".format(res.data))
    assert "year=2025/month=1" in res.data[0]

  def _refresh_table(self, unique_database):
    """Add a new file to the table and refresh it"""
    table_location = get_fs_path("/test-warehouse/%s.tbl" % unique_database)
    src_file = get_fs_path("/test-warehouse/alltypesagg_parquet/year=2010/month=1/"
                           "day=9/*.parq")
    dst_path = "%s/year=2025/month=1/alltypes.parq" % table_location
    self.filesystem_client.copy(src_file, dst_path, overwrite=True)
    self.execute_query("refresh %s.tbl partition (year=2025, month=1)" % unique_database)

  def _verify_refresh(self, unique_database):
    res = self.execute_query("select count(*) from %s.tbl where year=2025 and month=1"
                             % unique_database)
    assert res.data == ["1000"]

  def _drop_partition(self, unique_database):
    self.execute_query("alter table %s.tbl drop partition(year=2025, month=1)"
                       % unique_database)

  def _verify_no_partitions(self, unique_database):
    res = self.execute_query("show partitions %s.tbl" % unique_database)
    # The result should only have the "Total" line.
    assert len(res.data) == 1

  def _insert_table(self, unique_database):
    self.execute_query("insert into %s.tbl partition(year, month)"
                       " select * from functional.alltypes" % unique_database)

  def _verify_insert(self, unique_database):
    res = self.execute_scalar("select count(*) from %s.tbl" % unique_database)
    assert res == '7300'

  def _noop_verifier(self, unique_database):  # noqa: U100
    pass

  def _test_metadata_after_failover(self, unique_database, metadata_op_fn, verifier_fn):
    """Verify that the metadata is correct after failover. Returns the updated tuple of
    (active_catalogd, standby_catalogd)"""
    (active_catalogd, standby_catalogd) = self.__get_catalogds()
    catalogd_service_2 = standby_catalogd.service

    metadata_op_fn(unique_database)

    # Kill active catalogd
    active_catalogd.kill()

    # Wait for long enough for the statestore to detect the failure of active catalogd
    # and assign active role to standby catalogd.
    catalogd_service_2.wait_for_metric_value(
        "catalog-server.active-status", expected_value=True, timeout=30)
    assert catalogd_service_2.get_metric_value(
        "catalog-server.ha-number-active-status-change") > 0
    assert catalogd_service_2.get_metric_value("catalog-server.active-status")
    # Make sure coordinator has updated the active catalogd address.
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "catalog.active-catalogd-address",
        expected_value="{}:{}".format(catalogd_service_2.hostname,
                                      catalogd_service_2.service_port))

    for i in range(2):
      try:
        verifier_fn(unique_database)
        break
      except Exception as e:
        if i == 0:
          # Due to IMPALA-14228, we allow retry on connection failure to the previous
          # active catalogd. Example error message:
          # Couldn't open transport for xxx:26000 (connect() failed: Connection refused)
          assert str(active_catalogd.service.service_port) in str(e)
          LOG.info("Retry for error " + str(e))
          continue
        assert False, str(e)

    # Recover the cluster to have two catalogds
    active_catalogd.start()
    return standby_catalogd, active_catalogd

  def test_page_with_disable_ha(self):
    self.__test_catalog_ha_info_page()

  @CustomClusterTestSuite.with_args(start_args="--enable_catalogd_ha")
  def test_page_with_enable_ha(self):
    self.__test_catalog_ha_info_page()

  def __test_catalog_ha_info_page(self):
    for port in self.SS_TEST_PORT:
      response = requests.get(self.VARZ_URL.format(port) + "?json")
      assert response.status_code == requests.codes.ok
      varz_json = json.loads(response.text)
      ha_flags = [e for e in varz_json["flags"]
              if e["name"] == "enable_catalogd_ha"]
      assert len(ha_flags) == 1
      assert ha_flags[0]["default"] == "false"

      # High availability for the Catalog is enabled.
      if ha_flags[0]["current"] == "true":
        url = self.JSON_METRICS_URL.format(port) + "?json"
        metrics = json.loads(requests.get(url).text)
        if metrics["statestore.active-status"]:
          url = self.CATALOG_HA_INFO_URL.format(port) + "?json"
          catalog_ha_info = json.loads(requests.get(url).text)
          assert catalog_ha_info["active_catalogd_address"]\
                 == metrics["statestore.active-catalogd-address"]
        else:
          reponse = requests.get(self.CATALOG_HA_INFO_URL.format(port)).text
          assert reponse.__contains__("The current statestored is inactive.")
      else:
        page = requests.get(self.CATALOG_HA_INFO_URL.format(port))
        assert page.status_code == requests.codes.not_found
