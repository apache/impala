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

from beeswaxd.BeeswaxService import QueryState
from builtins import round
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.util.filesystem_utils import IS_S3, get_fs_path
from time import sleep

LOG = logging.getLogger('catalogd_ha_test')
DEFAULT_STATESTORE_SERVICE_PORT = 24000
DEFAULT_CATALOG_SERVICE_PORT = 26000
SLOW_BUILD_SYNC_DDL_DELAY_S = 20
SYNC_DDL_DELAY_S = build_flavor_timeout(
    10, slow_build_timeout=SLOW_BUILD_SYNC_DDL_DELAY_S)
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

  SS_TEST_PORT = ["25010"]

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
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false",
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
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false",
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
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false",
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
    catalogd_args="--catalogd_ha_reset_metadata_on_failover=false",
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
    statestored_args="--use_subscriber_id_as_catalogd_priority=true "
                     "--statestore_heartbeat_frequency_ms=1000",
    impalad_args="--debug_actions=IGNORE_NEW_ACTIVE_CATALOGD_ADDR:FAIL@1.0",
    start_args="--enable_catalogd_ha")
  def test_manual_failover_with_coord_ignore_notification(self):
    """Tests for Catalog Service manual failover with coordinators to ignore failover
    notification."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Restart standby catalogd with force_catalogd_active as true.
    catalogds[1].kill()
    catalogds[1].start(wait_until_ready=True,
                       additional_args="--force_catalogd_active=true")
    # Wait until original active catalogd becomes in-active.
    catalogd_service_1 = catalogds[0].service
    catalogd_service_1.wait_for_metric_value(
        "catalog-server.active-status", expected_value=False, timeout=15)
    assert(not catalogd_service_1.get_metric_value("catalog-server.active-status"))

    # Run query to create a table. Coordinator still send request to catalogd_service_1
    # so that the request will be rejected.
    ddl_query = "CREATE TABLE coordinator_ignore_notification (c int)"
    ex = self.execute_query_expect_failure(self.client, ddl_query)
    assert "Request for Catalog service is rejected" in str(ex)

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
    catalogd_args="--debug_actions='catalogd_wait_sync_ddl_version_delay:SLEEP@{0}'"
                  .format(SYNC_DDL_DELAY_S * 1000),
    start_args="--enable_catalogd_ha")
  def test_catalogd_failover_with_sync_ddl(self, unique_database):
    """Tests for Catalog Service force fail-over when running DDL with SYNC_DDL
    enabled."""
    # Verify two catalogd instances are created with one as active.
    catalogds = self.cluster.catalogds()
    assert(len(catalogds) == 2)
    catalogd_service_1 = catalogds[0].service
    catalogd_service_2 = catalogds[1].service
    assert(catalogd_service_1.get_metric_value("catalog-server.active-status"))
    assert(not catalogd_service_2.get_metric_value("catalog-server.active-status"))

    # Run DDL with SYNC_DDL enabled.
    client = self.cluster.impalads[0].service.create_beeswax_client()
    assert client is not None
    try:
      self.execute_query_expect_success(client, "set SYNC_DDL=1")
      ddl_query = "CREATE TABLE {database}.failover_sync_ddl (c int)"
      handle = client.execute_async(ddl_query.format(database=unique_database))

      # Restart standby catalogd with force_catalogd_active as true.
      start_s = time.time()
      catalogds[1].kill()
      catalogds[1].start(wait_until_ready=True,
                         additional_args="--force_catalogd_active=true")
      # Wait until original active catalogd becomes in-active.
      catalogd_service_1 = catalogds[0].service
      catalogd_service_1.wait_for_metric_value(
          "catalog-server.active-status", expected_value=False, timeout=15)
      assert(not catalogd_service_1.get_metric_value("catalog-server.active-status"))
      elapsed_s = time.time() - start_s
      assert elapsed_s < SYNC_DDL_DELAY_S, \
          "Catalogd failover took %s seconds to complete" % (elapsed_s)
      LOG.info("Catalogd failover took %s seconds to complete" % round(elapsed_s, 1))

      # Verify that the query is failed due to the Catalogd HA fail-over.
      self.wait_for_state(
          handle, QueryState.EXCEPTION, SYNC_DDL_DELAY_S * 2 + 10, client=client)
    finally:
      client.close()

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
