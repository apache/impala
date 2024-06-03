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
from builtins import range
import logging
import os
import pytest
import psutil
import re
import signal
import socket
import time
import threading

from subprocess import check_call
from tests.common.environ import build_flavor_timeout
from time import sleep

from impala.error import HiveServer2Error
from TCLIService import TCLIService

from beeswaxd.BeeswaxService import QueryState
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfNotHdfsMinicluster, SkipIfFS
from tests.hs2.hs2_test_suite import HS2TestSuite, needs_session

LOG = logging.getLogger(__name__)


class TestRestart(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_restart_statestore(self, cursor):
    """ Regression test of IMPALA-6973. After the statestore restarts, the metadata should
        eventually recover after being cleared by the new statestore.
    """

    self.cluster.statestored.restart()
    # We need to wait for the impalad to register to the new statestored and for a
    # non-empty catalog update from the new statestored. It cannot be expressed with the
    # existing metrics yet so we wait for some time here.
    wait_time_s = build_flavor_timeout(60, slow_build_timeout=100)
    sleep(wait_time_s)
    for retry in range(wait_time_s):
      try:
        cursor.execute("describe database functional")
        return
      except HiveServer2Error as e:
        assert "AnalysisException: Database does not exist: functional" in str(e),\
               "Unexpected exception: " + str(e)
        sleep(1)
    assert False, "Coordinator never received non-empty metadata from the restarted " \
           "statestore after {0} seconds".format(wait_time_s)

  @pytest.mark.execute_serially
  def test_restart_impala(self):
      """ This test aims to restart Impalad executor nodes between queries to exercise
      the cluster membership callback which removes stale connections to the restarted
      nodes."""

      self._start_impala_cluster([], num_coordinators=1, cluster_size=3)
      assert len(self.cluster.impalads) == 3

      client = self.cluster.impalads[0].service.create_beeswax_client()
      assert client is not None

      for i in range(5):
        self.execute_query_expect_success(client, "select * from functional.alltypes")
        node_to_restart = 1 + (i % 2)
        self.cluster.impalads[node_to_restart].restart()
        # Sleep for a bit for the statestore change in membership to propagate. The min
        # update frequency for statestore is 100ms but using a larger sleep time here
        # as certain builds (e.g. ASAN) can be really slow.
        sleep(3)

      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      # Debug action to delay statestore updates to give the restarted impalad time to
      # register itself before a membership topic update is generated.
      statestored_args="--debug_actions=DO_SUBSCRIBER_UPDATE:JITTER@10000")
  def test_statestore_update_after_impalad_restart(self):
      """Test that checks that coordinators are informed that an impalad went down even if
      the statestore doesn't send a membership update until after a new impalad has been
      restarted at the same location."""
      if self.exploration_strategy() != 'exhaustive':
        pytest.skip()

      assert len(self.cluster.impalads) == 3
      client = self.cluster.impalads[0].service.create_beeswax_client()
      assert client is not None

      handle = client.execute_async(
          "select count(*) from functional.alltypes where id = sleep(100000)")
      node_to_restart = self.cluster.impalads[2]
      node_to_restart.restart()
      # Verify that the query is cancelled due to the failed impalad quickly.
      self.wait_for_state(handle, QueryState.EXCEPTION, 20, client=client)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args="--catalog_topic_mode=minimal",
      impalad_args="--use_local_catalog=true")
  def test_catalog_connection_retries(self):
    """Test that connections to the catalogd are retried, both new connections and cached
    connections."""
    # Since this is a custom cluster test, each impalad should start off with no cached
    # connections to the catalogd. So the first call to __test_catalog_connection_retries
    # should test that new connections are retried.
    coordinator_service = self.cluster.impalads[0].service
    assert coordinator_service.get_metric_value(
        "catalog.server.client-cache.total-clients") == 0
    self.__test_catalog_connection_retries()

    # Since a query was just run that required loading metadata from the catalogd, there
    # should be a cached connection to the catalogd, so the second call to
    # __test_catalog_connection_retries should assert that broken cached connections are
    # retried.
    assert coordinator_service.get_metric_value(
        "catalog.server.client-cache.total-clients") == 1
    self.__test_catalog_connection_retries()

  def __test_catalog_connection_retries(self):
    """Test that a query retries connecting to the catalogd. Kills the catalogd, launches
    a query that requires catalogd access, starts the catalogd, and then validates that
    the query eventually finishes successfully."""
    self.cluster.catalogd.kill_and_wait_for_exit()

    query = "select * from functional.alltypes limit 10"
    query_handle = []

    # self.execute_query_async has to be run in a dedicated thread because it does not
    # truly run a query asynchronously. The query compilation has to complete before
    # execute_query_async can return. Since compilation requires catalogd access,
    # execute_query_async won't return until the catalogd is up and running.
    def execute_query_async():
      query_handle.append(self.execute_query_async(query))

    thread = threading.Thread(target=execute_query_async)
    thread.start()
    # Sleep until the query actually starts to try and access the catalogd. Set an
    # explicitly high value to avoid any race conditions. The connection is retried 3
    # times by default with a 10 second interval, so a high sleep time should not cause
    # any timeouts.
    sleep(5)

    self.cluster.catalogd.start()
    thread.join()
    max_wait_time = 300
    finished = self.client.wait_for_finished_timeout(query_handle[0], max_wait_time)
    assert finished, "Statement did not finish after {0} seconds".format(max_wait_time)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=5000 "
                     "--statestore_heartbeat_frequency_ms=10000")
  def test_restart_catalogd(self, unique_database):
    tbl_name = unique_database + ".join_aa"
    self.execute_query_expect_success(
        self.client, "create table {}(id int)".format(tbl_name))
    # Make the catalog object version grow large enough
    self.execute_query_expect_success(self.client, "invalidate metadata")

    # No need to care whether the dll is executed successfully, it is just to make
    # the local catalog cache of impalad out of sync
    for i in range(0, 10):
      try:
        query = "alter table {} add columns (age{} int)".format(tbl_name, i)
        self.execute_query_async(query)
      except Exception as e:
        LOG.info(str(e))
      if i == 5:
        self.cluster.catalogd.restart()

    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name))
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))

  WAIT_FOR_CATALOG_UPDATE_TIMEOUT_SEC = 5

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1,
    statestored_args="--statestore_update_frequency_ms=2000",
    impalad_args=("--wait_for_new_catalog_service_id_timeout_sec={} \
                  --wait_for_new_catalog_service_id_max_iterations=-1"
                  .format(WAIT_FOR_CATALOG_UPDATE_TIMEOUT_SEC)))
  def test_restart_catalogd_while_handling_rpc_response_with_timeout(self,
      unique_database):
    """Regression test for IMPALA-12267. We'd like to cause a situation where
         - The coordinator issues a DDL or DML query
         - Catalogd sends a response RPC
         - Catalogd is restarted and gets a new catalog service ID
         - The coordinator receives the update about the new catalogd from the statestore
           before processing the RPC from the old catalogd.
    Before IMPALA-12267 the coordinator hung infinitely in this situation, waiting for a
    statestore update with a new catalog service ID assuming the service ID it had was
    stale, but it already had the most recent one."""
    tbl_name = unique_database + ".handling_rpc_response_with_timeout"
    self.execute_query_expect_success(
        self.client, "create table {}(id int)".format(tbl_name))
    # Make the catalog object version grow large enough
    self.execute_query_expect_success(self.client, "invalidate metadata")

    # IMPALA-12616: If this sleep is not long enough, the alter table could wake up
    # before the new catalog service ID is finalized, and the query can fail due to the
    # difference in the service ID. This was a particular problem on s3, which runs a
    # bit slower.
    debug_action_sleep_time_sec = 30
    DEBUG_ACTION = ("WAIT_BEFORE_PROCESSING_CATALOG_UPDATE:SLEEP@{}"
                    .format(debug_action_sleep_time_sec * 1000))

    query = "alter table {} add columns (age int)".format(tbl_name)
    handle = self.execute_query_async(query, query_options={"debug_action": DEBUG_ACTION})

    # Wait a bit so the RPC from the catalogd arrives to the coordinator. Using a generous
    # value here gives the catalogd plenty of time to respond.
    time.sleep(5)

    self.cluster.catalogd.restart()

    # Wait for the query to finish.
    max_wait_time = (debug_action_sleep_time_sec
        + self.WAIT_FOR_CATALOG_UPDATE_TIMEOUT_SEC + 10)
    finished = self.client.wait_for_finished_timeout(handle, max_wait_time)
    assert finished, "Statement did not finish after {0} seconds".format(max_wait_time)

    self.assert_impalad_log_contains("WARNING",
        "Waiting for catalog update with a new catalog service ID timed out.")
    self.assert_impalad_log_contains("WARNING",
        "Ignoring catalog update result of catalog service ID")

    # Clear the query options so the following statements don't use the debug_action
    # set above.
    self.client.clear_configuration()

    self.execute_query_expect_success(self.client, "select age from {}".format(tbl_name))

    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name))
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))

  WAIT_FOR_CATALOG_UPDATE_MAX_ITERATIONS = 3
  STATESTORE_UPDATE_FREQ_SEC = 2

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1,
    statestored_args="--statestore_update_frequency_ms={}".format(
        STATESTORE_UPDATE_FREQ_SEC * 1000),
    impalad_args=("--wait_for_new_catalog_service_id_timeout_sec=-1 \
                  --wait_for_new_catalog_service_id_max_iterations={}"
                  .format(WAIT_FOR_CATALOG_UPDATE_MAX_ITERATIONS)))
  def test_restart_catalogd_while_handling_rpc_response_with_max_iters(self,
      unique_database):
    """We create the same situation as described in
    'test_restart_catalogd_while_handling_rpc_response_with_timeout()' but we get out of
    it not by timing out but by giving up waiting after receiving
    'WAIT_FOR_CATALOG_UPDATE_MAX_ITERATIONS' updates from the statestore that don't change
    the catalog service ID."""
    tbl_name = unique_database + ".handling_rpc_response_with_max_iters"
    self.execute_query_expect_success(
        self.client, "create table {}(id int)".format(tbl_name))
    # Make the catalog object version grow large enough
    self.execute_query_expect_success(self.client, "invalidate metadata")

    # IMPALA-12616: If this sleep is not long enough, the alter table could wake up
    # before the new catalog service ID is finalized, and the query can fail due to the
    # difference in the service ID. This was a particular problem on s3, which runs a
    # bit slower.
    debug_action_sleep_time_sec = 30
    DEBUG_ACTION = ("WAIT_BEFORE_PROCESSING_CATALOG_UPDATE:SLEEP@{}"
                    .format(debug_action_sleep_time_sec * 1000))

    query = "alter table {} add columns (age int)".format(tbl_name)
    handle = self.execute_query_async(query, query_options={"debug_action": DEBUG_ACTION})

    # Wait a bit so the RPC from the catalogd arrives to the coordinator. Using a generous
    # value here gives the catalogd plenty of time to respond.
    time.sleep(5)

    self.cluster.catalogd.restart()

    # Sleep until the coordinator is done with the debug action sleep and it starts
    # waiting for catalog updates.
    time.sleep(debug_action_sleep_time_sec + 0.5)

    # Clear the query options so the following statements don't use the debug_action
    # set above.
    self.client.clear_configuration()

    # Issue DML queries so that the coordinator receives catalog updates.
    for i in range(self.WAIT_FOR_CATALOG_UPDATE_MAX_ITERATIONS):
      try:
        query = "alter table {} add columns (age{} int)".format(tbl_name, i)
        self.execute_query_async(query)
        time.sleep(self.STATESTORE_UPDATE_FREQ_SEC)
      except Exception as e:
        LOG.info(str(e))

    # Wait for the query to finish.
    max_wait_time = 10
    finished = self.client.wait_for_finished_timeout(handle, max_wait_time)
    assert finished, "Statement did not finish after {0} seconds".format(max_wait_time)

    expected_log_msg = "Received {} non-empty catalog updates from the statestore " \
        "while waiting for an update with a new catalog service ID but the catalog " \
        "service ID has not changed. Giving up waiting.".format(
            self.WAIT_FOR_CATALOG_UPDATE_MAX_ITERATIONS)

    self.assert_impalad_log_contains("INFO", expected_log_msg)
    self.assert_impalad_log_contains("WARNING",
        "Ignoring catalog update result of catalog service ID")

    self.execute_query_expect_success(self.client, "select age from {}".format(tbl_name))

    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name))
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms=5000")
  def test_restart_catalogd_sync_ddl(self, unique_database):
    tbl_name = unique_database + ".join_aa"
    self.execute_query_expect_success(
        self.client, "create table {}(id int)".format(tbl_name))
    # Make the catalog object version grow large enough
    self.execute_query_expect_success(self.client, "invalidate metadata")
    query_options = {"sync_ddl": "true"}

    # No need to care whether the dll is executed successfully, it is just to make
    # the local catalog catche of impalad out of sync
    for i in range(0, 10):
      try:
        query = "alter table {} add columns (age{} int)".format(tbl_name, i)
        self.execute_query_async(query, query_options)
      except Exception as e:
        LOG.info(str(e))
      if i == 5:
        self.cluster.catalogd.restart()

    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name), query_options)
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))

  UPDATE_FREQUENCY_S = 10

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_update_frequency_ms={frequency_ms}"
    .format(frequency_ms=(UPDATE_FREQUENCY_S * 1000)))
  def test_restart_catalogd_twice(self, unique_database):
    tbl_name = unique_database + ".join_aa"
    self.cluster.catalogd.restart()
    query = "create table {}(id int)".format(tbl_name)
    query_handle = []

    def execute_query_async():
      query_handle.append(self.execute_query(query))

    thread = threading.Thread(target=execute_query_async)
    thread.start()
    sleep(self.UPDATE_FREQUENCY_S - 5)
    self.cluster.catalogd.restart()
    thread.join()
    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name))
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal",
      statestored_args="--statestore_update_frequency_ms=5000")
  def test_restart_catalogd_with_local_catalog(self, unique_database):
    tbl_name = unique_database + ".join_aa"
    self.execute_query_expect_success(
        self.client, "create table {}(id int)".format(tbl_name))
    # Make the catalog object version grow large enough
    self.execute_query_expect_success(self.client, "invalidate metadata")

    # No need to care whether the dll is executed successfully, it is just to make
    # the local catalog catche of impalad out of sync
    for i in range(0, 10):
      try:
        query = "alter table {} add columns (age{} int)".format(tbl_name, i)
        self.execute_query_async(query)
      except Exception as e:
        LOG.info(str(e))
      if i == 5:
        self.cluster.catalogd.restart()

    self.execute_query_expect_success(self.client,
        "alter table {} add columns (name string)".format(tbl_name))
    self.execute_query_expect_success(self.client, "select name from {}".format(tbl_name))
    self.execute_query_expect_success(self.client, "select age0 from {}".format(tbl_name))

  SUBSCRIBER_TIMEOUT_S = 2
  CANCELLATION_GRACE_PERIOD_S = 5

  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.scheduling
  @CustomClusterTestSuite.with_args(
    impalad_args="--statestore_subscriber_timeout_seconds={timeout_s} "
                 "--statestore_subscriber_recovery_grace_period_ms={recovery_period_ms}"
    .format(timeout_s=SUBSCRIBER_TIMEOUT_S,
            recovery_period_ms=(CANCELLATION_GRACE_PERIOD_S * 1000)),
    catalogd_args="--statestore_subscriber_timeout_seconds={timeout_s}".format(
      timeout_s=SUBSCRIBER_TIMEOUT_S))
  def test_restart_statestore_query_resilience(self):
    """IMPALA-7665: Test that after restarting statestore a momentary inconsistent
    cluster membership state will not result in query cancellation. Also make sure that
    queries get cancelled if a backend actually went down while the statestore was
    down or during the grace period."""
    slow_query = \
      "select distinct * from tpch_parquet.lineitem where l_orderkey > sleep(1000)"
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    try:
      handle = client.execute_async(slow_query)
      # Make sure query starts running.
      self.wait_for_state(handle, QueryState.RUNNING, 1000)
      profile = client.get_runtime_profile(handle)
      assert "NumBackends: 3" in profile, profile
      # Restart Statestore and wait till the grace period ends + some buffer.
      self.cluster.statestored.restart()
      self.cluster.statestored.service.wait_for_live_subscribers(4)
      sleep(self.CANCELLATION_GRACE_PERIOD_S + 1)
      assert client.get_state(handle) == QueryState.RUNNING
      # Now restart statestore and kill a backend while it is down, and make sure the
      # query fails when it comes back up.
      start_time = time.time()
      self.cluster.statestored.kill()
      self.cluster.impalads[1].kill()
      self.cluster.statestored.start()
      try:
        client.wait_for_finished_timeout(handle, 100)
        assert False, "Query expected to fail"
      except ImpalaBeeswaxException as e:
        assert "Failed due to unreachable impalad" in str(e), str(e)
        assert time.time() - start_time > self.CANCELLATION_GRACE_PERIOD_S + \
                                     self.SUBSCRIBER_TIMEOUT_S, \
          "Query got cancelled earlier than the cancellation grace period"
      # Now restart statestore and kill a backend after it comes back up, and make sure
      # the query eventually fails.
      # Make sure the new statestore has received update from catalog and sent it to the
      # impalad.
      catalogd_version = self.cluster.catalogd.service.get_catalog_version()
      impalad.service.wait_for_metric_value("catalog.curr-version", catalogd_version)
      handle = client.execute_async(slow_query)
      self.wait_for_state(handle, QueryState.RUNNING, 1000)
      profile = client.get_runtime_profile(handle)
      assert "NumBackends: 2" in profile, profile
      start_time = time.time()
      self.cluster.statestored.restart()
      # Make sure it has connected to the impalads before killing one.
      self.cluster.statestored.service.wait_for_live_subscribers(3)
      self.cluster.impalads[2].kill()
      try:
        client.wait_for_finished_timeout(handle, 100)
        assert False, "Query expected to fail"
      except ImpalaBeeswaxException as e:
        assert "Failed due to unreachable impalad" in str(e), str(e)
        assert time.time() - start_time > self.CANCELLATION_GRACE_PERIOD_S + \
                                     self.SUBSCRIBER_TIMEOUT_S, \
          "Query got cancelled earlier than the cancellation grace period"
    finally:
      client.close()


def parse_shutdown_result(result):
  """Parse the shutdown result string and return the strings (grace left,
  deadline left, queries registered, queries executing)."""
  assert len(result.data) == 1
  summary = result.data[0]
  match = re.match(r'shutdown grace period left: ([0-9ms]*), deadline left: ([0-9ms]*), '
                   r'queries registered on coordinator: ([0-9]*), queries executing: '
                   r'([0-9]*), fragment instances: [0-9]*', summary)
  assert match is not None, summary
  return match.groups()


class TestGracefulShutdown(CustomClusterTestSuite, HS2TestSuite):
  IDLE_SHUTDOWN_GRACE_PERIOD_S = 1
  IMPALA_SHUTDOWN_SIGNAL = signal.SIGRTMIN

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIfFS.shutdown_idle_fails
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --rpc_use_unix_domain_socket=false \
          --hostname={hostname}".format(grace_period=IDLE_SHUTDOWN_GRACE_PERIOD_S,
            hostname=socket.gethostname()))
  def test_shutdown_idle(self):
    """Test that idle impalads shut down in a timely manner after the shutdown grace
    period elapses."""
    impalad1 = psutil.Process(self.cluster.impalads[0].get_pid())
    impalad2 = psutil.Process(self.cluster.impalads[1].get_pid())
    impalad3 = psutil.Process(self.cluster.impalads[2].get_pid())

    # Test that a failed shut down from a bogus host or port fails gracefully.
    ex = self.execute_query_expect_failure(self.client,
        ":shutdown('e6c00ca5cd67b567eb96c6ecfb26f05')")
    assert "Could not find IPv4 address for:" in str(ex)
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:100000')")
    assert "invalid port:" in str(ex)
    assert ("This may be because the port specified is wrong.") not in str(ex)

    # Test that pointing to the wrong thrift service (the HS2 port) fails gracefully-ish.
    thrift_port = 21051  # HS2 port.
    ex = self.execute_query_expect_failure(self.client,
        ":shutdown('localhost:{0}')".format(thrift_port))
    assert ("failed with error 'RemoteShutdown() RPC failed") in str(ex)
    assert ("This may be because the port specified is wrong.") in str(ex)

    # Test RPC error handling with debug action.
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:27001')",
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})
    assert 'Rpc to 127.0.0.1:27001 failed with error \'Debug Action: ' \
        'CRS_SHUTDOWN_RPC:FAIL' in str(ex)

    # Test remote shutdown.
    LOG.info("Start remote shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client, ":shutdown('localhost:27001')",
        query_options={})

    # Remote shutdown does not require statestore.
    self.cluster.statestored.kill()
    self.cluster.statestored.wait_for_exit()
    self.execute_query_expect_success(self.client, ":shutdown('localhost:27002')",
        query_options={})

    # Test local shutdown, which should succeed even with injected RPC error.
    LOG.info("Start local shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client,
        ":shutdown('{0}:27000')".format(socket.gethostname()),
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})

    # Make sure that the impala daemons exit after the shutdown grace period plus a 10
    # second margin of error.
    start_time = time.time()
    LOG.info("Waiting for impalads to exit {0}".format(start_time))
    impalad1.wait()
    LOG.info("First impalad exited {0}".format(time.time()))
    impalad2.wait()
    LOG.info("Second impalad exited {0}".format(time.time()))
    impalad3.wait()
    LOG.info("Third impalad exited {0}".format(time.time()))
    shutdown_duration = time.time() - start_time
    assert shutdown_duration <= self.IDLE_SHUTDOWN_GRACE_PERIOD_S + 10

  @SkipIfFS.shutdown_idle_fails
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --rpc_use_unix_domain_socket=true \
          --hostname={hostname}".format(grace_period=IDLE_SHUTDOWN_GRACE_PERIOD_S,
            hostname=socket.gethostname()))
  def test_shutdown_idle_rpc_use_uds(self):
    """Test that idle impalads shut down in a timely manner after the shutdown grace
    period elapses."""
    impalad1 = psutil.Process(self.cluster.impalads[0].get_pid())
    impalad2 = psutil.Process(self.cluster.impalads[1].get_pid())
    impalad3 = psutil.Process(self.cluster.impalads[2].get_pid())

    # Test that a failed shut down from a bogus host or port fails gracefully.
    ex = self.execute_query_expect_failure(self.client,
        ":shutdown('e6c00ca5cd67b567eb96c6ecfb26f05')")
    assert "Could not find IPv4 address for:" in str(ex)
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:100000')")
    # IMPALA-11129: RPC return different error message for socket over Unix domain socket.
    assert "Connection refused" in str(ex)

    # Test that pointing to the wrong thrift service (the HS2 port) fails gracefully-ish.
    thrift_port = 21051  # HS2 port.
    ex = self.execute_query_expect_failure(self.client,
        ":shutdown('localhost:{0}')".format(thrift_port))
    assert ("failed with error 'RemoteShutdown() RPC failed") in str(ex)
    assert ("This may be because the port specified is wrong.") in str(ex)

    # Test RPC error handling with debug action.
    ex = self.execute_query_expect_failure(self.client, ":shutdown('localhost:27001')",
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})
    assert 'Rpc to 127.0.0.1:27001 failed with error \'Debug Action: ' \
        'CRS_SHUTDOWN_RPC:FAIL' in str(ex)

    # Test remote shutdown.
    LOG.info("Start remote shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client, ":shutdown('localhost:27001')",
        query_options={})

    # Remote shutdown does not require statestore.
    self.cluster.statestored.kill()
    self.cluster.statestored.wait_for_exit()
    self.execute_query_expect_success(self.client, ":shutdown('localhost:27002')",
        query_options={})

    # Test local shutdown, which should succeed even with injected RPC error.
    LOG.info("Start local shutdown {0}".format(time.time()))
    self.execute_query_expect_success(self.client,
        ":shutdown('{0}:27000')".format(socket.gethostname()),
        query_options={'debug_action': 'CRS_SHUTDOWN_RPC:FAIL'})

    # Make sure that the impala daemons exit after the shutdown grace period plus a 10
    # second margin of error.
    start_time = time.time()
    LOG.info("Waiting for impalads to exit {0}".format(start_time))
    impalad1.wait()
    LOG.info("First impalad exited {0}".format(time.time()))
    impalad2.wait()
    LOG.info("Second impalad exited {0}".format(time.time()))
    impalad3.wait()
    LOG.info("Third impalad exited {0}".format(time.time()))
    shutdown_duration = time.time() - start_time
    assert shutdown_duration <= self.IDLE_SHUTDOWN_GRACE_PERIOD_S + 10

  EXEC_SHUTDOWN_GRACE_PERIOD_S = 5
  EXEC_SHUTDOWN_DEADLINE_S = 10

  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.scheduling
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --hostname={hostname}".format(grace_period=EXEC_SHUTDOWN_GRACE_PERIOD_S,
            deadline=EXEC_SHUTDOWN_DEADLINE_S, hostname=socket.gethostname()))
  def test_shutdown_executor(self):
    self.do_test_shutdown_executor(fetch_delay_s=0)

  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.scheduling
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --stress_status_report_delay_ms={status_report_delay_ms} \
          --hostname={hostname}".format(grace_period=EXEC_SHUTDOWN_GRACE_PERIOD_S,
            deadline=EXEC_SHUTDOWN_DEADLINE_S, status_report_delay_ms=5000,
            hostname=socket.gethostname()))
  def test_shutdown_executor_with_delay(self):
    """Regression test for IMPALA-7931 that adds delays to status reporting and
    to fetching of results to trigger races that previously resulted in query failures."""
    print(self.exploration_strategy)
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip()
    self.do_test_shutdown_executor(fetch_delay_s=5)

  def do_test_shutdown_executor(self, fetch_delay_s):
    """Implementation of test that shuts down and then restarts an executor. This should
    not disrupt any queries that start after the shutdown or complete before the shutdown
    time limit. The test is parameterized by 'fetch_delay_s', the amount to delay before
    fetching from the query that must survive shutdown of an executor."""
    # Add sleeps to make sure that the query takes a couple of seconds to execute on the
    # executors.
    QUERY = "select count(*) from functional_parquet.alltypes where sleep(1) = bool_col"
    # Subtle: use a splittable file format like text for lineitem so that each backend
    # is guaranteed to get scan ranges that contain some actual rows. With Parquet on
    # S3, the files get broken into 32MB scan ranges and a backend might get unlucky
    # and only get scan ranges that don't contain the midpoint of any row group, and
    # therefore not actually produce any rows.
    SLOW_QUERY = "select count(*) from tpch.lineitem where sleep(1) = l_orderkey"
    SHUTDOWN_EXEC2 = ": shutdown('localhost:27001')"

    # Run this query before shutdown and make sure that it executes successfully on
    # all executors through the shutdown grace period without disruption.
    before_shutdown_handle = self.__exec_and_wait_until_running(QUERY)

    # Run this query which simulates getting stuck in admission control until after
    # the shutdown grace period expires. This demonstrates that queries don't get
    # cancelled if the cluster membership changes while they're waiting for admission.
    before_shutdown_admission_handle = self.execute_query_async(QUERY,
        {'debug_action': 'AC_BEFORE_ADMISSION:SLEEP@30000'})

    # Shut down and wait for the shutdown state to propagate through statestore.
    result = self.execute_query_expect_success(self.client, SHUTDOWN_EXEC2)
    assert parse_shutdown_result(result) == (
        "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S),
        "{0}s000ms".format(self.EXEC_SHUTDOWN_DEADLINE_S), "0", "1")

    # Check that the status is reflected on the debug page.
    web_json = self.cluster.impalads[1].service.get_debug_webpage_json("")
    assert web_json.get('is_quiescing', None) is True, web_json
    assert 'shutdown_status' in web_json, web_json

    self.impalad_test_service.wait_for_num_known_live_backends(2,
        timeout=self.EXEC_SHUTDOWN_GRACE_PERIOD_S + 5, interval=0.2,
        include_shutting_down=False)

    # Run another query, which shouldn't get scheduled on the new executor. We'll let
    # this query continue running through the full shutdown and restart cycle.
    after_shutdown_handle = self.__exec_and_wait_until_running(QUERY)

    # Wait for the impalad to exit, then start it back up and run another query, which
    # should be scheduled on it again.
    self.cluster.impalads[1].wait_for_exit()

    # Finish fetching results from the first query (which will be buffered on the
    # coordinator) after the backend exits. Add a delay before fetching to ensure
    # that the query is not torn down on the coordinator when the failure is
    # detected by the statestore (see IMPALA-7931).
    assert self.__fetch_and_get_num_backends(
        QUERY, before_shutdown_handle, delay_s=fetch_delay_s) == 3

    # Confirm that the query stuck in admission succeeded.
    assert self.__fetch_and_get_num_backends(
        QUERY, before_shutdown_admission_handle, timeout_s=30) == 2

    # Start the impalad back up and run another query, which should be scheduled on it
    # again.
    self.cluster.impalads[1].start()
    self.impalad_test_service.wait_for_num_known_live_backends(
        3, timeout=30, interval=0.2, include_shutting_down=False)
    after_restart_handle = self.__exec_and_wait_until_running(QUERY)

    # The query started while the backend was shut down should not run on that backend.
    assert self.__fetch_and_get_num_backends(QUERY, after_shutdown_handle) == 2
    assert self.__fetch_and_get_num_backends(QUERY, after_restart_handle) == 3

    # Test that a query will fail when the executor shuts down after the limit.
    deadline_expiry_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    result = self.execute_query_expect_success(self.client, SHUTDOWN_EXEC2)
    assert parse_shutdown_result(result) == (
        "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S),
        "{0}s000ms".format(self.EXEC_SHUTDOWN_DEADLINE_S), "0", "1")
    self.cluster.impalads[1].wait_for_exit()
    self.__check_deadline_expired(SLOW_QUERY, deadline_expiry_handle)

    # Test that we can reduce the deadline after setting it to a high value.
    # Run a query that will fail as a result of the reduced deadline.
    deadline_expiry_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    SHUTDOWN_EXEC3 = ": shutdown('localhost:27002', {0})"
    VERY_HIGH_DEADLINE = 5000
    HIGH_DEADLINE = 1000
    LOW_DEADLINE = 5
    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(HIGH_DEADLINE))
    grace, deadline, _, _ = parse_shutdown_result(result)
    assert grace == "{0}s000ms".format(self.EXEC_SHUTDOWN_GRACE_PERIOD_S)
    assert deadline == "{0}m{1}s".format(HIGH_DEADLINE // 60, HIGH_DEADLINE % 60)

    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(VERY_HIGH_DEADLINE))
    _, deadline, _, _ = parse_shutdown_result(result)
    LOG.info("Deadline is {0}".format(deadline))
    min_string, sec_string = re.match("([0-9]*)m([0-9]*)s", deadline).groups()
    assert int(min_string) * 60 + int(sec_string) <= HIGH_DEADLINE, \
        "Cannot increase deadline " + deadline

    result = self.execute_query_expect_success(
        self.client, SHUTDOWN_EXEC3.format(LOW_DEADLINE))
    _, deadline, _, queries_executing = parse_shutdown_result(result)
    assert deadline == "{0}s000ms".format(LOW_DEADLINE)
    assert int(queries_executing) > 0, "Slow query should still be running."
    self.cluster.impalads[2].wait_for_exit()
    self.__check_deadline_expired(SLOW_QUERY, deadline_expiry_handle)

  COORD_SHUTDOWN_GRACE_PERIOD_S = 5
  COORD_SHUTDOWN_DEADLINE_S = 120

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --hostname={hostname}".format(
          grace_period=COORD_SHUTDOWN_GRACE_PERIOD_S,
          deadline=COORD_SHUTDOWN_DEADLINE_S, hostname=socket.gethostname()),
      default_query_options=[("num_scanner_threads", "1")])
  @needs_session(TCLIService.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
                 close_session=False)
  def test_shutdown_coordinator(self):
    """Test that shuts down the coordinator. Running queries should finish but new
    requests should be rejected."""
    # Start a query running. This should complete successfully and keep the coordinator
    # up until it finishes. We set NUM_SCANNER_THREADS=1 above to make the runtime more
    # predictable.
    SLOW_QUERY = """select * from tpch_parquet.lineitem where sleep(1) < l_orderkey"""
    SHUTDOWN = ": shutdown()"
    SHUTDOWN_ERROR_PREFIX = 'Server is being shut down:'

    before_shutdown_handle = self.__exec_and_wait_until_running(SLOW_QUERY)
    before_shutdown_hs2_handle = self.execute_statement(SLOW_QUERY).operationHandle

    # Shut down the coordinator. Operations that start after this point should fail.
    result = self.execute_query_expect_success(self.client, SHUTDOWN)
    grace, deadline, registered, _ = parse_shutdown_result(result)
    assert grace == "{0}s000ms".format(self.COORD_SHUTDOWN_GRACE_PERIOD_S)
    assert deadline == "{0}m".format(self.COORD_SHUTDOWN_DEADLINE_S // 60), "4"
    assert registered == "3"

    # Expect that the beeswax shutdown error occurs when calling fn()
    def expect_beeswax_shutdown_error(fn):
      try:
        fn()
      except ImpalaBeeswaxException as e:
        assert SHUTDOWN_ERROR_PREFIX in str(e)
    expect_beeswax_shutdown_error(lambda: self.client.execute("select 1"))
    expect_beeswax_shutdown_error(lambda: self.client.execute_async("select 1"))

    # Test that the HS2 shutdown error occurs for various HS2 operations.
    self.execute_statement("select 1", None, TCLIService.TStatusCode.ERROR_STATUS,
        SHUTDOWN_ERROR_PREFIX)

    def check_hs2_shutdown_error(hs2_response):
      HS2TestSuite.check_response(hs2_response, TCLIService.TStatusCode.ERROR_STATUS,
        SHUTDOWN_ERROR_PREFIX)
    check_hs2_shutdown_error(self.hs2_client.OpenSession(TCLIService.TOpenSessionReq()))
    check_hs2_shutdown_error(self.hs2_client.GetInfo(TCLIService.TGetInfoReq(
        self.session_handle, TCLIService.TGetInfoType.CLI_MAX_DRIVER_CONNECTIONS)))
    check_hs2_shutdown_error(self.hs2_client.GetTypeInfo(
        TCLIService.TGetTypeInfoReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetCatalogs(
        TCLIService.TGetCatalogsReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetSchemas(
        TCLIService.TGetSchemasReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetTables(
        TCLIService.TGetTablesReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetTableTypes(
        TCLIService.TGetTableTypesReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetColumns(
        TCLIService.TGetColumnsReq(self.session_handle)))
    check_hs2_shutdown_error(self.hs2_client.GetFunctions(
        TCLIService.TGetFunctionsReq(self.session_handle, functionName="")))

    # Operations on running HS2 query still work.
    self.fetch_until(before_shutdown_hs2_handle,
        TCLIService.TFetchOrientation.FETCH_NEXT, 10)
    HS2TestSuite.check_response(self.hs2_client.CancelOperation(
        TCLIService.TCancelOperationReq(before_shutdown_hs2_handle)))
    HS2TestSuite.check_response(self.hs2_client.CloseOperation(
        TCLIService.TCloseOperationReq(before_shutdown_hs2_handle)))

    # Make sure that the beeswax query is still executing, then close it to allow the
    # coordinator to shut down.
    self.impalad_test_service.wait_for_query_state(self.client, before_shutdown_handle,
          self.client.QUERY_STATES['FINISHED'], timeout=20)
    self.client.close_query(before_shutdown_handle)
    self.cluster.impalads[0].wait_for_exit()

  def __exec_and_wait_until_running(self, query, timeout=20):
    """Execute 'query' with self.client and wait until it is in the RUNNING state.
    'timeout' controls how long we will wait"""
    # Fix number of scanner threads to make runtime more deterministic.
    handle = self.execute_query_async(query, {'num_scanner_threads': 1})
    self.impalad_test_service.wait_for_query_state(self.client, handle,
                self.client.QUERY_STATES['RUNNING'], timeout=20)
    return handle

  def __fetch_and_get_num_backends(self, query, handle, delay_s=0, timeout_s=20):
    """Fetch the results of 'query' from the beeswax handle 'handle', close the
    query and return the number of backends obtained from the profile."""
    self.impalad_test_service.wait_for_query_state(self.client, handle,
                self.client.QUERY_STATES['FINISHED'], timeout=timeout_s)
    if delay_s > 0:
      LOG.info("sleeping for {0}s".format(delay_s))
      time.sleep(delay_s)
    self.client.fetch(query, handle)
    profile = self.client.get_runtime_profile(handle)
    self.client.close_query(handle)
    backends_match = re.search("NumBackends: ([0-9]*)", profile)
    assert backends_match is not None, profile
    return int(backends_match.group(1))

  def __check_deadline_expired(self, query, handle):
    """Check that the query with 'handle' fails because of a backend hitting the
    deadline and shutting down."""
    try:
      self.client.fetch(query, handle)
      assert False, "Expected query to fail"
    except Exception as e:
      assert 'Failed due to unreachable impalad(s)' in str(e)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --shutdown_deadline_s={deadline} \
          --hostname={hostname}".format(grace_period=IDLE_SHUTDOWN_GRACE_PERIOD_S,
            deadline=EXEC_SHUTDOWN_DEADLINE_S, hostname=socket.gethostname()),
      cluster_size=1)
  def test_shutdown_signal(self):
    """Test that an idle impalad shuts down in a timely manner after the shutdown grace
    period elapses."""
    impalad = psutil.Process(self.cluster.impalads[0].get_pid())
    LOG.info(
      "Sending IMPALA_SHUTDOWN_SIGNAL(SIGRTMIN = {0}) signal to impalad PID = {1}",
      self.IMPALA_SHUTDOWN_SIGNAL, impalad.pid)
    impalad.send_signal(self.IMPALA_SHUTDOWN_SIGNAL)
    # Make sure that the impala daemon exits after the shutdown grace period plus a 10
    # second margin of error.
    start_time = time.time()
    LOG.info("Waiting for impalad to exit {0}".format(start_time))
    impalad.wait()
    shutdown_duration = time.time() - start_time
    assert shutdown_duration <= self.IDLE_SHUTDOWN_GRACE_PERIOD_S + 10
    # Make sure signal was received and the grace period and deadline are as expected.
    self.assert_impalad_log_contains('INFO',
      "Shutdown signal received. Current Shutdown Status: shutdown grace period left: "
      "{0}s000ms, deadline left: {1}s000ms".format(self.IDLE_SHUTDOWN_GRACE_PERIOD_S,
        self.EXEC_SHUTDOWN_DEADLINE_S))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1)
  def test_sending_multiple_shutdown_signals(self):
    """Test that multiple IMPALA_SHUTDOWN_SIGNAL signals are all handeled without
    crashing the process."""
    impalad = psutil.Process(self.cluster.impalads[0].get_pid())
    NUM_SIGNALS_TO_SEND = 10
    LOG.info(
      "Sending {0} IMPALA_SHUTDOWN_SIGNAL(SIGRTMIN = {1}) signals to impalad PID = {2}",
      NUM_SIGNALS_TO_SEND, self.IMPALA_SHUTDOWN_SIGNAL, impalad.pid)
    for i in range(NUM_SIGNALS_TO_SEND):
      impalad.send_signal(self.IMPALA_SHUTDOWN_SIGNAL)
    # Give shutdown thread some time to wake up and handle all the signals to avoid
    # flakiness.
    sleep(5)
    # Make sure all signals were received and the process is still up.
    self.assert_impalad_log_contains('INFO', "Shutdown signal received.",
                                     NUM_SIGNALS_TO_SEND)
    assert impalad.is_running(), "Impalad process should still be running."

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--shutdown_grace_period_s={grace_period} \
          --hostname={hostname}".format(grace_period=IDLE_SHUTDOWN_GRACE_PERIOD_S,
            hostname=socket.gethostname()), cluster_size=1)
  def test_graceful_shutdown_script(self):
    impalad = psutil.Process(self.cluster.impalads[0].get_pid())
    script = os.path.join(os.environ['IMPALA_HOME'], 'bin',
                          'graceful_shutdown_backends.sh')
    start_time = time.time()
    check_call([script, str(self.IDLE_SHUTDOWN_GRACE_PERIOD_S)])
    LOG.info("Waiting for impalad to exit {0}".format(start_time))
    impalad.wait()
    shutdown_duration = time.time() - start_time
    assert shutdown_duration <= self.IDLE_SHUTDOWN_GRACE_PERIOD_S + 10
