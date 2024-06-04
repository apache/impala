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
import os
import time
import threading

from subprocess import check_call
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import (
    CustomClusterTestSuite,
    DEFAULT_CLUSTER_SIZE)
from tests.common.skip import SkipIf
from tests.util.event_processor_utils import EventProcessorUtils
from tests.util.filesystem_utils import IS_ISILON, IS_LOCAL


NUM_SUBSCRIBERS = DEFAULT_CLUSTER_SIZE + 1


@SkipIf.is_test_jdk
class TestHiveMetaStoreFailure(CustomClusterTestSuite):
  """Tests to validate the Catalog Service continues to function even if the HMS
  fails."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestHiveMetaStoreFailure, cls).setup_class()

  @classmethod
  def run_hive_metastore(cls, if_not_running=False):
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/run-hive-server.sh')
    run_cmd = [script, '-only_metastore']
    if if_not_running:
      run_cmd.append('-if_not_running')
    check_call(run_cmd, close_fds=True)

  @classmethod
  def teardown_class(cls):
    # Make sure the metastore is running even if the test aborts somewhere unexpected
    # before restarting the metastore itself.
    cls.run_hive_metastore(if_not_running=True)
    super(TestHiveMetaStoreFailure, cls).teardown_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog --catalog_topic_mode=minimal',
    catalogd_args='--catalog_topic_mode=minimal')
  def test_hms_service_dies(self):
    """Regression test for IMPALA-823 to verify the catalog service works properly when
    HMS connections fail"""
    # Force the tables to be uncached and then kill the hive metastore.
    tbl_name = "functional.alltypes"
    self.client.execute("invalidate metadata %s" % tbl_name)
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)

    try:
      self.client.execute("describe %s" % tbl_name)
    except ImpalaBeeswaxException as e:
      print(str(e))
      assert "Failed to load metadata for table: %s. Running 'invalidate metadata %s' "\
          "may resolve this problem." % (tbl_name, tbl_name) in str(e)
    self.run_hive_metastore()

    self.client.execute("invalidate metadata %s" % tbl_name)
    self.client.execute("describe %s" % tbl_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog',
    catalogd_args='--catalog_topic_mode=minimal')
  def test_local_catalog_load_with_hms_state_change(self, unique_database):
    self.run_test_load_with_hms_down_and_up(unique_database,
                                            "local_catalog_load_with_hms_state_change")

  @pytest.mark.execute_serially
  def test_load_with_hms_state_change(self, unique_database):
    self.run_test_load_with_hms_down_and_up(unique_database,
                                            "load_with_hms_state_change")

  def run_test_load_with_hms_down_and_up(self, unique_database, table_name):
    table = unique_database + "." + table_name
    self.client.execute("create table {0} (i int)".format(table))
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)
    for _ in range(2):
      try:
        self.client.execute("describe {0}".format(table))
      except ImpalaBeeswaxException as e:
        assert "Failed to load metadata for table: %s. "\
               "Running 'invalidate metadata %s' may resolve this problem." \
               % (table, table) in str(e)
    self.run_hive_metastore()
    res = self.client.execute("describe {0}".format(table))
    assert res.data == ["i\tint\t"]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog --catalog_topic_mode=minimal',
    catalogd_args='--catalog_topic_mode=minimal')
  def test_hms_client_retries(self):
    """Test that a running query will trigger the retry logic in
    RetryingMetaStoreClient."""
    # Force the tables to be uncached and then kill the hive metastore.
    tbl_name = "functional.alltypes"
    self.client.execute("invalidate metadata %s" % tbl_name)
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)

    # Run a query asynchronously.
    query = "select * from {0} limit 1".format(tbl_name)
    thread = threading.Thread(target=lambda:
          self.execute_query_expect_success(self.client, query))
    thread.start()

    # Wait 1 second for the catalogd to start contacting HMS, then start HMS.
    time.sleep(1)
    self.run_hive_metastore()

    # Wait for the query to complete, assert that the HMS client retried the connection.
    thread.join()
    self.assert_catalogd_log_contains("INFO",
        "MetaStoreClient lost connection. Attempting to reconnect", expected_count=-1)

  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog',
    catalogd_args='--catalog_topic_mode=minimal')
  def test_event_processor_tolerate_hms_restart(self):
    """IMPALA-12561: Test that event-processor won't go into ERROR state when there are
    connection issues with HMS (mocked by a restart on HMS)"""
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    self.run_hive_metastore()
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

@SkipIf.is_test_jdk
class TestCatalogHMSFailures(CustomClusterTestSuite):
  """Test Catalog behavior when HMS is not present."""

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    super(TestCatalogHMSFailures, cls).setup_class()

  @classmethod
  def run_hive_metastore(cls, if_not_running=False):
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/run-hive-server.sh')
    run_cmd = [script, '-only_metastore']
    if if_not_running:
      run_cmd.append('-if_not_running')
    check_call(run_cmd, close_fds=True)

  @classmethod
  def cleanup_process(cls, proc):
    try:
      proc.kill()
    except Exception:
      pass
    try:
      proc.wait()
    except Exception:
      pass

  @classmethod
  def teardown_class(cls):
    # Make sure the metastore is running even if the test aborts somewhere unexpected
    # before restarting the metastore itself.
    cls.run_hive_metastore(if_not_running=True)
    super(TestCatalogHMSFailures, cls).teardown_class()

  @classmethod
  def reload_metadata(cls, client):
    client.execute('invalidate metadata')
    client.execute('show databases')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog --catalog_topic_mode=minimal',
    catalogd_args='--initial_hms_cnxn_timeout_s=120 --catalog_topic_mode=minimal')
  def test_kill_hms_after_catalog_init(self):
    """IMPALA-4278: If HMS dies after catalogd initialization, SQL statements that force
    metadata load should fail quickly. After HMS restart, metadata load should work
    again"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill HMS
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)

    # Metadata load should fail quickly
    start = time.time()
    try:
      self.reload_metadata(client)
    except ImpalaBeeswaxException as e:
      assert "Connection refused" in str(e)
    else:
      assert False, "Metadata load should have failed"
    end = time.time()
    assert end - start < 30, "Metadata load hasn't failed quickly enough"

    # Start HMS
    self.run_hive_metastore()

    # Metadata load should work now
    self.reload_metadata(client)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog --catalog_topic_mode=minimal',
    catalogd_args='--initial_hms_cnxn_timeout_s=120 --catalog_topic_mode=minimal')
  def test_start_catalog_before_hms(self):
    """IMPALA-4278: If catalogd is started with initial_hms_cnxn_timeout_s set to a value
    greater than HMS startup time, it will manage to establish connection to HMS even if
    HMS is started a little later"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill HMS
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)

    try:
      # Start the catalog service asynchronously.
      catalogd.start(wait_until_ready=False)
      # Wait 10s to be sure that the catalogd is in the 'trying to connect' phase of its
      # startup.
      time.sleep(10)

      # Start HMS and wait for catalogd to come up
      self.run_hive_metastore()
      statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', True, timeout=60)

      # Metadata load should work now
      self.reload_metadata(client)
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog --catalog_topic_mode=minimal',
    catalogd_args='--initial_hms_cnxn_timeout_s=30 --catalog_topic_mode=minimal')
  def test_catalogd_fails_if_hms_started_late(self):
    """IMPALA-4278: If the HMS is not started within initial_hms_cnxn_timeout_s, then the
    catalogd fails"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill HMS
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd, '-only_metastore'], close_fds=True)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)

    try:
      # Start the catalog service asynchronously.
      catalogd.start(wait_until_ready=False)
      # Wait 40s to be sure that the catalogd has been trying to connect to HMS longer
      # than initial_hms_cnxn_timeout_s.
      time.sleep(40)

      # Start HMS
      self.run_hive_metastore()

      # catalogd has terminated by now
      assert not catalogd.get_pid(), "catalogd should have terminated"
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)

    try:
      # Start the catalog service again and wait for it to come up.
      catalogd.start()
      statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', True, timeout=60)

      # Metadata load should work now
      self.reload_metadata(client)
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)
