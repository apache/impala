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
# Test Catalog behavior when HMS is not present

import os
import pytest
from subprocess import check_call
import time

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import (
    CustomClusterTestSuite,
    NUM_SUBSCRIBERS)
from tests.util.filesystem_utils import IS_ISILON, IS_LOCAL

class TestCatalogHMSFailures(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    super(TestCatalogHMSFailures, cls).setup_class()

  @classmethod
  def run_hive_server(cls):
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/run-hive-server.sh')
    run_cmd = [script]
    if IS_LOCAL or IS_ISILON:
      run_cmd.append('-only_metastore')
    check_call(run_cmd, close_fds=True)

  @classmethod
  def cleanup_process(cls, proc):
    try:
      proc.kill()
    except:
      pass
    try:
      proc.wait()
    except:
      pass

  @classmethod
  def teardown_class(cls):
    # Make sure the metastore is running even if the test aborts somewhere unexpected
    # before restarting the metastore itself.
    cls.run_hive_server()
    super(TestCatalogHMSFailures, cls).teardown_class()

  @classmethod
  def reload_metadata(cls, client):
    client.execute('invalidate metadata')
    client.execute('show databases')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args='--initial_hms_cnxn_timeout_s=120')
  def test_kill_hms_after_catalog_init(self, vector):
    """IMPALA-4278: If HMS dies after catalogd initialization, SQL statements that force
    metadata load should fail quickly. After HMS restart, metadata load should work
    again"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill Hive
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd], close_fds=True)

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

    # Start Hive
    self.run_hive_server()

    # Metadata load should work now
    self.reload_metadata(client)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args='--initial_hms_cnxn_timeout_s=120')
  def test_start_catalog_before_hms(self, vector):
    """IMPALA-4278: If catalogd is started with initial_hms_cnxn_timeout_s set to a value
    greater than HMS startup time, it will manage to establish connection to HMS even if
    HMS is started a little later"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill Hive
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd], close_fds=True)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)

    try:
      # Start the catalog service asynchronously.
      catalogd.start()
      # Wait 10s to be sure that the catalogd is in the 'trying to connect' phase of its
      # startup.
      time.sleep(10)

      # Start Hive and wait for catalogd to come up
      self.run_hive_server()
      statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)

      # Metadata load should work now
      self.reload_metadata(client)
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args='--initial_hms_cnxn_timeout_s=30')
  def test_catalogd_fails_if_hms_started_late(self, vector):
    """IMPALA-4278: If the HMS is not started within initial_hms_cnxn_timeout_s, then the
    catalogd fails"""
    # Make sure that catalogd is connected to HMS
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.reload_metadata(client)

    # Kill Hive
    kill_cmd = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/kill-hive-server.sh')
    check_call([kill_cmd], close_fds=True)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)

    try:
      # Start the catalog service asynchronously.
      catalogd.start()
      # Wait 40s to be sure that the catalogd has been trying to connect to HMS longer
      # than initial_hms_cnxn_timeout_s.
      time.sleep(40)

      # Start Hive
      self.run_hive_server()

      # catalogd has terminated by now
      assert catalogd.get_pid() == None, "catalogd should have terminated"
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)

    try:
      # Start the catalog service again and wait for it to come up.
      catalogd.start()
      statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)

      # Metadata load should work now
      self.reload_metadata(client)
    finally:
      # Make sure to clean up the catalogd process that we started
      self.cleanup_process(catalogd)
