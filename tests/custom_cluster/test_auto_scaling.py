#!/usr/bin/env impala-python
#
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

import logging
from time import sleep, time

from tests.util.auto_scaler import AutoScaler
from tests.util.concurrent_workload import ConcurrentWorkload
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOG = logging.getLogger("test_auto_scaling")


class TestAutoScaling(CustomClusterTestSuite):
  """This class contains tests that exercise the logic related to scaling clusters up and
  down by adding and removing groups of executors."""
  INITIAL_STARTUP_TIME_S = 10
  STATE_CHANGE_TIMEOUT_S = 45
  # This query will scan two partitions (month = 1, 2) and thus will have 1 fragment
  # instance per executor on groups of size 2. Each partition has 2 rows, so it performs
  # two comparisons and should take around 1 second to complete.
  QUERY = """select * from functional_parquet.alltypestiny where month < 3
             and id + random() < sleep(500)"""

  def _get_total_admitted_queries(self):
    return self.impalad_test_service.get_total_admitted_queries("default-pool")

  def _get_num_executors(self):
    return self.impalad_test_service.get_num_known_live_backends(only_executors=True)

  def _get_num_running_queries(self):
    return self.impalad_test_service.get_num_running_queries("default-pool")

  def test_single_workload(self):
    """This test exercises the auto-scaling logic in the admission controller. It spins up
    a base cluster (coordinator, catalog, statestore), runs some queries to observe that
    new executors are started, then stops the workload and observes that the cluster gets
    shutdown."""
    GROUP_SIZE = 2
    EXECUTOR_SLOTS = 3
    auto_scaler = AutoScaler(executor_slots=EXECUTOR_SLOTS, group_size=GROUP_SIZE)
    workload = None
    try:
      auto_scaler.start()
      sleep(self.INITIAL_STARTUP_TIME_S)

      workload = ConcurrentWorkload(self.QUERY, num_streams=5)
      LOG.info("Starting workload")
      workload.start()

      # Wait for workers to spin up
      assert any(self._get_num_executors() >= GROUP_SIZE or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Number of backends did not increase within %s s" % self.STATE_CHANGE_TIMEOUT_S

      # Wait until we admitted at least 10 queries
      assert any(self._get_total_admitted_queries() >= 10 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Did not admit enough queries within %s s" % self.STATE_CHANGE_TIMEOUT_S

      # Wait for second executor group to start
      num_expected = 2 * GROUP_SIZE
      assert any(self._get_num_executors() == num_expected or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
                     "Number of backends did not reach %s within %s s" % (
                     num_expected, self.STATE_CHANGE_TIMEOUT_S)

      # Wait for query rate to surpass the maximum for a single executor group plus 20%
      min_query_rate = 1.2 * EXECUTOR_SLOTS
      assert any(workload.get_query_rate() > min_query_rate or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
                     "Query rate did not surpass %s within %s s" % (
                     num_expected, self.STATE_CHANGE_TIMEOUT_S)

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      assert any(self._get_num_executors() == 0 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Backends did not shut down within %s s" % self.STATE_CHANGE_TIMEOUT_S

    finally:
      if workload:
        workload.stop()
      LOG.info("Stopping auto scaler")
      auto_scaler.stop()

  def test_single_group_maxed_out(self):
    """This test starts an auto scaler and limits it to a single executor group. It then
    makes sure that the query throughput does not exceed the expected limit."""
    GROUP_SIZE = 2
    EXECUTOR_SLOTS = 3
    auto_scaler = AutoScaler(executor_slots=EXECUTOR_SLOTS, group_size=GROUP_SIZE,
                             max_groups=1)
    workload = None
    try:
      auto_scaler.start()
      sleep(self.INITIAL_STARTUP_TIME_S)

      workload = ConcurrentWorkload(self.QUERY, num_streams=5)
      LOG.info("Starting workload")
      workload.start()

      # Wait for workers to spin up
      assert any(self._get_num_executors() >= GROUP_SIZE or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Number of backends did not increase within %s s" % self.STATE_CHANGE_TIMEOUT_S

      # Wait until we admitted at least 10 queries
      assert any(self._get_total_admitted_queries() >= 10 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Did not admit enough queries within %s s" % self.STATE_CHANGE_TIMEOUT_S

      # Sample the number of running queries for while
      SAMPLE_NUM_RUNNING_S = 30
      end_time = time() + SAMPLE_NUM_RUNNING_S
      num_running = []
      while time() < end_time:
        num_running.append(self._get_num_running_queries())
        sleep(1)

      # Must reach EXECUTOR_SLOTS but not exceed it
      assert max(num_running) == EXECUTOR_SLOTS, \
          "Unexpected number of running queries: %s" % num_running

      # Check that only a single group started
      assert self._get_num_executors() == GROUP_SIZE

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      assert any(self._get_num_executors() == 0 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Backends did not shut down within %s s" % self.STATE_CHANGE_TIMEOUT_S

    finally:
      if workload:
        workload.stop()
      LOG.info("Stopping auto scaler")
      auto_scaler.stop()

  def test_sequential_startup(self):
    """This test starts an executor group sequentially and observes that no queries are
    admitted until the group has been fully started."""
    # Larger groups size so it takes a while to start up
    GROUP_SIZE = 4
    EXECUTOR_SLOTS = 3
    auto_scaler = AutoScaler(executor_slots=EXECUTOR_SLOTS, group_size=GROUP_SIZE,
                             start_batch_size=1, max_groups=1)
    workload = None
    try:
      auto_scaler.start()
      sleep(self.INITIAL_STARTUP_TIME_S)

      workload = ConcurrentWorkload(self.QUERY, num_streams=5)
      LOG.info("Starting workload")
      workload.start()

      # Wait for first executor to start up
      assert any(self._get_num_executors() >= 1 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Number of backends did not increase within %s s" % self.STATE_CHANGE_TIMEOUT_S

      # Wait for remaining executors to start up and make sure that no queries are
      # admitted during startup
      end_time = time() + self.STATE_CHANGE_TIMEOUT_S
      startup_complete = False
      while time() < end_time:
        num_admitted = self._get_total_admitted_queries()
        num_backends = self._get_num_executors()
        if num_backends < GROUP_SIZE:
          assert num_admitted == 0, "%s/%s backends started but %s queries have " \
              "already been admitted." % (num_backends, GROUP_SIZE, num_admitted)
        if num_admitted > 0:
          assert num_backends == GROUP_SIZE
          startup_complete = True
          break
        sleep(1)

      assert startup_complete, "Did not start up in %s s" % self.STATE_CHANGE_TIMEOUT_S

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      assert any(self._get_num_executors() == 0 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Backends did not shut down within %s s" % self.STATE_CHANGE_TIMEOUT_S

    finally:
      if workload:
        workload.stop()
      LOG.info("Stopping auto scaler")
      auto_scaler.stop()
