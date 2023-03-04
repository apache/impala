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

from __future__ import absolute_import, division, print_function
from builtins import range
import logging
import pytest
from time import sleep, time

from tests.util.auto_scaler import AutoScaler
from tests.util.concurrent_workload import ConcurrentWorkload
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

LOG = logging.getLogger("test_auto_scaling")
TOTAL_BACKENDS_METRIC_NAME = "cluster-membership.backends.total"

class TestAutoScaling(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestAutoScaling, cls).setup_class()

  """This class contains tests that exercise the logic related to scaling clusters up and
  down by adding and removing groups of executors."""
  INITIAL_STARTUP_TIME_S = 10
  STATE_CHANGE_TIMEOUT_S = 60
  # This query will scan two partitions (month = 1, 2) and thus will have 1 fragment
  # instance per executor on groups of size 2. Each partition has 2 rows, so it performs
  # two comparisons and should take around 2 second to complete.
  QUERY = """select * from functional_parquet.alltypestiny where month < 3
             and id + random() < sleep(1000)"""

  def _get_total_admitted_queries(self):
    admitted_queries = self.impalad_test_service.get_total_admitted_queries(
      "default-pool")
    LOG.info("Current total admitted queries: %s", admitted_queries)
    return admitted_queries

  def _get_num_backends(self):
    metric_val = self.impalad_test_service.get_metric_value(TOTAL_BACKENDS_METRIC_NAME)
    LOG.info("Getting metric %s : %s", TOTAL_BACKENDS_METRIC_NAME, metric_val)
    return metric_val

  def _get_num_running_queries(self):
    running_queries = self.impalad_test_service.get_num_running_queries("default-pool")
    LOG.info("Current running queries: %s", running_queries)
    return running_queries

  def test_single_workload(self):
    """This test exercises the auto-scaling logic in the admission controller. It spins up
    a base cluster (coordinator, catalog, statestore), runs a workload to initiate a
    scaling up event as the queries start queuing, then stops the workload and observes
    that the cluster gets shutdown."""
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
      cluster_size = GROUP_SIZE + 1  # +1 to include coordinator.
      assert any(self._get_num_backends() >= cluster_size or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Number of backends did not increase within %s s" % self.STATE_CHANGE_TIMEOUT_S
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total-healthy") >= 1

      # Wait until we admitted at least 10 queries
      assert any(self._get_total_admitted_queries() >= 10 or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
          "Did not admit enough queries within %s s" % self.STATE_CHANGE_TIMEOUT_S
      # Wait for second executor group to start
      cluster_size = (2 * GROUP_SIZE) + 1
      assert any(self._get_num_backends() >= cluster_size or sleep(1)
                 for _ in range(self.STATE_CHANGE_TIMEOUT_S)), \
                     "Number of backends did not reach %s within %s s" % (
                     cluster_size, self.STATE_CHANGE_TIMEOUT_S)
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total-healthy") >= 2

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      self.impalad_test_service.wait_for_metric_value(
        TOTAL_BACKENDS_METRIC_NAME, 1,
        timeout=self.STATE_CHANGE_TIMEOUT_S, interval=1)
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total") == 0

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
                             max_groups=1, coordinator_slots=EXECUTOR_SLOTS)
    workload = None
    try:
      auto_scaler.start()
      sleep(self.INITIAL_STARTUP_TIME_S)

      workload = ConcurrentWorkload(self.QUERY, num_streams=5)
      LOG.info("Starting workload")
      workload.start()

      # Wait for workers to spin up
      cluster_size = GROUP_SIZE + 1  # +1 to include coordinator.
      self.impalad_test_service.wait_for_metric_value(
        TOTAL_BACKENDS_METRIC_NAME, cluster_size,
        timeout=self.STATE_CHANGE_TIMEOUT_S, interval=1)

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
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total-healthy") == 1

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      self.impalad_test_service.wait_for_metric_value(
        TOTAL_BACKENDS_METRIC_NAME, 1,
        timeout=self.STATE_CHANGE_TIMEOUT_S, interval=1)
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total") == 0

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
      self.impalad_test_service.wait_for_metric_value(
        "cluster-membership.executor-groups.total", 1,
        timeout=self.STATE_CHANGE_TIMEOUT_S, interval=1)

      # Wait for remaining executors to start up and make sure that no queries are
      # admitted during startup
      end_time = time() + self.STATE_CHANGE_TIMEOUT_S
      startup_complete = False
      cluster_size = GROUP_SIZE + 1  # +1 to include coordinator.
      while time() < end_time:
        num_admitted = self._get_total_admitted_queries()
        num_backends = self._get_num_backends()
        if num_backends < cluster_size:
          assert num_admitted == 0, "%s/%s backends started but %s queries have " \
              "already been admitted." % (num_backends, cluster_size, num_admitted)
        if num_admitted > 0:
          assert num_backends == cluster_size
          startup_complete = True
          break
        sleep(1)

      assert startup_complete, "Did not start up in %s s" % self.STATE_CHANGE_TIMEOUT_S

      LOG.info("Stopping workload")
      workload.stop()

      # Wait for workers to spin down
      self.impalad_test_service.wait_for_metric_value(
        TOTAL_BACKENDS_METRIC_NAME, 1,
        timeout=self.STATE_CHANGE_TIMEOUT_S, interval=1)
      assert self.impalad_test_service.get_metric_value(
        "cluster-membership.executor-groups.total") == 0

    finally:
      if workload:
        workload.stop()
      LOG.info("Stopping auto scaler")
      auto_scaler.stop()
