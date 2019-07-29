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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.concurrent_workload import ConcurrentWorkload

import logging
import pytest
from time import sleep

LOG = logging.getLogger("test_auto_scaling")


class TestExecutorGroups(CustomClusterTestSuite):
  """This class contains tests that exercise the logic related to scaling clusters up and
  down by adding and removing groups of executors. All tests start with a base cluster
  containing a dedicated coordinator, catalog, and statestore. Tests will then start
  executor groups and run queries to validate the behavior."""

  def setup_method(self, method):
    # Always start the base cluster with the coordinator in its own executor group.
    existing_args = method.func_dict.get("impalad_args", "")
    method.func_dict["impalad_args"] = "%s -executor_groups=coordinator" % existing_args
    method.func_dict["cluster_size"] = 1
    method.func_dict["num_exclusive_coordinators"] = 1
    self.num_groups = 1
    self.num_executors = 1
    super(TestExecutorGroups, self).setup_method(method)
    self.coordinator = self.cluster.impalads[0]

  def _group_name(self, name):
    # By convention, group names must start with their associated resource pool name
    # followed by a "-". Tests in this class all use the default resource pool.
    return "default-pool-%s" % name

  def _add_executor_group(self, name_suffix, min_size, num_executors=0,
                          max_concurrent_queries=0):
    """Adds an executor group to the cluster. 'min_size' specifies the minimum size for
    the new group to be considered healthy. 'num_executors' specifies the number of
    executors to start and defaults to 'min_size' but can be different from 'min_size' to
    start an unhealthy group. 'max_concurrent_queries' can be used to override the default
    (num cores). If 'name_suffix' is empty, no executor group is specified for the new
    backends and they will end up in the default group."""
    self.num_groups += 1
    if num_executors == 0:
      num_executors = min_size
    self.num_executors += num_executors
    name = self._group_name(name_suffix)
    LOG.info("Adding %s executors to group %s with minimum size %s" %
             (num_executors, name, min_size))
    cluster_args = ["--impalad_args=-max_concurrent_queries=%s" % max_concurrent_queries]
    if len(name_suffix) > 0:
      cluster_args.append("--impalad_args=-executor_groups=%s:%s" % (name, min_size))
    self._start_impala_cluster(options=cluster_args,
                               cluster_size=num_executors,
                               num_coordinators=0,
                               add_executors=True,
                               expected_num_executors=self.num_executors)

  def _get_total_admitted_queries(self):
    """Returns the total number of queries that have been admitted to the default resource
    pool."""
    return self.impalad_test_service.get_total_admitted_queries("default-pool")

  def _get_num_running_queries(self):
    """Returns the number of queries that are currently running in the default resource
    pool."""
    return self.impalad_test_service.get_num_running_queries("default-pool")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-queue_wait_timeout_ms=2000")
  def test_no_group_timeout(self):
    """Tests that a query submitted to a coordinator with no executor group times out."""
    result = self.execute_query_expect_failure(self.client, "select sleep(2)")
    assert "Admission for query exceeded timeout" in str(result)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0

  @pytest.mark.execute_serially
  def test_single_group(self):
    """Tests that we can start a single executor group and run a simple query."""
    QUERY = "select count(*) from functional.alltypestiny"
    self._add_executor_group("group1", 2)
    self.execute_query_expect_success(self.client, QUERY)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 1

  @pytest.mark.execute_serially
  def test_executor_group_starts_while_qeueud(self):
    """Tests that a query can stay in the queue of an empty cluster until an executor
    group comes online."""
    QUERY = "select count(*) from functional.alltypestiny"
    client = self.client
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "No healthy executor groups found for pool" in profile
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
    self._add_executor_group("group1", 2)
    client.wait_for_finished_timeout(handle, 20)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 1

  @pytest.mark.execute_serially
  def test_executor_group_health(self):
    """Tests that an unhealthy executor group will not run queries."""
    QUERY = "select count(*) from functional.alltypestiny"
    # Start cluster and group
    self._add_executor_group("group1", 2)
    self.coordinator.service.wait_for_metric_value(
      "cluster-membership.executor-groups.total-healthy", 1)
    client = self.client
    # Run query to validate
    self.execute_query_expect_success(client, QUERY)
    # Kill an executor
    executor = self.cluster.impalads[1]
    executor.kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2,
                                                   timeout=20)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
    # Run query and observe timeout
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "No healthy executor groups found for pool" in profile, profile
    # Restart executor
    executor.start()
    # Query should now finish
    client.wait_for_finished_timeout(handle, 20)
    # Run query and observe success
    self.execute_query_expect_success(client, QUERY)
    assert self.coordinator.service.wait_for_metric_value(
      "cluster-membership.executor-groups.total-healthy", 1)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-default_pool_max_requests=1")
  def test_executor_group_shutdown(self):
    """Tests that an executor group can shutdown and a query in the queue can still run
    successfully when the group gets restored."""
    self._add_executor_group("group1", 2)
    client = self.client
    q1 = client.execute_async("select sleep(5000)")
    q2 = client.execute_async("select sleep(3)")
    # Verify that q2 is queued up behind q1
    profile = client.get_runtime_profile(q2)
    assert "Initial admission queue reason: number of running queries" in profile, profile
    # Kill an executor
    executor = self.cluster.impalads[1]
    executor.kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2)
    # Wait for q1 to finish (sleep runs on the coordinator)
    client.wait_for_finished_timeout(q1, 20)
    # Check that q2 still hasn't run
    profile = client.get_runtime_profile(q2)
    assert "Admission result: Queued" in profile, profile
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
    # Restore executor group health
    executor.start()
    # Query should now finish
    client.wait_for_finished_timeout(q2, 20)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 1

  @pytest.mark.execute_serially
  def test_max_concurrent_queries(self):
    """Tests that the max_concurrent_queries flag works as expected."""
    self._add_executor_group("group1", 2, max_concurrent_queries=1)
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    client = self.client
    q1 = client.execute_async(QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(QUERY)
    profile = client.get_runtime_profile(q2)
    assert "Initial admission queue reason: No query slot available on host" in profile
    client.cancel(q1)
    client.cancel(q2)

  @pytest.mark.execute_serially
  def test_multiple_executor_groups(self):
    """Tests that two queries can run on two separate executor groups simultaneously."""
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    self._add_executor_group("group1", 2, max_concurrent_queries=1)
    self._add_executor_group("group2", 2, max_concurrent_queries=1)
    self.coordinator.service.wait_for_metric_value(
      "cluster-membership.executor-groups.total-healthy", 2)
    client = self.client
    q1 = client.execute_async(QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(QUERY)
    client.wait_for_admission_control(q2)
    profiles = [client.get_runtime_profile(q) for q in (q1, q2)]
    assert not any("Initial admission queue reason" in p for p in profiles), profiles
    client.cancel(q1)
    client.cancel(q2)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-max_concurrent_queries=1")
  def test_coordinator_concurrency(self):
    """Tests that the command line flag to limit the coordinator concurrency works as
    expected."""
    QUERY = "select sleep(1000)"
    # Add group with more slots than coordinator
    self._add_executor_group("group2", 2, max_concurrent_queries=3)
    # Try to run two queries and observe that one gets queued
    client = self.client
    q1 = client.execute_async(QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(QUERY)
    profile = client.get_runtime_profile(q2)
    assert "Initial admission queue reason" in profile
    client.cancel(q1)
    client.cancel(q2)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-max_concurrent_queries=16")
  def test_executor_concurrency(self):
    """Tests that the command line flag to limit query concurrency on executors works as
    expected."""
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    self._add_executor_group("group1", 2, max_concurrent_queries=3)

    workload = None
    try:
      workload = ConcurrentWorkload(QUERY, num_streams=5)
      LOG.info("Starting workload")
      workload.start()

      RAMP_UP_TIMEOUT_S = 60
      # Wait until we admitted at least 10 queries
      assert any(self._get_total_admitted_queries() >= 10 or sleep(1)
                  for _ in range(RAMP_UP_TIMEOUT_S)), \
          "Did not admit enough queries within %s s" % RAMP_UP_TIMEOUT_S

      # Sample the number of running queries for while
      NUM_RUNNING_SAMPLES = 30
      num_running = []
      for _ in xrange(NUM_RUNNING_SAMPLES):
        num_running.append(self._get_num_running_queries())
        sleep(1)

      # Must reach 3 but not exceed it
      assert max(num_running) == 3, \
          "Unexpected number of running queries: %s" % num_running

    finally:
      LOG.info("Stopping workload")
      if workload:
        workload.stop()

  @pytest.mark.execute_serially
  def test_sequential_startup_wait(self):
    """Tests that starting an executor group sequentially works as expected, i.e. queries
    don't fail and no queries are admitted until the group is in a healthy state."""
    QUERY = "select sleep(4)"
    # Start first executor
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total") == 1
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
    # Run query and observe that it gets queued
    client = self.client
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "Initial admission queue reason: No healthy executor groups found for pool" \
        in profile
    initial_state = client.get_state(handle)
    # Start another executor and observe that the query stays queued
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 3)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
    profile = client.get_runtime_profile(handle)
    assert client.get_state(handle) == initial_state
    # Start the remaining executor and observe that the query finishes
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 4)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 1
    client.wait_for_finished_timeout(handle, 20)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-queue_wait_timeout_ms=2000")
  def test_empty_default_group(self):
    """Tests that an empty default group is correctly marked as non-healthy and excluded
    from scheduling."""
    # Start default executor group
    self._add_executor_group("", min_size=2, num_executors=2,
                             max_concurrent_queries=3)
    # Run query to make sure things work
    QUERY = "select count(*) from functional.alltypestiny"
    self.execute_query_expect_success(self.client, QUERY)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 1
    # Kill executors to make group empty
    impalads = self.cluster.impalads
    impalads[1].kill()
    impalads[2].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1)
    # Run query to make sure it times out
    result = self.execute_query_expect_failure(self.client, QUERY)
    expected_error = "Query aborted:Admission for query exceeded timeout 2000ms in " \
                     "pool default-pool. Queued reason: No healthy executor groups " \
                     "found for pool default-pool."
    assert expected_error in str(result)
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.executor-groups.total-healthy") == 0
