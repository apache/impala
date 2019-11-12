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

import json
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
                          admission_control_slots=0):
    """Adds an executor group to the cluster. 'min_size' specifies the minimum size for
    the new group to be considered healthy. 'num_executors' specifies the number of
    executors to start and defaults to 'min_size' but can be different from 'min_size' to
    start an unhealthy group. 'admission_control_slots' can be used to override the
    default (num cores). If 'name_suffix' is empty, no executor group is specified for
    the new backends and they will end up in the default group."""
    self.num_groups += 1
    if num_executors == 0:
      num_executors = min_size
    self.num_executors += num_executors
    name = self._group_name(name_suffix)
    LOG.info("Adding %s executors to group %s with minimum size %s" %
             (num_executors, name, min_size))
    cluster_args = ["--impalad_args=-admission_control_slots=%s" %
                    admission_control_slots]
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

  def _wait_for_num_executor_groups(self, num_exec_grps, only_healthy=False):
    """Waits for the number of executor groups to reach 'num_exec_grps'. If 'only_healthy'
    is True, only the healthy executor groups are accounted for, otherwise all groups
    with at least one executor are accounted for."""
    if only_healthy:
      return self.coordinator.service.wait_for_metric_value(
        "cluster-membership.executor-groups.total-healthy", num_exec_grps, timeout=30)
    else:
      return self.coordinator.service.wait_for_metric_value(
        "cluster-membership.executor-groups.total", num_exec_grps, timeout=30)

  def _get_num_executor_groups(self, only_healthy=False):
    """Returns the number of executor groups with at least one executor. If
    'only_healthy' is True, only the number of healthy executor groups is returned."""
    if only_healthy:
      return self.coordinator.service.get_metric_value(
        "cluster-membership.executor-groups.total-healthy")
    else:
      return self.coordinator.service.get_metric_value(
        "cluster-membership.executor-groups.total")

  def _get_num_queries_executing_for_exec_group(self, group_name_suffix):
    """Returns the number of queries running on the executor group 'group_name_suffix'.
    None is returned if the group has no executors or does not exist."""
    METRIC_PREFIX = "admission-controller.executor-group.num-queries-executing.{0}"
    return self.coordinator.service.get_metric_value(
      METRIC_PREFIX.format(self._group_name(group_name_suffix)))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-queue_wait_timeout_ms=2000")
  def test_no_group_timeout(self):
    """Tests that a query submitted to a coordinator with no executor group times out."""
    result = self.execute_query_expect_failure(self.client, "select sleep(2)")
    assert "Admission for query exceeded timeout" in str(result)
    assert self._get_num_executor_groups(only_healthy=True) == 0

  @pytest.mark.execute_serially
  def test_single_group(self):
    """Tests that we can start a single executor group and run a simple query."""
    QUERY = "select count(*) from functional.alltypestiny"
    self._add_executor_group("group1", 2)
    self.execute_query_expect_success(self.client, QUERY)
    assert self._get_num_executor_groups(only_healthy=True) == 1

  @pytest.mark.execute_serially
  def test_executor_group_starts_while_qeueud(self):
    """Tests that a query can stay in the queue of an empty cluster until an executor
    group comes online."""
    QUERY = "select count(*) from functional.alltypestiny"
    client = self.client
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "Waiting for executors to start" in profile
    assert self._get_num_executor_groups(only_healthy=True) == 0
    self._add_executor_group("group1", 2)
    client.wait_for_finished_timeout(handle, 20)
    assert self._get_num_executor_groups(only_healthy=True) == 1

  @pytest.mark.execute_serially
  def test_executor_group_health(self):
    """Tests that an unhealthy executor group will not run queries."""
    QUERY = "select count(*) from functional.alltypestiny"
    # Start cluster and group
    self._add_executor_group("group1", 2)
    self._wait_for_num_executor_groups(1, only_healthy=True)
    client = self.client
    # Run query to validate
    self.execute_query_expect_success(client, QUERY)
    # Kill an executor
    executor = self.cluster.impalads[1]
    executor.kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2,
                                                   timeout=20)
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Run query and observe timeout
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "Waiting for executors to start" in profile, profile
    # Restart executor
    executor.start()
    # Query should now finish
    client.wait_for_finished_timeout(handle, 20)
    # Run query and observe success
    self.execute_query_expect_success(client, QUERY)
    self._wait_for_num_executor_groups(1, only_healthy=True)

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
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Restore executor group health
    executor.start()
    # Query should now finish
    client.wait_for_finished_timeout(q2, 20)
    assert self._get_num_executor_groups(only_healthy=True) == 1

  @pytest.mark.execute_serially
  def test_admission_slots(self):
    """Tests that the admission_control_slots flag works as expected to
    specify the number of admission slots on the executors."""
    self._add_executor_group("group1", 2, admission_control_slots=1)
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    client = self.client
    q1 = client.execute_async(QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(QUERY)
    profile = client.get_runtime_profile(q2)
    assert ("Initial admission queue reason: Not enough admission control slots "
            "available on host" in profile)
    client.cancel(q1)
    client.cancel(q2)

    # Test that a query that would occupy too many slots gets rejected
    result = self.execute_query_expect_failure(self.client,
        "select min(ss_list_price) from tpcds_parquet.store_sales", {'mt_dop': 64})
    assert "number of admission control slots needed" in str(result)
    assert "is greater than total slots available" in str(result)


  @pytest.mark.execute_serially
  def test_multiple_executor_groups(self):
    """Tests that two queries can run on two separate executor groups simultaneously."""
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    self._add_executor_group("group1", 2, admission_control_slots=1)
    self._add_executor_group("group2", 2, admission_control_slots=1)
    self._wait_for_num_executor_groups(2, only_healthy=True)
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
  @CustomClusterTestSuite.with_args(impalad_args="-admission_control_slots=1")
  def test_coordinator_concurrency(self):
    """Tests that the command line flag to limit the coordinator concurrency works as
    expected."""
    QUERY = "select sleep(1000)"
    # Add group with more slots than coordinator
    self._add_executor_group("group2", 2, admission_control_slots=3)
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
  def test_executor_concurrency(self):
    """Tests that the command line flag to limit query concurrency on executors works as
    expected."""
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    self._add_executor_group("group1", 2, admission_control_slots=3)

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

      # Sample the number of admitted queries on each backend for while.
      # Note that the total number of queries in the cluster can higher
      # than 3 because resources may be released on some backends, allowing
      # a new query to fit (see IMPALA-9073).
      NUM_SAMPLES = 30
      executor_slots_in_use = []
      for _ in xrange(NUM_SAMPLES):
        backends_json = json.loads(
            self.impalad_test_service.read_debug_webpage('backends?json'))
        for backend in backends_json['backends']:
          if backend['is_executor']:
            executor_slots_in_use.append(backend['admission_slots_in_use'])
        sleep(1)

      # Must reach 3 but not exceed it
      assert max(executor_slots_in_use) == 3, \
          "Unexpected number of slots in use: %s" % executor_slots_in_use

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
    assert self._get_num_executor_groups() == 1
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Run query and observe that it gets queued
    client = self.client
    handle = client.execute_async(QUERY)
    profile = client.get_runtime_profile(handle)
    assert "Initial admission queue reason: Waiting for executors to start" in profile
    initial_state = client.get_state(handle)
    # Start another executor and observe that the query stays queued
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 3)
    assert self._get_num_executor_groups(only_healthy=True) == 0
    profile = client.get_runtime_profile(handle)
    assert client.get_state(handle) == initial_state
    # Start the remaining executor and observe that the query finishes
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 4)
    assert self._get_num_executor_groups(only_healthy=True) == 1
    client.wait_for_finished_timeout(handle, 20)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-queue_wait_timeout_ms=2000")
  def test_empty_default_group(self):
    """Tests that an empty default group is correctly marked as non-healthy and excluded
    from scheduling."""
    # Start default executor group
    self._add_executor_group("", min_size=2, num_executors=2,
                             admission_control_slots=3)
    # Run query to make sure things work
    QUERY = "select count(*) from functional.alltypestiny"
    self.execute_query_expect_success(self.client, QUERY)
    assert self._get_num_executor_groups(only_healthy=True) == 1
    # Kill executors to make group empty
    impalads = self.cluster.impalads
    impalads[1].kill()
    impalads[2].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1)
    # Run query to make sure it times out
    result = self.execute_query_expect_failure(self.client, QUERY)
    expected_error = "Query aborted:Admission for query exceeded timeout 2000ms in " \
                     "pool default-pool. Queued reason: Waiting for executors to " \
                     "start. Only DDL queries can currently run."
    assert expected_error in str(result)
    assert self._get_num_executor_groups(only_healthy=True) == 0

  @pytest.mark.execute_serially
  def test_executor_group_num_queries_executing_metric(self):
    """Tests the functionality of the metric keeping track of the query load of executor
    groups."""
    # Query that runs on every executor
    QUERY = "select * from functional_parquet.alltypestiny \
             where month < 3 and id + random() < sleep(500);"
    group_names = ["group1", "group2"]
    self._add_executor_group(group_names[0], min_size=1, num_executors=1,
                             admission_control_slots=1)
    # Create an exec group of min size 2 to exercise the case where a group becomes
    # unhealthy.
    self._add_executor_group(group_names[1], min_size=2, num_executors=2,
                             admission_control_slots=1)
    self._wait_for_num_executor_groups(2, only_healthy=True)
    # Verify metrics for both groups appear.
    assert all(
      self._get_num_queries_executing_for_exec_group(name) == 0 for name in group_names)

    # First query will run on the first group. Verify the metric updates accordingly.
    client = self.client
    q1 = client.execute_async(QUERY)
    client.wait_for_admission_control(q1)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) == 1
    assert self._get_num_queries_executing_for_exec_group(group_names[1]) == 0

    # Similarly verify the metric updates accordingly when a query runs on the next group.
    q2 = client.execute_async(QUERY)
    client.wait_for_admission_control(q2)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) == 1
    assert self._get_num_queries_executing_for_exec_group(group_names[1]) == 1

    # Close both queries and verify metrics are updated accordingly.
    client.close_query(q1)
    client.close_query(q2)
    assert all(
      self._get_num_queries_executing_for_exec_group(name) == 0 for name in group_names)

    # Kill an executor from group2 to make that group unhealthy, then verify that the
    # metric is still there.
    self.cluster.impalads[-1].kill()
    self._wait_for_num_executor_groups(1, only_healthy=True)
    assert self._get_num_executor_groups() == 2
    assert all(
      self._get_num_queries_executing_for_exec_group(name) == 0 for name in group_names)

    # Kill the last executor from group2 so that it is removed from the exec group list,
    # then verify that the metric disappears.
    self.cluster.impalads[-2].kill()
    self._wait_for_num_executor_groups(1)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) == 0
    assert self._get_num_queries_executing_for_exec_group(group_names[1]) is None

    # Now make sure the metric accounts for already running queries that linger around
    # from when the group was healthy.
    # Block the query cancellation thread to allow the query to linger between exec group
    # going down and coming back up.
    client.set_configuration({"debug_action": "QUERY_CANCELLATION_THREAD:SLEEP@10000"})
    q3 = client.execute_async(QUERY)
    client.wait_for_admission_control(q3)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) == 1
    impalad_grp1 = self.cluster.impalads[-3]
    impalad_grp1.kill()
    self._wait_for_num_executor_groups(0)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) is None
    impalad_grp1.start()
    self._wait_for_num_executor_groups(1, only_healthy=True)
    assert self._get_num_queries_executing_for_exec_group(group_names[0]) == 1, \
      "The lingering query should be accounted for when the group comes back up."
    client.cancel(q3)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.executor-group.num-queries-executing.{0}".format(
        self._group_name(group_names[0])), 0, timeout=30)

  @pytest.mark.execute_serially
  def test_join_strategy_single_executor(self):
    """Tests that the planner picks the correct join strategy based on the current cluster
    size. This test uses an executor group with a minimum size of 1."""
    TABLE = "functional.alltypes"
    QUERY = "explain select * from %s a inner join %s b on a.id = b.id" % (TABLE, TABLE)

    # Predicates to assert that a certain join type was picked.
    def assert_broadcast_join():
      ret = self.execute_query_expect_success(self.client, QUERY)
      assert ":EXCHANGE [BROADCAST]" in str(ret)

    def assert_hash_join():
      ret = self.execute_query_expect_success(self.client, QUERY)
      assert ":EXCHANGE [HASH(b.id)]" in str(ret)

    # Without any executors we default to a hash join.
    assert_hash_join()

    # Add a healthy executor group of size 1 and observe that we switch to broadcast join.
    self._add_executor_group("group1", min_size=1, num_executors=1)
    assert_broadcast_join()

    # Add another executor to the same group and observe that with two executors we pick a
    # partitioned hash join.
    self._add_executor_group("group1", min_size=1, num_executors=1)
    assert_hash_join()

    # Kill an executor. The group remains healthy but its size decreases and we revert
    # back to a broadcast join.
    self.cluster.impalads[-1].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2,
                                                   timeout=20)
    assert_broadcast_join()

    # Kill a second executor. The group becomes unhealthy but we cache its last healthy
    # size and will continue to pick a broadcast join.
    self.cluster.impalads[-2].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1,
                                                   timeout=20)
    assert_broadcast_join()

  @pytest.mark.execute_serially
  def test_join_strategy_multiple_executors(self):
    """Tests that the planner picks the correct join strategy based on the current cluster
    size. This test uses an executor group which requires multiple executors to be
    healthy."""
    TABLE = "functional.alltypes"
    QUERY = "explain select * from %s a inner join %s b on a.id = b.id" % (TABLE, TABLE)

    # Predicate to assert that the planner decided on a hash join.
    def assert_hash_join():
      ret = self.execute_query_expect_success(self.client, QUERY)
      assert ":EXCHANGE [HASH(b.id)]" in str(ret)

    # Without any executors we default to a hash join.
    assert_hash_join()

    # Adding an unhealthy group will not affect the planner's decision.
    self._add_executor_group("group1", min_size=2, num_executors=1)
    assert_hash_join()

    # Adding a second executor makes the group healthy (note that the resulting join
    # strategy is the same though).
    self._add_executor_group("group1", min_size=2, num_executors=1)
    assert_hash_join()

    # Kill an executor. The unhealthy group does not affect the planner's decision, even
    # though only one executor is now online.
    self.cluster.impalads[-1].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2,
                                                   timeout=20)
    assert_hash_join()
