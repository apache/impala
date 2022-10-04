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
import os
import pytest
from time import sleep

LOG = logging.getLogger("test_auto_scaling")

# Non-trivial query that gets scheduled on all executors within a group.
TEST_QUERY = "select count(*) from functional.alltypes where month + random() < 3"

DEFAULT_RESOURCE_POOL = "default-pool"


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
    self.num_impalads = 1
    super(TestExecutorGroups, self).setup_method(method)
    self.coordinator = self.cluster.impalads[0]

  def _group_name(self, resource_pool, name_suffix):
    # By convention, group names must start with their associated resource pool name
    # followed by a "-". Tests in this class mostly use the default resource pool.
    return "%s-%s" % (resource_pool, name_suffix)

  def _add_executor_group(self, name_suffix, min_size, num_executors=0,
                          admission_control_slots=0, extra_args=None,
                          resource_pool=DEFAULT_RESOURCE_POOL):
    """Adds an executor group to the cluster. 'min_size' specifies the minimum size for
    the new group to be considered healthy. 'num_executors' specifies the number of
    executors to start and defaults to 'min_size' but can be different from 'min_size' to
    start an unhealthy group. 'admission_control_slots' can be used to override the
    default (num cores). If 'name_suffix' is empty, no executor group is specified for
    the new backends and they will end up in the default group."""
    self.num_groups += 1
    if num_executors == 0:
      num_executors = min_size
    self.num_impalads += num_executors
    name = self._group_name(resource_pool, name_suffix)
    LOG.info("Adding %s executors to group %s with minimum size %s" %
             (num_executors, name, min_size))
    cluster_args = ["--impalad_args=-admission_control_slots=%s" %
                    admission_control_slots]
    if len(name_suffix) > 0:
      cluster_args.append("--impalad_args=-executor_groups=%s:%s" % (name, min_size))
    if extra_args:
      cluster_args.append("--impalad_args=%s" % extra_args)
    self._start_impala_cluster(options=cluster_args,
                               cluster_size=num_executors,
                               num_coordinators=0,
                               add_executors=True,
                               expected_num_impalads=self.num_impalads)

  def _restart_coordinators(self, num_coordinators, extra_args=None):
    """Restarts the coordinator spawned in setup_method and enables the caller to start
    more than one coordinator by specifying 'num_coordinators'"""
    LOG.info("Adding a coordinator")
    cluster_args = ["--impalad_args=-executor_groups=coordinator"]
    if extra_args:
      cluster_args.append("--impalad_args=%s" % extra_args)
    self._start_impala_cluster(options=cluster_args,
                               cluster_size=num_coordinators,
                               num_coordinators=num_coordinators,
                               add_executors=False,
                               expected_num_impalads=num_coordinators,
                               use_exclusive_coordinators=True)
    self.coordinator = self.cluster.impalads[0]
    self.num_impalads = num_coordinators

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

  def _get_num_executor_groups(self, only_healthy=False, exec_group_set_prefix=None):
    """Returns the number of executor groups with at least one executor. If
    'only_healthy' is True, only the number of healthy executor groups is returned.
    If exec_group_set_prefix is used, it returns the metric corresponding to that
    executor group set."""
    metric_name = ""
    if exec_group_set_prefix is None:
      if only_healthy:
        metric_name = "cluster-membership.executor-groups.total-healthy"
      else:
        metric_name = "cluster-membership.executor-groups.total"
    else:
      if only_healthy:
        metric_name = "cluster-membership.group-set.executor-groups.total-healthy.{0}"\
          .format(exec_group_set_prefix)
      else:
        metric_name = "cluster-membership.group-set.executor-groups.total.{0}"\
          .format(exec_group_set_prefix)
    return self.coordinator.service.get_metric_value(metric_name)

  def _get_num_queries_executing_for_exec_group(self, group_name_prefix):
    """Returns the number of queries running on the executor group 'group_name_prefix'.
    None is returned if the group has no executors or does not exist."""
    METRIC_PREFIX = "admission-controller.executor-group.num-queries-executing.{0}"
    return self.coordinator.service.get_metric_value(
      METRIC_PREFIX.format(self._group_name(DEFAULT_RESOURCE_POOL, group_name_prefix)))

  def _assert_eventually_in_profile(self, query_handle, expected_str):
    """Assert with a timeout of 60 sec and a polling interval of 1 sec that the
    expected_str exists in the query profile."""
    self.assert_eventually(
      60, 1, lambda: expected_str in self.client.get_runtime_profile(query_handle))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="-queue_wait_timeout_ms=1000")
  def test_no_group(self):
    """Tests that a regular query submitted to a coordinator with no executor group
    times out but coordinator only queries can still run."""
    result = self.execute_query_expect_failure(self.client, TEST_QUERY)
    assert "Admission for query exceeded timeout" in str(result)
    assert self._get_num_executor_groups(only_healthy=True) == 0
    expected_group = "Executor Group: empty group (using coordinator only)"
    # Force the query to run on coordinator only.
    result = self.execute_query_expect_success(self.client, TEST_QUERY,
                                               query_options={'NUM_NODES': '1'})
    assert expected_group in str(result.runtime_profile)
    # Small query runs on coordinator only.
    result = self.execute_query_expect_success(self.client, "select 1")
    assert expected_group in str(result.runtime_profile)

  @pytest.mark.execute_serially
  def test_single_group(self):
    """Tests that we can start a single executor group and run a simple query."""
    self._add_executor_group("group1", 2)
    self.execute_query_expect_success(self.client, TEST_QUERY)
    assert self._get_num_executor_groups(only_healthy=True) == 1

  @pytest.mark.execute_serially
  def test_executor_group_starts_while_qeueud(self):
    """Tests that a query can stay in the queue of an empty cluster until an executor
    group comes online."""
    client = self.client
    handle = client.execute_async(TEST_QUERY)
    self._assert_eventually_in_profile(handle, "Waiting for executors to start")
    assert self._get_num_executor_groups(only_healthy=True) == 0
    self._add_executor_group("group1", 2)
    client.wait_for_finished_timeout(handle, 20)
    assert self._get_num_executor_groups(only_healthy=True) == 1

  @pytest.mark.execute_serially
  def test_executor_group_health(self):
    """Tests that an unhealthy executor group will not run queries."""
    # Start cluster and group
    self._add_executor_group("group1", 2)
    self._wait_for_num_executor_groups(1, only_healthy=True)
    client = self.client
    # Run query to validate
    self.execute_query_expect_success(client, TEST_QUERY)
    # Kill an executor
    executor = self.cluster.impalads[1]
    executor.kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2,
                                                   timeout=20)
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Run query and observe timeout
    handle = client.execute_async(TEST_QUERY)
    self._assert_eventually_in_profile(handle, "Waiting for executors to start")
    # Restart executor
    executor.start()
    # Query should now finish
    client.wait_for_finished_timeout(handle, 20)
    # Run query and observe success
    self.execute_query_expect_success(client, TEST_QUERY)
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
    self._assert_eventually_in_profile(
      q2, "Initial admission queue reason: number of running queries")
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
    self._assert_eventually_in_profile(q2, "Initial admission queue reason: Not enough "
                                           "admission control slots available on host")
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
    self._assert_eventually_in_profile(q2, "Initial admission queue reason")
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
    # Start first executor
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 2)
    assert self._get_num_executor_groups() == 1
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Run query and observe that it gets queued
    client = self.client
    handle = client.execute_async(TEST_QUERY)
    self._assert_eventually_in_profile(handle, "Initial admission queue reason:"
                                               " Waiting for executors to start")
    initial_state = client.get_state(handle)
    # Start another executor and observe that the query stays queued
    self._add_executor_group("group1", 3, num_executors=1)
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 3)
    assert self._get_num_executor_groups(only_healthy=True) == 0
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
    self.execute_query_expect_success(self.client, TEST_QUERY)
    assert self._get_num_executor_groups(only_healthy=True) == 1
    # Kill executors to make group empty
    impalads = self.cluster.impalads
    impalads[1].kill()
    impalads[2].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1)
    # Run query to make sure it times out
    result = self.execute_query_expect_failure(self.client, TEST_QUERY)
    expected_error = "Query aborted:Admission for query exceeded timeout 2000ms in " \
                     "pool default-pool. Queued reason: Waiting for executors to " \
                     "start. Only DDL queries and queries scheduled only on the " \
                     "coordinator (either NUM_NODES set to 1 or when small query " \
                     "optimization is triggered) can currently run."
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
        self._group_name(DEFAULT_RESOURCE_POOL, group_names[0])), 0, timeout=30)

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

    # Kill a second executor. The group becomes unhealthy and we go back to using the
    # expected size to plan which would result in a hash join
    self.cluster.impalads[-2].kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1,
                                                   timeout=20)
    assert_hash_join()

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

  @pytest.mark.execute_serially
  def test_admission_control_with_multiple_coords(self):
    """This test verifies that host level metrics like the num of admission slots used
    and memory admitted is disseminated correctly across the cluster and accounted for
    while making admission decisions. We run a query that takes up all of a particular
    resource (slots or memory) and check if attempting to run a query on the other
    coordinator results in queuing."""
    # A long running query that runs on every executor
    QUERY = "select * from functional_parquet.alltypes \
                 where month < 3 and id + random() < sleep(100);"
    # default_pool_mem_limit is set to enable mem based admission.
    self._restart_coordinators(num_coordinators=2,
                               extra_args="-default_pool_mem_limit=100g")
    # Create fresh clients
    second_coord_client = self.create_client_for_nth_impalad(1)
    self.create_impala_clients()
    # Add an exec group with a 4.001gb mem_limit, adding spare room for cancellation
    self._add_executor_group("group1", 2, admission_control_slots=2,
                             extra_args="-mem_limit=4.001g")
    assert self._get_num_executor_groups(only_healthy=True) == 1
    second_coord_client.set_configuration({'mt_dop': '2'})
    handle_for_second = second_coord_client.execute_async(QUERY)
    # Verify that the first coordinator knows about the query running on the second
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.agg-num-running.default-pool", 1, timeout=30)
    handle_for_first = self.execute_query_async(TEST_QUERY)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.local-num-queued.default-pool", 1, timeout=30)
    profile = self.client.get_runtime_profile(handle_for_first)
    assert "queue reason: Not enough admission control slots available on host" in \
           profile, profile
    self.close_query(handle_for_first)
    second_coord_client.close_query(handle_for_second)
    # Wait for first coordinator to get the admission update.
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.agg-num-running.default-pool", 0, timeout=30)
    # Now verify that mem based admission also works as intended. A max of mem_reserved
    # and mem_admitted is used for this. Since mem_limit is being used here, both will be
    # identical but this will at least test that code path as a sanity check.
    second_coord_client.clear_configuration()
    second_coord_client.set_configuration({'mem_limit': '4g'})
    handle_for_second = second_coord_client.execute_async(QUERY)
    # Verify that the first coordinator knows about the query running on the second
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.agg-num-running.default-pool", 1, timeout=30)
    handle_for_first = self.execute_query_async(TEST_QUERY)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.local-num-queued.default-pool", 1, timeout=30)
    profile = self.client.get_runtime_profile(handle_for_first)
    assert "queue reason: Not enough memory available on host" in profile, profile
    self.close_query(handle_for_first)
    second_coord_client.close_query(handle_for_second)

  @pytest.mark.execute_serially
  def test_admission_control_with_multiple_coords_and_exec_groups(self):
    """This test verifies that admission control accounting works when using multiple
    coordinators and multiple executor groups mapped to different resource pools and
    having different sizes."""
    # A long running query that runs on every executor
    LONG_QUERY = "select * from functional_parquet.alltypes \
                 where month < 3 and id + random() < sleep(100);"
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-allocation.xml")
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-empty.xml")
    # Start with a regular admission config with multiple pools and no resource limits.
    self._restart_coordinators(num_coordinators=2,
         extra_args="-vmodule admission-controller=3 "
                    "-expected_executor_group_sets=root.queue1:2,root.queue2:1 "
                    "-fair_scheduler_allocation_path %s "
                    "-llama_site_path %s" % (
                      fs_allocation_path, llama_site_path))

    # Create fresh clients
    second_coord_client = self.create_client_for_nth_impalad(1)
    self.create_impala_clients()
    # Add an exec group with a single admission slot and 2 executors.
    self._add_executor_group("group", 2, admission_control_slots=1,
                             resource_pool="root.queue1", extra_args="-mem_limit=2g")
    # Add an exec group with a single admission slot and only 1 executor.
    self._add_executor_group("group", 1, admission_control_slots=1,
                             resource_pool="root.queue2", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True) == 2
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.queue1") == 1
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.queue2") == 1

    # Execute a long running query on group 'queue1'
    self.client.set_configuration({'request_pool': 'queue1'})
    handle_long_running_queue1 = self.execute_query_async(LONG_QUERY)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.executor-group.num-queries-executing.root.queue1-group",
      1, timeout=30)
    profile = self.client.get_runtime_profile(handle_long_running_queue1)
    "Executor Group: root.queue1-group" in profile

    # Try to execute another query on group 'queue1'. This one should queue.
    handle_queued_query_queue1 = self.execute_query_async(TEST_QUERY)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.local-num-queued.root.queue1", 1, timeout=30)
    profile = self.client.get_runtime_profile(handle_queued_query_queue1)
    assert "queue reason: Not enough admission control slots available on host" in \
           profile, profile

    # Execute a query on group 'queue2'. This one will run as its running in another pool.
    result = self.execute_query_expect_success(self.client, TEST_QUERY,
                                               query_options={'request_pool': 'queue2'})
    assert "Executor Group: root.queue2-group" in str(result.runtime_profile)

    # Verify that multiple coordinators' accounting still works correctly in case of
    # multiple executor groups.

    # Run a query in group 'queue2' on the second coordinator
    second_coord_client.set_configuration({'request_pool': 'queue2'})
    second_coord_client.execute_async(LONG_QUERY)
    # Verify that the first coordinator knows about the query running on the second
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.agg-num-running.root.queue2", 1, timeout=30)

    # Check that attempting to run another query in 'queue2' will queue the query.
    self.client.set_configuration({'request_pool': 'queue2'})
    handle_queued_query_queue2 = self.execute_query_async(TEST_QUERY)
    self.coordinator.service.wait_for_metric_value(
      "admission-controller.local-num-queued.root.queue2", 1, timeout=30)
    profile = self.client.get_runtime_profile(handle_queued_query_queue2)
    assert "queue reason: Not enough admission control slots available on host" in \
           profile, profile

    self.client.close()
    second_coord_client.close()

  @pytest.mark.execute_serially
  def test_query_assignment_with_two_exec_groups(self):
    """This test verifies that query assignment works with two executor groups with
    different number of executors and memory limit in each."""
    # A small query with estimated memory per host of 10MB that can run on the small
    # executor group
    SMALL_QUERY = "select count(*) from tpcds_parquet.date_dim where d_year=2022;"
    # A large query with estimated memory per host of 132MB that can only run on
    # the large executor group.
    LARGE_QUERY = "select * from tpcds_parquet.store_sales where ss_item_sk = 1 limit 50;"
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")
    # Define two group sets: small and large
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-2-groups.xml")
    # Define the min-query-mem-limit and max-query-mem-limit properties of the two sets:
    # small: [0, 64MB]
    # large: [64MB+1Byte, 8PB]
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-2-groups.xml")
    # Start with a regular admission config with multiple pools and no resource limits.
    self._restart_coordinators(num_coordinators=1,
         extra_args="-vmodule admission-controller=3 "
                    "-expected_executor_group_sets=root.small:2,root.large:3 "
                    "-fair_scheduler_allocation_path %s "
                    "-llama_site_path %s" % (
                      fs_allocation_path, llama_site_path))

    # Create fresh client
    self.create_impala_clients()
    # Add an exec group with a single admission slot and 2 executors.
    self._add_executor_group("group", 2, admission_control_slots=1,
                             resource_pool="root.small", extra_args="-mem_limit=2g")
    # Add another exec group with a single admission slot and 3 executors.
    self._add_executor_group("group", 3, admission_control_slots=1,
                             resource_pool="root.large", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True) == 2
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.small") == 1
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.large") == 1

    # Expect to run the small query on the small group
    result = self.execute_query_expect_success(self.client, SMALL_QUERY)
    assert "Executor Group: root.small-group" in str(result.runtime_profile)

    # Expect to run the large query on the large group
    result = self.execute_query_expect_success(self.client, LARGE_QUERY)
    assert "Executor Group: root.large-group" in str(result.runtime_profile)

    # Force to run the large query on the small group should fail
    self.client.set_configuration({'request_pool': 'small'})
    result = self.execute_query_expect_failure(self.client, LARGE_QUERY)
    assert "The query does not fit any executor group set" in str(result)

    self.client.close()

  @pytest.mark.execute_serially
  def test_per_exec_group_set_metrics(self):
    """This test verifies that the metrics for each exec group set are updated
    appropriately."""
    self._restart_coordinators(num_coordinators=1,
         extra_args="-expected_executor_group_sets=root.queue1:2,root.queue2:1")

    # Add an unhealthy exec group in queue1 group set
    self._add_executor_group("group", 2, num_executors=1,
                             resource_pool="root.queue1", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.queue1") == 0
    assert self._get_num_executor_groups(exec_group_set_prefix="root.queue1") == 1
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.group-set.backends.total.root.queue1") == 1

    # Add another executor to the previous group to make healthy again
    self._add_executor_group("group", 2, num_executors=1,
                             resource_pool="root.queue1", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.queue1") == 1
    assert self._get_num_executor_groups(exec_group_set_prefix="root.queue1") == 1
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.group-set.backends.total.root.queue1") == 2

    # Add a healthy exec group in queue2 group set
    self._add_executor_group("group", 1,
                             resource_pool="root.queue2", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.queue2") == 1
    assert self._get_num_executor_groups(exec_group_set_prefix="root.queue2") == 1
    assert self.coordinator.service.get_metric_value(
      "cluster-membership.group-set.backends.total.root.queue2") == 1
