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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.util.concurrent_workload import ConcurrentWorkload

import json
import logging
import os
import pytest
import re
from time import sleep

LOG = logging.getLogger("test_auto_scaling")

# Non-trivial query that gets scheduled on all executors within a group.
TEST_QUERY = "select count(*) from functional.alltypes where month + random() < 3"

# A query to test CPU requirement. Estimated memory per host is 37MB.
CPU_TEST_QUERY = "select * from tpcds_parquet.store_sales where ss_item_sk = 1 limit 50"

# A query with full table scan characteristics.
# TODO: craft bigger grouping query that can assign to executor group set larger than
# root.tiny by default.
GROUPING_TEST_QUERY = ("select ss_item_sk from tpcds_parquet.store_sales"
    " group by (ss_item_sk) order by ss_item_sk limit 10")

# A scan query that is recognized as trivial and scheduled to run at coordinator only.
SINGLE_NODE_SCAN_QUERY = "select * from functional.alltypestiny"

# TPC-DS Q1 to test slightly more complex query.
TPCDS_Q1 = """
with customer_total_return as (
  select sr_customer_sk as ctr_customer_sk,
    sr_store_sk as ctr_store_sk,
    sum(SR_RETURN_AMT) as ctr_total_return
  from tpcds_partitioned_parquet_snap.store_returns,
    tpcds_partitioned_parquet_snap.date_dim
  where sr_returned_date_sk = d_date_sk
  and d_year = 2000
  group by sr_customer_sk, sr_store_sk
) select c_customer_id
from customer_total_return ctr1,
  tpcds_partitioned_parquet_snap.store,
  tpcds_partitioned_parquet_snap.customer
where ctr1.ctr_total_return > (
  select avg(ctr_total_return) * 1.2
  from customer_total_return ctr2
  where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'TN'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100
"""

DEFAULT_RESOURCE_POOL = "default-pool"

DEBUG_ACTION_DELAY_SCAN = "HDFS_SCANNER_THREAD_OBTAINED_RANGE:SLEEP@1000"


class TestExecutorGroups(CustomClusterTestSuite):
  """This class contains tests that exercise the logic related to scaling clusters up and
  down by adding and removing groups of executors. All tests start with a base cluster
  containing a dedicated coordinator, catalog, and statestore. Tests will then start
  executor groups and run queries to validate the behavior."""

  def setup_method(self, method):
    # Always start the base cluster with the coordinator in its own executor group.
    existing_args = method.__dict__.get("impalad_args", "")
    method.__dict__["impalad_args"] = "%s -executor_groups=coordinator" % existing_args
    method.__dict__["cluster_size"] = 1
    method.__dict__["num_exclusive_coordinators"] = 1
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

  def _add_executors(self, name_suffix, min_size, num_executors=0,
                    extra_args=None, resource_pool=DEFAULT_RESOURCE_POOL,
                    expected_num_impalads=0):
    """Adds given number of executors to the cluster. 'min_size' specifies the minimum
    size for the group to be considered healthy. 'num_executors' specifies the number of
    executors to start. If 'name_suffix' is empty, no executor group is specified for
    the new backends and they will end up in the default group."""
    if num_executors == 0:
      return
    name = self._group_name(resource_pool, name_suffix)
    LOG.info("Adding %s executors to group %s with minimum size %s" %
             (num_executors, name, min_size))
    cluster_args = []
    if len(name_suffix) > 0:
      cluster_args.append("--impalad_args=-executor_groups=%s:%s" % (name, min_size))
    if extra_args:
      cluster_args.append("--impalad_args=%s" % extra_args)
    self._start_impala_cluster(options=cluster_args,
                               cluster_size=num_executors,
                               num_coordinators=0,
                               add_executors=True,
                               expected_num_impalads=expected_num_impalads)
    self.num_impalads += num_executors

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

  def _verify_total_admitted_queries(self, resource_pool, expected_query_num):
    """Verify the total number of queries that have been admitted to the given resource
    pool on the Web admission site."""
    query_num = self.impalad_test_service.get_total_admitted_queries(resource_pool)
    assert query_num == expected_query_num, \
        "Not matched number of queries admitted to %s pool on the Web admission site." \
        % (resource_pool)

  def _verify_query_num_for_resource_pool(self, resource_pool, expected_query_num):
    """ Verify the number of queries which use the given resource pool on
    the Web queries site."""
    queries_json = self.impalad_test_service.get_queries_json()
    queries = queries_json.get("in_flight_queries") + \
              queries_json.get("completed_queries")
    query_num = 0
    for query in queries:
      if query["resource_pool"] == resource_pool:
        query_num += 1
    assert query_num == expected_query_num, \
        "Not matched number of queries using %s pool on the Web queries site: %s." \
        % (resource_pool, json)

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
  def test_executor_group_min_size_update(self):
    """Tests that we can update an executor group's min size without restarting
    coordinators."""
    # Start cluster and group
    self._add_executor_group("group1", min_size=1, num_executors=1)
    self._wait_for_num_executor_groups(1, only_healthy=True)
    client = self.client
    # Kill the executor
    executor = self.cluster.impalads[1]
    executor.kill()
    self.coordinator.service.wait_for_metric_value("cluster-membership.backends.total", 1,
                                                   timeout=20)
    assert self._get_num_executor_groups(only_healthy=True) == 0
    # Add a new executor to group1 with group min size 2
    self._add_executors("group1", min_size=2, num_executors=2, expected_num_impalads=3)
    assert self._get_num_executor_groups(only_healthy=True) == 1
    # Run query and observe success
    self.execute_query_expect_success(client, TEST_QUERY)

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
  def test_coordinator_concurrency_default(self):
    """Tests that concurrent coordinator-only queries ignore slot based admission
    in non multiple executor group set setup."""
    # Add group with more slots than coordinator
    self._add_executor_group("group2", 2, admission_control_slots=3)
    # Try to run two queries and observe that the second one is admitted immediately.
    client = self.client
    client.set_configuration_option('debug_action', DEBUG_ACTION_DELAY_SCAN)
    q1 = client.execute_async(SINGLE_NODE_SCAN_QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(SINGLE_NODE_SCAN_QUERY)
    self._assert_eventually_in_profile(q2, "Admitted immediately")
    # Assert other info strings in query profiles.
    self._assert_eventually_in_profile(q1, "Admitted immediately")
    self._assert_eventually_in_profile(q1, "empty group (using coordinator only)")
    client.cancel(q1)
    self._assert_eventually_in_profile(q2, "empty group (using coordinator only)")
    client.cancel(q2)

  @pytest.mark.execute_serially
  def test_coordinator_concurrency_two_exec_group_cluster(self):
    """Tests that concurrent coordinator-only queries respect slot based admission
    in multiple executor group set setup."""
    coordinator_test_args = "-admission_control_slots=1"
    self._setup_two_coordinator_two_exec_group_cluster(coordinator_test_args)
    # Create fresh clients
    self.create_impala_clients()
    # Try to run two queries and observe that the second one gets queued.
    client = self.client
    client.set_configuration_option('debug_action', DEBUG_ACTION_DELAY_SCAN)
    q1 = client.execute_async(SINGLE_NODE_SCAN_QUERY)
    client.wait_for_admission_control(q1)
    q2 = client.execute_async(SINGLE_NODE_SCAN_QUERY)
    self._assert_eventually_in_profile(q2,
        "Not enough admission control slots available")
    # Assert other info strings in query profiles.
    self._assert_eventually_in_profile(q1, "Admitted immediately")
    self._assert_eventually_in_profile(q1, "empty group (using coordinator only)")
    # Cancel q1 so that q2 is then admitted and has 'Executor Group' info string.
    client.cancel(q1)
    self._assert_eventually_in_profile(q2, "Admitted (queued)")
    self._assert_eventually_in_profile(q2, "empty group (using coordinator only)")
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
      for _ in range(NUM_SAMPLES):
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
    # The maximum memory can be used for query needs to subtract the codegen cache
    # capacity, which is 4GB - 10% * 4GB = 3.6GB.
    query_mem_limit = 4 * (1 - 0.1)
    second_coord_client.set_configuration({'mem_limit': str(query_mem_limit) + 'g'})
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
    # A small query with estimated memory per host of 16MB that can run on the small
    # executor group
    SMALL_QUERY = "select count(*) from tpcds_parquet.date_dim where d_year=2022;"
    # A large query with estimated memory per host of 132MB that can only run on
    # the large executor group.
    LARGE_QUERY = "select * from tpcds_parquet.store_sales where ss_item_sk = 1 limit 50;"
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")
    # Define three group sets: tiny, small and large
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-3-groups.xml")
    # Define the min-query-mem-limit, max-query-mem-limit,
    # max-query-cpu-core-per-node-limit and max-query-cpu-core-coordinator-limit
    # properties of the three sets:
    # tiny: [0, 64MB, 4, 4]
    # small: [0, 70MB, 8, 8]
    # large: [64MB+1Byte, 8PB, 64, 64]
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-3-groups.xml")
    # Start with a regular admission config with multiple pools and no resource limits.
    # Only populate executor froup sets small and large.
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

    # Force to run the large query on the small group.
    # Query should run successfully since exec group memory limit is ignored.
    self.client.set_configuration({'request_pool': 'small'})
    result = self.execute_query_expect_success(self.client, LARGE_QUERY)
    assert ("Verdict: query option REQUEST_POOL=small is set. "
        "Memory and cpu limit checking is skipped.") in str(result.runtime_profile)

    self.client.close()

  def _setup_three_exec_group_cluster(self, coordinator_test_args):
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")
    # Define three group sets: tiny, small and large
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-3-groups.xml")
    # Define the min-query-mem-limit, max-query-mem-limit,
    # max-query-cpu-core-per-node-limit and max-query-cpu-core-coordinator-limit
    # properties of the three sets:
    # tiny: [0, 64MB, 4, 4]
    # small: [0, 90MB, 8, 8]
    # large: [64MB+1Byte, 8PB, 64, 64]
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-3-groups.xml")

    # extra args template to start coordinator
    extra_args_template = ("-vmodule admission-controller=3 "
        "-admission_control_slots=8 "
        "-expected_executor_group_sets=root.tiny:1,root.small:2,root.large:3 "
        "-fair_scheduler_allocation_path %s "
        "-llama_site_path %s "
        "%s ")

    # Start with a regular admission config, multiple pools, no resource limits.
    self._restart_coordinators(num_coordinators=1,
        extra_args=extra_args_template % (fs_allocation_path, llama_site_path,
          coordinator_test_args))

    # Create fresh client
    self.create_impala_clients()
    # Add an exec group with 4 admission slots and 1 executors.
    self._add_executor_group("group", 1, admission_control_slots=4,
                             resource_pool="root.tiny", extra_args="-mem_limit=2g")
    # Add an exec group with 8 admission slots and 2 executors.
    self._add_executor_group("group", 2, admission_control_slots=8,
                             resource_pool="root.small", extra_args="-mem_limit=2g")
    # Add another exec group with 64 admission slots and 3 executors.
    self._add_executor_group("group", 3, admission_control_slots=64,
                             resource_pool="root.large", extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=True) == 3
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.tiny") == 1
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.small") == 1
    assert self._get_num_executor_groups(only_healthy=True,
                                         exec_group_set_prefix="root.large") == 1

  def _set_query_options(self, query_options):
    """Set query options by running it as an SQL statement.
    To mimic impala-shell behavior, use self.client.set_configuration() instead.
    """
    for k, v in query_options.items():
      self.execute_query_expect_success(self.client, "SET {}='{}'".format(k, v))

  def _run_query_and_verify_profile(self, query,
      expected_strings_in_profile, not_expected_in_profile=[]):
    """Run 'query' and assert existence of 'expected_strings_in_profile' and
    nonexistence of 'not_expected_in_profile' in query profile.
    Caller is reponsible to close self.client at the end of test."""
    result = self.execute_query_expect_success(self.client, query)
    for expected_profile in expected_strings_in_profile:
      assert expected_profile in str(result.runtime_profile)
    for not_expected in not_expected_in_profile:
      assert not_expected not in str(result.runtime_profile)
    return result

  def __verify_fs_writers(self, result, expected_num_writers,
                          expected_instances_per_host):
    assert 'HDFS WRITER' in result.exec_summary[0]['operator'], result.runtime_profile
    num_writers = int(result.exec_summary[0]['num_instances'])
    assert (num_writers == expected_num_writers), result.runtime_profile
    num_hosts = len(expected_instances_per_host)
    regex = (r'Per Host Number of Fragment Instances:'
             + (num_hosts * r'.*?\((.*?)\)') + r'.*?\n')
    matches = re.findall(regex, result.runtime_profile)
    assert len(matches) == 1 and len(matches[0]) == num_hosts, result.runtime_profile
    num_instances_per_host = [int(i) for i in matches[0]]
    num_instances_per_host.sort()
    expected_instances_per_host.sort()
    assert num_instances_per_host == expected_instances_per_host, result.runtime_profile

  @UniqueDatabase.parametrize(sync_ddl=True)
  @pytest.mark.execute_serially
  def test_query_cpu_count_divisor_default(self, unique_database):
    coordinator_test_args = ""
    self._setup_three_exec_group_cluster(coordinator_test_args)
    self.client.clear_configuration()

    # The default query options for this test.
    # Some test case will change these options along the test, but should eventually
    # restored to this default values.
    self._set_query_options({
      'COMPUTE_PROCESSING_COST': 'true',
      'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK'})

    # Expect to run the query on the tiny group by default.
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # Test disabling COMPUTE_PROCESING_COST. This will produce non-MT plan.
    self._set_query_options({'COMPUTE_PROCESSING_COST': 'false'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.large-group", "ExecutorGroupsConsidered: 3",
         "Verdict: Match"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # Test COMPUTE_PROCESING_COST=false and MT_DOP=2.
    self._set_query_options({'MT_DOP': '2'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
         "Verdict: Match"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # Test COMPUTE_PROCESING_COST=true and MT_DOP=2.
    # COMPUTE_PROCESING_COST should override MT_DOP.
    self._set_query_options({'COMPUTE_PROCESSING_COST': 'true'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # Unset MT_DOP
    self._set_query_options({'MT_DOP': '0'})

    # Create small table based on tpcds_parquet.store_sales that will be used later
    # for COMPUTE STATS test.
    self._run_query_and_verify_profile(
      ("create table {0}.{1} partitioned by (ss_sold_date_sk) as "
       "select ss_sold_time_sk, ss_net_paid_inc_tax, ss_net_profit, ss_sold_date_sk "
       "from tpcds_parquet.store_sales where ss_sold_date_sk < 2452184"
       ).format(unique_database, "store_sales_subset"),
      ["Executor Group: root.tiny", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    compute_stats_query = ("compute stats {0}.{1}").format(
        unique_database, "store_sales_subset")

    # Test that child queries unset REQUEST_POOL that was set by Frontend planner for
    # parent query. Currently both child query assign to root.tiny.
    # TODO: Revise the CTAS query above such that the two child queries assigned to
    # distinct executor group set.
    self._verify_total_admitted_queries("root.tiny", 4)
    self._verify_total_admitted_queries("root.small", 0)
    self._verify_total_admitted_queries("root.large", 1)
    self._run_query_and_verify_profile(compute_stats_query,
        ["ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query is not auto-scalable"],
        ["Query Options (set by configuration): REQUEST_POOL=",
         "EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:",
         "Executor Group:"])
    self._verify_total_admitted_queries("root.tiny", 6)
    self._verify_total_admitted_queries("root.small", 0)
    self._verify_total_admitted_queries("root.large", 1)

    # Test that child queries follow REQUEST_POOL that is set through client
    # configuration. Two child queries should all run in root.small.
    self.client.set_configuration({'REQUEST_POOL': 'root.small'})
    self._run_query_and_verify_profile(compute_stats_query,
        ["Query Options (set by configuration): REQUEST_POOL=root.small",
         "ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query is not auto-scalable"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])
    self._verify_total_admitted_queries("root.small", 2)
    self.client.clear_configuration()

    # Test that child queries follow REQUEST_POOL that is set through SQL statement.
    # Two child queries should all run in root.large.
    self._set_query_options({'REQUEST_POOL': 'root.large'})
    self._run_query_and_verify_profile(compute_stats_query,
        ["Query Options (set by configuration): REQUEST_POOL=root.large",
         "ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query is not auto-scalable"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])
    self._verify_total_admitted_queries("root.large", 3)

    # Test that REQUEST_POOL will override executor group selection
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Query Options (set by configuration): REQUEST_POOL=root.large",
         "Executor Group: root.large-group",
         ("Verdict: query option REQUEST_POOL=root.large is set. "
          "Memory and cpu limit checking is skipped."),
         "EffectiveParallelism: 3", "ExecutorGroupsConsidered: 1",
         "AvgAdmissionSlotsPerExecutor: 1"])

    # Test setting REQUEST_POOL=root.large and disabling COMPUTE_PROCESSING_COST
    self._set_query_options({'COMPUTE_PROCESSING_COST': 'false'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Query Options (set by configuration): REQUEST_POOL=root.large",
         "Executor Group: root.large-group",
         ("Verdict: query option REQUEST_POOL=root.large is set. "
          "Memory and cpu limit checking is skipped."),
         "ExecutorGroupsConsidered: 1"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # Unset REQUEST_POOL and restore COMPUTE_PROCESSING_COST.
    self._set_query_options({
      'REQUEST_POOL': '',
      'COMPUTE_PROCESSING_COST': 'true'})

    # Test that empty REQUEST_POOL should have no impact.
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1",
         "Verdict: Match"],
        ["Query Options (set by configuration): REQUEST_POOL="])

    # Test that GROUPING_TEST_QUERY will get assigned to the tiny group.
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
          "Verdict: Match", "CpuAsk: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # ENABLE_REPLAN=false should force query to run in first group (tiny).
    self._set_query_options({'ENABLE_REPLAN': 'false'})
    self._run_query_and_verify_profile(TEST_QUERY,
        ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query option ENABLE_REPLAN=false"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])
    # Unset ENABLE_REPLAN.
    self._set_query_options({'ENABLE_REPLAN': ''})

    # Trivial query should be assigned to tiny group by Frontend.
    # Backend may decide to run it in coordinator only.
    self._run_query_and_verify_profile("SELECT 1",
        ["Executor Group: empty group (using coordinator only)",
         "ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because the number of nodes is 1"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # CREATE/DROP database should work and assigned to tiny group.
    self._run_query_and_verify_profile(
        "CREATE DATABASE test_non_scalable_query;",
        ["ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query is not auto-scalable"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])
    self._run_query_and_verify_profile(
        "DROP DATABASE test_non_scalable_query;",
        ["ExecutorGroupsConsidered: 1",
         "Verdict: Assign to first group because query is not auto-scalable"],
        ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # Test combination of PROCESSING_COST_MIN_THREADS and MAX_FRAGMENT_INSTANCES_PER_NODE.
    # Show how CpuAsk and CpuAskBounded react to it, among other things.
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '3'})
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1",
         "CpuAsk: 1", "CpuAskBounded: 1"])
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '4'})
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1",
         "CpuAsk: 1", "CpuAskBounded: 1"])
    # Raising PROCESSING_COST_MIN_THREADS > 1 can push CpuAskBounded > CpuAsk.
    self._set_query_options({'PROCESSING_COST_MIN_THREADS': '2'})
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.small-group", "EffectiveParallelism: 4",
         "ExecutorGroupsConsidered: 2", "AvgAdmissionSlotsPerExecutor: 2",
         # at root.tiny
         "Verdict: Match", "CpuAsk: 2", "CpuAskBounded: 2",
         # at root.small
         "Verdict: Match", "CpuAsk: 4", "CpuAskBounded: 4"])
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '2'})
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 2",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 2",
         "CpuAsk: 2", "CpuAskBounded: 2"])
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '1'})
    result = self.execute_query_expect_failure(self.client, CPU_TEST_QUERY)
    status = (r"PROCESSING_COST_MIN_THREADS \(2\) can not be larger than "
              r"MAX_FRAGMENT_INSTANCES_PER_NODE \(1\).")
    assert re.search(status, str(result))
    # Unset PROCESSING_COST_MIN_THREADS and MAX_FRAGMENT_INSTANCES_PER_NODE.
    self._set_query_options({
      'MAX_FRAGMENT_INSTANCES_PER_NODE': '',
      'PROCESSING_COST_MIN_THREADS': ''})

    # BEGIN testing count queries
    # Test optimized count star query with 1824 scan ranges assign to tiny group.
    self._run_query_and_verify_profile(
        "SELECT count(*) FROM tpcds_parquet.store_sales",
        ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
         "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # Test optimized count star query with 383 scan ranges assign to tiny group.
    self._run_query_and_verify_profile(
       "SELECT count(*) FROM tpcds_parquet.store_sales WHERE ss_sold_date_sk < 2451200",
       ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
        "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # Test optimized count star query with 1 scan range detected as trivial query
    # and assign to tiny group.
    self._run_query_and_verify_profile(
       "SELECT count(*) FROM tpcds_parquet.date_dim",
       ["Executor Group: empty group (using coordinator only)",
        "ExecutorGroupsConsidered: 1",
        "Verdict: Assign to first group because the number of nodes is 1"],
       ["EffectiveParallelism:", "CpuAsk:", "AvgAdmissionSlotsPerExecutor:"])

    # Test unoptimized count star query assign to tiny group.
    self._run_query_and_verify_profile(
      ("SELECT count(*) FROM tpcds_parquet.store_sales "
       "WHERE ss_ext_discount_amt != 0.3857"),
      ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
       "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])

    # Test zero slot scan query assign to tiny group.
    self._run_query_and_verify_profile(
      "SELECT count(ss_sold_date_sk) FROM tpcds_parquet.store_sales",
      ["Executor Group: root.tiny-group", "EffectiveParallelism: 1",
       "ExecutorGroupsConsidered: 1", "AvgAdmissionSlotsPerExecutor: 1"])
    # END testing count queries

    # BEGIN testing insert + MAX_FS_WRITER
    # Test unpartitioned insert, small scan, no MAX_FS_WRITER.
    # Scanner and writer will collocate since num scanner equals to num writer (1).
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} as "
       "select id, year from functional_parquet.alltypes"
       ).format(unique_database, "test_ctas1"),
      ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 1", "AvgAdmissionSlotsPerExecutor: 1"])
    self.__verify_fs_writers(result, 1, [0, 1])

    # Test unpartitioned insert, small scan, no MAX_FS_WRITER, with limit.
    # The limit will cause an exchange node insertion between scanner and writer.
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} as "
       "select id, year from functional_parquet.alltypes limit 100000"
       ).format(unique_database, "test_ctas2"),
      ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 2", "AvgAdmissionSlotsPerExecutor: 2"])
    self.__verify_fs_writers(result, 1, [0, 2])

    # Test partitioned insert, small scan, no MAX_FS_WRITER.
    # Scanner and writer will collocate since num scanner equals to num writer (1).
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} partitioned by (year) as "
       "select id, year from functional_parquet.alltypes"
       ).format(unique_database, "test_ctas3"),
      ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 1", "AvgAdmissionSlotsPerExecutor: 1"])
    self.__verify_fs_writers(result, 1, [0, 1])

    # Test unpartitioned insert, large scan, no MAX_FS_WRITER.
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} as "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas4"),
      ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 1"])
    self.__verify_fs_writers(result, 1, [0, 1])

    # Test partitioned insert, large scan, no MAX_FS_WRITER.
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} partitioned by (ss_store_sk) as "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas5"),
      ["Executor Group: root.small-group", "ExecutorGroupsConsidered: 2",
       "Verdict: Match", "CpuAsk: 4"])
    self.__verify_fs_writers(result, 2, [0, 2, 2])

    # Test partitioned insert, large scan, high MAX_FS_WRITER.
    self._set_query_options({'MAX_FS_WRITERS': '30'})
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} partitioned by (ss_store_sk) as "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas6"),
      ["Executor Group: root.small-group", "ExecutorGroupsConsidered: 2",
       "Verdict: Match", "CpuAsk: 4"])
    self.__verify_fs_writers(result, 2, [0, 2, 2])

    # Test partitioned insert, large scan, low MAX_FS_WRITER.
    self._set_query_options({'MAX_FS_WRITERS': '2'})
    result = self._run_query_and_verify_profile(
      ("create table {0}.{1} partitioned by (ss_store_sk) as "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas7"),
      ["Executor Group: root.small-group", "ExecutorGroupsConsidered: 2",
       "Verdict: Match", "CpuAsk: 4", "AvgAdmissionSlotsPerExecutor: 2"])
    self.__verify_fs_writers(result, 2, [0, 2, 2])

    # Test that non-CTAS unpartitioned insert works. MAX_FS_WRITER=2.
    result = self._run_query_and_verify_profile(
      ("insert overwrite {0}.{1} "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas4"),
      ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
       "Verdict: Match", "CpuAsk: 1", "AvgAdmissionSlotsPerExecutor: 1"])
    self.__verify_fs_writers(result, 1, [0, 1])

    # Test that non-CTAS partitioned insert works. MAX_FS_WRITER=2.
    result = self._run_query_and_verify_profile(
      ("insert overwrite {0}.{1} (ss_item_sk, ss_ticket_number) "
       "partition (ss_store_sk) "
       "select ss_item_sk, ss_ticket_number, ss_store_sk "
       "from tpcds_parquet.store_sales").format(unique_database, "test_ctas7"),
      ["Executor Group: root.small-group", "ExecutorGroupsConsidered: 2",
       "Verdict: Match", "CpuAsk: 4", "AvgAdmissionSlotsPerExecutor: 2"])
    self.__verify_fs_writers(result, 2, [0, 2, 2])

    # Unset MAX_FS_WRITERS.
    self._set_query_options({'MAX_FS_WRITERS': ''})
    # END testing insert + MAX_FS_WRITER

    # BEGIN test slot count strategy
    # Unset SLOT_COUNT_STRATEGY to use default strategy, which is max # of instances
    # of any fragment on that backend.
    # TPCDS_Q1 at root.large_group will have following CoreCount trace:
    #   CoreCount={total=16 trace=F15:3+F01:1+F14:3+F03:1+F13:3+F05:1+F12:3+F07:1},
    #   coresRequired=16
    self._set_query_options({'SLOT_COUNT_STRATEGY': ''})
    result = self._run_query_and_verify_profile(TPCDS_Q1,
      ["Executor Group: root.large-group", "ExecutorGroupsConsidered: 3",
       "Verdict: Match", "CpuAsk: 16",
       "AdmissionSlots: 1"  # coordinator and executors all have 1 slot
       ],
      ["AvgAdmissionSlotsPerExecutor:", "AdmissionSlots: 6"])

    # Test with SLOT_COUNT_STRATEGY='PLANNER_CPU_ASK'.
    self._set_query_options({'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK'})
    result = self._run_query_and_verify_profile(TPCDS_Q1,
      ["Executor Group: root.large-group", "ExecutorGroupsConsidered: 3",
       "Verdict: Match", "CpuAsk: 16", "AvgAdmissionSlotsPerExecutor: 6",
       # coordinator has 1 slot
       "AdmissionSlots: 1",
       # 1 executor has F15:1+F01:1+F14:1+F03:1+F13:1+F05:1+F12:1+F07:1 = 8 slots
       "AdmissionSlots: 8",
       # 2 executors have F15:1+F14:1+F13:1+F12:1 = 4 slots
       "AdmissionSlots: 4"
       ])
    # END test slot count strategy

    # Check resource pools on the Web queries site and admission site
    self._verify_query_num_for_resource_pool("root.tiny", 18)
    self._verify_query_num_for_resource_pool("root.small", 4)
    self._verify_query_num_for_resource_pool("root.large", 7)
    self._verify_total_admitted_queries("root.tiny", 23)
    self._verify_total_admitted_queries("root.small", 7)
    self._verify_total_admitted_queries("root.large", 7)

  @pytest.mark.execute_serially
  def test_query_cpu_count_divisor_two(self):
    # Expect to run the query on the small group (driven by MemoryAsk),
    # But the CpuAsk is around half of EffectiveParallelism.
    coordinator_test_args = "-query_cpu_count_divisor=2 "
    self._setup_three_exec_group_cluster(coordinator_test_args)
    self._set_query_options({
      'COMPUTE_PROCESSING_COST': 'true',
      'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group",
         "CpuAsk: 1", "EffectiveParallelism: 1",
         "CpuCountDivisor: 2", "ExecutorGroupsConsidered: 1",
         "AvgAdmissionSlotsPerExecutor: 1"])

    # Test that QUERY_CPU_COUNT_DIVISOR option can override
    # query_cpu_count_divisor flag.
    self._set_query_options({'QUERY_CPU_COUNT_DIVISOR': '1.0'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group",
         "CpuAsk: 1", "EffectiveParallelism: 1",
         "CpuCountDivisor: 1", "ExecutorGroupsConsidered: 1",
         "AvgAdmissionSlotsPerExecutor: 1"])
    self._set_query_options({'QUERY_CPU_COUNT_DIVISOR': '0.5'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group",
         "CpuAsk: 2", "EffectiveParallelism: 1",
         "CpuCountDivisor: 0.5", "ExecutorGroupsConsidered: 1",
         "AvgAdmissionSlotsPerExecutor: 1"])
    self._set_query_options({'QUERY_CPU_COUNT_DIVISOR': '2.0'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.tiny-group",
         "CpuAsk: 1", "EffectiveParallelism: 1",
         "CpuCountDivisor: 2", "ExecutorGroupsConsidered: 1",
         "AvgAdmissionSlotsPerExecutor: 1"])

    # Check resource pools on the Web queries site and admission site
    self._verify_query_num_for_resource_pool("root.tiny", 4)
    self._verify_query_num_for_resource_pool("root.small", 0)
    self._verify_total_admitted_queries("root.tiny", 4)
    self._verify_total_admitted_queries("root.small", 0)

  @pytest.mark.execute_serially
  def test_query_cpu_count_divisor_fraction(self):
    # Expect to run the query on the large group
    coordinator_test_args = ("-min_processing_per_thread=550000 "
        "-query_cpu_count_divisor=0.03 ")
    self._setup_three_exec_group_cluster(coordinator_test_args)
    self._set_query_options({
      'COMPUTE_PROCESSING_COST': 'true',
      'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK',
      'MAX_FRAGMENT_INSTANCES_PER_NODE': '1'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.large-group", "EffectiveParallelism: 3",
         "ExecutorGroupsConsidered: 3", "CpuAsk: 167",
         "Verdict: Match", "AvgAdmissionSlotsPerExecutor: 1"])

    # set MAX_FRAGMENT_INSTANCES_PER_NODE=3.
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '3'})
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.large-group", "EffectiveParallelism: 9",
         "ExecutorGroupsConsidered: 3", "CpuAsk: 167",
         "Verdict: no executor group set fit. Admit to last executor group set.",
         "AvgAdmissionSlotsPerExecutor: 3"])

    # Unset MAX_FRAGMENT_INSTANCES_PER_NODE.
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': ''})

    # Expect that a query still admitted to last group even if
    # its resource requirement exceed the limit on that last executor group.
    # CpuAsk is 300 at root.large and 267 at other group sets.
    self._run_query_and_verify_profile(CPU_TEST_QUERY,
        ["Executor Group: root.large-group", "EffectiveParallelism: 9",
         "ExecutorGroupsConsidered: 3", "CpuAsk: 267", "CpuAsk: 300",
         "Verdict: no executor group set fit. Admit to last executor group set.",
         "AvgAdmissionSlotsPerExecutor: 3"])

    # Check resource pools on the Web queries site and admission site
    self._verify_query_num_for_resource_pool("root.large", 3)
    self._verify_total_admitted_queries("root.large", 3)

  @pytest.mark.execute_serially
  def test_no_skip_resource_checking(self):
    """This test check that executor group limit is enforced if
    skip_resource_checking_on_last_executor_group_set=false."""
    coordinator_test_args = ("-query_cpu_count_divisor=0.01 "
        "-skip_resource_checking_on_last_executor_group_set=false ")
    self._setup_three_exec_group_cluster(coordinator_test_args)
    self._set_query_options({
      'COMPUTE_PROCESSING_COST': 'true',
      'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK'})
    result = self.execute_query_expect_failure(self.client, CPU_TEST_QUERY)
    assert ("AnalysisException: The query does not fit largest executor group sets. "
        "Reason: not enough cpu cores (require=300, max=192).") in str(result)

  @pytest.mark.execute_serially
  def test_min_processing_per_thread_small(self):
    """Test processing cost with min_processing_per_thread smaller than default"""
    coordinator_test_args = "-min_processing_per_thread=500000"
    self._setup_three_exec_group_cluster(coordinator_test_args)

    # Test that GROUPING_TEST_QUERY will get assigned to the large group.
    self._set_query_options({
      'COMPUTE_PROCESSING_COST': 'true',
      'SLOT_COUNT_STRATEGY': 'PLANNER_CPU_ASK'})
    self._run_query_and_verify_profile(GROUPING_TEST_QUERY,
        ["Executor Group: root.large-group", "ExecutorGroupsConsidered: 3",
          "Verdict: Match", "CpuAsk: 15",
          "AvgAdmissionSlotsPerExecutor: 5"])

    # Test that high_scan_cost_query will get assigned to the large group.
    high_scan_cost_query = ("SELECT ss_item_sk FROM tpcds_parquet.store_sales "
        "WHERE ss_item_sk < 1000000 GROUP BY ss_item_sk LIMIT 10")
    self._run_query_and_verify_profile(high_scan_cost_query,
        ["Executor Group: root.small-group", "ExecutorGroupsConsidered: 2",
          "Verdict: Match", "CpuAsk: 4",
          "AvgAdmissionSlotsPerExecutor: 2"])

    # Test that high_scan_cost_query will get assigned to the tiny group
    # if MAX_FRAGMENT_INSTANCES_PER_NODE is limited to 1.
    self._set_query_options({'MAX_FRAGMENT_INSTANCES_PER_NODE': '1'})
    self._run_query_and_verify_profile(high_scan_cost_query,
        ["Executor Group: root.tiny-group", "ExecutorGroupsConsidered: 1",
          "Verdict: Match", "CpuAsk: 2",
          "AvgAdmissionSlotsPerExecutor: 1"])

    # Check resource pools on the Web queries site and admission site
    self._verify_query_num_for_resource_pool("root.tiny", 1)
    self._verify_query_num_for_resource_pool("root.small", 1)
    self._verify_query_num_for_resource_pool("root.large", 1)
    self._verify_total_admitted_queries("root.tiny", 1)
    self._verify_total_admitted_queries("root.small", 1)
    self._verify_total_admitted_queries("root.large", 1)

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

  def _setup_two_coordinator_two_exec_group_cluster(self, coordinator_test_args):
    """Start a cluster with two coordinators and two executor groups that mapped to
    the same request pool 'root.queue1'."""
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-allocation.xml")
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-empty.xml")
    # Start with a regular admission config with multiple pools and no resource limits.
    self._restart_coordinators(num_coordinators=2,
         extra_args="-fair_scheduler_allocation_path %s "
                    "-llama_site_path %s %s" %
                    (fs_allocation_path, llama_site_path, coordinator_test_args))
    # Add two executor groups with 2 admission slots and 1 executor.
    self._add_executor_group("group1", min_size=1, admission_control_slots=2,
                             resource_pool="root.queue1")
    self._add_executor_group("group2", min_size=1, admission_control_slots=2,
                             resource_pool="root.queue1")
    assert self._get_num_executor_groups(only_healthy=True) == 2

  def _execute_query_async_using_client_and_verify_exec_group(self, client, query,
    config_options, expected_group_str):
    """Execute 'query' asynchronously using 'client' with given 'config_options'.
    Assert existence of expected_group_str in query profile."""
    client.set_configuration(config_options)
    query_handle = client.execute_async(query)
    self.wait_for_state(query_handle, client.QUERY_STATES['RUNNING'], 30, client=client)
    assert expected_group_str in client.get_runtime_profile(query_handle)

  @pytest.mark.execute_serially
  def test_default_assign_policy_with_multiple_exec_groups_and_coordinators(self):
    """Tests that the default admission control assign policy is filling up executor
    groups one by one."""
    # A long running query that runs on every executor
    QUERY = "select * from functional_parquet.alltypes \
             where month < 3 and id + random() < sleep(100);"
    coordinator_test_args = ""
    self._setup_two_coordinator_two_exec_group_cluster(coordinator_test_args)
    # Create fresh clients
    self.create_impala_clients()
    second_coord_client = self.create_client_for_nth_impalad(1)
    # Check that the first two queries both run in 'group1'.
    self._execute_query_async_using_client_and_verify_exec_group(self.client,
        QUERY, {'request_pool': 'queue1'}, "Executor Group: root.queue1-group1")
    self._execute_query_async_using_client_and_verify_exec_group(second_coord_client,
        QUERY, {'request_pool': 'queue1'}, "Executor Group: root.queue1-group1")
    self.client.close()
    second_coord_client.close()

  @pytest.mark.execute_serially
  def test_load_balancing_with_multiple_exec_groups_and_coordinators(self):
    """Tests that the admission controller balance queries across multiple
    executor groups that mapped to the same request pool when setting
    balance_queries_across_executor_groups true."""
    # A long running query that runs on every executor
    QUERY = "select * from functional_parquet.alltypes \
             where month < 3 and id + random() < sleep(100);"
    coordinator_test_args = "-balance_queries_across_executor_groups=true"
    self._setup_two_coordinator_two_exec_group_cluster(coordinator_test_args)
    # Create fresh clients
    self.create_impala_clients()
    second_coord_client = self.create_client_for_nth_impalad(1)
    # Check that two queries run in two different groups.
    self._execute_query_async_using_client_and_verify_exec_group(self.client,
        QUERY, {'request_pool': 'queue1'}, "Executor Group: root.queue1-group1")
    self._execute_query_async_using_client_and_verify_exec_group(second_coord_client,
        QUERY, {'request_pool': 'queue1'}, "Executor Group: root.queue1-group2")
    self.client.close()
    second_coord_client.close()

  @pytest.mark.execute_serially
  def test_partial_availability(self):
    """Test query planning and execution when only 80% of executor is up.
    This test will run a query over 5 node executor group at its healthy threshold (4)
    and start the last executor after query is planned.
    """
    coordinator_test_args = ''
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")

    # Reuse cluster configuration from _setup_three_exec_group_cluster, but only start
    # root.large executor groups.
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-3-groups.xml")
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-3-groups.xml")

    # extra args template to start coordinator
    extra_args_template = ("-vmodule admission-controller=3 "
        "-admission_control_slots=8 "
        "-expected_executor_group_sets=root.large:5 "
        "-fair_scheduler_allocation_path %s "
        "-llama_site_path %s "
        "%s ")

    # Start with a regular admission config, multiple pools, no resource limits.
    self._restart_coordinators(num_coordinators=1,
        extra_args=extra_args_template % (fs_allocation_path, llama_site_path,
          coordinator_test_args))

    # Create fresh client
    self.create_impala_clients()
    # Start root.large exec group with 8 admission slots and 4 executors.
    healthy_threshold = 4
    self._add_executor_group("group", healthy_threshold, num_executors=healthy_threshold,
                             admission_control_slots=8, resource_pool="root.large",
                             extra_args="-mem_limit=2g")
    assert self._get_num_executor_groups(only_healthy=False) == 1
    assert self._get_num_executor_groups(only_healthy=False,
                                         exec_group_set_prefix="root.large") == 1

    # Run query and let it compile, but delay admission for 10s
    handle = self.execute_query_async(CPU_TEST_QUERY, {
      "COMPUTE_PROCESSING_COST": "true",
      "DEBUG_ACTION": "AC_BEFORE_ADMISSION:SLEEP@10000"})

    # Start the 5th executor.
    self._add_executors("group", healthy_threshold, num_executors=1,
        resource_pool="root.large", extra_args="-mem_limit=2g", expected_num_impalads=6)

    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 60)
    profile = self.client.get_runtime_profile(handle)
    assert "F00:PLAN FRAGMENT [RANDOM] hosts=4 instances=4" in profile, profile
    assert ("Scheduler Warning: Cluster membership might changed between planning and "
        "scheduling, F00 scheduled instance count (5) is higher than its effective "
        "count (4)") in profile, profile
    self.client.close_query(handle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1, cluster_size=1,
      impalad_args="--num_expected_executors=7")
  def test_expected_executors_no_healthy_groups(self):
    """Tests expected executor group size used for planning when no executor groups
    are healthy. Query planning should use expected executor group size from
    'num_expected_executors' in such cases. If 'expected_executor_group_sets' is
    set then executor group size is obtained from that config.
    """
    # Create fresh client
    self.create_impala_clients()

    # Compute stats on tpcds_parquet.store_sales
    self.execute_query_expect_success(self.client,
        "compute stats tpcds_parquet.store_sales", query_options={'NUM_NODES': '1'})

    # Query planning should use expected number hosts from 'num_expected_executors'.
    handle = self.execute_query_async(CPU_TEST_QUERY, {
      "COMPUTE_PROCESSING_COST": "true"})
    sleep(1)
    profile = self.client.get_runtime_profile(handle)
    assert "F00:PLAN FRAGMENT [RANDOM] hosts=7" in profile, profile
    # The query should run on default resource pool.
    assert "Request Pool: default-pool" in profile, profile

    coordinator_test_args = ''
    # The path to resources directory which contains the admission control config files.
    RESOURCES_DIR = os.path.join(os.environ['IMPALA_HOME'], "fe", "src", "test",
                                 "resources")

    # Reuse cluster configuration from _setup_three_exec_group_cluster, but only start
    # root.large executor groups.
    fs_allocation_path = os.path.join(RESOURCES_DIR, "fair-scheduler-3-groups.xml")
    llama_site_path = os.path.join(RESOURCES_DIR, "llama-site-3-groups.xml")

    # extra args template to start coordinator
    extra_args_template = ("-vmodule admission-controller=3 "
        "-expected_executor_group_sets=root.small:2,root.large:12 "
        "-fair_scheduler_allocation_path %s "
        "-llama_site_path %s "
        "%s ")

    # Start with a regular admission config, multiple pools, no resource limits.
    self._restart_coordinators(num_coordinators=1,
        extra_args=extra_args_template % (fs_allocation_path, llama_site_path,
          coordinator_test_args))

    self.create_impala_clients()

    # Small query should get planned using the small executor group set's resources.
    handle = self.execute_query_async(CPU_TEST_QUERY, {
      "COMPUTE_PROCESSING_COST": "true"})
    sleep(1)
    profile = self.client.get_runtime_profile(handle)
    assert "F00:PLAN FRAGMENT [RANDOM] hosts=2" in profile, profile
    # The query should run on root.small resource pool.
    assert "Request Pool: root.small" in profile, profile

    # Large query should get planned using the large executor group set's resources.
    handle = self.execute_query_async(GROUPING_TEST_QUERY, {
      "COMPUTE_PROCESSING_COST": "true"})
    sleep(1)
    profile = self.client.get_runtime_profile(handle)
    assert "F00:PLAN FRAGMENT [RANDOM] hosts=2" in profile, profile
    # The query should run on root.large resource pool.
    assert "Request Pool: root.small" in profile, profile

    self.client.close_query(handle)
