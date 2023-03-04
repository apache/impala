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
import pytest

from beeswaxd.BeeswaxService import QueryState
from tests.common.custom_cluster_test_suite import (
    DEFAULT_CLUSTER_SIZE,
    CustomClusterTestSuite)

# The exact query doesn't matter much for these tests, just want a query that touches
# data on all nodes.
QUERY = "select count(l_comment) from tpch.lineitem"

# The default number of statestore subscribers should be the default cluster size plus
# one for the catalogd.
DEFAULT_NUM_SUBSCRIBERS = DEFAULT_CLUSTER_SIZE + 1


class TestProcessFailures(CustomClusterTestSuite):
  """Validates killing and restarting impalad processes between query executions."""

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These tests only run in exhaustive')
    super(TestProcessFailures, cls).setup_class()

  @pytest.mark.execute_serially
  def test_restart_coordinator(self):
    """Restarts the coordinator between queries."""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()

    self.execute_query_expect_success(client, QUERY)

    statestored = self.cluster.statestored
    impalad.restart()
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS, timeout=60)

    # Reconnect
    client = impalad.service.create_beeswax_client()
    impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)
    self.execute_query_expect_success(client, QUERY)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1,
      impalad_args="--status_report_max_retry_s=600 --status_report_interval_ms=1000")
  def test_kill_coordinator(self):
    """"Tests that when a coordinator running multiple queries is killed, all
    running fragments on executors are cancelled."""
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    assert client is not None
    # A query which is cancelable and takes long time to execute
    query = "select * from tpch.lineitem t1, tpch.lineitem t2, tpch.lineitem t3 " \
        "where t1.l_orderkey = t2.l_orderkey and t1.l_orderkey = t3.l_orderkey and " \
        "t3.l_orderkey = t2.l_orderkey order by t1.l_orderkey, t2.l_orderkey, " \
        "t3.l_orderkey limit 300"
    num_concurrent_queries = 3
    handles = []

    # Run num_concurrent_queries asynchronously
    for _ in range(num_concurrent_queries):
      handles.append(client.execute_async(query))

    # Wait for the queries to start running
    for handle in handles:
      self.wait_for_state(handle, QueryState.RUNNING, 1000, client=client)

    # Kill the coordinator
    impalad.kill()

    # Assert that all executors have 0 in-flight fragments
    for i in range(1, len(self.cluster.impalads)):
      self.cluster.impalads[i].service.wait_for_metric_value(
        "impala-server.num-fragments-in-flight", 0, timeout=30)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args='--use_local_catalog',
      catalogd_args='--catalog_topic_mode=minimal')
  def test_restart_statestore(self):
    """Tests the cluster still functions when the statestore dies."""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    statestored = self.cluster.statestored
    statestored.kill()
    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', False, timeout=60)

    # impalad should still see the same number of live backends
    assert impalad.service.get_num_known_live_backends() == DEFAULT_CLUSTER_SIZE

    self.execute_query_expect_success(client, QUERY)
    # Reconnect
    statestored.start()

    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', True, timeout=60)
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS, timeout=60)

    # Wait for the number of live backends to reach the cluster size. Even though
    # all backends have subscribed to the statestore, this impalad may not have
    # received the update yet.
    impalad.service.wait_for_num_known_live_backends(DEFAULT_CLUSTER_SIZE, timeout=60)

    self.execute_query_expect_success(client, QUERY)

  @pytest.mark.execute_serially
  def test_kill_restart_worker(self):
    """Verifies a worker is able to be killed."""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, QUERY)

    # select a different impalad and restart it
    worker_impalad = self.cluster.get_different_impalad(impalad)

    # Start executing a query. It will be cancelled due to a killed worker.
    handle = client.execute_async(QUERY)

    statestored = self.cluster.statestored
    worker_impalad.kill()

    # First wait until the the statestore realizes the impalad has gone down.
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS - 1, timeout=60)
    # Wait until the impalad registers another instance went down.
    impalad.service.wait_for_num_known_live_backends(DEFAULT_CLUSTER_SIZE - 1, timeout=60)

    # Wait until the in-flight query has been cancelled.
    # The in-flight query should have been cancelled, reporting a failed worker as the
    # cause. The query may have been cancelled because the state store detected a failed
    # node, or because a stream sender failed to establish a thrift connection. It is
    # non-deterministic which of those paths will initiate cancellation, but in either
    # case the query status should include the failed (or unreachable) worker.
    query_id = handle.get_handle().id
    error_state = "Failed due to unreachable impalad"
    assert impalad.service.wait_for_query_status(client, query_id, error_state)

    # Assert that the query status on the query profile web page contains the expected
    # failed hostport.
    failed_hostport = "%s:%s" % (worker_impalad.service.hostname,
                                 worker_impalad.service.krpc_port)
    query_profile_page = impalad.service.read_query_profile_page(query_id)
    assert failed_hostport in query_profile_page,\
        "Query status did not contain expected hostport %s\n\n%s" % (failed_hostport,
            query_profile_page)

    # Should work fine even if a worker is down.
    self.execute_query_expect_success(client, QUERY)

    # Bring the worker back online and validate queries still work.
    worker_impalad.start()
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS, timeout=60)
    worker_impalad.service.wait_for_metric_value('catalog.ready', True, timeout=60)
    self.execute_query_expect_success(client, QUERY)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args='--use_local_catalog',
    catalogd_args='--catalog_topic_mode=minimal')
  @pytest.mark.xfail(run=False, reason="IMPALA-9848")
  def test_restart_catalogd(self):
    # Choose a random impalad verify a query can run against it.
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, QUERY)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS - 1, timeout=60)

    # We should still be able to execute queries using the impalad.
    self.execute_query_expect_success(client, QUERY)

    # Start the catalog service back up.
    catalogd.start()
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS, timeout=60)

    # Execute a query against the catalog service.
    impalad.service.wait_for_metric_value('catalog.ready', True, timeout=60)
    self.execute_query_expect_success(client, QUERY)

  @pytest.mark.execute_serially
  def test_restart_all_impalad(self):
    """Restarts all the impalads and runs a query"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_expect_success(client, QUERY)

    # Kill each impalad and wait for the statestore to register the failures.
    for impalad_proc in self.cluster.impalads:
      impalad_proc.kill()
    statestored = self.cluster.statestored

    # There should be 1 remining subscriber, the catalogd
    statestored.service.wait_for_live_subscribers(1, timeout=60)

    # Start each impalad back up and wait for the statestore to see them.
    for impalad_proc in self.cluster.impalads:
      impalad_proc.start()

    # The impalads should re-register with the statestore on restart at which point they
    # can execute queries.
    statestored.service.wait_for_live_subscribers(DEFAULT_NUM_SUBSCRIBERS, timeout=60)
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(DEFAULT_CLUSTER_SIZE, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', True, timeout=60)
      client = impalad.service.create_beeswax_client()
      self.execute_query_expect_success(client, QUERY)
      # Make sure the catalog service is actually back up by executing an operation
      # against it.
      self.execute_query_expect_success(client, QUERY)
