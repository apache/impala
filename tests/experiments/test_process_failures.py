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
# Impala process failure test suite

import pytest

from tests.common.custom_cluster_test_suite import (
    NUM_SUBSCRIBERS,
    CLUSTER_SIZE,
    CustomClusterTestSuite)

# The exact query doesn't matter much for these tests, just want a query that touches
# data on all nodes.
QUERY = "select count(l_comment) from lineitem"

# Validates killing and restarting impalad processes between query executions
class TestProcessFailures(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  def test_restart_coordinator(self, vector):
    """Restarts the coordinator between queries"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()

    self.execute_query_using_client(client, QUERY, vector)

    statestored = self.cluster.statestored
    impalad.restart()
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)

    # Reconnect
    client = impalad.service.create_beeswax_client()
    impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)
    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_restart_statestore(self, vector):
    """Tests the cluster still functions when the statestore dies"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    statestored = self.cluster.statestored
    statestored.kill()
    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', 0, timeout=60)

    # impalad should still see the same number of live backends
    assert impalad.service.get_num_known_live_backends() == CLUSTER_SIZE

    self.execute_query_using_client(client, QUERY, vector)
    # Reconnect
    statestored.start()

    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', 1, timeout=60)
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)

    # Wait for the number of live backends to reach the cluster size. Even though
    # all backends have subscribed to the statestore, this impalad may not have
    # received the update yet.
    impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE, timeout=60)

    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_kill_restart_worker(self, vector):
    """Verifies a worker is able to be killed"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_using_client(client, QUERY, vector)

    # select a different impalad and restart it
    worker_impalad = self.cluster.get_different_impalad(impalad)
    print "Coordinator impalad: %s Worker impalad: %s" % (impalad, worker_impalad)

    # Start executing a query. It will be cancelled due to a killed worker.
    handle = self.execute_query_async_using_client(client, QUERY, vector)

    statestored = self.cluster.statestored
    worker_impalad.kill()

    # First wait until the the statestore realizes the impalad has gone down.
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)
    # Wait until the impalad registers another instance went down.
    impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE - 1, timeout=60)

    # Wait until the in-flight query has been cancelled.
    # The in-flight query should have been cancelled, reporting a failed worker as the
    # cause. The query may have been cancelled because the state store detected a failed
    # node, or because a stream sender failed to establish a thrift connection. It is
    # non-deterministic which of those paths will initiate cancellation, but in either
    # case the query status should include the failed (or unreachable) worker.
    impalad.service.wait_for_query_state(client, handle,\
                                         client.QUERY_STATES['EXCEPTION'])

    # Wait for the query status on the query profile web page to contain the
    # expected failed hostport.
    query_id = handle.get_handle().id
    failed_hostport = "%s:%s" % (worker_impalad.service.hostname,\
                                 worker_impalad.service.be_port)
    query_status_match =\
      impalad.service.wait_for_query_status(client, query_id, failed_hostport)
    try:
      if not query_status_match:
        query_profile_page = impalad.service.read_query_profile_page(query_id)
        assert False, "Query status did not contain expected hostport %s\n\n%s"\
          % (failed_hostport, query_profile_page)
    finally:
      self.close_query_using_client(client, handle)

    # Should work fine even if a worker is down.
    self.execute_query_using_client(client, QUERY, vector)

    # Bring the worker back online and validate queries still work.
    worker_impalad.start()
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
    worker_impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)
    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_restart_catalogd(self, vector):
    # Choose a random impalad verify a query can run against it.
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_using_client(client, QUERY, vector)

    # Kill the catalogd.
    catalogd = self.cluster.catalogd
    catalogd.kill()

    # The statestore should detect the catalog service has gone down.
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS - 1, timeout=60)

    # We should still be able to execute queries using the impalad.
    self.execute_query_using_client(client, QUERY, vector)

    # Start the catalog service back up.
    catalogd.start()
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)

    # Execute a query against the catalog service.
    impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)
    self.execute_query_using_client(client, "refresh lineitem", vector)
    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_restart_all_impalad(self, vector):
    """Restarts all the impalads and runs a query"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_using_client(client, QUERY, vector)

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
    statestored.service.wait_for_live_subscribers(NUM_SUBSCRIBERS, timeout=60)
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE, timeout=60)
      impalad.service.wait_for_metric_value('catalog.ready', 1, timeout=60)
      client = impalad.service.create_beeswax_client()
      self.execute_query_using_client(client, QUERY, vector)
      # Make sure the catalog service is actually back up by executing an operation
      # against it.
      self.execute_query_using_client(client, "refresh lineitem", vector)
      self.execute_query_using_client(client, QUERY, vector)
