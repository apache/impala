#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Impala process failure test suite

import logging
import sys
import pytest
import time
import os
from random import choice
from subprocess import call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster
from time import sleep

IMPALA_HOME = os.environ['IMPALA_HOME']
CLUSTER_SIZE = 3
# The exact query doesn't matter much for these tests, just want a query that touches
# data on all nodes.
QUERY = "select count(l_comment) from lineitem"

# Validates killing and restarting impalad processes between query executions
class TestProcessFailures(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestProcessFailures, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] == 0)

  def setup_class(cls):
    # No-op, but needed to override base class setup which is not wanted in this
    # case.
    pass

  def teardown_class(cls):
    pass

  def setup_method(self, method):
    # Start a clean new cluster before each test
    self.__start_impala_cluster()
    sleep(3)
    self.cluster = ImpalaCluster()
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_backends(CLUSTER_SIZE, timeout=15)
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE, timeout=30)

  @classmethod
  def __stop_impala_cluster(cls):
    # TODO: Figure out a better way to handle case where processes are just starting
    # / cleaning up so that sleeps are not needed.
    sleep(2)
    call([os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'), '--kill_only'])
    sleep(2)

  @classmethod
  def __start_impala_cluster(cls):
    call([os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'),
        '--wait', '-s %d' % CLUSTER_SIZE])

  @pytest.mark.execute_serially
  def test_restart_coordinator(self, vector):
    """Restarts the coordinator between queries"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()

    self.execute_query_using_client(client, QUERY, vector)

    statestored = self.cluster.statestored
    impalad.restart()
    statestored.service.wait_for_live_backends(CLUSTER_SIZE, timeout=15)

    # Reconnect
    client = impalad.service.create_beeswax_client()
    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_restart_statestore(self, vector):
    """Tests the cluster still functions when the statestore dies"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    statestored = self.cluster.statestored
    statestored.kill()
    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', 0, timeout=30)

    # impalad should still see the same number of live backends
    assert impalad.service.get_num_known_live_backends() == CLUSTER_SIZE

    self.execute_query_using_client(client, QUERY, vector)
    # Reconnect
    statestored.start()

    impalad.service.wait_for_metric_value(
        'statestore-subscriber.connected', 1, timeout=30)
    statestored.service.wait_for_live_backends(CLUSTER_SIZE, timeout=15)

    # Wait for the number of live backends to reach the cluster size. Even though
    # all backends have subscribed to the statestore, this impalad may not have
    # received the update yet.
    impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE, timeout=30)

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
    handle = client.execute_query_async(QUERY)

    statestored = self.cluster.statestored
    worker_impalad.kill()

    # First wait until the the statestore realizes the impalad has gone down.
    statestored.service.wait_for_live_backends(CLUSTER_SIZE - 1, timeout=30)
    # Wait until the impalad registers another instance went down.
    impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE - 1, timeout=30)

    # Wait until the in-flight query has been cancelled.
    impalad.service.wait_for_query_state(client, handle,\
                                         client.query_states['EXCEPTION'])

    # The in-flight query should have been cancelled, reporting a failed worker as the
    # cause. The query may have been cancelled because the state store detected a failed
    # node, or because a stream sender failed to establish a thrift connection. It is
    # non-deterministic which of those paths will initiate cancellation, but in either
    # case the query status should include the failed (or unreachable) worker.
    assert client.get_state(handle) == client.query_states['EXCEPTION']
    
    # Wait for the query status on the query profile web page to contain the
    # expected failed hostport.
    failed_hostport = "%s:%s" % (worker_impalad.service.hostname,\
                                 worker_impalad.service.be_port)
    query_status_match =\
      impalad.service.wait_for_query_status(client, handle.id, failed_hostport)
    if not query_status_match:
      query_profile_page = impalad.service.read_query_profile_page(handle.id)
      assert False, "Query status did not contain expected hostport %s\n\n%s"\
        % (failed_hostport, query_profile_page)

    # Should work fine even if a worker is down.
    self.execute_query_using_client(client, QUERY, vector)

    # Bring the worker back online and validate queries still work.
    worker_impalad.start()
    statestored.service.wait_for_live_backends(CLUSTER_SIZE, timeout=30)
    self.execute_query_using_client(client, QUERY, vector)

  @pytest.mark.execute_serially
  def test_restart_cluster(self, vector):
    """Restarts all the impalads and runs a query"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    self.execute_query_using_client(client, QUERY, vector)

    # Kill each impalad and wait for the statestore to register the failures.
    for impalad_proc in self.cluster.impalads:
      impalad_proc.kill()
    statestored = self.cluster.statestored
    statestored.service.wait_for_live_backends(0, timeout=30)

    # Start each impalad back up and wait for the statestore to see them.
    for impalad_proc in self.cluster.impalads:
      impalad_proc.start()

    # The impalads should re-register with the statestore on restart at which point they
    # can execute queries.
    statestored.service.wait_for_live_backends(CLUSTER_SIZE, timeout=30)
    for impalad in self.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(CLUSTER_SIZE, timeout=30)
      client = impalad.service.create_beeswax_client()
      self.execute_query_using_client(client, QUERY, vector)
