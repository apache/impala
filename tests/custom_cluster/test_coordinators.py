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
# The base class that should be used for almost all Impala tests

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestCoordinators(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  def test_multiple_coordinators(self):
    """Test a cluster configuration in which not all impalad nodes are coordinators.
      Verify that only coordinators can accept client connections and that select and DDL
      queries run successfully."""

    db_name = "TEST_MUL_COORD_DB"
    self._start_impala_cluster([], num_coordinators=2, cluster_size=3)
    assert len(self.cluster.impalads) == 3

    coordinator1 = self.cluster.impalads[0]
    coordinator2 = self.cluster.impalads[1]
    worker = self.cluster.impalads[2]

    # Verify that Beeswax and HS2 client connections can't be established at a worker node
    beeswax_client = None
    try:
      beeswax_client = worker.service.create_beeswax_client()
    except: pass
    finally:
      assert beeswax_client is None

    hs2_client = None
    try:
      hs2_client = worker.service.create_hs2_client()
    except: pass
    finally:
      assert hs2_client is None

    # Verify that queries can successfully run on coordinator nodes
    try:
      client1 = coordinator1.service.create_beeswax_client()
      client2 = coordinator2.service.create_beeswax_client()

      # select queries
      self.execute_query_expect_success(client1, "select 1")
      self.execute_query_expect_success(client2, "select * from functional.alltypes");
      # DDL queries w/o SYNC_DDL
      self.execute_query_expect_success(client1, "refresh functional.alltypes")
      query_options = {"sync_ddl" : 1}
      self.execute_query_expect_success(client2, "refresh functional.alltypesagg",
          query_options)
      self.execute_query_expect_success(client1,
          "create database if not exists %s" % db_name, query_options)
      # Create a table using one coordinator
      self.execute_query_expect_success(client1,
          "create table %s.foo1 (col int)" % db_name, query_options)
      # Drop the table using the other coordinator
      self.execute_query_expect_success(client2, "drop table %s.foo1" % db_name,
          query_options)
      # Swap roles and repeat
      self.execute_query_expect_success(client2,
          "create table %s.foo2 (col int)" % db_name, query_options)
      self.execute_query_expect_success(client1, "drop table %s.foo2" % db_name,
          query_options)
      self.execute_query_expect_success(client1, "drop database %s cascade" % db_name)
    finally:
      # Ensure the worker hasn't received any table metadata
      num_tbls = worker.service.get_metric_value('catalog.num-tables')
      assert num_tbls == 0
      client1.close()
      client2.close()

  @pytest.mark.execute_serially
  def test_single_coordinator_cluster_config(self):
    """Test a cluster configuration with a single coordinator."""

    def exec_and_verify_num_executors(expected_num_of_executors):
      """Connects to the coordinator node, runs a query and verifies that certain
        operators are executed on 'expected_num_of_executors' nodes."""
      coordinator = self.cluster.impalads[0]
      try:
        client = coordinator.service.create_beeswax_client()
        assert client is not None
        query = "select count(*) from functional.alltypesagg"
        result = self.execute_query_expect_success(client, query)
        # Verify that SCAN and AGG are executed on the expected number of
        # executor nodes
        for rows in result.exec_summary:
          if rows['operator'] == 'OO:SCAN HDFS':
            assert rows['num_hosts'] == expected_num_of_executors
          elif rows['operator'] == '01:AGGREGATE':
            assert rows['num_hosts'] == expected_num_of_executors
      finally:
        client.close()

    # Cluster config where the coordinator can execute query fragments
    self._start_impala_cluster([], cluster_size=3, num_coordinators=1,
        use_exclusive_coordinators=False)
    exec_and_verify_num_executors(3)
    # Stop the cluster
    self._stop_impala_cluster()
    # Cluster config where the coordinator can only execute coordinator fragments
    self._start_impala_cluster([], cluster_size=3, num_coordinators=1,
        use_exclusive_coordinators=True)
    exec_and_verify_num_executors(2)
