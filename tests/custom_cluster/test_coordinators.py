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

from __future__ import absolute_import, division, print_function
import logging
import pytest
import os
import time
from subprocess import check_call
from tests.util.filesystem_utils import get_fs_path
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf, SkipIfFS

LOG = logging.getLogger('test_coordinators')
LOG.setLevel(level=logging.DEBUG)

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
    except Exception as e:
      LOG.info("Caught exception {0}".format(e))
    finally:
      assert beeswax_client is None

    hs2_client = None
    try:
      hs2_client = worker.service.create_hs2_client()
    except Exception as e:
      LOG.info("Caught exception {0}".format(e))
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
      client = None
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
        assert client is not None
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

  @pytest.mark.execute_serially
  def test_executor_only_lib_cache(self):
    """IMPALA-6670: checks that the lib-cache gets refreshed on executor-only nodes"""

    self._start_impala_cluster([], cluster_size=3, num_coordinators=1,
                               use_exclusive_coordinators=True)

    db_name = 'TEST_EXEC_ONLY_CACHE'

    # jar src/tgt paths
    old_src_path = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs.jar')
    new_src_path = os.path.join(
        os.environ['IMPALA_HOME'], 'testdata/udfs/impala-hive-udfs-modified.jar')
    tgt_dir = get_fs_path('/test-warehouse/{0}.db'.format(db_name))
    tgt_path = tgt_dir + "/tmp.jar"

    # copy jar with TestUpdateUdf (old) to tmp.jar
    check_call(["hadoop", "fs", "-mkdir", "-p", tgt_dir])
    self.filesystem_client.copy_from_local(old_src_path, tgt_path)

    coordinator = self.cluster.impalads[0]
    try:
      client = coordinator.service.create_beeswax_client()

      # create the database
      self.execute_query_expect_success(client,
          "create database if not exists %s" % db_name)

      # create a function for TestUpdateUdf (old)
      create_old_fn = (
          "create function `{0}`.`old_fn`(string) returns string LOCATION '{1}' "
          "SYMBOL='org.apache.impala.TestUpdateUdf'".format(db_name, tgt_path))
      self.execute_query_expect_success(client, create_old_fn);

      # run the query for TestUpdateUdf (old) and expect it to work
      old_query = (
          "select count(*) from functional.alltypes where "
          "`{0}`.old_fn(string_col) = 'Old UDF'".format(db_name));
      result = self.execute_query_expect_success(client, old_query)
      assert result.data == ['7300']

      # copy a new jar with TestUpdateUdf (new) and NewReplaceStringUdf to tmp.jar.
      self.filesystem_client.copy_from_local(new_src_path, tgt_path)

      # create a function for the updated TestUpdateUdf.
      create_new_fn = (
          "create function `{0}`.`new_fn`(string) returns string LOCATION '{1}' "
          "SYMBOL='org.apache.impala.TestUpdateUdf'".format(db_name, tgt_path))
      self.execute_query_expect_success(client, create_new_fn);

      # run the query for TestUdf (new) and expect the updated version to work.
      # the udf argument prevents constant expression optimizations, which can mask
      # incorrect lib-cache state/handling.
      # (bug behavior was to get the old version, so number of results would be = 0)
      # Note: if old_fn is run in the same way now, it will pick up the new
      #       implementation. that is current system behavior, so expected.
      new_query = (
          "select count(*) from functional.alltypes where "
          "`{0}`.new_fn(string_col) = 'New UDF'".format(db_name));
      result = self.execute_query_expect_success(client, new_query)
      assert result.data == ['7300']

      # create a function for NewReplaceStringUdf which does not exist in the previous
      # version of the jar.
      create_add_fn = (
          "create function `{0}`.`add_fn`(string) returns string LOCATION '{1}' "
          "SYMBOL='org.apache.impala.NewReplaceStringUdf'".format(db_name, tgt_path))
      self.execute_query_expect_success(client, create_add_fn);

      # run the query for ReplaceString and expect the query to run.
      # (bug behavior is to not find the class)
      add_query = (
          "select count(*) from functional.alltypes where "
          "`{0}`.add_fn(string_col) = 'not here'".format(db_name));
      result = self.execute_query_expect_success(client, add_query)
      assert result.data == ['0']

      # Copy jar to a new path.
      tgt_path_2 = tgt_dir + "/tmp2.jar"
      self.filesystem_client.copy_from_local(old_src_path, tgt_path_2)

      # Add the function.
      create_mismatch_fn = (
          "create function `{0}`.`mismatch_fn`(string) returns string LOCATION '{1}' "
          "SYMBOL='org.apache.impala.TestUpdateUdf'".format(db_name, tgt_path_2))
      self.execute_query_expect_success(client, create_mismatch_fn);

      # Run a query that'll run on only one executor.
      small_query = (
          "select count(*) from functional.tinytable where "
          "`{0}`.mismatch_fn(a) = 'x'").format(db_name)
      self.execute_query_expect_success(client, small_query)

      # Overwrite the jar, giving it a new mtime. The sleep prevents the write to
      # happen too quickly so that its within mtime granularity (1 second).
      time.sleep(2)
      self.filesystem_client.copy_from_local(new_src_path, tgt_path_2)

      # Run the query. Expect the query fails due to mismatched libs at the
      # coordinator and one of the executors.
      mismatch_query = (
          "select count(*) from functional.alltypes where "
          "`{0}`.mismatch_fn(string_col) = 'Old UDF'".format(db_name));
      result = self.execute_query_expect_failure(client, mismatch_query)
      assert "does not match the expected last modified time" in str(result)

      # Refresh, as suggested by the error message.
      # IMPALA-6719: workaround lower-cases db_name.
      self.execute_query_expect_success(client, "refresh functions " + db_name.lower())

      # The coordinator should have picked up the new lib, so retry the query.
      self.execute_query_expect_success(client, mismatch_query)

    # cleanup
    finally:
      self.execute_query_expect_success(client,
          "drop database if exists %s cascade" % db_name)
      if client is not None:
        client.close()
      self._stop_impala_cluster()

  @pytest.mark.execute_serially
  def test_exclusive_coordinator_plan(self):
    """Checks that a distributed plan does not assign scan fragments to a coordinator
    only node. """

    self._start_impala_cluster([], num_coordinators=1, cluster_size=3,
        use_exclusive_coordinators=True)

    assert len(self.cluster.impalads) == 3

    coordinator = self.cluster.impalads[0]
    worker1 = self.cluster.impalads[1]
    worker2 = self.cluster.impalads[2]

    client = None
    try:
      client = coordinator.service.create_beeswax_client()
      assert client is not None
      self.client = client

      client.execute("SET EXPLAIN_LEVEL=2")
      client.execute("SET TEST_REPLAN=0")

      # Ensure that the plan generated always uses only the executor nodes for scanning
      # Multi-partition table
      result = client.execute("explain select count(*) from functional.alltypes "
              "where id NOT IN (0,1,2) and string_col IN ('aaaa', 'bbbb', 'cccc', NULL) "
              "and mod(int_col,50) IN (0,1) and id IN (int_col);").data
      assert 'F00:PLAN FRAGMENT [RANDOM] hosts=2 instances=2' in result
      # Single partition table with 3 blocks
      result = client.execute("explain select * from tpch_parquet.lineitem "
              "union all select * from tpch_parquet.lineitem").data
      assert 'F02:PLAN FRAGMENT [RANDOM] hosts=2 instances=2' in result
    finally:
      assert client is not None
      self._stop_impala_cluster()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=3, num_exclusive_coordinators=1)
  def test_iceberg_metadata_scan_on_coord(self):
    """ Tests that Iceberg metadata scan fragments are scheduled on the coordinator. If
    such a fragment is scheduled on an executor, the below queries fail. Regression test
    for IMPALA-12809"""
    # A metadata table joined with itself.
    q1 = """select count(b.parent_id)
        from functional_parquet.iceberg_query_metadata.history a
        join functional_parquet.iceberg_query_metadata.history b
        on a.snapshot_id = b.snapshot_id"""
    self.execute_query_expect_success(self.client, q1)

    # A metadata table joined with a regular table.
    q2 = """select count(DISTINCT a.parent_id, a.is_current_ancestor)
        from functional_parquet.iceberg_query_metadata.history a
        join functional_parquet.alltypestiny c
        on a.is_current_ancestor = c.bool_col"""
    self.execute_query_expect_success(self.client, q2)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--queue_wait_timeout_ms=2000",
                                    cluster_size=1, num_exclusive_coordinators=1)
  def test_dedicated_coordinator_without_executors(self):
    """This test verifies that a query gets queued and times out when no executors are
    present but a coordinator only query gets executed."""
    # Pick a non-trivial query that needs to be scheduled on executors.
    query = "select count(*) from functional.alltypes where month + random() < 3"
    result = self.execute_query_expect_failure(self.client, query)
    expected_error = "Query aborted:Admission for query exceeded timeout 2000ms in " \
                     "pool default-pool. Queued reason: Waiting for executors to " \
                     "start."
    assert expected_error in str(result)
    # Now pick a coordinator only query.
    query = "select 1"
    self.execute_query_expect_success(self.client, query)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=1, num_exclusive_coordinators=1,
                                    impalad_args="-num_expected_executors=10")
  def test_num_expected_executors_flag(self):
    """Verifies that the '-num_expected_executors' flag is effective."""
    client = self.cluster.impalads[0].service.create_beeswax_client()
    client.execute("set explain_level=2")
    ret = client.execute("explain select * from functional.alltypes a inner join "
                         "functional.alltypes b on a.id = b.id;")
    num_hosts = "hosts=10 instances=10"
    assert num_hosts in str(ret)

  @SkipIfFS.hbase
  @SkipIf.skip_hbase
  @pytest.mark.execute_serially
  def test_executor_only_hbase(self):
    """Verifies HBase tables can be scanned by executor only impalads."""
    self._start_impala_cluster([], cluster_size=3, num_coordinators=1,
                         use_exclusive_coordinators=True)
    client = self.cluster.impalads[0].service.create_beeswax_client()
    query = "select count(*) from functional_hbase.alltypes"
    result = self.execute_query_expect_success(client, query)
    assert result.data == ['7300']
