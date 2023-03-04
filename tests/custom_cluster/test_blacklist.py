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

import pytest
import re
import shutil
import tempfile

from beeswaxd.BeeswaxService import QueryState
from tests.common.skip import SkipIfNotHdfsMinicluster
from tests.common.skip import SkipIfBuildType
from time import sleep

# The BE krpc port of the impalad to simulate disk errors in tests.
FAILED_KRPC_PORT = 27001


def _get_disk_write_fail_action(port):
  return "IMPALA_TMP_FILE_WRITE:127.0.0.1:{port}:FAIL".format(port=port)

# Tests that verify the behavior of the executor blacklist caused by RPC failure.
# Coordinator adds an executor node to its blacklist if the RPC to that node failed.
# Note: query-retry is not enabled by default.
@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestBlacklist(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestBlacklist, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_kill_impalad(self, cursor):
    """Test that verifies that when an impalad is killed, it is properly blacklisted."""
    # Run a query and verify that no impalads are blacklisted yet.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile

    # Kill an impalad
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()

    # Run a query which should fail as the impalad hasn't been blacklisted yet.
    try:
      self.execute_query("select count(*) from tpch.lineitem")
      assert False, "Query was expected to fail"
    except Exception as e:
      assert "Exec() rpc failed" in str(e)

    # Run another query which should succeed and verify the impalad was blacklisted.
    result = self.execute_query("select count(*) from tpch.lineitem")
    backends_json = self.cluster.impalads[0].service.get_debug_webpage_json("/backends")
    match = re.search("Blacklisted Executors: (.*)", result.runtime_profile)
    assert match.group(1) == "%s:%s" % \
        (killed_impalad.hostname, killed_impalad.service.krpc_port), \
        result.runtime_profile
    assert backends_json["num_blacklisted_backends"] == 1, backends_json
    assert backends_json["num_active_backends"] == 2, backends_json
    assert len(backends_json["backends"]) == 3, backends_json
    num_blacklisted = 0
    for backend_json in backends_json["backends"]:
      if str(killed_impalad.service.krpc_port) in backend_json["krpc_address"]:
        assert backend_json["is_blacklisted"], backend_json
        num_blacklisted += 1
      else:
        assert not backend_json["is_blacklisted"], backend_json
    assert num_blacklisted == 1, backends_json

    # Sleep for long enough for the statestore to remove the impalad from the cluster
    # membership, i.e. Statestore::FailedExecutorDetectionTime() + some padding
    sleep(12)
    # Run another query and verify nothing was blacklisted and only 2 backends were
    # scheduled on.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile
    assert re.search("NumBackends: 2", result.runtime_profile), result.runtime_profile

  @pytest.mark.execute_serially
  def test_restart_impalad(self, cursor):
    """Test that verifies the behavior when an impalad is killed, blacklisted, and then
    restarted."""
    # Run a query and verify that no impalads are blacklisted yet.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile

    # Kill an impalad
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()

    # Run a query which should fail as the impalad hasn't been blacklisted yet.
    try:
      self.execute_query("select count(*) from tpch.lineitem")
      assert False, "Query was expected to fail"
    except Exception as e:
      assert "Exec() rpc failed" in str(e)

    # Run another query which should succeed and verify the impalad was blacklisted.
    result = self.execute_query("select count(*) from tpch.lineitem")
    match = re.search("Blacklisted Executors: (.*)", result.runtime_profile)
    assert match.group(1) == "%s:%s" % \
        (killed_impalad.hostname, killed_impalad.service.krpc_port), \
        result.runtime_profile

    # Restart the impalad.
    killed_impalad.start()

    # Sleep for long enough for the statestore to update the membership to include the
    # restarted impalad, ImpaladProcess.start() won't return until the Impalad says its
    # ready to accept connections, at which point it will have already registered with the
    # statestore, so we don't need to sleep very long.
    sleep(2)

    # Run another query and verify nothing was blacklisted and all 3 backends were
    # scheduled on.
    result = self.execute_query("select count(*) from tpch.lineitem")
    assert re.search("Blacklisted Executors: (.*)", result.runtime_profile) is None, \
        result.runtime_profile
    assert re.search("NumBackends: 3", result.runtime_profile), result.runtime_profile

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(num_exclusive_coordinators=1,
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_kill_impalad_with_running_queries(self, cursor):
    """Verifies that when an Impala executor is killed while running a query, that the
    Coordinator blacklists the killed executor."""

    # Run a query asynchronously. With the debug actions, this query should take a few
    # minutes to complete.
    query = "select count(*) from tpch_parquet.lineitem t1, tpch_parquet.lineitem t2 \
        where t1.l_orderkey = t2.l_orderkey"
    handle = self.execute_query_async(query, query_options={
        'debug_action': '0:GETNEXT:DELAY|1:GETNEXT:DELAY'})

    # Wait for the query to start running
    self.wait_for_any_state(handle, [QueryState.RUNNING, QueryState.FINISHED], 10)

    # Kill one of the Impala executors
    killed_impalad = self.cluster.impalads[2]
    killed_impalad.kill()

    # Try to fetch results from the query. Fetch requests should fail because one of the
    # impalads running the query was killed. When the query fails, the Coordinator should
    # add the killed Impala executor to the blacklist (since a RPC to that node failed).
    try:
      self.client.fetch(query, handle)
      assert False, "Query was expected to fail"
    except Exception as e:
      # The query should fail due to an RPC error.
      assert "TransmitData() to " in str(e) or "EndDataStream() to " in str(e)

    # Run another query which should succeed and verify the impalad was blacklisted.
    self.client.clear_configuration()  # remove the debug actions
    result = self.execute_query("select count(*) from tpch.lineitem")
    match = re.search("Blacklisted Executors: (.*)", result.runtime_profile)
    assert match is not None and match.group(1) == "%s:%s" % \
      (killed_impalad.hostname, killed_impalad.service.krpc_port), \
      result.runtime_profile


# Tests that verify the behavior of the executor blacklist caused by disk IO failure.
# Coordinator adds an executor node to its blacklist if that node reported query
# execution status with error caused by its local faulty disk.
# Note: query-retry is not enabled by default and we assume it's not enabled in following
# tests.
@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestBlacklistFaultyDisk(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestBlacklistFaultyDisk, cls).setup_class()

  # Query with order by requires spill to disk if intermediate results don't fit in mem
  spill_query = """
      select o_orderdate, o_custkey, o_comment
      from tpch.orders
      order by o_orderdate
      """
  # Query against a big table with order by requires spill to disk if intermediate
  # results don't fit in memory.
  spill_query_big_table = """
      select l_orderkey, l_linestatus, l_shipdate, l_comment
      from tpch.lineitem
      order by l_orderkey
      """
  # Query without order by can be executed without spilling to disk.
  in_mem_query = """
      select o_orderdate, o_custkey, o_comment from tpch.orders
      """
  # Buffer pool limit that is low enough to force Impala to spill to disk when executing
  # spill_query.
  buffer_pool_limit = "45m"

  def __generate_scratch_dir(self, num):
    result = []
    for i in range(num):
      dir_path = tempfile.mkdtemp()
      self.created_dirs.append(dir_path)
      result.append(dir_path)
      print("Generated dir" + dir_path)
    return result

  def setup_method(self, method):
    # Don't call the superclass method to prevent starting Impala before each test. In
    # this class, each test is responsible for doing that because we want to generate
    # the parameter string to start-impala-cluster in each test method.
    self.created_dirs = []

  def teardown_method(self, method):
    for dir_path in self.created_dirs:
      shutil.rmtree(dir_path, ignore_errors=True)

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  def test_scratch_file_write_failure(self, vector):
    """ Test that verifies that when an impalad failed to execute query during spill-to-
        disk due to disk write error, it is properly blacklisted by coordinator."""

    # Start cluster with spill-to-disk enabled and one dedicated coordinator. Set a high
    # statestore heartbeat frequency so that blacklisted nodes are not timeout too
    # quickly.
    scratch_dirs = self.__generate_scratch_dir(2)
    self._start_impala_cluster([
        '--impalad_args=-logbuflevel=-1',
        '--impalad_args=--scratch_dirs={0}'.format(','.join(scratch_dirs)),
        '--impalad_args=--allow_multiple_scratch_dirs_per_device=true',
        '--impalad_args=--statestore_heartbeat_frequency_ms=2000',
        '--cluster_size=3', '--num_coordinators=1', '--use_exclusive_coordinators'])
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
        expected_count=2)

    # First set debug_action for query as empty.
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    vector.get_value('exec_option')['debug_action'] = ''
    coord_impalad = self.cluster.get_first_impalad()
    client = coord_impalad.service.create_beeswax_client()

    # Expect spill to disk to success with debug_action as empty. Verify all nodes are
    # active.
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    client.close_query(handle)

    backends_json = coord_impalad.service.get_debug_webpage_json("/backends")
    assert backends_json["num_active_backends"] == 3, backends_json
    assert len(backends_json["backends"]) == 3, backends_json

    # Set debug_action to inject disk write error for spill-to-disk on impalad for which
    # krpc port is 27001.
    vector.get_value('exec_option')['debug_action'] = \
        _get_disk_write_fail_action(FAILED_KRPC_PORT)

    # Should be able to execute in-memory query.
    handle = self.execute_query_async_using_client(client, self.in_mem_query, vector)
    results = client.fetch(self.in_mem_query, handle)
    assert results.success
    client.close_query(handle)

    # Expect spill to disk to fail due to disk failure on the impalad with disk failure.
    # Verify one node is blacklisted.
    disk_failure_impalad = self.cluster.impalads[1]
    assert disk_failure_impalad.service.krpc_port == FAILED_KRPC_PORT

    try:
      handle = self.execute_query_async_using_client(client, self.spill_query, vector)
      results = client.fetch(self.spill_query, handle)
      assert False, "Query was expected to fail"
    except Exception as e:
      assert "Query execution failure caused by local disk IO fatal error on backend" \
          in str(e)

    backends_json = coord_impalad.service.get_debug_webpage_json("/backends")
    assert backends_json["num_blacklisted_backends"] == 1, backends_json
    assert backends_json["num_active_backends"] == 2, backends_json
    assert len(backends_json["backends"]) == 3, backends_json
    num_blacklisted = 0
    for backend_json in backends_json["backends"]:
      if str(disk_failure_impalad.service.krpc_port) in backend_json["krpc_address"]:
        assert backend_json["is_blacklisted"], backend_json
        assert "Query execution failure caused by local disk IO fatal error on backend" \
            in backend_json["blacklist_cause"]
        num_blacklisted += 1
      else:
        assert not backend_json["is_blacklisted"], backend_json
    assert num_blacklisted == 1, backends_json

    # Should be able to re-execute same query since the impalad with injected disk error
    # for spill-to-disk was blacklisted.
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    # Verify that the runtime profile contains the "Blacklisted Executors" line with the
    # corresponding backend.
    runtime_profile = client.get_runtime_profile(handle)
    match = re.search("Blacklisted Executors: (.*)", runtime_profile)
    assert match is not None and match.group(1) == "%s:%s" % \
        (disk_failure_impalad.hostname, disk_failure_impalad.service.krpc_port), \
        runtime_profile
    client.close_query(handle)

    # Sleep for long enough time and verify blacklisted backend was removed from the
    # blacklist after blacklisting timeout.
    sleep(30)
    backends_json = coord_impalad.service.get_debug_webpage_json("/backends")
    assert backends_json["num_active_backends"] == 3, backends_json

    # Run the query again without the debug action and verify nothing was blacklisted
    # and all 3 backends were scheduled on.
    vector.get_value('exec_option')['debug_action'] = ''
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    results = client.fetch(self.spill_query, handle)
    assert results.success
    runtime_profile = client.get_runtime_profile(handle)
    assert re.search("Blacklisted Executors: (.*)", runtime_profile) is None, \
        runtime_profile
    assert re.search("NumBackends: 3", runtime_profile), runtime_profile
    client.close_query(handle)

    client.close()
