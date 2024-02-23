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

# TODO: Query retries can be triggered in several different ways, and some tests
# pick one approach for no particular reason, try to consolidate the different
# ways that retries are triggered.
# TODO: Re-factor tests into multiple classes.
# TODO: Add a test that cancels queries while a retry is running

from __future__ import absolute_import, division, print_function
from builtins import map, range
import pytest
import re
import shutil
import tempfile
import time

from random import randint

from RuntimeProfile.ttypes import TRuntimeProfileFormat
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.errors import Timeout
from tests.common.skip import (SkipIfEC, SkipIfBuildType, SkipIfFS,
    SkipIfNotHdfsMinicluster)

# The BE krpc port of the impalad to simulate rpc or disk errors in tests.
FAILED_KRPC_PORT = 27001


def _get_rpc_fail_action(port):
  return "IMPALA_SERVICE_POOL:127.0.0.1:{port}:ExecQueryFInstances:FAIL" \
      .format(port=port)


def _get_disk_fail_action(port):
  return "IMPALA_TMP_FILE_WRITE:127.0.0.1:{port}:FAIL".format(port=port)


# All tests in this class have SkipIfEC because all tests run a query and expect
# the query to be retried when killing a random impalad. On EC this does not always work
# because many queries that might run on three impalads for HDFS / S3 builds, might only
# run on two instances on EC builds. The difference is that EC creates smaller tables
# compared to data stored on HDFS / S3. If the query is only run on two instances, then
# randomly killing one impalad won't necessarily trigger a retry of the query.
@SkipIfEC.parquet_file_size
class TestQueryRetries(CustomClusterTestSuite):

  # A query that shuffles a lot of data. Useful when testing query retries since it
  # ensures that a query fails during a TransmitData RPC. The RPC failure will cause the
  # target impalad to be blacklisted and the query to be retried. The query also has to
  # run long enough so that it fails when an impalad is killed.
  _shuffle_heavy_query = "select * from tpch.lineitem t1, tpch.lineitem t2 where " \
      "t1.l_orderkey = t2.l_orderkey order by t1.l_orderkey, t2.l_orderkey limit 1"
  _shuffle_heavy_query_results = "1\t155190\t7706\t1\t17.00\t21168.23\t0.04\t0.02\tN" \
      "\tO\t1996-03-13\t1996-02-12\t1996-03-22\tDELIVER IN PERSON\tTRUCK" \
      "\tegular courts above the\t1\t15635\t638\t6\t32.00\t49620.16\t0.07\t0.02\tN\tO" \
      "\t1996-01-30\t1996-02-07\t1996-02-03\tDELIVER IN PERSON\tMAIL\tarefully slyly ex"

  # The following query has two union operands. The first operand executes quickly
  # and the second one executes slowly. So we can kill one impalad when some results
  # are ready and the query is still running and has more results.
  _union_query = """select count(*) from functional.alltypestiny
        union all
        select count(*) from functional.alltypes where bool_col = sleep(50)"""

  # A simple count query with predicate. The predicate is needed so that the planner does
  # not create the optimized count(star) query plan.
  _count_query = "select count(*) from tpch_parquet.lineitem where l_orderkey < 50"
  _count_query_result = "55"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_retries_from_cancellation_pool(self, cursor):
    """Tests that queries are retried instead of cancelled if one of the nodes leaves the
    cluster. The retries are triggered by the cancellation pool in the ImpalaServer. The
    cancellation pool listens for updates from the statestore and kills all queries that
    are running on any nodes that are no longer part of the cluster membership."""

    # The following query executes slowly, and does minimal TransmitData RPCs, so it is
    # likely that the statestore detects that the impalad has been killed before a
    # TransmitData RPC has occurred.
    query = "select count(*) from functional.alltypes where bool_col = sleep(50)"

    # Launch the query, wait for it to start running, and then kill an impalad.
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['RUNNING'], 60)

    # Kill a random impalad (but not the one executing the actual query).
    self.__kill_random_impalad()

    # Validate the query results.
    results = self.client.fetch(query, handle)
    assert results.success
    assert len(results.data) == 1
    assert int(results.data[0]) == 3650

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # Validate the state of the runtime profiles.
    retried_runtime_profile = self.client.get_runtime_profile(handle)
    self.__validate_runtime_profiles(
        retried_runtime_profile, handle.get_handle().id, retried_query_id)

    # Validate the state of the client log.
    self.__validate_client_log(handle, retried_query_id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.client.close_query(handle)
    self.__validate_web_ui_state()
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_kill_impalad_expect_retry(self):
    """Launch a query, wait for it to start running, kill a random impalad and then
    validate that the query has successfully been retried. Increase the statestore
    heartbeat frequency so that the query actually fails during execution. Otherwise, it
    is possible the statestore detects that the killed impalad has crashed before the
    query does. This is necessary since this test specifically attempts to test the code
    that retries queries when a query fails mid-execution."""

    # Launch a query, it should be retried.
    handle = self.execute_query_async(self._shuffle_heavy_query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['RUNNING'], 60)

    # Kill a random impalad.
    killed_impalad = self.__kill_random_impalad()

    # Assert that the query succeeded and returned the correct results.
    results = self.client.fetch(self._shuffle_heavy_query, handle)
    assert results.success
    assert len(results.data) == 1
    assert self._shuffle_heavy_query_results in results.data[0]

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # The runtime profile of the retried query.
    retried_runtime_profile = self.client.get_runtime_profile(handle)

    # Assert that the killed impalad shows up in the list of blacklisted executors from
    # the runtime profile.
    self.__assert_executors_blacklisted(killed_impalad, retried_runtime_profile)

    # Validate the state of the runtime profiles.
    self.__validate_runtime_profiles(
        retried_runtime_profile, handle.get_handle().id, retried_query_id)

    # Validate the state of the client log.
    self.__validate_client_log(handle, retried_query_id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.client.close_query(handle)
    self.__validate_web_ui_state()
    self.__validate_memz()

    # Assert that the web ui shows all queries are complete.
    completed_queries = self.cluster.get_first_impalad().service.get_completed_queries()

    # Assert that the most recently completed query is the retried query and it is marked
    # as 'FINISHED.
    assert completed_queries[0]['state'] == 'FINISHED'
    assert completed_queries[0]['query_id'] == self.__get_query_id_from_profile(
        retried_runtime_profile)

    # Assert that the second most recently completed query is the original query and it is
    # marked as 'RETRIED'.
    assert completed_queries[1]['state'] == 'RETRIED'
    assert completed_queries[1]['query_id'] == handle.get_handle().id

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_kill_impalad_expect_retries(self):
    """Similar to 'test_kill_impalad_expect_retry' except it runs multiple queries in
    parallel and then kills an impalad. Several of the code comments in
    'test_kill_impalad_expect_retry' apply here as well."""

    # Launch a set of concurrent queries.
    num_concurrent_queries = 3
    handles = []
    for _ in range(num_concurrent_queries):
      handle = self.execute_query_async(self._shuffle_heavy_query,
          query_options={'retry_failed_queries': 'true'})
      handles.append(handle)

    # Wait for each query to start running.
    running_state = self.client.QUERY_STATES['RUNNING']
    list(map(lambda handle: self.wait_for_state(handle, running_state, 60), handles))

    # Kill a random impalad.
    killed_impalad = self.__kill_random_impalad()

    # Fetch and validate the results from each query.
    for handle in handles:
      results = self.client.fetch(self._shuffle_heavy_query, handle)
      assert results.success
      assert len(results.data) == 1
      assert self._shuffle_heavy_query_results in results.data[0]

    # Validate the runtime profiles of each query.
    for handle in handles:
      retried_runtime_profile = self.client.get_runtime_profile(handle)
      self.__assert_executors_blacklisted(killed_impalad, retried_runtime_profile)

      retried_query_id = self.__get_retried_query_id_from_summary(handle)
      assert retried_query_id is not None

      self.__validate_runtime_profiles(
          retried_runtime_profile, handle.get_handle().id, retried_query_id)

      self.__validate_client_log(handle, retried_query_id)

      self.client.close_query(handle)

    # Validate the state of the Web UI.
    self.__validate_web_ui_state()
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_exec_rpc_failure(self):
    """Test ExecFInstance RPC failures. Set a really high statestort heartbeat frequency
    so that killed impalads are not removed from the cluster membership. This will cause
    Impala to still attempt an Exec RPC to the failed node, which should trigger a
    retry."""

    impalad_service = self.cluster.get_first_impalad().service

    # Kill an impalad, and run a query. The query should be retried.
    killed_impalad = self.__kill_random_impalad()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 60)

    # Validate that the query was retried.
    self.__validate_runtime_profiles_from_service(impalad_service, handle)

    # Assert that the query succeeded and returned the correct results.
    results = self.client.fetch(query, handle)
    assert results.success
    assert len(results.data) == 1
    assert self._count_query_result in results.data[0]

    # The runtime profile of the retried query.
    retried_runtime_profile = self.client.get_runtime_profile(handle)

    # Assert that the killed impalad shows up in the list of blacklisted executors from
    # the runtime profile.
    self.__assert_executors_blacklisted(killed_impalad, retried_runtime_profile)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # Validate the state of the runtime profiles.
    self.__validate_runtime_profiles(
        retried_runtime_profile, handle.get_handle().id, retried_query_id)

    # Validate the state of the client log.
    self.__validate_client_log(handle, retried_query_id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.client.close_query(handle)
    self.__validate_web_ui_state()
    self.__validate_memz()

  @SkipIfFS.shutdown_idle_fails
  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=" + _get_rpc_fail_action(FAILED_KRPC_PORT),
      statestored_args="--statestore_heartbeat_frequency_ms=1000 \
          --statestore_max_missed_heartbeats=2")
  def test_retry_exec_rpc_failure_before_admin_delay(self):
    """Test retried query triggered by RPC failures by simulating RPC errors at the port
    of the 2nd node in the cluster. Simulate admission delay for query with debug_action
    so that the 2nd node is removed from the blacklist when making schedule for retried
    query. Verify that retried query is executed successfully, while the 2nd node is not
    in the executor blacklist and it is not assigned to any fragment instance."""

    impalad_service = self.cluster.get_first_impalad().service
    rpc_not_accessible_impalad = self.cluster.impalads[1]
    assert rpc_not_accessible_impalad.service.krpc_port == FAILED_KRPC_PORT

    # The 2nd node cannot be accessible through KRPC so that it's added to blacklist
    # and the query should be retried. Add delay before admission so that the 2nd node
    # is removed from the blacklist before scheduler makes schedule for the retried
    # query.
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true',
                       'debug_action': 'AC_BEFORE_ADMISSION:SLEEP@18000'})
    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 80)

    # Validate that the query was retried.
    self.__validate_runtime_profiles_from_service(impalad_service, handle)

    # Assert that the query succeeded and returned the correct results.
    results = self.client.fetch(query, handle)
    assert results.success
    assert len(results.data) == 1
    assert self._count_query_result in results.data[0]

    # The runtime profile of the retried query.
    retried_runtime_profile = self.client.get_runtime_profile(handle)

    # Assert that the 2nd node does not show up in the list of blacklisted executors
    # from the runtime profile.
    self.__assert_executors_not_blacklisted(rpc_not_accessible_impalad,
        retried_runtime_profile)

    # Assert that the 2nd node is not assigned any fragment instance for retried query
    # execution.
    self.__assert_executors_not_assigned_any_finstance(rpc_not_accessible_impalad,
        retried_runtime_profile)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # Validate the state of the runtime profiles.
    self.__validate_runtime_profiles(
        retried_runtime_profile, handle.get_handle().id, retried_query_id)

    # Validate the state of the client log.
    self.__validate_client_log(handle, retried_query_id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.client.close_query(handle)
    self.__validate_web_ui_state()

  @SkipIfFS.shutdown_idle_fails
  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=" + _get_rpc_fail_action(FAILED_KRPC_PORT),
      statestored_args="--statestore_heartbeat_frequency_ms=1000 \
          --statestore_max_missed_heartbeats=2",
      cluster_size=2, num_exclusive_coordinators=1)
  def test_retry_query_failure_all_executors_blacklisted(self):
    """Test retried query triggered by RPC failures by simulating RPC errors at the port
    of the 2nd node, which is the only executor in the cluster. Simulate admission delay
    for query with debug_action so that the 2nd node is removed from the blacklist and
    added back to executor group when making schedule for retried query. Verify that
    retried query fails due to no executor available even the 2nd node is not in the
    executor blacklist."""

    rpc_not_accessible_impalad = self.cluster.impalads[1]
    assert rpc_not_accessible_impalad.service.krpc_port == FAILED_KRPC_PORT

    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true',
                       'debug_action': 'AC_BEFORE_ADMISSION:SLEEP@18000'})
    # Wait until the query fails.
    self.wait_for_state(handle, self.client.QUERY_STATES['EXCEPTION'], 140)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # The runtime profile and client log of the retried query, need to be retrieved
    # before fetching results, since the failed fetch attempt will close the
    # query handle.
    retried_runtime_profile = self.client.get_runtime_profile(handle)
    self.__validate_client_log(handle, retried_query_id)

    # Assert that the query failed since all executors are blacklisted and no executor
    # available for scheduling the query to be retried. To keep consistent with the other
    # blacklisting logic, Impalad return error message "Admission for query exceeded
    # timeout 60000ms in pool default-pool. Queued reason: Waiting for executors to
    # start...".
    try:
      self.client.fetch(self._shuffle_heavy_query, handle)
      assert False
    except ImpalaBeeswaxException as e:
      assert "Admission for query exceeded timeout 60000ms in pool default-pool." \
          in str(e)
      assert "Queued reason: Waiting for executors to start. Only DDL queries and " \
             "queries scheduled only on the coordinator (either NUM_NODES set to 1 " \
             "or when small query optimization is triggered) can currently run" in str(e)
      assert "Additional Details: Not Applicable" in str(e)

    # Assert that the RPC un-reachable impalad not shows up in the list of blacklisted
    # executors from the runtime profile.
    self.__assert_executors_not_blacklisted(rpc_not_accessible_impalad,
        retried_runtime_profile)

    # Assert that the query id of the original query is in the runtime profile of the
    # retried query.
    self.__validate_original_id_in_profile(retried_runtime_profile,
        handle.get_handle().id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.__validate_web_ui_state()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=1000")
  def test_multiple_retries(self):
    """Test that a query can only be retried once, and that if the retry attempt fails,
    it fails correctly and with the right error message. Multiple retry attempts are
    triggered by killing multiple impalads. The final attempt at retrying the query
    should indicate that the error was retryable, and that the max retry limit was
    exceeded."""

    # Launch a query, it should be retried.
    handle = self.execute_query_async(self._shuffle_heavy_query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['RUNNING'], 60)

    # Kill one impalad so that a retry is triggered.
    killed_impalad = self.cluster.impalads[1]
    killed_impalad.kill()

    # Wait until the retry is running.
    self.__wait_until_retry_state(handle, 'RETRIED')

    # Kill another impalad so that another retry is attempted.
    self.cluster.impalads[2].kill()

    # Wait until the query fails.
    self.wait_for_state(handle, self.client.QUERY_STATES['EXCEPTION'], 60)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # The runtime profile and client log of the retried query, need to be retrieved
    # before fetching results, since the failed fetch attempt will close the
    # query handle.
    retried_runtime_profile = self.client.get_runtime_profile(handle)
    self.__validate_client_log(handle, retried_query_id)

    # Assert that the query failed, since a query can only be retried once.
    try:
      self.client.fetch(self._shuffle_heavy_query, handle)
      assert False
    except ImpalaBeeswaxException as e:
      assert "Max retry limit was hit. Query was retried 1 time(s)." in str(e)

    # Assert that the killed impalad shows up in the list of blacklisted executors from
    # the runtime profile.
    self.__assert_executors_blacklisted(killed_impalad, retried_runtime_profile)

    # Assert that the query id of the original query is in the runtime profile of the
    # retried query.
    self.__validate_original_id_in_profile(retried_runtime_profile,
            handle.get_handle().id)

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    self.__validate_web_ui_state()
    self.__validate_memz()

  @pytest.mark.execute_serially
  def test_retry_fetched_rows(self):
    """Test that query retries are not triggered if some rows have already been
    fetched. Run a query, fetch some rows from it, kill one of the impalads that is
    running the query, and the validate that another fetch request fails."""
    query = "select * from functional.alltypes where bool_col = sleep(500)"
    # IMPALA-9856: The retry behavior that is being exercised in this test only applies
    # for query without result spooling enabled. Thus, we explicitly set
    # spool_query_results as false.
    query_options = {'retry_failed_queries': 'true', 'batch_size': '1',
        'spool_query_results': 'false'}
    handle = self.execute_query_async(query, query_options)
    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 60)

    self.client.fetch(query, handle, max_rows=1)

    self.cluster.impalads[1].kill()
    time.sleep(5)

    # Assert that attempt to fetch from the query handle fails.
    try:
      self.client.fetch(query, handle)
      assert False
    except Exception as e:
      assert "Failed due to unreachable impalad" in str(e)
      assert "Skipping retry of query_id=%s because the client has already " \
             "fetched some rows" % handle.get_handle().id in str(e)

  @pytest.mark.execute_serially
  def test_spooling_all_results_for_retries(self):
    """Test retryable queries with spool_all_results_for_retries=true will spool all
    results when results spooling is enabled."""
    handle = self.execute_query_async(self._union_query, query_options={
        'retry_failed_queries': 'true', 'spool_query_results': 'true',
        'spool_all_results_for_retries': 'true'})

    # Fetch one row first.
    results = self.client.fetch(self._union_query, handle, max_rows=1)
    assert len(results.data) == 1
    assert int(results.data[0]) == 8

    # All results are spooled since we are able to fetch some results.
    # Killing an impalad should not trigger query retry.
    self.__kill_random_impalad()
    time.sleep(5)

    # We are still able to fetch the remaining results.
    results = self.client.fetch(self._union_query, handle)
    assert len(results.data) == 1
    assert int(results.data[0]) == 3650

    # Verify no retry happens
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is None
    runtime_profile = self.client.get_runtime_profile(handle)
    assert self.__get_query_id_from_profile(runtime_profile) == handle.get_handle().id

    self.client.close_query(handle)

  @pytest.mark.execute_serially
  def test_query_retry_in_spooling(self):
    """Test retryable queries with results spooling enabled and
    spool_all_results_for_retries=true can be safely retried for failures that happen when
    it's still spooling the results"""
    handle = self.execute_query_async(self._union_query, query_options={
      'retry_failed_queries': 'true', 'spool_query_results': 'true',
      'spool_all_results_for_retries': 'true'})
    # Wait until the first union operand finishes, so some results are spooled.
    self.wait_for_progress(handle, 0.1, 60)

    self.__kill_random_impalad()

    # Still able to fetch the correct result since the query is retried.
    results = self.client.fetch(self._union_query, handle)
    assert len(results.data) == 2
    assert int(results.data[0]) == 8
    assert int(results.data[1]) == 3650

    # Verify the query has been retried
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    self.client.close_query(handle)

  @pytest.mark.execute_serially
  def test_retried_query_not_spooling_all_results(self):
    """Test retried query can return results immediately even when results spooling and
    spool_all_results_for_retries are enabled in the original query."""
    handle = self.execute_query_async(self._union_query, query_options={
      'retry_failed_queries': 'true', 'spool_query_results': 'true',
      'spool_all_results_for_retries': 'true'})
    # Wait until the first union operand finishes and then kill one impalad.
    self.wait_for_progress(handle, 0.1, 60)

    # Kill one impalad so the query will be retried.
    self.__kill_random_impalad()
    time.sleep(5)

    # Verify that we are able to fetch results of the first union operand while the query
    # is still executing the second union operand.
    results = self.client.fetch(self._union_query, handle, max_rows=1)
    assert len(results.data) == 1
    assert int(results.data[0]) == 8

    # Assert that the query is still executing the second union operand.
    summary = self.client.get_exec_summary(handle)
    assert summary.progress.num_completed_scan_ranges < summary.progress.total_scan_ranges

    self.client.close_query(handle)

  @pytest.mark.execute_serially
  def test_query_retry_reaches_spool_limit(self):
    """Test retryable queries with results spooling enabled and
    spool_all_results_for_retries=true that reach spooling mem limit will return rows and
    skip retry"""
    query = "select * from functional.alltypes where bool_col = sleep(500)"
    # Set lower values for spill-to-disk configs to force the above query to spill
    # spooled results and hit result queue limit.
    handle = self.execute_query_async(query, query_options={
        'batch_size': 1,
        'spool_query_results': True,
        'retry_failed_queries': True,
        'spool_all_results_for_retries': True,
        'min_spillable_buffer_size': 8 * 1024,
        'default_spillable_buffer_size': 8 * 1024,
        'max_result_spooling_mem': 8 * 1024,
        'max_row_size': 8 * 1024,
        'max_spilled_result_spooling_mem': 8 * 1024})

    # Wait until we can fetch some results
    results = self.client.fetch(query, handle, max_rows=1)
    assert len(results.data) == 1

    # PLAN_ROOT_SINK's reservation limit should be set at
    # 2 * DEFAULT_SPILLABLE_BUFFER_SIZE = 16 KB.
    plan_root_sink_reservation_limit = "PLAN_ROOT_SINK[\s\S]*?ReservationLimit: 16.00 KB"
    profile = self.client.get_runtime_profile(handle)
    assert re.search(plan_root_sink_reservation_limit, profile)

    # Assert that the query is still executing
    summary = self.client.get_exec_summary(handle)
    assert summary.progress.num_completed_scan_ranges < summary.progress.total_scan_ranges

    self.assert_impalad_log_contains('INFO', 'Cannot spool all results in the allocated'
        ' result spooling space. Query retry will be skipped if any results have been '
        'returned.', expected_count=1)

    # Kill one impalad and assert that the query is not retried.
    self.__kill_random_impalad()

    try:
      self.client.fetch(query, handle)
      assert False, "fetch should fail"
    except ImpalaBeeswaxException as e:
      assert "Failed due to unreachable impalad" in str(e)
      assert "Skipping retry of query_id=%s because the client has already " \
             "fetched some rows" % handle.get_handle().id in str(e)

  @pytest.mark.execute_serially
  def test_original_query_cancel(self):
    """Test canceling a retryable query with spool_all_results_for_retries=true. Make sure
    Coordinator::Wait() won't block in cancellation."""
    for state in ['RUNNING', 'FINISHED']:
      handle = self.execute_query_async(self._union_query, query_options={
        'retry_failed_queries': 'true', 'spool_query_results': 'true',
        'spool_all_results_for_retries': 'true'})
      self.wait_for_state(handle, self.client.QUERY_STATES[state], 60)

      # Cancel the query.
      self.client.cancel(handle)

      # Assert that attempt to fetch from the query handle fails with a cancellation
      # error
      try:
        self.client.fetch(self._union_query, handle)
        assert False
      except Exception as e:
        assert "Cancelled" in str(e)

  @pytest.mark.execute_serially
  def test_retry_finished_query(self):
    """Test that queries in FINISHED state can still be retried before the client fetch
    any rows. Sets batch_size to 1 so results will be available as soon as possible.
    The query state becomes FINISHED when results are available."""
    query = "select * from functional.alltypes where bool_col = sleep(50)"
    # IMPALA-9856: The retry behavior that is being exercised in this test only applies
    # for query without result spooling enabled. Thus, we explicitly set
    # spool_query_results as false.
    query_options = {'retry_failed_queries': 'true', 'batch_size': '1',
        'spool_query_results': 'false'}
    handle = self.execute_query_async(query, query_options)
    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 60)

    self.__kill_random_impalad()
    time.sleep(5)

    self.client.fetch(query, handle)

    # Verifies the query is retried.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    self.client.close_query(handle)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_cancel(self):
    """Trigger a query retry, and then cancel the retried query. Validate that the
    cancelled query fails with the correct error message. Set a really high statestore
    heartbeat frequency so that killed impalads are not removed from the cluster
    membership."""

    impalad_service = self.cluster.get_first_impalad().service

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})
    self.wait_for_state(handle, self.client.QUERY_STATES['FINISHED'], 60)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # Validate that the query was retried.
    profile_retried_query_id = \
        self.__validate_runtime_profiles_from_service(impalad_service, handle)
    assert profile_retried_query_id == retried_query_id
    self.__validate_client_log(handle, retried_query_id)

    # Cancel the query.
    self.client.cancel(handle)

    # Assert than attempt to fetch from the query handle fails with a cancellation
    # error
    try:
      self.client.fetch(query, handle)
      assert False
    except Exception as e:
        assert "Cancelled" in str(e)
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=RETRY_DELAY_CHECKING_ORIGINAL_DRIVER:SLEEP@1000",
      statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retrying_query_cancel(self):
    """Trigger a query retry, and then cancel and close the retried query in RETRYING
    state. Validate that it doesn't crash the impalad. Set a really high statestore
    heartbeat frequency so that killed impalads are not removed from the cluster
    membership."""

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})
    self.__wait_until_retry_state(handle, 'RETRYING')

    # Cancel the query.
    self.client.cancel(handle)

    # Check the original query retry status.
    profile = self.__get_original_query_profile(handle.get_handle().id)
    retry_status = re.search("Retry Status: (.*)", profile)
    assert retry_status.group(1) == 'RETRYING'

    self.client.close_query(handle)
    time.sleep(2)
    assert self.cluster.impalads[0].get_pid() is not None, "Coordinator crashed"
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retrying_query_before_inflight(self):
    """Trigger a query retry, and delay setting the original query inflight as that may
    happen after the query is retried. Validate that the query succeeds. Set a really
    high statestore heartbeat frequency so that killed impalads are not removed from
    the cluster membership."""

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true',
                       'debug_action': 'SET_QUERY_INFLIGHT:SLEEP@1000'})
    self.__wait_until_retry_state(handle, 'RETRIED')

    # SetQueryInflight will complete before execute_query_async returns because it will
    # be completed before Impala acknowledges that the query has started.
    page = self.cluster.get_first_impalad().service.get_debug_webpage_json('sessions')
    for session in page['sessions']:
      # Every session should have one completed query: either test setup, or the original
      # query that's being retried.
      assert session['inflight_queries'] < session['total_queries']

    self.client.close_query(handle)
    # If original query state closure is skipped, the coordinator will crash on a DCHECK.
    time.sleep(2)
    assert self.cluster.impalads[0].get_pid() is not None, "Coordinator crashed"
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=RETRY_DELAY_GET_QUERY_DRIVER:SLEEP@2000",
      statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_close_before_getting_query_driver(self):
    """Trigger a query retry, and then close the retried query before getting
    the query driver. Validate that it doesn't crash the impalad.
    Set a really high statestore heartbeat frequency so that killed impalads are not
    removed from the cluster membership."""

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true'})

    time.sleep(1)
    # close the query
    self.client.close_query(handle)

    time.sleep(2)
    impala_service = self.cluster.get_first_impalad().service
    self.assert_eventually(60, 0.1,
        lambda: impala_service.get_num_in_flight_queries() == 0,
        lambda: "in-flight queries: %d" % impala_service.get_num_in_flight_queries())
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=QUERY_RETRY_SET_RESULT_CACHE:FAIL",
      statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_result_cacheing_failed(self):
    """Test setting up results cacheing failed."""

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    self.hs2_client.set_configuration({'retry_failed_queries': 'true'})
    self.hs2_client.set_configuration_option('impala.resultset.cache.size', '1024')
    self.hs2_client.execute_async(query)
    # The number of in-flight queries is 0 at the beginning, then 1 when the original
    # query is submitted. It's 2 when the retried query is registered. Although the retry
    # will immediately fail due to the debug action and the 2 queries are unregistered,
    # the number can't come back to 0 immediately. The reason is the registered queries
    # are cleaned up by a backend thread. Sleep a while to make sure these finish.
    time.sleep(2)
    # TODO(IMPALA-10705): Verify the retry failure.
    # No queries running at the end.
    impala_service = self.cluster.get_first_impalad().service
    self.assert_eventually(60, 0.1,
        lambda: impala_service.get_num_in_flight_queries() == 0,
        lambda: "in-flight queries: %d" % impala_service.get_num_in_flight_queries())
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--debug_actions=QUERY_RETRY_SET_QUERY_IN_FLIGHT:FAIL",
      statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_set_query_in_flight_failed(self):
    """Test setting query in flight failed."""

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    self.execute_query_async(query, query_options={'retry_failed_queries': 'true'})
    # The number of in-flight queries is 0 at the beginning, then 1 when the original
    # query is submitted. It's 2 when the retried query is registered. Although the retry
    # will immediately fail due to the debug action and the 2 queries are unregistered,
    # the number can't come back to 0 immediately. The reason is the registered queries
    # are cleaned up by a backend thread. Sleep a while to make sure these finish.
    time.sleep(2)
    # TODO(IMPALA-10705): Verify the retry failure.
    # No queries running at the end.
    impala_service = self.cluster.get_first_impalad().service
    self.assert_eventually(60, 0.1,
        lambda: impala_service.get_num_in_flight_queries() == 0,
        lambda: "in-flight queries: %d" % impala_service.get_num_in_flight_queries())
    self.__validate_memz()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_timeout(self):
    """Trigger a query retry, and then leave the query handle open until the
    'query_timeout_s' causes the handle to be closed. Assert that the runtime profile of
    the original and retried queries are correct, and that the 'num-queries-expired'
    metric is properly incremented. Set a really high statestore heartbeat frequency so
    that killed impalads are not removed from the cluster membership."""

    impalad_service = self.cluster.get_first_impalad().service

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    handle = self.execute_query_async(query,
        query_options={'retry_failed_queries': 'true', 'exec_time_limit_s': '1'})
    self.wait_for_state(handle, self.client.QUERY_STATES['EXCEPTION'], 60)

    # Validate the live exec summary.
    retried_query_id = self.__get_retried_query_id_from_summary(handle)
    assert retried_query_id is not None

    # Wait for the query timeout to expire the query handle.
    time.sleep(5)

    # Validate that the query was retried.
    profile_retried_query_id = \
        self.__validate_runtime_profiles_from_service(impalad_service, handle)
    assert profile_retried_query_id == retried_query_id
    self.__validate_client_log(handle, retried_query_id)

    # Assert than attempt to fetch from the query handle fails with a query expired
    # error.
    try:
      self.client.fetch(query, handle)
      assert False
    except Exception as e:
        assert "expired due to execution time limit of 1s000ms" in str(e)

    # Assert that the impalad metrics show one expired query.
    assert impalad_service.get_metric_value('impala-server.num-queries-expired') == 1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--idle_session_timeout=1",
        statestored_args="--statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_session_timeout(self):
    """Similar to 'test_retry_query_timeout' except with an idle session timeout."""
    self.close_impala_clients()
    impalad_service = self.cluster.get_first_impalad().service

    # Kill an impalad, and run a query. The query should be retried.
    self.cluster.impalads[1].kill()
    query = self._count_query
    client = self.cluster.get_first_impalad().service.create_beeswax_client()
    client.set_configuration({'retry_failed_queries': 'true'})
    handle = client.execute_async(query)
    self.wait_for_state(handle, client.QUERY_STATES['FINISHED'], 60, client=client)

    # Wait for the idle session timeout to expire the session.
    time.sleep(5)

    # Validate that the query was retried. Skip validating client log since we can't
    # get it using the expired session.
    self.__validate_runtime_profiles_from_service(impalad_service, handle)

    # Assert than attempt to fetch from the query handle fails with a session expired
    # error.
    try:
      client.fetch(query, handle)
    except Exception as e:
      assert "Client session expired" in str(e)

    # Assert that the impalad metrics show one expired session.
    assert impalad_service.get_metric_value('impala-server.num-sessions-expired') == 1

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="-statestore_heartbeat_frequency_ms=60000")
  def test_retry_query_hs2(self):
    """Test query retries with the HS2 protocol. Enable the results set cache as well and
    test that query retries work with the results cache."""
    self.cluster.impalads[1].kill()
    query = self._count_query
    self.hs2_client.set_configuration({'retry_failed_queries': 'true'})
    self.hs2_client.set_configuration_option('impala.resultset.cache.size', '1024')
    handle = self.hs2_client.execute_async(query)
    self.wait_for_state(handle, 'FINISHED_STATE', 60, client=self.hs2_client)

    results = self.hs2_client.fetch(query, handle)
    assert results.success
    assert len(results.data) == 1
    assert results.data[0] == self._count_query_result

    # Validate the live exec summary.
    retried_query_id = \
        self.__get_retried_query_id_from_summary(handle, use_hs2_client=True)
    assert retried_query_id is not None

    # Validate the state of the runtime profiles.
    retried_runtime_profile = self.hs2_client.get_runtime_profile(handle,
        TRuntimeProfileFormat.STRING)
    self.__validate_runtime_profiles(
        retried_runtime_profile, self.hs2_client.get_query_id(handle), retried_query_id)
    self.__validate_client_log(handle, retried_query_id, use_hs2_client=True)
    self.impalad_test_service.wait_for_metric_value(
        'impala-server.resultset-cache.total-num-rows', 1, timeout=60)
    self.hs2_client.close_query(handle)

  def __validate_runtime_profiles_from_service(self, impalad_service, handle):
    """Wrapper around '__validate_runtime_profiles' that first fetches the retried profile
    from the web ui."""
    original_profile = impalad_service.read_query_profile_page(handle.get_handle().id)
    retried_query_id = self.__get_retried_query_id_from_profile(original_profile)
    retried_profile = impalad_service.read_query_profile_page(retried_query_id)
    self.__validate_runtime_profiles(
        retried_profile, handle.get_handle().id, retried_query_id)
    return retried_query_id

  def __get_retried_query_id_from_profile(self, profile):
    """Returns the entry for 'Retried Query Id' from the given profile, or 'None' if no
    such entry exists."""
    retried_query_id_search = re.search("Retried Query Id: (.*)", profile)
    if not retried_query_id_search: return None
    return retried_query_id_search.group(1)

  def __wait_until_retry_state(self, handle, retry_state, timeout=300):
    """Wait until the given query handle has been retried. This is achieved by polling the
    runtime profile of the query and checking the 'Retry Status' field."""

    def __get_retry_status():
      profile = self.__get_original_query_profile(handle.get_handle().id)
      retry_status = re.search("Retry Status: (.*)", profile)
      return retry_status.group(1) if retry_status else None

    start_time = time.time()
    retry_status = __get_retry_status()
    while retry_status != retry_state and time.time() - start_time < timeout:
      LOG.info("Wait 100ms for retry state {0}. Current retry state: {1}"
               .format(retry_state, retry_status))
      time.sleep(0.1)
      retry_status = __get_retry_status()
    if retry_status != retry_state:
      raise Timeout("query {0} was not retried within timeout".format
          (handle.get_handle().id))

  def __kill_random_impalad(self):
    """Kills a random impalad, except for the first node in the cluster, which should be
    the Coordinator. Returns the killed impalad."""
    killed_impalad = \
        self.cluster.impalads[randint(1, ImpalaTestSuite.get_impalad_cluster_size() - 1)]
    killed_impalad.kill()
    return killed_impalad

  def __get_query_id_from_profile(self, profile):
    """Extracts and returns the query id of the given profile."""
    query_id_search = re.search("Query \(id=(.*)\)", profile)
    assert query_id_search, "Invalid query profile, has no query id:\n{0}".format(
        profile)
    return query_id_search.group(1)

  def __get_original_query_profile(self, original_query_id):
    """Returns the query profile of the original query attempt."""
    # TODO (IMPALA-9229): there is no way to get the runtime profiles of the unsuccessful
    # query attempts from the ImpalaServer, so fetch them from the debug UI instead.
    return self.cluster.get_first_impalad().service.read_query_profile_page(
        original_query_id)

  def __validate_original_id_in_profile(self, retried_runtime_profile, original_query_id):
    """Validate that the orginal query id is in the 'Original Query Id' entry of the
    given retried runtime profile."""
    original_id_pattern = "Original Query Id: (.*)"
    original_id_search = re.search(original_id_pattern, retried_runtime_profile)
    assert original_id_search, \
        "Could not find original id pattern '{0}' in profile:\n{1}".format(
            original_id_pattern, retried_runtime_profile)
    assert original_id_search.group(1) == original_query_id

  def __validate_runtime_profiles(self, retried_runtime_profile, original_query_id,
                                  retried_query_id):
    """"Validate the runtime profiles of both the original and retried queries. The
    'retried_runtime_profile' refers to the runtime profile of the retried query (the
    most recent attempt of the query, which should have succeeded). The
    'original_runtime_profile' refers to the runtime profile of the original query (the
    original attempt of the query submitted by the user, which failed and had to be
    retried)."""

    # Check the retried query id in the retried runtime profile.
    assert retried_query_id == self.__get_query_id_from_profile(retried_runtime_profile)

    # Assert that the query id of the original query is in the runtime profile of the
    # retried query.
    self.__validate_original_id_in_profile(retried_runtime_profile,
            original_query_id)

    # Get the original runtime profile from the retried runtime profile.
    original_runtime_profile = self.__get_original_query_profile(original_query_id)

    # Validate the contents of the original runtime profile.
    self.__validate_original_runtime_profile(original_runtime_profile, retried_query_id)

    # Assert that the query options from the original and retried queries are the same.
    assert self.__get_query_options(original_runtime_profile) == \
        self.__get_query_options(retried_runtime_profile)

  def __get_query_options(self, profile):
    """Returns the query options from the given profile."""
    query_options_pattern = "Query Options \(set by configuration and planner\): (.*)"
    query_options = re.search(query_options_pattern, profile)
    assert query_options, profile
    return query_options.group(1)

  def __validate_original_runtime_profile(self, original_runtime_profile,
          retried_query_id):
    """Validate the contents of the runtime profile of the original query after the query
    has been retried."""
    # The runtime profile of the original query should reflect that the query failed due
    # a retryable error, and that it was retried.
    assert "Query State: EXCEPTION" in original_runtime_profile, original_runtime_profile
    assert "Impala Query State: ERROR" in original_runtime_profile, \
        original_runtime_profile
    assert "Query Status: " in original_runtime_profile, \
        original_runtime_profile
    assert "Retry Status: RETRIED" in original_runtime_profile, original_runtime_profile
    assert "Retry Cause: " in original_runtime_profile, \
        original_runtime_profile

    # Assert that the query id of the retried query is in the runtime profile of the
    # original query.
    assert "Retried Query Id: {0}".format(retried_query_id) \
        in original_runtime_profile, original_runtime_profile

    # Assert that the original query ran on all three nodes. All queries scan tables
    # large enough such that scan fragments are scheduled on all impalads.
    assert re.search("PLAN FRAGMENT.*instances=3", original_runtime_profile), \
        original_runtime_profile

  def __validate_web_ui_state(self):
    """Validate the state of the web ui after a query (or queries) have been retried.
    The web ui should list 0 queries as in flight, running, or queued."""

    impalad_service = self.cluster.get_first_impalad().service

    # Assert that the debug web ui shows all queries as completed
    self.assert_eventually(60, 0.1,
        lambda: impalad_service.get_num_in_flight_queries() == 0)
    assert impalad_service.get_num_running_queries('default-pool') == 0
    assert impalad_service.get_num_queued_queries('default-pool') == 0

  def __assert_executors_blacklisted(self, blacklisted_impalad, profile):
    """Validate that the given profile indicates that the given impalad was blacklisted
    during query execution."""
    assert "Blacklisted Executors: {0}:{1}".format(blacklisted_impalad.hostname,
        blacklisted_impalad.service.krpc_port) in profile, profile

  def __assert_executors_not_blacklisted(self, impalad, profile):
    """Validate that the given profile indicates that the given impalad was not
    blacklisted during retried query execution"""
    assert not ("Blacklisted Executors: {0}:{1}".format(impalad.hostname,
        impalad.service.krpc_port) in profile), profile

  def __assert_executors_not_assigned_any_finstance(self, impalad, profile):
    """Validate that the given profile indicates that the given impalad was not
    assigned any fragment instance for query execution"""
    assert not ("host={0}:{1}".format(impalad.hostname,
        impalad.service.krpc_port) in profile), profile

  def __validate_client_log(self, handle, retried_query_id, use_hs2_client=False):
    """Validate the GetLog result contains query retry information"""
    if use_hs2_client:
      client_log = self.hs2_client.get_log(handle)
    else:
      client_log = self.client.get_log(handle)
    assert "Original query failed:" in client_log
    query_id_search = re.search("Query has been retried using query id: (.*)\n",
                                client_log)
    assert query_id_search,\
      "Invalid client log, has no retried query id. Log=%s" % client_log
    assert query_id_search.group(1) == retried_query_id

  def __get_retried_query_id_from_summary(self, handle, use_hs2_client=False):
    if use_hs2_client:
      summary = self.hs2_client.get_exec_summary(handle)
    else:
      summary = self.client.get_exec_summary(handle)
    if summary.error_logs:
      for log in summary.error_logs:
        query_id_search = re.search("Retrying query using query id: (.*)", log)
        if query_id_search:
          return query_id_search.group(1)
    return None

  def __exist_queries_in_web_ui_memz(self):
    memz_breakdown = self.cluster.get_first_impalad() \
      .service.get_debug_webpage_json('memz')['detailed']
    query = re.compile("Query\([0-9a-f]{16}:[0-9a-f]{16}")
    return query.search(memz_breakdown)

  def __validate_memz(self):
    # Validate that all queries are released
    self.assert_eventually(60, 0.1,
        lambda: self.__exist_queries_in_web_ui_memz() is None)


# Tests that verify the query-retries are properly triggered by disk IO failure.
# Coordinator adds an executor node to its blacklist if that node reports query
# execution status with error caused by its local faulty disk, then retries the failed
# query.
@SkipIfNotHdfsMinicluster.tuned_for_minicluster
class TestQueryRetriesFaultyDisk(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestQueryRetriesFaultyDisk, cls).setup_class()

  # Query with order by requires spill to disk if intermediate results don't fit in mem
  spill_query = """
      select o_orderdate, o_custkey, o_comment
      from tpch.orders
      order by o_orderdate
      """
  # Buffer pool limit that is low enough to force Impala to spill to disk when executing
  # spill_query.
  buffer_pool_limit = "45m"

  def setup_method(self, method):
    # Don't call the superclass method to prevent starting Impala before each test. In
    # this class, each test is responsible for doing that because we want to generate
    # the parameter string to start-impala-cluster in each test method.
    self.created_dirs = []

  def teardown_method(self, method):
    for dir_path in self.created_dirs:
      shutil.rmtree(dir_path, ignore_errors=True)

  def __generate_scratch_dir(self, num):
    result = []
    for i in range(num):
      dir_path = tempfile.mkdtemp()
      self.created_dirs.append(dir_path)
      result.append(dir_path)
      print("Generated dir" + dir_path)
    return result

  def __validate_web_ui_state(self):
    """Validate the state of the web ui after a query (or queries) have been retried.
    The web ui should list 0 queries as in flight, running, or queued."""
    impalad_service = self.cluster.get_first_impalad().service

    # Assert that the debug web ui shows all queries as completed
    self.assert_eventually(60, 0.1,
        lambda: impalad_service.get_num_in_flight_queries() == 0)
    assert impalad_service.get_num_running_queries('default-pool') == 0
    assert impalad_service.get_num_queued_queries('default-pool') == 0

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  def test_retry_spill_to_disk_failed(self, vector):
    """ Test that verifies that when an impalad failed during spill-to-disk due to disk
        write error, it is properly blacklisted by coordinator and query-retry is
        triggered."""

    # Start cluster with spill-to-disk enabled and one dedicated coordinator. Set a
    # really high statestore heartbeat frequency so that blacklisted nodes are not
    # timeout too quickly.
    scratch_dirs = self.__generate_scratch_dir(1)
    self._start_impala_cluster([
        '--impalad_args=-logbuflevel=-1',
        '--impalad_args=--scratch_dirs={0}'.format(','.join(scratch_dirs)),
        '--impalad_args=--allow_multiple_scratch_dirs_per_device=false',
        '--impalad_args=--statestore_heartbeat_frequency_ms=60000',
        '--cluster_size=3', '--num_coordinators=1', '--use_exclusive_coordinators'])
    self.assert_impalad_log_contains("INFO", "Using scratch directory ",
        expected_count=1)

    # Set debug_action to inject disk write error for spill-to-disk on impalad for which
    # krpc port is 27001.
    vector.get_value('exec_option')['buffer_pool_limit'] = self.buffer_pool_limit
    vector.get_value('exec_option')['debug_action'] = \
        _get_disk_fail_action(FAILED_KRPC_PORT)
    vector.get_value('exec_option')['retry_failed_queries'] = "true"
    coord_impalad = self.cluster.get_first_impalad()
    client = coord_impalad.service.create_beeswax_client()

    disk_failure_impalad = self.cluster.impalads[1]
    assert disk_failure_impalad.service.krpc_port == FAILED_KRPC_PORT

    # Verify all nodes are active now.
    backends_json = self.cluster.impalads[0].service.get_debug_webpage_json("/backends")
    assert backends_json["num_active_backends"] == 3, backends_json
    assert len(backends_json["backends"]) == 3, backends_json

    # Expect the impalad with disk failure is blacklisted, and query-retry is triggered
    # and is completed successfully.
    handle = self.execute_query_async_using_client(client, self.spill_query, vector)
    results = client.fetch(self.spill_query, handle)
    assert results.success

    # Validate the state of the web ui. The query must be closed before validating the
    # state since it asserts that no queries are in flight.
    client.close_query(handle)
    client.close()
    self.__validate_web_ui_state()

    # Verify that the impalad with injected disk IO error is blacklisted.
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

    # Verify that the query is re-tried and finished.
    completed_queries = coord_impalad.service.get_completed_queries()
    # Assert that the most recently completed query is the retried query and it is marked
    # as 'FINISHED.
    assert completed_queries[0]['state'] == 'FINISHED'
    assert completed_queries[0]["rows_fetched"] == 1500000
    # Assert that the second most recently completed query is the original query and it
    # is marked as 'RETRIED'.
    assert completed_queries[1]['state'] == 'RETRIED'
    assert completed_queries[1]["rows_fetched"] == 0
    assert completed_queries[1]['query_id'] == handle.get_handle().id
