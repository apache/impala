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
from __future__ import absolute_import, division, print_function
from builtins import range
from multiprocessing.pool import ThreadPool

import pytest
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS, SkipIfExploration


@SkipIfFS.variable_listing_times
@SkipIfExploration.is_not_exhaustive()
class TestTopicUpdateFrequency(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_legacy_catalog(self):
    """Test legacy catalog mode with skipping DISABLED (timeout=0).
    This demonstrates the ORIGINAL PROBLEM that IMPALA-6671 fixed:
    fast queries SHOULD block because the GatherThread blocks on the locked table,
    preventing ALL topic updates and starving coordinators waiting for metadata."""
    self.__run_topic_update_test(expect_topic_updates_to_block=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=500"
    " --catalog_max_lock_skipped_topic_updates=1000")
  def test_topic_updates_unblock_legacy_catalog(self):
    """Test IMPALA-6671 in legacy catalog mode with skipping enabled.
    This is the ORIGINAL use case: fast queries should NOT block because
    the GatherThread skips locked tables and continues serializing others."""
    self.__run_topic_update_test(expect_topic_updates_to_block=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_local_catalog(self):
    """Test local catalog mode with skipping DISABLED (timeout=0).
    Fast queries should still NOT block, proving that the skipping mechanism is
    unnecessary (except for DDL queries with SYNC_DDL) in local catalog mode since
    coordinators fetch metadata via RPC and don't depend on topic updates."""
    self.__run_topic_update_test(expect_topic_updates_to_block=False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=500"
    " --catalog_max_lock_skipped_topic_updates=1000")
  def test_topic_updates_unblock_local_catalog(self):
    """Test IMPALA-6671 in local catalog mode with skipping enabled.
    Fast queries should NOT block because they fetch metadata via RPC,
    independent of topic updates."""
    self.__run_topic_update_test(expect_topic_updates_to_block=False)

  def __run_topic_update_test(self, expect_topic_updates_to_block):
    """Shared test logic for all test_topic_updates_unblock_* variants.
    Tests query blocking conditions as per IMPALA-6671 and verifies that
    unrelated queries are blocked or not blocked depending on catalog mode
    and whether the lock timeout mechanism is enabled."""
    # queries that we don't expect to block when a slow running blocking query is
    # running in parallel. We want these queries to request the metadata from catalogd
    # and hence the init queries invalidate the metadata before each test case run below.
    non_blocking_queries = [
      # each of these take milliseconds when there is no lock contention.
      "describe functional.emptytable",
      "select * from functional.tinytable limit 1",
      "show partitions functional.alltypessmall",
    ]
    # queries used to reset the metadata of the non_blocking_queries so that they will
    # reload the next time they are executed
    init_queries = [
      "invalidate metadata functional.emptytable",
      "invalidate metadata functional.tinytable",
      "invalidate metadata functional.alltypessmall",
    ]
    # make sure that the blocking query metadata is loaded in catalogd since table lock
    # is only acquired on loaded tables.
    self.client.execute("refresh tpcds.store_sales")
    self.client.execute("refresh functional.alltypes")
    # add the debug actions so that blocking queries take long time to complete while
    # holding the table lock. These debug actions are tuned such that each of the blocking
    # queries below take little more than 10 seconds (much slower than fast queries).
    debug_action = (
      "catalogd_refresh_hdfs_listing_delay:SLEEP@6|"
      "catalogd_table_recover_delay:SLEEP@2000|"
      "catalogd_update_stats_delay:SLEEP@2000")
    blocking_query_options = {
      "debug_action": debug_action
    }
    blocking_queries = [
      "refresh tpcds.store_sales",
      "alter table tpcds.store_sales recover partitions",
      "compute stats functional.alltypes",
    ]

    for blocking_query in blocking_queries:
      print("Running blocking query: {0}".format(blocking_query))
      for blocking_sync_ddl, non_blocking_sync_ddl in (
          ("false", "false"),
          ("true", "false"),
          ("true", "true"),
          ("false", "true"),
      ):
        blocking_query_options["sync_ddl"] = blocking_sync_ddl
        non_blocking_query_options = {"sync_ddl": non_blocking_sync_ddl}
        self.__run_topic_update_test_inner(blocking_query,
          non_blocking_queries, init_queries,
          blocking_query_options=blocking_query_options,
          non_blocking_query_options=non_blocking_query_options,
          expect_topic_updates_to_block=expect_topic_updates_to_block)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_legacy_catalog_sync_ddl_create_table(self,
      unique_database):
    """Legacy catalog, skipping disabled, sync_ddl CREATE TABLE should block."""
    self.__run_topic_update_test_sync_ddl_create_table(unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_local_catalog_sync_ddl_create_table(self,
      unique_database):
    """Local catalog, skipping disabled, sync_ddl CREATE TABLE should block."""
    self.__run_topic_update_test_sync_ddl_create_table(unique_database)

  def __run_topic_update_test_sync_ddl_create_table(self, unique_database,
      iceberg=False):
    """Independent shared logic for sync_ddl-only CREATE TABLE variants."""
    table_name = "non_blocking_ddl_iceberg_test" if iceberg else "non_blocking_ddl_test"
    stored_as = " stored as iceberg" if iceberg else ""
    non_blocking_queries = [
      "create table {0}.{1} (id int){2}".format(unique_database, table_name, stored_as),
    ]
    init_queries = [
      "drop table if exists {0}.{1}".format(unique_database, table_name),
    ]

    # make sure that the blocking query metadata is loaded in catalogd since table lock
    # is only acquired on loaded tables.
    self.client.execute("refresh tpcds.store_sales")
    self.client.execute("refresh functional.alltypes")
    debug_action = (
      "catalogd_refresh_hdfs_listing_delay:SLEEP@6|"
      "catalogd_table_recover_delay:SLEEP@2000|"
      "catalogd_update_stats_delay:SLEEP@2000")
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "true"
    }
    blocking_queries = [
      "refresh tpcds.store_sales",
      "alter table tpcds.store_sales recover partitions",
      "compute stats functional.alltypes",
    ]

    for blocking_query in blocking_queries:
      print("Running blocking query: {0}".format(blocking_query))
      self.__run_topic_update_test_inner(blocking_query,
        non_blocking_queries, init_queries,
        blocking_query_options=blocking_query_options,
        non_blocking_query_options={"sync_ddl": "true"},
        expect_topic_updates_to_block=True)

  def __run_topic_update_test_inner(self, slow_blocking_query, fast_queries,
      init_queries, blocking_query_options,
      non_blocking_query_options=None, blocking_query_min_time=2000,
      fast_query_timeout_ms=1000, non_blocking_impalad=0,
      expect_topic_updates_to_block=False):
    """This function runs the slow query in a Impala client and then creates separate
    Impala clients to run the fast_queries. It makes sure that the
    fast_queries don't get blocked by the slow query by making sure that
    fast_queries return back before the blocking query within a given expected
    timeout."""
    assert fast_query_timeout_ms < blocking_query_min_time
    # run the init queries first in sync_ddl mode so that all the impalads are starting
    # with a clean state.
    for q in init_queries:
      self.execute_query(q)

    pool = ThreadPool(processes=len(fast_queries))
    slow_query_pool = ThreadPool(processes=1)
    # run the slow query on the impalad-0 with the given query options
    slow_query_future = slow_query_pool.apply_async(self.exec_and_time,
      args=(slow_blocking_query, blocking_query_options, 0))
    # there is no other good way other than to wait for some time to make sure that
    # the slow query has been submitted and being compiled before we start the
    # non_blocking queries
    time.sleep(1)
    fast_query_futures = {}
    for fast_query in fast_queries:
      # run other queries on second impalad
      fast_query_futures[fast_query] = pool.apply_async(self.exec_and_time,
        args=(fast_query, non_blocking_query_options, non_blocking_impalad))

    for fast_query in fast_query_futures:
      if not expect_topic_updates_to_block:
        assert fast_query_futures[fast_query].get() < fast_query_timeout_ms, \
          "{0} did not complete within {1} msec".format(fast_query, fast_query_timeout_ms)
      else:
        # topic updates are expected to block and hence all the other queries should run
        # only after blocking query finishes.
        assert fast_query_futures[fast_query].get() > fast_query_timeout_ms, \
          "{0} completed within {1} msec".format(fast_query, fast_query_timeout_ms)
    # make sure that the slow query exceeds the given timeout; otherwise the test
    # doesn't make much sense.
    assert slow_query_future.get() > blocking_query_min_time, \
      "{0} query took less time than {1} msec".format(slow_blocking_query,
        blocking_query_min_time)
    # Shutdown the thread pools
    pool.terminate()
    slow_query_pool.terminate()
    # we wait for some time here to make sure that the topic updates from the last
    # query have been propagated so that next run of this method starts from a clean
    # state.
    time.sleep(2)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_tbl_max_wait_time_ms=500 "
                  "--catalog_max_lock_skipped_topic_updates=2",
    force_restart=True)
  def test_topic_updates_advance(self):
    """Test makes sure that if long running blocking queries are run continuously
    topic-update thread is not starved and it eventually blocks until it acquires a table
    lock."""
    # Each of these queries take complete about 2s with the debug action delays below.
    blocking_queries = [
      "refresh tpcds.store_sales",
      "refresh tpcds.store_sales",
      "alter table tpcds.store_sales recover partitions",
      "alter table tpcds.store_sales recover partitions",
      "compute stats functional.alltypes"
    ]
    debug_action = (
      "catalogd_refresh_hdfs_listing_delay:SLEEP@6|"
      "catalogd_table_recover_delay:SLEEP@2000|"
      "catalogd_update_stats_delay:SLEEP@2000")
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "true"
    }
    self.__run_loop_test(blocking_queries, blocking_query_options)

  def __run_loop_test(self, blocking_queries, blocking_query_options):
    """Runs the given list of queries with given query options in a loop
    and validates that topic update blocking is observed in catalogd logs."""
    slow_query_pool = ThreadPool(processes=len(blocking_queries))
    # run the slow query on the impalad-0 with the given query options
    # use index as key to support duplicate queries
    slow_query_futures = {}
    for idx, q in enumerate(blocking_queries):
      print("Running blocking query {0}".format(q))
      slow_query_futures[idx] = (q, slow_query_pool.apply_async(self.loop_exec,
        args=(q, blocking_query_options)))

    for idx, (q, future) in slow_query_futures.items():
      # Wait for all query loops to complete.
      future.get()

    # At least one lock-contention blocking entry should be present in catalogd logs.
    # TODO: find better way to test catalog_max_lock_skipped_topic_updates
    self.assert_catalogd_log_contains(
      "INFO",
      r"Topic update thread blocking until lock is acquired for table",
      expected_count=-1,
      timeout_s=10)
    slow_query_pool.terminate()

  def loop_exec(self, query, query_options, iterations=3, impalad=0):
    for iter in range(iterations):
      self.exec_and_time(query, query_options, impalad)


  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_tbl_max_wait_time_ms=120000 "
                  "--max_wait_time_for_sync_ddl_s=5")
  def test_topic_sync_ddl_error(self, unique_database):
    """Test makes sure that if a sync ddl query on a unrelated table is executed with
     timeout, it errors out if table is not published in topic updates within the timeout
     value."""
    sync_ddl_query = "create table {0}.{1} (c int)".format(unique_database, "test1")
    sync_ddl_query_2 = "create table {0}.{1} (c int)".format(unique_database, "test2")
    blocking_query = "refresh tpcds.store_sales"
    debug_action = "catalogd_refresh_hdfs_listing_delay:SLEEP@30"
    self.client.execute(blocking_query)
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "false"
    }
    slow_query_pool = ThreadPool(processes=1)
    # run the slow query on the impalad-0 with the given query options
    slow_query_future = slow_query_pool.apply_async(self.exec_and_time,
      args=(blocking_query, blocking_query_options, 0))
    # wait until the slow query is executing and blocking the topic update thread
    # to avoid any race conditions in the test
    time.sleep(1)
    # now run the sync ddl query; we should expect this sync ddl query to fail
    # since the timeout value is too low and topic update thread doesn't get unblocked
    # before the timeout.
    self.execute_query_expect_failure(self.client, sync_ddl_query, {"sync_ddl": "true"})
    # wait for the slow query to complete
    slow_query_future.get()
    # if query is not sync ddl it should not error out
    slow_query_future = slow_query_pool.apply_async(self.exec_and_time,
      args=(blocking_query, blocking_query_options, 0))
    # wait until the slow query is executing and blocking the topic update thread
    # to avoid any race conditions in the test
    time.sleep(1)
    self.execute_query_expect_success(self.client, sync_ddl_query_2,
      {"sync_ddl": "false"})
    slow_query_future.get()
    slow_query_pool.terminate()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_legacy_catalog_iceberg(self, unique_database):
    """Test legacy catalog mode with skipping DISABLED (timeout=0) using Iceberg table.
    This demonstrates the ORIGINAL PROBLEM that IMPALA-14801 fixed:
    fast queries SHOULD block because the GatherThread blocks on the locked Iceberg table,
    preventing ALL topic updates and starving coordinators waiting for metadata."""
    self.__run_topic_update_test_iceberg(
      expect_topic_updates_to_block=True, unique_database=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=500"
    " --catalog_max_lock_skipped_topic_updates=1000")
  def test_topic_updates_unblock_legacy_catalog_iceberg(self, unique_database):
    """Test IMPALA-14801 in legacy catalog mode with skipping enabled using Iceberg table.
    This is the ORIGINAL use case: fast queries should NOT block because
    the GatherThread skips locked tables and continues serializing others."""
    self.__run_topic_update_test_iceberg(
      expect_topic_updates_to_block=False, unique_database=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_local_catalog_iceberg(self, unique_database):
    """Test local catalog mode with skipping DISABLED (timeout=0) using Iceberg table.
    Fast queries should still NOT block, proving that the skipping mechanism is
    unnecessary (except for DDL queries with SYNC_DDL) in local catalog mode since
    coordinators fetch metadata via RPC and don't depend on topic updates."""
    self.__run_topic_update_test_iceberg(
      expect_topic_updates_to_block=False, unique_database=unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=500"
    " --catalog_max_lock_skipped_topic_updates=1000")
  def test_topic_updates_unblock_local_catalog_iceberg(self, unique_database):
    """Test IMPALA-6671 in local catalog mode with skipping enabled using Iceberg table.
    Fast queries should NOT block because they fetch metadata via RPC,
    independent of topic updates."""
    self.__run_topic_update_test_iceberg(
      expect_topic_updates_to_block=False, unique_database=unique_database)

  def __run_topic_update_test_iceberg(self, expect_topic_updates_to_block,
      unique_database):
    """Shared test logic for Iceberg variants - identical to regular tests but uses
    an Iceberg table for the blocking operation."""
    # Same fast queries as regular tests
    non_blocking_queries = [
      "describe functional.emptytable",
      "select * from functional.tinytable limit 1",
      "show partitions functional.alltypessmall",
    ]
    init_queries = [
      "invalidate metadata functional.emptytable",
      "invalidate metadata functional.tinytable",
      "invalidate metadata functional.alltypessmall",
    ]
    # Use a per-test Iceberg table to avoid mutating shared test tables.
    blocking_table = "{0}.topic_update_freq_iceberg".format(unique_database)
    self.client.execute("create table {0} (id int) stored as iceberg".format(
      blocking_table))
    self.client.execute("insert into {0} values (1)".format(blocking_table))
    # Ensure the table metadata is loaded in catalogd since table lock
    # is only acquired on loaded tables.
    self.client.execute("refresh {0}".format(blocking_table))
    # Debug action delays compute stats operations on the Iceberg table
    # catalogd_update_stats_delay works for both HdfsTable and IcebergTable
    # since IcebergTable delegates to its internal HdfsTable
    debug_action = "catalogd_update_stats_delay:SLEEP@2000"
    blocking_query_options = {
      "debug_action": debug_action,
    }
    blocking_queries = [
      "compute stats {0}".format(blocking_table),
    ]

    for blocking_query in blocking_queries:
      print("Running blocking Iceberg query: {0}".format(blocking_query))
      for blocking_sync_ddl, non_blocking_sync_ddl in (
          ("false", "false"),
          ("true", "false"),
          ("true", "true"),
          ("false", "true"),
      ):
        blocking_query_options["sync_ddl"] = blocking_sync_ddl
        non_blocking_query_options = {"sync_ddl": non_blocking_sync_ddl}
        self.__run_topic_update_test_inner(blocking_query,
          non_blocking_queries, init_queries,
          blocking_query_options=blocking_query_options,
          non_blocking_query_options=non_blocking_query_options,
          expect_topic_updates_to_block=expect_topic_updates_to_block)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=false",
    catalogd_args="--catalog_topic_mode=full --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_legacy_catalog_sync_ddl_create_table_iceberg(self,
      unique_database):
    """Legacy catalog, skipping disabled, sync_ddl CREATE Iceberg TABLE should block."""
    self.__run_topic_update_test_sync_ddl_create_table(unique_database, iceberg=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --topic_update_tbl_max_wait_time_ms=0")
  def test_topic_updates_block_local_catalog_sync_ddl_create_table_iceberg(self,
      unique_database):
    """Local catalog, skipping disabled, sync_ddl CREATE Iceberg TABLE should block."""
    self.__run_topic_update_test_sync_ddl_create_table(unique_database, iceberg=True)
