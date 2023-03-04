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
from tests.common.skip import SkipIfFS


@SkipIfFS.variable_listing_times
class TestTopicUpdateFrequency(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_tbl_max_wait_time_ms=500")
  def test_topic_updates_unblock(self):
    """Test to simulate query blocking conditions as per IMPALA-6671
    and makes sure that unrelated queries are not blocked by other long running
    queries which block topic updates."""
    # queries that we don't expect to block when a slow running blocking query is
    # running in parallel. We want these queries to request the metadata from catalogd
    # and hence the init queries invalidate the metadata before each test case run below.
    non_blocking_queries = [
      # each of these take about 2-4 seconds when there is no lock contention.
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
    # add the debug actions so that blocking queries take long time complete while
    # holding the table lock. These debug actions are tuned such that each of the blocking
    # queries below take little more than 10 seconds (2x slower than fast queries).
    debug_action = "catalogd_refresh_hdfs_listing_delay:SLEEP@30|catalogd_table_recover_delay:SLEEP@10000|catalogd_update_stats_delay:SLEEP@10000"
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "false"
    }
    blocking_queries = [
      "refresh tpcds.store_sales",
      "alter table tpcds.store_sales recover partitions",
      "compute stats functional.alltypes"
    ]

    for blocking_query in blocking_queries:
      print("Running blocking query: {0}".format(blocking_query))
      # blocking query is without sync_ddl
      blocking_query_options["sync_ddl"] = "false"
      self.__run_topic_update_test(blocking_query,
        non_blocking_queries, init_queries, blocking_query_options=blocking_query_options)
      # blocking query is with sync_ddl
      blocking_query_options["sync_ddl"] = "true"
      self.__run_topic_update_test(blocking_query,
        non_blocking_queries, init_queries, blocking_query_options=blocking_query_options)
      non_blocking_query_options = {
        "sync_ddl": "true",
      }
      self.__run_topic_update_test(blocking_query,
        non_blocking_queries, init_queries, blocking_query_options=blocking_query_options,
        non_blocking_query_options=non_blocking_query_options)
      blocking_query_options["sync_ddl"] = "false"
      self.__run_topic_update_test(blocking_query,
        non_blocking_queries, init_queries, blocking_query_options=blocking_query_options,
        non_blocking_query_options=non_blocking_query_options)

  def __run_topic_update_test(self, slow_blocking_query, fast_queries,
      init_queries, blocking_query_options,
      non_blocking_query_options=None, blocking_query_min_time=10000,
      fast_query_timeout_ms=6000, non_blocking_impalad=0,
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
    slow_query_pool = ThreadPool(processes=len(slow_blocking_query))
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
        assert fast_query_futures[
                 fast_query].get() < fast_query_timeout_ms, \
          "{0} did not complete within {1} msec".format(fast_query, fast_query_timeout_ms)
      else:
        # topic updates are expected to block and hence all the other queries should run
        # only after blocking query finishes.
        fast_query_futures[
          fast_query].get() > blocking_query_min_time, \
          "{0} did not complete within {1} msec".format(fast_query, fast_query_timeout_ms)
    # make sure that the slow query exceeds the given timeout; otherwise the test
    # doesn't make much sense.
    assert slow_query_future.get() > blocking_query_min_time, \
      "{0} query took less time than {1} msec".format(slow_blocking_query,
        blocking_query_min_time)
    # we wait for some time here to make sure that the topic updates from the last
    # query have been propagated so that next run of this method starts from a clean
    # state.
    time.sleep(2)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_tbl_max_wait_time_ms=500")
  def test_topic_updates_advance(self):
    """Test make sure that a if long running blocking queries are run continuously
    topic-update thread is not starved and it eventually blocks until it acquires a table
    lock."""
    # Each of these queries take complete about 30s with the debug action delays
    # below.
    blocking_queries = [
      "refresh tpcds.store_sales",
      "alter table tpcds.store_sales recover partitions",
      "compute stats functional.alltypes"
    ]
    debug_action = "catalogd_refresh_hdfs_listing_delay:SLEEP@30|catalogd_table_recover_delay:SLEEP@10000|catalogd_update_stats_delay:SLEEP@10000"
    # loop in sync_ddl mode so that we know the topic updates are being propagated.
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "true"
    }
    self.__run_loop_test(blocking_queries, blocking_query_options, 60000)

  def __run_loop_test(self, blocking_queries, blocking_query_options, timeout):
    """Runs the given list of queries with given query options in a loop
    and makes sure that they complete without any errors."""
    slow_query_pool = ThreadPool(processes=len(blocking_queries))
    # run the slow query on the impalad-0 with the given query options
    slow_query_futures = {}
    for q in blocking_queries:
      print("Running blocking query {0}".format(q))
      slow_query_futures[q] = slow_query_pool.apply_async(self.loop_exec,
        args=(q, blocking_query_options))

    for q in slow_query_futures:
      # make sure that queries complete eventually.
      durations = slow_query_futures[q].get()
      for i in range(len(durations)):
        assert durations[i] < timeout, "Query {0} iteration {1} did " \
                                       "not complete within {2}.".format(q, i, timeout)

  def loop_exec(self, query, query_options, iterations=3, impalad=0):
    durations = []
    for iter in range(iterations):
      durations.append(self.exec_and_time(query, query_options, impalad))
    return durations

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--topic_update_tbl_max_wait_time_ms=0")
  def test_topic_lock_timeout_disabled(self):
    """Test makes sure that the topic update thread blocks until tables are
    added to each topic update when topic_update_tbl_max_wait_time_ms is set to 0"""
    # queries that we don't expect to block when a slow running blocking query is
    # running in parallel. We want these queries to request the metadata from catalogd
    # and hence the init queries invalidate the metadata before each test case run below.
    non_blocking_queries = [
      # each of these take about 2-4 seconds when there is no lock contention.
      "describe functional.emptytable"
    ]
    # queries used to reset the metadata of the non_blocking_queries so that they will
    # reload the next time they are executed
    init_queries = [
      "invalidate metadata functional.emptytable",
    ]
    # make sure that the blocking query metadata is loaded in catalogd since table lock
    # is only acquired on loaded tables.
    blocking_query = "refresh tpcds.store_sales"
    debug_action = "catalogd_refresh_hdfs_listing_delay:SLEEP@30"
    self.client.execute(blocking_query)
    blocking_query_options = {
      "debug_action": debug_action,
      "sync_ddl": "false"
    }
    self.__run_topic_update_test(blocking_query,
      non_blocking_queries, init_queries, blocking_query_options=blocking_query_options,
      expect_topic_updates_to_block=True)

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
