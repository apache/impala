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

import os
import pytest
import time
from subprocess import call
from tests.common.environ import ImpalaTestClusterProperties
from tests.common.skip import SkipIfFS
from tests.util.filesystem_utils import IS_HDFS, IS_LOCAL


from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.event_processor_utils import EventProcessorUtils


class TestAutomaticCatalogInvalidation(CustomClusterTestSuite):
  """ Test that tables are cached in the catalogd after usage for the configured time
      and invalidated afterwards."""
  query = "select count(*) from functional.alltypes"
  # The following columns string presents in the catalog object iff the table loaded.
  metadata_cache_string = "columns (list) = list&lt;struct&gt;"
  url = "http://localhost:25020/catalog_object?object_type=TABLE&" \
        "object_name=functional.alltypes"

  # The test will run a query and assumes the table is loaded when the query finishes.
  # The timeout should be larger than the time of the query.
  timeout = 20 if ImpalaTestClusterProperties.get_instance().runs_slowly() or\
               (not IS_HDFS and not IS_LOCAL) else 10
  timeout_flag = "--invalidate_tables_timeout_s=" + str(timeout)
  metrics_test_ttl_s = 1
  metrics_test_timeout_flag = ("--invalidate_tables_timeout_s=" + str(metrics_test_ttl_s))

  def _get_catalog_object(self):
    """ Return the catalog object of functional.alltypes serialized to string. """
    return self.cluster.catalogd.service.read_debug_webpage(
        "catalog_object?object_type=TABLE&object_name=functional.alltypes")

  def _run_test(self):
    self.client.execute(self.query)
    # The table is cached after usage.
    assert self.metadata_cache_string in self._get_catalog_object()
    # Wait 5 * table TTL for the invalidation to take effect.
    max_wait_time = time.time() + self.timeout * 5
    while True:
      time.sleep(1)
      # The table is eventually evicted.
      if self.metadata_cache_string not in self._get_catalog_object():
        return
      assert time.time() < max_wait_time

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args=timeout_flag, impalad_args=timeout_flag)
  def test_v1_catalog(self):
    self._run_test()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args=timeout_flag + " --catalog_topic_mode=minimal",
      impalad_args=timeout_flag + " --use_local_catalog")
  def test_local_catalog(self):
    self._run_test()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args="--invalidate_tables_timeout_s=1",
                                    impalad_args="--invalidate_tables_timeout_s=1")
  def test_invalid_table(self):
    """ Regression test for IMPALA-7606. Tables failed to be loaded don't have a
        last used time and shouldn't be considered for invalidation."""
    self.execute_query_expect_failure(self.client, "select * from functional.bad_serde")
    # The table expires after 1 second. Sleeping for another logbufsecs=5 seconds to wait
    # for the log to be flushed. Wait 4 more seconds to reduce flakiness.
    time.sleep(10)
    assert "Unexpected exception thrown while attempting to automatically invalidate "\
        "tables" not in open(os.path.join(self.impala_log_dir, "catalogd.INFO")).read()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--invalidate_tables_on_memory_pressure "
                  "--invalidate_tables_gc_old_gen_full_threshold=0 "
                  "--invalidate_tables_fraction_on_memory_pressure=1",
    impalad_args="--invalidate_tables_on_memory_pressure")
  def test_memory_pressure(self):
    """ Test that memory-based invalidation kicks out all the tables after an GC."""
    self.execute_query(self.query)
    # This triggers a full GC as of openjdk 1.8.
    call(["jmap", "-histo:live", str(self.cluster.catalogd.get_pid())])
    # Sleep for logbufsecs=5 seconds to wait for the log to be flushed. Wait 5 more
    # seconds to reduce flakiness.
    time.sleep(10)
    assert self.metadata_cache_string not in self._get_catalog_object()

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(catalogd_args=timeout_flag, impalad_args=timeout_flag)
  def test_loaded_tables_metric(self, unique_database):
    """Test IMPALA-13863: catalog.num-loaded-tables metric tracks loaded tables
       correctly across various metadata operations including loading, invalidation,
       refresh, rename, and removal."""
    metric_name = "catalog.num-loaded-tables"
    catalogd = self.cluster.catalogd.service

    # Test 1: Loading increases counter
    self.execute_query("invalidate metadata")
    catalogd.wait_for_metric_value(metric_name, 0)

    self.execute_query(self.query)
    catalogd.wait_for_metric_value(metric_name, 1)

    # Test 2: Single table INVALIDATE METADATA decreases counter
    self.execute_query("invalidate metadata functional.alltypes")
    catalogd.wait_for_metric_value(metric_name, 0)

    # Test 3: REFRESH loaded table (counter should stay same)
    self.execute_query(self.query)
    catalogd.wait_for_metric_value(metric_name, 1)
    count_before_refresh = catalogd.get_metric_value(metric_name)

    self.execute_query("refresh functional.alltypes")
    # Wait for one metrics refresh cycle (REFRESH_METRICS_INTERVAL_MS)
    # to ensure the metric is updated
    time.sleep(1)
    count_after_refresh = catalogd.get_metric_value(metric_name)
    assert count_after_refresh == count_before_refresh, (
        "Count should stay same after REFRESH of loaded table (was %d, now %d)"
        % (count_before_refresh, count_after_refresh))

    # Test 4: ALTER TABLE RENAME (counter decreases because old loaded table is
    # removed and new table starts as IncompleteTable)
    self.execute_query("create table %s.test_rename_tbl (id int)" % unique_database)
    self.execute_query("select * from %s.test_rename_tbl" % unique_database)
    catalogd.wait_for_metric_value(metric_name, 2)

    self.execute_query("alter table %s.test_rename_tbl rename \
        to %s.test_renamed_tbl" % (unique_database, unique_database))
    catalogd.wait_for_metric_value(metric_name, 1)

    # Verify that accessing the renamed table increments the counter
    self.execute_query("select * from %s.test_renamed_tbl" % unique_database)
    catalogd.wait_for_metric_value(metric_name, 2)

    # Test 5: Load another table, then global INVALIDATE METADATA
    self.execute_query("select count(*) from functional.alltypessmall")
    catalogd.wait_for_metric_value(metric_name, 3)

    self.execute_query("invalidate metadata")
    catalogd.wait_for_metric_value(metric_name, 0)

    # Test 6: CREATE TABLE, load it, then DROP TABLE
    self.execute_query("create table %s.test_metric_tbl (id int)" % unique_database)
    # Wait for one metrics refresh cycle (REFRESH_METRICS_INTERVAL_MS)
    # to ensure the metric is updated
    time.sleep(1)
    count_after_create = catalogd.get_metric_value(metric_name)
    assert count_after_create == 0, (
        "Count should be 0 after creating table (got %d)" % count_after_create)

    self.execute_query("select * from %s.test_metric_tbl" % unique_database)
    catalogd.wait_for_metric_value(metric_name, 1)

    self.execute_query("drop table %s.test_metric_tbl" % unique_database)
    catalogd.wait_for_metric_value(metric_name, 0)

    # Test 7: Hive-side DROP TABLE processed via events
    self.execute_query("create table %s.hive_drop_tbl (id int, val string)"
        % unique_database)
    self.execute_query("select * from %s.hive_drop_tbl" % unique_database)
    catalogd.wait_for_metric_value(metric_name, 1)

    # Drop table from Hive side
    self.run_stmt_in_hive("drop table %s.hive_drop_tbl" % unique_database)
    EventProcessorUtils.wait_for_event_processing(self)
    catalogd.wait_for_metric_value(metric_name, 0)

    # Test 8: DROP DATABASE CASCADE with loaded table
    test_db = ImpalaTestSuite.get_random_name("test_db_")
    try:
      self.execute_query("create database if not exists %s" % test_db)
      self.execute_query("create table %s.t1 (id int, name string)" % test_db)
      self.execute_query("insert into %s.t1 values (1, 'test')" % test_db)
      self.execute_query("select * from %s.t1" % test_db)
      catalogd.wait_for_metric_value(metric_name, 1)
    finally:
      self.execute_query("drop database %s cascade" % test_db)
    catalogd.wait_for_metric_value(metric_name, 0)

    # Test 9: Automatic timeout-based invalidation
    self.execute_query(self.query)
    catalogd.wait_for_metric_value(metric_name, 1)
    assert self.metadata_cache_string in self._get_catalog_object()

    # Wait for automatic timeout-based invalidation to complete and metric to update.
    # ImpaladTableUsageTracker reports table usage with a delay of up to 15 seconds
    # (1.5 * REPORT_INTERVAL_MS where REPORT_INTERVAL_MS=10s), then the invalidation
    # TTL kicks in. Add ~1s extra for metric updates and ~1s buffer for RPC, serde, etc.
    # Max time = 15s (max report delay) + self.timeout (TTL) + 2s
    catalogd.wait_for_metric_value(metric_name, 0, timeout=self.timeout + 17)
    # Verify that the table metadata was actually invalidated
    assert self.metadata_cache_string not in self._get_catalog_object(), \
        "Table metadata should be invalidated after timeout"

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args=metrics_test_timeout_flag,
      impalad_args=metrics_test_timeout_flag)
  def test_invalidation_metrics(self):
    """Test catalog invalidation metrics track TTL and memory pressure
       based invalidations correctly."""

    ttl_metric = "catalog.num-ttl-invalidated-tables"
    memory_metric = "catalog.num-memory-pressure-invalidated-tables"
    ttl_10s = "catalog.ttl-invalidations-10s"
    ttl_1m = "catalog.ttl-invalidations-01m"
    ttl_5m = "catalog.ttl-invalidations-05m"
    ttl_30m = "catalog.ttl-invalidations-30m"
    loaded_tables_metric = "catalog.num-loaded-tables"
    last_ttl_ms = "catalog.last-ttl-invalidation-ms"
    last_ttl_tables = "catalog.last-ttl-invalidated-tables"
    last_mem_ms = "catalog.last-memory-pressure-invalidation-ms"
    last_mem_tables = "catalog.last-memory-pressure-invalidated-tables"
    catalogd = self.cluster.catalogd.service

    # Clear any loaded tables from previous tests to start with a clean slate
    self.execute_query("INVALIDATE METADATA")
    catalogd.wait_for_metric_value(loaded_tables_metric, 0, timeout=5)

    # Capture baseline counters - these are cumulative across test runs
    baseline_ttl_count = catalogd.get_metric_value(ttl_metric)
    baseline_memory_count = catalogd.get_metric_value(memory_metric)
    assert baseline_ttl_count == 0, "TTL metric should be 0"
    assert baseline_memory_count == 0, "Memory pressure metric should be 0"

    # Total max wait = 15s (report delay) + TTL + 2s (buffer)
    max_wait_time = 15 + self.metrics_test_ttl_s + 2

    # Test 1: Load a single table and verify it gets invalidated
    self.execute_query(self.query)
    catalogd.wait_for_metric_value(loaded_tables_metric, 1, timeout=5)

    # Wait for table to be unloaded
    catalogd.wait_for_metric_value(loaded_tables_metric, 0, timeout=max_wait_time,
        interval=2)

    catalogd.wait_for_metric_value(ttl_metric, 1, timeout=5, allow_greater=True)
    ttl_count_after_first = catalogd.get_metric_value(ttl_metric)
    assert ttl_count_after_first == 1, ("TTL invalidation metric should be 1 (got %d)"
        % ttl_count_after_first)

    last_ttl_ms_1 = catalogd.get_metric_value(last_ttl_ms)
    last_ttl_tables_1 = catalogd.get_metric_value(last_ttl_tables)
    assert last_ttl_tables_1 == 1, (
        "last-ttl-invalidated-tables should be 1 after first batch (got %d)"
        % last_ttl_tables_1)
    assert last_ttl_ms_1 > 0, (
        "last-ttl-invalidation-ms should be set after first batch (got %d)"
        % last_ttl_ms_1)
    assert catalogd.get_metric_value(last_mem_ms) == 0, (
        "last-memory-pressure-invalidation-ms should stay 0 (no memory-pressure batch)")
    assert catalogd.get_metric_value(last_mem_tables) == 0, (
        "last-memory-pressure-invalidated-tables should stay 0")

    # Test 2: Verify memory pressure metric remained unchanged
    memory_count_after_first = catalogd.get_metric_value(memory_metric)
    assert memory_count_after_first == baseline_memory_count, (
        "Memory pressure invalidation metric should remain %d (got %d)"
        % (baseline_memory_count, memory_count_after_first))

    # Test 3: Load multiple tables in one query
    join_query = (
        "SELECT 1 FROM functional.alltypessmall s "
        "INNER JOIN functional.alltypesagg a ON s.id = a.id "
        "INNER JOIN functional.alltypes t ON t.id = s.id LIMIT 1")
    self.execute_query(join_query)
    catalogd.wait_for_metric_value(loaded_tables_metric, 3, timeout=5)

    # Wait for all join tables to be unloaded
    catalogd.wait_for_metric_value(loaded_tables_metric, 0, timeout=max_wait_time,
        interval=2)

    # Wait for TTL metric to reach 4 (1 from first test + 3 from join)
    expected_final_ttl = 4
    catalogd.wait_for_metric_value(ttl_metric, expected_final_ttl,
        timeout=5, allow_greater=True)
    final_ttl_count = catalogd.get_metric_value(ttl_metric)
    assert final_ttl_count == expected_final_ttl, ("TTL invalidation metric should be "
        "%d (got %d)" % (expected_final_ttl, final_ttl_count))

    last_ttl_ms_2 = catalogd.get_metric_value(last_ttl_ms)
    last_ttl_tables_2 = catalogd.get_metric_value(last_ttl_tables)
    # last-ttl-invalidated-tables is the size of the most recent batch only. With a
    # short TTL, tables may expire across multiple daemon cycles (e.g. on multi-node
    # clusters where last-used times are refreshed at slightly different times).
    assert 1 <= last_ttl_tables_2 <= 3, (
        "last-ttl-invalidated-tables after join round should be 1-3 (got %d)"
        % last_ttl_tables_2)
    assert last_ttl_ms_2 >= last_ttl_ms_1, (
        "last-ttl-invalidation-ms should not go backwards (%d -> %d)"
        % (last_ttl_ms_1, last_ttl_ms_2))
    assert catalogd.get_metric_value(last_mem_ms) == 0, (
        "last-memory-pressure-invalidation-ms should still be 0")
    assert catalogd.get_metric_value(last_mem_tables) == 0, (
        "last-memory-pressure-invalidated-tables should still be 0")

    # Test 4: Verify memory pressure metric still unchanged
    final_memory_count = catalogd.get_metric_value(memory_metric)
    assert final_memory_count == baseline_memory_count, (
        "Memory pressure invalidation metric should remain %d (got %d)"
        % (baseline_memory_count, final_memory_count))

    # Test 5: Verify all sliding window counts are accessible and non-negative
    ttl_count_10s = catalogd.get_metric_value(ttl_10s)
    ttl_count_1m = catalogd.get_metric_value(ttl_1m)
    ttl_count_5m = catalogd.get_metric_value(ttl_5m)
    ttl_count_30m = catalogd.get_metric_value(ttl_30m)

    assert ttl_count_10s >= 0, (
        "TTL 10-sec count should be non-negative (got %d)" % ttl_count_10s)

    # Test 6: Verify window counts make sense (longer windows should contain at least
    # as many invalidations as shorter windows)
    assert ttl_count_1m >= ttl_count_10s, (
        "1-min count (%d) should be >= 10-sec count (%d)"
        % (ttl_count_1m, ttl_count_10s))
    assert ttl_count_5m >= ttl_count_1m, (
        "5-min count (%d) should be >= 1-min count (%d)" % (ttl_count_5m, ttl_count_1m))
    assert ttl_count_30m >= ttl_count_5m, (
        "30-min count (%d) should be >= 5-min count (%d)" % (ttl_count_30m, ttl_count_5m))

    # Test 7: Window counts should reflect the 4 invalidations we triggered
    assert ttl_count_30m == 4, (
        "30-min window should contain exactly the 4 invalidations we triggered (got %d)"
        % ttl_count_30m)

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--invalidate_tables_on_memory_pressure "
                  "--invalidate_tables_gc_old_gen_full_threshold=0 "
                  "--invalidate_tables_fraction_on_memory_pressure=1 "
                  "--invalidate_tables_timeout_s=0",
    impalad_args="--invalidate_tables_on_memory_pressure "
                  "--invalidate_tables_timeout_s=0")
  def test_memory_pressure_metrics(self):
    """Test memory pressure invalidation metrics work correctly."""

    ttl_metric = "catalog.num-ttl-invalidated-tables"
    memory_metric = "catalog.num-memory-pressure-invalidated-tables"
    memory_1m = "catalog.memory-pressure-invalidations-01m"
    memory_5m = "catalog.memory-pressure-invalidations-05m"
    memory_30m = "catalog.memory-pressure-invalidations-30m"
    loaded_tables_metric = "catalog.num-loaded-tables"
    last_mem_ms = "catalog.last-memory-pressure-invalidation-ms"
    last_mem_tables = "catalog.last-memory-pressure-invalidated-tables"
    catalogd = self.cluster.catalogd.service

    # Clear state and get baseline
    self.execute_query("INVALIDATE METADATA")
    catalogd.wait_for_metric_value(loaded_tables_metric, 0, timeout=5)

    baseline_ttl_count = catalogd.get_metric_value(ttl_metric)

    self.execute_query(self.query)
    catalogd.wait_for_metric_value(loaded_tables_metric, 1, timeout=5)

    # Trigger memory pressure invalidation using GC
    call(["jmap", "-histo:live", str(self.cluster.catalogd.get_pid())])

    catalogd.wait_for_metric_value(loaded_tables_metric, 0, timeout=15, interval=2)

    # Verify memory pressure metric increased
    catalogd.wait_for_metric_value(memory_metric, 1, timeout=5, allow_greater=True)

    assert catalogd.get_metric_value(last_mem_ms) > 0, (
        "last-memory-pressure-invalidation-ms should be set after eviction (got %d)"
        % catalogd.get_metric_value(last_mem_ms))
    assert catalogd.get_metric_value(last_mem_tables) >= 1, (
        "last-memory-pressure-invalidated-tables should be at least 1 (got %d)"
        % catalogd.get_metric_value(last_mem_tables))

    # Verify TTL metric unchanged
    final_ttl_count = catalogd.get_metric_value(ttl_metric)
    assert final_ttl_count == baseline_ttl_count, (
        "TTL metric should remain %d (got %d)"
        % (baseline_ttl_count, final_ttl_count))

    # Sliding windows for the single memory-pressure batch we triggered
    memory_count_1m = catalogd.get_metric_value(memory_1m)
    memory_count_5m = catalogd.get_metric_value(memory_5m)
    memory_count_30m = catalogd.get_metric_value(memory_30m)
    assert memory_count_1m == 1, (
        "1-min memory pressure window should show 1 invalidation (got %d)"
        % memory_count_1m)
    assert memory_count_5m == 1, (
        "5-min memory pressure window should show 1 invalidation (got %d)"
        % memory_count_5m)
    assert memory_count_30m == 1, (
        "30-min memory pressure window should show 1 invalidation (got %d)"
        % memory_count_30m)
