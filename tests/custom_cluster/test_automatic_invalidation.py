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
import os
import pytest
import time
from subprocess import call
from tests.common.environ import ImpalaTestClusterProperties
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

  def _get_catalog_object(self):
    """ Return the catalog object of functional.alltypes serialized to string. """
    return self.cluster.catalogd.service.read_debug_webpage(
        "catalog_object?object_type=TABLE&object_name=functional.alltypes")

  def _run_test(self, cursor):
    cursor.execute(self.query)
    cursor.fetchall()
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
  def test_v1_catalog(self, cursor):
    self._run_test(cursor)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      catalogd_args=timeout_flag + " --catalog_topic_mode=minimal",
      impalad_args=timeout_flag + " --use_local_catalog")
  def test_local_catalog(self, cursor):
    self._run_test(cursor)

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
