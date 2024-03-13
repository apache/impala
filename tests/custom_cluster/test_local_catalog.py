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

# Test behaviors specific to --use_local_catalog being enabled.

from __future__ import absolute_import, division, print_function
from builtins import range
import pytest
import queue
import random
import re
import threading
import time

from multiprocessing.pool import ThreadPool

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfHive2, SkipIfFS
from tests.util.filesystem_utils import WAREHOUSE

RETRY_PROFILE_MSG = 'Retried query planning due to inconsistent metadata'
CATALOG_VERSION_LOWER_BOUND = 'catalog.catalog-object-version-lower-bound'


class TestLocalCatalogCompactUpdates(CustomClusterTestSuite):

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_minimal_topic_updates_sync_ddl(self, unique_database):
    """
    Start Impala cluster with minimal catalog update topics and local catalog enabled.
    Run some smoke tests for SYNC_DDL to ensure that invalidations are propagated.
    """
    self._do_test_sync_ddl(unique_database)

  def _make_per_impalad_args(local_catalog_enabled):
    assert isinstance(local_catalog_enabled, list)
    args = ['--use_local_catalog=%s' % str(e).lower()
            for e in local_catalog_enabled]
    return "--per_impalad_args=" + ";".join(args)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      start_args=_make_per_impalad_args([True, False]),
      catalogd_args="--catalog_topic_mode=mixed")
  def test_mixed_topic_updates_sync_ddl(self, unique_database):
    """
    Same as above, but with 'mixed' mode catalog and different configs
    on the two different impalads used by the test.
    """
    self._do_test_sync_ddl(unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      start_args=_make_per_impalad_args([False, True]),
      catalogd_args="--catalog_topic_mode=mixed")
  def test_mixed_topic_updates_sync_ddl_2(self, unique_database):
    """
    Same as above, but with opposite configurations for the two
    impalads used in the test.
    """
    self._do_test_sync_ddl(unique_database)

  def _do_test_sync_ddl(self, unique_database):
    """ Implementation details for above two tests. """
    try:
      impalad1 = self.cluster.impalads[0]
      impalad2 = self.cluster.impalads[1]
      client1 = impalad1.service.create_beeswax_client()
      client2 = impalad2.service.create_beeswax_client()

      view = "%s.my_view" % unique_database

      # Try to describe the view before it exists - should get an error.
      # This should prime any caches in impalad2.
      err = self.execute_query_expect_failure(client2, "describe %s" % view)
      assert 'Could not resolve' in str(err)

      # Create it with SYNC_DDL from client 1.
      query_options = {"sync_ddl": 1}
      self.execute_query_expect_success(client1, "create view %s as select 1" % view,
          query_options)

      # It should be immediately visible from client 2.
      self.execute_query_expect_success(client2, "describe %s" % view)

      # Test global INVALIDATE METADATA
      new_db = unique_database + '_new'
      self.execute_query_expect_success(
        client1, "create database if not exists %s" % new_db, query_options)
      # The new database should be immediately visible from client 2.
      self.execute_query_expect_success(client2, "describe database %s" % new_db)
      # Drop database in Hive. Params: name, deleteData, cascade
      self.hive_client.drop_database(new_db, True, True)
      self.execute_query_expect_success(client1, "invalidate metadata", query_options)
      err = self.execute_query_expect_failure(client2, "describe database %s" % new_db)
      assert 'Database does not exist' in str(err)
    finally:
      client1.close()
      client2.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_restart_catalogd(self, unique_database):
    """
    Tests for the behavior of LocalCatalog when catalogd restarts.
    """
    try:
      impalad = self.cluster.impalads[0]
      client = impalad.service.create_beeswax_client()

      view = "%s.my_view" % unique_database
      self.execute_query_expect_success(client, "create view %s as select 1" % view)
      self.execute_query_expect_success(client, "select * from %s" % view)

      # Should not have any detected restarts, initially.
      self.assert_impalad_log_contains('WARNING', 'Detected catalog service restart',
                                       expected_count=0)

      # Kill catalogd, and while it's down, drop the view via HMS.
      self.cluster.catalogd.kill()
      # Drop the view via hive to ensure that when catalogd restarts,
      # the impalads see the dropped view.
      self.hive_client.drop_table(unique_database, "my_view", True)

      # Start catalogd again. We should see the view disappear once the
      # catalog pushes a new topic update.
      self.cluster.catalogd.start()
      NUM_ATTEMPTS = 30
      for attempt in range(NUM_ATTEMPTS):
        try:
          self.assert_impalad_log_contains('WARNING', 'Detected catalog service restart')
          err = self.execute_query_expect_failure(client, "select * from %s" % view)
          assert "Could not resolve table reference" in str(err)
          break
        except Exception as e:
          assert attempt < NUM_ATTEMPTS - 1, str(e)
        time.sleep(1)

    finally:
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal "
                  "--enable_incremental_metadata_updates=true")
  def test_invalidate_stale_partitions(self, unique_database):
    """
    Test that partition level invalidations are sent from catalogd and processed
    correctly in coordinators.
    TODO: Currently, there are no ways to get the cached partition ids in a LocalCatalog
     coordinator. So this test infers them based on the query pattern. However, this
     depends on the implementation details of catalogd which will evolve and may have to
     change the partition ids in this test. A more robust ways is a) get the cached
     partition id of a partition, b) run a DML on this partition, c) verify the old
     partition id is invalidate.
    """
    # Creates a partitioned table and inits 3 partitions on it. They are the first 3
    # partitions loaded in catalogd. So their partition ids are 0,1,2.
    self.execute_query("use " + unique_database)
    self.execute_query("create table my_part (id int) partitioned by (p int)")
    self.execute_query("alter table my_part add partition (p=0)")
    self.execute_query("alter table my_part add partition (p=1)")
    self.execute_query("alter table my_part add partition (p=2)")
    # Trigger a query on all partitions so they are loaded in local catalog cache.
    self.execute_query("select count(*) from my_part")
    # Update all partitions. We should receive invalidations for partition id=0,1,2.
    self.execute_query("insert into my_part partition(p) values (0,0),(1,1),(2,2)")

    log_regex = "Invalidated objects in cache: \[partition %s.my_part:p=\d \(id=%%d\)\]"\
                % unique_database
    self.assert_impalad_log_contains('INFO', log_regex % 0)
    self.assert_impalad_log_contains('INFO', log_regex % 1)
    self.assert_impalad_log_contains('INFO', log_regex % 2)

    # Trigger a query on all partitions so partitions with id=3,4,5 are loaded in local
    # catalog cache.
    self.execute_query("select count(*) from my_part")
    # Update all partitions. We should receive invalidations for partition id=3,4,5.
    # The new partitions are using id=6,7,8.
    self.execute_query(
        "insert overwrite my_part partition(p) values (0,0),(1,1),(2,2)")
    self.assert_impalad_log_contains('INFO', log_regex % 3)
    self.assert_impalad_log_contains('INFO', log_regex % 4)
    self.assert_impalad_log_contains('INFO', log_regex % 5)

    # Repeat the same test on non-partitioned tables
    self.execute_query("create table my_tbl (id int)")
    # Trigger a query to load the only partition which has partition id = 9.
    self.execute_query("select count(*) from my_tbl")
    # Update the table. So we should receive an invalidation on partition id = 9.
    self.execute_query("insert into my_tbl select 0")
    self.assert_impalad_log_contains(
        'INFO', "Invalidated objects in cache: \[partition %s.my_tbl: \(id=9\)\]"
                % unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_global_invalidate_metadata_with_sync_ddl(self, unique_database):
    try:
      impalad1 = self.cluster.impalads[0]
      impalad2 = self.cluster.impalads[1]
      client1 = impalad1.service.create_beeswax_client()
      client2 = impalad2.service.create_beeswax_client()

      # Create something to make the cache not empty.
      self.execute_query_expect_success(
        client1, "CREATE TABLE %s.my_tbl (i int)" % unique_database)
      self.execute_query_expect_success(
        client1, "CREATE FUNCTION %s.my_func LOCATION '%s/impala-hive-udfs.jar' "
                 "SYMBOL='org.apache.impala.TestUdf'" % (unique_database, WAREHOUSE))
      self.execute_query_expect_success(
        client1, "select * from functional.alltypestiny")
      version_lower_bound = impalad1.service.get_metric_value(
        CATALOG_VERSION_LOWER_BOUND)

      # Reset catalog with SYNC_DDL from client 2.
      query_options = {"sync_ddl": 1}
      self.execute_query_expect_success(client2, "INVALIDATE METADATA", query_options)
      assert version_lower_bound < impalad1.service.get_metric_value(
        CATALOG_VERSION_LOWER_BOUND)
      version_lower_bound = impalad1.service.get_metric_value(
        CATALOG_VERSION_LOWER_BOUND)
      assert version_lower_bound == impalad2.service.get_metric_value(
        CATALOG_VERSION_LOWER_BOUND)
    finally:
      client1.close()
      client2.close()


class TestLocalCatalogRetries(CustomClusterTestSuite):

  def _check_metadata_retries(self, queries):
    """
    Runs 'queries' concurrently, recording any inconsistent metadata exceptions.
    'queries' is a list of query strings. The queries are run by two threads,
    each one selecting a random query to run in a loop.
    """
    # Tracks number of inconsistent metadata exceptions.
    inconsistent_seen = [0]
    inconsistent_seen_lock = threading.Lock()
    # Tracks query failures for all other reasons.
    failed_queries = queue.Queue()
    try:
      client1 = self.cluster.impalads[0].service.create_beeswax_client()
      client2 = self.cluster.impalads[1].service.create_beeswax_client()

      def stress_thread(client):
        # Loops, picks a random query in each iteration, runs it,
        # and looks for retries and InconsistentMetadataFetchExceptions.
        attempt = 0
        while inconsistent_seen[0] == 0 and attempt < 200:
          q = random.choice(queries)
          attempt += 1
          try:
            print('Attempt', attempt, 'client', str(client))
            ret = self.execute_query_unchecked(client, q)
          except Exception as e:
            if 'InconsistentMetadataFetchException' in str(e):
              with inconsistent_seen_lock:
                inconsistent_seen[0] += 1
            else:
              failed_queries.put((q, str(e)))

      threads = [threading.Thread(target=stress_thread, args=(c,))
                 for c in [client1, client2]]
      for t in threads:
        t.start()
      for t in threads:
        # When there are failures, they're observed quickly.
        # 600s is enough for 200 attempts.
        t.join(600)

      assert failed_queries.empty(),\
          "Failed query count non zero: %s" % list(failed_queries.queue)

    finally:
      client1.close()
      client2.close()
    return inconsistent_seen[0]

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_fetch_metadata_retry(self):
    """
    Tests that operations that fetch metadata (excluding those fetches needed for
    query planning) retry when they hit an InconsistentMetadataFetchException.
    """
    queries = [
      "show column stats functional.alltypes",
      "show table stats functional.alltypes",
      "describe extended functional.alltypes",
      "show tables in functional like 'all*'",
      "show files in functional.alltypes",
      "refresh functional.alltypes"]
    seen = self._check_metadata_retries(queries)
    assert seen == 0, "Saw inconsistent metadata"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true --local_catalog_max_fetch_retries=0"
                   " --inject_latency_before_catalog_fetch_ms=500",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_replan_limit(self):
    """
    Tests that the flag to limit the number of retries works and that
    an inconsistent metadata exception when running concurrent reads/writes
    is seen. With the max retries set to 0, no retries are expected and with
    the concurrent read/write workload, an inconsistent metadata exception is
    expected. Setting inject_latency_before_catalog_fetch_ms to increases the
    possibility of a stale request which throws the expected exception.
    """
    queries = [
      'refresh functional.alltypes',
      'refresh functional.alltypes partition (year=2009, month=4)',
      'select count(*) from functional.alltypes where month=4']
    seen = self._check_metadata_retries(queries)
    assert seen > 0, "Did not observe inconsistent metadata"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_replan_on_stale_metadata(self, unique_database):
    """
    Tests that when metadata is inconsistent while planning a query,
    the query planner retries the query.
    """
    try:
      impalad1 = self.cluster.impalads[0]
      impalad2 = self.cluster.impalads[1]
      client1 = impalad1.service.create_beeswax_client()
      client2 = impalad2.service.create_beeswax_client()

      # Create a view in client 1, cache the table list including that view in
      # client 2, and then drop it in client 1. While we've still cached the
      # table list, try to describe the view from client 2 -- it should fail
      # with the normal error message even though it had the inconsistent cache.
      view = "%s.my_view" % unique_database
      self.execute_query_expect_success(client1, "create view %s as select 1" % view)
      self.execute_query_expect_success(client2, "show tables")
      self.execute_query_expect_success(client1, "drop view %s" % view)
      err = self.execute_query_expect_failure(client2, "describe %s" % view)
      assert "Could not resolve path" in str(err)

      # Run a mix of concurrent REFRESH and queries against different subsets
      # of partitions. This causes partial views of the table to get cached,
      # and then as the new partitions are loaded, we detect the version skew
      # and issue re-plans. We run the concurrent workload until the profile
      # indicates that a replan has happened.
      # We expect stress_thread to cause a re-plan. The counter is stored in a
      # mutable container so that stress_thread can update it.
      # TODO: consolidate with _check_metadata_retries.
      replans_seen = [0]
      replans_seen_lock = threading.Lock()

      # Queue to propagate exceptions from failed queries, if any.
      failed_queries = queue.Queue()

      def stress_thread(client):
        while replans_seen[0] == 0:
          # TODO(todd) EXPLAIN queries don't currently yield a profile, so
          # we have to actually run a COUNT query.
          q = random.choice([
              'invalidate metadata functional.alltypes',
              'select count(*) from functional.alltypes where month=4',
              'select count(*) from functional.alltypes where month=5'])

          try:
            ret = self.execute_query_expect_success(client, q)
          except Exception as e:
            failed_queries.put((q, str(e)))
            continue

          if RETRY_PROFILE_MSG in ret.runtime_profile:
            with replans_seen_lock:
              replans_seen[0] += 1

      threads = [threading.Thread(target=stress_thread, args=(c,))
                 for c in [client1, client2]]
      for t in threads:
        t.start()
      for t in threads:
        t.join(30)
      assert failed_queries.empty(), "Failed queries encountered: %s" %\
          list(failed_queries.queue)
      assert replans_seen[0] > 0, "Did not trigger any re-plans"

    finally:
      client1.close()
      client2.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true --inject_latency_after_catalog_fetch_ms=50",
      catalogd_args="--catalog_topic_mode=minimal",
      cluster_size=1)
  def test_invalidation_races(self, unique_database):
    """
    Regression test for IMPALA-7534: races where invalidation of the table list
    could be skipped, causing spurious "table not found" errors.
    """
    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self):
        self.c = test_self.create_impala_client()

    t = ThreadPool(processes=8)
    tls = ThreadLocalClient()

    def do_table(i):
      for q in [
        "create table {db}.t{i} (i int)",
        "describe {db}.t{i}",
        "drop table {db}.t{i}",
        "create database {db}_{i}",
        "show tables in {db}_{i}",
        "drop database {db}_{i}"]:
        self.execute_query_expect_success(tls.c, q.format(
            db=unique_database, i=i))

    # Prior to fixing IMPALA-7534, this test would fail within 20-30 iterations,
    # so 100 should be quite reliable as a regression test.
    NUM_ITERS = 100
    for i in t.imap_unordered(do_table, range(NUM_ITERS)):
      pass

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true "
                   "--inject_failure_ratio_in_catalog_fetch=0.1 "
                   "--inject_latency_after_catalog_fetch_ms=100",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_fetch_metadata_retry_in_piggybacked_failures(self, unique_database):
    test_self = self

    class ThreadLocalClient(threading.local):
      def __init__(self):
        self.c = test_self.create_impala_client()

    NUM_THREADS = 8
    t = ThreadPool(processes=NUM_THREADS)
    tls = ThreadLocalClient()

    self.execute_query(
        "create table {0}.tbl (i int) partitioned by (p int)".format(unique_database))
    self.execute_query(
        "insert into {0}.tbl partition(p) values (0,0)".format(unique_database))

    def read_part(i):
      self.execute_query_expect_success(
          tls.c, "select * from {0}.tbl where p=0".format(unique_database))

    # Prior to fixing IMPALA-12670, this test would fail within 20 iterations,
    # so 100 should be quite reliable as a regression test.
    NUM_ITERS = 100
    for k in range(NUM_ITERS):
      # Read the same partition in concurrent queries so requests can be piggybacked.
      for i in t.imap_unordered(read_part, range(NUM_THREADS)):
        pass
      # Refresh to invalidate the partition in local catalog cache
      self.execute_query("refresh {0}.tbl partition(p=0)".format(unique_database))

class TestLocalCatalogObservability(CustomClusterTestSuite):
  def get_catalog_cache_metrics(self, impalad):
    """ Returns catalog cache metrics as a dict by scraping the json metrics page on the
    given impalad"""
    child_groups =\
        impalad.service.get_debug_webpage_json('metrics')['metric_group']['child_groups']
    for group in child_groups:
      if group['name'] != 'impala-server': continue
      # Filter catalog cache metrics.
      for child_group in group['child_groups']:
        if child_group['name'] != 'catalog': continue
        metrics_data = [(metric['name'], metric['value'])
            for metric in child_group['metrics'] if 'catalog.cache' in metric['name']]
        return dict(metrics_data)
    assert False, "Catalog cache metrics not found in %s" % child_groups

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_cache_metrics(self, unique_database):
    """
    Test that profile output includes impalad local cache metrics. Also verifies that
    the daemon level metrics are updated between query runs.
    """
    try:
      impalad = self.cluster.impalads[0]
      # Make sure local catalog mode is enabled and visible on web UI.
      assert '(Local Catalog Mode)' in impalad.service.read_debug_webpage('/')
      # Make sure /catalog_object endpoint is disabled on web UI.
      assert 'No URI handler for &apos;/catalog_object&apos;' \
        in impalad.service.read_debug_webpage('/catalog_object')
      client = impalad.service.create_beeswax_client()
      cache_hit_rate_metric_key = "catalog.cache.hit-rate"
      cache_miss_rate_metric_key = "catalog.cache.miss-rate"
      cache_hit_count_metric_key = "catalog.cache.hit-count"
      cache_request_count_metric_key = "catalog.cache.request-count"
      cache_entry_median_size_key = "catalog.cache.entry-median-size"
      cache_entry_99th_size_key = "catalog.cache.entry-99th-size"
      cache_request_count_prev_run = 0
      cache_hit_count_prev_run = 0
      test_table_name = "%s.test_cache_metrics_test_tbl" % unique_database
      # A mix of queries of various types.
      queries_to_test = ["select count(*) from functional.alltypes",
          "explain select count(*) from functional.alltypes",
          "create table %s (a int)" % test_table_name,
          "drop table %s" % test_table_name]
      for _ in range(0, 10):
        for query in queries_to_test:
          ret = self.execute_query_expect_success(client, query)
          assert ret.runtime_profile.count("Frontend:") == 1
          assert ret.runtime_profile.count("CatalogFetch") > 1
          cache_metrics = self.get_catalog_cache_metrics(impalad)
          cache_hit_rate = cache_metrics[cache_hit_rate_metric_key]
          cache_miss_rate = cache_metrics[cache_miss_rate_metric_key]
          cache_hit_count = cache_metrics[cache_hit_count_metric_key]
          cache_request_count = cache_metrics[cache_request_count_metric_key]
          assert cache_hit_rate > 0.0 and cache_hit_rate < 1.0
          assert cache_miss_rate > 0.0 and cache_miss_rate < 1.0
          assert cache_hit_count > cache_hit_count_prev_run,\
              "%s not updated between two query runs, query - %s"\
              % (cache_hit_count_metric_key, query)
          assert cache_request_count > cache_request_count_prev_run,\
             "%s not updated betweeen two query runs, query - %s"\
             % (cache_request_count_metric_key, query)

          cache_entry_median_size = cache_metrics[cache_entry_median_size_key]
          cache_entry_99th_size = cache_metrics[cache_entry_99th_size_key]
          assert cache_entry_median_size > 300 and cache_entry_median_size < 3000
          assert cache_entry_99th_size > 12500 and cache_entry_99th_size < 19000

          cache_hit_count_prev_run = cache_hit_count
          cache_request_count_prev_run = cache_request_count
    finally:
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_lightweight_rpc_metrics(self):
    """Verify catalogd client cache for lightweight RPCs is used correctly"""
    # Fetching the db and table list should be lightweight requests
    self.execute_query("describe database functional")
    self.execute_query("show tables in functional")
    impalad = self.cluster.impalads[0].service
    assert 0 == impalad.get_metric_value("catalog.server.client-cache.total-clients")
    assert 1 == impalad.get_metric_value(
        "catalog.server.client-cache.total-clients-for-lightweight-rpc")


class TestFullAcid(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @SkipIfHive2.acid
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_full_acid_support(self):
    """IMPALA-9685: canary test for full acid support in local catalog"""
    self.execute_query("show create table functional_orc_def.alltypestiny")
    res = self.execute_query("select id from functional_orc_def.alltypestiny")
    res.data.sort()
    assert res.data == ['0', '1', '2', '3', '4', '5', '6', '7']

  @SkipIfHive2.acid
  @SkipIfFS.hive
  @pytest.mark.execute_serially
  def test_full_acid_scans(self, vector, unique_database):
    self.run_test_case('QueryTest/full-acid-scans', vector, use_db=unique_database)

class TestReusePartitionMetadata(CustomClusterTestSuite):
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal")
  def test_reuse_partition_meta(self, unique_database):
    """
    Test that unchanged partition metadata can be shared across table versions.
    """
    self.execute_query(
        "create table %s.alltypes like functional.alltypes" % unique_database)
    self.execute_query("insert into %s.alltypes partition(year, month) "
                       "select * from functional.alltypes" % unique_database)
    # Make sure the table is unloaded either in catalogd or coordinator.
    self.execute_query("invalidate metadata %s.alltypes" % unique_database)
    # First time: misses all(24) partitions.
    self.check_missing_partitions(unique_database, 24, 24)
    # Second time: hits all(24) partitions.
    self.check_missing_partitions(unique_database, 0, 24)

    # Alter comment on the table. Partition metadata should be reusable.
    self.execute_query(
        "comment on table %s.alltypes is null" % unique_database)
    self.check_missing_partitions(unique_database, 0, 24)

    # Refresh one partition. Although table version bumps, metadata cache of other
    # partitions should be reusable.
    self.execute_query(
        "refresh %s.alltypes partition(year=2009, month=1)" % unique_database)
    self.check_missing_partitions(unique_database, 1, 24)

    # Drop one partition. Although table version bumps, metadata cache of existing
    # partitions should be reusable.
    self.execute_query(
        "alter table %s.alltypes drop partition(year=2009, month=1)" % unique_database)
    self.check_missing_partitions(unique_database, 0, 23)

    # Add back one partition. The partition meta is loaded in catalogd but not the
    # coordinator. So we still miss its meta. For other partitions, we can reuse them.
    self.execute_query(
        "insert into %s.alltypes partition(year=2009, month=1) "
        "select 0,true,0,0,0,0,0,0,'a','a',NULL" % unique_database)
    self.check_missing_partitions(unique_database, 1, 24)

  def check_missing_partitions(self, unique_database, partition_misses, total_partitions):
    """Helper method for checking number of missing partitions while selecting
     all partitions of the alltypes table"""
    ret = self.execute_query_expect_success(
        self.client, "explain select count(*) from %s.alltypes" % unique_database)
    assert ("partitions=%d" % total_partitions) in ret.get_data()
    match = re.search(r"CatalogFetch.Partitions.Misses: (\d+)", ret.runtime_profile)
    assert len(match.groups()) == 1
    assert match.group(1) == str(partition_misses)
