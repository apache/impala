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

import pytest
import Queue
import random
import threading
import time

from multiprocessing.pool import ThreadPool

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

RETRY_PROFILE_MSG = 'Retried query planning due to inconsistent metadata'

class TestCompactCatalogUpdates(CustomClusterTestSuite):

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
    finally:
      client1.close()
      client2.close()

    # Global 'INVALIDATE METADATA' is not supported on any impalads running in
    # local_catalog mode, but should work on impalads running in the default
    # mode.
    # TODO(IMPALA-7506): support this!
    for impalad in self.cluster.impalads:
      client = impalad.service.create_beeswax_client()
      try:
        if "--use_local_catalog=true" in impalad.cmd:
          err = self.execute_query_expect_failure(client, 'INVALIDATE METADATA')
          assert 'not supported' in str(err)
        else:
          self.execute_query_expect_success(client, "INVALIDATE METADATA")
      finally:
        client.close()

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
      for attempt in xrange(NUM_ATTEMPTS):
        try:
          self.assert_impalad_log_contains('WARNING', 'Detected catalog service restart')
          err = self.execute_query_expect_failure(client, "select * from %s" % view)
          assert "Could not resolve table reference" in str(err)
          break
        except Exception, e:
          assert attempt < NUM_ATTEMPTS - 1, str(e)
        time.sleep(1)

    finally:
      client.close()


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
    failed_queries = Queue.Queue()
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
            ret = self.execute_query_unchecked(client, q)
          except Exception, e:
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
        t.join(30)

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
      impalad_args="--use_local_catalog=true --local_catalog_max_fetch_retries=0",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_replan_limit(self):
    """
    Tests that the flag to limit the number of retries works and that
    an inconsistent metadata exception when running concurrent reads/writes
    is seen. With the max retries set to 0, no retries are expected and with
    the concurrent read/write workload, an inconsistent metadata exception is
    expected.
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
      failed_queries = Queue.Queue()

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
    for i in t.imap_unordered(do_table, xrange(NUM_ITERS)):
      pass


class TestObservability(CustomClusterTestSuite):
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
      client = impalad.service.create_beeswax_client()
      cache_hit_rate_metric_key = "catalog.cache.hit-rate"
      cache_miss_rate_metric_key = "catalog.cache.miss-rate"
      cache_hit_count_metric_key = "catalog.cache.hit-count"
      cache_request_count_metric_key = "catalog.cache.request-count"
      cache_request_count_prev_run = 0
      cache_hit_count_prev_run = 0
      test_table_name = "%s.test_cache_metrics_test_tbl" % unique_database
      # A mix of queries of various types.
      queries_to_test = ["select count(*) from functional.alltypes",
          "explain select count(*) from functional.alltypes",
          "create table %s (a int)" % test_table_name,
          "drop table %s" % test_table_name]
      for _ in xrange(0, 10):
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
          cache_hit_count_prev_run = cache_hit_count
          cache_request_count_prev_run = cache_request_count
    finally:
      client.close()
