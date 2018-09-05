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
import random
import threading
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

RETRY_PROFILE_MSG = 'Retrying query planning due to inconsistent metadata'

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
      replans_seen = [0]
      replans_seen_lock = threading.Lock()

      def stress_thread(client):
        while replans_seen[0] == 0:
          # TODO(todd) EXPLAIN queries don't currently yield a profile, so
          # we have to actually run a COUNT query.
          q = random.choice([
              'refresh functional.alltypes',
              'select count(*) from functional.alltypes where month=4',
              'select count(*) from functional.alltypes where month=5'])
          ret = self.execute_query_expect_success(client, q)
          if RETRY_PROFILE_MSG in ret.runtime_profile:
            with replans_seen_lock:
              replans_seen[0] += 1

      threads = [threading.Thread(target=stress_thread, args=(c,))
                 for c in [client1, client2]]
      for t in threads:
        t.start()
      for t in threads:
        t.join(30)
      assert replans_seen[0] > 0, "Did not trigger any re-plans"

    finally:
      client1.close()
      client2.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal")
  def test_cache_profile_metrics(self):
    """
    Test that profile output includes impalad local cache metrics.
    """
    try:
      client = self.cluster.impalads[0].service.create_beeswax_client()
      query = "select count(*) from functional.alltypes"
      ret = self.execute_query_expect_success(client, query)
      assert ret.runtime_profile.count("Frontend:") == 1
      assert ret.runtime_profile.count("CatalogFetch") > 1
    finally:
      client.close()
