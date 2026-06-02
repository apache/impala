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

import logging
import pytest
import threading

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType, SkipIfExploration
from tests.util.retry import retry


@SkipIfBuildType.not_dev_build
class TestCatalogWait(CustomClusterTestSuite):
  """Impalad coordinators must wait for their local replica of the catalog to be
     initialized from the statestore prior to opening up client ports.
     This test simulates a failed or slow catalog on impalad startup."""

  def expect_connection(self, impalad):
    impalad.service.create_hs2_client()

  def expect_no_connection(self, impalad):
    with pytest.raises(Exception) as e:
      impalad.service.create_hs2_client()
      assert 'Could not connect to' in str(e.value)

  @pytest.mark.execute_serially
  def test_delayed_impalad_catalog(self):
    """ Tests client interactions with the cluster when one of the daemons,
        impalad[2], is delayed in initializing its local catalog replica.
        This delay is simulated on the impalad side, and the catalogd starts
        up normally."""

    # On startup, expect only two executors to be registered.
    self._start_impala_cluster(["--catalog_init_delays=0,0,200000"],
                               expected_num_impalads=2,
                               expected_subscribers=4)

    # Expect that impalad[2] is not ready.
    self.cluster.impalads[2].service.wait_for_metric_value('impala-server.ready', 0);

    # Expect that impalad[0,1] are both ready and with initialized catalog.
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.ready', 1);
    self.cluster.impalads[0].service.wait_for_metric_value('catalog.ready', 1);
    self.cluster.impalads[1].service.wait_for_metric_value('impala-server.ready', 1);
    self.cluster.impalads[1].service.wait_for_metric_value('catalog.ready', 1);

    # Expect that connections can be made to impalads[0,1], but not to impalads[2].
    self.expect_connection(self.cluster.impalads[0])
    self.expect_connection(self.cluster.impalads[1])
    self.expect_no_connection(self.cluster.impalads[2])

    # Issues a query to check that impalad[2] does not evaluate any fragments
    # and does not prematurely register itself as an executor. The former is
    # verified via query fragment metrics and the latter would fail if registered
    # but unable to process fragments.
    client0 = self.cluster.impalads[0].service.create_hs2_client()
    client1 = self.cluster.impalads[1].service.create_hs2_client()

    self.execute_query_expect_success(client0, "select * from functional.alltypes");
    self.execute_query_expect_success(client1, "select * from functional.alltypes");

    # Check that fragments were run on impalad[0,1] and none on impalad[2].
    # Each ready impalad runs a fragment per query and one coordinator fragment. With
    # two queries, one coordinated per ready impalad, that should be 3 total fragments.
    self.cluster.impalads[0].service.wait_for_metric_value('impala-server.num-fragments', 3);
    self.cluster.impalads[1].service.wait_for_metric_value('impala-server.num-fragments', 3);
    self.cluster.impalads[2].service.wait_for_metric_value('impala-server.num-fragments', 0);


@SkipIfBuildType.not_dev_build
class TestCatalogStartupDelay(CustomClusterTestSuite):
  """This test injects a real delay in catalogd startup. The impalads are expected to be
     able to tolerate this delay, either because they wait (as coordinators do) or
     because they don't need anything from the catalogd. This is done for a few
     different cluster setups (different metadata, exclusive coordinators). This
     is not testing anything beyond successful startup."""

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('Catalog startup delay tests only run in exhaustive')
    super(TestCatalogStartupDelay, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--stress_catalog_startup_delay_ms=60000")
  def test_default_metadata_settings(self):
    """This variant tests the default metadata settings."""
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before the catalogd finishes starting up.
    self.execute_query("select count(*) from functional.alltypes")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--stress_catalog_startup_delay_ms=60000 --catalog_topic_mode=minimal")
  def test_local_catalog(self):
    """This variant tests with the local catalog."""
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before the catalogd finishes starting up.
    self.execute_query("select count(*) from functional.alltypes")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    num_exclusive_coordinators=1,
    impalad_args="--use_local_catalog=true",
    catalogd_args="--stress_catalog_startup_delay_ms=60000 --catalog_topic_mode=minimal")
  def test_local_catalog_excl_coord(self):
    """This variant tests with the local catalog and an exclusive coordinator. The
       purpose is to verify that executors do not break."""
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before the catalogd finishes starting up.
    self.execute_query("select count(*) from functional.alltypes")


class TestCatalogStartupDeadlock(CustomClusterTestSuite):
  @SkipIfExploration.is_not_exhaustive()
  @CustomClusterTestSuite.with_args(
      cluster_size=2,
      num_exclusive_coordinators=1,
      catalogd_args="--reset_metadata_lock_duration_ms=1 "
                    "--debug_actions=reset_metadata_loop_locked:SLEEP@2000|"
                    "reset_metadata_loop_unlocked:SLEEP@500")
  def test_catalog_startup_deadlock(self):
    """Regression test for IMPALA-14949 to verify that Catalogd does not deadlock during
       startup when processing the initial reset metadata requests while under load."""
    stop_workload = False

    # Impyla logs a vast amount of errors when Catalogd is down, so turn off logging to
    # avoid cluttering the test output.
    for logger in ["thrift.transport.TSocket", "impala.hiveserver2"]:
      lg = logging.getLogger(logger)
      lg.handlers = []
      lg.propagate = False
      lg.disabled = True

    def run_query_workload():
      with self.create_impala_client() as client:
        while True:
          if stop_workload:
            break

          try:
            self.execute_query_unchecked(client,
                "invalidate metadata functional_parquet.widetable_1000_cols")
            self.execute_query_unchecked(client,
                "refresh functional_parquet.widetable_1000_cols")
            self.execute_query_unchecked(client,
                "select * from functional_parquet.widetable_1000_cols")
          except Exception:
            # Ignore exceptions since Catalogd will be down during its restart.
            pass

    # Spawn threads to repeatedly run queries that will force metadata reloading.
    threads = []
    num_threads = 25
    for _ in range(num_threads):
      t = threading.Thread(target=run_query_workload)
      t.start()
      threads.append(t)

    try:
      # Allow time for the threads to start and run queries.
      def count_completed_queries():
        queries_json = \
            self.cluster.get_first_impalad().service.get_debug_webpage_json('/queries')
        return len(queries_json["completed_queries"]) > 100

      assert retry(
          func=count_completed_queries, max_attempts=60, sleep_time_s=2, backoff=1), \
          "Expected some queries to complete but not enough completed."

      # Force stop Catalogd to trigger the deadlock scenario.
      self.cluster.catalogd.kill()
      self.cluster.catalogd.wait_for_exit()

      # Wait for queries from each thread to queue up. These queries will be in CREATED
      # state since they require interacting with the Catalogd which is currently down.
      inflight_query = None
      num_inflight = None

      def count_inflight_queries():
        nonlocal inflight_query, num_inflight
        queries_json = \
            self.cluster.get_first_impalad().service.get_debug_webpage_json('/queries')
        num_inflight = len(queries_json["in_flight_queries"])
        if num_inflight > 0:
          inflight_query = queries_json["in_flight_queries"][0]["query_id"]
        return num_inflight == num_threads

      assert retry(
          func=count_inflight_queries, max_attempts=30, sleep_time_s=2, backoff=1), \
          "Expected {} queries to be in-flight but there were {}." \
              .format(num_threads, num_inflight)

      # Start Catalogd again.
      self.cluster.catalogd.start(wait_until_ready=False)
      self.assert_catalogd_log_contains("INFO",
          "resetMetadata request: INVALIDATE ALL issued by null", timeout_s=10)

      # Assert queries are progressing and not hung due to the deadlock. Since so many
      # queries are being run, a particular query will roll off the list of 200 most
      # recent queries. Thus to assert the query completed, verify it is no longer in the
      # list of in-flight queries.
      def check_query_progress():
        assert inflight_query is not None, "No in-flight query."
        query_completed = True
        queries_json = \
            self.cluster.get_first_impalad().service.get_debug_webpage_json('/queries')

        for query in queries_json["in_flight_queries"]:
          if query["query_id"] == inflight_query:
            query_completed = False
            break

        return query_completed

      assert retry(
          func=check_query_progress, max_attempts=60, sleep_time_s=2, backoff=1), \
          "Expected query '{}' to complete but it did not".format(inflight_query)
    finally:
      stop_workload = True

      # If the deadlock occurred, the queries will be stuck in-flight and never complete
      # which means the test will hung instead of failing. To prevent this, check that
      # no queries are in-flight at the end of the test, and force shutdown the
      # coordinator to force end the hung queries.
      def check_no_deadlock():
        queries_json = \
            self.cluster.get_first_impalad().service.get_debug_webpage_json('/queries')

        return len(queries_json["in_flight_queries"]) == 0

      if not retry(func=check_no_deadlock, max_attempts=60, sleep_time_s=0.5, backoff=1):
        for impalad in self.cluster.impalads:
          impalad.kill()

      for t in threads:
        t.join()
