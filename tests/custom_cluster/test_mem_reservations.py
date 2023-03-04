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
import pytest
import threading

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import LOG
from tests.verifiers.metric_verifier import MetricVerifier

class TestMemReservations(CustomClusterTestSuite):
  """Tests for memory reservations that require custom cluster arguments."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--buffer_pool_limit=2g --memory_maintenance_sleep_time_ms=100")
  def test_per_backend_min_reservation(self, vector):
    """Tests that the per-backend minimum reservations are used (IMPALA-4833).
       The test sets the buffer_pool_limit very low (2gb), and then runs a query against
       two different coordinators. The query was created to have different minimum
       reservation requirements between the coordinator node and the backends. If the
       per-backend minimum reservations are not used, then one of the queries fails to
       acquire its minimum reservation. This was verified to fail before IMPALA-4833, and
       succeeds after.

       Memory maintenance sleep time is set low so we can verify that buffers are
       released.
    """
    assert len(self.cluster.impalads) == 3

    # This query will have scan fragments on all nodes, but the coordinator fragment
    # has 6 analytic nodes, 5 sort nodes, and an aggregation.
    COORDINATOR_QUERY = """
    select max(t.c1), avg(t.c2), min(t.c3), avg(c4), avg(c5), avg(c6)
    from (select
        max(tinyint_col) over (order by int_col) c1,
        avg(tinyint_col) over (order by smallint_col) c2,
        min(tinyint_col) over (order by smallint_col desc) c3,
        rank() over (order by int_col desc) c4,
        dense_rank() over (order by bigint_col) c5,
        first_value(tinyint_col) over (order by bigint_col desc) c6
        from functional.alltypes) t;
        """

    # This query has two grouping aggregations on each node.
    SYMMETRIC_QUERY = """
    select count(*)
    from (select distinct * from functional.alltypes) v"""

    # Set the DEFAULT_SPILLABLE_BUFFER_SIZE and MIN_SPILLABLE_BUFFER_SIZE to 64MiB.
    # so that for COORDINATOR_QUERY, the coordinator node requires ~1.2gb and the
    # other backends require ~200mb and for SYMMETRIC_QUERY all backends require
    # ~1.05gb.
    CONFIG_MAP = {'DEFAULT_SPILLABLE_BUFFER_SIZE': '67108864',
                  'MIN_SPILLABLE_BUFFER_SIZE': '67108864'}

    class QuerySubmitThread(threading.Thread):
      def __init__(self, query, coordinator):
        super(QuerySubmitThread, self).__init__()
        self.query = query
        self.coordinator = coordinator
        self.error = None

      def run(self):
        client = self.coordinator.service.create_beeswax_client()
        try:
          client.set_configuration(CONFIG_MAP)
          for i in range(20):
            result = client.execute(self.query)
            assert result.success
            assert len(result.data) == 1
        except Exception as e:
          self.error = str(e)
        finally:
          client.close()

    # Create two threads to submit COORDINATOR_QUERY to two different coordinators concurrently.
    # They should both succeed.
    threads = [QuerySubmitThread(COORDINATOR_QUERY, self.cluster.impalads[i])
              for i in range(2)]
    for t in threads: t.start()
    for t in threads:
      t.join()
      assert t.error is None

    # Create two threads to submit COORDINATOR_QUERY to one coordinator and
    # SYMMETRIC_QUERY to another coordinator. One of the queries should fail because
    # memory would be overcommitted on daemon 0.
    threads = [QuerySubmitThread(COORDINATOR_QUERY, self.cluster.impalads[0]),
               QuerySubmitThread(SYMMETRIC_QUERY, self.cluster.impalads[1])]
    for t in threads: t.start()
    num_errors = 0
    for t in threads:
      t.join()
      if t.error is not None:
        assert "Failed to get minimum memory reservation" in t.error
        LOG.info("Query failed with error: %s", t.error)
        LOG.info(t.query)
        num_errors += 1
    assert num_errors == 1

    # Check that free buffers are released over time. We set the memory maintenance sleep
    # time very low above so this should happen quickly.
    verifiers = [MetricVerifier(i.service) for i in self.cluster.impalads]
    for v in verifiers:
      v.wait_for_metric("buffer-pool.free-buffers", 0, timeout=60)
      v.wait_for_metric("buffer-pool.free-buffer-bytes", 0, timeout=60)
