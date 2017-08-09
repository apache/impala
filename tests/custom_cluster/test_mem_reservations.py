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

import pytest
import threading

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestMemReservations(CustomClusterTestSuite):
  """Tests for memory reservations that require custom cluster arguments."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--buffer_pool_limit=2g")
  def test_per_backend_min_reservation(self, vector):
    """Tests that the per-backend minimum reservations are used (IMPALA-4833).
       The test sets the buffer_pool_limit very low (2gb), and then runs a query against
       two different coordinators. The query was created to have different minimum
       reservation requirements between the coordinator node and the backends. If the
       per-backend minimum reservations are not used, then one of the queries fails to
       acquire its minimum reservation. This was verified to fail before IMPALA-4833, and
       succeeds after.
    """
    assert len(self.cluster.impalads) == 3

    # This query will have scan fragments on all nodes, but the coordinator fragment
    # has 6 analytic nodes, 5 sort nodes, and an aggregation.
    QUERY = """
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

    # Set the DEFAULT_SPILLABLE_BUFFER_SIZE and MIN_SPILLABLE_BUFFER_SIZE to 64MiB
    # so that the coordinator node requires ~1.2gb and the other backends require ~200mb.
    CONFIG_MAP = {'DEFAULT_SPILLABLE_BUFFER_SIZE': '67108864',
                  'MIN_SPILLABLE_BUFFER_SIZE': '67108864'}

    # Create two threads to submit QUERY to two different coordinators concurrently.
    class QuerySubmitThread(threading.Thread):
      def __init__(self, coordinator):
        super(QuerySubmitThread, self).__init__()
        self.coordinator = coordinator
        self.error = None

      def run(self):
        client = self.coordinator.service.create_beeswax_client()
        try:
          client.set_configuration(CONFIG_MAP)
          for i in xrange(20):
            result = client.execute(QUERY)
            assert result.success
            assert len(result.data) == 1
        except Exception, e:
          self.error = str(e)
        finally:
          client.close()

    threads = [QuerySubmitThread(self.cluster.impalads[i]) for i in xrange(2)]
    for t in threads: t.start()
    for t in threads:
      t.join()
      assert t.error is None
