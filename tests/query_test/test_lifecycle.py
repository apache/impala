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
import pytest
import time
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.verifiers.metric_verifier import MetricVerifier

# TODO: Debug actions leak into other tests in the same suite (if not explicitly
# unset). Ensure they get unset between tests.
class TestFragmentLifecycleWithDebugActions(ImpalaTestSuite):
  """Using the debug action interface, check that failed queries correctly clean up *all*
  fragments"""

  IN_FLIGHT_FRAGMENTS = "impala-server.num-fragments-in-flight"
  @classmethod
  def get_workload(self):
    return 'functional'

  @pytest.mark.execute_serially
  def test_failure_in_prepare(self):
    # Fail the scan node
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]
    self.client.execute("SET DEBUG_ACTION='-1:0:PREPARE:FAIL'");
    try:
      self.client.execute("SELECT COUNT(*) FROM functional.alltypes")
      assert "Query should have thrown an error"
    except ImpalaBeeswaxException:
      pass

    for v in verifiers:
      v.wait_for_metric(self.IN_FLIGHT_FRAGMENTS, 0)

  @pytest.mark.execute_serially
  def test_failure_in_prepare_multi_fragment(self):
    # Test that if one fragment fails that the others are cleaned up during the ensuing
    # cancellation.
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]
    # Fail the scan node
    self.client.execute("SET DEBUG_ACTION='-1:0:PREPARE:FAIL'");

    # Force a query plan that will have three fragments or more.
    try:
      self.client.execute("SELECT COUNT(*) FROM functional.alltypes a JOIN [SHUFFLE] \
        functional.alltypes b on a.id = b.id")
      assert "Query should have thrown an error"
    except ImpalaBeeswaxException:
      pass

    for v in verifiers:
      # Long timeout required because fragments may be blocked while sending data. The
      # default value of --datastream_sender_timeout_ms is 120s before they wake up and
      # cancel themselves.
      #
      # TODO: Fix when we have cancellable RPCs.
      v.wait_for_metric(self.IN_FLIGHT_FRAGMENTS, 0, timeout=125)

class TestFragmentLifecycle(ImpalaTestSuite):
  def test_finst_cancel_when_query_complete(self):
    """Regression test for IMPALA-4295: if a query returns all its rows before all its
    finsts have completed, it should cancel the finsts and complete promptly."""
    now = time.time()

    # Query designed to produce 1024 (the limit) rows very quickly from the first union
    # child, but the second one takes a very long time to complete. Without fix for
    # IMPALA-4295, the whole query waits for the second child to complete.

    # Due to IMPALA-5671, the limit must be a multiple of the row batch size - if it's
    # reached during production of a row batch, processing moves to the second child, and
    # the query will take a long time complete.
    self.client.execute("with l as (select 1 from functional.alltypes), r as"
      " (select count(*) from tpch_parquet.lineitem a cross join tpch_parquet.lineitem b)"
      "select * from l union all (select * from r) LIMIT 1024")
    end = time.time()

    # Query typically completes in < 2s, but if cross join is fully evaluated, will take >
    # 10 minutes. Pick 2 minutes as a reasonable midpoint to avoid false negatives.
    assert end - now < 120, "Query took too long to complete: " + duration + "s"
