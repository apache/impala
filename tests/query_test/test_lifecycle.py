# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
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

import pytest
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.verifiers.metric_verifier import MetricVerifier

class TestFragmentLifecycle(ImpalaTestSuite):
  """Using the debug action interface, check that failed queries correctly clean up *all*
  fragments"""

  @classmethod
  def get_workload(self):
    return 'functional'

  @pytest.mark.execute_serially
  def test_failure_in_prepare(self):
    # Fail the scan node
    self.client.execute("SET DEBUG_ACTION='-1:0:PREPARE:FAIL'");
    try:
      self.client.execute("SELECT COUNT(*) FROM functional.alltypes")
      assert "Query should have thrown an error"
    except ImpalaBeeswaxException:
      pass
    verifiers = [ MetricVerifier(i.service) for i in ImpalaCluster().impalads ]

    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0)

  @pytest.mark.execute_serially
  def test_failure_in_prepare_multi_fragment(self):
    # Test that if one fragment fails that the others are cleaned up during the ensuing
    # cancellation.

    # Fail the scan node
    self.client.execute("SET DEBUG_ACTION='-1:0:PREPARE:FAIL'");

    # Force a query plan that will have three fragments or more.
    try:
      self.client.execute("SELECT COUNT(*) FROM functional.alltypes a JOIN [SHUFFLE] \
        functional.alltypes b on a.id = b.id")
      assert "Query should have thrown an error"
    except ImpalaBeeswaxException:
      pass

    verifiers = [ MetricVerifier(i.service) for i in ImpalaCluster().impalads ]
    for v in verifiers:
      # Long timeout required because fragments may be blocked while sending data, default
      # timeout is 60s before they wake up and cancel themselves.
      #
      # TODO: Fix when we have cancellable RPCs.
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0, timeout=65)
