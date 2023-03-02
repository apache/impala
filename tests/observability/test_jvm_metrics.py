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
from tests.common.impala_test_suite import ImpalaTestSuite


class TestJvmMetrics(ImpalaTestSuite):
  """Class for checking JVM Metrics."""

  def test_jvm_metrics(self):
    """The following metrics should exist and come from the JVM."""
    impalad = self.impalad_test_service
    # These metrics can't possibly be zero, so we assert that
    # they're positive.
    NON_ZERO_METRICS = [
      "jvm.heap.current-usage-bytes",
      "jvm.heap.max-usage-bytes",
      "jvm.non-heap.committed-usage-bytes",
      "jvm.total.current-usage-bytes",
    ]
    for m in NON_ZERO_METRICS:
      assert impalad.get_metric_value(m) > 0

    # One can imagine somehow there being no GCs, so do
    # a weaker assertion that they're non-negative and non-None.
    for m in ["jvm.gc_count", "jvm.gc_time_millis"]:
      assert impalad.get_metric_value(m) >= 0
