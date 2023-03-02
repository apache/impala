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

from tests.common.environ import IS_DOCKERIZED_TEST_CLUSTER
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.util.filesystem_utils import IS_EC, IS_HDFS, IS_ENCRYPTED


class TestIOMetrics(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIOMetrics, cls).add_test_dimensions()
    # Run with num_nodes=1 to make it easy to verify metric changes.
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension(num_nodes=1))

  # Issue a local query and test that read metrics are updated.
  @pytest.mark.execute_serially
  def test_local_read(self, vector):
    # Accumulate metrics that are expected to update from a read, and metrics that are
    # expected not to change with this configuration. Metrics that shouldn't change for
    # this test should be 0 throughout the whole test suite so we can just verify they're
    # 0 after running our query. Omits cached-bytes-read because it has its own test.
    expect_nonzero_metrics = ["impala-server.io-mgr.bytes-read"]
    expect_zero_metrics = []

    def append_metric(metric, expect_nonzero):
      (expect_nonzero_metrics if expect_nonzero else expect_zero_metrics).append(metric)

    append_metric("impala-server.io-mgr.encrypted-bytes-read", IS_ENCRYPTED)
    append_metric("impala-server.io-mgr.erasure-coded-bytes-read", IS_EC)
    append_metric("impala-server.io-mgr.short-circuit-bytes-read",
        IS_HDFS and not IS_DOCKERIZED_TEST_CLUSTER)
    append_metric("impala-server.io-mgr.local-bytes-read",
        IS_HDFS and not IS_DOCKERIZED_TEST_CLUSTER)

    nonzero_before = self.impalad_test_service.get_metric_values(expect_nonzero_metrics)

    result = self.execute_query("select count(*) from tpch.nation")
    assert(len(result.data) == 1)
    assert(result.data[0] == '25')
    nation_data_file_length = 2199

    nonzero_after = self.impalad_test_service.get_metric_values(expect_nonzero_metrics)

    zero_values = self.impalad_test_service.get_metric_values(expect_zero_metrics)
    assert(len(expect_zero_metrics) == len(zero_values))
    LOG.info("Verifying %s expect-zero metrics.", len(expect_zero_metrics))
    for metric, value in zip(expect_zero_metrics, zero_values):
      LOG.info("%s: %s", metric, value)
      assert(value == 0)

    assert(len(expect_nonzero_metrics) == len(nonzero_before) == len(nonzero_after))
    LOG.info("Verifying %s expect-non-zero metrics.", len(expect_nonzero_metrics))
    for metric, before, after in \
        zip(expect_nonzero_metrics, nonzero_before, nonzero_after):
      LOG.info("%s: %s -> %s", metric, before, after)
      assert(before + nation_data_file_length == after)
