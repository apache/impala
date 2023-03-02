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

# This module attempts to enforce infrastructural assumptions that bind test tools to
# product or other constraints. We want to stop these assumptions from breaking at
# pre-merge time, not later.

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.performance.workload import Workload
from tests.util.parse_util import (
    EXPECTED_TPCDS_QUERIES_COUNT, EXPECTED_TPCH_NESTED_QUERIES_COUNT,
    EXPECTED_TPCH_QUERIES_COUNT)


class TestPerfInfra(ImpalaTestSuite):

  @pytest.mark.parametrize(
      'count_map',
      [('tpcds', EXPECTED_TPCDS_QUERIES_COUNT, []),
       ('tpch_nested', EXPECTED_TPCH_NESTED_QUERIES_COUNT, []),
       ('tpch', EXPECTED_TPCH_QUERIES_COUNT, []),
       ("tpch", 1, ["TPCH-Q1"]),
       ("tpch", 12, ["TPCH-Q1.*", "TPCH-Q4"])])
  def test_run_workload_finds_queries(self, count_map):
    "Test that the perf tests select the expected number of queries to run."
    workload, num_expected, query_name_filters = count_map
    w = Workload(workload, query_name_filters)
    assert len(w._query_map) == num_expected
