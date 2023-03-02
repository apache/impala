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

from decimal import Decimal

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfBuildType
from tests.comparison.cluster import MiniCluster
from tests.util.parse_util import (
    EXPECTED_TPCDS_QUERIES_COUNT, EXPECTED_TPCH_NESTED_QUERIES_COUNT,
    EXPECTED_TPCH_STRESS_QUERIES_COUNT, match_memory_estimate)
from tests.stress.concurrent_select import load_tpc_queries
from tests.util.filesystem_utils import IS_LOCAL


class TestStressInfra(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_stress_binary_search_start_point(self):
    """
    Test that the stress test can use EXPLAIN to find the start point for its binary
    search.
    """
    result = self.client.execute("explain select 1")
    mem_limit, units = match_memory_estimate(result.data)
    assert isinstance(units, str) and units.upper() in ('T', 'G', 'M', 'K', ''), (
        'unexpected units {u} from explain memory estimation\n{output}:'.format(
            u=units, output='\n'.join(result.data)))
    assert Decimal(mem_limit) >= 0, (
        'unexpected value from explain\n:' + '\n'.join(result.data))

  @pytest.mark.parametrize(
      'count_map',
      [('tpcds', EXPECTED_TPCDS_QUERIES_COUNT),
       ('tpch_nested', EXPECTED_TPCH_NESTED_QUERIES_COUNT),
       ('tpch', EXPECTED_TPCH_STRESS_QUERIES_COUNT)])
  def test_stress_finds_workloads(self, count_map):
    """
    Test that the stress test will properly load TPC workloads.
    """
    workload, count = count_map
    queries = load_tpc_queries(workload)
    assert count == len(queries)
    for query in queries:
      assert query.name is not None

  @SkipIfBuildType.remote
  def tests_minicluster_obj(self):
    """
    Test that the minicluster abstraction finds the minicluster.
    """
    if self.exploration_strategy() == "exhaustive":
      pytest.skip("Test does not need to run in exhaustive")
    cluster = MiniCluster()
    if IS_LOCAL:
      expected_pids = 1
    else:
      expected_pids = 3
    assert expected_pids == len(cluster.impala.for_each_impalad(lambda i: i.find_pid()))
