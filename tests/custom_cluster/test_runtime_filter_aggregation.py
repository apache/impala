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
import math
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout, ImpalaTestClusterProperties
from tests.common.test_dimensions import (
  add_mandatory_exec_option,
  add_exec_option_dimension
)

# slow_build_timeout is set to 200000 to avoid failures like IMPALA-8064 where the
# runtime filters don't arrive in time.
WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=200000)

# Check whether the Impala under test in slow build. Query option ASYNC_CODEGEN will
# be enabled when test runs for slow build like ASAN, TSAN, UBSAN, etc. This avoid
# failures like IMPALA-9889 where the runtime filters don't arrive in time due to
# the slowness of codegen.
build_runs_slowly = ImpalaTestClusterProperties.get_instance().runs_slowly()


class TestRuntimeFilterAggregation(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRuntimeFilterAggregation, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'max_num_filters_aggregated_per_host', 2)
    # Exercise small, non-fatal jitters.
    add_exec_option_dimension(
      cls, 'debug_action', ['', 'QUERY_STATE_BEFORE_INIT_GLOBAL:JITTER@200]'])
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(cluster_size=6, num_exclusive_coordinators=1)
  def test_basic_filters(self, vector):
    num_filters_per_host = vector.get_exec_option('max_num_filters_aggregated_per_host')
    num_backend = 5  # exclude coordinator
    num_updates = (num_backend if num_filters_per_host == 0
        else int(math.ceil(num_backend / num_filters_per_host)))
    vars = {
      '$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS),
      '$NUM_FILTER_UPDATES': str(num_updates)
    }
    self.run_test_case('QueryTest/runtime_filters', vector, test_file_vars=vars)
    self.run_test_case('QueryTest/bloom_filters', vector)


class TestLateQueryStateInit(CustomClusterTestSuite):
  """Test that distributed runtime filter aggregation still works
  when remote query state of intermediate aggregator node is late to initialize."""
  _wait_time = WAIT_TIME_MS // 20
  _init_delay = [100, _wait_time]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLateQueryStateInit, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'max_num_filters_aggregated_per_host', 2)
    add_mandatory_exec_option(cls, 'runtime_filter_wait_time_ms', cls._wait_time)
    # Inject sleep in second impalad since sort_runtime_filter_aggregator_cadidate=true
    # and the first one (coordinator) will never be selected as intermediate aggregator.
    actions = ["QUERY_STATE_BEFORE_INIT:27001:SLEEP@{0}".format(d) for d in
        cls._init_delay]
    add_exec_option_dimension(cls, 'debug_action', actions)
    # Enable query option ASYNC_CODEGEN for slow build
    if build_runs_slowly:
      add_mandatory_exec_option(cls, "async_codegen", 1)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--sort_runtime_filter_aggregator_candidates=true --logbuflevel=-1")
  def test_late_query_state_init(self, vector):
    """Test that distributed runtime filter aggregation still works
    when remote query state of intermediate aggregator node is late to initialize."""
    query = ('select count(*) from functional.alltypes p '
             'join [SHUFFLE] functional.alltypestiny b '
             'on p.month = b.int_col and b.month = 1 and b.string_col = "1"')
    exec_options = vector.get_value('exec_option')
    result = self.execute_query_expect_success(self.client, query, exec_options)
    assert result.data[0] == '620'

    # Expect no log printed in short init delay scenario.
    # In long init delay scenario, two possible situations can happen:
    # 1. The build scanner assigned in first impalad exchange all build rows (1) to
    #    the second impalad that is blocked at QueryExecMgr::StartQuery. This indirectly
    #    delay all impalad because all JOIN BUILD fragment need to wait for EOS signal
    #    from exchange sender. The probability for this case is 1/3.
    # 2. The build scanner exchange all build rows to other impalads than the second one.
    #    The JOIN BUILD fragment in first and third impalads immediately receive EOS
    #    signal from exchange sender, complete build, and send their filter update to
    #    the second impalad. The second impalad stay blocked at QueryExecMgr::StartQuery
    #    and filter update need to wait until it gives up. The probability for this case
    #    is 2/3.
    expected = -1 if str(self._init_delay[-1]) in exec_options['debug_action'] else 0
    all_blocked = 'UpdateFilterFromRemote RPC called with remaining wait time'
    preagg_blocked = 'QueryState for query_id={0} no'.format(result.query_id)
    log_pattern = '({0}|{1})'.format(all_blocked, preagg_blocked)
    if expected == -1:
      if 'Filter 0 inflight for final aggregation' in result.runtime_profile:
        log_pattern = all_blocked  # case 1.
      else:
        log_pattern = preagg_blocked  # case 2.
    self.assert_log_contains('impalad_node1', 'INFO', log_pattern, expected)
