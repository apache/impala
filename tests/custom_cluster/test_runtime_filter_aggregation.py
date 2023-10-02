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
from tests.common.test_dimensions import add_mandatory_exec_option

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
