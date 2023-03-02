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
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS


class TestRuntimeProfile(CustomClusterTestSuite):
  """Tests for non-default runtime profile behaviour."""

  @classmethod
  def get_workload(self):
    return 'tpch'

  PERIODIC_COUNTER_UPDATE_FLAG = '--periodic_counter_update_period_ms=50'

  # Test depends on block size < 256MiB so larger table is stored in at least 4 blocks.
  @SkipIfFS.large_block_size
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args('--gen_experimental_profile=true ' +
      PERIODIC_COUNTER_UPDATE_FLAG)
  def test_runtime_profile_aggregated(self, vector):
    """Sanity-check the new experimental profile format.
    --periodic_counter_update_period_ms is decreased to get more samples for time series
    counters."""
    # Add an executor with the flag disabled. The aggregated profile should still be
    # generated correctly
    self._start_impala_cluster(options=['--impalad_args=' +
                                        self.PERIODIC_COUNTER_UPDATE_FLAG],
                               cluster_size=1,
                               num_coordinators=0,
                               add_executors=True,
                               expected_num_impalads=4)
    self.run_test_case('runtime-profile-aggregated', vector)
