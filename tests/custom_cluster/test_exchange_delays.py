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
from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfBuildType
from tests.util.filesystem_utils import IS_S3, IS_ADLS, IS_ISILON, IS_OZONE, IS_ENCRYPTED

# IMPALA-6100: add additional margin for error for slow build types.
SLOW_BUILD_TIMEOUT=20000
DELAY_MS = build_flavor_timeout(10000, slow_build_timeout=SLOW_BUILD_TIMEOUT)
# IMPALA-6381: Isilon can behave as a slow build.
if IS_ISILON:
  DELAY_MS = SLOW_BUILD_TIMEOUT

@SkipIfBuildType.not_dev_build
class TestExchangeDelays(CustomClusterTestSuite):
  """Tests for handling delays in finding data stream receivers"""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "--stress_datastream_recvr_delay_ms={0}".format(DELAY_MS)
      + " --datastream_sender_timeout_ms=5000"
      + " --impala_slow_rpc_threshold_ms=500")
  def test_exchange_small_delay(self, vector):
    """Test delays in registering data stream receivers where the first one or two
    batches will time out before the receiver registers, but subsequent batches will
    arrive after the receiver registers. Before IMPALA-2987, this scenario resulted in
    incorrect results.
    """
    self.run_test_case('QueryTest/exchange-delays', vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "--stress_datastream_recvr_delay_ms={0}".format(DELAY_MS)
      + " --datastream_sender_timeout_ms=1"
      + " --impala_slow_rpc_threshold_ms=500")
  def test_exchange_large_delay(self, vector):
    """Test delays in registering data stream receivers where all of the batches sent
    will time out before the receiver registers. Before IMPALA-2987, this scenario
    resulted in the query hanging.
    """
    self.run_test_case('QueryTest/exchange-delays', vector)

  # The SQL used for test_exchange_large_delay_zero_rows requires that the scan complete
  # before the fragment sends the EOS message. A slow scan can cause this test to fail,
  # because the receivers could be set up before the fragment starts sending (and thus
  # can't time out). Use a longer delay for platforms that have slow scans:
  # IMPALA-6811: S3/ADLS have slow scans.
  # IMPALA-6866: Isilon has slow scans (and is counted as a slow build above).
  # Scans with Ozone encryption are also slower.
  SLOW_SCAN_EXTRA_DELAY_MS = 10000
  if IS_S3 or IS_ADLS or IS_ISILON or (IS_OZONE and IS_ENCRYPTED):
    DELAY_MS += SLOW_SCAN_EXTRA_DELAY_MS

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "--stress_datastream_recvr_delay_ms={0}".format(DELAY_MS)
      + " --datastream_sender_timeout_ms=1"
      + " --impala_slow_rpc_threshold_ms=500")
  def test_exchange_large_delay_zero_rows(self, vector):
    """Test the special case when no batches are sent and the EOS message times out."""
    self.run_test_case('QueryTest/exchange-delays-zero-rows', vector)
