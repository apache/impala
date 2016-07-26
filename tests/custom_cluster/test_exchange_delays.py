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

import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType

@SkipIfBuildType.not_dev_build
class TestExchangeDelays(CustomClusterTestSuite):
  """Tests for handling delays in finding data stream receivers"""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_datastream_recvr_delay_ms=10000"
        " --datastream_sender_timeout_ms=5000")
  def test_exchange_small_delay(self, vector):
    """Test delays in registering data stream receivers where the first one or two
    batches will time out before the receiver registers, but subsequent batches will
    arrive after the receiver registers. Before IMPALA-2987, this scenario resulted in
    incorrect results.
    """
    self.run_test_case('QueryTest/exchange-delays', vector)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_datastream_recvr_delay_ms=10000"
        " --datastream_sender_timeout_ms=1")
  def test_exchange_large_delay(self, vector):
    """Test delays in registering data stream receivers where all of the batches sent
    will time out before the receiver registers. Before IMPALA-2987, this scenario
    resulted in the query hanging.
    """
    self.run_test_case('QueryTest/exchange-delays', vector)
