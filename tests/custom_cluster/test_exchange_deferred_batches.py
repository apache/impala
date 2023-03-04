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
from builtins import range
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType


@SkipIfBuildType.not_dev_build
class TestExchangeDeferredBatches(CustomClusterTestSuite):

    @classmethod
    def get_workload(cls):
      return 'functional-query'

    @classmethod
    def setup_class(cls):
      if cls.exploration_strategy() != 'exhaustive':
        pytest.skip('runs only in exhaustive')
      super(TestExchangeDeferredBatches, cls).setup_class()

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(
        "--stress_datastream_recvr_delay_ms=3000"
        + " --exchg_node_buffer_size_bytes=1024"
        + " --datastream_service_num_deserialization_threads=1"
        + " --impala_slow_rpc_threshold_ms=500")
    def test_exchange_small_buffer(self, vector):
        """Exercise the code which handles deferred row batches. In particular,
        the exchange buffer is set to a small value to cause incoming row batches
        to be deferred at the receiver. Also, use a single deserialization thread
        to limit the speed in which the deferred row batches are dequeued. These
        settings help expose the race in IMPALA-8239 when there is any error
        deserializing deferred row batches."""

        TEST_QUERY = "select count(*) from tpch.lineitem t1, tpch.lineitem t2 " +\
           "where t1.l_orderkey = t2.l_orderkey"
        EXPECTED_RESULT = ['30012985']

        for i in range(10):
            # Simulate row batch insertion failure. This triggers IMPALA-8239.
            debug_action = 'RECVR_ADD_BATCH:FAIL@0.8'
            self.execute_query_expect_failure(self.client, TEST_QUERY,
                query_options={'debug_action': debug_action})

        for i in range(10):
            # Simulate row batch insertion failure. This triggers IMPALA-8239.
            debug_action = 'RECVR_UNPACK_PAYLOAD:FAIL@0.8'
            self.execute_query_expect_failure(self.client, TEST_QUERY,
                query_options={'debug_action': debug_action})

        # Do a run with no debug action to make sure things are sane.
        result = self.execute_query(TEST_QUERY, vector.get_value('exec_option'))
        assert result.data == EXPECTED_RESULT
