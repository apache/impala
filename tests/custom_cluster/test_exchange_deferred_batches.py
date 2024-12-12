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
import time
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType
from tests.util.cancel_util import cancel_query_and_validate_state


@SkipIfBuildType.not_dev_build
class TestExchangeDeferredBatches(CustomClusterTestSuite):

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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      " --exchg_node_buffer_size_bytes=32768"
      + " --datastream_service_num_deserialization_threads=16")
  def test_parallel_deserialization_buffer_limit(self, vector):
    """The test reproduces the scenario in IMPALA-13475 - delay is added to
    the receiver to cause most TransmitData RPCs to be deferred and buffer
    size is set to a small enough value where the queue can only hold 2 RowBatches
    which is not enough for a row batch from all 3 senders. This means that the
    receiver must limit the number of parallel deserialization tasks started to
    ensure that the queue has enough memory for the result of all started tasks."""

    TEST_QUERY = "select count(*) from tpch.lineitem t1, tpch.lineitem t2 " +\
        "where t1.l_orderkey = t2.l_orderkey"
    EXPECTED_RESULT = ['30012985']

    # There are ~1100 row batches per exchange node under the join, with 1ms
    # sleep they combine to guaranteed >2s run time.
    query_options = {'debug_action': 'RECVR_ADD_BATCH:SLEEP@1'}
    start = time.time()
    result = self.execute_query(TEST_QUERY, query_options=query_options)
    elapsed = time.time() - start
    assert result.data == EXPECTED_RESULT
    assert elapsed > 2, \
        "Query should take >2s due to RECVR_ADD_BATCH:SLEEP@1, got %.2fs" % elapsed

    # Test that the query can be cancelled at several points. The query takes around
    # 3sec due to the debug_action.
    for cancel_delay in [0.5, 1.0, 1.5, 2.0, 2.5]:
      cancel_query_and_validate_state(TEST_QUERY,
          query_options, vector.get_value('table_format'), cancel_delay)
