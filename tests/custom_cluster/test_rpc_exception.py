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
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf, SkipIfBuildType

@SkipIfBuildType.not_dev_build
class TestRPCException(CustomClusterTestSuite):
  """Tests Impala exception handling in TransmitData() RPC to make sure no
     duplicated row batches are sent. """
  # DataStreamService rpc names
  TRANSMIT_DATA_RPC = "TransmitData"
  END_DATA_STREAM_RPC = "EndDataStream"

  # Error to specify for ImpalaServicePool to reject rpcs with a 'server too busy' error.
  REJECT_TOO_BUSY_MSG = "REJECT_TOO_BUSY"

  # The BE krpc port of the impalad these tests simulate rpc errors at.
  KRPC_PORT = 27002

  # This query ends up calling TransmitData() more than 2048 times to ensure
  # proper test coverage.
  TEST_QUERY = "select count(*) from tpch_parquet.lineitem t1, tpch_parquet.lineitem t2 \
      where t1.l_orderkey = t2.l_orderkey"
  EXPECTED_RESULT = ['30012985']

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestRPCException, cls).setup_class()

  def _get_num_fails(self, impalad):
    num_fails = impalad.service.get_metric_value("impala.debug_action.fail")
    if num_fails is None:
      return 0
    return num_fails

  # Execute TEST_QUERY repeatedly until the FAIL debug action has been hit. If
  # 'exception_string' is None, it's expected to always complete sucessfully with result
  # matching EXPECTED_RESULT. Otherwise, it's expected to either succeed or fail with
  # the error 'exception_string'.
  def execute_test_query(self, exception_string):
    impalad = self.cluster.impalads[2]
    assert impalad.service.krpc_port == self.KRPC_PORT
    # Re-run the query until the metrics show that we hit the debug action or we've run 10
    # times. Each test in this file has at least a 50% chance of hitting the action per
    # run, so there's at most a (1/2)^10 chance that this loop will fail spuriously.
    i = 0
    while self._get_num_fails(impalad) == 0 and i < 10:
      i += 1
      try:
        result = self.client.execute(self.TEST_QUERY)
        assert result.data == self.EXPECTED_RESULT, "Query returned unexpected results."
      except ImpalaBeeswaxException as e:
        if exception_string is None:
          raise e
        assert exception_string in str(e), "Query failed with unexpected exception."
    assert self._get_num_fails(impalad) > 0, "Debug action wasn't hit after 10 iters."

  def _get_fail_action(rpc, error=None, port=KRPC_PORT, p=0.1):
    """Returns a debug action that causes rpcs with the name 'rpc' that are sent to the
    impalad at 'port' to FAIL with probability 'p' and return 'error' if specified."""
    debug_action = "IMPALA_SERVICE_POOL:127.0.0.1:{port}:{rpc}:FAIL@{probability}" \
        .format(rpc=rpc, probability=p, port=port)
    if error is not None:
      debug_action += "@" + error
    return debug_action

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--debug_actions=" +
      _get_fail_action(rpc=TRANSMIT_DATA_RPC, error=REJECT_TOO_BUSY_MSG))
  def test_transmit_data_retry(self):
    """Run a query where TransmitData may fail with a "server too busy" error. We should
    always retry in this case, so the query should always eventually succeed."""
    self.execute_test_query(None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--debug_actions=" +
      _get_fail_action(rpc=TRANSMIT_DATA_RPC, error=REJECT_TOO_BUSY_MSG) +
      "|" + _get_fail_action(rpc=TRANSMIT_DATA_RPC))
  def test_transmit_data_error(self):
    """Run a query where TransmitData may fail with a "server too busy" or with a generic
    error. The query should either succeed or fail with the given error."""
    self.execute_test_query("Debug Action: IMPALA_SERVICE_POOL:FAIL@0.1")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--debug_actions=" +
      _get_fail_action(rpc=END_DATA_STREAM_RPC, error=REJECT_TOO_BUSY_MSG, p=0.5))
  def test_end_data_stream_retry(self):
    """Run a query where EndDataStream may fail with a "server too busy" error. We should
    always retry in this case, so the query should always eventually succeed."""
    self.execute_test_query(None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--debug_actions=" +
      _get_fail_action(rpc=END_DATA_STREAM_RPC, error=REJECT_TOO_BUSY_MSG, p=0.5) +
      "|" + _get_fail_action(rpc=END_DATA_STREAM_RPC, p=0.5))
  def test_end_data_stream_error(self):
    """Run a query where EndDataStream may fail with a "server too busy" or with a generic
    error. The query should either succeed or fail with the given error."""
    self.execute_test_query("Debug Action: IMPALA_SERVICE_POOL:FAIL@0.5")
