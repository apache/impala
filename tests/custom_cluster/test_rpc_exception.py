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
from tests.common.skip import SkipIfBuildType

@SkipIfBuildType.not_dev_build
class TestRPCException(CustomClusterTestSuite):
  """Tests Impala exception handling in TransmitData() RPC to make sure no
     duplicated row batches are sent. """
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

  # Execute TEST_QUERY. If 'exception_string' is None, it's expected to complete
  # sucessfully with result matching EXPECTED_RESULT. Otherwise, it's expected
  # to fail with 'exception_string'.
  def execute_query(self, exception_string):
    try:
      result = self.client.execute(self.TEST_QUERY)
      assert result.data == self.EXPECTED_RESULT
      assert not exception_string
    except ImpalaBeeswaxException as e:
      assert exception_string in str(e)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--fault_injection_rpc_exception_type=1"
      " --fault_injection_rpc_type=5")
  def test_transmitdata_send_fail(self, vector):
    self.execute_query(None)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--fault_injection_rpc_exception_type=2"
      " --fault_injection_rpc_type=5")
  def test_transmitdata_recv_fail(self, vector):
    self.execute_query("Called read on non-open socket")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--fault_injection_rpc_exception_type=3"
      " --fault_injection_rpc_type=5")
  def test_transmitdata_secure_send_error(self, vector):
    self.execute_query("SSL_write: SSL resource temporarily unavailable")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--fault_injection_rpc_exception_type=4"
      " --fault_injection_rpc_type=5")
  def test_transmitdata_secure_recv_error(self, vector):
    self.execute_query("SSL_read: SSL resource temporarily unavailable")

