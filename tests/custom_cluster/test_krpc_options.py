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
import socket
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import DEFAULT_KRPC_PORT


class TestKrpcOptions(CustomClusterTestSuite):
  """Test for different options when using KRPC."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestKrpcOptions, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--rpc_use_loopback=true")
  def test_krpc_use_loopback(self, vector):
    """Sanity test for the --rpc_use_loopback flag."""
    # Run a query that will execute on multiple hosts.
    self.client.execute("select min(int_col) from functional_parquet.alltypes")

    # Check that we can connect on multiple interfaces.
    sock = socket.socket()
    sock.connect(("127.0.0.1", DEFAULT_KRPC_PORT))
    sock.close()
    sock = socket.socket()
    sock.connect((socket.gethostname(), DEFAULT_KRPC_PORT))
    sock.close()
