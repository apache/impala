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
from tests.util.shell_util import exec_process


class TestKrpcSocket(CustomClusterTestSuite):
  """Test for different types of socket used by KRPC."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--rpc_use_unix_domain_socket=false")
  def test_krpc_use_tcp_socket(self, vector):
    """Sanity test for KRPC running over TCP socket."""

    # Run a query that will execute on multiple hosts.
    self.execute_query_expect_success(self.client,
        "select min(int_col) from functional_parquet.alltypes", vector=vector)

    # Find the listening TCP ports via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lnt")
    assert rc == 0, "Error finding listening TCP ports\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify port number DEFAULT_KRPC_PORT, DEFAULT_KRPC_PORT+1, and DEFAULT_KRPC_PORT+2
    # are in the list.
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT)) == 1)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 1)) == 1)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 2)) == 1)

    # Find the listening Unix sockets via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lx")
    assert rc == 0, "Error finding Unix sockets\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify that KRPC are not binding to Unix domain socket.
    assert(netstat_output.count("@impala-krpc") == 0)

    # Check that we can connect on TCP port of KRPC.
    sock = socket.socket()
    sock.connect(("localhost", DEFAULT_KRPC_PORT))
    sock.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--rpc_use_unix_domain_socket=true")
  def test_krpc_use_unix_domain_socket(self, vector):
    """Sanity test for KRPC running over Unix domain socket.
    Use IP address as the unique Id for UDS address.
    """

    # Run a query that will execute on multiple hosts.
    self.execute_query_expect_success(self.client,
        "select min(int_col) from functional_parquet.alltypes", vector=vector)

    # Find the listening TCP ports via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lnt")
    assert rc == 0, "Error finding listening TCP port\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify port number DEFAULT_KRPC_PORT, DEFAULT_KRPC_PORT+1, and DEFAULT_KRPC_PORT+2
    # are not in the list.
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT)) == 0)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 1)) == 0)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 2)) == 0)

    # Find the listening Unix sockets via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lx")
    assert rc == 0, "Error finding Unix sockets\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify that KRPC are binding to Unix domain socket.
    assert(netstat_output.count("@impala-krpc") == 3)

    # Not try to connect to Unix domain socket of KRPC since "Abstract Namespace"
    # may be not supported by the Python socket.

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--rpc_use_unix_domain_socket=true --uds_address_unique_id=backend_id")
  def test_krpc_uds_address_backend_id(self, vector):
    """Sanity test for KRPC running over Unix domain socket.
    Use BackendId as the unique Id for UDS address.
    """

    # Run a query that will execute on multiple hosts.
    self.execute_query_expect_success(self.client,
        "select min(int_col) from functional_parquet.alltypes", vector=vector)

    # Find the listening TCP ports via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lnt")
    assert rc == 0, "Error finding listening TCP port\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify port number DEFAULT_KRPC_PORT, DEFAULT_KRPC_PORT+1, and DEFAULT_KRPC_PORT+2
    # are not in the list.
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT)) == 0)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 1)) == 0)
    assert(netstat_output.count(":{0}".format(DEFAULT_KRPC_PORT + 2)) == 0)

    # Find the listening Unix sockets via netstat.
    rc, netstat_output, stderr = exec_process("netstat -lx")
    assert rc == 0, "Error finding Unix sockets\nstdout={0}\nstderr={1}".format(
        netstat_output, stderr)
    # Verify that KRPC are binding to Unix domain socket.
    assert(netstat_output.count("@impala-krpc") == 3)
