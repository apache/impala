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
import tempfile
import socket
import pexpect
import os

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_service import ImpaladService
from tests.common.test_vector import ImpalaTestVector
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import ImpalaShell, get_shell_cmd, get_impalad_port, spawn_shell
# Follow tests/shell/test_shell_interactive.py naming.
from shell.impala_shell import ImpalaShell as ImpalaShellClass
from tests.verifiers.metric_verifier import MetricVerifier

NUM_QUERIES = 'impala-server.num-queries'

class TestShellInteractiveReconnect(CustomClusterTestSuite):
  """ Check if interactive shell is using the current DB after reconnecting """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_manual_reconnect(self):
    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      p = ImpalaShell(vector)
      p.send_cmd("USE functional")
      # Connect without arguments works because the custom cluster will have the default
      # HS2 and Beeswax ports.
      p.send_cmd("CONNECT")
      p.send_cmd("SHOW TABLES")

      result = p.get_result()
      assert "alltypesaggmultifilesnopart" in result.stdout, result.stdout

  @pytest.mark.execute_serially
  def test_auto_reconnect(self):
    impalad = ImpaladService(socket.getfqdn())

    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      p = ImpalaShell(vector)
      # ImpalaShell startup may issue query to get server info - get num queries after
      # starting shell.
      start_num_queries = impalad.get_metric_value(NUM_QUERIES)
      p.send_cmd("USE functional")

      # wait for the USE command to finish
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 1)
      assert impalad.wait_for_num_in_flight_queries(0)

      self._start_impala_cluster([])

      p.send_cmd("SHOW TABLES")
      result = p.get_result()
      assert "alltypesaggmultifilesnopart" in result.stdout, result.stdout

  @pytest.mark.execute_serially
  def test_auto_reconnect_after_impalad_died(self):
    """Test reconnect after restarting the remote impalad without using connect;"""
    # Use pexpect instead of ImpalaShell() since after using get_result() in ImpalaShell()
    # to check Disconnect, send_cmd() will no longer have any effect so we can not check
    # reconnect.
    impalad = ImpaladService(socket.getfqdn())

    # Iterate over test vector within test function to avoid restarting cluster.
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      proc = spawn_shell(get_shell_cmd(vector))
      proc.expect("{0}] default>".format(get_impalad_port(vector)))
      # ImpalaShell startup may issue query to get server info - get num queries after
      # starting shell.
      start_num_queries = impalad.get_metric_value(NUM_QUERIES)
      proc.sendline("use tpch;")

      # wait for the USE command to finish
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 1)
      assert impalad.wait_for_num_in_flight_queries(0)

      # Disconnect
      self.cluster.impalads[0].kill()
      proc.sendline("show tables;")
      # Search from [1:] since the square brackets "[]" are special characters in regex
      proc.expect(ImpalaShellClass.DISCONNECTED_PROMPT[1:])
      # Restarting Impalad
      self.cluster.impalads[0].start()
      # Check reconnect
      proc.sendline("show tables;")
      proc.expect("nation")
      proc.expect("{0}] tpch>".format(get_impalad_port(vector)))
      proc.sendeof()
      proc.wait()

      # Ensure no sessions or queries are left dangling.
      verifier = MetricVerifier(self.impalad_test_service)
      verifier.verify_metrics_are_zero()
