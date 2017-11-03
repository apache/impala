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
import tempfile
import socket

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_service import ImpaladService
from tests.common.skip import SkipIfBuildType
from tests.shell.util import ImpalaShell, move_shell_history, restore_shell_history

class TestShellInteractiveReconnect(CustomClusterTestSuite):
  """ Check if interactive shell is using the current DB after reconnecting """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    super(TestShellInteractiveReconnect, cls).setup_class()

    cls.tempfile_name = tempfile.mktemp()
    move_shell_history(cls.tempfile_name)

  @classmethod
  def teardown_class(cls):
    restore_shell_history(cls.tempfile_name)
    super(TestShellInteractiveReconnect, cls).teardown_class()

  @pytest.mark.execute_serially
  def test_manual_reconnect(self):
    p = ImpalaShell()
    p.send_cmd("USE functional")
    p.send_cmd("CONNECT")
    p.send_cmd("SHOW TABLES")

    result = p.get_result()
    assert "alltypesaggmultifilesnopart" in result.stdout

  @pytest.mark.execute_serially
  def test_auto_reconnect(self):
    NUM_QUERIES = 'impala-server.num-queries'

    impalad = ImpaladService(socket.getfqdn())
    start_num_queries = impalad.get_metric_value(NUM_QUERIES)

    p = ImpalaShell()
    p.send_cmd("USE functional")

    # wait for the USE command to finish
    impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 1)
    impalad.wait_for_num_in_flight_queries(0)

    self._start_impala_cluster([])

    p.send_cmd("SHOW TABLES")
    result = p.get_result()
    assert "alltypesaggmultifilesnopart" in result.stdout

