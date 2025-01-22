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

from __future__ import absolute_import
import os
from subprocess import check_output

from tests.common.base_test_suite import BaseTestSuite


class TestRunWorkload(BaseTestSuite):

  def test_run_workload(self):
    """Test that bin/run-workload.py still works."""
    impala_home = os.getenv('IMPALA_HOME')
    cmd = [
      os.path.join(impala_home, 'bin/run-workload.py'), '-w', 'tpch', '--num_clients=2',
      '--query_names=TPCH-Q1', '--table_format=text/none',
      '--exec_options=disable_codegen:False']
    kerberos_arg = os.getenv('KERB_ARGS')
    if kerberos_arg is not None:
      cmd.append(kerberos_arg)
    output = check_output(cmd, universal_newlines=True)

    """
    Full stdout is like this:

    Workload: TPCH, Scale Factor:

    Table Format: text/none/none
    +---------+---------------------+----------------+-----------+
    | Query   | Start Time          | Time Taken (s) | Client ID |
    +---------+---------------------+----------------+-----------+
    | TPCH-Q1 | 2025-01-27 15:40:28 | 5.59           | 1         |
    | TPCH-Q1 | 2025-01-27 15:40:28 | 5.65           | 2         |
    +---------+---------------------+----------------+-----------+
    """
    assert "Workload: TPCH, Scale Factor:" in output
    assert "Table Format: text/none/none" in output
    assert "Query" in output
    assert "Start Time" in output
    assert "Time Taken (s)" in output
    assert "Client ID" in output
    assert "TPCH-Q1" in output
