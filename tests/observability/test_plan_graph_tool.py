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

import os
import tempfile
from subprocess import check_output

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import HS2

IMPALA_HOME = os.environ['IMPALA_HOME']
PLAN_GRAPH = os.path.join(IMPALA_HOME, 'bin/diagnostics/experimental/plan-graph.py')


class TestPlanGraphTool(ImpalaTestSuite):
  """Tests to ensure profile changes won't break plan-graph.py."""

  @classmethod
  def default_test_protocol(cls):
    return HS2

  def test_runtime_filter_parsing(self):
    query_opts = {'runtime_filter_wait_time_ms': 10000}
    res = self.execute_query("""
        select STRAIGHT_JOIN count(l_orderkey)
        from tpch_parquet.lineitem join tpch_parquet.orders
        on l_orderkey = o_orderkey
        where o_custkey < 1000""", query_opts)
    assert res.success

    with tempfile.TemporaryDirectory() as tmpdir:
      profile_path = os.path.join(tmpdir, 'profile.txt')
      with open(profile_path, 'w') as profile_file:
        profile_file.write(res.runtime_profile)
      output = check_output([PLAN_GRAPH, profile_path], text=True)

    expected = (
        r'RF000[bloom]\no_orderkey => l_orderkey\nLOCAL, fpp 2.06e-06\n1.00 MB')
    assert expected in output, output
