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
from tests.common.impala_test_suite import ImpalaTestSuite


class TestQueryExecTimeLimit(ImpalaTestSuite):
  """Tests the exec_time_limit_s query option."""

  @classmethod
  def get_workload(cls):
    return 'tpch'

  def test_exec_time_limit_enforced(self):
    """Test that queries exceeding exec_time_limit_s are properly cancelled."""
    exec_options = dict()
    exec_options['exec_time_limit_s'] = "1"
    query = "SELECT COUNT(*) FROM tpch.lineitem L1, tpch.lineitem L2"
    try:
      self.execute_query(query, exec_options)
      assert False, "Query was expected to time out but succeeded."
    except Exception as e:
      assert "expired due to execution time limit" in str(e), (
        "Unexpected exception: {}".format(e)
      )

  def test_exec_time_limit_long_plan(self):
    """Test that queries with a long planning time completing within
    exec_time_limit_s succeed."""
    exec_options = dict()
    exec_options['exec_time_limit_s'] = "2"
    # Set debug action to wait in the plan phase for 10s.
    exec_options['debug_action'] = "plan_create:SLEEP@10000"
    query = "SELECT * FROM tpch.lineitem limit 1"
    result = self.execute_query(query, exec_options)
    assert result.success, "Query failed unexpectedly within exec_time_limit_s."

  def test_exec_time_limit_not_exceeded(self):
    """Test that queries completing within exec_time_limit_s succeed."""
    exec_options = dict()
    exec_options['exec_time_limit_s'] = "60"
    query = "SELECT COUNT(*) FROM tpch.lineitem"
    result = self.execute_query(query, exec_options)
    assert result.success, "Query failed unexpectedly within exec_time_limit_s."

  def test_exec_time_limit_zero(self):
    """Test that setting exec_time_limit_s to 0 disables the limit."""
    exec_options = dict()
    exec_options['exec_time_limit_s'] = "0"
    query = "SELECT COUNT(*) FROM tpch.lineitem"
    result = self.execute_query(query, exec_options)
    assert result.success, "Query with exec_time_limit_s=0 failed unexpectedly."
