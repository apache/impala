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

# Tests end-to-end codegen behaviour.

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension_from_dict
from tests.common.test_result_verifier import get_node_exec_options,\
    assert_codegen_enabled

class TestCodegen(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCodegen, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension_from_dict({
        'exec_single_node_rows_threshold' : [0]}))
    # No need to run this on all file formats. Run it on text/none, which has stats
    # computed.
    cls.ImpalaTestMatrix.add_constraint(
              lambda v: v.get_value('table_format').file_format == 'text' and
                        v.get_value('table_format').compression_codec == 'none')

  def test_disable_codegen(self, vector):
    """Test that codegen is enabled/disabled by the planner as expected."""
    self.run_test_case('QueryTest/disable-codegen', vector)

  def test_select_node_codegen(self, vector):
    """Test that select node is codegened"""
    result = self.execute_query('select * from (select * from functional.alltypes '
        'limit 1000000) t1 where int_col > 10 limit 10')
    exec_options = get_node_exec_options(result.runtime_profile, 1)
    # Make sure test fails if there are no exec options in the profile for the node
    assert len(exec_options) > 0
    assert_codegen_enabled(result.runtime_profile, [1])