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

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf
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
        'limit 1000000) t1 where int_col > 10 limit 10',
        {'disable_codegen_rows_threshold': 7000})
    exec_options = get_node_exec_options(result.runtime_profile, 1)
    # Make sure test fails if there are no exec options in the profile for the node
    assert len(exec_options) > 0
    assert_codegen_enabled(result.runtime_profile, [1])

  def test_datastream_sender_codegen(self, vector):
    """Test the KrpcDataStreamSender's codegen logic"""
    self.run_test_case('QueryTest/datastream-sender-codegen', vector)

  def test_codegen_failure_for_char_type(self, vector):
    """IMPALA-7288: Regression tests for the codegen failure path when working with a
    CHAR column type. Until IMPALA-3207 is completely fixed there are various paths where
    we need to bail out of codegen."""
    # Previously codegen failed in TextConverter::CodegenWriteSlot() if there was a CHAR
    # column, but IMPALA-9747 changed this so that instead of failing codegen for all
    # columns, the codegen'd code for the CHAR column calls the interpreted version of the
    # function.

    # Previously codegen for this join failed in HashTableCtx::CodegenEquals() because of
    # missing ScalarFnCall codegen support, which was added in IMPALA-7331.
    result = self.execute_query("select 1 from functional.chars_tiny t1, "
                                "functional.chars_tiny t2 "
                                "where t1.cs = cast(t2.cs as string)")
    profile_str = str(result.runtime_profile)
    assert "Probe Side Codegen Enabled" in profile_str, profile_str
    assert "Build Side Codegen Enabled" in profile_str, profile_str

    # Codegen for this join fails because it is joining two CHAR exprs.
    result = self.execute_query("select 1 from functional.chars_tiny t1, "
                                "functional.chars_tiny t2 "
                                "where t1.cs = t2.cs")
    profile_str = str(result.runtime_profile)
    assert ("Probe Side Codegen Disabled: HashTableCtx::CodegenHashRow(): CHAR NYI"
            in profile_str), profile_str
    assert ("Build Side Codegen Disabled: HashTableCtx::CodegenHashRow(): CHAR NYI"
            in profile_str), profile_str

    # Previously codegen for this join failed in HashTableCtx::CodegenEvalRow() because of
    # missing ScalarFnCall codegen support, which was added in IMPALA-7331.
    result = self.execute_query("select 1 from functional.chars_tiny t1, "
                                "functional.chars_tiny t2 where t1.cs = "
                                "FROM_TIMESTAMP(cast(t2.cs as string), 'yyyyMMdd')")
    profile_str = str(result.runtime_profile)
    assert "Probe Side Codegen Enabled" in profile_str, profile_str
    assert "Build Side Codegen Enabled" in profile_str, profile_str

  def test_const_scalar_expr_in_union(self, vector, unique_database):
    """Test that codegen is disabled for const scalar expressions in a UNION node.
    if, however the UNION node is under a subplan then codegen is not disabled for
    const expressions."""
    self.run_test_case('QueryTest/union-const-scalar-expr-codegen', vector,
        use_db=unique_database)
