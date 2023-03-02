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

# Functional tests running the TPCH workload.

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension

class TestTpchNestedQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch_nested'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchNestedQuery, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # The nested tpch data is currently only available in parquet and orc.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet', 'orc'])

  def test_tpch_q1(self, vector):
    self.run_test_case(self.get_workload() + '-q1', vector)

  def test_tpch_q2(self, vector):
    self.run_test_case(self.get_workload() + '-q2', vector)

  def test_tpch_q3(self, vector):
    self.run_test_case(self.get_workload() + '-q3', vector)

  def test_tpch_q4(self, vector):
    self.run_test_case(self.get_workload() + '-q4', vector)

  def test_tpch_q5(self, vector):
    self.run_test_case(self.get_workload() + '-q5', vector)

  def test_tpch_q6(self, vector):
    self.run_test_case(self.get_workload() + '-q6', vector)

  def test_tpch_q7(self, vector):
    self.run_test_case(self.get_workload() + '-q7', vector)

  def test_tpch_q8(self, vector):
    self.run_test_case(self.get_workload() + '-q8', vector)

  def test_tpch_q9(self, vector):
    self.run_test_case(self.get_workload() + '-q9', vector)

  def test_tpch_q10(self, vector):
    self.run_test_case(self.get_workload() + '-q10', vector)

  def test_tpch_q11(self, vector):
    self.run_test_case(self.get_workload() + '-q11', vector)

  def test_tpch_q12(self, vector):
    self.run_test_case(self.get_workload() + '-q12', vector)

  def test_tpch_q13(self, vector):
    self.run_test_case(self.get_workload() + '-q13', vector)

  def test_tpch_q14(self, vector):
    self.run_test_case(self.get_workload() + '-q14', vector)

  def test_tpch_q15(self, vector):
    self.run_test_case(self.get_workload() + '-q15', vector)

  def test_tpch_q16(self, vector):
    self.run_test_case(self.get_workload() + '-q16', vector)

  def test_tpch_q17(self, vector):
    self.run_test_case(self.get_workload() + '-q17', vector)

  def test_tpch_q18(self, vector):
    self.run_test_case(self.get_workload() + '-q18', vector)

  def test_tpch_q19(self, vector):
    self.run_test_case(self.get_workload() + '-q19', vector)

  def test_tpch_q20(self, vector):
    self.run_test_case(self.get_workload() + '-q20', vector)

  def test_tpch_q21(self, vector):
    self.run_test_case(self.get_workload() + '-q21', vector)

  def test_tpch_q22(self, vector):
    self.run_test_case(self.get_workload() + '-q22', vector)
