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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfOldAggsJoins
from tests.common.test_dimensions import create_single_exec_option_dimension

@SkipIfOldAggsJoins.nested_types
class TestTpchNestedQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch_nested'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchNestedQuery, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # The nested tpch data is currently only available in parquet.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  def test_tpch_q1(self, vector):
    self.run_test_case('tpch-q1', vector)

  def test_tpch_q2(self, vector):
    self.run_test_case('tpch-q2', vector)

  def test_tpch_q3(self, vector):
    self.run_test_case('tpch-q3', vector)

  def test_tpch_q4(self, vector):
    self.run_test_case('tpch-q4', vector)

  def test_tpch_q5(self, vector):
    self.run_test_case('tpch-q5', vector)

  def test_tpch_q6(self, vector):
    self.run_test_case('tpch-q6', vector)

  def test_tpch_q7(self, vector):
    self.run_test_case('tpch-q7', vector)

  def test_tpch_q8(self, vector):
    self.run_test_case('tpch-q8', vector)

  def test_tpch_q9(self, vector):
    self.run_test_case('tpch-q9', vector)

  def test_tpch_q10(self, vector):
    self.run_test_case('tpch-q10', vector)

  def test_tpch_q11(self, vector):
    self.run_test_case('tpch-q11', vector)

  def test_tpch_q12(self, vector):
    self.run_test_case('tpch-q12', vector)

  def test_tpch_q13(self, vector):
    self.run_test_case('tpch-q13', vector)

  def test_tpch_q14(self, vector):
    self.run_test_case('tpch-q14', vector)

  def test_tpch_q15(self, vector):
    self.run_test_case('tpch-q15', vector)

  def test_tpch_q16(self, vector):
    self.run_test_case('tpch-q16', vector)

  def test_tpch_q17(self, vector):
    self.run_test_case('tpch-q17', vector)

  def test_tpch_q18(self, vector):
    self.run_test_case('tpch-q18', vector)

  def test_tpch_q19(self, vector):
    self.run_test_case('tpch-q19', vector)

  def test_tpch_q20(self, vector):
    self.run_test_case('tpch-q20', vector)

  def test_tpch_q21(self, vector):
    self.run_test_case('tpch-q21', vector)

  def test_tpch_q22(self, vector):
    self.run_test_case('tpch-q22', vector)
