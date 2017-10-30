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

# Functional tests running the TPC-DS workload
#
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    is_supported_insert_format)

class TestTpcdsQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsQuery, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format not in ['rc', 'hbase', 'kudu'] and\
        v.get_value('table_format').compression_codec in ['none', 'snap'] and\
        v.get_value('table_format').compression_type != 'record')

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on the execution time for these tests in core by running only
      # against parquet.
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in ['parquet'])

    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)

  @pytest.mark.execute_serially
  # Marked serially to make sure it runs first.
  def test_tpcds_count(self, vector):
    self.run_test_case('count', vector)

  def test_tpcds_q1(self, vector):
    self.run_test_case(self.get_workload() + '-q1', vector)

  def test_tpcds_q2(self, vector):
    self.run_test_case(self.get_workload() + '-q2', vector)

  def test_tpcds_q3(self, vector):
    self.run_test_case(self.get_workload() + '-q3', vector)

  def test_tpcds_q4(self, vector):
    self.run_test_case(self.get_workload() + '-q4', vector)

  def test_tpcds_q6(self, vector):
    self.run_test_case(self.get_workload() + '-q6', vector)

  def test_tpcds_q7(self, vector):
    self.run_test_case(self.get_workload() + '-q7', vector)

  def test_tpcds_q8(self, vector):
    self.run_test_case(self.get_workload() + '-q8', vector)

  def test_tpcds_q10a(self, vector):
    self.run_test_case(self.get_workload() + '-q10a', vector)

  def test_tpcds_q11(self, vector):
    self.run_test_case(self.get_workload() + '-q11', vector)

  def test_tpcds_q12(self, vector):
    self.run_test_case(self.get_workload() + '-q12', vector)

  def test_tpcds_q13(self, vector):
    self.run_test_case(self.get_workload() + '-q13', vector)

  def test_tpcds_q15(self, vector):
    self.run_test_case(self.get_workload() + '-q15', vector)

  def test_tpcds_q16(self, vector):
    self.run_test_case(self.get_workload() + '-q16', vector)

  def test_tpcds_q17(self, vector):
    self.run_test_case(self.get_workload() + '-q17', vector)

  def test_tpcds_q18a(self, vector):
    self.run_test_case(self.get_workload() + '-q18a', vector)

  def test_tpcds_q19(self, vector):
    self.run_test_case(self.get_workload() + '-q19', vector)

  def test_tpcds_q20(self, vector):
    self.run_test_case(self.get_workload() + '-q20', vector)

  def test_tpcds_q21(self, vector):
    self.run_test_case(self.get_workload() + '-q21', vector)

  def test_tpcds_q22a(self, vector):
    self.run_test_case(self.get_workload() + '-q22a', vector)

  def test_tpcds_q25(self, vector):
    self.run_test_case(self.get_workload() + '-q25', vector)

  def test_tpcds_q29(self, vector):
    self.run_test_case(self.get_workload() + '-q29', vector)

  def test_tpcds_q32(self, vector):
    self.run_test_case(self.get_workload() + '-q32', vector)

  def test_tpcds_q33(self, vector):
    self.run_test_case(self.get_workload() + '-q33', vector)

  def test_tpcds_q34(self, vector):
    self.run_test_case(self.get_workload() + '-q34', vector)

  def test_tpcds_q37(self, vector):
    self.run_test_case(self.get_workload() + '-q37', vector)

  def test_tpcds_q39_1(self, vector):
    self.run_test_case(self.get_workload() + '-q39-1', vector)

  def test_tpcds_q39_2(self, vector):
    self.run_test_case(self.get_workload() + '-q39-2', vector)

  def test_tpcds_q40(self, vector):
    self.run_test_case(self.get_workload() + '-q40', vector)

  def test_tpcds_q41(self, vector):
    self.run_test_case(self.get_workload() + '-q41', vector)

  def test_tpcds_q42(self, vector):
    self.run_test_case(self.get_workload() + '-q42', vector)

  def test_tpcds_q43(self, vector):
    self.run_test_case(self.get_workload() + '-q43', vector)

  def test_tpcds_q46(self, vector):
    self.run_test_case(self.get_workload() + '-q46', vector)

  def test_tpcds_q50(self, vector):
    self.run_test_case(self.get_workload() + '-q50', vector)

  def test_tpcds_q51(self, vector):
    self.run_test_case(self.get_workload() + '-q51', vector)

  def test_tpcds_q51a(self, vector):
    self.run_test_case(self.get_workload() + '-q51a', vector)

  def test_tpcds_q52(self, vector):
    self.run_test_case(self.get_workload() + '-q52', vector)

  def test_tpcds_q53(self, vector):
    self.run_test_case(self.get_workload() + '-q53', vector)

  def test_tpcds_q54(self, vector):
    self.run_test_case(self.get_workload() + '-q54', vector)

  def test_tpcds_q55(self, vector):
    self.run_test_case(self.get_workload() + '-q55', vector)

  def test_tpcds_q56(self, vector):
    self.run_test_case(self.get_workload() + '-q56', vector)

  def test_tpcds_q60(self, vector):
    self.run_test_case(self.get_workload() + '-q60', vector)

  def test_tpcds_q61(self, vector):
    self.run_test_case(self.get_workload() + '-q61', vector)

  def test_tpcds_q62(self, vector):
    self.run_test_case(self.get_workload() + '-q62', vector)

  def test_tpcds_q64(self, vector):
    self.run_test_case(self.get_workload() + '-q64', vector)

  def test_tpcds_q65(self, vector):
    self.run_test_case(self.get_workload() + '-q65', vector)

  def test_tpcds_q67a(self, vector):
    self.run_test_case(self.get_workload() + '-q67a', vector)

  def test_tpcds_q68(self, vector):
    self.run_test_case(self.get_workload() + '-q68', vector)

  def test_tpcds_q69(self, vector):
    self.run_test_case(self.get_workload() + '-q69', vector)

  def test_tpcds_q70a(self, vector):
    self.run_test_case(self.get_workload() + '-q70a', vector)

  def test_tpcds_q71(self, vector):
    self.run_test_case(self.get_workload() + '-q71', vector)

  def test_tpcds_q72(self, vector):
    self.run_test_case(self.get_workload() + '-q72', vector)

  def test_tpcds_q73(self, vector):
    self.run_test_case(self.get_workload() + '-q73', vector)

  def test_tpcds_q74(self, vector):
    self.run_test_case(self.get_workload() + '-q74', vector)

  def test_tpcds_q75(self, vector):
    self.run_test_case(self.get_workload() + '-q75', vector)

  def test_tpcds_q76(self, vector):
    self.run_test_case(self.get_workload() + '-q76', vector)

  def test_tpcds_q77a(self, vector):
    self.run_test_case(self.get_workload() + '-q77a', vector)

  def test_tpcds_q78(self, vector):
    self.run_test_case(self.get_workload() + '-q78', vector)

  def test_tpcds_q79(self, vector):
    self.run_test_case(self.get_workload() + '-q79', vector)

  def test_tpcds_q80a(self, vector):
    self.run_test_case(self.get_workload() + '-q80a', vector)

  def test_tpcds_q81(self, vector):
    self.run_test_case(self.get_workload() + '-q81', vector)

  def test_tpcds_q82(self, vector):
    self.run_test_case(self.get_workload() + '-q82', vector)

  def test_tpcds_q84(self, vector):
    self.run_test_case(self.get_workload() + '-q84', vector)

  def test_tpcds_q86a(self, vector):
    self.run_test_case(self.get_workload() + '-q86a', vector)

  def test_tpcds_q88(self, vector):
    self.run_test_case(self.get_workload() + '-q88', vector)

  def test_tpcds_q91(self, vector):
    self.run_test_case(self.get_workload() + '-q91', vector)

  def test_tpcds_q92(self, vector):
    self.run_test_case(self.get_workload() + '-q92', vector)

  def test_tpcds_q94(self, vector):
    self.run_test_case(self.get_workload() + '-q94', vector)

  def test_tpcds_q95(self, vector):
    self.run_test_case(self.get_workload() + '-q95', vector)

  def test_tpcds_q96(self, vector):
    self.run_test_case(self.get_workload() + '-q96', vector)

  def test_tpcds_q97(self, vector):
    self.run_test_case(self.get_workload() + '-q97', vector)

  def test_tpcds_q98(self, vector):
    self.run_test_case(self.get_workload() + '-q98', vector)

  def test_tpcds_q99(self, vector):
    self.run_test_case(self.get_workload() + '-q99', vector)


class TestTpcdsInsert(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return TestTpcdsQuery.get_workload() + '-insert'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsInsert, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        is_supported_insert_format(v.get_value('table_format')))
    if cls.exploration_strategy() == 'core' and not pytest.config.option.table_formats:
      # Don't run on core, unless the user explicitly wants to validate a specific table
      # format. Each test vector takes > 30s to complete and it doesn't add much
      # additional coverage on top of what's in the functional insert test suite
      cls.ImpalaTestMatrix.add_constraint(lambda v: False);

  def test_tpcds_partitioned_insert(self, vector):
    self.run_test_case('partitioned-insert', vector)
