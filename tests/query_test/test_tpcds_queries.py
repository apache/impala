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
from __future__ import absolute_import, division, print_function
import os
import pytest
from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import (
    SkipIfBuildType,
    SkipIfDockerizedCluster)
from tests.common.test_dimensions import (
    add_mandatory_exec_option,
    create_single_exec_option_dimension,
    is_supported_insert_format)

class TestTpcdsQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsQuery, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format not in ['rc', 'hbase', 'kudu'] and
        v.get_value('table_format').compression_codec in ['none', 'snap'] and
        v.get_value('table_format').compression_type != 'record')
    add_mandatory_exec_option(cls, 'decimal_v2', 0)

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on the execution time for these tests in core by running only
      # against parquet.
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format in ['parquet'])

    cls.ImpalaTestMatrix.add_constraint(lambda v:
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

  def test_tpcds_q9(self, vector):
    self.run_test_case(self.get_workload() + '-q9', vector)

  def test_tpcds_q10a(self, vector):
    self.run_test_case(self.get_workload() + '-q10a', vector)

  def test_tpcds_q11(self, vector):
    self.run_test_case(self.get_workload() + '-q11', vector)

  def test_tpcds_q12(self, vector):
    self.run_test_case(self.get_workload() + '-q12', vector)

  def test_tpcds_q13(self, vector):
    self.run_test_case(self.get_workload() + '-q13', vector)

  def test_tpcds_q14_1(self, vector):
    self.run_test_case(self.get_workload() + '-q14-1', vector)

  def test_tpcds_q14_2(self, vector):
    self.run_test_case(self.get_workload() + '-q14-2', vector)

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

  def test_tpcds_q23_1(self, vector):
    self.run_test_case(self.get_workload() + '-q23-1', vector)

  def test_tpcds_q23_2(self, vector):
    self.run_test_case(self.get_workload() + '-q23-2', vector)

  def test_tpcds_q24_1(self, vector):
    self.run_test_case(self.get_workload() + '-q24-1', vector)

  def test_tpcds_q24_2(self, vector):
    self.run_test_case(self.get_workload() + '-q24-2', vector)

  def test_tpcds_q25(self, vector):
    self.run_test_case(self.get_workload() + '-q25', vector)

  def test_tpcds_q26(self, vector):
    self.run_test_case(self.get_workload() + '-q26', vector)

  def test_tpcds_q29(self, vector):
    self.run_test_case(self.get_workload() + '-q29', vector)

  def test_tpcds_q30(self, vector):
    self.run_test_case(self.get_workload() + '-q30', vector)

  def test_tpcds_q32(self, vector):
    self.run_test_case(self.get_workload() + '-q32', vector)

  def test_tpcds_q33(self, vector):
    self.run_test_case(self.get_workload() + '-q33', vector)

  def test_tpcds_q34(self, vector):
    self.run_test_case(self.get_workload() + '-q34', vector)

  def test_tpcds_q37(self, vector):
    self.run_test_case(self.get_workload() + '-q37', vector)

  def test_tpcds_q38(self, vector):
    self.run_test_case(self.get_workload() + '-q38', vector)

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

  def test_tpcds_q44(self, vector):
    self.run_test_case(self.get_workload() + '-q44', vector)

  def test_tpcds_q46(self, vector):
    self.run_test_case(self.get_workload() + '-q46', vector)

  def test_tpcds_q47(self, vector):
    self.run_test_case(self.get_workload() + '-q47', vector)

  def test_tpcds_q48(self, vector):
    self.run_test_case(self.get_workload() + '-q48', vector)

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

  def test_tpcds_q57(self, vector):
    self.run_test_case(self.get_workload() + '-q57', vector)

  def test_tpcds_q58(self, vector):
    self.run_test_case(self.get_workload() + '-q58', vector)

  def test_tpcds_q59(self, vector):
    self.run_test_case(self.get_workload() + '-q59', vector)

  def test_tpcds_q60(self, vector):
    self.run_test_case(self.get_workload() + '-q60', vector)

  def test_tpcds_q61(self, vector):
    self.run_test_case(self.get_workload() + '-q61', vector)

  def test_tpcds_q62(self, vector):
    self.run_test_case(self.get_workload() + '-q62', vector)

  def test_tpcds_q63(self, vector):
    self.run_test_case(self.get_workload() + '-q63', vector)

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

  def test_tpcds_q83(self, vector):
    self.run_test_case(self.get_workload() + '-q83', vector)

  def test_tpcds_q84(self, vector):
    self.run_test_case(self.get_workload() + '-q84', vector)

  def test_tpcds_q85(self, vector):
    self.run_test_case(self.get_workload() + '-q85', vector)

  def test_tpcds_q86a(self, vector):
    self.run_test_case(self.get_workload() + '-q86a', vector)

  def test_tpcds_q87(self, vector):
    self.run_test_case(self.get_workload() + '-q87', vector)

  def test_tpcds_q88(self, vector):
    self.run_test_case(self.get_workload() + '-q88', vector)

  def test_tpcds_q89(self, vector):
    self.run_test_case(self.get_workload() + '-q89', vector)

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


class TestTpcdsDecimalV2Query(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsDecimalV2Query, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format not in ['rc', 'hbase', 'kudu'] and
        v.get_value('table_format').compression_codec in ['none', 'snap'] and
        v.get_value('table_format').compression_type != 'record')

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on the execution time for these tests in core by running only
      # against parquet.
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format in ['parquet'])

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['batch_size'] == 0)

  def test_tpcds_q1(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q1', vector)

  def test_tpcds_q2(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q2', vector)

  def test_tpcds_q3(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q3', vector)

  def test_tpcds_q4(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q4', vector)

  def test_tpcds_q5(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q5', vector)

  def test_tpcds_q6(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q6', vector)

  def test_tpcds_q7(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q7', vector)

  def test_tpcds_q8(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q8', vector)

  def test_tpcds_q9(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q9', vector)

  def test_tpcds_q10a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q10a', vector)

  def test_tpcds_q11(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q11', vector)

  def test_tpcds_q12(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q12', vector)

  def test_tpcds_q13(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q13', vector)

  def test_tpcds_q14_1(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q14-1', vector)

  def test_tpcds_q14_2(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q14-2', vector)

  def test_tpcds_q15(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q15', vector)

  def test_tpcds_q16(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q16', vector)

  def test_tpcds_q17(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q17', vector)

  def test_tpcds_q18(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q18', vector)

  def test_tpcds_q18a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q18a', vector)

  def test_tpcds_q19(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q19', vector)

  def test_tpcds_q20(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q20', vector)

  def test_tpcds_q21(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q21', vector)

  def test_tpcds_q22(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q22', vector)

  def test_tpcds_q22a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q22a', vector)

  def test_tpcds_q23_1(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q23-1', vector)

  def test_tpcds_q23_2(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q23-2', vector)

  def test_tpcds_q24_1(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q24-1', vector)

  def test_tpcds_q24_2(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q24-2', vector)

  def test_tpcds_q25(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q25', vector)

  def test_tpcds_q26(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q26', vector)

  def test_tpcds_q27(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q27', vector)

  def test_tpcds_q28(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q28', vector)

  def test_tpcds_q29(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q29', vector)

  def test_tpcds_q30(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q30', vector)

  def test_tpcds_q31(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q31', vector)

  def test_tpcds_q32(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q32', vector)

  def test_tpcds_q33(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q33', vector)

  def test_tpcds_q34(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q34', vector)

  def test_tpcds_q35(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q35a', vector)

  def test_tpcds_q36(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q36', vector)

  def test_tpcds_q37(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q37', vector)

  def test_tpcds_q38(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q38', vector)

  def test_tpcds_q39_1(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q39-1', vector)

  def test_tpcds_q39_2(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q39-2', vector)

  def test_tpcds_q40(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q40', vector)

  def test_tpcds_q41(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q41', vector)

  def test_tpcds_q42(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q42', vector)

  def test_tpcds_q43(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q43', vector)

  def test_tpcds_q44(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q44', vector)

  def test_tpcds_q45(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q45', vector)

  def test_tpcds_q46(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q46', vector)

  def test_tpcds_q47(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q47', vector)

  def test_tpcds_q48(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q48', vector)

  def test_tpcds_q49(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q49', vector)

  def test_tpcds_q50(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q50', vector)

  def test_tpcds_q51(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q51', vector)

  def test_tpcds_q51a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q51a', vector)

  def test_tpcds_q52(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q52', vector)

  def test_tpcds_q53(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q53', vector)

  def test_tpcds_q54(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q54', vector)

  def test_tpcds_q55(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q55', vector)

  def test_tpcds_q56(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q56', vector)

  def test_tpcds_q57(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q57', vector)

  def test_tpcds_q58(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q58', vector)

  def test_tpcds_q59(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q59', vector)

  def test_tpcds_q60(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q60', vector)

  def test_tpcds_q61(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q61', vector)

  def test_tpcds_q62(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q62', vector)

  def test_tpcds_q63(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q63', vector)

  def test_tpcds_q64(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q64', vector)

  def test_tpcds_q65(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q65', vector)

  def test_tpcds_q66(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q66', vector)

  def test_tpcds_q67(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q67', vector)

  def test_tpcds_q67a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q67a', vector)

  def test_tpcds_q68(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q68', vector)

  def test_tpcds_q69(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q69', vector)

  def test_tpcds_q70(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q70', vector)

  def test_tpcds_q70a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q70a', vector)

  def test_tpcds_q71(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q71', vector)

  def test_tpcds_q72(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q72', vector)

  def test_tpcds_q73(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q73', vector)

  def test_tpcds_q74(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q74', vector)

  def test_tpcds_q75(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q75', vector)

  def test_tpcds_q76(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q76', vector)

  def test_tpcds_q77(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q77', vector)

  def test_tpcds_q77a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q77a', vector)

  def test_tpcds_q78(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q78', vector)

  def test_tpcds_q79(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q79', vector)

  def test_tpcds_q80(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q80', vector)

  def test_tpcds_q80a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q80a', vector)

  def test_tpcds_q81(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q81', vector)

  def test_tpcds_q82(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q82', vector)

  def test_tpcds_q83(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q83', vector)

  def test_tpcds_q84(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q84', vector)

  def test_tpcds_q85(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q85', vector)

  def test_tpcds_q86(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q86', vector)

  def test_tpcds_q86a(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q86a', vector)

  def test_tpcds_q87(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q87', vector)

  def test_tpcds_q88(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q88', vector)

  def test_tpcds_q89(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q89', vector)

  def test_tpcds_q90(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q90', vector)

  def test_tpcds_q91(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q91', vector)

  def test_tpcds_q92(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q92', vector)

  def test_tpcds_q93(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q93', vector)

  def test_tpcds_q94(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q94', vector)

  def test_tpcds_q95(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q95', vector)

  def test_tpcds_q96(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q96', vector)

  def test_tpcds_q97(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q97', vector)

  def test_tpcds_q98(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q98', vector)

  def test_tpcds_q99(self, vector):
    self.run_test_case(self.get_workload() + '-decimal_v2-q99', vector)


class TestTpcdsInsert(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return TestTpcdsQuery.get_workload() + '-insert'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsInsert, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        is_supported_insert_format(v.get_value('table_format')))

  def test_tpcds_partitioned_insert(self, vector):
    self.run_test_case('partitioned-insert', vector)

  def test_expr_insert(self, vector):
    self.run_test_case('expr-insert', vector)


class TestTpcdsUnmodified(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'tpcds-unmodified'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsUnmodified, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format not in ['rc', 'hbase', 'kudu'] and
        v.get_value('table_format').compression_codec in ['none', 'snap'] and
        v.get_value('table_format').compression_type != 'record')

    if cls.exploration_strategy() != 'exhaustive':
      # Cut down on the execution time for these tests in core by running only
      # against parquet.
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format in ['parquet'])

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['batch_size'] == 0)

  def test_tpcds_q31(self, vector):
    self.run_test_case('tpcds-q31', vector)

  def test_tpcds_q35a(self, vector):
    self.run_test_case('tpcds-q35a', vector)

  def test_tpcds_q48(self, vector):
    self.run_test_case('tpcds-q48', vector)

  def test_tpcds_q59(self, vector):
    self.run_test_case('tpcds-q59', vector)

  def test_tpcds_q89(self, vector):
    self.run_test_case('tpcds-q89', vector)


@SkipIfDockerizedCluster.insufficient_mem_limit
class TestTpcdsQueryWithProcessingCost(TestTpcdsQuery):
  @classmethod
  def get_workload(cls):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsQueryWithProcessingCost, cls).add_test_dimensions()
    add_mandatory_exec_option(cls, 'compute_processing_cost', 1)
    add_mandatory_exec_option(cls, 'max_fragment_instances_per_node', 4)
    add_mandatory_exec_option(cls, 'slot_count_strategy', 'planner_cpu_ask')

  def test_tpcds_q51a(self, vector):
    """Reduce max_fragment_instances_per_node to lower memory requirement."""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['max_fragment_instances_per_node'] = 2
    self.run_test_case(self.get_workload() + '-q51a', new_vector)

  def test_tpcds_q67a(self, vector):
    """Reduce max_fragment_instances_per_node to lower memory requirement."""
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['max_fragment_instances_per_node'] = 2
    self.run_test_case(self.get_workload() + '-q67a', new_vector)


@SkipIfBuildType.dev_build
@SkipIfDockerizedCluster.insufficient_mem_limit
class TestTpcdsQueryForJdbcTables(ImpalaTestSuite):
  """TPCDS query tests for external jdbc tables."""

  @classmethod
  def get_workload(self):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsQueryForJdbcTables, cls).add_test_dimensions()

    # For exhaustive test on Jenkins, cls.exploration_strategy() returns 'core'
    # (IMPALA-4904, IMPALA-4914). The workaround is to get exploration strategy from
    # environment variable 'EXPLORATION_STRATEGY'.
    try:
      env_exploration_strategy = os.environ['EXPLORATION_STRATEGY']
    except KeyError:
      env_exploration_strategy = ''
    if env_exploration_strategy != 'exhaustive' \
        and cls.exploration_strategy() != 'exhaustive':
      pytest.skip("These tests only run in exhaustive mode.")
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')
    add_mandatory_exec_option(cls, 'clean_dbcp_ds_cache', 'false')

  # Marked serially to make sure it runs first.
  def test_tpcds_count(self, vector):
    self.run_test_case('count', vector, use_db='tpcds_jdbc')

  def test_tpcds_q1(self, vector):
    self.run_test_case('tpcds-decimal_v2-q1', vector, use_db='tpcds_jdbc')

  def test_tpcds_q2(self, vector):
    self.run_test_case('tpcds-decimal_v2-q2', vector, use_db='tpcds_jdbc')

  def test_tpcds_q3(self, vector):
    self.run_test_case('tpcds-decimal_v2-q3', vector, use_db='tpcds_jdbc')

  def test_tpcds_q4(self, vector):
    self.run_test_case('tpcds-decimal_v2-q4', vector, use_db='tpcds_jdbc')

  def test_tpcds_q5(self, vector):
    self.run_test_case('tpcds-decimal_v2-q5', vector, use_db='tpcds_jdbc')

  def test_tpcds_q6(self, vector):
    self.run_test_case('tpcds-decimal_v2-q6', vector, use_db='tpcds_jdbc')

  def test_tpcds_q7(self, vector):
    self.run_test_case('tpcds-decimal_v2-q7', vector, use_db='tpcds_jdbc')

  def test_tpcds_q8(self, vector):
    self.run_test_case('tpcds-decimal_v2-q8', vector, use_db='tpcds_jdbc')

  def test_tpcds_q9(self, vector):
    self.run_test_case('tpcds-decimal_v2-q9', vector, use_db='tpcds_jdbc')

  def test_tpcds_q10a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q10a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q11(self, vector):
    self.run_test_case('tpcds-decimal_v2-q11', vector, use_db='tpcds_jdbc')

  def test_tpcds_q12(self, vector):
    self.run_test_case('tpcds-decimal_v2-q12', vector, use_db='tpcds_jdbc')

  def test_tpcds_q13(self, vector):
    self.run_test_case('tpcds-decimal_v2-q13', vector, use_db='tpcds_jdbc')

  def test_tpcds_q14_1(self, vector):
    self.run_test_case('tpcds-decimal_v2-q14-1', vector, use_db='tpcds_jdbc')

  def test_tpcds_q14_2(self, vector):
    self.run_test_case('tpcds-decimal_v2-q14-2', vector, use_db='tpcds_jdbc')

  def test_tpcds_q15(self, vector):
    self.run_test_case('tpcds-decimal_v2-q15', vector, use_db='tpcds_jdbc')

  def test_tpcds_q16(self, vector):
    self.run_test_case('tpcds-decimal_v2-q16', vector, use_db='tpcds_jdbc')

  def test_tpcds_q17(self, vector):
    self.run_test_case('tpcds-decimal_v2-q17', vector, use_db='tpcds_jdbc')

  def test_tpcds_q18(self, vector):
    self.run_test_case('tpcds-decimal_v2-q18', vector, use_db='tpcds_jdbc')

  def test_tpcds_q18a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q18a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q19(self, vector):
    self.run_test_case('tpcds-decimal_v2-q19', vector, use_db='tpcds_jdbc')

  def test_tpcds_q20(self, vector):
    self.run_test_case('tpcds-decimal_v2-q20', vector, use_db='tpcds_jdbc')

  def test_tpcds_q21(self, vector):
    self.run_test_case('tpcds-decimal_v2-q21', vector, use_db='tpcds_jdbc')

  def test_tpcds_q22(self, vector):
    self.run_test_case('tpcds-decimal_v2-q22', vector, use_db='tpcds_jdbc')

  def test_tpcds_q22a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q22a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q23_1(self, vector):
    self.run_test_case('tpcds-decimal_v2-q23-1', vector, use_db='tpcds_jdbc')

  def test_tpcds_q23_2(self, vector):
    self.run_test_case('tpcds-decimal_v2-q23-2', vector, use_db='tpcds_jdbc')

  def test_tpcds_q24_1(self, vector):
    self.run_test_case('tpcds-decimal_v2-q24-1', vector, use_db='tpcds_jdbc')

  def test_tpcds_q24_2(self, vector):
    self.run_test_case('tpcds-decimal_v2-q24-2', vector, use_db='tpcds_jdbc')

  def test_tpcds_q25(self, vector):
    self.run_test_case('tpcds-decimal_v2-q25', vector, use_db='tpcds_jdbc')

  def test_tpcds_q26(self, vector):
    self.run_test_case('tpcds-decimal_v2-q26', vector, use_db='tpcds_jdbc')

  def test_tpcds_q27(self, vector):
    self.run_test_case('tpcds-decimal_v2-q27', vector, use_db='tpcds_jdbc')

  def test_tpcds_q28(self, vector):
    self.run_test_case('tpcds-decimal_v2-q28', vector, use_db='tpcds_jdbc')

  def test_tpcds_q29(self, vector):
    self.run_test_case('tpcds-decimal_v2-q29', vector, use_db='tpcds_jdbc')

  def test_tpcds_q30(self, vector):
    self.run_test_case('tpcds-decimal_v2-q30', vector, use_db='tpcds_jdbc')

  def test_tpcds_q31(self, vector):
    self.run_test_case('tpcds-decimal_v2-q31', vector, use_db='tpcds_jdbc')

  def test_tpcds_q32(self, vector):
    self.run_test_case('tpcds-decimal_v2-q32', vector, use_db='tpcds_jdbc')

  def test_tpcds_q33(self, vector):
    self.run_test_case('tpcds-decimal_v2-q33', vector, use_db='tpcds_jdbc')

  def test_tpcds_q34(self, vector):
    self.run_test_case('tpcds-decimal_v2-q34', vector, use_db='tpcds_jdbc')

  def test_tpcds_q35a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q35a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q36(self, vector):
    self.run_test_case('tpcds-decimal_v2-q36', vector, use_db='tpcds_jdbc')

  def test_tpcds_q37(self, vector):
    self.run_test_case('tpcds-decimal_v2-q37', vector, use_db='tpcds_jdbc')

  def test_tpcds_q38(self, vector):
    self.run_test_case('tpcds-decimal_v2-q38', vector, use_db='tpcds_jdbc')

  def test_tpcds_q39_1(self, vector):
    self.run_test_case('tpcds-decimal_v2-q39-1', vector, use_db='tpcds_jdbc')

  def test_tpcds_q39_2(self, vector):
    self.run_test_case('tpcds-decimal_v2-q39-2', vector, use_db='tpcds_jdbc')

  def test_tpcds_q40(self, vector):
    self.run_test_case('tpcds-decimal_v2-q40', vector, use_db='tpcds_jdbc')

  def test_tpcds_q41(self, vector):
    self.run_test_case('tpcds-decimal_v2-q41', vector, use_db='tpcds_jdbc')

  def test_tpcds_q42(self, vector):
    self.run_test_case('tpcds-decimal_v2-q42', vector, use_db='tpcds_jdbc')

  def test_tpcds_q43(self, vector):
    self.run_test_case('tpcds-decimal_v2-q43', vector, use_db='tpcds_jdbc')

  def test_tpcds_q44(self, vector):
    self.run_test_case('tpcds-decimal_v2-q44', vector, use_db='tpcds_jdbc')

  def test_tpcds_q45(self, vector):
    self.run_test_case('tpcds-decimal_v2-q45', vector, use_db='tpcds_jdbc')

  def test_tpcds_q46(self, vector):
    self.run_test_case('tpcds-decimal_v2-q46', vector, use_db='tpcds_jdbc')

  def test_tpcds_q47(self, vector):
    self.run_test_case('tpcds-decimal_v2-q47', vector, use_db='tpcds_jdbc')

  def test_tpcds_q48(self, vector):
    self.run_test_case('tpcds-decimal_v2-q48', vector, use_db='tpcds_jdbc')

  def test_tpcds_q49(self, vector):
    self.run_test_case('tpcds-decimal_v2-q49', vector, use_db='tpcds_jdbc')

  def test_tpcds_q50(self, vector):
    self.run_test_case('tpcds-decimal_v2-q50', vector, use_db='tpcds_jdbc')

  def test_tpcds_q51(self, vector):
    self.run_test_case('tpcds-decimal_v2-q51', vector, use_db='tpcds_jdbc')

  def test_tpcds_q51a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q51a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q52(self, vector):
    self.run_test_case('tpcds-decimal_v2-q52', vector, use_db='tpcds_jdbc')

  def test_tpcds_q53(self, vector):
    self.run_test_case('tpcds-decimal_v2-q53', vector, use_db='tpcds_jdbc')

  def test_tpcds_q54(self, vector):
    self.run_test_case('tpcds-decimal_v2-q54', vector, use_db='tpcds_jdbc')

  def test_tpcds_q55(self, vector):
    self.run_test_case('tpcds-decimal_v2-q55', vector, use_db='tpcds_jdbc')

  def test_tpcds_q56(self, vector):
    self.run_test_case('tpcds-decimal_v2-q56', vector, use_db='tpcds_jdbc')

  def test_tpcds_q57(self, vector):
    self.run_test_case('tpcds-decimal_v2-q57', vector, use_db='tpcds_jdbc')

  def test_tpcds_q58(self, vector):
    self.run_test_case('tpcds-decimal_v2-q58', vector, use_db='tpcds_jdbc')

  def test_tpcds_q59(self, vector):
    self.run_test_case('tpcds-decimal_v2-q59', vector, use_db='tpcds_jdbc')

  def test_tpcds_q60(self, vector):
    self.run_test_case('tpcds-decimal_v2-q60', vector, use_db='tpcds_jdbc')

  def test_tpcds_q61(self, vector):
    self.run_test_case('tpcds-decimal_v2-q61', vector, use_db='tpcds_jdbc')

  def test_tpcds_q62(self, vector):
    self.run_test_case('tpcds-decimal_v2-q62', vector, use_db='tpcds_jdbc')

  def test_tpcds_q63(self, vector):
    self.run_test_case('tpcds-decimal_v2-q63', vector, use_db='tpcds_jdbc')

  def test_tpcds_q64(self, vector):
    self.run_test_case('tpcds-decimal_v2-q64', vector, use_db='tpcds_jdbc')

  def test_tpcds_q65(self, vector):
    self.run_test_case('tpcds-decimal_v2-q65', vector, use_db='tpcds_jdbc')

  def test_tpcds_q66(self, vector):
    self.run_test_case('tpcds-decimal_v2-q66', vector, use_db='tpcds_jdbc')

  def test_tpcds_q67(self, vector):
    self.run_test_case('tpcds-decimal_v2-q67', vector, use_db='tpcds_jdbc')

  def test_tpcds_q67a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q67a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q68(self, vector):
    self.run_test_case('tpcds-decimal_v2-q68', vector, use_db='tpcds_jdbc')

  def test_tpcds_q69(self, vector):
    self.run_test_case('tpcds-decimal_v2-q69', vector, use_db='tpcds_jdbc')

  def test_tpcds_q70(self, vector):
    self.run_test_case('tpcds-decimal_v2-q70', vector, use_db='tpcds_jdbc')

  def test_tpcds_q70a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q70a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q71(self, vector):
    self.run_test_case('tpcds-decimal_v2-q71', vector, use_db='tpcds_jdbc')

  def test_tpcds_q72(self, vector):
    self.run_test_case('tpcds-decimal_v2-q72', vector, use_db='tpcds_jdbc')

  def test_tpcds_q73(self, vector):
    self.run_test_case('tpcds-decimal_v2-q73', vector, use_db='tpcds_jdbc')

  def test_tpcds_q74(self, vector):
    self.run_test_case('tpcds-decimal_v2-q74', vector, use_db='tpcds_jdbc')

  def test_tpcds_q75(self, vector):
    self.run_test_case('tpcds-decimal_v2-q75', vector, use_db='tpcds_jdbc')

  def test_tpcds_q76(self, vector):
    self.run_test_case('tpcds-decimal_v2-q76', vector, use_db='tpcds_jdbc')

  def test_tpcds_q77(self, vector):
    self.run_test_case('tpcds-decimal_v2-q77', vector, use_db='tpcds_jdbc')

  def test_tpcds_q77a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q77a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q78(self, vector):
    self.run_test_case('tpcds-decimal_v2-q78', vector, use_db='tpcds_jdbc')

  def test_tpcds_q79(self, vector):
    self.run_test_case('tpcds-decimal_v2-q79', vector, use_db='tpcds_jdbc')

  def test_tpcds_q80(self, vector):
    self.run_test_case('tpcds-decimal_v2-q80', vector, use_db='tpcds_jdbc')

  def test_tpcds_q80a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q80a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q81(self, vector):
    self.run_test_case('tpcds-decimal_v2-q81', vector, use_db='tpcds_jdbc')

  def test_tpcds_q82(self, vector):
    self.run_test_case('tpcds-decimal_v2-q82', vector, use_db='tpcds_jdbc')

  def test_tpcds_q83(self, vector):
    self.run_test_case('tpcds-decimal_v2-q83', vector, use_db='tpcds_jdbc')

  def test_tpcds_q84(self, vector):
    self.run_test_case('tpcds-decimal_v2-q84', vector, use_db='tpcds_jdbc')

  def test_tpcds_q85(self, vector):
    self.run_test_case('tpcds-decimal_v2-q85', vector, use_db='tpcds_jdbc')

  def test_tpcds_q86(self, vector):
    self.run_test_case('tpcds-decimal_v2-q86', vector, use_db='tpcds_jdbc')

  def test_tpcds_q86a(self, vector):
    self.run_test_case('tpcds-decimal_v2-q86a', vector, use_db='tpcds_jdbc')

  def test_tpcds_q87(self, vector):
    self.run_test_case('tpcds-decimal_v2-q87', vector, use_db='tpcds_jdbc')

  def test_tpcds_q88(self, vector):
    self.run_test_case('tpcds-decimal_v2-q88', vector, use_db='tpcds_jdbc')

  def test_tpcds_q89(self, vector):
    self.run_test_case('tpcds-decimal_v2-q89', vector, use_db='tpcds_jdbc')

  def test_tpcds_q90(self, vector):
    self.run_test_case('tpcds-decimal_v2-q90', vector, use_db='tpcds_jdbc')

  def test_tpcds_q91(self, vector):
    self.run_test_case('tpcds-decimal_v2-q91', vector, use_db='tpcds_jdbc')

  def test_tpcds_q92(self, vector):
    self.run_test_case('tpcds-decimal_v2-q92', vector, use_db='tpcds_jdbc')

  def test_tpcds_q93(self, vector):
    self.run_test_case('tpcds-decimal_v2-q93', vector, use_db='tpcds_jdbc')

  def test_tpcds_q94(self, vector):
    self.run_test_case('tpcds-decimal_v2-q94', vector, use_db='tpcds_jdbc')

  def test_tpcds_q95(self, vector):
    self.run_test_case('tpcds-decimal_v2-q95', vector, use_db='tpcds_jdbc')

  def test_tpcds_q96(self, vector):
    self.run_test_case('tpcds-decimal_v2-q96', vector, use_db='tpcds_jdbc')

  def test_tpcds_q97(self, vector):
    self.run_test_case('tpcds-decimal_v2-q97', vector, use_db='tpcds_jdbc')

  def test_tpcds_q98(self, vector):
    self.run_test_case('tpcds-decimal_v2-q98', vector, use_db='tpcds_jdbc')

  def test_tpcds_q99(self, vector):
    self.run_test_case('tpcds-decimal_v2-q99', vector, use_db='tpcds_jdbc')
