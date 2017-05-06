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
  def get_workload(self):
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
    self.run_test_case('tpcds-q1', vector)

  def test_tpcds_q2(self, vector):
    self.run_test_case('tpcds-q2', vector)

  def test_tpcds_q3(self, vector):
    self.run_test_case('tpcds-q3', vector)

  def test_tpcds_q4(self, vector):
    self.run_test_case('tpcds-q4', vector)

  def test_tpcds_q6(self, vector):
    self.run_test_case('tpcds-q6', vector)

  def test_tpcds_q7(self, vector):
    self.run_test_case('tpcds-q7', vector)

  def test_tpcds_q8(self, vector):
    self.run_test_case('tpcds-q8', vector)

  def test_tpcds_q19(self, vector):
    self.run_test_case('tpcds-q19', vector)

  def test_tpcds_q23(self, vector):
    self.run_test_case('tpcds-q23-1', vector)
    self.run_test_case('tpcds-q23-2', vector)

  def test_tpcds_q27(self, vector):
    self.run_test_case('tpcds-q27', vector)
    self.run_test_case('tpcds-q27a', vector)

  def test_tpcds_q28(self, vector):
    self.run_test_case('tpcds-q28', vector)

  def test_tpcds_q34(self, vector):
    self.run_test_case('tpcds-q34', vector)

  def test_tpcds_q42(self, vector):
    self.run_test_case('tpcds-q42', vector)

  def test_tpcds_q43(self, vector):
    self.run_test_case('tpcds-q43', vector)

  def test_tpcds_q46(self, vector):
    self.run_test_case('tpcds-q46', vector)

  def test_tpcds_q47(self, vector):
    self.run_test_case('tpcds-q47', vector)

  def test_tpcds_q52(self, vector):
    self.run_test_case('tpcds-q52', vector)

  def test_tpcds_q53(self, vector):
    self.run_test_case('tpcds-q53', vector)

  def test_tpcds_q55(self, vector):
    self.run_test_case('tpcds-q55', vector)

  def test_tpcds_q59(self, vector):
    self.run_test_case('tpcds-q59', vector)

  def test_tpcds_q61(self, vector):
    self.run_test_case('tpcds-q61', vector)

  def test_tpcds_q63(self, vector):
    self.run_test_case('tpcds-q63', vector)

  def test_tpcds_q65(self, vector):
    self.run_test_case('tpcds-q65', vector)

  def test_tpcds_q68(self, vector):
    self.run_test_case('tpcds-q68', vector)

  def test_tpcds_q73(self, vector):
    self.run_test_case('tpcds-q73', vector)

  def test_tpcds_q79(self, vector):
    self.run_test_case('tpcds-q79', vector)

  def test_tpcds_q88(self, vector):
    self.run_test_case('tpcds-q88', vector)

  def test_tpcds_q89(self, vector):
    self.run_test_case('tpcds-q89', vector)

  def test_tpcds_q96(self, vector):
    self.run_test_case('tpcds-q96', vector)

  def test_tpcds_q98(self, vector):
    self.run_test_case('tpcds-q98', vector)

class TestTpcdsInsert(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpcds-insert'

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
