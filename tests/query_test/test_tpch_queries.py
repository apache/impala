#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running the TPCH workload.
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestTpchQuery(ImpalaTestSuite):
  @classmethod
  def get_dataset(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchQuery, cls).add_test_dimensions()
    # The tpch tests take a long time to execute so restrict the combinations they
    # execute over
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'seq' and\
        v.get_value('table_format').compression_codec == 'snap' and\
        v.get_value('table_format').compression_type != 'record')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] != 1)

  def test_tpch_q1(self, vector):
    self.run_test_case('tpch-q1', vector)

  @pytest.mark.execute_serially
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

  # TODO: This TPCH query is not yet supported.
  #  def test_tpch_q7(self, vector):
  #    self.run_test_case('tpch-q7', vector)

  def test_tpch_q8(self, vector):
    self.run_test_case('tpch-q8', vector)

  def test_tpch_q9(self, vector):
    self.run_test_case('tpch-q9', vector)

  def test_tpch_q10(self, vector):
    self.run_test_case('tpch-q10', vector)

  @pytest.mark.execute_serially
  def test_tpch_q11(self, vector):
    self.run_test_case('tpch-q11', vector)

  def test_tpch_q12(self, vector):
    self.run_test_case('tpch-q12', vector)

  def test_tpch_q13(self, vector):
    self.run_test_case('tpch-q13', vector)

  def test_tpch_q14(self, vector):
    self.run_test_case('tpch-q14', vector)

  @pytest.mark.execute_serially
  def test_tpch_q15(self, vector):
    self.run_test_case('tpch-q15', vector)

  @pytest.mark.execute_serially
  def test_tpch_q16(self, vector):
    self.run_test_case('tpch-q16', vector)

  @pytest.mark.execute_serially
  def test_tpch_q17(self, vector):
    self.run_test_case('tpch-q17', vector)

  @pytest.mark.execute_serially
  def test_tpch_q18(self, vector):
    self.run_test_case('tpch-q18', vector)

  def test_tpch_q19(self, vector):
    self.run_test_case('tpch-q19', vector)

  @pytest.mark.execute_serially
  def test_tpch_q20(self, vector):
    self.run_test_case('tpch-q20', vector)

  # TODO: Disabled due to outer join bug
  @pytest.mark.xfail(run=False)
  def test_tpch_q21(self, vector):
    self.run_test_case('tpch-q21', vector)

  # TODO: Disabled due to outer join bug
  @pytest.mark.xfail(run=False)
  def test_tpch_q22(self, vector):
    self.run_test_case('tpch-q22', vector)
