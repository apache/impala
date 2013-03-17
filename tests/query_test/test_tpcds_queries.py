#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running the TPC-DS workload
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestTpcdsQuery(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpcds'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpcdsQuery, cls).add_test_dimensions()
    # Cut down on the execution time for these tests
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'rc' and\
        v.get_value('table_format').compression_codec in ['none', 'snap'] and\
        v.get_value('table_format').compression_type != 'record')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)

  def test_tpcds_q27(self, vector):
    self.run_test_case('tpcds-q27', vector)

  def test_tpcds_q42(self, vector):
    self.run_test_case('tpcds-q42', vector)

  def test_tpcds_q43(self, vector):
    self.run_test_case('tpcds-q43', vector)

  def test_tpcds_q46(self, vector):
    self.run_test_case('tpcds-q46', vector)

  def test_tpcds_q52(self, vector):
    self.run_test_case('tpcds-q52', vector)

  def test_tpcds_q55(self, vector):
    self.run_test_case('tpcds-q55', vector)

  def test_tpcds_q73(self, vector):
    self.run_test_case('tpcds-q73', vector)

  def test_tpcds_q79(self, vector):
    self.run_test_case('tpcds-q79', vector)

  def test_tpcds_q96(self, vector):
    self.run_test_case('tpcds-q96', vector)

  def test_tpcds_q98(self, vector):
    self.run_test_case('tpcds-q98', vector)
