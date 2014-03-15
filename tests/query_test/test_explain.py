#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Functional tests running EXPLAIN statements.
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

# Tests the different explain levels [0-3] on a few queries.
# TODO: Clean up this test to use an explain level test dimension and appropriate
# result sub-sections for the expected explain plans.
class TestExplain(ImpalaTestSuite):
  # Value for the num_scanner_threads query option to ensure that the memory estimates of
  # scan nodes are consistent even when run on machines with different numbers of cores.
  NUM_SCANNER_THREADS = 1

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestExplain, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('exec_option')['batch_size'] == 0 and\
        v.get_value('exec_option')['disable_codegen'] == False and\
        v.get_value('exec_option')['num_nodes'] != 1)

  def test_explain_level0(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 0
    self.run_test_case('QueryTest/explain-level0', vector)

  def test_explain_level1(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 1
    self.run_test_case('QueryTest/explain-level1', vector)

  @pytest.mark.xfail(run=False, reason="The test for missing table stats fails for avro")
  def test_explain_level2(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 2
    self.run_test_case('QueryTest/explain-level2', vector)

  @pytest.mark.xfail(run=False, reason="The test for missing table stats fails for avro")
  def test_explain_level3(self, vector):
    vector.get_value('exec_option')['num_scanner_threads'] = self.NUM_SCANNER_THREADS
    vector.get_value('exec_option')['explain_level'] = 3
    self.run_test_case('QueryTest/explain-level3', vector)
