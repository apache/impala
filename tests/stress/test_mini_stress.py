#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
import pytest
from time import sleep
from tests.common.test_vector import TestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.test_file_parser import QueryTestSectionReader

# Number of times to execute each query
NUM_ITERATIONS = 5

# Each client will get a different test id.
TEST_IDS = xrange(0, 10)

# Runs many queries in parallel. The goal is to have this complete in a reasonable amount
# of time and be run as part of all test runs.
class TestMiniStress(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMiniStress, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('test_id', *TEST_IDS))
    cls.TestMatrix.add_constraint(lambda v: v.get_value('exec_option')['batch_size'] == 0)

  @pytest.mark.stress
  def test_mini_stress(self, vector):
    for i in xrange(NUM_ITERATIONS):
      self.run_test_case('stress', vector)
