# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted performance tests.
# These queries are already run as part of our performance benchmarking.
# Additionally, we don't get any 'extra' coverage from them, so they're
# not an essential part of functional verification.
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite

class TestTargetedPerf(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'targeted-perf'

  @classmethod
  def add_test_dimension(cls):
    super(TestTargetedPerf, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v: v.get_value('exec_option')['batch_size'] == 0)

  def test_perf_aggregation(self, vector):
    self.run_test_case('aggregation', vector)

  def test_perf_limit(self, vector):
    self.run_test_case('limit', vector)

  def test_perf_string(self, vector):
    self.run_test_case('string', vector)
