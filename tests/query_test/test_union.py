#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Tests for Union
#
import logging
import pytest
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.impala_test_suite import ImpalaTestSuite, SINGLE_NODE_ONLY

class TestUnion(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnion, cls).add_test_dimensions()

  def test_union(self, vector):
    self.run_test_case('QueryTest/union', vector)
