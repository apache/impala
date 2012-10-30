#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for Impala joins
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestJoinQueries(ImpalaTestSuite):
  @classmethod
  def get_dataset(cls):
    return 'functional-query'

  def test_joins(self, vector):
    self.run_test_case('QueryTest/joins', vector)

  def test_outer_joins(self, vector):
    self.run_test_case('QueryTest/outer-joins', vector)
