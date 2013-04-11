#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for Impala joins
#
import logging
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestJoinQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestJoinQueries.BATCH_SIZES))
    # TODO: Look into splitting up join tests to accomodate hbase.
    # Joins with hbase tables produce drastically different results.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'hbase')

  def test_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/joins', new_vector)

  def test_outer_joins(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/outer-joins', new_vector)
