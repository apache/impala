#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted tests for decimal type.
#
import logging
import pytest
from copy import copy
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestDecimalQueries(ImpalaTestSuite):
  BATCH_SIZES = [0, 1]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDecimalQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestDecimalQueries.BATCH_SIZES))

    # On CDH4, hive does not support decimal so we can't run these tests against
    # the other file formats. Enable them on C5.
    cls.TestMatrix.add_constraint(lambda v:\
        (v.get_value('table_format').file_format == 'text' and
         v.get_value('table_format').compression_codec == 'none') or
         v.get_value('table_format').file_format == 'parquet')

  def test_queries(self, vector):
    new_vector = copy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/decimal', new_vector)

