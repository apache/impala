#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestParquetInsertBase(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetInsertBase, cls).add_test_dimensions()
    # This test only inserts into the parquet format.
    cls.TestMatrix.add_dimension(TestDimension('table_format', \
        *[TableFormatInfo.create_from_string(cls.get_workload(), 'parquet/none')]))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('exec_option')['batch_size'] == 0)

class TestParquetInsertTpch(TestParquetInsertBase):
  @classmethod
  def get_workload(self):
    return 'tpch'

  def test_insert(self, vector):
    self.run_test_case('insert_parquet', vector)

class TestParquetInsertTpcds(TestParquetInsertBase):
  @classmethod
  def get_workload(self):
    return 'tpcds'

  def test_insert(self, vector):
    self.run_test_case('insert_parquet', vector)
