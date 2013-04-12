#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension

class TestInsertQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))
    # Insert is currently only supported for text and parquet
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['text', 'parquet'])
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert1(self, vector):
    self.run_test_case('QueryTest/insert', vector)

  @pytest.mark.execute_serially
  def test_insert_overwrite(self, vector):
    self.run_test_case('QueryTest/insert_overwrite', vector)

  @pytest.mark.execute_serially
  def test_insert_null(self, vector):
    # This test case doesn't make sense for parquet.
    if vector.get_value('table_format').file_format == 'parquet':
      pytest.skip()
    self.run_test_case('QueryTest/insert_null', vector)
