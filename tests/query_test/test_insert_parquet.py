#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension

# TODO: Add Gzip back.  IMPALA-424
PARQUET_CODECS = ['none', 'snappy']

# Test a smaller parquet file size as well
# TODO: these tests take a while so we don't want to go through too many sizes but
# we should in more exhaustive testing
PARQUET_FILE_SIZES = [0, 32 * 1024 * 1024]

class TestInsertParquetQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertParquetQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))

    cls.TestMatrix.add_dimension(TestDimension("compression_codec", *PARQUET_CODECS));
    cls.TestMatrix.add_dimension(TestDimension("file_size", *PARQUET_FILE_SIZES));

    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestInsertParquetQueries, cls).setup_class()

  @pytest.mark.execute_serially
  def test_insert_parquet(self, vector):
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = \
        vector.get_value('file_size')
    vector.get_value('exec_option')['PARQUET_COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('insert_parquet', vector, multiple_impalad=True)
