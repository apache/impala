# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import os
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.skip import SkipIfS3, SkipIfIsilon
from tests.util.filesystem_utils import WAREHOUSE

# TODO: Add Gzip back.  IMPALA-424
PARQUET_CODECS = ['none', 'snappy']

# Test a smaller parquet file size as well
# TODO: these tests take a while so we don't want to go through too many sizes but
# we should in more exhaustive testing
PARQUET_FILE_SIZES = [0, 32 * 1024 * 1024]
@SkipIfS3.insert
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
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('insert_parquet', vector, multiple_impalad=True)

@SkipIfS3.insert
class TestInsertParquetInvalidCodec(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertParquetInvalidCodec, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.TestMatrix.add_dimension(TestDimension("compression_codec", 'bzip2'));
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestInsertParquetInvalidCodec, cls).setup_class()

  def test_insert_parquet_invalid_codec(self, vector):
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert_parquet_invalid_codec', vector,\
                       multiple_impalad=True)


@SkipIfS3.insert
class TestInsertParquetVerifySize(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertParquetVerifySize, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')
    cls.TestMatrix.add_dimension(TestDimension("compression_codec", *PARQUET_CODECS));

  @classmethod
  def setup_class(cls):
    super(TestInsertParquetVerifySize, cls).setup_class()

  @pytest.mark.execute_serially
  @SkipIfIsilon.hdfs_block_size
  def test_insert_parquet_verify_size(self, vector):
    # Test to verify that the result file size is close to what we expect.i
    TBL = "parquet_insert_size"
    DROP = "drop table if exists {0}".format(TBL)
    CREATE = ("create table parquet_insert_size like tpch_parquet.orders"
              " stored as parquet location '{0}/{1}'".format(WAREHOUSE, TBL))
    QUERY = "insert overwrite {0} select * from tpch.orders".format(TBL)
    DIR = "test-warehouse/{0}/".format(TBL)
    BLOCK_SIZE = 40 * 1024 * 1024

    self.execute_query(DROP)
    self.execute_query(CREATE)

    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = BLOCK_SIZE
    vector.get_value('exec_option')['COMPRESSION_CODEC'] =\
        vector.get_value('compression_codec')
    vector.get_value('exec_option')['num_nodes'] = 1
    self.execute_query(QUERY, vector.get_value('exec_option'))

    # Get the files in hdfs and verify. There can be at most 1 file that is smaller
    # that the BLOCK_SIZE. The rest should be within 80% of it and not over.
    found_small_file = False
    ls = self.hdfs_client.list_dir(DIR)
    for f in ls['FileStatuses']['FileStatus']:
      if f['type'] != 'FILE':
        continue
      length = f['length']
      print length
      assert length < BLOCK_SIZE
      if length < BLOCK_SIZE * 0.80:
        assert found_small_file == False
        found_small_file = True
