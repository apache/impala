# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Targeted Impala insert tests

import os
import pytest

from shutil import rmtree
from subprocess import check_call
from tempfile import mkdtemp as make_tmp_dir

from tests.common.environ import impalad_basedir
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfIsilon, SkipIfLocal
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_vector import TestDimension
from tests.util.filesystem_utils import get_fs_path, WAREHOUSE

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
  @SkipIfLocal.multiple_impalad
  def test_insert_parquet(self, vector):
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = \
        vector.get_value('file_size')
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('insert_parquet', vector, multiple_impalad=True)

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

  @SkipIfLocal.multiple_impalad
  def test_insert_parquet_invalid_codec(self, vector):
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert_parquet_invalid_codec', vector,\
                       multiple_impalad=True)


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
  @SkipIfLocal.hdfs_client
  def test_insert_parquet_verify_size(self, vector):
    # Test to verify that the result file size is close to what we expect.i
    TBL = "parquet_insert_size"
    DROP = "drop table if exists {0}".format(TBL)
    CREATE = ("create table parquet_insert_size like tpch_parquet.orders"
              " stored as parquet location '{0}/{1}'".format(WAREHOUSE, TBL))
    QUERY = "insert overwrite {0} select * from tpch.orders".format(TBL)
    DIR = get_fs_path("test-warehouse/{0}/".format(TBL))
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
    sizes = self.filesystem_client.get_all_file_sizes(DIR)
    for size in sizes:
      assert size < BLOCK_SIZE, "File size greater than expected.\
          Expected: {0}, Got: {1}".format(BLOCK_SIZE, size)
      if size < BLOCK_SIZE * 0.80:
        assert found_small_file == False
        found_small_file = True

class TestHdfsParquetTableWriter(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsParquetTableWriter, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_def_level_encoding(self, vector, unique_database):
    """IMPALA-3376: Tests that parquet files are written to HDFS correctly by generating a
    parquet table and running the parquet-reader tool on it, which performs sanity
    checking, such as that the correct number of definition levels were encoded.
    """
    table_name = "test_hdfs_parquet_table_writer"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
    self.execute_query("drop table if exists %s" % qualified_table_name)
    self.execute_query("create table %s stored as parquet as select l_linenumber from "
        "tpch_parquet.lineitem limit 180000" % qualified_table_name)

    tmp_dir = make_tmp_dir()
    try:
      hdfs_file = get_fs_path('/test-warehouse/%s.db/%s/*.parq'
          % (unique_database, table_name))
      check_call(['hdfs', 'dfs', '-copyToLocal', hdfs_file, tmp_dir])

      for root, subdirs, files in os.walk(tmp_dir):
        for f in files:
          if not f.endswith('parq'):
            continue
          check_call([os.path.join(impalad_basedir, 'util/parquet-reader'), '--file',
              os.path.join(tmp_dir, str(f))])
    finally:
      self.execute_query("drop table %s" % qualified_table_name)
      rmtree(tmp_dir)
