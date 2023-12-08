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

from __future__ import absolute_import, division, print_function
import os
import pytest
import re

from testdata.common import widetable
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import (SkipIfFS, SkipIfLocal, SkipIfHive2,
    SkipIfNotHdfsMinicluster)
from tests.common.test_dimensions import (
    create_exec_option_dimension,
    create_uncompressed_text_dimension,
    create_single_exec_option_dimension,
    is_supported_insert_format)
from tests.common.test_result_verifier import (
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension
from tests.verifiers.metric_verifier import MetricVerifier

PARQUET_CODECS = ['none', 'snappy', 'gzip', 'zstd', 'lz4']

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
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0],
          sync_ddl=[0]))
      cls.ImpalaTestMatrix.add_dimension(
          create_uncompressed_text_dimension(cls.get_workload()))
    else:
      cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
          cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0, 1, 16],
          sync_ddl=[0, 1]))
      cls.ImpalaTestMatrix.add_dimension(
          ImpalaTestDimension("compression_codec", *PARQUET_CODECS));
      # Insert is currently only supported for text and parquet
      # For parquet, we want to iterate through all the compression codecs
      # TODO: each column in parquet can have a different codec.  We could
      # test all the codecs in one table/file with some additional flags.
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet' or \
            (v.get_value('table_format').file_format == 'text' and \
            v.get_value('compression_codec') == 'none'))
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').compression_codec == 'none')
      # Only test other batch sizes for uncompressed parquet to keep the execution time
      # within reasonable bounds.
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('exec_option')['batch_size'] == 0 or \
            (v.get_value('table_format').file_format == 'parquet' and \
            v.get_value('compression_codec') == 'none'))

  @pytest.mark.execute_serially
  def test_insert_large_string(self, vector, unique_database):
    """Test handling of large strings in inserter and scanner."""
    if "-Xcheck:jni" in os.environ.get("LIBHDFS_OPTS", ""):
      pytest.skip("Test unreasonably slow with JNI checking.")
    table_name = unique_database + ".insert_largestring"

    self.client.set_configuration_option("mem_limit", "4gb")
    self.client.set_configuration_option("max_row_size", "257mb")
    file_format = vector.get_value('table_format').file_format
    if file_format == "parquet":
      stored_as = file_format
    else:
      assert file_format == "text"
      stored_as = "textfile"
    self.client.execute("""
        create table {0}
        stored as {1} as
        select repeat('AZ', 128 * 1024 * 1024) as s""".format(table_name, stored_as))

    # Make sure it produces correct result when materializing no tuples.
    result = self.client.execute("select count(*) from {0}".format(table_name))
    assert result.data == ["1"]

    # Make sure it got the length right.
    result = self.client.execute("select length(s) from {0}".format(table_name))
    assert result.data == [str(2 * 128 * 1024 * 1024)]

    # Spot-check the data.
    result = self.client.execute(
        "select substr(s, 200 * 1024 * 1024, 5) from {0}".format(table_name))
    assert result.data == ["ZAZAZ"]

    # IMPALA-7648: test that we gracefully fail when there is not enough memory
    # to fit the scanned string in memory.
    # IMPALA-9856: Disable result spooling for this query since it is intended to test
    # for OOM.
    self.client.set_configuration_option("spool_query_results", "0")
    self.client.set_configuration_option("mem_limit", "50M")
    try:
      self.client.execute("select s from {0}".format(table_name))
      assert False, "Expected query to fail"
    except Exception as e:
      assert "Memory limit exceeded" in str(e)


  @classmethod
  def setup_class(cls):
    super(TestInsertQueries, cls).setup_class()

  @UniqueDatabase.parametrize(sync_ddl=True)
  # ABFS partition names cannot end in periods
  @SkipIfFS.file_or_folder_name_ends_with_period
  def test_insert(self, vector, unique_database):
    if (vector.get_value('table_format').file_format == 'parquet'):
      vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
          vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})

  @SkipIfHive2.acid
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_acid_insert(self, vector, unique_database):
    exec_options = vector.get_value('exec_option')
    file_format = vector.get_value('table_format').file_format
    if (file_format == 'parquet'):
      exec_options['COMPRESSION_CODEC'] = vector.get_value('compression_codec')
    exec_options['DEFAULT_FILE_FORMAT'] = file_format
    self.run_test_case('QueryTest/acid-insert', vector, unique_database,
        multiple_impalad=exec_options['sync_ddl'] == 1)

  @SkipIfHive2.acid
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_acid_nonacid_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-nonacid-insert', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @SkipIfHive2.acid
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_acid_insert_fail(self, vector, unique_database):
    self.run_test_case('QueryTest/acid-insert-fail', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @UniqueDatabase.parametrize(sync_ddl=True)
  @pytest.mark.execute_serially
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_insert_mem_limit(self, vector, unique_database):
    if (vector.get_value('table_format').file_format == 'parquet'):
      vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
          vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert-mem-limit', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})
    # IMPALA-7023: These queries can linger and use up memory, causing subsequent
    # tests to hit memory limits. Wait for some time to allow the query to
    # be reclaimed.
    verifiers = [MetricVerifier(i.service)
                 for i in ImpalaCluster.get_e2e_test_cluster().impalads]
    for v in verifiers:
      v.wait_for_metric("impala-server.num-fragments-in-flight", 0, timeout=180)

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_overwrite(self, vector, unique_database):
    self.run_test_case('QueryTest/insert_overwrite', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_bad_expr(self, vector, unique_database):
    # The test currently relies on codegen being disabled to trigger an error in
    # the output expression of the table sink.
    if vector.get_value('exec_option')['disable_codegen']:
      self.run_test_case('QueryTest/insert_bad_expr', vector, unique_database,
          multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1,
          test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
          .get_db_name_from_format(vector.get_value('table_format'))})

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_random_partition(self, vector, unique_database):
    """Regression test for IMPALA-402: partitioning by rand() leads to strange behaviour
    or crashes."""
    self.run_test_case('QueryTest/insert-random-partition', vector, unique_database,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

class TestInsertWideTable(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertWideTable, cls).add_test_dimensions()

    # Only vary codegen
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0]))

    # Inserts only supported on text and parquet
    # TODO: Enable 'text'/codec once the compressed text writers are in.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' or \
        v.get_value('table_format').file_format == 'text')
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

    # Don't run on core. This test is very slow (IMPALA-864) and we are unlikely to
    # regress here.
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v: False);

  @SkipIfLocal.parquet_file_size
  def test_insert_wide_table(self, vector, unique_database):
    table_format = vector.get_value('table_format')

    # Text can't handle as many columns as Parquet (codegen takes forever)
    num_cols = 1000 if table_format.file_format == 'text' else 2000

    table_name = unique_database + ".insert_widetable"
    if vector.get_value('exec_option')['disable_codegen']:
      table_name += "_codegen_disabled"

    col_descs = widetable.get_columns(num_cols)
    create_stmt = "CREATE TABLE " + table_name + "(" + ','.join(col_descs) + ")"
    if vector.get_value('table_format').file_format == 'parquet':
      create_stmt += " stored as parquet"
    self.client.execute(create_stmt)

    # Get a single row of data
    col_vals = widetable.get_data(num_cols, 1, quote_strings=True)[0]
    insert_stmt = "INSERT INTO " + table_name + " VALUES(" + col_vals + ")"
    self.client.execute(insert_stmt)

    result = self.client.execute("select count(*) from " + table_name)
    assert result.data == ["1"]

    result = self.client.execute("select * from " + table_name)
    types = result.column_types
    labels = result.column_labels
    expected = QueryTestResult([col_vals], types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels, order_matters=False)
    assert expected == actual

class TestInsertPartKey(ImpalaTestSuite):
  """Regression test for IMPALA-875"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertPartKey, cls).add_test_dimensions()
    # Only run for a single table type
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text'))
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert_part_key(self, vector):
    """Test that partition column exprs are cast to the correct type. See IMPALA-875."""
    self.run_test_case('QueryTest/insert_part_key', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  def test_escaped_partition_values(self, unique_database):
    """Test for special characters in partition values."""
    tbl = unique_database + ".tbl"
    self.execute_query(
        "create table {}(i int) partitioned by (p string) stored as parquet".format(tbl))
    # "\\'" is used to represent a single quote since the insert statement uses a single
    # quote on the partition value. "\\\\" is used for backslash since it gets escaped
    # again in parsing the insert statement.
    special_characters = "SpecialCharacters\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r" \
                         "\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B" \
                         "\x1C\x1D\x1E\x1F\"\x7F\\'%*/:=?\\\\{[]#^"
    part_value = "SpecialCharacters\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r" \
                 "\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B" \
                 "\x1C\x1D\x1E\x1F\"\x7F'%*/:=?\\{[]#^"
    part_dir = "p=SpecialCharacters%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%" \
                "10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%22%7F%27%25%2A" \
                "%2F%3A%3D%3F%5C%7B%5B%5D%23%5E"
    res = self.execute_query(
        "insert into {} partition(p='{}') values (0)".format(tbl, special_characters))
    assert res.data[0] == part_dir + ": 1"
    res = self.client.execute("select p from {}".format(tbl))
    assert res.data[0] == part_value
    res = self.execute_query("show partitions " + tbl)
    # There is a "\t" in the partition value making splitting of the result line
    # difficult, hence we only verify that the value is present in string
    # representation of the whole row.
    assert part_value in res.data[0]
    assert part_dir in res.data[0]

  def test_escaped_partition_values_iceberg(self, unique_database):
    """Test for special characters in partition values for iceberg tables"""
    tbl = unique_database + ".tbl"
    self.execute_query("create table {} (id int, p string) partitioned by"
                       " spec (identity(p)) stored by iceberg".format(tbl))
    # "\\'" is used to represent a single quote since the insert statement uses a single
    # quote on the partition value. "\\\\" is used for backslash since it gets escaped
    # again in parsing the insert statement.
    special_characters = "SpecialCharacters\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r" \
                         "\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B" \
                         "\x1C\x1D\x1E\x1F\"\x7F\\'%*/:=?\\\\{[]#^"
    part_value = "SpecialCharacters\x01\x02\x03\x04\x05\x06\x07\b\t\n\v\f\r" \
                 "\x0E\x0F\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1A\x1B" \
                 "\x1C\x1D\x1E\x1F\"\x7F'%*/:=?\\{[]#^"
    part_dir = "p=SpecialCharacters%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%" \
               "10%11%12%13%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%22%7F%27%25%2A" \
               "%2F%3A%3D%3F%5C%7B%5B%5D%23%5E"
    show_part_value = "SpecialCharacters\\u0001\\u0002\\u0003\\u0004\\u0005\\u0006" \
                      "\\u0007\\b\\t\\n\\u000B\\f\\r\\u000E\\u000F\\u0010" \
                      "\\u0011\\u0012\\u0013\\u0014\\u0015\\u0016\\u0017" \
                      "\\u0018\\u0019\\u001A\\u001B\\u001C\\u001D\\u001E" \
                     "\\u001F\\\"\\u007F\'%*\\/:=?\\\\{[]#^"
    res = self.execute_query(
        "insert into {} values (0, '{}')".format(tbl, special_characters))
    assert res.data[0] == part_dir + ": 1"
    res = self.client.execute("select p from {}".format(tbl))
    assert res.data[0] == part_value
    res = self.execute_query("show partitions " + tbl)
    # There is a "\t" in the partition value making splitting of the result line
    # difficult, hence we only verify that the value is present in string
    # representation of the whole row.
    assert show_part_value in res.data[0]


class TestInsertNullQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertNullQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))

    # These tests only make sense for inserting into a text table with special
    # logic to handle all the possible ways NULL needs to be written as ascii
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
          (v.get_value('table_format').file_format == 'text' and \
           v.get_value('table_format').compression_codec == 'none'))

  @classmethod
  def setup_class(cls):
    super(TestInsertNullQueries, cls).setup_class()

  def test_insert_null(self, vector, unique_database):
    self.run_test_case('QueryTest/insert_null', vector, unique_database,
        test_file_vars={'$ORIGINAL_DB': ImpalaTestSuite
        .get_db_name_from_format(vector.get_value('table_format'))})


class TestInsertFileExtension(ImpalaTestSuite):
  """Tests that files written to a table have the correct file extension. Asserts that
  Parquet files end with .parq and text files end with .txt."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertFileExtension, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        is_supported_insert_format(v.get_value('table_format')))
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  @classmethod
  def setup_class(cls):
    super(TestInsertFileExtension, cls).setup_class()

  def test_file_extension(self, vector, unique_database):
    table_format = vector.get_value('table_format').file_format
    if table_format == 'parquet':
      file_extension = '.parq'
      stored_as_format = 'parquet'
    else:
      file_extension = '.txt'
      stored_as_format = 'textfile'
    table_name = "{0}_table".format(table_format)
    ctas_query = "create table {0}.{1} stored as {2} as select 1".format(
        unique_database, table_name, stored_as_format)
    self.execute_query_expect_success(self.client, ctas_query)
    for path in self.filesystem_client.ls("test-warehouse/{0}.db/{1}".format(
        unique_database, table_name)):
      if not path.startswith('_'): assert path.endswith(file_extension)


class TestInsertHdfsWriterLimit(ImpalaTestSuite):
  """Test to make sure writer fragment instances are distributed evenly when using max
      hdfs_writers query option."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertHdfsWriterLimit, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'parquet'))

  @UniqueDatabase.parametrize(sync_ddl=True)
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_insert_writer_limit(self, unique_database):
    # Root internal (non-leaf) fragment.
    query = "create table {0}.test1 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2, mt_dop=0,
                                           expected_num_instances_per_host=[1, 2, 2])
    # Root coordinator fragment.
    query = "create table {0}.test2 as select int_col from " \
            "functional_parquet.alltypes limit 100000".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2, mt_dop=0,
                                           expected_num_instances_per_host=[1, 1, 2])
    # Root scan fragment. Instance count within limit.
    query = "create table {0}.test3 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=4, mt_dop=0,
                                           expected_num_instances_per_host=[1, 1, 1])

  @UniqueDatabase.parametrize(sync_ddl=True)
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_mt_dop_writer_limit(self, unique_database):
    # Root internal (non-leaf) fragment.
    query = "create table {0}.test1 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=11, mt_dop=10,
                                           expected_num_instances_per_host=[11, 12, 12])
    # Root coordinator fragment.
    query = "create table {0}.test2 as select int_col from " \
            "functional_parquet.alltypes limit 100000".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2, mt_dop=10,
                                           expected_num_instances_per_host=[8, 8, 9])
    # Root scan fragment. Instance count within limit.
    query = "create table {0}.test3 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=30, mt_dop=10,
                                           expected_num_instances_per_host=[8, 8, 8])

  @UniqueDatabase.parametrize(sync_ddl=True)
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_processing_cost_writer_limit(self, unique_database):
    """Test both scenario of partitioned and unpartitioned insert.
    All of the unpartitioned testscases will result in one instance writer because the
    output volume is less than 256MB. Partitoned insert will result in 1 writer per node
    unless max_fs_writers is set lower than num nodes."""
    # Root internal (non-leaf) fragment.
    query = "create table {0}.test1 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2,
                                           expected_num_instances_per_host=[1, 1, 2],
                                           processing_cost_min_threads=1)
    # Root coordinator fragment.
    query = "create table {0}.test2 as select int_col from " \
            "functional_parquet.alltypes limit 100000".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2,
                                           expected_num_instances_per_host=[1, 1, 2],
                                           processing_cost_min_threads=1)
    # Root internal (non-leaf) fragment. Instance count within limit.
    query = "create table {0}.test3 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=30,
                                           expected_num_instances_per_host=[1, 1, 2],
                                           processing_cost_min_threads=1)
    # Root internal (non-leaf) fragment. No max_fs_writers.
    # Scan node and writer sink should always be in separate fragment with cost-based
    # scaling.
    query = "create table {0}.test4 as select int_col from " \
            "functional_parquet.alltypes".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=0,
                                           expected_num_instances_per_host=[1, 1, 2],
                                           processing_cost_min_threads=1)
    # Partitioned insert with 6 distinct partition values.
    # Should create at least 1 writer per node.
    query = "create table {0}.test5 partitioned by (ss_store_sk) as " \
            "select ss_item_sk, ss_ticket_number, ss_store_sk " \
            "from tpcds_parquet.store_sales".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=0,
                                           expected_num_instances_per_host=[2, 2, 2],
                                           processing_cost_min_threads=1)
    # Partitioned insert can still be limited by max_fs_writers option.
    query = "create table {0}.test6 partitioned by (ss_store_sk) as " \
            "select ss_item_sk, ss_ticket_number, ss_store_sk " \
            "from tpcds_parquet.store_sales".format(unique_database)
    self.__run_insert_and_verify_instances(query, max_fs_writers=2,
                                           expected_num_instances_per_host=[1, 2, 2],
                                           processing_cost_min_threads=1)

  def __run_insert_and_verify_instances(self, query, max_fs_writers=0, mt_dop=0,
                                        processing_cost_min_threads=0,
                                        expected_num_instances_per_host=[]):
    self.client.set_configuration_option("max_fs_writers", max_fs_writers)
    self.client.set_configuration_option("mt_dop", mt_dop)
    if processing_cost_min_threads > 0:
      self.client.set_configuration_option("compute_processing_cost", "true")
      self.client.set_configuration_option("processing_cost_min_threads",
          processing_cost_min_threads)
    # Test depends on both planner and scheduler to see the same state of the cluster
    # having 3 executors, so to reduce flakiness we make sure all 3 executors are up
    # and running.
    self.impalad_test_service.wait_for_metric_value("cluster-membership.backends.total",
                                                    3)
    result = self.client.execute(query)
    assert 'HDFS WRITER' in result.exec_summary[0]['operator'], result.runtime_profile
    if (max_fs_writers > 0):
      num_writers = int(result.exec_summary[0]['num_instances'])
      assert (num_writers <= max_fs_writers), result.runtime_profile
    regex = r'Per Host Number of Fragment Instances' \
            r':.*?\((.*?)\).*?\((.*?)\).*?\((.*?)\).*?\n'
    matches = re.findall(regex, result.runtime_profile)
    assert len(matches) == 1 and len(matches[0]) == 3, result.runtime_profile
    num_instances_per_host = [int(i) for i in matches[0]]
    num_instances_per_host.sort()
    expected_num_instances_per_host.sort()
    assert num_instances_per_host == expected_num_instances_per_host, \
      result.runtime_profile
    self.client.clear_configuration()


class TestInsertNonPartitionedTable(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertNonPartitionedTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'text'
          and v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  @classmethod
  def setup_class(cls):
    super(TestInsertNonPartitionedTable, cls).setup_class()

  def test_insert_load_file_fail(self, vector, unique_database):
    """Tests metadata won't be corrupted after file metadata loading fails
    in non-partitioned tables."""
    table_name = '{0}.{1}'.format(unique_database, 'test_unpartition_tbl')
    self.client.execute('create table {0}(f0 int)'
        .format(table_name))
    self.client.execute('insert overwrite table {0} select 0'
        .format(table_name))
    result = self.client.execute("select f0 from {0}".format(table_name))
    assert result.data == ["0"]

    exec_options = vector.get_value('exec_option')
    exec_options['debug_action'] = 'catalogd_load_file_metadata_throw_exception'
    try:
      self.execute_query("insert overwrite table {0} select 1"
          .format(table_name), exec_options)
      assert False, "Expected query to fail."
    except Exception as e:
      assert "Failed to load metadata for table:" in str(e)

    exec_options['debug_action'] = ''
    self.execute_query("insert overwrite table {0} select 2"
        .format(table_name), exec_options)
    result = self.client.execute("select f0 from {0}".format(table_name))
    assert result.data == ["2"]


class TestUnsafeImplicitCasts(ImpalaTestSuite):
  """Test to check 'allow_unsafe_casts' query-option behaviour on insert statements."""
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnsafeImplicitCasts, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'parquet'))

  def test_unsafe_insert(self, vector, unique_database):
    create_stmt = """create table {0}.unsafe_insert(tinyint_col tinyint,
      smallint_col smallint, int_col int, bigint_col bigint, float_col float,
      double_col double, decimal_col decimal, timestamp_col timestamp, date_col date,
      string_col string, varchar_col varchar(100), char_col char(100),
      bool_col boolean, binary_col binary)""".format(unique_database)
    create_partitioned_stmt = """create table {0}.unsafe_insert_partitioned(int_col int,
      tinyint_col tinyint) partitioned by(string_col string)""".format(unique_database)
    self.client.execute(create_stmt)
    self.client.execute(create_partitioned_stmt)
    vector.get_value('exec_option')['allow_unsafe_casts'] = "true"
    self.run_test_case('QueryTest/insert-unsafe', vector, unique_database)
