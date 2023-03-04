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
from builtins import map, range, round
import os

from collections import namedtuple
from datetime import datetime, date
from decimal import Decimal
from subprocess import check_call
from parquet.ttypes import ColumnOrder, SortingColumn, TypeDefinedOrder, ConvertedType

from tests.common.environ import impalad_basedir
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfEC, SkipIfFS, SkipIfLocal
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import get_fs_path, WAREHOUSE
from tests.util.get_parquet_metadata import (decode_stats_value,
    get_parquet_metadata_from_hdfs_folder)

PARQUET_CODECS = ['none', 'snappy', 'gzip', 'zstd', 'lz4']
IMPALA_HOME = os.environ['IMPALA_HOME']


class RoundFloat():
  """Class to compare floats after rounding them to a specified number of digits. This
  can be used in scenarios where floating point precision is an issue.
  """
  def __init__(self, value, num_digits):
    self.value = value
    self.num_digits = num_digits

  def __eq__(self, numeral):
    """Compares this objects's value to a numeral after rounding it."""
    return round(self.value, self.num_digits) == round(numeral, self.num_digits)

  def __hash__(self):
    return hash(round(self.value, self.num_digits))


class TimeStamp():
  """Class to construct timestamps with a default format specifier."""
  def __init__(self, value):
    # This member must be called 'timetuple'. Only if this class has a member called
    # 'timetuple' will the datetime __eq__ function forward an unknown equality check to
    # this method by returning NotImplemented:
    # https://docs.python.org/2/library/datetime.html#datetime.datetime
    self.timetuple = datetime.strptime(value, '%Y-%m-%d %H:%M:%S.%f')

  def __eq__(self, other_timetuple):
    """Compares this objects's value to another timetuple."""
    return self.timetuple == other_timetuple

  def __hash__(self):
    return hash(self.timetuple)


class Date():
  """Class to compare dates specified as year-month-day to dates specified as days since
  epoch.
  """
  def __init__(self, year, month, day):
    self.days_since_epoch = (date(year, month, day) - date(1970, 1, 1)).days

  def __eq__(self, other_days_since_eopch):
    return self.days_since_epoch == other_days_since_eopch

  def __hash__(self):
    return hash(self.days_since_epoch)


ColumnStats = namedtuple('ColumnStats', ['name', 'min', 'max', 'null_count'])

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
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))

    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension("compression_codec", *PARQUET_CODECS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension("file_size", *PARQUET_FILE_SIZES))

    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.multiple_impalad
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_parquet(self, vector, unique_database):
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = \
        vector.get_value('file_size')
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('insert_parquet', vector, unique_database, multiple_impalad=True)


class TestParquetQueriesMultiCodecs(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetQueriesMultiCodecs, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_parquet_multi_codecs(self, vector, unique_database):
    # Tests that parquet files are written/read correctly when using multiple codecs
    self.run_test_case('QueryTest/insert_parquet_multi_codecs', vector, unique_database,
        multiple_impalad=True)
    base_table = "{0}.{1}".format(unique_database, "t1_default")
    test_table = "{0}.{1}".format(unique_database, "t1_zstd_gzip")
    # select all rows and compare the data in base_table and test_table
    base_result = self.execute_query("select * from {0} order by c3".format(base_table))
    test_result = self.execute_query("select * from {0} order by c3".format(test_table))
    verify_query_result_is_equal(test_result.data, base_result.data)


class TestInsertParquetInvalidCodec(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertParquetInvalidCodec, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension("compression_codec", 'bzip2'))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.multiple_impalad
  def test_insert_parquet_invalid_codec(self, vector, unique_database):
    self.run_test_case('QueryTest/insert_parquet_invalid_codec', vector, unique_database)


class TestInsertParquetVerifySize(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertParquetVerifySize, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension("compression_codec", *PARQUET_CODECS))

  @SkipIfFS.hdfs_block_size
  @SkipIfLocal.hdfs_client
  def test_insert_parquet_verify_size(self, vector, unique_database):
    # Test to verify that the result file size is close to what we expect.
    tbl_name = "parquet_insert_size"
    fq_tbl_name = unique_database + "." + tbl_name
    location = get_fs_path("test-warehouse/{0}.db/{1}/"
                           .format(unique_database, tbl_name))
    create = ("create table {0} like tpch_parquet.orders stored as parquet"
              .format(fq_tbl_name, location))
    query = "insert overwrite {0} select * from tpch.orders".format(fq_tbl_name)
    block_size = 40 * 1024 * 1024

    self.execute_query(create)
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = block_size
    vector.get_value('exec_option')['COMPRESSION_CODEC'] =\
        vector.get_value('compression_codec')
    vector.get_value('exec_option')['num_nodes'] = 1
    self.execute_query(query, vector.get_value('exec_option'))

    # Get the files in hdfs and verify. There can be at most 1 file that is smaller
    # that the block_size. The rest should be within 80% of it and not over.
    found_small_file = False
    sizes = self.filesystem_client.get_all_file_sizes(location)
    for size in sizes:
      assert size < block_size, "File size greater than expected.\
          Expected: {0}, Got: {1}".format(block_size, size)
      if size < block_size * 0.80:
        assert not found_small_file
        found_small_file = True


class TestHdfsParquetTableWriter(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsParquetTableWriter, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_def_level_encoding(self, vector, unique_database, tmpdir):
    """IMPALA-3376: Tests that parquet files are written to HDFS correctly by generating a
    parquet table and running the parquet-reader tool on it, which performs sanity
    checking, such as that the correct number of definition levels were encoded.
    """
    table_name = "test_hdfs_parquet_table_writer"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
    self.execute_query("create table %s stored as parquet as select l_linenumber from "
                       "tpch_parquet.lineitem limit 180000" % qualified_table_name)

    hdfs_file = get_fs_path('/test-warehouse/%s.db/%s/*.parq'
                            % (unique_database, table_name))
    check_call(['hdfs', 'dfs', '-copyToLocal', hdfs_file, tmpdir.strpath])

    for root, subdirs, files in os.walk(tmpdir.strpath):
      for f in files:
        if not f.endswith('parq'):
          continue
        check_call([os.path.join(IMPALA_HOME, "bin/run-binary.sh"),
                    os.path.join(impalad_basedir, 'util/parquet-reader'), '--file',
                    os.path.join(tmpdir.strpath, str(f))])

  def test_sorting_columns(self, vector, unique_database, tmpdir):
    """Tests that RowGroup::sorting_columns gets populated when the table has SORT BY
    columns."""
    source_table = "functional_parquet.alltypessmall"
    target_table = "test_write_sorting_columns"
    qualified_target_table = "{0}.{1}".format(unique_database, target_table)
    hdfs_path = get_fs_path("/test-warehouse/{0}.db/{1}/".format(unique_database,
        target_table))

    # Create table
    query = "create table {0} sort by (int_col, id) like {1} stored as parquet".format(
        qualified_target_table, source_table)
    self.execute_query(query)

    # Insert data
    query = ("insert into {0} partition(year, month) select * from {1}").format(
        qualified_target_table, source_table)
    self.execute_query(query)

    # Download hdfs files and extract rowgroup metadata
    file_metadata_list = get_parquet_metadata_from_hdfs_folder(hdfs_path, tmpdir.strpath)
    row_groups = []

    for file_metadata in file_metadata_list:
      row_groups.extend(file_metadata.row_groups)

    # Verify that the files have the sorted_columns set
    expected = [SortingColumn(4, False, False), SortingColumn(0, False, False)]
    for row_group in row_groups:
      assert row_group.sorting_columns == expected

  def test_set_column_orders(self, vector, unique_database, tmpdir):
    """Tests that the Parquet writers set FileMetaData::column_orders."""
    source_table = "functional_parquet.alltypessmall"
    target_table = "test_set_column_orders"
    qualified_target_table = "{0}.{1}".format(unique_database, target_table)
    hdfs_path = get_fs_path("/test-warehouse/{0}.db/{1}/".format(unique_database,
        target_table))

    # Create table
    query = "create table {0} like {1} stored as parquet".format(qualified_target_table,
        source_table)
    self.execute_query(query)

    # Insert data
    query = ("insert into {0} partition(year, month) select * from {1}").format(
        qualified_target_table, source_table)
    self.execute_query(query)

    # Download hdfs files and verify column orders
    file_metadata_list = get_parquet_metadata_from_hdfs_folder(hdfs_path, tmpdir.strpath)

    expected_col_orders = [ColumnOrder(TYPE_ORDER=TypeDefinedOrder())] * 11

    for file_metadata in file_metadata_list:
      assert file_metadata.column_orders == expected_col_orders

  def test_read_write_integer_logical_types(self, vector, unique_database, tmpdir):
    """IMPALA-5052: Read and write signed integer parquet logical types
    This test creates a src_tbl like a parquet file. The parquet file was generated
    to have columns with different signed integer logical types. The test verifies
    that parquet file written by the hdfs parquet table writer using the generated
    file has the same column type metadata as the generated one."""
    hdfs_path = "{1}/{0}.db/signed_integer_logical_types.parquet".\
        format(unique_database, WAREHOUSE)
    self.filesystem_client.copy_from_local(os.environ['IMPALA_HOME'] +
        '/testdata/data/signed_integer_logical_types.parquet', hdfs_path)
    # Create table with signed integer logical types
    src_tbl = "{0}.{1}".format(unique_database, "read_write_logical_type_src")
    create_tbl_stmt = """create table {0} like parquet "{1}"
        stored as parquet""".format(src_tbl, hdfs_path)
    result = self.execute_query_expect_success(self.client, create_tbl_stmt)
    # Check to see if the src_tbl column types matches the schema of the parquet
    # file from which it was generated
    result_src = self.execute_query_expect_success(self.client, "describe %s" %src_tbl)
    for line in result_src.data:
      line_split = line.split()
      if line_split[0] == "id":
        assert line_split[1] == 'int'
      elif line_split[0] == "tinyint_col":
        assert line_split[1] == 'tinyint'
      elif line_split[0] == "smallint_col":
        assert line_split[1] == 'smallint'
      elif line_split[0] == "int_col":
        assert line_split[1] == 'int'
      else:
        assert line_split[0] == 'bigint_col' and line_split[1] == 'bigint'

    # Insert values in this table
    insert_stmt = "insert into table {0} values(1, 2, 3, 4, 5)".format(src_tbl)
    result = self.execute_query_expect_success(self.client, insert_stmt)

    # To test the integer round tripping, a new dst_tbl is created by using the parquet
    # file written by the src_tbl and running the following tests -
    #   1. inserting same values into src and dst table and reading it back and comparing
    #      them.
    #   2. Ensuring that the column types in dst_tbl matches the column types in the
    #      schema of the parquet file that was used to generate the src_tbl
    result = self.execute_query_expect_success(self.client, "show files in %s" %src_tbl)
    hdfs_path = result.data[0].split("\t")[0]
    dst_tbl = "{0}.{1}".format(unique_database, "read_write_logical_type_dst")
    create_tbl_stmt = 'create table {0} like parquet "{1}"'.format(dst_tbl, hdfs_path)
    result = self.execute_query_expect_success(self.client, create_tbl_stmt)
    result_dst = self.execute_query_expect_success(self.client, "describe %s" % dst_tbl)
    for line in result_dst.data:
      line_split = line.split()
      if line_split[0] == "id":
        assert line_split[1] == 'int'
      elif line_split[0] == "tinyint_col":
        assert line_split[1] == 'tinyint'
      elif line_split[0] == "smallint_col":
        assert line_split[1] == 'smallint'
      elif line_split[0] == "int_col":
        assert line_split[1] == 'int'
      else:
        assert line_split[0] == 'bigint_col' and line_split[1] == 'bigint'

    insert_stmt = "insert into table {0} values(1, 2, 3, 4, 5)".format(dst_tbl)
    self.execute_query_expect_success(self.client, insert_stmt)
    # Check that the values inserted are same in both src and dst tables
    result_src = self.execute_query_expect_success(self.client, "select * from %s"
            % src_tbl)
    result_dst = self.execute_query_expect_success(self.client, "select * from %s"
            % dst_tbl)
    assert result_src.data == result_dst.data

  def _ctas_and_get_metadata(self, vector, unique_database, tmp_dir, source_table,
                             table_name="test_hdfs_parquet_table_writer"):
    """CTAS 'source_table' into a Parquet table and returns its Parquet metadata."""
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
                                                                 table_name))

    # Setting num_nodes = 1 ensures that the query is executed on the coordinator,
    # resulting in a single parquet file being written.
    query = ("create table {0} stored as parquet as select * from {1}").format(
      qualified_table_name, source_table)
    vector.get_value('exec_option')['num_nodes'] = 1
    self.execute_query_expect_success(self.client, query,
                                      vector.get_value('exec_option'))

    file_metadata_list = get_parquet_metadata_from_hdfs_folder(hdfs_path, tmp_dir)
    assert len(file_metadata_list) == 1
    assert file_metadata_list[0] is not None
    return file_metadata_list[0]

  @staticmethod
  def _get_schema(schemas, column_name):
    """Searches 'schemas' for a schema with name 'column_name'. Asserts if non is found.
    """
    for schema in schemas:
      if schema.name == column_name:
        return schema
    assert False, "schema element %s not found" % column_name

  @staticmethod
  def _check_only_one_member_var_is_set(obj, var_name):
    """Checks that 'var_name' is the only member of 'obj' that is not None. Useful to
    check Thrift unions."""
    keys = [k for k, v in vars(obj).items() if v is not None]
    assert keys == [var_name]

  def _check_no_logical_type(self, schemas, column_name):
    """Checks that the schema with name 'column_name' has no logical or converted type."""
    schema = self._get_schema(schemas, column_name)
    assert schema.converted_type is None
    assert schema.logicalType is None

  def _check_int_logical_type(self, schemas, column_name, bit_width):
    """Checks that the schema with name 'column_name' has logical and converted type that
    describe a signed integer with 'bit_width' bits."""
    schema = self._get_schema(schemas, column_name)
    bit_width_to_converted_type_map = {
        8: ConvertedType.INT_8,
        16: ConvertedType.INT_16,
        32: ConvertedType.INT_32,
        64: ConvertedType.INT_64
    }
    assert schema.converted_type == bit_width_to_converted_type_map[bit_width]
    assert schema.logicalType is not None
    self._check_only_one_member_var_is_set(schema.logicalType, "INTEGER")
    assert schema.logicalType.INTEGER is not None
    assert schema.logicalType.INTEGER.bitWidth == bit_width
    assert schema.logicalType.INTEGER.isSigned

  def _check_decimal_logical_type(self, schemas, column_name, precision, scale):
    """Checks that the schema with name 'column_name' has logical and converted type that
    describe a decimal with given 'precision' and 'scale'."""
    schema = self._get_schema(schemas, column_name)
    assert schema.converted_type == ConvertedType.DECIMAL
    assert schema.precision == precision
    assert schema.scale == scale
    assert schema.logicalType is not None
    self._check_only_one_member_var_is_set(schema.logicalType, "DECIMAL")
    assert schema.logicalType.DECIMAL.precision == precision
    assert schema.logicalType.DECIMAL.scale == scale

  def test_logical_types(self, vector, unique_database, tmpdir):
    """Tests that the Parquet writers set logical type and converted type correctly
    for all types except DECIMAL"""
    source = "functional.alltypestiny"
    file_metadata = \
        self._ctas_and_get_metadata(vector, unique_database, tmpdir.strpath, source)
    schemas = file_metadata.schema

    self._check_int_logical_type(schemas, "tinyint_col", 8)
    self._check_int_logical_type(schemas, "smallint_col", 16)
    self._check_int_logical_type(schemas, "int_col", 32)
    self._check_int_logical_type(schemas, "bigint_col", 64)

    self._check_no_logical_type(schemas, "bool_col")
    self._check_no_logical_type(schemas, "float_col")
    self._check_no_logical_type(schemas, "double_col")

    # By default STRING has no logical type, see IMPALA-5982.
    self._check_no_logical_type(schemas, "string_col")

    # Currently TIMESTAMP is written as INT96 and has no logical type.
    # This test will break once INT64 becomes the default Parquet type for TIMESTAMP
    # columns in the future (IMPALA-5049).
    self._check_no_logical_type(schemas, "timestamp_col")

  def test_decimal_logical_types(self, vector, unique_database, tmpdir):
    """Tests that the Parquet writers set logical type and converted type correctly
       for DECIMAL type."""
    source = "functional.decimal_tiny"
    file_metadata = \
        self._ctas_and_get_metadata(vector, unique_database, tmpdir.strpath, source)
    schemas = file_metadata.schema

    self._check_decimal_logical_type(schemas, "c1", 10, 4)
    self._check_decimal_logical_type(schemas, "c2", 15, 5)
    self._check_decimal_logical_type(schemas, "c3", 1, 1)

  def _check_int64_timestamp_logical_type(self, schemas, column_name, unit):
    """Checks that the schema with name 'column_name' has logical and converted type that
       describe a timestamp with the given unit."""
    schema = self._get_schema(schemas, column_name)
    assert schema.logicalType is not None
    self._check_only_one_member_var_is_set(schema.logicalType, "TIMESTAMP")
    assert schema.logicalType.TIMESTAMP.unit is not None
    self._check_only_one_member_var_is_set(
        schema.logicalType.TIMESTAMP.unit, unit.upper())
    # Non UTC-normalized timestamps have no converted_type to avoid confusing older
    # readers that would interpret these as UTC-normalized.
    assert schema.converted_type is None
    assert not schema.logicalType.TIMESTAMP.isAdjustedToUTC

  def _ctas_and_check_int64_timestamps(self, vector, unique_database, tmpdir, unit):
    """CTAS a table using 'unit' int64 timestamps and checks columns metadata."""
    source = "functional.alltypestiny"
    timestamp_type = 'int64_' + unit
    vector.get_value('exec_option')['parquet_timestamp_type'] = timestamp_type
    file_metadata = self._ctas_and_get_metadata(vector, unique_database, tmpdir.strpath,
                                                source, table_name=timestamp_type)
    schemas = file_metadata.schema
    self._check_int64_timestamp_logical_type(schemas, "timestamp_col", unit)

  def test_int64_timestamp_logical_type(self, vector, unique_database, tmpdir):
    """Tests that correct metadata is written for int64 timestamps."""
    self._ctas_and_check_int64_timestamps(vector, unique_database, tmpdir, "millis")
    self._ctas_and_check_int64_timestamps(vector, unique_database, tmpdir, "micros")
    self._ctas_and_check_int64_timestamps(vector, unique_database, tmpdir, "nanos")

  # Skip test for non-HDFS environment as it uses Hive statement.
  # Hive statement is being used as Impala's result are converted
  # by python to string. In both HS2 and beewax, it only handles float
  # precision uptil 16 decimal digits and test needs 17.
  # IMPALA-9365 describes why HS2 is not started on non-HDFS test env.
  @SkipIfFS.hive
  def test_double_precision(self, vector, unique_database):
    # IMPALA-10654: Test inserting double into Parquet table retains the precision.
    src_tbl = "{0}.{1}".format(unique_database, "i10654_parquet")
    create_tbl_stmt = """create table {0} (dbl1 double)
        stored as parquet""".format(src_tbl)
    self.execute_query_expect_success(self.client, create_tbl_stmt)
    insert_tbl_stmt = """insert into table {0}
        values (-0.43149576573887316)""".format(src_tbl)
    self.execute_query_expect_success(self.client, insert_tbl_stmt)
    select_stmt = """select * from {0}""".format(src_tbl)
    result = self.run_stmt_in_hive(select_stmt)
    assert result.split('\n')[1] == '-0.43149576573887316'


@SkipIfFS.hive
# TODO: Should we move this to test_parquet_stats.py?
class TestHdfsParquetTableStatsWriter(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsParquetTableStatsWriter, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def _decode_row_group_stats(self, schemas, row_group_stats):
    """Decodes and return a list of statistics for a single row group."""
    decoded = []
    assert len(schemas) == len(row_group_stats)
    for schema, stats in zip(schemas, row_group_stats):
      if stats is None:
        decoded.append(None)
        continue
      min_value = None
      max_value = None

      if stats.min_value is not None and stats.max_value is not None:
        min_value = decode_stats_value(schema, stats.min_value)
        max_value = decode_stats_value(schema, stats.max_value)

      null_count = stats.null_count
      assert null_count is not None

      decoded.append(ColumnStats(schema.name, min_value, max_value, null_count))

    assert len(decoded) == len(schemas)
    return decoded

  def _get_row_group_stats_from_file_metadata(self, file_metadata):
    """Returns a list of statistics for each row group in Parquet file metadata
    'file_metadata'. The result is a two-dimensional list, containing stats by
    row group and column."""
    # We only support flat schemas, the additional element is the root element.
    schemas = file_metadata.schema[1:]
    file_stats = []
    for row_group in file_metadata.row_groups:
      num_columns = len(row_group.columns)
      assert num_columns == len(schemas)
      column_stats = [c.meta_data.statistics for c in row_group.columns]
      file_stats.append(self._decode_row_group_stats(schemas, column_stats))

    return file_stats

  def _get_row_group_stats_from_hdfs_folder(self, hdfs_path, tmp_dir):
    """Returns a list of statistics for each row group in all parquet files i 'hdfs_path'.
    'tmp_dir' needs to be supplied by the caller and will be used to store temporary
    files. The caller is responsible for cleaning up 'tmp_dir'. The result is a
    two-dimensional list, containing stats by row group and column."""
    row_group_stats = []

    file_metadata_list = get_parquet_metadata_from_hdfs_folder(hdfs_path, tmp_dir)
    for file_metadata in file_metadata_list:
      row_group_stats.extend(self._get_row_group_stats_from_file_metadata(file_metadata))

    return row_group_stats

  def _validate_parquet_stats(self, hdfs_path, tmp_dir, expected_values,
                              skip_col_idxs = None):
    """Validates that 'hdfs_path' contains exactly one parquet file and that the rowgroup
    statistics in that file match the values in 'expected_values'. Columns indexed by
    'skip_col_idx' are excluded from the verification of the expected values. 'tmp_dir'
    needs to be supplied by the caller and will be used to store temporary files. The
    caller is responsible for cleaning up 'tmp_dir'.
    """
    skip_col_idxs = skip_col_idxs or []
    # The caller has to make sure that the table fits into a single row group. We enforce
    # it here to make sure the results are predictable and independent of how the data
    # could get written across multiple files.
    row_group_stats = self._get_row_group_stats_from_hdfs_folder(hdfs_path, tmp_dir)
    assert(len(row_group_stats)) == 1
    table_stats = row_group_stats[0]

    num_columns = len(table_stats)
    assert num_columns == len(expected_values)

    for col_idx, stats, expected in zip(list(range(num_columns)),
                                        table_stats, expected_values):
      if col_idx in skip_col_idxs:
        continue
      if not expected:
        assert not stats
        continue
      assert stats == expected

  def _ctas_table_and_verify_stats(self, vector, unique_database, tmp_dir, source_table,
                                   expected_values,
                                   table_name="test_hdfs_parquet_table_writer"):
    """Copies 'source_table' into a parquet table and makes sure that the row group
    statistics in the resulting parquet file match those in 'expected_values'. 'tmp_dir'
    needs to be supplied by the caller and will be used to store temporary files. The
    caller is responsible for cleaning up 'tmp_dir'.
    """
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
                                                                 table_name))

    # Setting num_nodes = 1 ensures that the query is executed on the coordinator,
    # resulting in a single parquet file being written.
    self.execute_query("drop table if exists {0}".format(qualified_table_name))
    query = ("create table {0} stored as parquet as select * from {1}").format(
        qualified_table_name, source_table)
    vector.get_value('exec_option')['num_nodes'] = 1
    self.execute_query(query, vector.get_value('exec_option'))
    self._validate_parquet_stats(hdfs_path, tmp_dir, expected_values)

  def test_write_statistics_alltypes(self, vector, unique_database, tmpdir):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    values.
    """
    # Expected values for functional.alltypes
    expected_min_max_values = [
        ColumnStats('id', 0, 7299, 0),
        ColumnStats('bool_col', False, True, 0),
        ColumnStats('tinyint_col', 0, 9, 0),
        ColumnStats('smallint_col', 0, 9, 0),
        ColumnStats('int_col', 0, 9, 0),
        ColumnStats('bigint_col', 0, 90, 0),
        ColumnStats('float_col', 0, RoundFloat(9.9, 1), 0),
        ColumnStats('double_col', 0, RoundFloat(90.9, 1), 0),
        ColumnStats('date_string_col', '01/01/09', '12/31/10', 0),
        ColumnStats('string_col', '0', '9', 0),
        ColumnStats('timestamp_col', TimeStamp('2009-01-01 00:00:00.0'),
                    TimeStamp('2010-12-31 05:09:13.860000'), 0),
        ColumnStats('year', 2009, 2010, 0),
        ColumnStats('month', 1, 12, 0),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      "functional.alltypes", expected_min_max_values)

  def test_write_statistics_date(self, vector, unique_database, tmpdir):
    """Test that writing Date values to a parquet file populates the rowgroup statistics
    with the correct values.
    Date column statistics are tested separately as Date type is not supported across
    all file formats, therefore we couldn't add a Date column to 'alltypes' table yet.
    """
    expected_min_max_values = [
        ColumnStats('id_col', 0, 31, 0),
        ColumnStats('date_col', Date(1, 1, 1), Date(9999, 12, 31), 2),
        ColumnStats('date_part', Date(1, 1, 1), Date(9999, 12, 31), 0),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      "functional.date_tbl", expected_min_max_values)

  def test_write_statistics_decimal(self, vector, unique_database, tmpdir):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    values for decimal columns.
    """
    # Expected values for functional.decimal_tbl
    expected_min_max_values = [
        ColumnStats('d1', 1234, 132842, 0),
        ColumnStats('d2', 111, 2222, 0),
        ColumnStats('d3', Decimal('1.23456789'), Decimal('12345.6789'), 0),
        ColumnStats('d4', Decimal('0.123456789'), Decimal('0.123456789'), 0),
        ColumnStats('d5', Decimal('0.1'), Decimal('12345.789'), 0),
        ColumnStats('d6', 1, 1, 0)
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      "functional.decimal_tbl", expected_min_max_values)

  def test_write_statistics_multi_page(self, vector, unique_database, tmpdir):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    values. This test write a single parquet file with several pages per column.
    """
    # Expected values for tpch_parquet.customer
    expected_min_max_values = [
        ColumnStats('c_custkey', 1, 150000, 0),
        ColumnStats('c_name', 'Customer#000000001', 'Customer#000150000', 0),
        ColumnStats('c_address', '   2uZwVhQvwA', 'zzxGktzXTMKS1BxZlgQ9nqQ', 0),
        ColumnStats('c_nationkey', 0, 24, 0),
        ColumnStats('c_phone', '10-100-106-1617', '34-999-618-6881', 0),
        ColumnStats('c_acctbal', Decimal('-999.99'), Decimal('9999.99'), 0),
        ColumnStats('c_mktsegment', 'AUTOMOBILE', 'MACHINERY', 0),
        ColumnStats('c_comment', ' Tiresias according to the slyly blithe instructions '
                    'detect quickly at the slyly express courts. express dinos wake ',
                    'zzle. blithely regular instructions cajol', 0),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      "tpch_parquet.customer", expected_min_max_values)

  def test_write_statistics_null(self, vector, unique_database, tmpdir):
    """Test that we don't write min/max statistics for null columns. Ensure null_count
    is set for columns with null values."""
    expected_min_max_values = [
        ColumnStats('a', 'a', 'a', 0),
        ColumnStats('b', '', '', 0),
        ColumnStats('c', None, None, 1),
        ColumnStats('d', None, None, 1),
        ColumnStats('e', None, None, 1),
        ColumnStats('f', 'a\x00b', 'a\x00b', 0),
        ColumnStats('g', '\x00', '\x00', 0)
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      "functional.nulltable", expected_min_max_values)

  def test_write_statistics_char_types(self, vector, unique_database, tmpdir):
    """Test that Impala correctly writes statistics for char columns."""
    table_name = "test_char_types"
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)

    create_table_stmt = "create table {0} (c3 char(3), vc varchar, st string);".format(
        qualified_table_name)
    self.execute_query(create_table_stmt)

    insert_stmt = """insert into {0} values
        (cast("def" as char(3)), "ghj xyz", "abc xyz"),
        (cast("abc" as char(3)), "def 123 xyz", "lorem ipsum"),
        (cast("xy" as char(3)), "abc banana", "dolor dis amet")
        """.format(qualified_table_name)
    self.execute_query(insert_stmt)
    expected_min_max_values = [
        ColumnStats('c3', 'abc', 'xy', 0),
        ColumnStats('vc', 'abc banana', 'ghj xyz', 0),
        ColumnStats('st', 'abc xyz', 'lorem ipsum', 0)
    ]
    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_table_name, expected_min_max_values)

  def test_write_statistics_negative(self, vector, unique_database, tmpdir):
    """Test that Impala correctly writes statistics for negative values."""
    view_name = "test_negative_view"
    qualified_view_name = "{0}.{1}".format(unique_database, view_name)

    # Create a view to generate test data with negative values by negating every other
    # row.
    create_view_stmt = """create view {0} as select
        id * cast(pow(-1, id % 2) as int) as id,
        int_col * cast(pow(-1, id % 2) as int) as int_col,
        bigint_col * cast(pow(-1, id % 2) as bigint) as bigint_col,
        float_col * pow(-1, id % 2) as float_col,
        double_col * pow(-1, id % 2) as double_col
        from functional.alltypes""".format(qualified_view_name)
    self.execute_query(create_view_stmt)

    expected_min_max_values = [
        ColumnStats('id', -7299, 7298, 0),
        ColumnStats('int_col', -9, 8, 0),
        ColumnStats('bigint_col', -90, 80, 0),
        ColumnStats('float_col', RoundFloat(-9.9, 1), RoundFloat(8.8, 1), 0),
        ColumnStats('double_col', RoundFloat(-90.9, 1), RoundFloat(80.8, 1), 0),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_view_name, expected_min_max_values)

  def test_write_statistics_multiple_row_groups(self, vector, unique_database, tmpdir):
    """Test that writing multiple row groups works as expected. This is done by inserting
    into a table using the SORT BY clause and then making sure that the min and max values
    of row groups don't overlap."""
    source_table = "tpch_parquet.orders"
    target_table = "test_hdfs_parquet_table_writer"
    qualified_target_table = "{0}.{1}".format(unique_database, target_table)
    hdfs_path = get_fs_path("/test-warehouse/{0}.db/{1}/".format(
        unique_database, target_table))

    # Insert a large amount of data on a single backend with a limited parquet file size.
    # This will result in several files being written, exercising code that tracks
    # statistics for row groups.
    query = "create table {0} sort by (o_orderkey) like {1} stored as parquet".format(
        qualified_target_table, source_table)
    self.execute_query(query, vector.get_value('exec_option'))
    query = ("insert into {0} select * from {1}").format(
        qualified_target_table, source_table)
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_file_size'] = 8 * 1024 * 1024
    self.execute_query(query, vector.get_value('exec_option'))

    # Get all stats for the o_orderkey column
    row_group_stats = self._get_row_group_stats_from_hdfs_folder(hdfs_path,
                                                                 tmpdir.strpath)
    assert len(row_group_stats) > 1
    orderkey_stats = [s[0] for s in row_group_stats]

    # Make sure that they don't overlap by ordering by the min value, then looking at
    # boundaries.
    orderkey_stats.sort(key = lambda s: s.min)
    for l, r in zip(orderkey_stats, orderkey_stats[1:]):
      assert l.max <= r.min

  def test_write_statistics_float_infinity(self, vector, unique_database, tmpdir):
    """Test that statistics for -inf and inf are written correctly."""
    table_name = "test_float_infinity"
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)

    create_table_stmt = "create table {0} (f float, d double);".format(
        qualified_table_name)
    self.execute_query(create_table_stmt)

    insert_stmt = """insert into {0} values
        (cast('-inf' as float), cast('-inf' as double)),
        (cast('inf' as float), cast('inf' as double))""".format(qualified_table_name)
    self.execute_query(insert_stmt)

    expected_min_max_values = [
        ColumnStats('f', float('-inf'), float('inf'), 0),
        ColumnStats('d', float('-inf'), float('inf'), 0),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_table_name, expected_min_max_values)

  def test_write_null_count_statistics(self, vector, unique_database, tmpdir):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    null_count. This test ensures that the null_count is correct for a table with multiple
    null values."""

    # Expected values for tpch_parquet.customer
    expected_min_max_values = [
        ColumnStats('id', '8600000US00601', '8600000US999XX', 0),
        ColumnStats('zip', '00601', '999XX', 0),
        ColumnStats('description1', '\"00601 5-Digit ZCTA', '\"999XX 5-Digit ZCTA', 0),
        ColumnStats('description2', ' 006 3-Digit ZCTA\"', ' 999 3-Digit ZCTA\"', 0),
        ColumnStats('income', 0, 189570, 29),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
      "functional_parquet.zipcode_incomes", expected_min_max_values)

  def test_write_int64_timestamp_statistics(self, vector, unique_database, tmpdir):
    """Test that writing a parquet file populates the rowgroup statistics correctly for
    int64 milli/micro/nano timestamps."""
    table_name = "int96_nanos"
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)

    create_table_stmt = "create table {0} (ts timestamp);".format(qualified_table_name)
    self.execute_query(create_table_stmt)

    insert_stmt = """insert into {0} values
        ("1969-12-31 23:59:59.999999999"),
        ("1970-01-01 00:00:00.001001001")""".format(qualified_table_name)
    self.execute_query(insert_stmt)

    vector.get_value('exec_option')['parquet_timestamp_type'] = "int64_millis"
    expected_min_max_values = [
      ColumnStats('ts', -1, 1, 0)
    ]
    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_table_name,
                                      expected_min_max_values,
                                      table_name="int64_millis")

    vector.get_value('exec_option')['parquet_timestamp_type'] = "int64_micros"
    expected_min_max_values = [
      ColumnStats('ts', -1, 1001, 0)
    ]
    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_table_name,
                                      expected_min_max_values,
                                      table_name="int64_micros")

    # Insert values that fall outside the valid range for int64_nanos. These should
    # be inserted as NULLs and not affect min/max stats.
    insert_stmt = """insert into {0} values
        ("1677-09-21 00:12:43.145224191"),
        ("2262-04-11 23:47:16.854775808")""".format(qualified_table_name)
    self.execute_query(insert_stmt)

    vector.get_value('exec_option')['parquet_timestamp_type'] = "int64_nanos"
    expected_min_max_values = [
      ColumnStats('ts', -1, 1001001, 2)
    ]
    self._ctas_table_and_verify_stats(vector, unique_database, tmpdir.strpath,
                                      qualified_table_name,
                                      expected_min_max_values,
                                      table_name="int64_nanos")

  def test_too_many_columns(self, vector, unique_database):
    """Test that writing a Parquet table with too many columns results in an error."""
    num_cols = 12000
    query = "create table %s.wide stored as parquet as select \n" % unique_database
    query += ", ".join(map(str, range(num_cols)))
    query += ";\n"
    result = self.execute_query_expect_failure(self.client, query)
    assert "Minimum required block size must be less than 2GB" in str(result)
