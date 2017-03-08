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

from collections import namedtuple
from shutil import rmtree
from subprocess import check_call
from tempfile import mkdtemp as make_tmp_dir
from parquet.ttypes import SortingColumn

from tests.common.environ import impalad_basedir
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfIsilon, SkipIfLocal, SkipIfS3
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata, decode_stats_value

# TODO: Add Gzip back.  IMPALA-424
PARQUET_CODECS = ['none', 'snappy']


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

ColumnStats = namedtuple('ColumnStats', ['name', 'min', 'max'])

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

    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.multiple_impalad
  @UniqueDatabase.parametrize(sync_ddl=True)
  def test_insert_parquet(self, vector, unique_database):
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = \
        vector.get_value('file_size')
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('insert_parquet', vector, unique_database, multiple_impalad=True)


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
    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.multiple_impalad
  def test_insert_parquet_invalid_codec(self, vector):
    vector.get_value('exec_option')['COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert_parquet_invalid_codec', vector,
                       multiple_impalad=True)


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
    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').file_format == 'parquet')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
                                        v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension("compression_codec", *PARQUET_CODECS))

  @SkipIfIsilon.hdfs_block_size
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

  def test_def_level_encoding(self, vector, unique_database):
    """IMPALA-3376: Tests that parquet files are written to HDFS correctly by generating a
    parquet table and running the parquet-reader tool on it, which performs sanity
    checking, such as that the correct number of definition levels were encoded.
    """
    table_name = "test_hdfs_parquet_table_writer"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
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

  def test_sorting_columns(self, vector, unique_database, tmpdir):
    """Tests that RowGroup::sorting_columns gets populated when specifying a sortby()
    insert hint."""
    source_table = "functional_parquet.alltypessmall"
    target_table = "test_write_sorting_columns"
    qualified_target_table = "{0}.{1}".format(unique_database, target_table)
    hdfs_path = get_fs_path("/test-warehouse/{0}.db/{1}/".format(unique_database,
        target_table))

    # Create table
    # TODO: Simplify once IMPALA-4167 (insert hints in CTAS) has been fixed.
    query = "create table {0} like {1} stored as parquet".format(qualified_target_table,
        source_table)
    self.execute_query(query)

    # Insert data
    query = ("insert into {0} partition(year, month) /* +sortby(int_col, id) */ "
        "select * from {1}").format(qualified_target_table, source_table)
    self.execute_query(query)

    # Download hdfs files and extract rowgroup metadata
    row_groups = []
    check_call(['hdfs', 'dfs', '-get', hdfs_path, tmpdir.strpath])

    for root, subdirs, files in os.walk(tmpdir.strpath):
      for f in files:
        parquet_file = os.path.join(root, str(f))
        file_meta_data = get_parquet_metadata(parquet_file)
        row_groups.extend(file_meta_data.row_groups)

    # Verify that the files have the sorted_columns set
    expected = [SortingColumn(4, False, False), SortingColumn(0, False, False)]
    for row_group in row_groups:
      assert row_group.sorting_columns == expected


@SkipIfIsilon.hive
@SkipIfLocal.hive
@SkipIfS3.hive
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

      if stats.min is None and stats.max is None:
        decoded.append(None)
        continue

      assert stats.min is not None and stats.max is not None
      min_value = decode_stats_value(schema, stats.min)
      max_value = decode_stats_value(schema, stats.max)
      decoded.append(ColumnStats(schema.name, min_value, max_value))

    assert len(decoded) == len(schemas)
    return decoded

  def _get_row_group_stats_from_file(self, parquet_file):
    """Returns a list of statistics for each row group in file 'parquet_file'. The result
    is a two-dimensional list, containing stats by row group and column."""
    file_meta_data = get_parquet_metadata(parquet_file)
    # We only support flat schemas, the additional element is the root element.
    schemas = file_meta_data.schema[1:]
    file_stats = []
    for row_group in file_meta_data.row_groups:
      num_columns = len(row_group.columns)
      assert num_columns == len(schemas)
      column_stats = [c.meta_data.statistics for c in row_group.columns]
      file_stats.append(self._decode_row_group_stats(schemas, column_stats))

    return file_stats

  def _get_row_group_stats_from_hdfs_folder(self, hdfs_path):
    """Returns a list of statistics for each row group in all parquet files in
    'hdfs_path'. The result is a two-dimensional list, containing stats by row group and
    column."""
    row_group_stats = []

    try:
      tmp_dir = make_tmp_dir()
      check_call(['hdfs', 'dfs', '-get', hdfs_path, tmp_dir])

      for root, subdirs, files in os.walk(tmp_dir):
        for f in files:
          parquet_file = os.path.join(root, str(f))
          row_group_stats.extend(self._get_row_group_stats_from_file(parquet_file))

    finally:
      rmtree(tmp_dir)

    return row_group_stats

  def _validate_min_max_stats(self, hdfs_path, expected_values, skip_col_idxs = None):
    """Validates that 'hdfs_path' contains exactly one parquet file and that the rowgroup
    statistics in that file match the values in 'expected_values'. Columns indexed by
    'skip_col_idx' are excluded from the verification of the expected values.
    """
    skip_col_idxs = skip_col_idxs or []
    # The caller has to make sure that the table fits into a single row group. We enforce
    # it here to make sure the results are predictable and independent of how the data
    # could get written across multiple files.
    row_group_stats = self._get_row_group_stats_from_hdfs_folder(hdfs_path)
    assert(len(row_group_stats)) == 1
    table_stats = row_group_stats[0]

    num_columns = len(table_stats)
    assert num_columns == len(expected_values)

    for col_idx, stats, expected in zip(range(num_columns), table_stats, expected_values):
      if col_idx in skip_col_idxs:
        continue
      if not expected:
        assert not stats
        continue
      assert stats == expected

  def _ctas_table_and_verify_stats(self, vector, unique_database, source_table,
                                   expected_values, hive_skip_col_idx = None):
    """Copies 'source_table' into a parquet table and makes sure that the row group
    statistics in the resulting parquet file match those in 'expected_values'. The
    comparison is performed against both Hive and Impala. For Hive, columns indexed by
    'hive_skip_col_idx' are excluded from the verification of the expected values.
    """
    table_name = "test_hdfs_parquet_table_writer"
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
                                                                 table_name))

    # Validate against Hive.
    self.execute_query("drop table if exists {0}".format(qualified_table_name))
    self.run_stmt_in_hive("create table {0} stored as parquet as select * from "
                          "{1}".format(qualified_table_name, source_table))
    self.execute_query("invalidate metadata {0}".format(qualified_table_name))
    self._validate_min_max_stats(hdfs_path, expected_values, hive_skip_col_idx)

    # Validate against Impala. Setting exec_single_node_rows_threshold and adding a limit
    # clause ensures that the query is executed on the coordinator, resulting in a single
    # parquet file being written.
    num_rows = self.execute_scalar("select count(*) from {0}".format(source_table))
    self.execute_query("drop table {0}".format(qualified_table_name))
    query = ("create table {0} stored as parquet as select * from {1} limit "
             "{2}").format(qualified_table_name, source_table, num_rows)
    vector.get_value('exec_option')['EXEC_SINGLE_NODE_ROWS_THRESHOLD'] = num_rows
    self.execute_query(query, vector.get_value('exec_option'))
    self._validate_min_max_stats(hdfs_path, expected_values)

  def test_write_statistics_alltypes(self, vector, unique_database):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    values.
    """
    # Expected values for functional.alltypes
    expected_min_max_values = [
        ColumnStats('id', 0, 7299),
        ColumnStats('bool_col', False, True),
        ColumnStats('tinyint_col', 0, 9),
        ColumnStats('smallint_col', 0, 9),
        ColumnStats('int_col', 0, 9),
        ColumnStats('bigint_col', 0, 90),
        ColumnStats('float_col', 0, RoundFloat(9.9, 1)),
        ColumnStats('double_col', 0, RoundFloat(90.9, 1)),
        None,
        None,
        None,
        ColumnStats('year', 2009, 2010),
        ColumnStats('month', 1, 12),
    ]

    # Skip comparison of unsupported columns types with Hive.
    hive_skip_col_idx = [8, 9, 10]

    self._ctas_table_and_verify_stats(vector, unique_database, "functional.alltypes",
                                      expected_min_max_values, hive_skip_col_idx)

  def test_write_statistics_decimal(self, vector, unique_database):
    """Test that Impala does not write statistics for decimal columns."""
    # Expected values for functional.decimal_tbl
    expected_min_max_values = [None, None, None, None, None, None]

    # Skip comparison of unsupported columns types with Hive.
    hive_skip_col_idx = range(len(expected_min_max_values))

    self._ctas_table_and_verify_stats(vector, unique_database, "functional.decimal_tbl",
                                      expected_min_max_values, hive_skip_col_idx)

  def test_write_statistics_multi_page(self, vector, unique_database):
    """Test that writing a parquet file populates the rowgroup statistics with the correct
    values. This test write a single parquet file with several pages per column.
    """
    # Expected values for tpch_parquet.customer
    expected_min_max_values = [
        ColumnStats('c_custkey', 1, 150000),
        None,
        None,
        ColumnStats('c_nationkey', 0, 24),
        None,
        None,
        None,
        None,
    ]

    # Skip comparison of unsupported columns types with Hive.
    hive_skip_col_idx = [1, 2, 4, 5, 6, 7]

    self._ctas_table_and_verify_stats(vector, unique_database, "tpch_parquet.customer",
                                      expected_min_max_values, hive_skip_col_idx)

  def test_write_statistics_null(self, vector, unique_database):
    """Test that we don't write min/max statistics for null columns."""
    expected_min_max_values = [None, None, None, None, None, None, None]

    # Skip comparison of unsupported columns types with Hive.
    hive_skip_col_idx = range(len(expected_min_max_values))

    self._ctas_table_and_verify_stats(vector, unique_database, "functional.nulltable",
                                      expected_min_max_values, hive_skip_col_idx)

  def test_write_statistics_char_types(self, vector, unique_database):
    """Test that Impala does not write statistics for char columns."""
    expected_min_max_values = [None, None, None]

    # Skip comparison of unsupported columns types with Hive.
    hive_skip_col_idx = range(len(expected_min_max_values))

    self._ctas_table_and_verify_stats(vector, unique_database, "functional.chars_formats",
                                      expected_min_max_values, hive_skip_col_idx)

  def test_write_statistics_negative(self, vector, unique_database):
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
        ColumnStats('id', -7299, 7298),
        ColumnStats('int_col', -9, 8),
        ColumnStats('bigint_col', -90, 80),
        ColumnStats('float_col', RoundFloat(-9.9, 1), RoundFloat(8.8, 1)),
        ColumnStats('double_col', RoundFloat(-90.9, 1), RoundFloat(80.8, 1)),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, qualified_view_name,
                                      expected_min_max_values)

  def test_write_statistics_multiple_row_groups(self, vector, unique_database):
    """Test that writing multiple row groups works as expected. This is done by inserting
    into a table using the sortby() hint and then making sure that the min and max values
    of row groups don't overlap."""
    source_table = "tpch_parquet.orders"
    target_table = "test_hdfs_parquet_table_writer"
    qualified_target_table = "{0}.{1}".format(unique_database, target_table)
    hdfs_path = get_fs_path("/test-warehouse/{0}.db/{1}/".format(
        unique_database, target_table))

    # Insert a large amount of data on a single backend with a limited parquet file size.
    # This will result in several files being written, exercising code that tracks
    # statistics for row groups.
    num_rows = self.execute_scalar("select count(*) from {0}".format(source_table))
    query = "create table {0} like {1} stored as parquet".format(qualified_target_table,
                                                                 source_table)
    self.execute_query(query, vector.get_value('exec_option'))
    query = ("insert into {0} /* +sortby(o_orderkey) */ select * from {1} limit"
             "{2}").format(qualified_target_table, source_table, num_rows)
    vector.get_value('exec_option')['EXEC_SINGLE_NODE_ROWS_THRESHOLD'] = num_rows
    vector.get_value('exec_option')['PARQUET_FILE_SIZE'] = 8 * 1024 * 1024
    self.execute_query(query, vector.get_value('exec_option'))

    # Get all stats for the o_orderkey column
    row_group_stats = self._get_row_group_stats_from_hdfs_folder(hdfs_path)
    assert len(row_group_stats) > 1
    orderkey_stats = [s[0] for s in row_group_stats]

    # Make sure that they don't overlap by ordering by the min value, then looking at
    # boundaries.
    orderkey_stats.sort(key = lambda s: s.min)
    for l, r in zip(orderkey_stats, orderkey_stats[1:]):
      assert l.max <= r.min

  def test_write_statistics_float_infinity(self, vector, unique_database):
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
        ColumnStats('f', float('-inf'), float('inf')),
        ColumnStats('d', float('-inf'), float('inf')),
    ]

    self._ctas_table_and_verify_stats(vector, unique_database, qualified_table_name,
                                      expected_min_max_values)
