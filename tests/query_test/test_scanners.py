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

# This test suite validates the scanners by running queries against ALL file formats and
# their permutations (e.g. compression codec/compression type). This works by exhaustively
# generating the table format test vectors for this specific test suite. This way, other
# tests can run with the normal exploration strategy and the overall test runtime doesn't
# explode.

import os
import pytest
import random
import re
import tempfile
from copy import deepcopy
from parquet.ttypes import ConvertedType
from subprocess import check_call

from testdata.common import widetable
from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import (
    SkipIfS3,
    SkipIfADLS,
    SkipIfIsilon,
    SkipIfLocal)
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_result_verifier import (
    parse_column_types,
    parse_column_labels,
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path
from tests.util.hdfs_util import NAMENODE
from tests.util.get_parquet_metadata import get_parquet_metadata
from tests.util.test_file_parser import QueryTestSectionReader


class TestScannersAllTableFormats(ImpalaTestSuite):
  BATCH_SIZES = [0, 1, 16]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormats, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      # The purpose of this test is to get some base coverage of all the file formats.
      # Even in 'core', we'll test each format by using the pairwise strategy.
      cls.ImpalaTestMatrix.add_dimension(cls.create_table_info_dimension('pairwise'))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('batch_size', *TestScannersAllTableFormats.BATCH_SIZES))

  def test_scanners(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/scanners', new_vector)

  def test_hdfs_scanner_profile(self, vector):
    if vector.get_value('table_format').file_format in ('kudu', 'hbase'):
      pytest.skip()
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 0
    self.run_test_case('QueryTest/hdfs_scanner_profile', new_vector)

# Test all the scanners with a simple limit clause. The limit clause triggers
# cancellation in the scanner code paths.
class TestScannersAllTableFormatsWithLimit(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormatsWithLimit, cls).add_test_dimensions()

  def test_limit(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 1
    self._test_limit(vector)
    # IMPALA-3337: when continuing on error, the error log should not show errors
    # (e.g. "Cancelled").
    vector.get_value('exec_option')['abort_on_error'] = 0
    self._test_limit(vector)

  def _test_limit(self, vector):
    # Use a small batch size so changing the limit affects the timing of cancellation
    vector.get_value('exec_option')['batch_size'] = 100
    iterations = 50
    query_template = "select * from alltypes limit %s"
    for i in range(1, iterations):
      # Vary the limit to vary the timing of cancellation
      limit = (iterations * 100) % 1000 + 1
      query = query_template % limit
      result = self.execute_query(query, vector.get_value('exec_option'),
          table_format=vector.get_value('table_format'))
      assert len(result.data) == limit
      # IMPALA-3337: The error log should be empty.
      assert not result.log

# Test case to verify the scanners work properly when the table metadata (specifically the
# number of columns in the table) does not match the number of columns in the data file.
class TestUnmatchedSchema(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnmatchedSchema, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Avro has a more advanced schema evolution process which is covered in more depth
    # in the test_avro_schema_evolution test suite.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format != 'avro')

  def _create_test_table(self, vector):
    """
    Creates the test table

    Cannot be done in a setup method because we need access to the current test vector
    """
    self._drop_test_table(vector)
    self.execute_query_using_client(self.client,
        "create external table jointbl_test like jointbl", vector)

    # Update the location of the new table to point the same location as the old table
    location = self._get_table_location('jointbl', vector)
    self.execute_query_using_client(self.client,
        "alter table jointbl_test set location '%s'" % location, vector)

  def _drop_test_table(self, vector):
    self.execute_query_using_client(self.client,
        "drop table if exists jointbl_test", vector)

  def test_unmatched_schema(self, vector):
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("IMPALA-2890: Missing Kudu DDL support")

    table_format = vector.get_value('table_format')
    # jointbl has no columns with unique values. When loaded in hbase, the table looks
    # different, as hbase collapses duplicates.
    if table_format.file_format == 'hbase':
      pytest.skip()
    self._create_test_table(vector)
    self.run_test_case('QueryTest/test-unmatched-schema', vector)
    self._drop_test_table(vector)


# Tests that scanners can read a single-column, single-row, 10MB table
class TestWideRow(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWideRow, cls).add_test_dimensions()
    # I can't figure out how to load a huge row into hbase
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format != 'hbase')

  def test_wide_row(self, vector):
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("KUDU-666: Kudu support for large values")

    new_vector = deepcopy(vector)
    # Use a 5MB scan range, so we will have to perform 5MB of sync reads
    new_vector.get_value('exec_option')['max_scan_range_length'] = 5 * 1024 * 1024
    # We need > 10 MB of memory because we're creating extra buffers:
    # - 10 MB table / 5 MB scan range = 2 scan ranges, each of which may allocate ~20MB
    # - Sync reads will allocate ~5MB of space
    # The 100MB value used here was determined empirically by raising the limit until the
    # query succeeded for all file formats -- I don't know exactly why we need this much.
    # TODO: figure out exact breakdown of memory usage (IMPALA-681)
    new_vector.get_value('exec_option')['mem_limit'] = 100 * 1024 * 1024
    self.run_test_case('QueryTest/wide-row', new_vector)

class TestWideTable(ImpalaTestSuite):
  # TODO: expand this to more rows when we have the capability
  NUM_COLS = [250, 500, 1000]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWideTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension("num_cols", *cls.NUM_COLS))
    # To cut down on test execution time, only run in exhaustive.
    if cls.exploration_strategy() != 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v: False)

  def test_wide_table(self, vector):
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("IMPALA-3718: Extend Kudu functional test support")

    NUM_COLS = vector.get_value('num_cols')
    # Due to the way HBase handles duplicate row keys, we have different number of
    # rows in HBase tables compared to HDFS tables.
    NUM_ROWS = 10 if vector.get_value('table_format').file_format != 'hbase' else 2
    DB_NAME = QueryTestSectionReader.get_db_name(vector.get_value('table_format'))
    TABLE_NAME = "%s.widetable_%s_cols" % (DB_NAME, NUM_COLS)

    result = self.client.execute("select count(*) from %s " % TABLE_NAME)
    assert result.data == [str(NUM_ROWS)]

    expected_result = widetable.get_data(NUM_COLS, NUM_ROWS, quote_strings=True)
    result = self.client.execute("select * from %s" % TABLE_NAME)

    if vector.get_value('table_format').file_format == 'hbase':
      assert len(result.data) == NUM_ROWS
      return

    types = parse_column_types(result.schema)
    labels = parse_column_labels(result.schema)
    expected = QueryTestResult(expected_result, types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels,
        order_matters=False)
    assert expected == actual


class TestParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquet, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet(self, vector):
    self.run_test_case('QueryTest/parquet', vector)

  def test_corrupt_files(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-continue-on-error', vector)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-abort-on-error', vector)

  def test_timestamp_out_of_range(self, vector, unique_database):
    """IMPALA-4363: Test scanning parquet files with an out of range timestamp."""
    self.client.execute(("create table {0}.out_of_range_timestamp (ts timestamp) "
        "stored as parquet").format(unique_database))
    out_of_range_timestamp_loc = get_fs_path(
        "/test-warehouse/{0}.db/{1}".format(unique_database, "out_of_range_timestamp"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/out_of_range_timestamp.parquet",
        out_of_range_timestamp_loc])

    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/out-of-range-timestamp-continue-on-error',
        vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/out-of-range-timestamp-abort-on-error',
        vector, unique_database)

  def test_zero_rows(self, vector, unique_database):
    """IMPALA-3943: Tests that scanning files with num_rows=0 in the file footer
    succeeds without errors."""
    # Create test table with a file that has 0 rows and 0 row groups.
    self.client.execute("create table %s.zero_rows_zero_row_groups (c int) "
        "stored as parquet" % unique_database)
    zero_rows_zero_row_groups_loc = get_fs_path(
        "/test-warehouse/%s.db/%s" % (unique_database, "zero_rows_zero_row_groups"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/zero_rows_zero_row_groups.parquet",
        zero_rows_zero_row_groups_loc])
    # Create test table with a file that has 0 rows and 1 row group.
    self.client.execute("create table %s.zero_rows_one_row_group (c int) "
        "stored as parquet" % unique_database)
    zero_rows_one_row_group_loc = get_fs_path(
        "/test-warehouse/%s.db/%s" % (unique_database, "zero_rows_one_row_group"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/zero_rows_one_row_group.parquet",
        zero_rows_one_row_group_loc])

    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-zero-rows', vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-zero-rows', vector, unique_database)

  def test_repeated_root_schema(self, vector, unique_database):
    """IMPALA-4826: Tests that running a scan on a schema where the root schema's
       repetetion level is set to REPEATED succeeds without errors."""
    self.client.execute("create table %s.repeated_root_schema (i int) "
        "stored as parquet" % unique_database)
    repeated_root_schema_loc = get_fs_path(
        "/test-warehouse/%s.db/%s" % (unique_database, "repeated_root_schema"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/repeated_root_schema.parquet",
        repeated_root_schema_loc])

    result = self.client.execute("select * from %s.repeated_root_schema" % unique_database)
    assert len(result.data) == 300

  def test_huge_num_rows(self, vector, unique_database):
    """IMPALA-5021: Tests that a zero-slot scan on a file with a huge num_rows in the
    footer succeeds without errors."""
    self.client.execute("create table %s.huge_num_rows (i int) stored as parquet"
      % unique_database)
    huge_num_rows_loc = get_fs_path(
        "/test-warehouse/%s.db/%s" % (unique_database, "huge_num_rows"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/huge_num_rows.parquet",
        huge_num_rows_loc])
    result = self.client.execute("select count(*) from %s.huge_num_rows"
      % unique_database)
    assert len(result.data) == 1
    assert "4294967294" in result.data

  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  @SkipIfS3.hive
  def test_multi_compression_types(self, vector, unique_database):
    """IMPALA-5448: Tests that parquet splits with multi compression types are counted
    correctly. Cases tested:
    - parquet file with columns using the same compression type
    - parquet files using snappy and gzip compression types
    """
    self.client.execute("create table %s.alltypes_multi_compression like"
        " functional_parquet.alltypes" % unique_database)
    hql_format = "set parquet.compression={codec};" \
        "insert into table %s.alltypes_multi_compression" \
        "  partition (year = {year}, month = {month})" \
        "  select id, bool_col, tinyint_col, smallint_col, int_col, bigint_col," \
        "    float_col, double_col,date_string_col,string_col,timestamp_col" \
        "  from functional_parquet.alltypes" \
        "  where year = {year} and month = {month}" % unique_database
    check_call(['hive', '-e', hql_format.format(codec="snappy", year=2010, month=1)])
    check_call(['hive', '-e', hql_format.format(codec="gzip", year=2010, month=2)])

    self.client.execute("create table %s.multi_compression (a string, b string)"
        " stored as parquet" % unique_database)
    multi_compression_tbl_loc =\
        get_fs_path("/test-warehouse/%s.db/%s" % (unique_database, "multi_compression"))
    check_call(['hdfs', 'dfs', '-copyFromLocal', os.environ['IMPALA_HOME'] +
        "/testdata/multi_compression_parquet_data/tinytable_0_gzip_snappy.parq",
        multi_compression_tbl_loc])
    check_call(['hdfs', 'dfs', '-copyFromLocal', os.environ['IMPALA_HOME'] +
        "/testdata/multi_compression_parquet_data/tinytable_1_snappy_gzip.parq",
        multi_compression_tbl_loc])

    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/hdfs_parquet_scan_node_profile',
                       vector, unique_database)

  def test_corrupt_rle_counts(self, vector, unique_database):
    """IMPALA-3646: Tests that a certain type of file corruption for plain
    dictionary encoded values is gracefully handled. Cases tested:
    - incorrect literal count of 0 for the RLE encoded dictionary indexes
    - incorrect repeat count of 0 for the RLE encoded dictionary indexes
    """
    # Create test table and copy the corrupt files into it.
    self.client.execute(
        "create table %s.bad_rle_counts (c bigint) stored as parquet" % unique_database)
    bad_rle_counts_tbl_loc =\
        get_fs_path("/test-warehouse/%s.db/%s" % (unique_database, "bad_rle_counts"))
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/bad_rle_literal_count.parquet",
        bad_rle_counts_tbl_loc])
    check_call(['hdfs', 'dfs', '-copyFromLocal',
        os.environ['IMPALA_HOME'] + "/testdata/data/bad_rle_repeat_count.parquet",
        bad_rle_counts_tbl_loc])
    # Querying the corrupted files should not DCHECK or crash.
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-corrupt-rle-counts', vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-corrupt-rle-counts-abort',
                       vector, unique_database)

  def test_bad_compressed_page_size(self, vector, unique_database):
    """IMPALA-6353: Tests that a parquet dict page with 0 compressed_page_size is
    gracefully handled. """
    self.client.execute(
        "create table %s.bad_compressed_dict_page_size (col string) stored as parquet"
        % unique_database)
    tbl_loc = get_fs_path("/test-warehouse/%s.db/%s" % (unique_database,
        "bad_compressed_dict_page_size"))
    check_call(['hdfs', 'dfs', '-copyFromLocal', os.environ['IMPALA_HOME'] +
        "/testdata/data/bad_compressed_dict_page_size.parquet", tbl_loc])
    self.run_test_case('QueryTest/parquet-bad-compressed-dict-page-size', vector,
        unique_database)

  def test_def_levels(self, vector, unique_database):
    """Test that Impala behaves as expected when decoding def levels with different
       encodings - RLE, BIT_PACKED, etc."""
    self.client.execute(("""CREATE TABLE {0}.alltypesagg_bitpacked (
          id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT,
          int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE,
          date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP,
          year INT, month INT, day INT) STORED AS PARQUET""").format(unique_database))
    alltypesagg_loc = get_fs_path(
        "/test-warehouse/{0}.db/{1}".format(unique_database, "alltypesagg_bitpacked"))
    check_call(['hdfs', 'dfs', '-copyFromLocal', os.environ['IMPALA_HOME'] +
        "/testdata/data/alltypes_agg_bitpacked_def_levels.parquet", alltypesagg_loc])
    self.client.execute("refresh {0}.alltypesagg_bitpacked".format(unique_database));

    self.run_test_case('QueryTest/parquet-def-levels', vector, unique_database)

  def test_bad_compression_codec(self, vector, unique_database):
    """IMPALA-6593: test the bad compression codec is handled gracefully. """
    self.client.execute(("""CREATE TABLE {0}.bad_codec (
          id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT,
          int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE,
          date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP,
          year INT, month INT) STORED AS PARQUET""").format(unique_database))
    tbl_loc = get_fs_path("/test-warehouse/%s.db/%s" % (unique_database,
        "bad_codec"))
    check_call(['hdfs', 'dfs', '-copyFromLocal', os.environ['IMPALA_HOME'] +
        "/testdata/data/bad_codec.parquet", tbl_loc])
    self.run_test_case('QueryTest/parquet-bad-codec', vector, unique_database)

  @SkipIfS3.hdfs_block_size
  @SkipIfADLS.hdfs_block_size
  @SkipIfIsilon.hdfs_block_size
  @SkipIfLocal.multiple_impalad
  def test_misaligned_parquet_row_groups(self, vector):
    """IMPALA-3989: Test that no warnings are issued when misaligned row groups are
    encountered. Make sure that 'NumScannersWithNoReads' counters are set to the number of
    scanners that end up doing no reads because of misaligned row groups.
    """
    # functional.parquet.alltypes is well-formatted. 'NumScannersWithNoReads' counters are
    # set to 0.
    table_name = 'functional_parquet.alltypes'
    self._misaligned_parquet_row_groups_helper(table_name, 7300)
    # lineitem_multiblock_parquet/000000_0 is ill-formatted but every scanner reads some
    # row groups. 'NumScannersWithNoReads' counters are set to 0.
    table_name = 'functional_parquet.lineitem_multiblock'
    self._misaligned_parquet_row_groups_helper(table_name, 20000)
    # lineitem_sixblocks.parquet is ill-formatted but every scanner reads some row groups.
    # 'NumScannersWithNoReads' counters are set to 0.
    table_name = 'functional_parquet.lineitem_sixblocks'
    self._misaligned_parquet_row_groups_helper(table_name, 40000)
    # Scanning lineitem_one_row_group.parquet finds two scan ranges that end up doing no
    # reads because the file is poorly formatted.
    table_name = 'functional_parquet.lineitem_multiblock_one_row_group'
    self._misaligned_parquet_row_groups_helper(
        table_name, 40000, num_scanners_with_no_reads=2)

  def _misaligned_parquet_row_groups_helper(
      self, table_name, rows_in_table, num_scanners_with_no_reads=0, log_prefix=None):
    """Checks if executing a query logs any warnings and if there are any scanners that
    end up doing no reads. 'log_prefix' specifies the prefix of the expected warning.
    'num_scanners_with_no_reads' indicates the expected number of scanners that don't read
    anything because the underlying file is poorly formatted
    """
    query = 'select * from %s' % table_name
    result = self.client.execute(query)
    assert len(result.data) == rows_in_table
    assert (not result.log and not log_prefix) or \
        (log_prefix and result.log.startswith(log_prefix))

    runtime_profile = str(result.runtime_profile)
    num_scanners_with_no_reads_list = re.findall(
        'NumScannersWithNoReads: ([0-9]*)', runtime_profile)

    # This will fail if the number of impalads != 3
    # The fourth fragment is the "Averaged Fragment"
    assert len(num_scanners_with_no_reads_list) == 4

    # Calculate the total number of scan ranges that ended up not reading anything because
    # an underlying file was poorly formatted.
    # Skip the Averaged Fragment; it comes first in the runtime profile.
    total = 0
    for n in num_scanners_with_no_reads_list[1:]:
      total += int(n)
    assert total == num_scanners_with_no_reads

  @SkipIfS3.hdfs_block_size
  @SkipIfADLS.hdfs_block_size
  @SkipIfIsilon.hdfs_block_size
  @SkipIfLocal.multiple_impalad
  def test_multiple_blocks(self, vector):
    # For IMPALA-1881. The table functional_parquet.lineitem_multiblock has 3 blocks, so
    # each impalad should read 1 scan range.
    table_name = 'functional_parquet.lineitem_multiblock'
    self._multiple_blocks_helper(table_name, 20000, ranges_per_node=1)
    table_name = 'functional_parquet.lineitem_sixblocks'
    # 2 scan ranges per node should be created to read 'lineitem_sixblocks' because
    # there are 6 blocks and 3 scan nodes.
    self._multiple_blocks_helper(table_name, 40000, ranges_per_node=2)

  @SkipIfS3.hdfs_block_size
  @SkipIfADLS.hdfs_block_size
  @SkipIfIsilon.hdfs_block_size
  @SkipIfLocal.multiple_impalad
  def test_multiple_blocks_one_row_group(self, vector):
    # For IMPALA-1881. The table functional_parquet.lineitem_multiblock_one_row_group has
    # 3 blocks but only one row group across these blocks. We test to see that only one
    # scan range reads everything from this row group.
    table_name = 'functional_parquet.lineitem_multiblock_one_row_group'
    self._multiple_blocks_helper(
        table_name, 40000, one_row_group=True, ranges_per_node=1)

  def _multiple_blocks_helper(
      self, table_name, rows_in_table, one_row_group=False, ranges_per_node=1):
    """ This function executes a simple SELECT query on a multiblock parquet table and
    verifies the number of ranges issued per node and verifies that at least one row group
    was read. If 'one_row_group' is True, then one scan range is expected to read the data
    from the entire table regardless of the number of blocks. 'ranges_per_node' indicates
    how many scan ranges we expect to be issued per node. """

    query = 'select count(l_orderkey) from %s' % table_name
    result = self.client.execute(query)
    assert len(result.data) == 1
    assert result.data[0] == str(rows_in_table)

    runtime_profile = str(result.runtime_profile)
    num_row_groups_list = re.findall('NumRowGroups: ([0-9]*)', runtime_profile)
    scan_ranges_complete_list = re.findall(
        'ScanRangesComplete: ([0-9]*)', runtime_profile)
    num_rows_read_list = re.findall('RowsRead: [0-9.K]* \(([0-9]*)\)', runtime_profile)

    REGEX_UNIT_SECOND = "[0-9]*[s]*[0-9]*[.]*[0-9]*[nm]*[s]*"
    REGEX_MIN_MAX_FOOTER_PROCESSING_TIME = \
        ("FooterProcessingTime: \(Avg: %s ; \(Min: (%s) ; Max: (%s) ; "
            "Number of samples: %s\)" % (REGEX_UNIT_SECOND, REGEX_UNIT_SECOND,
            REGEX_UNIT_SECOND, "[0-9]*"))
    footer_processing_time_list = re.findall(
        REGEX_MIN_MAX_FOOTER_PROCESSING_TIME, runtime_profile)

    # This will fail if the number of impalads != 3
    # The fourth fragment is the "Averaged Fragment"
    assert len(num_row_groups_list) == 4
    assert len(scan_ranges_complete_list) == 4
    assert len(num_rows_read_list) == 4

    total_num_row_groups = 0
    # Skip the Averaged Fragment; it comes first in the runtime profile.
    for num_row_groups in num_row_groups_list[1:]:
      total_num_row_groups += int(num_row_groups)
      if not one_row_group: assert int(num_row_groups) > 0

    if one_row_group:
      # If it's the one row group test, only one scan range should read all the data from
      # that row group.
      assert total_num_row_groups == 1
      for rows_read in num_rows_read_list[1:]:
        if rows_read != '0': assert rows_read == str(rows_in_table)

    for scan_ranges_complete in scan_ranges_complete_list:
      assert int(scan_ranges_complete) == ranges_per_node

    # This checks if the SummaryStatsCounter works correctly. When there is one scan
    # range per node, we verify that the FooterProcessingTime counter has the min, max
    # and average values as the same since we have only one sample (i.e. only one range)
    # TODO: Test this for multiple ranges per node as well. This requires parsing the
    # stat times as strings and comparing if min <= avg <= max.
    if ranges_per_node == 1:
      for min_max_time in footer_processing_time_list:
        # Assert that (min == avg == max)
        assert min_max_time[0] == min_max_time[1] == min_max_time[2] != 0

  def test_annotate_utf8_option(self, vector, unique_database):
    if self.exploration_strategy() != 'exhaustive': pytest.skip("Only run in exhaustive")

    # Create table
    TABLE_NAME = "parquet_annotate_utf8_test"
    qualified_table_name = "%s.%s" % (unique_database, TABLE_NAME)
    query = 'create table %s (a string, b char(10), c varchar(10), d string) ' \
            'stored as parquet' % qualified_table_name
    self.client.execute(query)

    # Insert data that should have UTF8 annotation
    query = 'insert overwrite table %s '\
            'values("a", cast("b" as char(10)), cast("c" as varchar(10)), "d")' \
            % qualified_table_name
    self.execute_query(query, {'parquet_annotate_strings_utf8': True})

    def get_schema_elements():
      # Copy the created file to the local filesystem and parse metadata
      local_file = '/tmp/utf8_test_%s.parq' % random.randint(0, 10000)
      LOG.info("test_annotate_utf8_option local file name: " + local_file)
      hdfs_file = get_fs_path('/test-warehouse/%s.db/%s/*.parq'
          % (unique_database, TABLE_NAME))
      check_call(['hadoop', 'fs', '-copyToLocal', hdfs_file, local_file])
      metadata = get_parquet_metadata(local_file)

      # Extract SchemaElements corresponding to the table columns
      a_schema_element = metadata.schema[1]
      assert a_schema_element.name == 'a'
      b_schema_element = metadata.schema[2]
      assert b_schema_element.name == 'b'
      c_schema_element = metadata.schema[3]
      assert c_schema_element.name == 'c'
      d_schema_element = metadata.schema[4]
      assert d_schema_element.name == 'd'

      os.remove(local_file)
      return a_schema_element, b_schema_element, c_schema_element, d_schema_element

    # Check that the schema uses the UTF8 annotation
    a_schema_elt, b_schema_elt, c_schema_elt, d_schema_elt = get_schema_elements()
    assert a_schema_elt.converted_type == ConvertedType.UTF8
    assert b_schema_elt.converted_type == ConvertedType.UTF8
    assert c_schema_elt.converted_type == ConvertedType.UTF8
    assert d_schema_elt.converted_type == ConvertedType.UTF8

    # Create table and insert data that should not have UTF8 annotation for strings
    self.execute_query(query, {'parquet_annotate_strings_utf8': False})

    # Check that the schema does not use the UTF8 annotation except for CHAR and VARCHAR
    # columns
    a_schema_elt, b_schema_elt, c_schema_elt, d_schema_elt = get_schema_elements()
    assert a_schema_elt.converted_type == None
    assert b_schema_elt.converted_type == ConvertedType.UTF8
    assert c_schema_elt.converted_type == ConvertedType.UTF8
    assert d_schema_elt.converted_type == None

  def test_resolution_by_name(self, vector, unique_database):
    self.run_test_case('QueryTest/parquet-resolution-by-name', vector,
                       use_db=unique_database)

  def test_decimal_encodings(self, vector, unique_database):
    # Create a table using an existing data file with dictionary-encoded, variable-length
    # physical encodings for decimals.
    TABLE_NAME = "decimal_encodings"
    self.client.execute('''create table if not exists %s.%s
    (small_dec decimal(9,2), med_dec decimal(18,2), large_dec decimal(38,2))
    STORED AS PARQUET''' % (unique_database, TABLE_NAME))

    table_loc = get_fs_path(
      "/test-warehouse/%s.db/%s" % (unique_database, TABLE_NAME))
    for file_name in ["binary_decimal_dictionary.parquet",
                      "binary_decimal_no_dictionary.parquet"]:
      data_file_path = os.path.join(os.environ['IMPALA_HOME'],
                                    "testdata/data/", file_name)
      check_call(['hdfs', 'dfs', '-copyFromLocal', data_file_path, table_loc])

    self.run_test_case('QueryTest/parquet-decimal-formats', vector, unique_database)

# We use various scan range lengths to exercise corner cases in the HDFS scanner more
# thoroughly. In particular, it will exercise:
# 1. default scan range
# 2. scan range with no tuple
# 3. tuple that span across multiple scan ranges
# 4. scan range length = 16 for ParseSse() execution path
MAX_SCAN_RANGE_LENGTHS = [0, 1, 2, 5, 16, 17, 32]

class TestScanRangeLengths(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScanRangeLengths, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))

  def test_scan_ranges(self, vector):
    vector.get_value('exec_option')['max_scan_range_length'] =\
        vector.get_value('max_scan_range_length')
    self.run_test_case('QueryTest/hdfs-tiny-scan', vector)

# More tests for text scanner
# 1. Test file that ends w/o tuple delimiter
# 2. Test file with escape character
class TestTextScanRangeLengths(ImpalaTestSuite):
  ESCAPE_TABLE_LIST = ["testescape_16_lf", "testescape_16_crlf",
      "testescape_17_lf", "testescape_17_crlf",
      "testescape_32_lf", "testescape_32_crlf"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTextScanRangeLengths, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')

  def test_text_scanner(self, vector):
    vector.get_value('exec_option')['max_scan_range_length'] =\
        vector.get_value('max_scan_range_length')
    self.execute_query_expect_success(self.client, "drop stats "
        "functional.table_no_newline_part")
    self.execute_query_expect_success(self.client, "compute stats "
        "functional.table_no_newline_part")
    self.run_test_case('QueryTest/hdfs-text-scan', vector)

    # Test various escape char cases. We have to check the count(*) result against
    # the count(col) result because if the scan range is split right after the escape
    # char, the escape char has no effect because we cannot scan backwards to the
    # previous scan range.
    for t in self.ESCAPE_TABLE_LIST:
      expected_result = self.client.execute("select count(col) from " + t)
      result = self.client.execute("select count(*) from " + t)
      assert result.data == expected_result.data

# Tests behavior of split "\r\n" delimiters.
class TestTextSplitDelimiters(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTextSplitDelimiters, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')

  def test_text_split_delimiters(self, vector, unique_database):
    """Creates and queries a datafile that exercises interesting edge cases around split
    "\r\n" delimiters. The data file contains the following 4-byte scan ranges:

    abc\r   First scan range, ends with split \r\n
            - materializes (abc)
    \nde\r  Initial delimiter found, scan range ends with split \r\n
            - materializes (de)
    \nfg\r  Initial delimiter found, scan range ends with \r
            - materializes (fg),(hij)
    hij\r   Initial delimiter is \r at end
            - materializes (klm)
    klm\r   Initial delimiter is split \r\n
            - materializes nothing
    \nno\r  Final scan range, initial delimiter found, ends with \r
            - materializes (no)
    """
    DATA = "abc\r\nde\r\nfg\rhij\rklm\r\nno\r"
    max_scan_range_length = 4
    expected_result = ['abc', 'de', 'fg', 'hij', 'klm', 'no']

    self._create_and_query_test_table(
      vector, unique_database, DATA, max_scan_range_length, expected_result)

  def test_text_split_across_buffers_delimiter(self, vector, unique_database):
    """Creates and queries a datafile that exercises a split "\r\n" across io buffers (but
    within a single scan range). We use a 32MB file and 16MB scan ranges, so there are two
    scan ranges of two io buffers each. The first scan range exercises a split delimiter
    in the main text parsing algorithm. The second scan range exercises correctly
    identifying a split delimiter as the first in a scan range."""
    DEFAULT_IO_BUFFER_SIZE = 8 * 1024 * 1024
    data = ('a' * (DEFAULT_IO_BUFFER_SIZE - 1) + "\r\n" + # first scan range
            'b' * (DEFAULT_IO_BUFFER_SIZE - 3) + "\r\n" +
            'a' * (DEFAULT_IO_BUFFER_SIZE - 1) + "\r\n" +     # second scan range
            'b' * (DEFAULT_IO_BUFFER_SIZE - 1))
    assert len(data) == DEFAULT_IO_BUFFER_SIZE * 4

    max_scan_range_length = DEFAULT_IO_BUFFER_SIZE * 2
    expected_result = data.split("\r\n")

    self._create_and_query_test_table(
      vector, unique_database, data, max_scan_range_length, expected_result)

  def _create_and_query_test_table(self, vector, unique_database, data,
        max_scan_range_length, expected_result):
    TABLE_NAME = "test_text_split_delimiters"
    qualified_table_name = "%s.%s" % (unique_database, TABLE_NAME)
    location = get_fs_path("/test-warehouse/%s_%s" % (unique_database, TABLE_NAME))
    query = "create table %s (s string) location '%s'" % (qualified_table_name, location)
    self.client.execute(query)

    with tempfile.NamedTemporaryFile() as f:
      f.write(data)
      f.flush()
      check_call(['hadoop', 'fs', '-copyFromLocal', f.name, location])
    self.client.execute("refresh %s" % qualified_table_name);

    vector.get_value('exec_option')['max_scan_range_length'] = max_scan_range_length
    query = "select * from %s" % qualified_table_name
    result = self.execute_query_expect_success(
      self.client, query, vector.get_value('exec_option'))

    assert sorted(result.data) == sorted(expected_result)

# Test for IMPALA-1740: Support for skip.header.line.count
class TestTextScanRangeLengths(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTextScanRangeLengths, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec in ['none', 'gzip'])

  def test_text_scanner_with_header(self, vector, unique_database):
    self.run_test_case('QueryTest/hdfs-text-scan-with-header', vector,
                       test_file_vars={'$UNIQUE_DB': unique_database})


# Missing Coverage: No coverage for truncated files errors or scans.
@SkipIfS3.hive
@SkipIfADLS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestScanTruncatedFiles(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScanTruncatedFiles, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

    # This test takes about a minute to complete due to the Hive commands that are
    # executed. To cut down on runtime, limit the test to exhaustive exploration
    # strategy.
    # TODO: Test other file formats
    if cls.exploration_strategy() == 'exhaustive':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'text' and
          v.get_value('table_format').compression_codec == 'none')
    else:
      cls.ImpalaTestMatrix.add_constraint(lambda v: False)

  def test_scan_truncated_file_empty(self, vector, unique_database):
    self.scan_truncated_file(0, unique_database)

  def test_scan_truncated_file(self, vector, unique_database):
    self.scan_truncated_file(10, unique_database)

  def scan_truncated_file(self, num_rows, db_name):
    fq_tbl_name = db_name + ".truncated_file_test"
    self.execute_query("create table %s (s string)" % fq_tbl_name)
    self.run_stmt_in_hive("insert overwrite table %s select string_col from "
        "functional.alltypes" % fq_tbl_name)

    # Update the Impala metadata
    self.execute_query("refresh %s" % fq_tbl_name)

    # Insert overwrite with a truncated file
    self.run_stmt_in_hive("insert overwrite table %s select string_col from "
        "functional.alltypes limit %s" % (fq_tbl_name, num_rows))

    result = self.execute_query("select count(*) from %s" % fq_tbl_name)
    assert(len(result.data) == 1)
    assert(result.data[0] == str(num_rows))
