# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This test suite validates the scanners by running queries against ALL file formats and
# their permutations (e.g. compression codec/compression type). This works by exhaustively
# generating the table format test vectors for this specific test suite. This way, other
# tests can run with the normal exploration strategy and the overall test runtime doesn't
# explode.

import logging
import pytest
from copy import deepcopy
from subprocess import call, check_call

from testdata.common import widetable
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.util.test_file_parser import *
from tests.util.filesystem_utils import WAREHOUSE
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.skip import SkipIfS3, SkipIfIsilon

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
      cls.TestMatrix.add_dimension(cls.create_table_info_dimension('pairwise'))
    cls.TestMatrix.add_dimension(
        TestDimension('batch_size', *TestScannersAllTableFormats.BATCH_SIZES))

  def test_scanners(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/scanners', new_vector)

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
    # Use a small batch size so changing the limit affects the timing of cancellation
    vector.get_value('exec_option')['batch_size'] = 100
    iterations = 50
    query_template = "select * from alltypes limit %s"
    for i in range(1, iterations):
      # Vary the limit to vary the timing of cancellation
      query = query_template % ((iterations * 100) % 1000 + 1)
      self.execute_query(query, vector.get_value('exec_option'),
          table_format=vector.get_value('table_format'))

# Test case to verify the scanners work properly when the table metadata (specifically the
# number of columns in the table) does not match the number of columns in the data file.
class TestUnmatchedSchema(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnmatchedSchema, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Avro has a more advanced schema evolution process which is covered in more depth
    # in the test_avro_schema_evolution test suite.
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format != 'avro')

  def _get_table_location(self, table_name, vector):
    result = self.execute_query_using_client(self.client,
        "describe formatted %s" % table_name, vector)
    for row in result.data:
      if 'Location:' in row:
        return row.split('\t')[1]
    # This should never happen.
    assert 0, 'Unable to get location for table: ' + table_name

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
    cls.TestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format != 'hbase')

  def test_wide_row(self, vector):
    new_vector = deepcopy(vector)
    # Use a 5MB scan range, so we will have to perform 5MB of sync reads
    new_vector.get_value('exec_option')['max_scan_range_length'] = 5 * 1024 * 1024
    # We need > 10 MB of memory because we're creating extra buffers:
    # - 10 MB table / 5 MB scan range = 2 scan ranges, each of which may allocate ~20MB
    # - Sync reads will allocate ~5MB of space
    # The 80MB value used here was determined empirically by raising the limit until the
    # query succeeded for all file formats -- I don't know exactly why we need this much.
    # TODO: figure out exact breakdown of memory usage (IMPALA-681)
    new_vector.get_value('exec_option')['mem_limit'] = 80 * 1024 * 1024
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
    cls.TestMatrix.add_dimension(TestDimension("num_cols", *cls.NUM_COLS))
    # To cut down on test execution time, only run in exhaustive.
    if cls.exploration_strategy() != 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v: False)

  def test_wide_table(self, vector):
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
    cls.TestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet(self, vector):
    self.run_test_case('QueryTest/parquet', vector)

  @SkipIfS3.hdfs_block_size
  @SkipIfIsilon.hdfs_block_size
  def test_verify_runtime_profile(self, vector):
    # For IMPALA-1881. The table functional_parquet.lineitem_multiblock has 3 blocks, so
    # we verify if each impalad reads one block by checking if each impalad reads at
    # least one row group.
    DB_NAME = 'functional_parquet'
    TABLE_NAME = 'lineitem_multiblock'
    query = 'select count(l_orderkey) from %s.%s' % (DB_NAME, TABLE_NAME)
    result = self.client.execute(query)
    assert len(result.data) == 1
    assert result.data[0] == '20000'

    runtime_profile = str(result.runtime_profile)
    num_row_groups_list = re.findall('NumRowGroups: ([0-9]*)', runtime_profile)
    scan_ranges_complete_list = re.findall('ScanRangesComplete: ([0-9]*)', runtime_profile)
    # This will fail if the number of impalads != 3
    # The fourth fragment is the "Averaged Fragment"
    assert len(num_row_groups_list) == 4
    assert len(scan_ranges_complete_list) == 4

    # Skip the Averaged Fragment; it comes first in the runtime profile.
    for num_row_groups in num_row_groups_list[1:]:
      assert int(num_row_groups) > 0

    for scan_ranges_complete in scan_ranges_complete_list[1:]:
      assert int(scan_ranges_complete) == 1

# Missing coverage: Impala can query a table with complex types created by Hive on a
# non-hdfs filesystem.
@SkipIfS3.hive
@SkipIfIsilon.hive
class TestParquetComplexTypes(ImpalaTestSuite):
  COMPLEX_COLUMN_TABLE = "functional_parquet.nested_column_types"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetComplexTypes, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Only run on delimited text with no compression.
    cls.TestMatrix.add_dimension(create_parquet_dimension(cls.get_workload()))

  # This tests we can read the scalar-typed columns from a Parquet table that also has
  # complex-typed columns.
  # TODO: remove this when we can read complex-typed columns (complex types testing should
  # supercede this)
  def test_complex_column_types(self, vector):
    self._drop_complex_column_table()

    # Partitioned case
    create_table_stmt = """
      CREATE TABLE IF NOT EXISTS {0} (
        a int,
        b ARRAY<STRUCT<c:INT, d:STRING>>,
        e MAP<STRING,INT>,
        f string,
        g ARRAY<INT>,
        h STRUCT<i:DOUBLE>
      ) PARTITIONED BY (p1 INT, p2 STRING)
      STORED AS PARQUET;
    """.format(self.COMPLEX_COLUMN_TABLE)

    insert_stmt = """
      INSERT OVERWRITE TABLE {0}
      PARTITION (p1=1, p2="partition1")
      SELECT 1, array(named_struct("c", 2, "d", "foo")), map("key1", 10, "key2", 20),
        "bar", array(2,3,4,5), named_struct("i", 1.23)
      FROM functional_parquet.tinytable limit 2;
    """.format(self.COMPLEX_COLUMN_TABLE)

    check_call(["hive", "-e", create_table_stmt])
    check_call(["hive", "-e", insert_stmt])

    self.execute_query("invalidate metadata %s" % self.COMPLEX_COLUMN_TABLE)

    result = self.execute_query("select count(*) from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 1)
    assert(result.data[0] == "2")

    result = self.execute_query("select a from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "1")

    result = self.execute_query(
      "select p1, a from %s where p1 = 1" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "1\t1")

    result = self.execute_query("select f from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "bar")

    result = self.execute_query(
      "select p2, f from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "partition1\tbar")

    # Unpartitioned case
    self._drop_complex_column_table()

    create_table_stmt = """
      CREATE TABLE IF NOT EXISTS {0} (
        a int,
        b ARRAY<STRUCT<c:INT, d:STRING>>,
        e MAP<STRING,INT>,
        f string,
        g ARRAY<INT>,
        h STRUCT<i:DOUBLE>
      ) STORED AS PARQUET;
    """.format(self.COMPLEX_COLUMN_TABLE)

    insert_stmt = """
      INSERT OVERWRITE TABLE {0}
      SELECT 1, array(named_struct("c", 2, "d", "foo")), map("key1", 10, "key2", 20),
        "bar", array(2,3,4,5), named_struct("i", 1.23)
      FROM functional_parquet.tinytable limit 2;
    """.format(self.COMPLEX_COLUMN_TABLE)

    check_call(["hive", "-e", create_table_stmt])
    check_call(["hive", "-e", insert_stmt])

    self.execute_query("invalidate metadata %s" % self.COMPLEX_COLUMN_TABLE)

    result = self.execute_query("select count(*) from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 1)
    assert(result.data[0] == "2")

    result = self.execute_query("select a from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "1")

    result = self.execute_query("select f from %s" % self.COMPLEX_COLUMN_TABLE)
    assert(len(result.data) == 2)
    assert(result.data[1] == "bar")

  @classmethod
  def teardown_class(cls):
    cls._drop_complex_column_table()

  @classmethod
  def _drop_complex_column_table(cls):
    cls.client.execute("drop table if exists %s" % cls.COMPLEX_COLUMN_TABLE)

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
    cls.TestMatrix.add_dimension(
        TestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))

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
    cls.TestMatrix.add_dimension(
        TestDimension('max_scan_range_length', *MAX_SCAN_RANGE_LENGTHS))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
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

# Missing Coverage: No coverage for truncated files errors or scans.
@SkipIfS3.hive
@SkipIfIsilon.hive
@pytest.mark.execute_serially
class TestScanTruncatedFiles(ImpalaTestSuite):
  TEST_DB = 'test_truncated_file'

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScanTruncatedFiles, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

    # This test takes about a minute to complete due to the Hive commands that are
    # executed. To cut down on runtime, limit the test to exhaustive exploration
    # strategy.
    # TODO: Test other file formats
    if cls.exploration_strategy() == 'exhaustive':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'text' and\
          v.get_value('table_format').compression_codec == 'none')
    else:
      cls.TestMatrix.add_constraint(lambda v: False)

  def setup_method(self, method):
    self.cleanup_db(TestScanTruncatedFiles.TEST_DB)
    self.client.execute("create database %s location '%s/%s.db'" %
        (TestScanTruncatedFiles.TEST_DB, WAREHOUSE,
        TestScanTruncatedFiles.TEST_DB))

  def teardown_method(self, method):
    self.cleanup_db(TestScanTruncatedFiles.TEST_DB)

  def test_scan_truncated_file_empty(self, vector):
    self.scan_truncated_file(0)

  def test_scan_truncated_file(self, vector):
    self.scan_truncated_file(10)

  def scan_truncated_file(self, num_rows):
    db_name = TestScanTruncatedFiles.TEST_DB
    tbl_name = "tbl"
    self.execute_query("use %s" % db_name)
    self.execute_query("create table %s (s string)" % tbl_name)
    call(["hive", "-e", "INSERT OVERWRITE TABLE %s.%s SELECT string_col from "\
        "functional.alltypes" % (db_name, tbl_name)])

    # Update the Impala metadata
    self.execute_query("refresh %s" % tbl_name)

    # Insert overwrite with a truncated file
    call(["hive", "-e", "INSERT OVERWRITE TABLE %s.%s SELECT string_col from "\
        "functional.alltypes limit %s" % (db_name, tbl_name, num_rows)])

    result = self.execute_query("select count(*) from %s" % tbl_name)
    assert(len(result.data) == 1)
    assert(result.data[0] == str(num_rows))
