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

from __future__ import absolute_import, division, print_function
from builtins import range
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
    SkipIf,
    SkipIfFS,
    SkipIfHive2,
    SkipIfHive3,
    SkipIfLocal,
    SkipIfNotHdfsMinicluster)
from tests.common.test_dimensions import (
    add_exec_option_dimension,
    add_mandatory_exec_option,
    create_single_exec_option_dimension,
    create_exec_option_dimension,
    create_uncompressed_text_dimension)
from tests.common.file_utils import (
    create_table_from_parquet,
    create_table_and_copy_files)
from tests.common.test_result_verifier import (
    QueryTestResult,
    parse_result_rows)
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import IS_HDFS, get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata
from tests.util.parse_util import get_bytes_summary_stats_counter
from tests.util.test_file_parser import QueryTestSectionReader

# Test scanners with denial of reservations at varying frequency. This will affect the
# number of scanner threads that can be spun up.
DEBUG_ACTION_DIMS = [None,
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@0.5',
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@1.0']

# Trigger injected soft limit failures when scanner threads check memory limit.
DEBUG_ACTION_DIMS.append('HDFS_SCANNER_THREAD_CHECK_SOFT_MEM_LIMIT:FAIL@0.5')

MT_DOP_VALUES = [0, 1, 4]
BATCH_SIZES = [0, 1, 16]


class TestScannersAllTableFormats(ImpalaTestSuite):

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
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        batch_sizes=BATCH_SIZES, debug_action_options=DEBUG_ACTION_DIMS))
    add_exec_option_dimension(cls, 'mt_dop', MT_DOP_VALUES)

  def test_scanners(self, vector):
    self.run_test_case('QueryTest/scanners', vector)

  def test_many_nulls(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      # manynulls table not loaded for HBase
      pytest.skip()
    # Copy over test dimensions to the matching query options.
    self.run_test_case('QueryTest/scanners-many-nulls', vector)


class TestScannersAllTableFormatsDefaultOptions(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormatsDefaultOptions, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      # The purpose of this test is to get some base coverage of all the file formats.
      # Even in 'core', we'll test each format by using the pairwise strategy.
      cls.ImpalaTestMatrix.add_dimension(cls.create_table_info_dimension('pairwise'))

  def test_hdfs_scanner_profile(self, vector):
    if vector.get_value('table_format').file_format in ('kudu', 'hbase') or \
       vector.get_value('exec_option')['num_nodes'] != 0:
      pytest.skip()
    self.run_test_case('QueryTest/hdfs_scanner_profile', vector)

  def test_string_escaping(self, vector):
    """Test handling of string escape sequences."""
    if vector.get_value('table_format').file_format == 'rc':
      # IMPALA-7778: RCFile scanner incorrectly ignores escapes for now.
      self.run_test_case('QueryTest/string-escaping-rcfile-bug', vector)
    else:
      self.run_test_case('QueryTest/string-escaping', vector)


class TestScannersVirtualColumns(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersVirtualColumns, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      # The purpose of this test is to get some base coverage of all the file formats.
      # Even in 'core', we'll test each format by using the pairwise strategy.
      cls.ImpalaTestMatrix.add_dimension(cls.create_table_info_dimension('pairwise'))
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        batch_sizes=BATCH_SIZES))

  def test_virtual_column_input_file_name(self, vector, unique_database):
    file_format = vector.get_value('table_format').file_format
    if file_format in ['hbase', 'kudu']:
      # Virtual column INPUT__FILE__NAME is only supported for filesystem-based tables.
      pytest.skip()
    self.run_test_case('QueryTest/virtual-column-input-file-name', vector)
    if file_format in ['orc', 'parquet']:
      self.run_test_case('QueryTest/virtual-column-input-file-name-complextypes', vector)
    if file_format == 'text':
      self.run_test_case('QueryTest/virtual-column-input-file-name-in-table', vector,
          use_db=unique_database)

  def test_virtual_column_file_position_generic(self, vector):
    # Generic tests about virtual column file position.
    file_format = vector.get_value('table_format').file_format
    # TODO: add support for other file format
    if file_format not in ['parquet', 'orc']:
      pytest.skip()
    self.run_test_case('QueryTest/virtual-column-file-position-generic', vector)

  def test_mixing_virtual_columns(self, vector, unique_database):
    # Test queries with multiple virtual columns.
    file_format = vector.get_value('table_format').file_format
    # TODO: add support for other file formats, especially ORC
    if file_format not in ['parquet']:
      pytest.skip()
    create_table_from_parquet(self.client, unique_database, 'alltypes_tiny_pages')
    self.run_test_case('QueryTest/mixing-virtual-columns', vector, unique_database)


class TestScannersVirtualColumnsNegative(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersVirtualColumnsNegative, cls).add_test_dimensions()
    # In the tests we explicitly refer to the databases, i.e. no need to
    # run this test with multiple file formats.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_virtual_column_file_position_negative(self, vector):
    self.run_test_case('QueryTest/virtual-column-file-position-negative', vector)


class TestIcebergVirtualColumns(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergVirtualColumns, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_dimension(cls.create_table_info_dimension('pairwise'))
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        batch_sizes=BATCH_SIZES))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_partition_columns(self, vector):
    """Tests partition-level Iceberg-only virtual columns."""
    self.run_test_case('QueryTest/iceberg-virtual-partition-columns', vector)


# Test all the scanners with a simple limit clause. The limit clause triggers
# cancellation in the scanner code paths.
class TestScannersAllTableFormatsWithLimit(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersAllTableFormatsWithLimit, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'mt_dop', MT_DOP_VALUES)
    add_mandatory_exec_option(cls, 'batch_size', 100)

  def test_limit(self, vector):
    vector.get_value('exec_option')['abort_on_error'] = 1
    self._test_limit(vector)
    # IMPALA-3337: when continuing on error, the error log should not show errors
    # (e.g. "Cancelled").
    vector.get_value('exec_option')['abort_on_error'] = 0
    self._test_limit(vector)

  def _test_limit(self, vector):
    # Use a small batch size so changing the limit affects the timing of cancellation
    iterations = 50
    query_template = "select * from alltypes limit %s"
    for i in range(1, iterations):
      # Vary the limit to vary the timing of cancellation
      limit = (i * 100) % 1001 + 1
      query = query_template % limit
      result = self.execute_query(query, vector.get_value('exec_option'),
          table_format=vector.get_value('table_format'))
      assert len(result.data) == limit
      # IMPALA-3337: The error log should be empty.
      assert not result.log


class TestScannersMixedTableFormats(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannersMixedTableFormats, cls).add_test_dimensions()
    # Only run with a single dimension format, since the table includes mixed formats.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        batch_sizes=BATCH_SIZES, debug_action_options=DEBUG_ACTION_DIMS))
    add_exec_option_dimension(cls, 'mt_dop', MT_DOP_VALUES)

  def test_mixed_format(self, vector):
    self.run_test_case('QueryTest/mixed-format', vector)


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

  def _create_test_table(self, vector, unique_database):
    """
    Creates the test table

    Cannot be done in a setup method because we need access to the current test vector
    """
    file_format = vector.get_value('table_format').file_format
    if file_format == 'orc':
      # TODO: Enable this test on non-HDFS filesystems once IMPALA-9365 is resolved.
      if not IS_HDFS: pytest.skip()
      self.run_stmt_in_hive(
          "create table {0}.jointbl_test like functional.jointbl "
          "stored as orc".format(unique_database))
      self.run_stmt_in_hive(
          'insert into {0}.jointbl_test '
          'select * from functional_orc_def.jointbl'.format(unique_database))
      self.execute_query_using_client(self.client,
          'invalidate metadata {0}.jointbl_test'.format(unique_database),
          vector)
    else:
      self.execute_query_using_client(self.client,
          "create external table {0}.jointbl_test like jointbl".format(
              unique_database), vector)

      # Update the location of the new table to point the same location as the old table
      location = self._get_table_location('jointbl', vector)
      self.execute_query_using_client(self.client,
          "alter table {0}.jointbl_test set location '{1}'".format(
              unique_database, location), vector)

  def test_unmatched_schema(self, vector, unique_database):
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("IMPALA-2890: Missing Kudu DDL support")

    table_format = vector.get_value('table_format')
    # jointbl has no columns with unique values. When loaded in hbase, the table looks
    # different, as hbase collapses duplicates.
    if table_format.file_format == 'hbase':
      pytest.skip()
    self._create_test_table(vector, unique_database)
    self.run_test_case('QueryTest/test-unmatched-schema', vector, use_db=unique_database)


# Tests that scanners can read a single-column, single-row, 10MB table
class TestWideRow(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWideRow, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(debug_action_options=DEBUG_ACTION_DIMS))

    # Use a 5MB scan range, so we will have to perform 5MB of sync reads
    add_mandatory_exec_option(cls, 'max_scan_range_length', 5 * 1024 * 1024)
    # We need > 10 MB of memory because we're creating extra buffers:
    # - 10 MB table / 5 MB scan range = 2 scan ranges, each of which may allocate ~20MB
    # - Sync reads will allocate ~5MB of space
    # - Result spooling require 32 MB initial reservation (2 page of 16 MB each) to fit
    #   10 MB row.
    # The 132MB value used here was determined empirically by raising the limit until the
    # query succeeded for all file formats -- I don't know exactly why we need this much.
    # TODO: figure out exact breakdown of memory usage (IMPALA-681)
    add_mandatory_exec_option(cls, 'mem_limit', 132 * 1024 * 1024)
    # Specify that the query should able to handle 10 MB MAX_ROW_SIZE.
    add_mandatory_exec_option(cls, 'max_row_size', 10 * 1024 * 1024)

    # I can't figure out how to load a huge row into hbase
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format != 'hbase')

  def test_wide_row(self, vector):
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("KUDU-666: Kudu support for large values")
    self.run_test_case('QueryTest/wide-row', vector)


class TestWideTable(ImpalaTestSuite):
  # TODO: expand this to more rows when we have the capability
  NUM_COLS = [250, 500, 1000]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestWideTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(debug_action_options=DEBUG_ACTION_DIMS))
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

    types = result.column_types
    labels = result.column_labels
    expected = QueryTestResult(expected_result, types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels,
        order_matters=False)
    assert expected == actual


class TestHdfsScannerSkew(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsScannerSkew, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ('text') and
        v.get_value('table_format').compression_codec == 'none')

  @SkipIfLocal.multiple_impalad
  @pytest.mark.execute_serially
  @SkipIf.not_hdfs
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_mt_dop_skew_lpt(self, vector, unique_database):
    """IMPALA-11539: Sanity check for MT scan nodes to make sure that the intra-node
       skew is mitigated. For intra-node scan range assignment we are using dynamic
       load balancing with a shared queue between the instances. With IMPALA-11539
       the items in the queue are ordered by scan sizes from largest to smallest, i.e.
       we are doing Longest-Processing Time (LPT) scheduling."""
    def count_intra_node_skew(profile):
      SKEW_THRESHOLD = 0.80
      lines = [line.strip() for line in profile.splitlines() if "- BytesRead: " in line]
      assert len(lines) == 7  # Averaged fragment + 6 fragment
      bytes_read_array = []
      for i in range(1, len(lines)):
        # A line looks like:
        # - BytesRead: 202.77 MB (212617555)
        # we only need '212617555' from it
        bytes_read_str = re.findall(r'\((\d+)\)', lines[i])[0]
        bytes_read = int(bytes_read_str)
        bytes_read_array.append(bytes_read)
      count_skew = 0
      # MT_DOP fragments are next to each other in the profile, so fragment instances
      # belonging to a single executor starts at 0, 2, 4
      for i in [0, 2, 4]:
        a = bytes_read_array[i]
        b = bytes_read_array[i + 1]
        if a < b:
          ratio = float(a) / float(b)
        else:
          ratio = float(b) / float(a)
        print("Intra-node bytes read ratio:", ratio)
        if ratio < SKEW_THRESHOLD:
          count_skew += 1
          print("Profile of skewed execution: ", profile)
      return count_skew

    tbl_name = unique_database + ".lineitem_skew"
    with self.create_impala_client() as imp_client:
      imp_client.set_configuration_option('mt_dop', '2')
      imp_client.execute("""create table {} like tpch.lineitem""".format(tbl_name))
      # Create a couple of small data files
      for i in range(1, 11):
        imp_client.execute("""insert into {} select * from tpch.lineitem
                              where l_orderkey % 11 = 0""".format(tbl_name))
      # Create a couple of large files
      imp_client.execute("insert into {} select * from tpch.lineitem".format(tbl_name))

      # Let's execute the test multiple time to avoid flakiness
      cnt_fail = 0
      for i in range(0, 5):
        results = imp_client.execute(
            """select min(l_orderkey),min(l_partkey),min(l_suppkey),min(l_linenumber),
                      min(l_quantity),min(l_extendedprice),min(l_discount),min(l_tax),
                      min(l_returnflag),min(l_linestatus),min(l_shipdate),min(l_commitdate),
                      min(l_receiptdate),min(l_shipinstruct),min(l_shipmode),min(l_comment)
               from {}""".format(tbl_name))
        profile = results.runtime_profile
        cnt_fail += count_intra_node_skew(profile)
      assert cnt_fail <= 5


class TestHudiParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHudiParquet, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(debug_action_options=DEBUG_ACTION_DIMS))
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_hudiparquet(self, vector):
    self.run_test_case('QueryTest/hudi-parquet', vector)


class TestIceberg(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIceberg, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(debug_action_options=DEBUG_ACTION_DIMS))
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_iceberg_query(self, vector):
    self.run_test_case('QueryTest/iceberg-query', vector)

  def test_iceberg_old_fileformat(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-old-fileformat', vector, use_db=unique_database)

  def test_iceberg_profile(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-profile', vector, use_db=unique_database)


class TestParquet(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquet, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension(debug_action_options=DEBUG_ACTION_DIMS))
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet(self, vector):
    self.run_test_case('QueryTest/parquet', vector)

  def test_virtual_column_file_position_parquet(self, vector, unique_database):
    # Parquet-specific tests for virtual column FILE__POSITION
    create_table_from_parquet(self.client, unique_database, 'alltypes_tiny_pages')
    create_table_from_parquet(self.client, unique_database,
        'customer_multiblock_page_index')
    create_table_from_parquet(self.client, unique_database,
        'customer_nested_multiblock_multipage')
    new_vector = deepcopy(vector)
    for late_mat in [-1, 1, 17]:
      new_vector.get_value('exec_option')['parquet_late_materialization_threshold'] = \
          late_mat
      for read_stats in ['true', 'false']:
        new_vector.get_value('exec_option')['parquet_read_statistics'] = read_stats
        self.run_test_case('QueryTest/virtual-column-file-position-parquet', new_vector,
            unique_database)

  def test_corrupt_files(self, vector):
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['num_nodes']  # .test file sets num_nodes
    new_vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-continue-on-error', new_vector)
    new_vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-abort-on-error', new_vector)

  def test_default_scale(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, "no_scale")
    self.run_test_case('QueryTest/default-scale', vector, unique_database)

  def test_timestamp_out_of_range(self, vector, unique_database):
    """IMPALA-4363: Test scanning parquet files with an out of range timestamp.
       Also tests IMPALA-7595: Test Parquet timestamp columns where the time part
       is out of the valid range [0..24H).
    """
    # out of range date part
    create_table_from_parquet(self.client, unique_database, "out_of_range_timestamp")

    # out of range time part
    create_table_from_parquet(self.client, unique_database, "out_of_range_time_of_day")

    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/out-of-range-timestamp-continue-on-error',
        vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/out-of-range-timestamp-abort-on-error',
        vector, unique_database)

  def test_dateless_timestamp_parquet(self, vector, unique_database):
    """Test scanning parquet files which still includes dateless timestamps."""
    tbl_name = "timestamp_table"
    create_sql = "create table %s.%s (t timestamp) stored as parquet" % (
        unique_database, tbl_name)
    create_table_and_copy_files(self.client, create_sql, unique_database, tbl_name,
        ["/testdata/data/dateless_timestamps.parq"])

    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/dateless_timestamp_parquet', new_vector,
        use_db=unique_database)

  def test_date_out_of_range_parquet(self, vector, unique_database):
    """Test scanning parquet files with an out of range date."""
    create_table_from_parquet(self.client, unique_database, "out_of_range_date")

    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/out-of-range-date', new_vector, unique_database)

  def test_pre_gregorian_date_parquet(self, vector, unique_database):
    """Test date interoperability issues between Impala and Hive 2.1.1 when scanning
       a parquet table that contains dates that precede the introduction of Gregorian
       calendar in 1582-10-15.
    """
    create_table_from_parquet(self.client, unique_database, "hive2_pre_gregorian")
    self.run_test_case('QueryTest/hive2-pre-gregorian-date', vector, unique_database)

  def test_zero_rows(self, vector, unique_database):
    """IMPALA-3943: Tests that scanning files with num_rows=0 in the file footer
    succeeds without errors."""
    # Create test table with a file that has 0 rows and 0 row groups.
    create_table_from_parquet(self.client, unique_database, "zero_rows_zero_row_groups")
    # Create test table with a file that has 0 rows and 1 row group.
    create_table_from_parquet(self.client, unique_database, "zero_rows_one_row_group")

    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-zero-rows', vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-zero-rows', vector, unique_database)

  def test_repeated_root_schema(self, vector, unique_database):
    """IMPALA-4826: Tests that running a scan on a schema where the root schema's
       repetetion level is set to REPEATED succeeds without errors."""
    create_table_from_parquet(self.client, unique_database, "repeated_root_schema")

    result = self.client.execute(
        "select * from %s.repeated_root_schema" % unique_database)
    assert len(result.data) == 300

  def test_huge_num_rows(self, vector, unique_database):
    """IMPALA-5021: Tests that a zero-slot scan on a file with a huge num_rows in the
    footer succeeds without errors."""
    create_table_from_parquet(self.client, unique_database, "huge_num_rows")
    result = self.client.execute("select count(*) from %s.huge_num_rows"
      % unique_database)
    assert len(result.data) == 1
    assert "4294967294" in result.data

  @SkipIfFS.hive
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
    self.run_stmt_in_hive(hql_format.format(codec="snappy", year=2010, month=1))
    self.run_stmt_in_hive(hql_format.format(codec="gzip", year=2010, month=2))

    test_files = ["testdata/multi_compression_parquet_data/tinytable_0_gzip_snappy.parq",
                  "testdata/multi_compression_parquet_data/tinytable_1_snappy_gzip.parq"]
    create_table_and_copy_files(self.client, "create table {db}.{tbl} "
                                             "(a string, b string) stored as parquet",
                                unique_database, "multi_compression",
                                test_files)

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
    test_files = ["testdata/data/bad_rle_literal_count.parquet",
                  "testdata/data/bad_rle_repeat_count.parquet"]
    create_table_and_copy_files(self.client,
                                "create table {db}.{tbl} (c bigint) stored as parquet",
                                unique_database, "bad_rle_counts", test_files)
    # Querying the corrupted files should not DCHECK or crash.
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/parquet-corrupt-rle-counts', vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/parquet-corrupt-rle-counts-abort',
                       vector, unique_database)

  def corrupt_footer_len_common(self, vector, unique_database, testname_postfix):
    """Common code shared by some tests (such as the ones included in IMPALA-6442 patch).
       It creates a simple table then loads manually corrupted Parquet file, and runs
       simple query to trigger the printing of related messages. Individual test checks if
       the printed messageses are expected.
    """
    test_file = "testdata/data/corrupt_footer_len_" + testname_postfix + ".parquet"
    test_table = "corrupt_footer_len_" + testname_postfix
    test_spec = "QueryTest/parquet-corrupt-footer-len-" + testname_postfix
    # Create test table and copy the corrupt files into it.
    test_files = [test_file]
    create_table_and_copy_files(self.client,
                                "create table {db}.{tbl} (c bigint) stored as parquet",
                                unique_database, test_table, test_files)
    # Querying the corrupted files should not DCHECK or crash.
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case(test_spec, vector, unique_database)
    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case(test_spec, vector, unique_database)

  def test_corrupt_footer_len_decr(self, vector, unique_database):
    """IMPALA-6442: Misleading file offset reporting in error messages.
       Case tested: decrease the original Parquet footer size by 1, thus metadata
       deserialization fails and prints expected error message with correct file offset of
       the Parquet file metadata (footer).
    """
    self.corrupt_footer_len_common(vector, unique_database, "decr")

  def test_corrupt_footer_len_incr(self, vector, unique_database):
    """IMPALA-6442: Misleading file offset reporting in error messages.
       Case tested: make the Parquet footer size bigger than the file, thus the footer
       can not be loaded and corresponding error message is printed.
    """
    self.corrupt_footer_len_common(vector, unique_database, "incr")

  def test_bad_compressed_page_size(self, vector, unique_database):
    """IMPALA-6353: Tests that a parquet dict page with 0 compressed_page_size is
    gracefully handled. """
    create_table_from_parquet(self.client, unique_database,
                              "bad_compressed_dict_page_size")
    self.run_test_case('QueryTest/parquet-bad-compressed-dict-page-size', vector,
        unique_database)

  def test_def_levels(self, vector, unique_database):
    """Test that Impala behaves as expected when decoding def levels with different
       encodings - RLE, BIT_PACKED, etc."""
    create_table_from_parquet(self.client, unique_database,
                              "alltypes_agg_bitpacked_def_levels")
    self.run_test_case('QueryTest/parquet-def-levels', vector, unique_database)

  def test_bad_compression_codec(self, vector, unique_database):
    """IMPALA-6593: test the bad compression codec is handled gracefully. """
    test_files = ["testdata/data/bad_codec.parquet"]
    create_table_and_copy_files(self.client, """CREATE TABLE {db}.{tbl} (
          id INT, bool_col BOOLEAN, tinyint_col TINYINT, smallint_col SMALLINT,
          int_col INT, bigint_col BIGINT, float_col FLOAT, double_col DOUBLE,
          date_string_col STRING, string_col STRING, timestamp_col TIMESTAMP,
          year INT, month INT) STORED AS PARQUET""",
                                unique_database, "bad_codec",
                                test_files)
    self.run_test_case('QueryTest/parquet-bad-codec', vector, unique_database)

  def test_num_values_def_levels_mismatch(self, vector, unique_database):
    """IMPALA-6589: test the bad num_values handled correctly. """
    create_table_from_parquet(self.client, unique_database,
                              "num_values_def_levels_mismatch")
    self.run_test_case('QueryTest/parquet-num-values-def-levels-mismatch',
        vector, unique_database)

    """IMPALA-11134: Impala returns "Couldn't skip rows in file" error for old
    (possibly corrupt) Parquet file where there are more def levels than num_values"""
    create_table_from_parquet(self.client, unique_database,
        "too_many_def_levels")
    result = self.client.execute("select i_item_id from {0}."
        "too_many_def_levels where i_item_sk = 350963".format(unique_database))
    assert len(result.data) == 1
    assert "AAAAAAAACPKFFAAA" in result.data

  @SkipIfFS.hdfs_small_block
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

    num_scanners_with_no_reads_list = re.findall(
        'NumScannersWithNoReads: ([0-9]*)', result.runtime_profile)

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

  @SkipIfFS.hdfs_small_block
  @SkipIfLocal.multiple_impalad
  def test_multiple_blocks_mt_dop(self, vector):
    """Sanity check for MT scan nodes to make sure all blocks from the same file are read.
    2 scan ranges per node should be created to read 'lineitem_sixblocks' because
    there are 6 blocks and 3 scan nodes. We set mt_dop to 2, so ideally every instance
    should read a single range, but since they share a queue its not deterministic and
    instead we verify sum of ranges read on a backend is 2."""
    query = 'select count(l_orderkey) from functional_parquet.lineitem_sixblocks'
    try:
      self.client.set_configuration_option('mt_dop', '2')
      result = self.client.execute(query)
      TOTAL_ROWS = 40000
      ranges_complete_list = re.findall(r'ScanRangesComplete: ([0-9]*)',
        result.runtime_profile)
      num_rows_read_list = re.findall(r'RowsRead: [0-9.K]* \(([0-9]*)\)',
        result.runtime_profile)
      # The extra fragment is the "Averaged Fragment"
      assert len(num_rows_read_list) == 7
      assert len(ranges_complete_list) == 7

      # Extract the host for each fragment instance. The first is the coordinator
      # fragment instance.
      host_list = re.findall(r'host=(\S+:[0-9]*)', result.runtime_profile)
      assert len(host_list) == 7

      total_rows_read = 0
      # Skip the Averaged Fragment; it comes first in the runtime profile.
      for num_row_read in num_rows_read_list[1:]:
        total_rows_read += int(num_row_read)
      assert total_rows_read == TOTAL_ROWS

      # Again skip the Averaged Fragment; it comes first in the runtime profile.
      # With mt_dop 2, every backend will have 2 instances.
      ranges_per_host = {}
      for i in range(1, 7):
        host = host_list[i]
        if host not in ranges_per_host:
          ranges_per_host[host] = 0
        ranges_per_host[host] += int(ranges_complete_list[i])
      for host in ranges_per_host:
        assert ranges_per_host[host] == 2
    finally:
      self.client.clear_configuration()

  @SkipIfFS.hdfs_small_block
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

  @SkipIfFS.hdfs_small_block
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

    num_row_groups_list = re.findall('NumRowGroups: ([0-9]*)', result.runtime_profile)
    scan_ranges_complete_list = re.findall(
        'ScanRangesComplete: ([0-9]*)', result.runtime_profile)
    num_rows_read_list = re.findall(r'RowsRead: [0-9.K]* \(([0-9]*)\)',
        result.runtime_profile)

    REGEX_UNIT_SECOND = "[0-9]*[s]*[0-9]*[.]*[0-9]*[nm]*[s]*"
    REGEX_MIN_MAX_FOOTER_PROCESSING_TIME = \
        (r"FooterProcessingTime: \(Avg: %s ; \(Min: (%s) ; Max: (%s) ; "
         r"Number of samples: %s\)" % (REGEX_UNIT_SECOND, REGEX_UNIT_SECOND,
            REGEX_UNIT_SECOND, "[0-9]*"))
    footer_processing_time_list = re.findall(
        REGEX_MIN_MAX_FOOTER_PROCESSING_TIME, result.runtime_profile)

    # This will fail if the number of impalads != 3
    # The fourth fragment is the "Averaged Fragment"
    assert len(num_row_groups_list) == 4, result.runtime_profile
    assert len(scan_ranges_complete_list) == 4, result.runtime_profile
    assert len(num_rows_read_list) == 4, result.runtime_profile

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
    assert a_schema_elt.converted_type is None
    assert b_schema_elt.converted_type == ConvertedType.UTF8
    assert c_schema_elt.converted_type == ConvertedType.UTF8
    assert d_schema_elt.converted_type is None

  def test_resolution_by_name(self, vector, unique_database):
    self.run_test_case('QueryTest/parquet-resolution-by-name', vector,
                       use_db=unique_database)

  def test_decimal_encodings(self, vector, unique_database):
    # Create a table using an existing data file with dictionary-encoded, variable-length
    # physical encodings for decimals.
    test_files = ["testdata/data/binary_decimal_dictionary.parquet",
                  "testdata/data/binary_decimal_no_dictionary.parquet"]
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (small_dec decimal(9,2), med_dec decimal(18,2), large_dec decimal(38,2))
         STORED AS PARQUET""", unique_database, "decimal_encodings", test_files)

    create_table_from_parquet(self.client, unique_database, 'decimal_stored_as_int32')
    create_table_from_parquet(self.client, unique_database, 'decimal_stored_as_int64')
    create_table_from_parquet(self.client, unique_database,
        'decimal_padded_fixed_len_byte_array')
    create_table_from_parquet(self.client, unique_database,
        'decimal_padded_fixed_len_byte_array2')

    self.run_test_case('QueryTest/parquet-decimal-formats', vector, unique_database)

  def test_rle_encoded_bools(self, vector, unique_database):
    """IMPALA-6324: Test that Impala decodes RLE encoded booleans correctly."""
    create_table_from_parquet(self.client, unique_database, "rle_encoded_bool")
    self.run_test_case(
        'QueryTest/parquet-rle-encoded-bool', vector, unique_database)

  def test_dict_encoding_with_large_bit_width(self, vector, unique_database):
    """IMPALA-7147: Test that Impala can decode dictionary encoded pages where the
       dictionary index bit width is larger than the encoded byte's bit width.
    """
    TABLE_NAME = "dict_encoding_with_large_bit_width"
    create_table_from_parquet(self.client, unique_database, TABLE_NAME)
    result = self.execute_query(
        "select * from {0}.{1}".format(unique_database, TABLE_NAME))
    assert(len(result.data) == 33)

  def test_rle_dictionary_encoding(self, vector, unique_database):
    """IMPALA-6434: Add support to decode RLE_DICTIONARY encoded pages."""
    TABLE_NAME = "alltypes_tiny_rle_dictionary"
    create_table_from_parquet(self.client, unique_database, TABLE_NAME)
    self.run_test_case("QueryTest/parquet-rle-dictionary", vector, unique_database)

  def test_type_widening(self, vector, unique_database):
    """IMPALA-6373: Test that Impala can read parquet file with column types smaller than
       the schema with larger types"""
    TABLE_NAME = "primitive_type_widening"
    create_table_and_copy_files(self.client, """CREATE TABLE {db}.{tbl} (
        a smallint, b int, c bigint, d double, e int, f bigint, g double, h int,
        i double, j double) STORED AS PARQUET""", unique_database, TABLE_NAME,
        ["/testdata/data/{0}.parquet".format(TABLE_NAME)])

    self.run_test_case("QueryTest/parquet-type-widening", vector, unique_database)

  def test_error_propagation_race(self, vector, unique_database):
    """IMPALA-7662: failed scan signals completion before error is propagated. To
    reproduce, we construct a table with two Parquet files, one valid and another
    invalid. The scanner thread for the invalid file must propagate the error
    before we mark the whole scan complete."""
    if vector.get_value('exec_option')['debug_action'] is not None:
      pytest.skip(".test file needs to override debug action")
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['debug_action']
    create_table_and_copy_files(self.client,
        "CREATE TABLE {db}.{tbl} (s STRING) STORED AS PARQUET",
        unique_database, "bad_magic_number", ["testdata/data/bad_magic_number.parquet"])
    # We need the ranges to all be scheduled on the same impalad.
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case("QueryTest/parquet-error-propagation-race", new_vector,
                       unique_database)

  def test_int64_timestamps(self, vector, unique_database):
    """IMPALA-5050: Test that Parquet columns with int64 physical type and
       timestamp_millis/timestamp_micros logical type can be read both as
       int64 and as timestamp.
    """
    # Tiny plain encoded parquet file.
    TABLE_NAME = "int64_timestamps_plain"
    create_table_from_parquet(self.client, unique_database, TABLE_NAME)

    TABLE_NAME = "int64_bigints_plain"
    CREATE_SQL = """CREATE TABLE {0}.{1} (
                      new_logical_milli_utc BIGINT,
                      new_logical_milli_local BIGINT,
                      new_logical_micro_utc BIGINT,
                      new_logical_micro_local BIGINT
                     ) STORED AS PARQUET""".format(unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/int64_timestamps_plain.parquet"])

    # Larger dictionary encoded parquet file.
    TABLE_NAME = "int64_timestamps_dict"
    CREATE_SQL = """CREATE TABLE {0}.{1} (
                      id INT,
                      new_logical_milli_utc TIMESTAMP,
                      new_logical_milli_local TIMESTAMP,
                      new_logical_micro_utc TIMESTAMP,
                      new_logical_micro_local TIMESTAMP
                     ) STORED AS PARQUET""".format(unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/{0}.parquet".format(TABLE_NAME)])

    TABLE_NAME = "int64_bigints_dict"
    CREATE_SQL = """CREATE TABLE {0}.{1} (
                      id INT,
                      new_logical_milli_utc BIGINT,
                      new_logical_milli_local BIGINT,
                      new_logical_micro_utc BIGINT,
                      new_logical_micro_local BIGINT
                     ) STORED AS PARQUET""".format(unique_database, TABLE_NAME)
    create_table_and_copy_files(self.client, CREATE_SQL, unique_database, TABLE_NAME,
        ["/testdata/data/int64_timestamps_dict.parquet"])

    TABLE_NAME = "int64_timestamps_at_dst_changes"
    create_table_from_parquet(self.client, unique_database, TABLE_NAME)

    TABLE_NAME = "int64_timestamps_nano"
    create_table_from_parquet(self.client, unique_database, TABLE_NAME)

    self.run_test_case(
        'QueryTest/parquet-int64-timestamps', vector, unique_database)

  def _is_summary_stats_counter_empty(self, counter):
    """Returns true if the given TSummaryStatCounter is empty, false otherwise"""
    return counter.max_value == counter.min_value == counter.sum ==\
           counter.total_num_values == 0

  def test_page_size_counters(self, vector):
    """IMPALA-6964: Test that the counter Parquet[Un]compressedPageSize is updated
       when reading [un]compressed Parquet files, and that the counter
       Parquet[Un]compressedPageSize is not updated."""
    # lineitem_sixblocks is not compressed so ParquetCompressedPageSize should be empty,
    # but ParquetUncompressedPageSize should have been updated. Query needs an order by
    # so that all rows are read. Only access a couple of columns to reduce query runtime.
    result = self.client.execute("select l_orderkey"
                                 " from functional_parquet.lineitem_sixblocks"
                                 " order by l_orderkey limit 10")

    compressed_page_size_summaries = get_bytes_summary_stats_counter(
        "ParquetCompressedPageSize", result.runtime_profile)

    assert len(compressed_page_size_summaries) > 0
    for summary in compressed_page_size_summaries:
      assert self._is_summary_stats_counter_empty(summary)

    uncompressed_page_size_summaries = get_bytes_summary_stats_counter(
        "ParquetUncompressedPageSize", result.runtime_profile)

    # validate that some uncompressed data has been read; we don't validate the exact
    # amount as the value can change depending on Parquet format optimizations, Impala
    # scanner optimizations, etc.
    assert len(uncompressed_page_size_summaries) > 0
    for summary in uncompressed_page_size_summaries:
      assert not self._is_summary_stats_counter_empty(summary)

    # alltypestiny is compressed so both ParquetCompressedPageSize and
    # ParquetUncompressedPageSize should have been updated
    # Query needs an order by so that all rows are read.
    result = self.client.execute("select int_col from functional_parquet.alltypestiny"
                                 " order by int_col limit 10")

    for summary_name in ("ParquetCompressedPageSize", "ParquetUncompressedPageSize"):
      page_size_summaries = get_bytes_summary_stats_counter(
          summary_name, result.runtime_profile)
      assert len(page_size_summaries) > 0
      for summary in page_size_summaries:
        assert not self._is_summary_stats_counter_empty(summary)

  @SkipIfFS.hdfs_small_block
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_bytes_read_per_column(self, vector):
    """IMPALA-6964: Test that the counter Parquet[Un]compressedBytesReadPerColumn is
       updated when reading [un]compressed Parquet files, and that the counter
       Parquet[Un]CompressedBytesReadPerColumn is not updated."""
    # lineitem_sixblocks is not compressed so ParquetCompressedBytesReadPerColumn should
    # be empty, but ParquetUncompressedBytesReadPerColumn should have been updated
    # Query needs an order by so that all rows are read. Only access a couple of
    # columns to reduce query runtime.
    result = self.client.execute("select l_orderkey, l_partkey "
                                 "from functional_parquet.lineitem_sixblocks "
                                 " order by l_orderkey limit 10")

    compressed_bytes_read_per_col_summaries = get_bytes_summary_stats_counter(
        "ParquetCompressedBytesReadPerColumn", result.runtime_profile)

    # One aggregated counter and three per-instance counters. Agg counter is first.
    assert len(compressed_bytes_read_per_col_summaries) == 4
    for summary in compressed_bytes_read_per_col_summaries:
      assert self._is_summary_stats_counter_empty(summary)

    uncompressed_bytes_read_per_col_summaries = get_bytes_summary_stats_counter(
        "ParquetUncompressedBytesReadPerColumn", result.runtime_profile)

    # One aggregated counter and three per-instance counters. Agg counter is first.
    assert len(uncompressed_bytes_read_per_col_summaries) == 4
    for i, summary in enumerate(uncompressed_bytes_read_per_col_summaries):
      assert not self._is_summary_stats_counter_empty(summary), summary
      # There are 2 columns read from in lineitem_sixblocks so there should be 2 samples
      # per instance a 6 total.
      if i == 0:
          assert summary.total_num_values == 6
      else:
          assert summary.total_num_values == 2

    # alltypestiny is compressed so both ParquetCompressedBytesReadPerColumn and
    # ParquetUncompressedBytesReadPerColumn should have been updated
    # Query needs an order by so that all rows are read.
    result = self.client.execute("select * from functional_parquet.alltypestiny"
                                 " order by int_col limit 10")

    for summary_name in ("ParquetCompressedBytesReadPerColumn",
                         "ParquetUncompressedBytesReadPerColumn"):
      bytes_read_per_col_summaries = get_bytes_summary_stats_counter(summary_name,
          result.runtime_profile)
      # One aggregated counter and three per-instance counters. Agg counter is first.
      assert len(bytes_read_per_col_summaries) == 4
      for i, summary in enumerate(bytes_read_per_col_summaries):
        assert not self._is_summary_stats_counter_empty(summary)
        # There are 11 columns in alltypestiny so there should be 11 samples per instance.
        if i == 0:
          assert summary.total_num_values == 33
        else:
          assert summary.total_num_values == 11

  def test_decimal_precision_and_scale_widening(self, vector, unique_database):
    """IMPALA-7087: Tests that Parquet files stored with a lower precision or scale than
       the table metadata can be read by Impala.
    """
    # The file binary_decimal_precision_widening is written with schema (decimal(9,2),
    # decimal(18,2), decimal(38,2))
    binary_decimal_test_files =\
        ["testdata/data/binary_decimal_precision_and_scale_widening.parquet"]

    # Test reading Parquet files when the table has a higher precision than the file
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (small_dec decimal(38,2), med_dec decimal(38,2), large_dec decimal(38,2))
        STORED AS PARQUET""", unique_database, "binary_decimal_precision_widening",
        binary_decimal_test_files)

    # Test reading Parquet files when the table has a higher scale than the file
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (small_dec decimal(9,4), med_dec decimal(18,4), large_dec decimal(38,4))
        STORED AS PARQUET""", unique_database, "binary_decimal_scale_widening",
        binary_decimal_test_files)

    # Test reading Parquet files when the table has a higher precision and scale than the
    # file
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (small_dec decimal(38,4), med_dec decimal(38,4), large_dec decimal(38,4))
        STORED AS PARQUET""", unique_database,
        "binary_decimal_precision_and_scale_widening", binary_decimal_test_files)

    # Test Parquet precision and scale widening when decimals are stored as INT32
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (team string, score decimal(12, 6)) STORED AS PARQUET""", unique_database,
        "int32_decimal_precision_and_scale_widening",
        ["testdata/data/decimal_stored_as_int32.parquet"])

    # Test Parquet precision and scale widening when decimals are stored as INT64
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (team string, score decimal(32, 8)) STORED AS PARQUET""", unique_database,
        "int64_decimal_precision_and_scale_widening",
        ["testdata/data/decimal_stored_as_int64.parquet"])

    # Unlike the file binary_decimal_precision_and_scale_widening.parquet, all the values
    # in binary_decimal_no_dictionary.parquet cannot be converted to a higher scale
    # without overflowing
    create_table_and_copy_files(self.client, """create table if not exists {db}.{tbl}
        (small_dec decimal(9,4), med_dec decimal(18,4), large_dec decimal(38,4))
        STORED AS PARQUET""", unique_database, "scale_overflow",
        ["testdata/data/binary_decimal_no_dictionary.parquet"])

    self.run_test_case("QueryTest/parquet-decimal-precision-and-scale-widening", vector,
                       unique_database)

  def test_decimal_precision_and_scale_altering(self, vector, unique_database):
    self.run_test_case(
        "QueryTest/parquet-decimal-precision-and-scale-altering", vector, unique_database)

  def test_row_size_gt_4096_queries(self, unique_database):
    table_format = 'parquet'
    table_name = "{0}.{1}_{2}".format(
        unique_database, "t_row_size_gt_4096", table_format)

    # create table
    field_string = ', '.join('field{} STRING'.format(i) for i in range(1, 601))
    create_sql = "CREATE TABLE {} (id INT, {}) STORED AS {}".format(
        table_name, field_string, table_format)
    self.client.execute(create_sql)

    # insert data
    id_generation_sql = """
    WITH ten AS (
      SELECT 0 AS n
      UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3
      UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6
      UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9
    )
    SELECT
      row_number() OVER (ORDER BY a.n) AS id
    FROM
      ten a, ten b, ten c, ten d
    LIMIT
      2000
    """
    field_string = ', '.join(['CAST(RAND() AS STRING) AS field{}'.format(i)
                                for i in range(1, 601)])
    insert_sql = "INSERT INTO {} SELECT CAST(s.id AS INT), {} FROM ({}) s;".format(
        table_name, field_string, id_generation_sql)
    self.execute_query_expect_success(self.client, insert_sql)

    # do a query
    query_sql = "SELECT * FROM {} where field1 = '123'".format(table_name)
    self.execute_query_expect_success(self.client, query_sql)


# We use various scan range lengths to exercise corner cases in the HDFS scanner more
# thoroughly. In particular, it will exercise:
# 1. default scan range
# 2. scan range with no tuple
# 3. tuple that span across multiple scan ranges
# 4. scan range length = 16 for ParseSse() execution path
# 5. scan range fits at least one row
MAX_SCAN_RANGE_LENGTHS = [0, 1, 2, 5, 16, 17, 32, 512]


class TestScanRangeLengths(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScanRangeLengths, cls).add_test_dimensions()
    add_exec_option_dimension(cls, 'max_scan_range_length', MAX_SCAN_RANGE_LENGTHS)
    # Test doesn't need to be run for non-HDFS table formats.
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: not v.get_value('table_format').file_format in ('kudu', 'hbase'))

  def test_scan_ranges(self, vector):
    self.run_test_case('QueryTest/hdfs-tiny-scan', vector)


# Scan range lengths for TPC-H data sets. Test larger scan range sizes. Random
# variation to the length is added by the test in order to exercise edge cases.
TPCH_SCAN_RANGE_LENGTHS = [128 * 1024, 16 * 1024 * 1024]


class TestTpchScanRangeLengths(ImpalaTestSuite):
  """Exercise different scan range lengths on the larger TPC-H data sets."""

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTpchScanRangeLengths, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('scan_range_length', *TPCH_SCAN_RANGE_LENGTHS))

  def test_tpch_scan_ranges(self, vector):
    # Randomly adjust the scan range length to exercise different code paths.
    max_scan_range_length = \
        int(vector.get_value('scan_range_length') * (random.random() + 0.5))
    LOG.info("max_scan_range_length={0}".format(max_scan_range_length))
    vector.get_value('exec_option')['max_scan_range_length'] = max_scan_range_length
    self.run_test_case('tpch-scan-range-lengths', vector)


@SkipIf.not_s3
class TestParquetScanRangeAssigment(ImpalaTestSuite):
  """Test scan range assignment for Parquet files on S3. Since scan range assignment
  cannot be validated in the S3PlannerTest (see IMPALA-8942), validate it here."""

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetScanRangeAssigment, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def test_scan_range_skew(self, vector):
    """Validate that each scanner reads an even number of row groups (e.g. there is no
    skew). While scan ranges might be assigned evenly, scanners skip Parquet scan ranges
    that do not process a range that overlaps the Parquet row-group midpoint."""

    # Run TPC-H Q6, which re-produces the scan range assignment bug described in
    # IMPALA-3453.
    result = self.execute_query("select sum(l_extendedprice * l_discount) as revenue "
        "from tpch_parquet.lineitem where l_shipdate >= '1994-01-01' and "
        "l_shipdate < '1995-01-01' and l_discount between 0.05 and 0.07 and "
        "l_quantity < 24")

    # NumRowGroups tracks the number of row groups actually read, not necessarily the
    # number assigned. Assert that each fragment processed exactly one row group.
    self.__assert_counter_equals(r'NumRowGroups: (\d+)', 1, result.runtime_profile)
    # ScanRangesComplete tracks the number of scan ranges assigned to each fragment.
    # Assert that each fragment was assigned exactly one scan range.
    self.__assert_counter_equals(r'ScanRangesComplete: (\d+)', 1, result.runtime_profile)
    # NumScannersWithNoReads tracks the number of scan ranges that did not trigger any
    # reads. In the case of Parquet, this counter would be > 0 if a fragment was assigned
    # a scan range that does *not* contain the midpoint of a Parquet row group. Assert
    # that this value is always 0.
    self.__assert_counter_equals(r'NumScannersWithNoReads: (\d+)', 0,
        result.runtime_profile)

  def __assert_counter_equals(self, counter_regex, value, runtime_profile):
    """Helper method that asserts that the given counter_regex is in the given
    runtime_profile and that each occurence of the counter matches the expected value."""
    num_row_groups_counters = re.findall(counter_regex, runtime_profile)
    assert len(num_row_groups_counters) > 1
    for num_row_groups in num_row_groups_counters: assert int(num_row_groups) == value


# More tests for text scanner
# 1. Test file that ends w/o tuple delimiter
# 2. Test file with escape character
class TestTextScanRangeLengths1(ImpalaTestSuite):
  ESCAPE_TABLE_LIST = ["testescape_16_lf", "testescape_16_crlf",
      "testescape_17_lf", "testescape_17_crlf",
      "testescape_32_lf", "testescape_32_crlf"]

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTextScanRangeLengths1, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    add_exec_option_dimension(cls, 'max_scan_range_length', MAX_SCAN_RANGE_LENGTHS)

  def test_text_scanner(self, vector):
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
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

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
    data = ('a' * (DEFAULT_IO_BUFFER_SIZE - 1) + "\r\n" +  # first scan range
            'b' * (DEFAULT_IO_BUFFER_SIZE - 3) + "\r\n" +
            'a' * (DEFAULT_IO_BUFFER_SIZE - 1) + "\r\n" +  # second scan range
            'b' * (DEFAULT_IO_BUFFER_SIZE - 1))
    assert len(data) == DEFAULT_IO_BUFFER_SIZE * 4

    max_scan_range_length = DEFAULT_IO_BUFFER_SIZE * 2
    expected_result = data.split("\r\n")

    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['max_row_size'] = 9 * 1024 * 1024

    self._create_and_query_test_table(
      new_vector, unique_database, data, max_scan_range_length, expected_result)

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
      self.filesystem_client.copy_from_local(f.name, location)
    self.client.execute("refresh %s" % qualified_table_name)

    vector.get_value('exec_option')['max_scan_range_length'] = max_scan_range_length
    query = "select * from %s" % qualified_table_name
    result = self.execute_query_expect_success(
      self.client, query, vector.get_value('exec_option'))

    assert sorted(result.data) == sorted(expected_result)


# Test for IMPALA-1740: Support for skip.header.line.count
class TestTextScanRangeLengths2(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTextScanRangeLengths2, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec in ['none', 'gzip'])

  def test_text_scanner_with_header(self, vector, unique_database):
    # Remove to allow .test file to set abort_on_error.
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/hdfs-text-scan-with-header', new_vector,
                       test_file_vars={'$UNIQUE_DB': unique_database})

  def test_dateless_timestamp_text(self, vector, unique_database):
    """Test scanning text files which still includes dateless timestamps."""
    tbl_name = "timestamp_text_table"
    create_sql = "create table %s.%s (t timestamp) stored as textfile" % (
        unique_database, tbl_name)
    create_table_and_copy_files(self.client, create_sql, unique_database, tbl_name,
        ["/testdata/data/dateless_timestamps.txt"])

    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/dateless_timestamp_text', new_vector, unique_database)


# Missing Coverage: No coverage for truncated files errors or scans.
@SkipIfFS.hive
class TestScanTruncatedFiles(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
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
      cls.ImpalaTestMatrix.add_dimension(
          create_uncompressed_text_dimension(cls.get_workload()))
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

    # The file will not exist if the table is empty and the insert is done by Hive 3, so
    # another refresh is needed.
    self.execute_query("refresh %s" % fq_tbl_name)

    result = self.execute_query("select count(*) from %s" % fq_tbl_name)
    assert(len(result.data) == 1)
    assert(result.data[0] == str(num_rows))


class TestUncompressedText(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUncompressedText, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  # IMPALA-5315: Test support for date/time in unpadded format
  def test_scan_lazy_timestamp(self, vector, unique_database):
    test_files = ["testdata/data/lazy_timestamp.csv"]
    create_table_and_copy_files(self.client, """CREATE TABLE {db}.{tbl} (ts TIMESTAMP)""",
                                unique_database, "lazy_ts", test_files)
    self.run_test_case('QueryTest/select-lazy-timestamp', vector, unique_database)


class TestOrc(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestOrc, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'orc')
    add_exec_option_dimension(cls, 'orc_schema_resolution', [0, 1])

  @SkipIfFS.hdfs_small_block
  @SkipIfLocal.multiple_impalad
  def test_misaligned_orc_stripes(self, vector, unique_database):
    self._build_lineitem_table_helper(unique_database, 'lineitem_threeblocks',
        'lineitem_threeblocks.orc')
    self._build_lineitem_table_helper(unique_database, 'lineitem_sixblocks',
        'lineitem_sixblocks.orc')
    self._build_lineitem_table_helper(unique_database,
        'lineitem_orc_multiblock_one_stripe',
        'lineitem_orc_multiblock_one_stripe.orc')

    # functional_orc.alltypes is well-formatted. 'NumScannersWithNoReads' counters are
    # set to 0.
    table_name = 'functional_orc_def.alltypes'
    self._misaligned_orc_stripes_helper(table_name, 7300)
    # lineitem_threeblock.orc is ill-formatted but every scanner reads some stripes.
    # 'NumScannersWithNoReads' counters are set to 0.
    table_name = unique_database + '.lineitem_threeblocks'
    self._misaligned_orc_stripes_helper(table_name, 16000)
    # lineitem_sixblocks.orc is ill-formatted but every scanner reads some stripes.
    # 'NumScannersWithNoReads' counters are set to 0.
    table_name = unique_database + '.lineitem_sixblocks'
    self._misaligned_orc_stripes_helper(table_name, 30000)
    # Scanning lineitem_orc_multiblock_one_stripe.orc finds two scan ranges that end up
    # doing no reads because the file is poorly formatted.
    table_name = unique_database + '.lineitem_orc_multiblock_one_stripe'
    self._misaligned_orc_stripes_helper(
      table_name, 16000, num_scanners_with_no_reads=2)

  def _build_lineitem_table_helper(self, db, tbl, file):
    self.client.execute("create table %s.%s like tpch.lineitem stored as orc" % (db, tbl))
    tbl_loc = get_fs_path("/test-warehouse/%s.db/%s" % (db, tbl))
    # set block size to 156672 so lineitem_threeblocks.orc occupies 3 blocks,
    # lineitem_sixblocks.orc occupies 6 blocks.
    check_call(['hdfs', 'dfs', '-Ddfs.block.size=156672', '-copyFromLocal', '-d', '-f',
        os.environ['IMPALA_HOME'] + "/testdata/LineItemMultiBlock/" + file, tbl_loc])
    self.client.execute("refresh %s.%s" % (db, tbl))

  def _misaligned_orc_stripes_helper(
          self, table_name, rows_in_table, num_scanners_with_no_reads=0):
    """Checks if 'num_scanners_with_no_reads' indicates the expected number of scanners
    that don't read anything because the underlying file is poorly formatted.
    Additionally, test that select count(star) match with expected number of rows.
    """
    query = 'select * from %s' % table_name
    result = self.client.execute(query)
    assert len(result.data) == rows_in_table

    num_scanners_with_no_reads_list = re.findall(
      'NumScannersWithNoReads: ([0-9]*)', result.runtime_profile)

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

    # Test that select count(star) match with expected number of rows.
    query = 'select count(*) from %s' % table_name
    result = self.client.execute(query)
    assert int(result.data[0]) == rows_in_table

  # Skip this test on non-HDFS filesystems, because orc-type-check.test contains Hive
  # queries that hang in some cases (IMPALA-9345). It would be possible to separate
  # the tests that use Hive and run most tests on S3, but I think that running these on
  # S3 doesn't add too much coverage.
  # TODO(IMPALA-12349): Rename the columns to use the correct names (see
  # test_type_conversions_hive3).
  @SkipIfFS.hive
  @SkipIfHive3.non_acid
  def test_type_conversions_hive2(self, vector, unique_database):
    # Create "illtypes" tables whose columns can't match the underlining ORC file's.
    # Create an "safetypes" table likes above but ORC columns can still fit into it.
    # Reuse the data files of alltypestiny and date_tbl in funtional_orc_def.
    tbl_loc = get_fs_path("/test-warehouse/alltypestiny_orc_def")
    self.client.execute("""create external table %s.illtypes (c1 boolean, c2 float,
        c3 boolean, c4 tinyint, c5 smallint, c6 int, c7 boolean, c8 string, c9 int,
        c10 float, c11 bigint) partitioned by (year int, month int) stored as ORC
        location '%s';""" % (unique_database, tbl_loc))
    self.client.execute("""create external table %s.illtypes_ts_to_date (c1 boolean,
        c2 float, c3 boolean, c4 tinyint, c5 smallint, c6 int, c7 boolean, c8 string,
        c9 int, c10 float, c11 date) partitioned by (year int, month int) stored as ORC
        location '%s';""" % (unique_database, tbl_loc))
    self.client.execute("""create external table %s.safetypes (c1 bigint, c2 boolean,
        c3 smallint, c4 int, c5 bigint, c6 bigint, c7 double, c8 double, c9 char(3),
        c10 varchar(3), c11 timestamp) partitioned by (year int, month int) stored as ORC
        location '%s';""" % (unique_database, tbl_loc))
    self.client.execute("""create external table %s.illtypes_date_tbl (c1 boolean,
        c2 timestamp) partitioned by (date_part date) stored as ORC location '%s';"""
        % (unique_database, "/test-warehouse/date_tbl_orc_def"))
    self.client.execute("alter table %s.illtypes recover partitions" % unique_database)
    self.client.execute("alter table %s.illtypes_ts_to_date recover partitions"
        % unique_database)
    self.client.execute("alter table %s.safetypes recover partitions" % unique_database)
    self.client.execute("alter table %s.illtypes_date_tbl recover partitions"
        % unique_database)

    # Create a decimal table whose precisions don't match the underlining orc files.
    # Reuse the data files of functional_orc_def.decimal_tbl.
    decimal_loc = get_fs_path("/test-warehouse/decimal_tbl_orc_def")
    self.client.execute("""create external table %s.mismatch_decimals (d1 decimal(8,0),
        d2 decimal(8,0), d3 decimal(19,10), d4 decimal(20,20), d5 decimal(2,0))
        partitioned by (d6 decimal(9,0)) stored as orc location '%s'"""
        % (unique_database, decimal_loc))
    self.client.execute("alter table %s.mismatch_decimals recover partitions"
        % unique_database)

    self.run_test_case('DataErrorsTest/orc-type-checks', vector, unique_database)

  # Skip this test on non-HDFS filesystems, because orc-type-check.test contains Hive
  # queries that hang in some cases (IMPALA-9345). It would be possible to separate
  # the tests that use Hive and run most tests on S3, but I think that running these on
  # S3 doesn't add too much coverage.
  @SkipIfFS.hive
  @SkipIfHive2.acid
  def test_type_conversions_hive3(self, vector, unique_database):
    # Create "illtypes" tables whose columns can't match the underlining ORC file's.
    # Create an "safetypes" table likes above but ORC columns can still fit into it.
    # Reuse the data files of alltypestiny and date_tbl in funtional_orc_def.
    def create_plain_orc_table(fq_tbl_src, fq_tbl_dest):
      self.run_stmt_in_hive(
          "create table %s like %s stored as orc" % (fq_tbl_dest, fq_tbl_src))
      self.run_stmt_in_hive("insert into %s select * from %s" % (fq_tbl_dest, fq_tbl_src))
      self.client.execute("invalidate metadata %s" % fq_tbl_dest)
    tmp_alltypes = unique_database + ".alltypes"
    create_plain_orc_table("functional.alltypestiny", tmp_alltypes)
    tbl_loc = self._get_table_location(tmp_alltypes, vector)
    self.client.execute("""create table %s.illtypes (id boolean, bool_col float,
        tinyint_col boolean, smallint_col tinyint, int_col smallint, bigint_col int,
        float_col boolean, double_col string, date_string_col int, string_col float,
        timestamp_col bigint) partitioned by (year int, month int) stored as ORC
        location '%s'""" % (unique_database, tbl_loc))
    self.client.execute("""create table %s.illtypes_ts_to_date (id boolean,
        bool_col float, tinyint_col boolean, smallint_col tinyint, int_col smallint,
        bigint_col int, float_col boolean, double_col string,
        date_string_col int, string_col float, timestamp_col date)
        partitioned by (year int, month int) stored as ORC
        location '%s'""" % (unique_database, tbl_loc))
    self.client.execute("""create table %s.safetypes (id bigint, bool_col boolean,
        tinyint_col smallint, smallint_col int, int_col bigint, bigint_col bigint,
        float_col double, double_col double, date_string_col char(3),
        string_col varchar(3), timestamp_col timestamp)
        partitioned by (year int, month int) stored as ORC
        location '%s'""" % (unique_database, tbl_loc))
    tmp_date_tbl = unique_database + ".date_tbl"
    create_plain_orc_table("functional.date_tbl", tmp_date_tbl)
    date_tbl_loc = self._get_table_location(tmp_date_tbl, vector)
    self.client.execute("""create table %s.illtypes_date_tbl (id boolean,
        date_col timestamp) partitioned by (date_part date) stored as ORC location '%s'"""
        % (unique_database, date_tbl_loc))
    self.client.execute("alter table %s.illtypes recover partitions" % unique_database)
    self.client.execute("alter table %s.illtypes_ts_to_date recover partitions"
        % unique_database)
    self.client.execute("alter table %s.safetypes recover partitions" % unique_database)
    self.client.execute("alter table %s.illtypes_date_tbl recover partitions"
        % unique_database)

    # Create a decimal table whose precisions don't match the underlining orc files.
    # Reuse the data files of functional_orc_def.decimal_tbl.
    tmp_decimal_tbl = unique_database + ".decimal_tbl"
    create_plain_orc_table("functional.decimal_tbl", tmp_decimal_tbl)
    decimal_loc = self._get_table_location(tmp_decimal_tbl, vector)
    self.client.execute("""create table %s.mismatch_decimals (d1 decimal(8,0),
        d2 decimal(8,0), d3 decimal(19,10), d4 decimal(20,20), d5 decimal(2,0))
        partitioned by (d6 decimal(9,0)) stored as orc location '%s'"""
        % (unique_database, decimal_loc))
    self.client.execute("alter table %s.mismatch_decimals recover partitions"
        % unique_database)

    self.run_test_case('DataErrorsTest/orc-type-checks', vector, unique_database)

  def test_orc_timestamp_out_of_range(self, vector, unique_database):
      """Test the validation of out-of-range timestamps."""
      test_files = ["testdata/data/out_of_range_timestamp.orc"]
      create_table_and_copy_files(self.client, "create table {db}.{tbl} "
                                               "(ts timestamp) stored as orc",
                                  unique_database, "out_of_range_timestamp", test_files)
      new_vector = deepcopy(vector)
      del new_vector.get_value('exec_option')['abort_on_error']
      self.run_test_case('DataErrorsTest/orc-out-of-range-timestamp',
                         new_vector, unique_database)

  def test_orc_timestamp_with_local_timezone(self, vector, unique_database):
      """Test scanning of ORC file that contains 'timstamp with local timezone'."""
      test_files = ["testdata/data/timestamp_with_local_timezone.orc"]
      create_table_and_copy_files(self.client,
          "create table {db}.{tbl} "
          "(id int, user string, action string, event_time timestamp) "
          "stored as orc", unique_database, "timestamp_with_local_timezone", test_files)
      self.run_test_case("QueryTest/orc_timestamp_with_local_timezone", vector,
          unique_database)

  def _run_invalid_schema_test(self, unique_database, test_name, expected_error):
    """Copies 'test_name'.orc to a table and runs a simple query. These tests should
       cause an error during the processing of the ORC schema, so the file's columns do
       not have to match with the table's columns.
    """
    test_files = ["testdata/data/%s.orc" % test_name]
    create_table_and_copy_files(self.client,
        "CREATE TABLE {db}.{tbl} (id BIGINT) STORED AS ORC",
        unique_database, test_name, test_files)
    err = self.execute_query_expect_failure(self.client,
        "select count(id) from {0}.{1}".format(unique_database, test_name))
    assert expected_error in str(err)

  def test_invalid_schema(self, vector, unique_database):
    """Test scanning of ORC file with malformed schema."""
    self._run_invalid_schema_test(unique_database, "corrupt_schema",
        "Encountered parse error in tail of ORC file")
    self._run_invalid_schema_test(unique_database, "corrupt_root_type",
        "Root of the selected type returned by the ORC lib is not STRUCT: boolean.")

  def test_date_out_of_range_orc(self, vector, unique_database):
    """Test scanning orc files with an out of range date."""
    orc_tbl_name = "out_of_range_date_orc"
    create_sql = "create table %s.%s (d date) stored as orc" % (unique_database,
        orc_tbl_name)
    create_table_and_copy_files(self.client, create_sql, unique_database, orc_tbl_name,
        ["/testdata/data/out_of_range_date.orc"])

    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/out-of-range-date-orc', new_vector, unique_database)

  def test_pre_gregorian_date_orc(self, vector, unique_database):
    """Test date interoperability issues between Impala and Hive 2.1.1 when scanning
       an orc table that contains dates that precede the introduction of Gregorian
       calendar in 1582-10-15.
    """
    orc_tbl_name = "hive2_pre_gregorian_orc"
    create_sql = "create table %s.%s (d date) stored as orc" % (unique_database,
        orc_tbl_name)
    create_table_and_copy_files(self.client, create_sql, unique_database, orc_tbl_name,
        ["/testdata/data/hive2_pre_gregorian.orc"])

    self.run_test_case('QueryTest/hive2-pre-gregorian-date-orc', vector, unique_database)

  @SkipIfFS.hive
  def test_missing_field_orc(self, unique_database):
    # Test scanning orc files with missing fields in file meta.
    orc_tbl_name = unique_database + ".missing_field_orc"
    self.client.execute("create table %s (f0 int) stored as orc" % orc_tbl_name)
    self.run_stmt_in_hive("insert into table %s select 1" % orc_tbl_name)
    self.client.execute("refresh %s" % orc_tbl_name)

    self.client.execute("alter table %s add columns(f1 int)" % orc_tbl_name)
    result = self.client.execute("select f1 from %s " % orc_tbl_name)
    assert result.data == ['NULL']

    self.client.execute("alter table %s add columns(f2 STRUCT<s0:STRING, s1:STRING>)"
                        % orc_tbl_name)
    result = self.client.execute("select f2.s0 from %s " % orc_tbl_name)
    assert result.data == ['NULL']

    orc_tbl_name = unique_database + ".missing_field_full_txn_test"
    self.client.execute("create table %s(f0 int) stored as orc "
                        "tblproperties('transactional'='true')" % orc_tbl_name)
    self.run_stmt_in_hive("insert into %s values(0)" % orc_tbl_name)
    self.run_stmt_in_hive("alter table %s add columns(f1 int)" % orc_tbl_name)
    self.run_stmt_in_hive("insert into %s values(1,1)" % orc_tbl_name)
    self.client.execute("refresh %s" % orc_tbl_name)
    result = self.client.execute("select f1 from %s" % orc_tbl_name)
    assert len(result.data) == 2
    assert '1' in result.data
    assert 'NULL' in result.data

    # TODO: add a test case for Iceberg tables once IMPALA-10542 is done.
    # orc_tbl_name = unique_database + ".missing_field_iceberg_test"
    # self.client.execute("create table %s (f0 int) stored as iceberg "
    #                     "tblproperties('write.format.default' = 'orc')"
    #                     % orc_tbl_name)
    # self.run_stmt_in_hive("insert into %s values(0)" % orc_tbl_name)
    # self.run_stmt_in_hive("alter table %s add columns(f1 int)" % orc_tbl_name)
    # self.run_stmt_in_hive("insert into %s values(1,1)" % orc_tbl_name)
    # self.client.execute("refresh %s" % orc_tbl_name)
    # result = self.client.execute("select f1 from %s" % orc_tbl_name)
    # assert len(result.data) == 2
    # assert '1' in result.data
    # assert 'NULL' in result.data

    orc_tbl_name = unique_database + ".lineitem_orc_ext"
    test_file = "/test-warehouse/tpch.lineitem_orc_def"
    create_sql = "create external table %s like tpch_orc_def.lineitem " \
                 "location '%s'" % (orc_tbl_name, test_file)
    self.client.execute(create_sql)
    self.client.execute("alter table %s add columns (new_col int)" % orc_tbl_name)
    result = self.execute_query("select count(*) from %s where new_col is null"
                                % orc_tbl_name)
    assert len(result.data) == 1
    assert '6001215' in result.data


class TestScannerReservation(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestScannerReservation, cls).add_test_dimensions()
    # Only run with a single dimension - all queries are format-specific and
    # reference tpch or tpch_parquet directly.
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_scanners(self, vector):
    self.run_test_case('QueryTest/scanner-reservation', vector)


class TestErasureCoding(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIf.not_ec
  def test_erasure_coding(self, vector):
    self.run_test_case('QueryTest/hdfs-erasure-coding', vector)


class TestBinaryType(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBinaryType, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format != 'json')

  def test_binary_type(self, vector):
    self.run_test_case('QueryTest/binary-type', vector)


class TestBinaryInComplexType(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestBinaryInComplexType, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format in ['parquet', 'orc'])

  def test_binary_in_complex_type(self, vector):
    self.run_test_case('QueryTest/binary-in-complex-type', vector)


class TestParquetV2(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetV2, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_v2(self, vector):
    self.run_test_case('QueryTest/parquet-v2', vector)


class TestSingleFileTable(ImpalaTestSuite):
  """IMPALA-12589: Regression test for corner case behavior where a table LOCATION might
  point to a file instead of a directory. Expect SELECT and SHOW FILES to still work in
  that case."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSingleFileTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'text')

  def test_single_file_table(self, vector, unique_database):
    # Create a simple table with one column.
    params = {"db": unique_database, "tbl": "single_file_table"}
    create_tbl_ddl = ("create external table {db}.{tbl} (c1 int) "
                      "stored as textfile").format(**params)
    self.execute_query_expect_success(self.client, create_tbl_ddl)

    # Insert one value to the table.
    insert_stmt = "insert into {db}.{tbl} values (1)".format(**params)
    self.execute_query_expect_success(self.client, insert_stmt)

    # Show files and get the path to the first data file.
    show_files_stmt = "show files in {db}.{tbl}".format(**params)
    res = self.execute_query_expect_success(self.client, show_files_stmt)
    assert len(res.data) == 1
    hdfs_file_path = res.data[0].split("\t")[0]
    params['new_location'] = hdfs_file_path

    # Alter location to point a data file.
    alter_stmt = "alter table {db}.{tbl} set location '{new_location}'".format(**params)
    self.execute_query_expect_success(self.client, alter_stmt)

    # Show files and count star should still work.
    res = self.execute_query_expect_success(self.client, show_files_stmt)
    assert res.data[0].split("\t")[0] == (hdfs_file_path + '/')
    select_stmt = "select count(*) from {db}.{tbl}".format(**params)
    res = self.execute_query_expect_success(self.client, select_stmt)
    assert res.data[0].split("\t")[0] == '1'
