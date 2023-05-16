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
import random
import string

from collections import namedtuple
from subprocess import check_call
from parquet.ttypes import BoundaryOrder, ColumnIndex, OffsetIndex, PageHeader, PageType

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import (
    decode_stats_value,
    get_parquet_metadata,
    read_serialized_object
)

PAGE_INDEX_MAX_STRING_LENGTH = 64


@SkipIfLocal.parquet_file_size
class TestHdfsParquetTableIndexWriter(ImpalaTestSuite):
  """Since PARQUET-922 page statistics can be written before the footer.
  The tests in this class checks if Impala writes the page indices correctly.
  It is temporarily a custom cluster test suite because we need to set the
  enable_parquet_page_index_writing command-line flag for the Impala daemon
  in order to make it write the page index.
  TODO: IMPALA-5843 Once Impala is able to read the page index and also write it by
  default, this test suite should be moved back to query tests.
  """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsParquetTableIndexWriter, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def _get_row_group_from_file(self, parquet_file):
    """Returns namedtuples that contain the schema, stats, offset_index, column_index,
    and page_headers for each column in the first row group in file 'parquet_file'. Fails
    if the file contains multiple row groups.
    """
    ColumnInfo = namedtuple('ColumnInfo', ['schema', 'stats', 'offset_index',
        'column_index', 'page_headers'])

    file_meta_data = get_parquet_metadata(parquet_file)
    assert len(file_meta_data.row_groups) == 1
    # We only support flat schemas, the additional element is the root element.
    schemas = file_meta_data.schema[1:]
    row_group = file_meta_data.row_groups[0]
    assert len(schemas) == len(row_group.columns)
    row_group_index = []
    with open(parquet_file) as file_handle:
      for column, schema in zip(row_group.columns, schemas):
        column_index_offset = column.column_index_offset
        column_index_length = column.column_index_length
        column_index = None
        if column_index_offset and column_index_length:
          assert(column_index_offset >= 0 and column_index_length >= 0)
          column_index = read_serialized_object(ColumnIndex, file_handle,
                                                column_index_offset, column_index_length)
        column_meta_data = column.meta_data
        stats = None
        if column_meta_data:
          stats = column_meta_data.statistics

        offset_index_offset = column.offset_index_offset
        offset_index_length = column.offset_index_length
        offset_index = None
        page_headers = []
        if offset_index_offset and offset_index_length:
          assert(offset_index_offset >= 0 and offset_index_length >= 0)
          offset_index = read_serialized_object(OffsetIndex, file_handle,
                                                offset_index_offset, offset_index_length)
          for page_loc in offset_index.page_locations:
            assert(page_loc.offset >= 0 and page_loc.compressed_page_size >= 0)
            page_header = read_serialized_object(PageHeader, file_handle, page_loc.offset,
                                                 page_loc.compressed_page_size)
            page_headers.append(page_header)

        column_info = ColumnInfo(schema, stats, offset_index, column_index, page_headers)
        row_group_index.append(column_info)
    return row_group_index

  def _get_row_groups_from_hdfs_folder(self, hdfs_path, tmpdir):
    """Returns a list of column infos (containing the schema, stats, offset_index,
    column_index, and page_headers) for the first row group in all parquet files in
    'hdfs_path'.
    """
    row_group_indexes = []
    check_call(['hdfs', 'dfs', '-get', hdfs_path, tmpdir.strpath])
    for root, subdirs, files in os.walk(tmpdir.strpath):
      for f in files:
        parquet_file = os.path.join(root, str(f))
        row_group_indexes.append(self._get_row_group_from_file(parquet_file))
    return row_group_indexes

  def _validate_page_locations(self, page_locations):
    """Validate that the page locations are in order."""
    for previous_loc, current_loc in zip(page_locations[:-1], page_locations[1:]):
      assert previous_loc.offset < current_loc.offset
      assert previous_loc.first_row_index < current_loc.first_row_index

  def _validate_null_stats(self, index_size, column_info):
    """Validates the statistics stored in null_pages and null_counts."""
    column_index = column_info.column_index
    column_stats = column_info.stats
    assert column_index.null_pages is not None
    assert len(column_index.null_pages) == index_size
    assert column_index.null_counts is not None
    assert len(column_index.null_counts) == index_size

    for page_is_null, null_count, page_header in zip(column_index.null_pages,
        column_index.null_counts, column_info.page_headers):
      assert page_header.type == PageType.DATA_PAGE
      num_values = page_header.data_page_header.num_values
      assert not page_is_null or null_count == num_values

    if column_stats:
      assert column_stats.null_count == sum(column_index.null_counts)

  def _validate_min_max_values(self, index_size, column_info):
    """Validate min/max values of the pages in a column chunk."""
    column_index = column_info.column_index
    min_values = column_info.column_index.min_values
    assert len(min_values) == index_size
    max_values = column_info.column_index.max_values
    assert len(max_values) == index_size

    if not column_info.stats:
      return

    column_min_value_str = column_info.stats.min_value
    column_max_value_str = column_info.stats.max_value
    if column_min_value_str is None or column_max_value_str is None:
      # If either is None, then both need to be None.
      assert column_min_value_str is None and column_max_value_str is None
      # No min and max value, all pages need to be null
      for idx, null_page in enumerate(column_index.null_pages):
        assert null_page, "Page {} of column {} is not null, \
            but doesn't have min and max values!".format(idx, column_index.schema.name)
      # Everything is None, no further checks needed.
      return

    column_min_value = decode_stats_value(column_info.schema, column_min_value_str)
    for null_page, page_min_str in zip(column_index.null_pages, min_values):
      if not null_page:
        page_min_value = decode_stats_value(column_info.schema, page_min_str)
        # If type is str, page_min_value might have been truncated.
        if isinstance(page_min_value, basestring):
          assert page_min_value >= column_min_value[:len(page_min_value)]
        else:
          assert page_min_value >= column_min_value

    column_max_value = decode_stats_value(column_info.schema, column_max_value_str)
    for null_page, page_max_str in zip(column_index.null_pages, max_values):
      if not null_page:
        page_max_value = decode_stats_value(column_info.schema, page_max_str)
        # If type is str, page_max_value might have been truncated and incremented.
        if (isinstance(page_max_value, basestring) and
            len(page_max_value) == PAGE_INDEX_MAX_STRING_LENGTH):
          max_val_prefix = page_max_value.rstrip('\0')
          assert max_val_prefix[:-1] <= column_max_value
        else:
          assert page_max_value <= column_max_value

  def _validate_ordering(self, ordering, schema, null_pages, min_values, max_values):
    """Check if the ordering of the values reflects the value of 'ordering'."""

    def is_sorted(l, reverse=False):
      if not reverse:
        return all(a <= b for a, b in zip(l, l[1:]))
      else:
        return all(a >= b for a, b in zip(l, l[1:]))

    # Filter out null pages and decode the actual min/max values.
    actual_min_values = [decode_stats_value(schema, min_val)
                         for min_val, is_null in zip(min_values, null_pages)
                         if not is_null]
    actual_max_values = [decode_stats_value(schema, max_val)
                         for max_val, is_null in zip(max_values, null_pages)
                         if not is_null]

    # For ASCENDING and DESCENDING, both min and max values need to be sorted.
    if ordering == BoundaryOrder.ASCENDING:
      assert is_sorted(actual_min_values)
      assert is_sorted(actual_max_values)
    elif ordering == BoundaryOrder.DESCENDING:
      assert is_sorted(actual_min_values, reverse=True)
      assert is_sorted(actual_max_values, reverse=True)
    else:
      assert ordering == BoundaryOrder.UNORDERED
      # For UNORDERED, min and max values cannot be both sorted.
      assert not is_sorted(actual_min_values) or not is_sorted(actual_max_values)
      assert (not is_sorted(actual_min_values, reverse=True) or
              not is_sorted(actual_max_values, reverse=True))

  def _validate_boundary_order(self, column_info):
    """Validate that min/max values are really in the order specified by
    boundary order.
    """
    column_index = column_info.column_index
    self._validate_ordering(column_index.boundary_order, column_info.schema,
        column_index.null_pages, column_index.min_values, column_index.max_values)

  def _validate_parquet_page_index(self, hdfs_path, tmpdir):
    """Validates that 'hdfs_path' contains exactly one parquet file and that the rowgroup
    index in that file is in the valid format.
    """
    row_group_indexes = self._get_row_groups_from_hdfs_folder(hdfs_path, tmpdir)
    for columns in row_group_indexes:
      for column_info in columns:
        try:
          index_size = len(column_info.offset_index.page_locations)
          assert index_size > 0
          self._validate_page_locations(column_info.offset_index.page_locations)
          self._validate_null_stats(index_size, column_info)
          self._validate_min_max_values(index_size, column_info)
          self._validate_boundary_order(column_info)
        except AssertionError as e:
          e.args += ("Validation failed on column {}.".format(column_info.schema.name),)
          raise

  def _ctas_table(self, vector, unique_database, source_db, source_table,
                                   tmpdir, sorting_column=None):
    """Copies the contents of 'source_db.source_table' into a parquet table.
    """
    qualified_source_table = "{0}.{1}".format(source_db, source_table)
    table_name = source_table
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    # Setting num_nodes = 1 ensures that the query is executed on the coordinator,
    # resulting in a single parquet file being written.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.execute_query("drop table if exists {0}".format(qualified_table_name))
    if sorting_column is None:
      query = ("create table {0} stored as parquet as select * from {1}").format(
          qualified_table_name, qualified_source_table)
    else:
      query = ("create table {0} sort by({1}) stored as parquet as select * from {2}"
               ).format(qualified_table_name, sorting_column, qualified_source_table)
    self.execute_query(query, vector.get_value('exec_option'))

  def _ctas_table_and_verify_index(self, vector, unique_database, source_db, source_table,
                                   tmpdir, sorting_column=None):
    """Copies 'source_table' into a parquet table and makes sure that the index
    in the resulting parquet file is valid.
    """
    self._ctas_table(vector, unique_database, source_db, source_table, tmpdir,
        sorting_column)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
        source_table))
    qualified_source_table = "{0}.{1}".format(unique_database, source_table)
    self._validate_parquet_page_index(hdfs_path, tmpdir.join(qualified_source_table))

  def _create_table_with_values_of_type(self, col_type, vector, unique_database,
                                       table_name, values_sql):
    """Creates a parquet table that has a single string column, then invokes an insert
    statement on it with the 'values_sql' parameter. E.g. 'values_sql' is "('asdf')".
    It returns the HDFS path for the table.
    """
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    self.execute_query("drop table if exists {0}".format(qualified_table_name))
    vector.get_value('exec_option')['num_nodes'] = 1
    query = ("create table {0} (x {1}) stored as parquet").format(
        qualified_table_name, col_type)
    self.execute_query(query, vector.get_value('exec_option'))
    self.execute_query("insert into {0} values {1}".format(qualified_table_name,
        values_sql), vector.get_value('exec_option'))
    return get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
        table_name))

  def test_ctas_tables(self, vector, unique_database, tmpdir):
    """Test different Parquet files created via CTAS statements."""

    # Test that writing a parquet file populates the rowgroup indexes with the correct
    # values.
    self._ctas_table_and_verify_index(vector, unique_database, "functional", "alltypes",
        tmpdir)

    # Test that writing a parquet file populates the rowgroup indexes with the correct
    # values, using decimal types.
    self._ctas_table_and_verify_index(vector, unique_database, "functional",
        "decimal_tbl", tmpdir)

    # Test that writing a parquet file populates the rowgroup indexes with the correct
    # values, using date types.
    self._ctas_table_and_verify_index(vector, unique_database, "functional", "date_tbl",
        tmpdir)

    # Test that writing a parquet file populates the rowgroup indexes with the correct
    # values, using char types.
    self._ctas_table_and_verify_index(vector, unique_database, "functional",
        "chars_formats", tmpdir)

    # Test that we don't write min/max values in the index for null columns.
    # Ensure null_count is set for columns with null values.
    self._ctas_table_and_verify_index(vector, unique_database, "functional", "nulltable",
        tmpdir)

    # Test that when a ColumnChunk is written across multiple pages, the index is
    # valid.
    self._ctas_table_and_verify_index(vector, unique_database, "tpch", "customer",
        tmpdir)
    self._ctas_table_and_verify_index(vector, unique_database, "tpch", "orders",
        tmpdir)

    # Test that when the schema has a sorting column, the index is valid.
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional_parquet", "zipcode_incomes", tmpdir, "id")

    # Test table with wide row.
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional_parquet", "widerow", tmpdir)

    # Test tables with wide rows and many columns.
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional_parquet", "widetable_250_cols", tmpdir)
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional_parquet", "widetable_500_cols", tmpdir)
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional_parquet", "widetable_1000_cols", tmpdir)

    # Test table with 40001 distinct values that aligns full page with full dictionary.
    self._ctas_table_and_verify_index(vector, unique_database,
        "functional", "empty_parquet_page_source_impala10186", tmpdir)

  def test_max_string_values(self, vector, unique_database, tmpdir):
    """Test string values that are all 0xFFs or end with 0xFFs."""

    col_type = "STRING"
    # String value is all of 0xFFs but its length is less than PAGE_INDEX_TRUNCATE_LENGTH.
    short_tbl = "short_tbl"
    short_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
        unique_database, short_tbl,
        "(rpad('', {0}, chr(255)))".format(PAGE_INDEX_MAX_STRING_LENGTH - 1))
    self._validate_parquet_page_index(short_hdfs_path, tmpdir.join(short_tbl))

    # String value is all of 0xFFs and its length is PAGE_INDEX_TRUNCATE_LENGTH.
    fit_tbl = "fit_tbl"
    fit_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
        unique_database, fit_tbl,
        "(rpad('', {0}, chr(255)))".format(PAGE_INDEX_MAX_STRING_LENGTH))
    self._validate_parquet_page_index(fit_hdfs_path, tmpdir.join(fit_tbl))

    # All bytes are 0xFFs and the string is longer then PAGE_INDEX_TRUNCATE_LENGTH, so we
    # should not write page statistics.
    too_long_tbl = "too_long_tbl"
    too_long_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
        unique_database, too_long_tbl, "(rpad('', {0}, chr(255)))".format(
            PAGE_INDEX_MAX_STRING_LENGTH + 1))
    row_group_indexes = self._get_row_groups_from_hdfs_folder(too_long_hdfs_path,
        tmpdir.join(too_long_tbl))
    column = row_group_indexes[0][0]
    assert column.column_index is None
    # We always write the offset index
    assert column.offset_index is not None

    # Test string with value that starts with 'aaa' following with 0xFFs and its length is
    # greater than PAGE_INDEX_TRUNCATE_LENGTH. Max value should be 'aab'.
    aaa_tbl = "aaa_tbl"
    aaa_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
        unique_database, aaa_tbl,
        "(rpad('aaa', {0}, chr(255)))".format(PAGE_INDEX_MAX_STRING_LENGTH + 1))
    row_group_indexes = self._get_row_groups_from_hdfs_folder(aaa_hdfs_path,
        tmpdir.join(aaa_tbl))
    column = row_group_indexes[0][0]
    assert len(column.column_index.max_values) == 1
    max_value = column.column_index.max_values[0]
    assert max_value == 'aab'

  def test_row_count_limit(self, vector, unique_database, tmpdir):
    """Tests that we can set the page row count limit via a query option.
    """
    vector.get_value('exec_option')['parquet_page_row_count_limit'] = 20
    table_name = "alltypessmall"
    self._ctas_table_and_verify_index(vector, unique_database, "functional", table_name,
        tmpdir)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
        table_name))
    row_group_indexes = self._get_row_groups_from_hdfs_folder(hdfs_path,
        tmpdir.join(table_name))
    for row_group in row_group_indexes:
      for column in row_group:
        for page_header in column.page_headers:
          if page_header.data_page_header is not None:
            assert page_header.data_page_header.num_values == 20
    result_row20 = self.execute_query(
        "select * from {0}.{1} order by id".format(unique_database, table_name))
    result_orig = self.execute_query(
        "select * from functional.alltypessmall order by id")
    assert result_row20.data == result_orig.data

  def test_row_count_limit_nulls(self, vector, unique_database, tmpdir):
    """Tests that we can set the page row count limit on a table with null values.
    """
    vector.get_value('exec_option')['parquet_page_row_count_limit'] = 2
    null_tbl = 'null_table'
    null_tbl_path = self._create_table_with_values_of_type("STRING", vector,
        unique_database, null_tbl, "(NULL), (NULL), ('foo'), (NULL), (NULL), (NULL)")
    row_group_indexes = self._get_row_groups_from_hdfs_folder(null_tbl_path,
        tmpdir.join(null_tbl))
    column = row_group_indexes[0][0]
    assert column.column_index.null_pages[0] == True
    assert column.column_index.null_pages[1] == False
    assert column.column_index.null_pages[2] == True
    assert len(column.page_headers) == 3
    for page_header in column.page_headers:
      assert page_header.data_page_header.num_values == 2

  def test_disable_page_index_writing(self, vector, unique_database, tmpdir):
    """Tests that we can disable page index writing via a query option.
    """
    vector.get_value('exec_option')['parquet_write_page_index'] = False
    table_name = "alltypessmall"
    self._ctas_table(vector, unique_database, "functional", table_name, tmpdir)
    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
        table_name))
    row_group_indexes = self._get_row_groups_from_hdfs_folder(hdfs_path,
        tmpdir.join(table_name))
    for row_group in row_group_indexes:
      for column in row_group:
        assert column.offset_index is None
        assert column.column_index is None

  def test_matched_page_and_dict_limits(self, vector, unique_database, tmpdir):
    """Tests that we don't produce empty pages when the page and dictionary fill
    simultaneously. Dictionary limit is 40000.
    """
    vector.get_value('exec_option')['parquet_page_row_count_limit'] = 20000

    table_name = "empty_parquet_page_dict_limit"
    qualified_table_name = "{0}.{1}".format(unique_database, table_name)
    # Setting num_nodes = 1 ensures that the query is executed on the coordinator,
    # resulting in a single parquet file being written.
    vector.get_value('exec_option')['num_nodes'] = 1
    query = ("create table {0} stored as parquet as select distinct l_orderkey as n "
             "from tpch_parquet.lineitem order by l_orderkey limit 40001").format(
        qualified_table_name)
    self.execute_query(query, vector.get_value('exec_option'))

    hdfs_path = get_fs_path('/test-warehouse/{0}.db/{1}/'.format(unique_database,
        table_name))
    self._validate_parquet_page_index(hdfs_path, tmpdir.join(qualified_table_name))

  def test_row_count_limit_large_string(self, vector, unique_database, tmpdir):
    """Tests that if we write a very large string after hitting page row count limit, we
    don't write an empty page.
    """
    vector.get_value('exec_option')['parquet_page_row_count_limit'] = 2
    string_tbl = 'string_table'
    string_tbl_path = self._create_table_with_values_of_type("STRING", vector,
        unique_database, string_tbl, "('a'), ('b'), ('{0}')".format(
            ''.join([random.choice(string.ascii_letters + string.digits)
                     for _ in range(64 * 1024 + 1)])))
    self._validate_parquet_page_index(string_tbl_path, tmpdir.join(
        "{0}.{1}".format(unique_database, string_tbl)))

  def test_nan_values_for_floating_types(self, vector, unique_database, tmpdir):
    """ IMPALA-7304: Impala doesn't write column index for floating-point columns
    until PARQUET-1222 is resolved. This is modified by:
    IMPALA-8498: Write column index for floating types when NaN is not present.
    """
    for col_type in ["float", "double"]:
      nan_val = "(CAST('NaN' as " + col_type + "))"
      # Table contains no NaN values.
      no_nan_tbl = "no_nan_tbl_" + col_type
      no_nan_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
          unique_database, no_nan_tbl, "(1.5), (2.3), (4.5), (42.42), (3.1415), (0.0)")
      self._validate_parquet_page_index(no_nan_hdfs_path, tmpdir.join(no_nan_tbl))
      row_group_indexes = self._get_row_groups_from_hdfs_folder(no_nan_hdfs_path,
          tmpdir.join(no_nan_tbl))
      column = row_group_indexes[0][0]
      assert column.column_index is not None

      # Table contains NaN as first value.
      first_nan_tbl = "first_nan_tbl_" + col_type
      first_nan_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
          unique_database, first_nan_tbl,
          nan_val + ", (2.3), (4.5), (42.42), (3.1415), (0.0)")
      row_group_indexes = self._get_row_groups_from_hdfs_folder(first_nan_hdfs_path,
          tmpdir.join(first_nan_tbl))
      column = row_group_indexes[0][0]
      assert column.column_index is None

      # Table contains NaN as last value.
      last_nan_tbl = "last_nan_tbl_" + col_type
      last_nan_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
          unique_database, last_nan_tbl,
          "(1.5), (2.3), (42.42), (3.1415), (0.0), " + nan_val)
      row_group_indexes = self._get_row_groups_from_hdfs_folder(last_nan_hdfs_path,
          tmpdir.join(last_nan_tbl))
      column = row_group_indexes[0][0]
      assert column.column_index is None

      # Table contains NaN value in the middle.
      mid_nan_tbl = "mid_nan_tbl_" + col_type
      mid_nan_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
          unique_database, mid_nan_tbl,
          "(2.3), (4.5), " + nan_val + ", (42.42), (3.1415), (0.0)")
      row_group_indexes = self._get_row_groups_from_hdfs_folder(mid_nan_hdfs_path,
          tmpdir.join(mid_nan_tbl))
      column = row_group_indexes[0][0]
      assert column.column_index is None

      # Table contains only NaN values.
      only_nan_tbl = "only_nan_tbl_" + col_type
      only_nan_hdfs_path = self._create_table_with_values_of_type(col_type, vector,
          unique_database, only_nan_tbl, (nan_val + ", ") * 3 + nan_val)
      row_group_indexes = self._get_row_groups_from_hdfs_folder(only_nan_hdfs_path,
          tmpdir.join(only_nan_tbl))
      column = row_group_indexes[0][0]
      assert column.column_index is None
