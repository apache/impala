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

from collections import namedtuple
import math
import os
from subprocess import check_call

from impala_thrift_gen.parquet.ttypes import BloomFilterHeader
from tests.common.file_utils import create_table_and_copy_files
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata, read_serialized_object


class TestParquetBloomFilter(ImpalaTestSuite):
  """
  This suite tests Parquet Bloom filter optimizations.
  """

  # Filename relative to $IMPALA_HOME. Some functions ('create_table_and_copy_files')
  # prepend $IMPALA_HOME automatically, so we cannot include it here. Other functions,
  # like 'get_parquet_metadata'  need a full path.
  PARQUET_TEST_FILE = 'testdata/data/parquet-bloom-filtering.parquet'
  BloomFilterData = namedtuple('BloomFilterData', ['header', 'directory'])

  # The statement used to create the test tables.
  create_stmt = 'create table {db}.{tbl} (        '\
                '  int8_col TINYINT,              '\
                '  int16_col SMALLINT,            '\
                '  int32_col INT,                 '\
                '  int64_col BIGINT,              '\
                '  float_col FLOAT,               '\
                '  double_col DOUBLE,             '\
                '  string_col STRING,             '\
                '  char_col VARCHAR(3)            '\
                ')                                '\
                'stored as parquet                '
  set_tbl_props_stmt = 'alter table {db}.{tbl} '                            \
                       'set TBLPROPERTIES ("parquet.bloom.filter.columns"="'\
                       'int8_col   : 512,'                                  \
                       'int16_col  : 512,'                                  \
                       'int32_col  : 512,'                                  \
                       'int64_col  : 512,'                                  \
                       'float_col  : 512,'                                  \
                       'double_col : 512,'                                  \
                       'string_col : 512,'                                  \
                       '");'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetBloomFilter, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_bloom_filtering(self, vector, unique_database):
    """ Tests that Parquet Bloom filtering works when it is enabled. """
    self._create_test_table_from_file(str(unique_database), self.PARQUET_TEST_FILE)

    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filtering'] = True
    self.run_test_case('QueryTest/parquet-bloom-filter', vector, unique_database)

  def test_parquet_bloom_filtering_disabled(self, vector, unique_database):
    """ Check that there is no Parquet Bloom filtering when it is disabled. """
    self._create_test_table_from_file(str(unique_database), self.PARQUET_TEST_FILE)

    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filtering'] = False
    self.run_test_case('QueryTest/parquet-bloom-filter-disabled', vector, unique_database)

  def test_parquet_bloom_filtering_schema_change(self, vector, unique_database):
    """ Regression test for IMPALA-11345. Tests that the query does not fail when a new
    column is added to the table schema but the old Parquet files do not contain it and
    therefore no column is found for a conjunct while preparing Bloom filtering. """
    vector.get_value('exec_option')['parquet_bloom_filtering'] = True

    tbl_name = 'changed_schema'

    stmts = [
      'create table {db}.{tbl} (id INT) stored as parquet',
      'insert into {db}.{tbl} values (1),(2),(3)',
      'alter table {db}.{tbl} add columns (name STRING)',
      'insert into {db}.{tbl} values (4, "James")',
      'select * from {db}.{tbl} where name in ("Lily")'
    ]

    for stmt in stmts:
      self.execute_query_expect_success(self.client,
          stmt.format(db=str(unique_database), tbl=tbl_name),
          vector.get_value('exec_option'))

  def test_write_parquet_bloom_filter(self, vector, unique_database, tmpdir):
    # Get Bloom filters from the first row group of file PARQUET_TEST_FILE.
    reference_col_to_bloom_filter = self._get_first_row_group_bloom_filters(
        self.PARQUET_TEST_FILE)

    # Create a new Parquet file with the same data as in PARQUET_TEST_FILE.
    tbl_name = 'parquet_bloom_filter_writing'
    hdfs_path = self._create_empty_test_table(vector, str(unique_database), tbl_name)
    self._set_tbl_props_to_match_test_file(vector, str(unique_database), tbl_name)
    self._populate_table(vector, str(unique_database), tbl_name)

    # Get the created Parquet file and extract the Bloom filters.
    col_to_bloom_filter_list = self._get_first_row_group_bloom_filters_from_hdfs_dir(
        hdfs_path, tmpdir)
    # There should be exactly one file as we have written one row group.
    assert len(col_to_bloom_filter_list) == 1
    col_to_bloom_filter = col_to_bloom_filter_list[0]
    self._compare_bloom_filters_to_reference(
        reference_col_to_bloom_filter, col_to_bloom_filter)

    # Query an element that is and one that is not present in the table in column
    # 'column_name'. In the first case there should be no skipping, in the second case we
    # should skip the row group because of dictionary filtering, not Bloom filtering as
    # all the elements fit in the dictionary and if there is a dictionary we use that, not
    # the Bloom filter.
    column_name = 'int64_col'
    self._query_element_check_profile(vector, str(unique_database), tbl_name, column_name,
        0, ['NumBloomFilteredRowGroups: 0 (0)'], ['NumBloomFilteredRowGroups: 1 (1)'])
    self._query_element_check_profile(vector, str(unique_database), tbl_name, column_name,
        1, ['NumBloomFilteredRowGroups: 0 (0)', 'NumDictFilteredRowGroups: 1 (1)'],
        ['NumBloomFilteredRowGroups: 1 (1)', 'NumDictFilteredRowGroups: 0 (0)'])

  def test_fallback_from_dict(self, vector, unique_database, tmpdir):
    """ Tests falling back from dict encoding to plain encoding and using a Bloom filter
    after reaching the max dict size. """
    tbl_name = 'fallback_from_dict'
    column_name = 'col'
    self._create_table_dict_overflow(vector, str(unique_database), tbl_name,
        column_name, True)

    # Get the created Parquet file and extract the Bloom filters.
    hdfs_path = self._get_hdfs_path(str(unique_database), tbl_name)
    col_idx_to_bloom_filter_list = self._get_first_row_group_bloom_filters_from_hdfs_dir(
        hdfs_path, tmpdir)
    # There should be exactly one file as we have written one row group.
    assert len(col_idx_to_bloom_filter_list) == 1
    col_idx_to_bloom_filter = col_idx_to_bloom_filter_list[0]
    # The index of the only column is 0.
    bloom_filter_data = col_idx_to_bloom_filter[0]
    bitset = bloom_filter_data.directory

    # We should have inserted 'max_dict' + 1 elements into the Bloom filter when falling
    # back. If the implementation is incorrect and we did not copy the elements in the
    # dictionary of the dict encoding to the Bloom filter, only 1 element will have been
    # inserted, meaning that exactly 8 bytes have non-zero values. If there are more
    # non-zero bytes we can assume that the implementation does not have this error.
    assert isinstance(bitset, bytes)
    nonzero = 0
    for byte in bitset:
      if byte != 0:
        nonzero += 1
    assert nonzero > 8

    # Query an element that is and one that is not present in the table and check whether
    # we correctly do not skip and skip the row group, respectively.
    self._query_element_check_profile(vector, str(unique_database), tbl_name, column_name,
        2, ['NumBloomFilteredRowGroups: 0 (0)'], ['NumBloomFilteredRowGroups: 1 (1)'])
    self._query_element_check_profile(vector, str(unique_database), tbl_name, column_name,
        3, ['NumBloomFilteredRowGroups: 1 (1)'], ['NumBloomFilteredRowGroups: 0 (0)'])

  def test_fallback_from_dict_if_no_bloom_tbl_props(self, vector, unique_database,
      tmpdir):
    """ Tests falling back from dict encoding to plain encoding when Bloom filtering is
    not enabled in table properties, after reaching the max dict size. We check that no
    Bloom filter is written. """
    tbl_name = 'fallback_from_dict'
    column_name = 'col'
    self._create_table_dict_overflow(vector, str(unique_database), tbl_name, column_name,
        False)

    # Get the created Parquet file.
    hdfs_path = self._get_hdfs_path(str(unique_database), tbl_name)
    col_idx_to_bloom_filter_list = self._get_first_row_group_bloom_filters_from_hdfs_dir(
        hdfs_path, tmpdir)
    # There should be exactly one file as we have written one row group.
    assert len(col_idx_to_bloom_filter_list) == 1
    col_idx_to_bloom_filter = col_idx_to_bloom_filter_list[0]
    # There should be no Bloom filter.
    assert(len(col_idx_to_bloom_filter) == 0)

  def _query_element_check_profile(self, vector, db_name, tbl_name, col_name,
          element, strings_in_profile, strings_not_in_profile):
    """ Run a query filtering on column 'col_name' having value 'element' and asserts
    that the query profile contains the strings in 'strings_in_profile' and that it does
    not contain the strings in 'strings_not_in_profile'. Can be used for example to
    check whether the Bloom filter was used. """
    query_stmt = 'select {col_name} from {db}.{tbl} where {col_name} = {value}'

    result_in_table = self.execute_query(query_stmt.format(col_name=col_name,
        db=db_name, tbl=tbl_name, value=element),
        vector.get_value('exec_option'))

    for s in strings_in_profile:
      assert s in result_in_table.runtime_profile
    for s in strings_not_in_profile:
      assert s not in result_in_table.runtime_profile

  def _create_table_dict_overflow(self, vector, db_name, tbl_name, column_name,
      bloom_tbl_prop):
    max_dict_size = 40000
    ndv = max_dict_size + 1
    fpp = 0.05
    bitset_size = self._optimal_bitset_size(ndv, fpp)

    bloom_tbl_props = \
        'TBLPROPERTIES("parquet.bloom.filter.columns"="{col_name}:{size}")'.format(
            col_name=column_name, size=bitset_size)

    # Create a parquet table containing only even numbers so an odd number should be
    # filtered out based on the Bloom filter (if there is one).
    create_stmt_template = 'create table {db}.{tbl} stored as parquet {tbl_props} \
        as (select (row_number() over (order by o_orderkey)) * 2 as {col} \
        from tpch_parquet.orders limit {ndv})'
    create_stmt = create_stmt_template.format(db=db_name, tbl=tbl_name,
        tbl_props=bloom_tbl_props if bloom_tbl_prop else "", col=column_name, ndv=ndv)

    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filter_write'] = 'IF_NO_DICT'
    self.execute_query(create_stmt, vector.get_value('exec_option'))

  def _optimal_bitset_size(self, ndv, fpp):
    """ Based on ParquetBloomFilter::OptimalByteSize() in
    be/src/util/parquet-bloom-filter.h """
    log_res = None
    if (ndv == 0):
      log_res = 0
    else:
      words_in_bucket = 8.0
      m = -words_in_bucket * ndv / math.log(1 - math.pow(fpp, 1.0 / words_in_bucket))
      log_res = max(0, math.ceil(math.log(m / 8, 2)))
    return int(2 ** log_res)

  def _create_test_table_from_file(self, db_name, filename):
    create_table_and_copy_files(self.client, self.create_stmt,
                                db_name, 'parquet_bloom_filter', [filename])

  def _create_empty_test_table(self, vector, db_name, tbl_name):
    self.execute_query("drop table if exists {0}.{1}".format(db_name, tbl_name))
    vector.get_value('exec_option')['num_nodes'] = 1
    query = self.create_stmt.format(db=db_name, tbl=tbl_name)
    self.execute_query(query, vector.get_value('exec_option'))
    return self._get_hdfs_path(db_name, tbl_name)

  def _get_hdfs_path(self, db_name, tbl_name):
    return get_fs_path('/test-warehouse/{0}.db/{1}/'.format(db_name, tbl_name))

  def _set_tbl_props_to_match_test_file(self, vector, db_name, tbl_name):
    vector.get_value('exec_option')['num_nodes'] = 1
    query = self.set_tbl_props_stmt.format(db=db_name, tbl=tbl_name)
    self.execute_query(query, vector.get_value('exec_option'))

  def _populate_table(self, vector, db_name, tbl_name):
    # Populate the table with even numbers, as in the first row group in the file
    # PARQUET_TEST_FILE
    query_format = 'insert into {db}.{tbl} values {{values}}'.format(
        db=db_name, tbl=tbl_name)
    rows = []
    for i in range(100):
      k = (i * 2) % 128
      row_values = '({0}, {0}, {0}, {0}, {0}.0, {0}.0, \
          "{0}", cast("{0}" as VARCHAR(3)))'.format(k)
      rows.append(row_values)
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filter_write'] = 'ALWAYS'
    self.execute_query(query_format.format(values=", ".join(rows)),
        vector.get_value('exec_option'))

  def _get_first_row_group_bloom_filters_from_hdfs_dir(self, hdfs_path, tmpdir):
    """ Returns the bloom filters from the first row group (like
    _get_first_row_group_bloom_filters) from each file in the hdfs directory. """
    # Get the created Parquet file and extract the Bloom filters.
    check_call(['hdfs', 'dfs', '-get', hdfs_path, tmpdir.strpath])
    col_to_bloom_filter_list = []
    for root, _subdirs, files in os.walk(tmpdir.strpath):
      for filename in files:
        parquet_file = os.path.join(root, str(filename))
        col_to_bloom_filter_list.append(
            self._get_first_row_group_bloom_filters(parquet_file))
    return col_to_bloom_filter_list

  def _get_first_row_group_bloom_filters(self, parquet_file):
    # While other functions require a filename relative to $IMPALA_HOME, and prepend the
    # path of $IMPALA_HOME but this one does not so we have to prepend it ourselves.
    filename = os.path.join(os.environ['IMPALA_HOME'],
        parquet_file)
    file_meta_data = get_parquet_metadata(filename)
    # We only support flat schemas, the additional element is the root element.
    schemas = file_meta_data.schema[1:]
    # We are only interested in the first row group.
    row_group = file_meta_data.row_groups[0]
    assert len(schemas) == len(row_group.columns)
    col_to_bloom_filter = dict()
    with open(filename, 'rb') as file_handle:
      for i, column in enumerate(row_group.columns):
        column_meta_data = column.meta_data
        if column_meta_data and column_meta_data.bloom_filter_offset:
          bloom_filter = self._try_read_bloom_filter(file_handle,
              column_meta_data.bloom_filter_offset)
          if bloom_filter:
            col_to_bloom_filter[i] = bloom_filter
    return col_to_bloom_filter

  def _try_read_bloom_filter(self, file_handle, bloom_filter_offset):
    (header, header_size) = self._try_read_bloom_filter_header(
        file_handle, bloom_filter_offset)
    if header is None:
      return None
    file_handle.seek(bloom_filter_offset + header_size)
    bloom_filter_bytes = file_handle.read(header.numBytes)
    return self.BloomFilterData(header, bloom_filter_bytes)

  def _try_read_bloom_filter_header(self, file_handle, bloom_filter_offset):
    """ Returns the Bloom filter header and its size. If it is not found, None is returned
    instead of the header and the size is unspecified. """
    header = None
    header_size = 8
    while (header_size <= 1024 and header is None):
      try:
        header = read_serialized_object(BloomFilterHeader, file_handle,
          bloom_filter_offset, header_size)
      except EOFError:
        header_size *= 2
    return (header, header_size)

  def _compare_bloom_filters_to_reference(self,
       reference_col_to_bloom_filter, col_to_bloom_filter):
    expected_cols = [0, 1, 2, 3, 4, 5, 6]
    assert sorted(col_to_bloom_filter.keys()) == expected_cols,\
        "All columns except the last one (VARCHAR(3)) should have a Bloom filter."
    for col in expected_cols:
      (exp_header, exp_directory) = reference_col_to_bloom_filter[col]
      (header, directory) = col_to_bloom_filter[col]

      assert exp_header == header
      assert exp_directory == directory,\
          "Incorrect directory for Bloom filter for column no. {}.".format(col)

  # -- Iceberg-native Bloom filter properties (IMPALA-12700) -------------------
  ONE_MIB = 1024 * 1024

  def test_iceberg_bloom_filter_enabled(self, vector, unique_database, tmpdir):
    """ An Iceberg table honors 'write.parquet.bloom-filter-enabled.column.<col>'.
    Enabled columns get a Bloom filter sized at the default max-bytes (1 MiB) when no
    NDV is known; other columns get none. """
    tbl = 'ice_bloom_enabled'
    self._create_iceberg_bloom_table(unique_database, tbl,
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-enabled.column.s'='true'")
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    assert sorted(col_to_bytes.keys()) == ['i', 's']
    assert col_to_bytes['i'] == self.ONE_MIB
    assert col_to_bytes['s'] == self.ONE_MIB

  def test_iceberg_bloom_filter_max_bytes(self, vector, unique_database, tmpdir):
    """ 'write.parquet.bloom-filter-max-bytes' controls the Bloom filter size. """
    tbl = 'ice_bloom_max_bytes'
    self._create_iceberg_bloom_table(unique_database, tbl,
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-max-bytes'='131072'")
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    assert col_to_bytes == {'i': 131072}

  def test_iceberg_bloom_filter_ndv(self, vector, unique_database, tmpdir):
    """ An explicit 'write.parquet.bloom-filter-ndv.column.<col>' sizes the filter. """
    tbl = 'ice_bloom_ndv'
    self._create_iceberg_bloom_table(unique_database, tbl,
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-ndv.column.i'='100000',"
        "'write.parquet.bloom-filter-max-bytes'='8388608'")
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    # optimal size for ndv=100000, fpp=0.01 is 128 KiB (< 8 MiB max-bytes).
    assert col_to_bytes == {'i': self._optimal_bitset_size(100000, 0.01)}
    assert col_to_bytes == {'i': 131072}

  def test_iceberg_bloom_filter_ndv_from_stats(self, vector, unique_database, tmpdir):
    """ When no explicit NDV is given, the column's NDV statistic is used to size the
    filter (IMPALA-12700 enhancement over parquet-java). """
    tbl = 'ice_bloom_ndv_stats'
    self._create_iceberg_bloom_table(unique_database, tbl,
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-max-bytes'='8388608'")
    # Set the NDV statistic explicitly so the size is deterministic.
    self.execute_query("alter table `{0}`.`{1}` set column stats i ('numDVs'='100000')"
        .format(unique_database, tbl))
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    # The stats NDV (100000) sizes the filter to 128 KiB, well below the 8 MiB max.
    assert col_to_bytes == {'i': 131072}

  def test_iceberg_bloom_filter_impala_prop_precedence(self, vector, unique_database,
      tmpdir):
    """ The Impala-specific 'parquet.bloom.filter.columns' property wins over the
    Iceberg-native properties for columns present in both. """
    tbl = 'ice_bloom_precedence'
    self._create_iceberg_bloom_table(unique_database, tbl,
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-ndv.column.i'='1000000',"
        "'write.parquet.bloom-filter-enabled.column.s'='true',"
        "'write.parquet.bloom-filter-max-bytes'='8388608',"
        "'parquet.bloom.filter.columns'='i:1024'")
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    # 'i': Impala property (1024) wins over the Iceberg-derived size.
    # 's': only enabled via the Iceberg property, no NDV/stats -> the configured
    # max-bytes (8 MiB).
    assert col_to_bytes == {'i': 1024, 's': 8388608}

  def test_iceberg_bloom_filter_optimize(self, vector, unique_database, tmpdir):
    """ OPTIMIZE rewrites the data files of an Iceberg table, so the compacted file
    honors the Iceberg-native Bloom filter properties, including properties that were set
    after the original data files had been written. """
    tbl = 'ice_bloom_optimize'
    fq_tbl = "`{0}`.`{1}`".format(unique_database, tbl)
    # Format version 2 so the table also accepts DELETE.
    self._create_iceberg_bloom_table(unique_database, tbl, "'format-version'='2'")
    # Two INSERTs without Bloom filter properties create two files without Bloom filters.
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    self._insert_iceberg_bloom_rows(vector, unique_database, tbl)
    file_to_bytes = self._get_iceberg_bloom_filter_bytes_per_file(
        unique_database, tbl, tmpdir)
    assert list(file_to_bytes.values()) == [{}, {}]
    # Add a delete file so OPTIMIZE also has to merge delete deltas.
    self.execute_query("delete from {0} where i = 3".format(fq_tbl))

    self.execute_query("alter table {0} set tblproperties("
        "'write.parquet.bloom-filter-enabled.column.i'='true',"
        "'write.parquet.bloom-filter-ndv.column.i'='100000',"
        "'write.parquet.bloom-filter-enabled.column.s'='true',"
        "'write.parquet.bloom-filter-max-bytes'='8388608')".format(fq_tbl))
    # The exec options set by _insert_iceberg_bloom_rows() (num_nodes=1 and
    # parquet_bloom_filter_write=ALWAYS) make OPTIMIZE write a single file with Bloom
    # filters.
    self.execute_query("optimize table {0}".format(fq_tbl),
        vector.get_value('exec_option'))
    col_to_bytes = self._get_iceberg_bloom_filter_bytes(unique_database, tbl, tmpdir)
    # 'i': sized by the explicit NDV (128 KiB). 's': no NDV -> max-bytes (8 MiB).
    assert col_to_bytes == {'i': 131072, 's': 8388608}
    assert self.execute_query("select i, s from {0} order by i".format(fq_tbl)).data == \
        ['1\ta', '1\ta', '2\tb', '2\tb']

  @SkipIfFS.hive
  def test_iceberg_bloom_filter_invalid_prop_warns(self, vector, unique_database):
    """ When an Iceberg table carries invalid Parquet Bloom filter properties (e.g. set
    by another engine, bypassing Impala's CREATE / ALTER validation), Impala does not
    fail the write: it ignores the properties and surfaces a warning to the user, while
    the write itself succeeds (and simply produces no Bloom filters). This holds for
    every write path that emits data files: INSERT, UPDATE, MERGE and OPTIMIZE.
    IMPALA-12700. """
    fq_tbl = "{0}.{1}".format(unique_database, 'ice_bloom_invalid')
    # Format version 2 so the table also accepts UPDATE / MERGE.
    self.execute_query(
        "create table {0} (i int, s string, d double) stored as iceberg "
        "tblproperties('format-version'='2')".format(fq_tbl))
    # Set an invalid 'max-bytes' through Hive, which does not run Impala's property
    # validation, mimicking a table configured by another engine.
    self.run_stmt_in_hive(
        "alter table {0} set tblproperties"
        "('write.parquet.bloom-filter-max-bytes'='not_a_number')".format(fq_tbl))
    self.execute_query("refresh {0}".format(fq_tbl))
    exec_opts = vector.get_value('exec_option')

    def assert_warns(result):
      assert "Ignoring invalid Iceberg Parquet Bloom filter properties" in result.log
      assert "write.parquet.bloom-filter-max-bytes" in result.log

    # Every write path that produces data files warns, yet still succeeds (the invalid
    # property is ignored, not fatal).
    assert_warns(self.execute_query(
        "insert into {0} values (1, 'a', 1.0)".format(fq_tbl), exec_opts))
    assert_warns(self.execute_query(
        "update {0} set d = 2.0 where i = 1".format(fq_tbl), exec_opts))
    assert_warns(self.execute_query(
        "merge into {0} target using (select 1 id) source on target.i = source.id "
        "when matched then update set d = 3.0 "
        "when not matched then insert values (2, 'b', 4.0)".format(fq_tbl), exec_opts))
    assert_warns(self.execute_query("optimize table {0}".format(fq_tbl), exec_opts))
    # The row is still present (and updated) despite the ignored Bloom filter properties.
    # Compare 'd' in SQL, as the string format of DOUBLE results differs between versions.
    assert self.execute_query(
        "select i, d = 3.0 from {0}".format(fq_tbl)).data == ['1\ttrue']

  def _create_iceberg_bloom_table(self, db_name, tbl_name, tbl_props):
    self.execute_query("drop table if exists `{0}`.`{1}`".format(db_name, tbl_name))
    self.execute_query(
        "create table `{0}`.`{1}` (i int, s string, d double) stored as iceberg "
        "tblproperties({2})".format(db_name, tbl_name, tbl_props))

  def _insert_iceberg_bloom_rows(self, vector, db_name, tbl_name):
    vector.get_value('exec_option')['num_nodes'] = 1
    # ALWAYS ensures the Bloom filter is written even for fully dict-encoded columns.
    vector.get_value('exec_option')['parquet_bloom_filter_write'] = 'ALWAYS'
    self.execute_query(
        "insert into `{0}`.`{1}` values (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', 3.0)"
        .format(db_name, tbl_name), vector.get_value('exec_option'))

  def _get_iceberg_bloom_filter_bytes(self, db_name, tbl_name, tmpdir):
    """ Returns a dict of column name -> Bloom filter bitset size (numBytes) for the
    columns that have a Bloom filter in the (single) data file of an Iceberg table. """
    file_to_bytes = self._get_iceberg_bloom_filter_bytes_per_file(
        db_name, tbl_name, tmpdir)
    assert len(file_to_bytes) == 1, \
        "Expected exactly one data file, found: {0}".format(list(file_to_bytes.keys()))
    return list(file_to_bytes.values())[0]

  def _get_iceberg_bloom_filter_bytes_per_file(self, db_name, tbl_name, tmpdir):
    """ Returns a dict of file path -> (column name -> Bloom filter bitset size) for every
    Parquet file of an Iceberg table. """
    result = self.execute_query("show files in `{0}`.`{1}`".format(db_name, tbl_name))
    file_to_bytes = dict()
    for row in result.data:
      path = row.split('\t')[0]
      if path.endswith('.parq') or path.endswith('.parquet'):
        file_to_bytes[path] = self._get_bloom_filter_bytes_from_file(path, tmpdir)
    return file_to_bytes

  def _get_bloom_filter_bytes_from_file(self, path, tmpdir):
    local_file = os.path.join(tmpdir.strpath, 'ice_data.parq')
    check_call(['hdfs', 'dfs', '-get', '-f', path, local_file])

    file_meta_data = get_parquet_metadata(local_file)
    schema = file_meta_data.schema[1:]  # Skip the root element.
    row_group = file_meta_data.row_groups[0]
    assert len(schema) == len(row_group.columns)
    col_to_bytes = dict()
    with open(local_file, 'rb') as file_handle:
      for i, column in enumerate(row_group.columns):
        column_meta_data = column.meta_data
        if column_meta_data and column_meta_data.bloom_filter_offset:
          (header, _size) = self._try_read_bloom_filter_header(
              file_handle, column_meta_data.bloom_filter_offset)
          if header is not None:
            col_to_bytes[schema[i].name] = header.numBytes
    return col_to_bytes
