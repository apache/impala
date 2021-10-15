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

import datetime
import os
import pytest
import random
import time

from subprocess import check_call
from parquet.ttypes import ConvertedType

from avro.datafile import DataFileReader
from avro.io import DatumReader
import json

from tests.common.impala_test_suite import ImpalaTestSuite, LOG
from tests.common.skip import SkipIf

from tests.util.filesystem_utils import get_fs_path
from tests.util.get_parquet_metadata import get_parquet_metadata

class TestIcebergTable(ImpalaTestSuite):
  """Tests related to Iceberg tables."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_iceberg_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-negative', vector, use_db=unique_database)

  def test_create_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-create', vector, use_db=unique_database)

  def test_alter_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-alter', vector, use_db=unique_database)

  def test_truncate_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-truncate', vector, use_db=unique_database)

  @SkipIf.not_hdfs
  def test_drop_incomplete_table(self, vector, unique_database):
    """Test DROP TABLE when the underlying directory is deleted. In that case table
    loading fails, but we should be still able to drop the table from Impala."""
    tbl_name = unique_database + ".synchronized_iceberg_tbl"
    cat_location = "/test-warehouse/" + unique_database
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.catalog',
                      'iceberg.catalog_location'='{1}')""".format(tbl_name, cat_location))
    self.hdfs_client.delete_file_dir(cat_location, True)
    self.execute_query_expect_success(self.client, """drop table {0}""".format(tbl_name))

  @SkipIf.not_hdfs
  def test_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-insert', vector, use_db=unique_database)

  def test_partitioned_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partitioned-insert', vector,
        use_db=unique_database)

  @SkipIf.not_hdfs
  def test_insert_overwrite(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-overwrite', vector, use_db=unique_database)

  def test_ctas(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-ctas', vector, use_db=unique_database)

  def test_partition_transform_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partition-transform-insert', vector,
        use_db=unique_database)

  def test_iceberg_orc_field_id(self, vector):
    self.run_test_case('QueryTest/iceberg-orc-field-id', vector)

  def test_catalogs(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-catalogs', vector, use_db=unique_database)

  def test_describe_history(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-table-history', vector, use_db=unique_database)

    # Create a table with multiple snapshots and verify the table history.
    tbl_name = unique_database + ".iceberg_multi_snapshots"
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.tables')""".format(tbl_name))
    result = self.client.execute("INSERT INTO {0} VALUES (1)".format(tbl_name))
    result = self.client.execute("INSERT INTO {0} VALUES (2)".format(tbl_name))
    result = self.client.execute("DESCRIBE HISTORY {0}".format(tbl_name))
    assert(len(result.data) == 2)
    first_snapshot = result.data[0].split("\t")
    second_snapshot = result.data[1].split("\t")
    # Check that first snapshot is older than the second snapshot.
    assert(first_snapshot[0] < second_snapshot[0])
    # Check that second snapshot's parent ID is the snapshot ID of the first snapshot.
    assert(first_snapshot[1] == second_snapshot[2])
    # The first snapshot has no parent snapshot ID.
    assert(first_snapshot[2] == "NULL")
    # Check "is_current_ancestor" column.
    assert(first_snapshot[3] == "TRUE" and second_snapshot[3] == "TRUE")

  def test_time_travel(self, vector, unique_database):
    tbl_name = unique_database + ".time_travel"

    def execute_query_ts(query):
      impalad_client.execute(query)
      return str(datetime.datetime.now())

    def expect_results(query, expected_results):
      data = impalad_client.execute(query)
      assert len(data.data) == len(expected_results)
      for r in expected_results:
        assert r in data.data

    def expect_results_t(ts, expected_results):
      expect_results(
          "select * from {0} for system_time as of {1}".format(tbl_name, ts),
          expected_results)

    def expect_results_v(snapshot_id, expected_results):
      expect_results(
          "select * from {0} for system_version as of {1}".format(tbl_name, snapshot_id),
          expected_results)

    def quote(s):
      return "'{0}'".format(s)

    def cast_ts(ts):
        return "CAST({0} as timestamp)".format(quote(ts))

    def get_snapshots():
      data = impalad_client.execute("describe history {0}".format(tbl_name))
      ret = list()
      for row in data.data:
        fields = row.split('\t')
        ret.append(fields[1])
      return ret

    def impala_now():
      now_data = impalad_client.execute("select now()")
      return now_data.data[0]

    # We are setting the TIMEZONE query option in this test, so let's create a local
    # impala client.
    with self.create_impala_client() as impalad_client:
      # Iceberg doesn't create a snapshot entry for the initial empty table
      impalad_client.execute("create table {0} (i int) stored as iceberg".format(tbl_name))
      ts_1 = execute_query_ts("insert into {0} values (1)".format(tbl_name))
      ts_2 = execute_query_ts("insert into {0} values (2)".format(tbl_name))
      ts_3 = execute_query_ts("truncate table {0}".format(tbl_name))
      time.sleep(5)
      ts_4 = execute_query_ts("insert into {0} values (100)".format(tbl_name))
      # Query table as of timestamps.
      expect_results_t("now()", ['100'])
      expect_results_t(quote(ts_1), ['1'])
      expect_results_t(quote(ts_2), ['1', '2'])
      expect_results_t(quote(ts_3), [])
      expect_results_t(cast_ts(ts_3) + " + interval 1 seconds", [])
      expect_results_t(quote(ts_4), ['100'])
      expect_results_t(cast_ts(ts_4) + " - interval 5 seconds", [])
      # Future queries return the current snapshot.
      expect_results_t(cast_ts(ts_4) + " + interval 1 hours", ['100'])
      # Query table as of snapshot IDs.
      snapshots = get_snapshots()
      expect_results_v(snapshots[0], ['1'])
      expect_results_v(snapshots[1], ['1', '2'])
      expect_results_v(snapshots[2], [])
      expect_results_v(snapshots[3], ['100'])

      # SELECT diff
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_new}'
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_old}'""".format(
                     tbl=tbl_name, ts_new=ts_2, ts_old=ts_1),
                     ['2'])
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_new}
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_old}""".format(
                     tbl=tbl_name, v_new=snapshots[1], v_old=snapshots[0]),
                     ['2'])
      # Mix SYSTEM_TIME ans SYSTEM_VERSION
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_new}
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_old}'""".format(
                     tbl=tbl_name, v_new=snapshots[1], ts_old=ts_1),
                     ['2'])
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_new}'
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_old}""".format(
                     tbl=tbl_name, ts_new=ts_2, v_old=snapshots[0]),
                     ['2'])

      # Query old snapshot
      try:
        impalad_client.execute("SELECT * FROM {0} FOR SYSTEM_TIME AS OF {1}".format(
            tbl_name, "now() - interval 2 years"))
        assert False  # Exception must be thrown
      except Exception as e:
        assert "Cannot find a snapshot older than" in str(e)
      # Query invalid snapshot
      try:
        impalad_client.execute("SELECT * FROM {0} FOR SYSTEM_VERSION AS OF 42".format(
            tbl_name))
        assert False  # Exception must be thrown
      except Exception as e:
        assert "Cannot find snapshot with ID 42" in str(e)

      # Check that timezone is interpreted in local timezone controlled by query option
      # TIMEZONE
      impalad_client.execute("truncate table {0}".format(tbl_name))
      impalad_client.execute("insert into {0} values (1111)".format(tbl_name))
      impalad_client.execute("SET TIMEZONE='Europe/Budapest'")
      now_budapest = impala_now()
      expect_results_t(quote(now_budapest), ['1111'])

      # Let's switch to Tokyo time. Tokyo time is always greater than Budapest time.
      impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
      now_tokyo = impala_now()
      expect_results_t(quote(now_tokyo), ['1111'])
      try:
        # Interpreting Budapest time in Tokyo time points to the past when the table
        # didn't exist.
        expect_results_t(quote(now_budapest), [])
        assert False
      except Exception as e:
        assert "Cannot find a snapshot older than" in str(e)

  @SkipIf.not_hdfs
  def test_strings_utf8(self, vector, unique_database):
    # Create table
    table_name = "ice_str_utf8"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
    query = 'create table %s (a string) stored as iceberg' % qualified_table_name
    self.client.execute(query)

    # Inserted string data should have UTF8 annotation regardless of query options.
    query = 'insert into %s values ("impala")' % qualified_table_name
    self.execute_query(query, {'parquet_annotate_strings_utf8': False})

    # Copy the created file to the local filesystem and parse metadata
    local_file = '/tmp/iceberg_utf8_test_%s.parq' % random.randint(0, 10000)
    LOG.info("test_strings_utf8 local file name: " + local_file)
    hdfs_file = get_fs_path('/test-warehouse/%s.db/%s/data/*.parq'
        % (unique_database, table_name))
    check_call(['hadoop', 'fs', '-copyToLocal', hdfs_file, local_file])
    metadata = get_parquet_metadata(local_file)

    # Extract SchemaElements corresponding to the table column
    a_schema_element = metadata.schema[1]
    assert a_schema_element.name == 'a'

    # Check that the schema uses the UTF8 annotation
    assert a_schema_element.converted_type == ConvertedType.UTF8

    os.remove(local_file)

  # Get hdfs path to manifest list that belongs to the sanpshot identified by
  # 'snapshot_counter'.
  def get_manifest_list_hdfs_path(self, tmp_path_prefix, db_name, table_name,
      snapshot_counter):
    local_path = '%s_%s.metadata.json' % (tmp_path_prefix, random.randint(0, 10000))
    hdfs_path = get_fs_path('/test-warehouse/%s.db/%s/metadata/%s*.metadata.json'
        % (db_name, table_name, snapshot_counter))
    check_call(['hadoop', 'fs', '-copyToLocal', hdfs_path, local_path])

    manifest_list_hdfs_path = None
    try:
      with open(local_path, 'r') as fp:
        metadata = json.load(fp)
        current_snapshot_id = metadata['current-snapshot-id']
        for snapshot in metadata['snapshots']:
          if snapshot['snapshot-id'] == current_snapshot_id:
            manifest_list_hdfs_path = snapshot['manifest-list']
            break
    finally:
      os.remove(local_path)
    return manifest_list_hdfs_path

  # Get list of hdfs paths to manifest files from the manifest list avro file.
  def get_manifest_hdfs_path_list(self, tmp_path_prefix, manifest_list_hdfs_path):
    local_path = '%s_%s.manifest_list.avro' % (tmp_path_prefix, random.randint(0, 10000))
    check_call(['hadoop', 'fs', '-copyToLocal', manifest_list_hdfs_path, local_path])

    manifest_hdfs_path_list = []
    reader = None
    try:
      with open(local_path, 'rb') as fp:
        reader = DataFileReader(fp, DatumReader())
        for manifest in reader:
          manifest_hdfs_path_list.append(manifest['manifest_path'])
    finally:
      if reader:
        reader.close()
      os.remove(local_path)
    return manifest_hdfs_path_list

  # Get 'data_file' structs from avro manifest files.
  def get_data_file_list(self, tmp_path_prefix, manifest_hdfs_path_list):
    datafiles = []
    for hdfs_path in manifest_hdfs_path_list:
      local_path = '%s_%s.manifest.avro' % (tmp_path_prefix, random.randint(0, 10000))
      check_call(['hadoop', 'fs', '-copyToLocal', hdfs_path, local_path])

      reader = None
      try:
        with open(local_path, 'rb') as fp:
          reader = DataFileReader(fp, DatumReader())
          datafiles.extend([rec['data_file'] for rec in reader])
      finally:
        if reader:
          reader.close()
        os.remove(local_path)
    return datafiles

  @SkipIf.not_hdfs
  def test_writing_metrics_to_metadata(self, vector, unique_database):
    # Create table
    table_name = "ice_stats"
    qualified_table_name = "%s.%s" % (unique_database, table_name)
    query = 'create table %s ' \
        '(s string, i int, b boolean, bi bigint, ts timestamp, dt date, ' \
        'dc decimal(10, 3)) ' \
        'stored as iceberg' \
        % qualified_table_name
    self.client.execute(query)

    # Insert data
    # 1st data file:
    query = 'insert into %s values ' \
        '("abc", 3, true, NULL, "1970-01-03 09:11:22", NULL, 56.34), ' \
        '("def", NULL, false, NULL, "1969-12-29 14:45:59", DATE"1969-01-01", -10.0), ' \
        '("ghij", 1, NULL, 123456789000000, "1970-01-01", DATE"1970-12-31", NULL), ' \
        '(NULL, 0, NULL, 234567890000001, NULL, DATE"1971-01-01", NULL)' \
        % qualified_table_name
    self.execute_query(query)
    # 2nd data file:
    query = 'insert into %s values ' \
        '(NULL, NULL, NULL, NULL, NULL, NULL, NULL), ' \
        '(NULL, NULL, NULL, NULL, NULL, NULL, NULL)' \
        % qualified_table_name
    self.execute_query(query)

    # Get hdfs path to manifest list file
    manifest_list_hdfs_path = self.get_manifest_list_hdfs_path(
        '/tmp/iceberg_metrics_test', unique_database, table_name, '00002')

    # Get the list of hdfs paths to manifest files
    assert manifest_list_hdfs_path is not None
    manifest_hdfs_path_list = self.get_manifest_hdfs_path_list(
        '/tmp/iceberg_metrics_test', manifest_list_hdfs_path)

    # Get 'data_file' records from manifest files.
    assert manifest_hdfs_path_list is not None and len(manifest_hdfs_path_list) > 0
    datafiles = self.get_data_file_list('/tmp/iceberg_metrics_test',
        manifest_hdfs_path_list)

    # Check column stats in datafiles
    assert datafiles is not None and len(datafiles) == 2

    # The 1st datafile contains the 2 NULL rows
    assert datafiles[0]['record_count'] == 2
    assert datafiles[0]['column_sizes'] == \
        [{'key': 1, 'value': 39},
         {'key': 2, 'value': 39},
         {'key': 3, 'value': 25},
         {'key': 4, 'value': 39},
         {'key': 5, 'value': 39},
         {'key': 6, 'value': 39},
         {'key': 7, 'value': 39}]
    assert datafiles[0]['null_value_counts'] == \
        [{'key': 1, 'value': 2},
         {'key': 2, 'value': 2},
         {'key': 3, 'value': 2},
         {'key': 4, 'value': 2},
         {'key': 5, 'value': 2},
         {'key': 6, 'value': 2},
         {'key': 7, 'value': 2}]
    # Upper/lower bounds should be empty lists
    assert datafiles[0]['lower_bounds'] == []
    assert datafiles[0]['upper_bounds'] == []

    # 2nd datafile
    assert datafiles[1]['record_count'] == 4
    assert datafiles[1]['column_sizes'] == \
        [{'key': 1, 'value': 66},
         {'key': 2, 'value': 56},
         {'key': 3, 'value': 26},
         {'key': 4, 'value': 59},
         {'key': 5, 'value': 68},
         {'key': 6, 'value': 56},
         {'key': 7, 'value': 53}]
    assert datafiles[1]['null_value_counts'] == \
        [{'key': 1, 'value': 1},
         {'key': 2, 'value': 1},
         {'key': 3, 'value': 2},
         {'key': 4, 'value': 2},
         {'key': 5, 'value': 1},
         {'key': 6, 'value': 1},
         {'key': 7, 'value': 2}]
    assert datafiles[1]['lower_bounds'] == \
        [{'key': 1, 'value': 'abc'},
         # INT is serialized as 4-byte little endian
         {'key': 2, 'value': '\x00\x00\x00\x00'},
         # BOOLEAN is serialized as 0x00 for FALSE
         {'key': 3, 'value': '\x00'},
         # BIGINT is serialized as 8-byte little endian
         {'key': 4, 'value': '\x40\xaf\x0d\x86\x48\x70\x00\x00'},
         # TIMESTAMP is serialized as 8-byte little endian (number of microseconds since
         # 1970-01-01 00:00:00)
         {'key': 5, 'value': '\xc0\xd7\xff\x06\xd0\xff\xff\xff'},
         # DATE is serialized as 4-byte little endian (number of days since 1970-01-01)
         {'key': 6, 'value': '\x93\xfe\xff\xff'},
         # Unlike other numerical values, DECIMAL is serialized as big-endian.
         {'key': 7, 'value': '\xd8\xf0'}]
    assert datafiles[1]['upper_bounds'] == \
        [{'key': 1, 'value': 'ghij'},
         # INT is serialized as 4-byte little endian
         {'key': 2, 'value': '\x03\x00\x00\x00'},
         # BOOLEAN is serialized as 0x01 for TRUE
         {'key': 3, 'value': '\x01'},
         # BIGINT is serialized as 8-byte little endian
         {'key': 4, 'value': '\x81\x58\xc2\x97\x56\xd5\x00\x00'},
         # TIMESTAMP is serialized as 8-byte little endian (number of microseconds since
         # 1970-01-01 00:00:00)
         {'key': 5, 'value': '\x80\x02\x86\xef\x2f\x00\x00\x00'},
         # DATE is serialized as 4-byte little endian (number of days since 1970-01-01)
         {'key': 6, 'value': '\x6d\x01\x00\x00'},
         # Unlike other numerical values, DECIMAL is serialized as big-endian.
         {'key': 7, 'value': '\x00\xdc\x14'}]

  def test_using_upper_lower_bound_metrics(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-upper-lower-bound-metrics', vector,
        use_db=unique_database)

  def test_writing_many_files(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-write-many-files', vector,
        use_db=unique_database)

  @pytest.mark.execute_serially
  def test_writing_many_files_stress(self, vector, unique_database):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    self.run_test_case('QueryTest/iceberg-write-many-files-stress', vector,
        use_db=unique_database)

  def test_consistent_scheduling(self, vector, unique_database):
    """IMPALA-10914: This test verifies that Impala schedules scan ranges consistently for
    Iceberg tables."""
    def collect_split_stats(profile):
      splits = [l.strip() for l in profile.splitlines() if "Hdfs split stats" in l]
      splits.sort()
      return splits

    with self.create_impala_client() as impalad_client:
      impalad_client.execute("use " + unique_database)
      impalad_client.execute("""create table line_ice stored as iceberg
                                as select * from tpch_parquet.lineitem""")
      first_result = impalad_client.execute("""select count(*) from line_ice""")
      ref_profile = first_result.runtime_profile
      ref_split_stats = collect_split_stats(ref_profile)

      for i in range(0, 10):
        # Subsequent executions of the same query should schedule scan ranges similarly.
        result = impalad_client.execute("""select count(*) from line_ice""")
        profile = result.runtime_profile
        split_stats = collect_split_stats(profile)
        assert ref_split_stats == split_stats
