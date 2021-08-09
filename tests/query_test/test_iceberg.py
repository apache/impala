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
import random
import time

from subprocess import check_call
from parquet.ttypes import ConvertedType

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
      self.execute_query(query)
      return str(datetime.datetime.now())

    def expect_results(query, expected_results):
      data = self.execute_query(query)
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
      data = self.execute_query("describe history {0}".format(tbl_name))
      ret = list()
      for row in data.data:
        fields = row.split('\t')
        ret.append(fields[1])
      return ret

    def impala_now():
      now_data = self.execute_query("select now()")
      return now_data.data[0]

    # Iceberg doesn't create a snapshot entry for the initial empty table
    self.execute_query("create table {0} (i int) stored as iceberg".format(tbl_name))
    ts_1 = execute_query_ts("insert into {0} values (1)".format(tbl_name))
    ts_2 = execute_query_ts("insert into {0} values (2)".format(tbl_name))
    ts_3 = execute_query_ts("truncate table {0}".format(tbl_name))
    time.sleep(1)
    ts_4 = execute_query_ts("insert into {0} values (100)".format(tbl_name))
    # Query table as of timestamps.
    expect_results_t("now()", ['100'])
    expect_results_t(quote(ts_1), ['1'])
    expect_results_t(quote(ts_2), ['1', '2'])
    expect_results_t(quote(ts_3), [])
    expect_results_t(quote(ts_4), ['100'])
    expect_results_t(cast_ts(ts_4) + " - interval 1 seconds", [])
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
      self.execute_query("SELECT * FROM {0} FOR SYSTEM_TIME AS OF {1}".format(
          tbl_name, "now() - interval 2 years"))
      assert False  # Exception must be thrown
    except Exception as e:
      assert "Cannot find a snapshot older than" in str(e)
    # Query invalid snapshot
    try:
      self.execute_query("SELECT * FROM {0} FOR SYSTEM_VERSION AS OF 42".format(tbl_name))
      assert False  # Exception must be thrown
    except Exception as e:
      assert "Cannot find snapshot with ID 42" in str(e)

    # Check that timezone is interpreted in local timezone controlled by query option
    # TIMEZONE
    self.execute_query("truncate table {0}".format(tbl_name))
    self.execute_query("insert into {0} values (1111)".format(tbl_name))
    self.execute_query("SET TIMEZONE='Europe/Budapest'")
    now_budapest = impala_now()
    expect_results_t(quote(now_budapest), ['1111'])

    # Let's switch to Tokyo time. Tokyo time is always greater than Budapest time.
    self.execute_query("SET TIMEZONE='Asia/Tokyo'")
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
