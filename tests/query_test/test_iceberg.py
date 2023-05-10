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

from __future__ import absolute_import, division, print_function
from builtins import range
import datetime
import logging
import os
import pytest
import pytz
import random

import re
import time

from subprocess import check_call
from parquet.ttypes import ConvertedType

from avro.datafile import DataFileReader
from avro.io import DatumReader
import json

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.iceberg_test_suite import IcebergTestSuite
from tests.common.skip import SkipIf, SkipIfDockerizedCluster
from tests.common.file_utils import (
  create_iceberg_table_from_directory,
  create_table_from_parquet)
from tests.shell.util import run_impala_shell_cmd
from tests.util.filesystem_utils import get_fs_path, IS_HDFS
from tests.util.get_parquet_metadata import get_parquet_metadata
from tests.util.iceberg_util import cast_ts, quote, get_snapshots, IcebergCatalogs

LOG = logging.getLogger(__name__)


class TestIcebergTable(IcebergTestSuite):
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

  def test_expire_snapshots(self, unique_database):
    tbl_name = unique_database + ".expire_snapshots"
    iceberg_catalogs = IcebergCatalogs(unique_database)
    for catalog_properties in iceberg_catalogs.get_iceberg_catalog_properties():
      # We are setting the TIMEZONE query option in this test, so let's create a local
      # impala client.
      with self.create_impala_client() as impalad_client:
        # Iceberg doesn't create a snapshot entry for the initial empty table
        impalad_client.execute("""
          create table {0} (i int) stored as iceberg
          TBLPROPERTIES ({1})""".format(tbl_name, catalog_properties))
        ts_0 = datetime.datetime.now()
        insert_q = "insert into {0} values (1)".format(tbl_name)
        ts_1 = self.execute_query_ts(impalad_client, insert_q)
        time.sleep(5)
        impalad_client.execute(insert_q)
        time.sleep(5)
        ts_2 = self.execute_query_ts(impalad_client, insert_q)
        impalad_client.execute(insert_q)

        # There should be 4 snapshots initially
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_0, 4)
        # Expire the oldest snapshot and test that the oldest one was expired
        expire_q = "alter table {0} execute expire_snapshots({1})"
        impalad_client.execute(expire_q.format(tbl_name, cast_ts(ts_1)))
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_0, 3)
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_1, 3)

        # Expire with a timestamp in which the interval does not touch existing snapshot
        impalad_client.execute(expire_q.format(tbl_name, cast_ts(ts_1)))
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_0, 3)

        # Expire all, but retain 1
        impalad_client.execute(expire_q.format(tbl_name,
            cast_ts(datetime.datetime.now())))
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_2, 1)

        # Change number of retained snapshots, then expire all
        impalad_client.execute("""alter table {0} set tblproperties
            ('history.expire.min-snapshots-to-keep' = '2')""".format(tbl_name))
        impalad_client.execute(insert_q)
        impalad_client.execute(insert_q)
        impalad_client.execute(expire_q.format(tbl_name,
            cast_ts(datetime.datetime.now())))
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_0, 2)

        # Check that timezone is interpreted in local timezone controlled by query option
        # TIMEZONE.
        impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
        impalad_client.execute(insert_q)
        ts_tokyo = self.impala_now(impalad_client)
        impalad_client.execute("SET TIMEZONE='Europe/Budapest'")
        impalad_client.execute(insert_q)
        impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
        impalad_client.execute(expire_q.format(tbl_name, cast_ts(ts_tokyo)))
        self.expect_num_snapshots_from(impalad_client, tbl_name, ts_tokyo, 1)
        impalad_client.execute("DROP TABLE {0}".format(tbl_name))

  def test_truncate_iceberg_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-truncate', vector, use_db=unique_database)

  @SkipIf.not_dfs
  def test_drop_incomplete_table(self, vector, unique_database):
    """Test DROP TABLE when the underlying directory is deleted. In that case table
    loading fails, but we should be still able to drop the table from Impala."""
    tbl_name = unique_database + ".synchronized_iceberg_tbl"
    cat_location = get_fs_path("/test-warehouse/" + unique_database)
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.catalog',
                      'iceberg.catalog_location'='{1}')""".format(tbl_name, cat_location))
    self.filesystem_client.delete_file_dir(cat_location, True)
    self.execute_query_expect_success(self.client, """drop table {0}""".format(tbl_name))

  @SkipIf.not_dfs(reason="Dfs required as test to directly delete files.")
  def test_drop_corrupt_table(self, unique_database):
    self._do_test_drop_corrupt_table(unique_database, do_invalidate=False)

  @SkipIf.not_dfs(reason="Dfs required as test to directly delete files.")
  def test_drop_corrupt_table_with_invalidate(self, unique_database):
    self._do_test_drop_corrupt_table(unique_database, do_invalidate=True)

  def _do_test_drop_corrupt_table(self, unique_database, do_invalidate):
    """Test that if the underlying iceberg metadata directory is deleted, then a query
      fails with a reasonable error message, and the table can be dropped successfully."""
    table = "corrupt_iceberg_tbl"
    full_table_name = unique_database + "." + table
    self.client.execute("""create table {0} (i int) stored as iceberg""".
                        format(full_table_name))
    metadata_location = get_fs_path("""/test-warehouse/{0}.db/{1}/metadata""".format(
      unique_database, table))
    assert self.filesystem_client.exists(metadata_location)
    status = self.filesystem_client.delete_file_dir(metadata_location, True)
    assert status, "Delete failed with {0}".format(status)
    assert not self.filesystem_client.exists(metadata_location)

    if do_invalidate:
      # Invalidate so that table loading problems will happen in the catalog.
      self.client.execute("invalidate metadata {0}".format(full_table_name))

    # Query should now fail.
    err = self.execute_query_expect_failure(self.client, """select * from {0}""".
                                            format(full_table_name))
    result = str(err)
    assert "AnalysisException: Failed to load metadata for table" in result
    assert ("Failed to load metadata for table" in result  # local catalog
            or "Error loading metadata for Iceberg table" in result)  # default catalog
    self.execute_query_expect_success(self.client, """drop table {0}""".
                                      format(full_table_name))

  def test_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-insert', vector, use_db=unique_database)

  def test_partitioned_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partitioned-insert', vector,
        use_db=unique_database)

  def test_insert_overwrite(self, vector, unique_database):
    """Run iceberg-overwrite tests, then test that INSERT INTO/OVERWRITE queries running
    concurrently with a long running INSERT OVERWRITE are handled gracefully. query_a is
    started before query_b/query_c, but query_b/query_c are supposed to finish before
    query_a. query_a should fail because the overwrite should not erase query_b/query_c's
    result."""
    # Run iceberg-overwrite.test
    self.run_test_case('QueryTest/iceberg-overwrite', vector, use_db=unique_database)

    # Create test dataset for concurrency tests and warm-up the test table
    tbl_name = unique_database + ".overwrite_tbl"
    self.client.execute("""create table {0} (i int)
        partitioned by spec (truncate(3, i))
        stored as iceberg""".format(tbl_name))
    self.client.execute("insert into {0} values (1), (2), (3);".format(tbl_name))

    # Test queries: 'a' is the long running query while 'b' and 'c' are the short ones
    query_a = """insert overwrite {0} select sleep(5000);""".format(tbl_name)
    query_b = """insert overwrite {0} select * from {0};""".format(tbl_name)
    query_c = """insert into {0} select * from {0};""".format(tbl_name)

    # Test concurrent INSERT OVERWRITEs, the exception closes the query handle.
    handle = self.client.execute_async(query_a)
    time.sleep(1)
    self.client.execute(query_b)
    try:
      self.client.wait_for_finished_timeout(handle, 30)
      assert False
    except ImpalaBeeswaxException as e:
      assert "Found conflicting files" in str(e)

    # Test INSERT INTO during INSERT OVERWRITE, the exception closes the query handle.
    handle = self.client.execute_async(query_a)
    time.sleep(1)
    self.client.execute(query_c)
    try:
      self.client.wait_for_finished_timeout(handle, 30)
      assert False
    except ImpalaBeeswaxException as e:
      assert "Found conflicting files" in str(e)

  def test_ctas(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-ctas', vector, use_db=unique_database)

  def test_partition_transform_insert(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-partition-transform-insert', vector,
        use_db=unique_database)

  def test_iceberg_orc_field_id(self, vector):
    self.run_test_case('QueryTest/iceberg-orc-field-id', vector)

  def test_catalogs(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-catalogs', vector, use_db=unique_database)

  def test_missing_field_ids(self, vector):
    self.run_test_case('QueryTest/iceberg-missing-field-ids', vector)

  def test_migrated_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-migrated-tables', vector, unique_database)

  def test_migrated_table_field_id_resolution(self, vector, unique_database):
    create_iceberg_table_from_directory(self.client, unique_database,
                                        "iceberg_migrated_alter_test", "parquet")
    create_iceberg_table_from_directory(self.client, unique_database,
                                        "iceberg_migrated_complex_test", "parquet")
    create_iceberg_table_from_directory(self.client, unique_database,
                                        "iceberg_migrated_alter_test_orc", "orc")
    create_iceberg_table_from_directory(self.client, unique_database,
                                        "iceberg_migrated_complex_test_orc", "orc")
    self.run_test_case('QueryTest/iceberg-migrated-table-field-id-resolution',
                       vector, unique_database)

  def test_describe_history(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-table-history', vector, use_db=unique_database)

    # Create a table with multiple snapshots and verify the table history.
    tbl_name = unique_database + ".iceberg_multi_snapshots"
    self.client.execute("""create table {0} (i int) stored as iceberg
        tblproperties('iceberg.catalog'='hadoop.tables')""".format(tbl_name))
    self.client.execute("INSERT INTO {0} VALUES (1)".format(tbl_name))
    self.client.execute("INSERT INTO {0} VALUES (2)".format(tbl_name))
    snapshots = get_snapshots(self.client, tbl_name, expected_result_size=2)
    first_snapshot = snapshots[0]
    second_snapshot = snapshots[1]
    # Check that first snapshot is older than the second snapshot.
    assert(first_snapshot.get_creation_time() < second_snapshot.get_creation_time())
    # Check that second snapshot's parent ID is the snapshot ID of the first snapshot.
    assert(first_snapshot.get_snapshot_id() == second_snapshot.get_parent_id())
    # The first snapshot has no parent snapshot ID.
    assert(first_snapshot.get_parent_id() is None)
    # Check "is_current_ancestor" column.
    assert(first_snapshot.is_current_ancestor())
    assert(second_snapshot.is_current_ancestor())

  def test_execute_rollback_negative(self, vector):
    """Negative test for EXECUTE ROLLBACK."""
    self.run_test_case('QueryTest/iceberg-rollback-negative', vector)

  def test_execute_rollback(self, unique_database):
    """Test for EXECUTE ROLLBACK."""
    iceberg_catalogs = IcebergCatalogs(unique_database)
    for catalog_properties in iceberg_catalogs.get_iceberg_catalog_properties():
      # Create a table with multiple snapshots.
      tbl_name = unique_database + ".iceberg_execute_rollback"
      # We are setting the TIMEZONE query option in this test, so let's create a local
      # impala client.
      with self.create_impala_client() as impalad_client:
        orig_timezone = 'America/Los_Angeles'
        impalad_client.execute("SET TIMEZONE='" + orig_timezone + "'")
        impalad_client.execute("""
          create table {0} (i int) stored as iceberg
          TBLPROPERTIES ({1})""".format(tbl_name, catalog_properties))
        initial_snapshots = 3
        for i in range(initial_snapshots):
          impalad_client.execute("INSERT INTO {0} VALUES ({1})".format(tbl_name, i))
        snapshots = get_snapshots(impalad_client, tbl_name,
          expected_result_size=initial_snapshots)

        output = self.rollback_to_id(tbl_name, snapshots[1].get_snapshot_id())
        LOG.info("success output={0}".format(output))

        # We rolled back, but that creates a new snapshot, so now there are 4.
        snapshots = get_snapshots(impalad_client, tbl_name, expected_result_size=4)
        # The new snapshot has the same id (and parent id) as the snapshot we rolled back
        # to, but it has a different creation time.
        assert snapshots[1].get_snapshot_id() == snapshots[3].get_snapshot_id()
        assert snapshots[1].get_parent_id() == snapshots[3].get_parent_id()
        assert snapshots[1].get_creation_time() < snapshots[3].get_creation_time()
        # The "orphaned" snapshot is now not a current ancestor.
        assert not snapshots[2].is_current_ancestor()

        # We cannot roll back to a snapshot that is not a current ancestor.
        output = self.rollback_to_id_expect_failure(tbl_name,
            snapshots[2].get_snapshot_id(),
            expected_text="Cannot roll back to snapshot, not an ancestor of the current "
                          "state")

        # Create another snapshot.
        before_insert = datetime.datetime.now(pytz.timezone(orig_timezone))
        impalad_client.execute("INSERT INTO {0} VALUES ({1})".format(tbl_name, 4))
        snapshots = get_snapshots(impalad_client, tbl_name, expected_result_size=5)

        # Rollback to before the last insert.
        self.rollback_to_ts(impalad_client, tbl_name, before_insert)
        # This creates another snapshot.
        snapshots = get_snapshots(impalad_client, tbl_name, expected_result_size=6)
        # The snapshot id is the same, the dates differ
        assert snapshots[3].get_snapshot_id() == snapshots[5].get_snapshot_id()
        assert snapshots[3].get_creation_time() < snapshots[5].get_creation_time()
        assert not snapshots[4].is_current_ancestor()

        # Show that the EXECUTE ROLLBACK is respecting the current timezone.
        # To do this we try to roll back to a time for which there is no
        # snapshot, this will fail with an error message that includes the specified
        # time. We parse out that time. By doing this in two timezones we can see
        # that the parameter being used was affected by the current timezone.
        one_hour_ago = before_insert - datetime.timedelta(hours=1)
        # We use Timezones from Japan and Iceland to avoid any DST complexities.
        impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
        japan_ts = self.get_snapshot_ts_from_failed_rollback(
            impalad_client, tbl_name, one_hour_ago)
        impalad_client.execute("SET TIMEZONE='Iceland'")
        iceland_ts = self.get_snapshot_ts_from_failed_rollback(
            impalad_client, tbl_name, one_hour_ago)
        diff_hours = (iceland_ts - japan_ts) / (1000 * 60 * 60)
        assert diff_hours == 9

        impalad_client.execute("DROP TABLE {0}".format(tbl_name))

  def get_snapshot_ts_from_failed_rollback(self, client, tbl_name, ts):
    """Run an EXECUTE ROLLBACK which is expected to fail.
    Parse the error message to extract the timestamp for which there
    was no snapshot, and convert the string to an integer"""
    try:
      self.rollback_to_ts(client, tbl_name, ts)
      assert False, "Query should have failed"
    except ImpalaBeeswaxException as e:
      result = re.search(r".*no valid snapshot older than: (\d+)", str(e))
      time_str = result.group(1)
      snapshot_ts = int(time_str)
      assert snapshot_ts > 0, "did not decode snapshot ts from {0}".format(result)
      return snapshot_ts

  def rollback_to_ts(self, client, tbl_name, ts):
    """Rollback a table to a snapshot timestamp."""
    query = "ALTER TABLE {0} EXECUTE ROLLBACK ('{1}');".format(tbl_name, ts.isoformat())
    return self.execute_query_expect_success(client, query)

  def rollback_to_id(self, tbl_name, id):
    """Rollback a table to a snapshot id."""
    query = "ALTER TABLE {0} EXECUTE ROLLBACK ({1});".format(tbl_name, id)
    return self.execute_query_expect_success(self.client, query)

  def rollback_to_id_expect_failure(self, tbl_name, id, expected_text=None):
    """Attempt to roll back a table to a snapshot id, expecting a failure."""
    query = "ALTER TABLE {0} EXECUTE ROLLBACK ({1});".format(tbl_name, id)
    output = self.execute_query_expect_failure(self.client, query)
    if expected_text:
      assert expected_text in str(output)
    return output

  def test_describe_history_params(self, unique_database):
    tbl_name = unique_database + ".describe_history"

    # We are setting the TIMEZONE query option in this test, so let's create a local
    # impala client.
    with self.create_impala_client() as impalad_client:
      # Iceberg doesn't create a snapshot entry for the initial empty table
      impalad_client.execute("create table {0} (i int) stored as iceberg"
          .format(tbl_name))
      insert_q = "insert into {0} values (1)".format(tbl_name)
      ts_1 = self.execute_query_ts(impalad_client, insert_q)
      time.sleep(5)
      ts_2 = self.execute_query_ts(impalad_client, insert_q)
      time.sleep(5)
      ts_3 = self.execute_query_ts(impalad_client, insert_q)
      # Describe history without predicate
      data = impalad_client.execute("DESCRIBE HISTORY {0}".format(tbl_name))
      assert len(data.data) == 3

      # Describe history with FROM predicate
      self.expect_num_snapshots_from(impalad_client, tbl_name,
          ts_1 - datetime.timedelta(hours=1), 3)
      self.expect_num_snapshots_from(impalad_client, tbl_name, ts_1, 2)
      self.expect_num_snapshots_from(impalad_client, tbl_name, ts_3, 0)

      # Describe history with BETWEEN <ts> AND <ts> predicate
      self.expect_results_between(impalad_client, tbl_name, ts_1, ts_2, 1)
      self.expect_results_between(impalad_client, tbl_name,
          ts_1 - datetime.timedelta(hours=1), ts_2, 2)
      self.expect_results_between(impalad_client, tbl_name,
          ts_1 - datetime.timedelta(hours=1), ts_2 + datetime.timedelta(hours=1), 3)

      # Check that timezone is interpreted in local timezone controlled by query option
      # TIMEZONE. Persist the local times first and create a new snapshot.
      impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
      now_tokyo = self.impala_now(impalad_client)
      impalad_client.execute("SET TIMEZONE='Europe/Budapest'")
      now_budapest = self.impala_now(impalad_client)
      self.execute_query_ts(impalad_client, "insert into {0} values (4)".format(tbl_name))
      self.expect_num_snapshots_from(impalad_client, tbl_name, now_budapest, 1)

      # Let's switch to Tokyo time. Tokyo time is always greater than Budapest time.
      impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
      self.expect_num_snapshots_from(impalad_client, tbl_name, now_tokyo, 1)

      # Interpreting Budapest time in Tokyo time points to the past.
      self.expect_num_snapshots_from(impalad_client, tbl_name, now_budapest, 4)

  def test_time_travel(self, unique_database):
    tbl_name = unique_database + ".time_travel"

    def expect_results(query, expected_results, expected_cols):
      data = impalad_client.execute(query)
      assert len(data.data) == len(expected_results)
      for r in expected_results:
        assert r in data.data
      expected_col_labels = expected_cols['labels']
      expected_col_types = expected_cols['types']
      assert data.column_labels == expected_col_labels
      assert data.column_types == expected_col_types

    def expect_for_count_star(query, expected):
      data = impalad_client.execute(query)
      assert len(data.data) == 1
      assert expected in data.data
      assert "NumRowGroups" not in data.runtime_profile

    def expect_results_t(ts, expected_results, expected_cols):
      expect_results(
          "select * from {0} for system_time as of {1}".format(tbl_name, ts),
          expected_results, expected_cols)

    def expect_for_count_star_t(ts, expected):
      expect_for_count_star(
          "select count(*) from {0} for system_time as of {1}".format(tbl_name, ts),
          expected)

    def expect_results_v(snapshot_id, expected_results, expected_cols):
      expect_results(
          "select * from {0} for system_version as of {1}".format(tbl_name, snapshot_id),
          expected_results, expected_cols)

    def expect_for_count_star_v(snapshot_id, expected):
      expect_for_count_star(
          "select count(*) from {0} for system_version as of {1}".format(
              tbl_name, snapshot_id),
          expected)

    def impala_now():
      now_data = impalad_client.execute("select now()")
      return now_data.data[0]

    # We are setting the TIMEZONE query option in this test, so let's create a local
    # impala client.
    with self.create_impala_client() as impalad_client:
      # Iceberg doesn't create a snapshot entry for the initial empty table
      impalad_client.execute("create table {0} (i int) stored as iceberg"
          .format(tbl_name))
      ts_1 = self.execute_query_ts(impalad_client, "insert into {0} values (1)"
          .format(tbl_name))
      ts_2 = self.execute_query_ts(impalad_client, "insert into {0} values (2)"
          .format(tbl_name))
      ts_3 = self.execute_query_ts(impalad_client, "truncate table {0}".format(tbl_name))
      time.sleep(5)
      ts_4 = self.execute_query_ts(impalad_client, "insert into {0} values (100)"
          .format(tbl_name))
      ts_no_ss = self.execute_query_ts(impalad_client,
          "alter table {0} add column {1} bigint"
          .format(tbl_name, "j"))
      ts_5 = self.execute_query_ts(impalad_client, "insert into {0} (i,j) values (3, 103)"
                                   .format(tbl_name))

      # Descriptions of the different schemas we expect to see as Time Travel queries
      # use the schema from the specified time or snapshot.
      #
      # When the schema is just the 'J' column.
      j_cols = {
        'labels': ['J'],
        'types': ['BIGINT']
      }
      # When the schema is just the 'I' column.
      i_cols = {
        'labels': ['I'],
        'types': ['INT']
      }
      # When the schema is the 'I' and 'J' columns.
      ij_cols = {
        'labels': ['I', 'J'],
        'types': ['INT', 'BIGINT']
      }

      # Query table as of timestamps.
      expect_results_t("now()", ['100\tNULL', '3\t103'], ij_cols)
      expect_results_t(quote(ts_1), ['1'], i_cols)
      expect_results_t(quote(ts_2), ['1', '2'], i_cols)
      expect_results_t(quote(ts_3), [], i_cols)
      expect_results_t(cast_ts(ts_3) + " + interval 1 seconds", [], i_cols)
      expect_results_t(quote(ts_4), ['100'], i_cols)
      expect_results_t(cast_ts(ts_4) + " - interval 5 seconds", [], i_cols)
      # There is no new snapshot created by the schema change between ts_4 and ts_no_ss.
      # So at ts_no_ss we see the schema as of ts_4
      expect_results_t(quote(ts_no_ss), ['100'], i_cols)
      expect_results_t(quote(ts_5), ['100\tNULL', '3\t103'], ij_cols)
      # Future queries return the current snapshot.
      expect_results_t(cast_ts(ts_5) + " + interval 1 hours", ['100\tNULL', '3\t103'],
                       ij_cols)

      # Query table as of snapshot IDs.
      snapshots = get_snapshots(impalad_client, tbl_name, expected_result_size=5)
      expect_results_v(snapshots[0].get_snapshot_id(), ['1'], i_cols)
      expect_results_v(snapshots[1].get_snapshot_id(), ['1', '2'], i_cols)
      expect_results_v(snapshots[2].get_snapshot_id(), [], i_cols)
      expect_results_v(snapshots[3].get_snapshot_id(), ['100'], i_cols)
      expect_results_v(snapshots[4].get_snapshot_id(), ['100\tNULL', '3\t103'], ij_cols)

      # Test of plain count star optimization
      # 'NumRowGroups' and 'NumFileMetadataRead' should not appear in profile
      expect_for_count_star_t("now()", '2')
      expect_for_count_star_t(quote(ts_1), '1')
      expect_for_count_star_t(quote(ts_2), '2')
      expect_for_count_star_t(quote(ts_3), '0')
      expect_for_count_star_t(cast_ts(ts_3) + " + interval 1 seconds", '0')
      expect_for_count_star_t(quote(ts_4), '1')
      expect_for_count_star_t(cast_ts(ts_4) + " - interval 5 seconds", '0')
      expect_for_count_star_t(cast_ts(ts_5), '2')
      expect_for_count_star_t(cast_ts(ts_5) + " + interval 1 hours", '2')
      expect_for_count_star_v(snapshots[0].get_snapshot_id(), '1')
      expect_for_count_star_v(snapshots[1].get_snapshot_id(), '2')
      expect_for_count_star_v(snapshots[2].get_snapshot_id(), '0')
      expect_for_count_star_v(snapshots[3].get_snapshot_id(), '1')
      expect_for_count_star_v(snapshots[4].get_snapshot_id(), '2')

      # SELECT diff
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_new}'
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_old}'""".format(
                     tbl=tbl_name, ts_new=ts_2, ts_old=ts_1),
                     ['2'], i_cols)
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_new}
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_old}""".format(
                     tbl=tbl_name, v_new=snapshots[1].get_snapshot_id(),
                     v_old=snapshots[0].get_snapshot_id()),
                     ['2'], i_cols)
      # Mix SYSTEM_TIME and SYSTEM_VERSION
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_new}
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_old}'""".format(
                     tbl=tbl_name, v_new=snapshots[1].get_snapshot_id(), ts_old=ts_1),
                     ['2'], i_cols)
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_new}'
                        MINUS
                        SELECT * FROM {tbl} FOR SYSTEM_VERSION AS OF {v_old}""".format(
                     tbl=tbl_name, ts_new=ts_2, v_old=snapshots[0].get_snapshot_id()),
                     ['2'], i_cols)
      expect_results("""SELECT * FROM {tbl} FOR SYSTEM_TIME AS OF '{ts_new}'
                        MINUS
                        SELECT *, NULL FROM {tbl} FOR SYSTEM_TIME
                        AS OF '{ts_old}'""".format(
                     tbl=tbl_name, ts_new=ts_5, ts_old=ts_4),
                     ['3\t103'], ij_cols)

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

      # Go back to one column
      impalad_client.execute("alter table {0} drop column i".format(tbl_name))

      # Test that deleted column is not selectable.
      try:
        impalad_client.execute("SELECT i FROM {0}".format(tbl_name))
        assert False  # Exception must be thrown
      except Exception as e:
        assert "Could not resolve column/field reference: 'i'" in str(e)

      # Back at ts_2 the deleted 'I' column is there
      expect_results("SELECT * FROM {0} FOR SYSTEM_TIME AS OF '{1}'".
                     format(tbl_name, ts_2), ['1', '2'], i_cols)
      expect_results("SELECT i FROM {0} FOR SYSTEM_TIME AS OF '{1}'".
                     format(tbl_name, ts_2), ['1', '2'], i_cols)

      # Check that timezone is interpreted in local timezone controlled by query option
      # TIMEZONE
      impalad_client.execute("truncate table {0}".format(tbl_name))
      impalad_client.execute("insert into {0} values (1111)".format(tbl_name))
      impalad_client.execute("SET TIMEZONE='Europe/Budapest'")
      now_budapest = impala_now()
      expect_results_t(quote(now_budapest), ['1111'], j_cols)

      # Let's switch to Tokyo time. Tokyo time is always greater than Budapest time.
      impalad_client.execute("SET TIMEZONE='Asia/Tokyo'")
      now_tokyo = impala_now()
      expect_results_t(quote(now_tokyo), ['1111'], j_cols)
      try:
        # Interpreting Budapest time in Tokyo time points to the past when the table
        # didn't exist.
        expect_results_t(quote(now_budapest), [], j_cols)
        assert False
      except Exception as e:
        assert "Cannot find a snapshot older than" in str(e)

  @SkipIf.not_dfs
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

  @SkipIf.not_dfs
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
    assert datafiles[0]['value_counts'] == \
        [{'key': 1, 'value': 2},
         {'key': 2, 'value': 2},
         {'key': 3, 'value': 2},
         {'key': 4, 'value': 2},
         {'key': 5, 'value': 2},
         {'key': 6, 'value': 2},
         {'key': 7, 'value': 2}]
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
    assert datafiles[1]['value_counts'] == \
        [{'key': 1, 'value': 4},
         {'key': 2, 'value': 4},
         {'key': 3, 'value': 4},
         {'key': 4, 'value': 4},
         {'key': 5, 'value': 4},
         {'key': 6, 'value': 4},
         {'key': 7, 'value': 4}]
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

  @pytest.mark.execute_serially
  def test_table_load_time_for_many_files(self, vector, unique_database):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    tbl_name = unique_database + ".iceberg_many_files"
    self.execute_query("""CREATE TABLE {}
        PARTITIONED BY SPEC (bucket(2039, l_orderkey))
        STORED AS ICEBERG
        AS SELECT * FROM tpch_parquet.lineitem""".format(tbl_name))
    self.execute_query("invalidate metadata")
    start_time = time.time()
    self.execute_query("describe formatted {}".format(tbl_name))
    elapsed_time = time.time() - start_time
    if IS_HDFS:
      time_limit = 10
    else:
      time_limit = 20
    assert elapsed_time < time_limit

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

  def test_in_predicate_push_down(self, vector, unique_database):
    self.execute_query("SET RUNTIME_FILTER_MODE=OFF")
    self.run_test_case('QueryTest/iceberg-in-predicate-push-down', vector,
                       use_db=unique_database)

  def test_is_null_predicate_push_down(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-is-null-predicate-push-down', vector,
                       use_db=unique_database)

  def test_compound_predicate_push_down(self, vector, unique_database):
      self.run_test_case('QueryTest/iceberg-compound-predicate-push-down', vector,
                         use_db=unique_database)

  def test_plain_count_star_optimization(self, vector, unique_database):
      self.run_test_case('QueryTest/iceberg-plain-count-star-optimization', vector,
                         use_db=unique_database)

  def test_create_table_like_table(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-create-table-like-table', vector,
                       use_db=unique_database)

  def test_table_owner(self, vector, unique_database):
    self.run_table_owner_test(vector, unique_database, "some_random_user")
    self.run_table_owner_test(vector, unique_database, "another_random_user")

  def run_table_owner_test(self, vector, db_name, user_name):
    # Create Iceberg table with a given user running the query.
    tbl_name = "iceberg_table_owner"
    sql_stmt = 'CREATE TABLE {0}.{1} (i int) STORED AS ICEBERG'.format(
      db_name, tbl_name)
    args = ['-u', user_name, '-q', sql_stmt]
    run_impala_shell_cmd(vector, args)

    # Run DESCRIBE FORMATTED to get the owner of the table.
    args = ['-q', 'DESCRIBE FORMATTED {0}.{1}'.format(db_name, tbl_name)]
    results = run_impala_shell_cmd(vector, args)
    result_rows = results.stdout.strip().split('\n')

    # Find the output row with the owner.
    owner_row = ""
    for row in result_rows:
      if "Owner:" in row:
        owner_row = row
    assert owner_row != "", "DESCRIBE output doesn't contain owner" + results.stdout
    # Verify that the user running the query is the owner of the table.
    assert user_name in owner_row, "Unexpected owner of Iceberg table. " + \
      "Expected user name: {0}. Actual output row: {1}".format(user_name, owner_row)

    args = ['-q', 'DROP TABLE {0}.{1}'.format(db_name, tbl_name)]
    results = run_impala_shell_cmd(vector, args)

  def test_multiple_storage_locations(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-multiple-storage-locations-table',
                       vector, unique_database)

  def test_mixed_file_format(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-mixed-file-format', vector,
                      unique_database)

  def test_load(self, vector, unique_database):
    """Test LOAD DATA INPATH for Iceberg tables, the first part of this method inits the
    target directory, copies existing test data to HDFS. The second part runs the test
    cases then cleans up the test directory.
    """
    # Test 1-6 init: target orc/parquet file and directory
    SRC_DIR = os.path.join(os.environ['IMPALA_HOME'],
        "testdata/data/iceberg_test/iceberg_mixed_file_format_test/data/{0}")
    DST_DIR = "/tmp/" + unique_database + "/parquet/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    file_parq1 = "00000-0-data-gfurnstahl_20220906113044_157fc172-f5d3-4c70-8653-" \
        "fff150b6136a-job_16619542960420_0002-1-00001.parquet"
    file_parq2 = "00000-0-data-gfurnstahl_20220906114830_907f72c7-36ac-4135-8315-" \
        "27ff880faff0-job_16619542960420_0004-1-00001.parquet"
    self.filesystem_client.copy_from_local(SRC_DIR.format(file_parq1), DST_DIR)
    self.filesystem_client.copy_from_local(SRC_DIR.format(file_parq2), DST_DIR)
    DST_DIR = "/tmp/" + unique_database + "/orc/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    file_orc1 = "00000-0-data-gfurnstahl_20220906113255_8d49367d-e338-4996-ade5-" \
        "ee500a19c1d1-job_16619542960420_0003-1-00001.orc"
    file_orc2 = "00000-0-data-gfurnstahl_20220906114900_9c1b7b46-5643-428f-a007-" \
        "519c5500ed04-job_16619542960420_0004-1-00001.orc"
    self.filesystem_client.copy_from_local(SRC_DIR.format(file_orc1), DST_DIR)
    self.filesystem_client.copy_from_local(SRC_DIR.format(file_orc2), DST_DIR)
    # Test 7 init: overwrite
    DST_DIR = "/tmp/" + unique_database + "/overwrite/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    self.filesystem_client.copy_from_local(SRC_DIR.format(file_parq1), DST_DIR)
    # Test 8 init: mismatching parquet schema format
    SRC_DIR = os.path.join(os.environ['IMPALA_HOME'], "testdata/data/iceberg_test/"
        "iceberg_partitioned/data/event_time_hour=2020-01-01-08/action=view/{0}")
    DST_DIR = "/tmp/" + unique_database + "/mismatching_schema/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    file = "00001-1-b975a171-0911-47c2-90c8-300f23c28772-00000.parquet"
    self.filesystem_client.copy_from_local(SRC_DIR.format(file), DST_DIR)
    # Test 9 init: partitioned
    DST_DIR = "/tmp/" + unique_database + "/partitioned/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    self.filesystem_client.copy_from_local(SRC_DIR.format(file), DST_DIR)
    # Test 10 init: hidden files
    DST_DIR = "/tmp/" + unique_database + "/hidden/"
    self.filesystem_client.make_dir(DST_DIR, permission=777)
    self.filesystem_client.create_file(DST_DIR + "_hidden.1", "Test data 123")
    self.filesystem_client.create_file(DST_DIR + "_hidden_2.1", "Test data 123")
    self.filesystem_client.create_file(DST_DIR + ".hidden_3", "Test data 123")
    self.filesystem_client.create_file(DST_DIR + ".hidden_4.1", "Test data 123")
    self.filesystem_client.copy_from_local(SRC_DIR.format(file), DST_DIR)

    # Init test table
    create_iceberg_table_from_directory(self.client, unique_database,
        "iceberg_mixed_file_format_test", "parquet")

    # Execute tests
    self.run_test_case('QueryTest/iceberg-load', vector, use_db=unique_database)
    # Clean up temporary directory
    self.filesystem_client.delete_file_dir("/tmp/{0}".format(unique_database), True)

  def test_table_sampling(self, vector):
    self.run_test_case('QueryTest/iceberg-tablesample', vector,
        use_db="functional_parquet")

  def _create_table_like_parquet_helper(self, vector, unique_database, tbl_name,
                                        expect_success):
    create_table_from_parquet(self.client, unique_database, tbl_name)
    args = ['-q', "show files in {0}.{1}".format(unique_database, tbl_name)]
    results = run_impala_shell_cmd(vector, args)
    result_rows = results.stdout.strip().split('\n')
    hdfs_file = None
    for row in result_rows:
      if "://" in row:
        hdfs_file = row.split('|')[1].lstrip()
        break
    assert hdfs_file

    iceberg_tbl_name = "iceberg_{0}".format(tbl_name)
    sql_stmt = "create table {0}.{1} like parquet '{2}' stored as iceberg".format(
      unique_database, iceberg_tbl_name, hdfs_file
    )
    args = ['-q', sql_stmt]

    return run_impala_shell_cmd(vector, args, expect_success=expect_success)

  def test_create_table_like_parquet(self, vector, unique_database):
    tbl_name = 'alltypes_tiny_pages'
    # Not all types are supported by iceberg
    self._create_table_like_parquet_helper(vector, unique_database, tbl_name, False)

    tbl_name = "create_table_like_parquet_test"
    results = self._create_table_like_parquet_helper(vector, unique_database, tbl_name,
                                                     True)
    result_rows = results.stdout.strip().split('\n')
    assert result_rows[3].split('|')[1] == ' Table has been created. '

    sql_stmt = "describe {0}.{1}".format(unique_database, tbl_name)
    args = ['-q', sql_stmt]
    parquet_results = run_impala_shell_cmd(vector, args)
    parquet_result_rows = parquet_results.stdout.strip().split('\n')

    parquet_column_name_type_list = []
    for row in parquet_result_rows[1:-2]:
      parquet_column_name_type_list.append(row.split('|')[1:3])

    sql_stmt = "describe {0}.iceberg_{1}".format(unique_database, tbl_name)
    args = ['-q', sql_stmt]
    iceberg_results = run_impala_shell_cmd(vector, args)
    iceberg_result_rows = iceberg_results.stdout.strip().split('\n')

    iceberg_column_name_type_list = []
    for row in iceberg_result_rows[1:-2]:
      iceberg_column_name_type_list.append(row.split('|')[1:3])

    assert parquet_column_name_type_list == iceberg_column_name_type_list

  def test_compute_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-compute-stats', vector, unique_database)

  def test_virtual_columns(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-virtual-columns', vector, unique_database)

  def test_avro_file_format(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-avro', vector, unique_database)


class TestIcebergV2Table(IcebergTestSuite):
  """Tests related to Iceberg V2 tables."""

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestIcebergV2Table, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  # The test uses pre-written Iceberg tables where the position delete files refer to
  # the data files via full URI, i.e. they start with 'hdfs://localhost:2050/...'. In the
  # dockerised environment the namenode is accessible on a different hostname/port.
  @SkipIfDockerizedCluster.internal_hostname
  @SkipIf.hardcoded_uris
  def test_plain_count_star_optimization(self, vector):
      self.run_test_case('QueryTest/iceberg-v2-plain-count-star-optimization',
                         vector)

  @SkipIfDockerizedCluster.internal_hostname
  @SkipIf.hardcoded_uris
  def test_read_position_deletes(self, vector):
    self.run_test_case('QueryTest/iceberg-v2-read-position-deletes', vector)

  @SkipIfDockerizedCluster.internal_hostname
  @SkipIf.hardcoded_uris
  def test_read_position_deletes_orc(self, vector):
    self.run_test_case('QueryTest/iceberg-v2-read-position-deletes-orc', vector)

  @SkipIfDockerizedCluster.internal_hostname
  @SkipIf.hardcoded_uris
  def test_table_sampling_v2(self, vector):
    self.run_test_case('QueryTest/iceberg-tablesample-v2', vector,
        use_db="functional_parquet")

  def test_metadata_tables(self, vector, unique_database):
    self.run_test_case('QueryTest/iceberg-metadata-tables', vector,
        use_db="functional_parquet")
