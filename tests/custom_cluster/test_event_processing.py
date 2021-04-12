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
import logging
import random
import string
import pytest

from tests.common.skip import SkipIfHive2
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.skip import (SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon,
                               SkipIfGCS, SkipIfLocal)
from tests.util.hive_utils import HiveDbWrapper
from tests.util.event_processor_utils import EventProcessorUtils
from tests.util.filesystem_utils import WAREHOUSE


@SkipIfS3.hive
@SkipIfABFS.hive
@SkipIfADLS.hive
@SkipIfGCS.hive
@SkipIfIsilon.hive
@SkipIfLocal.hive
class TestEventProcessing(CustomClusterTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""
  CATALOG_URL = "http://localhost:25020"
  PROCESSING_TIMEOUT_S = 10

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--blacklisted_dbs=testBlackListedDb "
                 "--blacklisted_tables=functional_parquet.testBlackListedTbl",
    catalogd_args="--blacklisted_dbs=testBlackListedDb "
                  "--blacklisted_tables=functional_parquet.testBlackListedTbl "
                  "--hms_event_polling_interval_s=1")
  def test_events_on_blacklisted_objects(self):
    """Executes hive queries on blacklisted database and tables and makes sure that
    event processor does not error out
    """
    try:
      event_id_before = EventProcessorUtils.get_last_synced_event_id()
      # create a blacklisted database from hive and make sure event is ignored
      self.run_stmt_in_hive("create database TESTblackListedDb")
      # wait until all the events generated above are processed
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      assert EventProcessorUtils.get_last_synced_event_id() > event_id_before
      # make sure that the blacklisted db is ignored
      assert "TESTblackListedDb".lower() not in self.all_db_names()

      event_id_before = EventProcessorUtils.get_last_synced_event_id()
      self.run_stmt_in_hive("create table testBlackListedDb.testtbl (id int)")
      # create a table on the blacklisted database with a different case
      self.run_stmt_in_hive("create table TESTBLACKlISTEDDb.t2 (id int)")
      self.run_stmt_in_hive(
        "create table functional_parquet.testBlackListedTbl (id int, val string)"
        " partitioned by (part int) stored as parquet")
      self.run_stmt_in_hive(
        "alter table functional_parquet.testBlackListedTbl add partition (part=1)")
      # wait until all the events generated above are processed
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      assert EventProcessorUtils.get_last_synced_event_id() > event_id_before
      # make sure that the black listed table is not created
      table_names = self.client.execute("show tables in functional_parquet").get_data()
      assert "testBlackListedTbl".lower() not in table_names

      event_id_before = EventProcessorUtils.get_last_synced_event_id()
      # generate a table level event with a different case
      self.run_stmt_in_hive("drop table functional_parquet.TESTBlackListedTbl")
      # wait until all the events generated above are processed
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      assert EventProcessorUtils.get_last_synced_event_id() > event_id_before
    finally:
      self.run_stmt_in_hive("drop database testBlackListedDb cascade")
      self.run_stmt_in_hive("drop table functional_parquet.testBlackListedTbl")

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  @SkipIfHive2.acid
  def test_transactional_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    self.run_test_insert_events(unique_database, is_transactional=True)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    self.run_test_insert_events(unique_database)

  def run_test_insert_events(self, unique_database, is_transactional=False):
    """Test for insert event processing. Events are created in Hive and processed in
    Impala. The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    # Test table with no partitions.
    tbl_insert_nopart = 'tbl_insert_nopart'
    self.run_stmt_in_hive(
      "drop table if exists %s.%s" % (unique_database, tbl_insert_nopart))
    tblproperties = ""
    if is_transactional:
      tblproperties = "tblproperties ('transactional'='true'," \
          "'transactional_properties'='insert_only')"
    self.run_stmt_in_hive("create table %s.%s (id int, val int) %s"
        % (unique_database, tbl_insert_nopart, tblproperties))
    EventProcessorUtils.wait_for_event_processing(self)
    # Test CTAS and insert by Impala with empty results (IMPALA-10765).
    self.execute_query("create table {db}.ctas_tbl {prop} as select * from {db}.{tbl}"
        .format(db=unique_database, tbl=tbl_insert_nopart, prop=tblproperties))
    self.execute_query("insert into {db}.ctas_tbl select * from {db}.{tbl}"
        .format(db=unique_database, tbl=tbl_insert_nopart))
    # Test insert into table, this will fire an insert event.
    self.run_stmt_in_hive("insert into %s.%s values(101, 200)"
        % (unique_database, tbl_insert_nopart))
    # With MetastoreEventProcessor running, the insert event will be processed. Query the
    # table from Impala.
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" %
        (unique_database, tbl_insert_nopart))
    assert data.split('\t') == ['101', '200']

    # Test insert overwrite. Overwrite the existing value.
    self.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
        % (unique_database, tbl_insert_nopart))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" %
        (unique_database, tbl_insert_nopart))
    assert data.split('\t') == ['101', '201']
    # Test insert overwrite by Impala with empty results (IMPALA-10765).
    self.execute_query("insert overwrite {db}.{tbl} select * from {db}.ctas_tbl"
                       .format(db=unique_database, tbl=tbl_insert_nopart))
    result = self.execute_query("select * from {db}.{tbl}"
                                .format(db=unique_database, tbl=tbl_insert_nopart))
    assert len(result.data) == 0

    # Test partitioned table.
    tbl_insert_part = 'tbl_insert_part'
    self.run_stmt_in_hive("drop table if exists %s.%s"
        % (unique_database, tbl_insert_part))
    self.run_stmt_in_hive("create table %s.%s (id int, name string) "
        "partitioned by(day int, month int, year int) %s"
        % (unique_database, tbl_insert_part, tblproperties))
    EventProcessorUtils.wait_for_event_processing(self)
    # Test insert overwrite by Impala with empty results (IMPALA-10765).
    self.execute_query(
        "create table {db}.ctas_part partitioned by (day, month, year) {prop} as "
        "select * from {db}.{tbl}".format(db=unique_database, tbl=tbl_insert_part,
            prop=tblproperties))
    self.execute_query(
        "insert into {db}.ctas_part partition(day=0, month=0, year=0) select id, "
        "name from {db}.{tbl}".format(db=unique_database, tbl=tbl_insert_part))
    # Insert data into partitions.
    self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(101, 'x')" % (unique_database, tbl_insert_part))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['101', 'x', '28', '3', '2019']

    # Test inserting into existing partitions.
    self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(102, 'y')" % (unique_database, tbl_insert_part))
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select count(*) from %s.%s where day=28 and month=3 "
        "and year=2019" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['2']
    # Test inserting into existing partitions by Impala with empty results
    # (IMPALA-10765).
    self.execute_query("insert into {db}.{tbl} partition(day=28, month=03, year=2019) "
                       "select id, name from {db}.ctas_part"
                       .format(db=unique_database, tbl=tbl_insert_part))

    # Test insert overwrite into existing partitions
    self.run_stmt_in_hive("insert overwrite table %s.%s partition(day=28, month=03, "
        "year=2019)" "values(101, 'z')" % (unique_database, tbl_insert_part))
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify that the data is present in Impala.
    data = self.execute_scalar("select * from %s.%s where day=28 and month=3 and"
        " year=2019 and id=101" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['101', 'z', '28', '3', '2019']
    # Test insert overwrite into existing partitions by Impala with empty results
    # (IMPALA-10765).
    self.execute_query("insert overwrite {db}.{tbl} "
                       "partition(day=28, month=03, year=2019) "
                       "select id, name from {db}.ctas_part"
                       .format(db=unique_database, tbl=tbl_insert_part))
    result = self.execute_query("select * from {db}.{tbl} "
                                "where day=28 and month=3 and year=2019"
                                .format(db=unique_database, tbl=tbl_insert_part))
    assert len(result.data) == 0

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_iceberg_inserts(self):
    """IMPALA-10735: INSERT INTO Iceberg table fails during INSERT event generation
    This test doesn't test event processing. It tests that Iceberg INSERTs still work
    when HMS event polling is enabled.
    IMPALA-10736 tracks adding proper support for Hive Replication."""
    db_name = self.__get_random_name("iceberg_insert_event_db_")
    tbl_name = "ice_test"
    try:
      self.execute_query("create database if not exists {0}".format(db_name))
      self.execute_query("""
          create table {0}.{1} (i int)
          partition by spec (i bucket 5)
          stored as iceberg;""".format(db_name, tbl_name))
      self.execute_query("insert into {0}.{1} values (1)".format(db_name, tbl_name))
      data = self.execute_scalar("select * from {0}.{1}".format(db_name, tbl_name))
      assert data == '1'
    finally:
      self.execute_query("drop database if exists {0} cascade".format(db_name))

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  @SkipIfHive2.acid
  def test_empty_partition_events_transactional(self, unique_database):
    self._run_test_empty_partition_events(unique_database, True)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_self_events(self, unique_database):
    """Runs multiple queries which generate events and makes
    sure that tables and partitions are not refreshed the queries is run from Impala. If
    the queries are run from Hive, we make sure that the tables and partitions are
    refreshed"""
    self.__run_self_events_test(unique_database, True)
    self.__run_self_events_test(unique_database, False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=1",
    cluster_size=1)
  def test_create_drop_events(self, unique_database):
    self.__run_create_drop_test(unique_database, "database")
    self.__run_create_drop_test(unique_database, "table")
    self.__run_create_drop_test(unique_database, "table", True)
    self.__run_create_drop_test(unique_database, "table", True, True)
    self.__run_create_drop_test(unique_database, "partition")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal --hms_event_polling_interval_s=1",
    cluster_size=1)
  def test_local_catalog_create_drop_events(self, unique_database):
    self.__run_create_drop_test(unique_database, "database")
    self.__run_create_drop_test(unique_database, "table")
    self.__run_create_drop_test(unique_database, "table", True)
    self.__run_create_drop_test(unique_database, "table", True, True)
    self.__run_create_drop_test(unique_database, "partition")

  def __run_create_drop_test(self, db, type, rename=False, rename_db=False):
    if type == "table":
      if not rename:
        queries = [
          "create table {0}.test_{1} (i int)".format(db, 1),
          "drop table {0}.test_{1}".format(db, 1)
        ]
      else:
        db_1 = "{}_1".format(db)
        if rename_db:
          self.execute_query_expect_success(self.create_impala_client(),
            "drop database if exists {0} cascade".format(db_1))
          self.execute_query_expect_success(self.create_impala_client(),
            "create database {0}".format(db_1))
        self.execute_query_expect_success(self.create_impala_client(),
          "create table if not exists {0}.rename_test_1 (i int)".format(db))
        if rename_db:
          queries = [
            "alter table {0}.rename_test_1 rename to {1}.rename_test_1".format(db, db_1),
            "alter table {0}.rename_test_1 rename to {1}.rename_test_1".format(db_1, db)
          ]
        else:
          queries = [
            "alter table {0}.rename_test_1 rename to {0}.rename_test_2".format(db),
            "alter table {0}.rename_test_2 rename to {0}.rename_test_1".format(db)
          ]
      create_metric_name = "tables-added"
      removed_metric_name = "tables-removed"
    elif type == "database":
      self.execute_query_expect_success(self.create_impala_client(),
        "drop database if exists {0}".format("test_create_drop_db"))
      queries = [
        "create database {db}".format(db="test_create_drop_db"),
        "drop database {db}".format(db="test_create_drop_db")
      ]
      create_metric_name = "databases-added"
      removed_metric_name = "databases-removed"
    else:
      tbl_name = "test_create_drop_partition"
      self.execute_query_expect_success(self.create_impala_client(),
          "create table {db}.{tbl} (c int) partitioned by (p int)".format(
              db=db, tbl=tbl_name))
      queries = [
        "alter table {db}.{tbl} add partition (p=1)".format(db=db, tbl=tbl_name),
        "alter table {db}.{tbl} drop partition (p=1)".format(db=db, tbl=tbl_name)
      ]
      create_metric_name = "partitions-added"
      removed_metric_name = "partitions-removed"

    # get the metric before values
    EventProcessorUtils.wait_for_event_processing(self)
    create_metric_val_before = EventProcessorUtils.\
      get_event_processor_metric(create_metric_name, 0)
    removed_metric_val_before = EventProcessorUtils.\
      get_event_processor_metric(removed_metric_name, 0)
    events_skipped_before = EventProcessorUtils.\
      get_event_processor_metric('events-skipped', 0)
    num_iters = 200
    for iter in xrange(num_iters):
      for q in queries:
        try:
          self.execute_query_expect_success(self.create_impala_client(), q)
        except Exception as e:
          print("Failed in {} iterations. Error {}".format(iter, str(e)))
          raise
    EventProcessorUtils.wait_for_event_processing(self)
    create_metric_val_after = EventProcessorUtils. \
      get_event_processor_metric(create_metric_name, 0)
    removed_metric_val_after = EventProcessorUtils. \
      get_event_processor_metric(removed_metric_name, 0)
    events_skipped_after = EventProcessorUtils. \
      get_event_processor_metric('events-skipped', 0)
    num_delete_event_entries = EventProcessorUtils. \
      get_event_processor_metric('delete-event-log-size', 0)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    # None of the queries above should actually trigger a add/remove object from events
    assert int(create_metric_val_after) == int(create_metric_val_before)
    assert int(removed_metric_val_after) == int(removed_metric_val_before)
    # each query set generates 2 events and both of them should be skipped
    assert int(events_skipped_after) == num_iters * 2 + int(events_skipped_before)
    # make sure that there are no more entries in the delete event log
    assert int(num_delete_event_entries) == 0

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_event_based_replication(self):
    self.__run_event_based_replication_tests()

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=10")
  def test_drop_table_events(self):
    """IMPALA-10187: Event processing fails on multiple events + DROP TABLE.
    This test issues ALTER TABLE + DROP in quick succession and checks whether event
    processing still works.
    """
    event_proc_timeout = 15
    db_name = self.__get_random_name("drop_event_db_")
    with HiveDbWrapper(self, db_name):
      tbl_name = "foo"
      self.run_stmt_in_hive("""
          drop table if exists {db}.{tbl};
          create table {db}.{tbl} (id int);
          insert into {db}.{tbl} values(1);""".format(db=db_name, tbl=tbl_name))
      # With MetastoreEventProcessor running, the insert event will be processed. Query
      # the table from Impala.
      EventProcessorUtils.wait_for_event_processing(self, event_proc_timeout)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select * from %s.%s" % (db_name, tbl_name))
      assert data == '1'
      # Execute ALTER TABLE + DROP in quick succession so they will be processed in the
      # same event batch.
      self.run_stmt_in_hive("""
          alter table {db}.{tbl} set tblproperties ('foo'='bar');
          drop table {db}.{tbl};""".format(db=db_name, tbl=tbl_name))
      EventProcessorUtils.wait_for_event_processing(self, event_proc_timeout)
      # Check that the event processor status is still ACTIVE.
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  def __run_event_based_replication_tests(self, transactional=True):
    """Hive Replication relies on the insert events generated on the tables.
    This test issues some basic replication commands from Hive and makes sure
    that the replicated table has correct data."""
    TBLPROPERTIES = self.__get_transactional_tblproperties(transactional)
    source_db = self.__get_random_name("repl_source_")
    target_db = self.__get_random_name("repl_target_")
    unpartitioned_tbl = "unpart_tbl"
    partitioned_tbl = "part_tbl"
    try:
      self.run_stmt_in_hive("create database {0}".format(source_db))
      self.run_stmt_in_hive(
        "alter database {0} set dbproperties ('repl.source.for'='xyz')".format(source_db))
      # explicit create table command since create table like doesn't allow tblproperties
      self.client.execute("create table {0}.{1} (a string, b string) stored as parquet"
        " {2}".format(source_db, unpartitioned_tbl, TBLPROPERTIES))
      EventProcessorUtils.wait_for_event_processing(self)
      self.client.execute(
        "create table {0}.{1} (id int, bool_col boolean, tinyint_col tinyint, "
        "smallint_col smallint, int_col int, bigint_col bigint, float_col float, "
        "double_col double, date_string string, string_col string, "
        "timestamp_col timestamp) partitioned by (year int, month int) stored as parquet"
        " {2}".format(source_db, partitioned_tbl, TBLPROPERTIES))

      # case I: insert
      # load the table with some data from impala, this also creates new partitions.
      self.client.execute("insert into {0}.{1}"
        " select * from functional.tinytable".format(source_db,
          unpartitioned_tbl))
      self.client.execute("insert into {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      rows_in_unpart_tbl = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(source_db, unpartitioned_tbl)).split('\t')[
        0])
      rows_in_part_tbl = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(source_db, partitioned_tbl)).split('\t')[0])
      assert rows_in_unpart_tbl > 0
      assert rows_in_part_tbl > 0
      # bootstrap the replication
      self.run_stmt_in_hive("repl dump {0}".format(source_db))
      # create a target database where tables will be replicated
      self.client.execute("create database {0}".format(target_db))
      # replicate the table from source to target
      self.run_stmt_in_hive("repl load {0} into {1}".format(source_db, target_db))
      EventProcessorUtils.wait_for_event_processing(self)
      assert unpartitioned_tbl in self.client.execute(
        "show tables in {0}".format(target_db)).get_data()
      assert partitioned_tbl in self.client.execute(
        "show tables in {0}".format(target_db)).get_data()
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl))
          .split('\t')[0])
      assert rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert rows_in_part_tbl == rows_in_part_tbl_target

      # case II: insert into existing partitions.
      self.client.execute("insert into {0}.{1}"
        " select * from functional.tinytable".format(
          source_db, unpartitioned_tbl))
      self.client.execute("insert into {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      self.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      self.run_stmt_in_hive("repl load {0} into {1}".format(source_db, target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing(self)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl)).split('\t')[0])
      assert 2 * rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert 2 * rows_in_part_tbl == rows_in_part_tbl_target

      # Case III: insert overwrite
      # impala does a insert overwrite of the tables.
      self.client.execute("insert overwrite table {0}.{1}"
        " select * from functional.tinytable".format(
          source_db, unpartitioned_tbl))
      self.client.execute("insert overwrite table {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      self.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      self.run_stmt_in_hive("repl load {0} into {1}".format(source_db, target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing(self)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl)).split('\t')[0])
      assert rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert rows_in_part_tbl == rows_in_part_tbl_target

      # Case IV: CTAS which creates a transactional table.
      self.client.execute(
        "create table {0}.insertonly_nopart_ctas {1} as "
        "select * from {0}.{2}".format(source_db, TBLPROPERTIES, unpartitioned_tbl))
      self.client.execute(
        "create table {0}.insertonly_part_ctas partitioned by (year, month) {1}"
        " as select * from {0}.{2}".format(source_db, TBLPROPERTIES, partitioned_tbl))
      self.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      self.run_stmt_in_hive("repl load {0} into {1}".format(source_db, target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing(self)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_source = int(self.execute_scalar("select count(*) from "
        "{0}.insertonly_nopart_ctas".format(source_db)).split('\t')[0])
      rows_in_unpart_tbl_target = int(self.execute_scalar("select count(*) from "
          "{0}.insertonly_nopart_ctas".format(target_db)).split('\t')[0])
      assert rows_in_unpart_tbl_source == rows_in_unpart_tbl_target
      rows_in_unpart_tbl_source = int(self.execute_scalar("select count(*) from "
        "{0}.insertonly_part_ctas".format(source_db)).split('\t')[0])
      rows_in_unpart_tbl_target = int(self.execute_scalar("select count(*) from "
        "{0}.insertonly_part_ctas".format(target_db)).split('\t')[0])
      assert rows_in_unpart_tbl_source == rows_in_unpart_tbl_target

      # Case V: truncate table
      # impala truncates both the tables. Make sure replication sees that.
      self.client.execute("truncate table {0}.{1}".format(source_db, unpartitioned_tbl))
      self.client.execute("truncate table {0}.{1}".format(source_db, partitioned_tbl))
      self.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      self.run_stmt_in_hive("repl load {0} into {1}".format(source_db, target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing(self)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(self.execute_scalar(
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl)).split('\t')[0])
      assert rows_in_unpart_tbl_target == 0
      assert rows_in_part_tbl_target == 0
    finally:
      src_db = self.__get_db_nothrow(source_db)
      target_db_obj = self.__get_db_nothrow(target_db)
      if src_db is not None:
        self.run_stmt_in_hive(
          "alter database {0} set dbproperties ('repl.source.for'='')".format(source_db))
        self.run_stmt_in_hive("drop database if exists {0} cascade".format(source_db))
      if target_db_obj is not None:
        self.run_stmt_in_hive("drop database if exists {0} cascade".format(target_db))
      # workaround for HIVE-24135. the managed db location doesn't get cleaned up
      if src_db is not None and src_db.managedLocationUri is not None:
        self.filesystem_client.delete_file_dir(src_db.managedLocationUri, True)
      if target_db_obj is not None and target_db_obj.managedLocationUri is not None:
        self.filesystem_client.delete_file_dir(target_db_obj.managedLocationUri, True)

  def __get_db_nothrow(self, name):
    try:
      return self.hive_client.get_database(name)
    except:
      return None

  def _run_test_empty_partition_events(self, unique_database, is_transactional):
    test_tbl = unique_database + ".test_events"
    TBLPROPERTIES = self.__get_transactional_tblproperties(is_transactional)
    self.run_stmt_in_hive("create table {0} (key string, value string) \
      partitioned by (year int) stored as parquet {1}".format(test_tbl, TBLPROPERTIES))
    EventProcessorUtils.wait_for_event_processing(self)
    self.client.execute("describe {0}".format(test_tbl))

    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')

    self.run_stmt_in_hive(
      "alter table {0} add if not exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop if exists partition (year=2019)".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  def __get_tbl_location(self, db_name, tbl_name):
    assert self.hive_client is not None
    return self.hive_client.get_table(db_name, tbl_name).sd.location

  def __get_transactional_tblproperties(self, is_transactional):
    """
    Util method to generate the tblproperties for transactional tables
    """
    tblproperties = ""
    if is_transactional:
       tblproperties = "tblproperties ('transactional'='true'," \
           "'transactional_properties'='insert_only')"
    return tblproperties

  def __run_self_events_test(self, db_name, use_impala):
    recover_tbl_name = self.__get_random_name("tbl_")
    # create a table similar to alltypes so that we can recover the partitions on it
    # later in one of the test queries
    alltypes_tab_location = self.__get_tbl_location("functional", "alltypes")
    self.client.execute(
      "create external table {0}.{1} like functional.alltypes location '{2}'".format(
        db_name, recover_tbl_name, alltypes_tab_location))
    self.client.execute("refresh {0}.{1}".format(db_name, recover_tbl_name))
    if use_impala:
      queries = self.__get_impala_test_queries(db_name, recover_tbl_name)
      # some queries do not trigger self-event evaluation (creates and drops) however,
      # its still good to confirm that we don't do unnecessary refreshes in such cases
      # For such queries we use a different metrics events-skipped to confirm that these
      # events are skipped.
      for stmt in queries[False]:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala, False)
      # All the queries with True key should confirm that the events-skipped counter
      # is also incremented
      for stmt in queries[True]:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala)
    else:
      queries = self.__get_hive_test_queries(db_name, recover_tbl_name)
      for stmt in queries:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala)

  def __get_impala_test_queries(self, db_name, recover_tbl_name):
    tbl_name = self.__get_random_name("tbl_")
    acid_tbl_name = self.__get_random_name("acid_tbl_")
    acid_no_part_tbl_name = self.__get_random_name("acid_no_part_tbl_")
    tbl2 = self.__get_random_name("tbl_")
    view_name = self.__get_random_name("view_")
    view2 = self.__get_random_name("view_")
    # create a empty table for both partitioned and unpartitioned case for testing insert
    # events
    empty_unpartitioned_tbl = self.__get_random_name("empty_unpart_tbl_")
    empty_partitioned_tbl = self.__get_random_name("empty_parttbl_")
    self.client.execute(
      "create table {0}.{1} (c1 int)".format(db_name, empty_unpartitioned_tbl))
    self.client.execute(
      "create table {0}.{1} (c1 int) partitioned by (part int)".format(db_name,
        empty_partitioned_tbl))
    acid_props = self.__get_transactional_tblproperties(True)
    self_event_test_queries = {
      # Queries which will increment the events-skipped counter
      True: [
          # ALTER_DATABASE case
          "comment on database {0} is 'self-event test database'".format(db_name),
          "alter database {0} set owner user `test-user`".format(db_name),
          "create function {0}.f() returns int location '{1}/libTestUdfs.so' "
          "symbol='NoArgs'".format(db_name, WAREHOUSE),
          "drop function {0}.f()".format(db_name),
          # ALTER_TABLE case
          "alter table {0}.{1} set TBLPROPERTIES ('k'='v')".format(db_name, tbl_name),
          "alter table {0}.{1} ADD COLUMN c1 int".format(db_name, tbl_name),
          "alter table {0}.{1} ALTER COLUMN C1 set comment 'c1 comment'".format(db_name,
                                                                                tbl_name),
          "alter table {0}.{1} ADD COLUMNS (c2 int, c3 string)".format(db_name, tbl_name),
          "alter table {0}.{1} DROP COLUMN c1".format(db_name, tbl_name),
          "alter table {0}.{1} DROP COLUMN c2".format(db_name, tbl_name),
          "alter table {0}.{1} DROP COLUMN c3".format(db_name, tbl_name),
          "alter table {0}.{1} set owner user `test-user`".format(db_name, tbl_name),
          "alter table {0}.{1} set owner role `test-role`".format(db_name, tbl_name),
          "alter view {0}.{1} set owner user `test-view-user`".format(db_name, view_name),
          "alter view {0}.{1} set owner role `test-view-role`".format(db_name, view_name),
          # compute stats will generates ALTER_PARTITION
          "compute stats {0}.{1}".format(db_name, tbl_name),
          # insert into a existing partition; generates INSERT self-event
          "insert into table {0}.{1} partition "
          "(year, month) select * from functional.alltypessmall where year=2009 "
          "and month=1".format(db_name, tbl_name),
          # insert overwrite query from Impala also generates a INSERT self-event
          "insert overwrite table {0}.{1} partition "
          "(year, month) select * from functional.alltypessmall where year=2009 "
          "and month=1".format(db_name, tbl_name)],
      # Queries which will not increment the events-skipped counter
      False: [
          "create table {0}.{1} like functional.alltypessmall "
          "stored as parquet".format(db_name, tbl_name),
          "create view {0}.{1} as select * from functional.alltypessmall "
          "where year=2009".format(db_name, view_name),
          # in case of rename we process it as drop+create and hence
          # the events-skipped counter is not updated. Instead if this event is processed,
          # it will increment the tables-added and tables-removed counters.
          "alter table {0}.{1} rename to {0}.{2}".format(db_name, tbl_name, tbl2),
          "alter table {0}.{1} rename to {0}.{2}".format(db_name, tbl2, tbl_name),
          "alter view {0}.{1} rename to {0}.{2}".format(db_name, view_name, view2),
          "alter view {0}.{1} rename to {0}.{2}".format(db_name, view2, view_name),
          # ADD_PARTITION cases
          # dynamic partition insert (creates new partitions)
          "insert into table {0}.{1} partition (year,month) "
          "select * from functional.alltypessmall where month % 2 = 0".format(db_name,
            tbl_name),
          "insert overwrite table {0}.{1} partition (year,month) "
          "select * from functional.alltypessmall where month % 2 = 1".format(db_name,
            tbl_name),
          # we add this statement below just to make sure that the subsequent statement is
          # a no-op
          "alter table {0}.{1} add if not exists partition (year=2100, month=1)".format(
            db_name, tbl_name),
          "alter table {0}.{1} add if not exists partition (year=2100, month=1)".format(
            db_name, tbl_name),
          # DROP_PARTITION cases
          "alter table {0}.{1} drop if exists partition (year=2100, month=1)".format(
            db_name, tbl_name),
          # drop non-existing partition; essentially this is a no-op
          "alter table {0}.{1} drop if exists partition (year=2100, month=1)".format(
            db_name, tbl_name),
          # empty table case where no insert events are generated
          "insert overwrite {0}.{1} select * from {0}.{1}".format(
            db_name, empty_unpartitioned_tbl),
          "insert overwrite {0}.{1} partition(part) select * from {0}.{1}".format(
            db_name, empty_partitioned_tbl),
          # in case of ACID tables no INSERT event is generated as the COMMIT event
          # contains the related data
          "create table {0}.{1} (c1 int) {2}".format(db_name,
            acid_no_part_tbl_name, acid_props),
          "insert into table {0}.{1} values (1) ".format(db_name, acid_no_part_tbl_name),
          "insert overwrite table {0}.{1} select * from {0}.{1}".format(
            db_name, acid_no_part_tbl_name),
          "truncate table {0}.{1}".format(db_name, acid_no_part_tbl_name),
          # the table is empty so the following insert adds 0 rows
          "insert overwrite table {0}.{1} select * from {0}.{1}".format(
            db_name, acid_no_part_tbl_name),
          "create table {0}.{1} (c1 int) partitioned by (part int) {2}".format(db_name,
            acid_tbl_name, acid_props),
          "insert into table {0}.{1} partition (part=1) "
            "values (1) ".format(db_name, acid_tbl_name),
          "insert into table {0}.{1} partition (part) select id, int_col "
            "from functional.alltypestiny".format(db_name, acid_tbl_name),
          # repeat the same insert, now it writes to existing partitions
          "insert into table {0}.{1} partition (part) select id, int_col "
            "from functional.alltypestiny".format(db_name, acid_tbl_name),
          # following insert overwrite is used instead of truncate, because truncate
          # leads to a non-self event that reloads the table
          "insert overwrite table {0}.{1} partition (part) select id, int_col "
            "from functional.alltypestiny where id=-1".format(db_name, acid_tbl_name),
          "insert overwrite table {0}.{1} partition (part) select id, int_col "
            "from functional.alltypestiny".format(db_name, acid_tbl_name),
          "insert overwrite {0}.{1} partition(part) select * from {0}.{1}".format(
            db_name, acid_tbl_name),
          # recover partitions will generate add_partition events
          "alter table {0}.{1} recover partitions".format(db_name, recover_tbl_name)
      ]
    }
    return self_event_test_queries

  def __get_hive_test_queries(self, db_name, recover_tbl_name):
    tbl_name = self.__get_random_name("hive_test_tbl_")
    tbl2 = self.__get_random_name("hive_renamed_tbl_")
    view_name = self.__get_random_name("hive_view_")
    # we use a custom table schema to make it easier to change columns later in the
    # test_queries
    self.client.execute("create table {0}.{1} (key int) partitioned by "
                        "(part int) stored as parquet".format(db_name, tbl_name))
    self.client.execute(
      "create view {0}.{1} as select * from functional.alltypessmall where year=2009"
        .format(db_name, view_name))
    # events-processor only refreshes loaded tables, hence its important to issue a
    # refresh here so that table is in loaded state
    self.client.execute("refresh {0}.{1}".format(db_name, tbl_name))
    self_event_test_queries = [
      # ALTER_DATABASE cases
      "alter database {0} set dbproperties ('comment'='self-event test database')".format(
        db_name),
      "alter database {0} set owner user `test-user`".format(db_name),
      # ALTER_TABLE case
      "alter table {0}.{1} set tblproperties ('k'='v')".format(db_name, tbl_name),
      "alter table {0}.{1} add columns (value string)".format(db_name, tbl_name),
      "alter table {0}.{1} set owner user `test-user`".format(db_name, tbl_name),
      "alter table {0}.{1} set owner role `test-role`".format(db_name, tbl_name),
      # need to set this config to make sure the dynamic partition insert works below
      "set hive.exec.dynamic.partition.mode=nonstrict",
      # ADD_PARTITION cases
      "insert into table {0}.{1} partition (part=2009) "
      "select id as key, string_col as value from functional.alltypessmall".format(
        db_name, tbl_name),
      # add partition
      "alter table {0}.{1} add if not exists partition (part=1111)".format(
        db_name, tbl_name),
      # add existing partition; essentially this is a no-op
      "alter table {0}.{1} add if not exists partition (part=1111)".format(
        db_name, tbl_name),
      # DROP_PARTITION cases
      "alter table {0}.{1} drop if exists partition (part=1111)".format(
        db_name, tbl_name),
      # drop non-existing partition; essentially this is a no-op
      "alter table {0}.{1} drop if exists partition (part=1111)".format(
        db_name, tbl_name),
      # compute stats will generates ALTER_PARTITION
      "analyze table {0}.{1} compute statistics for columns".format(db_name, tbl_name),
      "msck repair table {0}.{1}".format(db_name, recover_tbl_name),
      # we rename in the end since impala will have the new table in unloaded
      # state after rename and hence any events later will be ignored anyways.
      "alter table {0}.{1} rename to {0}.{2}".format(db_name, tbl_name, tbl2),
      "alter view {0}.{1} rename to {0}.{2}".format(db_name, view_name,
        self.__get_random_name("view_")),
    ]
    return self_event_test_queries

  @staticmethod
  def __get_self_event_metrics():
    """
    Gets the tables-refreshed, partitions-refreshed and events-skipped metric values
    from Metastore EventsProcessor
    """
    tbls_refreshed_count = EventProcessorUtils.get_event_processor_metric(
      'tables-refreshed', 0)
    partitions_refreshed_count = EventProcessorUtils.get_event_processor_metric(
      'partitions-refreshed', 0)
    events_skipped_count = EventProcessorUtils.get_event_processor_metric(
      'events-skipped', 0)
    return int(tbls_refreshed_count), int(partitions_refreshed_count), \
      int(events_skipped_count)

  def __exec_sql_and_check_selfevent_counter(self, stmt, use_impala_client,
                                             check_events_skipped_counter=True):
    """
    Method runs a given query statement using a impala client or hive client based on the
    argument use_impala_client and confirms if the self-event related counters are as
    expected based on whether we expect a self-event or not. If the
    check_self_event_counter is False it skips checking the events-skipped metric.
    """
    EventProcessorUtils.wait_for_event_processing(self)
    tbls_refreshed, partitions_refreshed,\
      events_skipped = self.__get_self_event_metrics()
    last_synced_event = EventProcessorUtils.get_last_synced_event_id()
    logging.info("Running statement in {1}: {0}".format(stmt,
      "impala" if use_impala_client else "hive"))
    if not use_impala_client:
      self.run_stmt_in_hive(stmt)
    else:
      self.client.execute(stmt)

    EventProcessorUtils.wait_for_event_processing(self)
    tbls_refreshed_after, partitions_refreshed_after,\
      events_skipped_after = self.__get_self_event_metrics()
    last_synced_event_after = EventProcessorUtils.get_last_synced_event_id()
    # we assume that any event which comes due to stmts run from impala-client are
    # self-events
    logging.info(
      "Event id before {0} event id after {1}".format(last_synced_event,
        last_synced_event_after))
    if use_impala_client:
      # self-event counter must increase if this is a self-event if
      # check_self_event_counter is set
      # some of the test queries generate no events at all. If that is the case
      # skip the below comparison
      if last_synced_event_after > last_synced_event:
        if check_events_skipped_counter:
          assert events_skipped_after > events_skipped, \
            "Failing query(impala={}): {}".format(use_impala_client, stmt)
      # if this is a self-event, no table or partitions should be refreshed
      assert tbls_refreshed == tbls_refreshed_after, \
        "Failing query(impala={}): {}".format(use_impala_client, stmt)
      assert partitions_refreshed == partitions_refreshed_after, \
        "Failing query(impala={}): {}".format(use_impala_client, stmt)
    else:
      # hive was used to run the stmts, any events generated should not have been deemed
      # as self events
      assert events_skipped == events_skipped_after

  @staticmethod
  def __get_random_name(prefix=''):
    """
    Gets a random name used to create unique database or table
    """
    return prefix + ''.join(random.choice(string.ascii_lowercase) for i in range(5))
