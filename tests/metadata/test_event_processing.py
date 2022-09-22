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
from subprocess import check_call
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfHive2, SkipIfCatalogV2
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfFS.hive
@SkipIfCatalogV2.hms_event_polling_disabled()
class TestEventProcessing(ImpalaTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""
  CATALOG_URL = "http://localhost:25020"
  PROCESSING_TIMEOUT_S = 10

  @SkipIfHive2.acid
  def test_transactional_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    self.run_test_insert_events(unique_database, is_transactional=True)

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

  def test_iceberg_inserts(self):
    """IMPALA-10735: INSERT INTO Iceberg table fails during INSERT event generation
    This test doesn't test event processing. It tests that Iceberg INSERTs still work
    when HMS event polling is enabled.
    IMPALA-10736 tracks adding proper support for Hive Replication."""
    db_name = ImpalaTestSuite.get_random_name("iceberg_insert_event_db_")
    tbl_name = "ice_test"
    try:
      self.execute_query("create database if not exists {0}".format(db_name))
      self.execute_query("""
          create table {0}.{1} (i int)
          partitioned by spec (bucket(5, i))
          stored as iceberg;""".format(db_name, tbl_name))
      self.execute_query("insert into {0}.{1} values (1)".format(db_name, tbl_name))
      data = self.execute_scalar("select * from {0}.{1}".format(db_name, tbl_name))
      assert data == '1'
    finally:
      self.execute_query("drop database if exists {0} cascade".format(db_name))

  @SkipIfHive2.acid
  def test_empty_partition_events_transactional(self, unique_database):
    self._run_test_empty_partition_events(unique_database, True)

  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  def test_event_based_replication(self):
    self.__run_event_based_replication_tests()

  def __run_event_based_replication_tests(self, transactional=True):
    """Hive Replication relies on the insert events generated on the tables.
    This test issues some basic replication commands from Hive and makes sure
    that the replicated table has correct data."""
    TBLPROPERTIES = self.__get_transactional_tblproperties(transactional)
    source_db = ImpalaTestSuite.get_random_name("repl_source_")
    target_db = ImpalaTestSuite.get_random_name("repl_target_")
    unpartitioned_tbl = "unpart_tbl"
    partitioned_tbl = "part_tbl"
    try:
      self.run_stmt_in_hive("create database {0}".format(source_db))
      self.run_stmt_in_hive(
        "alter database {0} set dbproperties ('repl.source.for'='xyz')".format(source_db))
      EventProcessorUtils.wait_for_event_processing(self)
      # explicit create table command since create table like doesn't allow tblproperties
      self.client.execute("create table {0}.{1} (a string, b string) stored as parquet"
        " {2}".format(source_db, unpartitioned_tbl, TBLPROPERTIES))
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
    except Exception:
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

  @pytest.mark.execute_serially
  def test_load_data_from_impala(self, unique_database):
    tbl_nopart = "tbl_nopart"
    tbl_part = "tbl_part"
    staging_dir = "/tmp/{0}".format(unique_database)
    check_call(["hdfs", "dfs", "-mkdir", staging_dir])
    try:
      self.execute_query(
        "drop table if exists {0}.{1} purge".format(unique_database, tbl_nopart))
      self.execute_query(
        "drop table if exists {0}.{1} purge".format(unique_database, tbl_part))

      self.execute_query(
        "create table {0}.{1} like functional_parquet.tinytable stored as parquet"
        .format(unique_database, tbl_nopart))
      self.execute_query(
        "create table {0}.{1} like functional_parquet.alltypessmall stored as \
        parquet".format(unique_database, tbl_part))
      EventProcessorUtils.wait_for_event_processing(self)

      check_call([
        "hdfs", "dfs", "-cp", "/test-warehouse/tinytable_parquet", staging_dir])
      last_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
      self.execute_query("load data inpath '{0}/tinytable_parquet' \
        into table {1}.{2}".format(staging_dir, unique_database, tbl_nopart))
      # Check if there is an insert event fired after load data statement.
      events = EventProcessorUtils.get_next_notification(self.hive_client, last_event_id)
      assert len(events) == 1
      last_event = events[0]
      assert last_event.dbName == unique_database
      assert last_event.tableName == tbl_nopart
      assert last_event.eventType == "INSERT"

      check_call(["hdfs", "dfs", "-cp", "/test-warehouse/alltypessmall_parquet",
        staging_dir])
      self.execute_query(
        "alter table {0}.{1} add partition (year=2009,month=1)".format(
          unique_database, tbl_part))
      last_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
      self.execute_query(
        "load data inpath '{0}/alltypessmall_parquet/year=2009/month=1' \
        into table {1}.{2} partition (year=2009,month=1)".format(
          staging_dir, unique_database, tbl_part))
      # Check if there is an insert event fired after load data statement.
      events = EventProcessorUtils.get_next_notification(self.hive_client, last_event_id)
      assert len(events) == 1
      last_event = events[0]
      assert last_event.dbName == unique_database
      assert last_event.tableName == tbl_part
      assert last_event.eventType == "INSERT"
    finally:
      check_call(["hdfs", "dfs", "-rm", "-r", "-skipTrash", staging_dir])

  def __get_transactional_tblproperties(self, is_transactional):
    """
    Util method to generate the tblproperties for transactional tables
    """
    tblproperties = ""
    if is_transactional:
       tblproperties = "tblproperties ('transactional'='true'," \
           "'transactional_properties'='insert_only')"
    return tblproperties
