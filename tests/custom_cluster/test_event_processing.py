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
  def test_transactional_insert_events(self):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    self.run_test_insert_events(is_transactional=True)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_insert_events(self):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    self.run_test_insert_events()

  def run_test_insert_events(self, is_transactional=False):
    """Test for insert event processing. Events are created in Hive and processed in
    Impala. The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    db_name = self.__get_random_name("insert_event_db_")
    tblproperties = self.__get_transactional_tblproperties(is_transactional)
    with HiveDbWrapper(self, db_name):
      # Test table with no partitions.
      test_tbl_name = 'tbl_insert_nopart'
      self.run_stmt_in_hive("drop table if exists %s.%s" % (db_name, test_tbl_name))
      self.run_stmt_in_hive("create table %s.%s (id int, val int) %s"
         % (db_name, test_tbl_name, tblproperties))
      # Test insert into table, this will fire an insert event.
      self.run_stmt_in_hive("insert into %s.%s values(101, 200)"
         % (db_name, test_tbl_name))
      # With MetastoreEventProcessor running, the insert event will be processed. Query
      # the table from Impala.
      EventProcessorUtils.wait_for_event_processing(self)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select * from %s.%s" % (db_name, test_tbl_name))
      assert data.split('\t') == ['101', '200']

      # Test insert overwrite. Overwrite the existing value.
      self.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
         % (db_name, test_tbl_name))
      # Make sure the event has been processed.
      EventProcessorUtils.wait_for_event_processing(self)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select * from %s.%s" % (db_name, test_tbl_name))
      assert data.split('\t') == ['101', '201']

      # Test partitioned table.
      test_part_tblname = 'tbl_insert_part'
      self.run_stmt_in_hive("drop table if exists %s.%s" % (db_name, test_part_tblname))
      self.run_stmt_in_hive("create table %s.%s (id int, name string) "
         "partitioned by(day int, month int, year int) %s"
         % (db_name, test_part_tblname, tblproperties))
      # Insert data into partitions.
      self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(101, 'x')" % (db_name, test_part_tblname))
      # Make sure the event has been processed.
      EventProcessorUtils.wait_for_event_processing(self)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select * from %s.%s" % (db_name, test_part_tblname))
      assert data.split('\t') == ['101', 'x', '28', '3', '2019']

      # Test inserting into existing partitions.
      self.run_stmt_in_hive("insert into %s.%s partition(day=28, month=03, year=2019)"
         "values(102, 'y')" % (db_name, test_part_tblname))
      EventProcessorUtils.wait_for_event_processing(self)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select count(*) from %s.%s where day=28 and month=3 "
         "and year=2019" % (db_name, test_part_tblname))
      assert data.split('\t') == ['2']

      # Test insert overwrite into existing partitions
      self.run_stmt_in_hive("insert overwrite table %s.%s partition(day=28, month=03, "
         "year=2019)" "values(101, 'z')" % (db_name, test_part_tblname))
      EventProcessorUtils.wait_for_event_processing(self)
      # Verify that the data is present in Impala.
      data = self.execute_scalar("select * from %s.%s where day=28 and month=3 and"
         " year=2019 and id=101" % (db_name, test_part_tblname))
      assert data.split('\t') == ['101', 'z', '28', '3', '2019']

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

  @pytest.mark.xfail(run=False, reason="This is failing due to HIVE-23995")
  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_event_based_replication(self):
    self.__run_event_based_replication_tests()

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
    if use_impala:
      queries = self.__get_impala_test_queries(db_name, recover_tbl_name)
      # some queries do not trigger self-event evaluation (creates and drops) however,
      # its still good to confirm that we don't do unnecessary refreshes in such cases
      for stmt in queries[False]:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala, False)
      # All the queries with True key should confirm that the self-events-skipped counter
      # is also incremented
      for stmt in queries[True]:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala)
    else:
      queries = self.__get_hive_test_queries(db_name, recover_tbl_name)
      for stmt in queries:
        self.__exec_sql_and_check_selfevent_counter(stmt, use_impala)

  def __get_impala_test_queries(self, db_name, recover_tbl_name):
    tbl_name = self.__get_random_name("tbl_")
    tbl2 = self.__get_random_name("tbl_")
    view_name = self.__get_random_name("view_")
    # create a empty table for both partitioned and unpartitioned case for testing insert
    # events
    empty_unpartitioned_tbl = self.__get_random_name("insert_test_tbl_")
    empty_partitioned_tbl = self.__get_random_name("insert_test_parttbl_")
    self.client.execute(
      "create table {0}.{1} (c1 int)".format(db_name, empty_unpartitioned_tbl))
    self.client.execute(
      "create table {0}.{1} (c1 int) partitioned by (part int)".format(db_name,
        empty_partitioned_tbl))
    self_event_test_queries = {
      # Queries which will increment the self-events-skipped counter
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
          "alter table {0}.{1} rename to {0}.{2}".format(db_name, tbl_name, tbl2),
          "alter view {0}.{1} set owner user `test-view-user`".format(db_name, view_name),
          "alter view {0}.{1} set owner role `test-view-role`".format(db_name, view_name),
          "alter view {0}.{1} rename to {0}.{2}".format(db_name, view_name,
                                                        self.__get_random_name("view_")),
          # ADD_PARTITION cases
          # dynamic partition insert (creates new partitions)
          "insert into table {0}.{1} partition (year,month) "
          "select * from functional.alltypessmall".format(db_name, tbl2),
          # add partition
          "alter table {0}.{1} add if not exists partition (year=1111, month=1)".format(
            db_name, tbl2),
          # compute stats will generates ALTER_PARTITION
          "compute stats {0}.{1}".format(db_name, tbl2),
          "alter table {0}.{1} recover partitions".format(db_name, recover_tbl_name)],
      # Queries which will not increment the self-events-skipped counter
      False: [
          "create table {0}.{1} like functional.alltypessmall "
          "stored as parquet".format(db_name, tbl_name),
          "create view {0}.{1} as select * from functional.alltypessmall "
            "where year=2009".format(db_name, view_name),
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
      ]
    }
    if HIVE_MAJOR_VERSION >= 3:
      # insert into a existing partition; generates INSERT self-event
      self_event_test_queries[True].append("insert into table {0}.{1} partition "
          "(year, month) select * from functional.alltypessmall where year=2009 "
          "and month=1".format(db_name, tbl2))
      # insert overwrite query from Impala also generates a INSERT self-event
      self_event_test_queries[True].append("insert overwrite table {0}.{1} partition "
         "(year, month) select * from functional.alltypessmall where year=2009 "
         "and month=1".format(db_name, tbl2))
    return self_event_test_queries

  def __get_hive_test_queries(self, db_name, recover_tbl_name):
    tbl_name = self.__get_random_name("tbl_")
    tbl2 = self.__get_random_name("tbl_")
    view_name = self.__get_random_name("view_")
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
      "alter table {0}.{1} rename to {0}.{2}".format(db_name, tbl_name, tbl2),
      "alter view {0}.{1} rename to {0}.{2}".format(db_name, view_name,
                                                    self.__get_random_name("view_")),
      # need to set this config to make sure the dynamic partition insert works below
      "set hive.exec.dynamic.partition.mode=nonstrict",
      # ADD_PARTITION cases
      "insert into table {0}.{1} partition (part=2009) "
      "select id as key, string_col as value from functional.alltypessmall".format(
        db_name, tbl2),
      # add partition
      "alter table {0}.{1} add if not exists partition (part=1111)".format(
        db_name, tbl2),
      # add existing partition; essentially this is a no-op
      "alter table {0}.{1} add if not exists partition (part=1111)".format(
        db_name, tbl2),
      # DROP_PARTITION cases
      "alter table {0}.{1} drop if exists partition (part=1111)".format(
        db_name, tbl2),
      # drop non-existing partition; essentially this is a no-op
      "alter table {0}.{1} drop if exists partition (part=1111)".format(
        db_name, tbl2),
      # compute stats will generates ALTER_PARTITION
      "analyze table {0}.{1} compute statistics for columns".format(db_name, tbl2),
      "msck repair table {0}.{1}".format(db_name, recover_tbl_name)
    ]
    return self_event_test_queries

  @staticmethod
  def __get_self_event_metrics():
    """
    Gets the self-events-skipped, tables-refreshed and partitions-refreshed metric values
    from Metastore EventsProcessor
    """
    tbls_refreshed_count = EventProcessorUtils.get_event_processor_metric(
      'tables-refreshed', 0)
    partitions_refreshed_count = EventProcessorUtils.get_event_processor_metric(
      'partitions-refreshed', 0)
    self_events_count = EventProcessorUtils.get_event_processor_metric(
      'self-events-skipped', 0)
    return int(self_events_count), int(tbls_refreshed_count), int(
      partitions_refreshed_count)

  def __exec_sql_and_check_selfevent_counter(self, stmt, use_impala_client,
                                             check_self_event_counter=True):
    """
    Method runs a given query statement using a impala client or hive client based on the
    argument use_impala_client and confirms if the self-event related counters are as
    expected based on whether we expect a self-event or not. If the
    check_self_event_counter is False it skips checking the self-events-skipped metric.
    """
    self_events, tbls_refreshed, partitions_refreshed = self.__get_self_event_metrics()
    if not use_impala_client:
      self.run_stmt_in_hive(stmt)
    else:
      self.client.execute(stmt)

    EventProcessorUtils.wait_for_event_processing(self)
    self_events_after, tbls_refreshed_after, partitions_refreshed_after = \
      self.__get_self_event_metrics()
    # we assume that any event which comes due to stmts run from impala-client are
    # self-events
    if use_impala_client:
      # self-event counter must increase if this is a self-event if
      # check_self_event_counter is set
      if check_self_event_counter:
        assert self_events_after > self_events
      # if this is a self-event, no table or partitions should be refreshed
      assert tbls_refreshed == tbls_refreshed_after
      assert partitions_refreshed == partitions_refreshed_after
    else:
      # hive was used to run the stmts, any events generated should not have been deemed
      # as self events
      assert self_events == self_events_after

  @staticmethod
  def __get_random_name(prefix=''):
    """
    Gets a random name used to create unique database or table
    """
    return prefix + ''.join(random.choice(string.ascii_lowercase) for i in range(5))
