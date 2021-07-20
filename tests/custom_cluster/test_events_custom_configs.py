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
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite
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
class TestEventProcessingCustomConfigs(CustomClusterTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog for non-default configurations"""
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

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=10")
  def test_drop_table_events(self):
    """IMPALA-10187: Event processing fails on multiple events + DROP TABLE.
    This test issues ALTER TABLE + DROP in quick succession and checks whether event
    processing still works.
    """
    event_proc_timeout = 15
    db_name = ImpalaTestSuite.get_random_name("drop_event_db_")
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

  @CustomClusterTestSuite.with_args(
    cluster_size=1,
    catalogd_args="--hms_event_polling_interval_s=1")
  def test_create_drop_events(self, unique_database):
    """Regression test for IMPALA-10502. The test runs very slow with default
    statestored update frequency and hence this is changed to a custom cluster
    test."""
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
    """
    Test is similar to the test_create_drop_events except this runs on local
    """
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
            "alter table {0}.rename_test_1 rename to {1}.rename_test_1".format(db,
              db_1),
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
    create_metric_val_before = EventProcessorUtils. \
      get_event_processor_metric(create_metric_name, 0)
    removed_metric_val_before = EventProcessorUtils. \
      get_event_processor_metric(removed_metric_name, 0)
    events_skipped_before = EventProcessorUtils. \
      get_event_processor_metric('events-skipped', 0)
    num_iters = 100
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
  def test_self_events(self, unique_database):
    """Runs multiple queries which generate events and makes
    sure that tables and partitions are not refreshed the queries is run from Impala. If
    the queries are run from Hive, we make sure that the tables and partitions are
    refreshed"""
    self.__run_self_events_test(unique_database, True)
    self.__run_self_events_test(unique_database, False)

  def __run_self_events_test(self, db_name, use_impala):
    recover_tbl_name = ImpalaTestSuite.get_random_name("tbl_")
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
    tbl_name = ImpalaTestSuite.get_random_name("tbl_")
    acid_tbl_name = ImpalaTestSuite.get_random_name("acid_tbl_")
    acid_no_part_tbl_name = ImpalaTestSuite.get_random_name("acid_no_part_tbl_")
    tbl2 = ImpalaTestSuite.get_random_name("tbl_")
    view_name = ImpalaTestSuite.get_random_name("view_")
    view2 = ImpalaTestSuite.get_random_name("view_")
    # create a empty table for both partitioned and unpartitioned case for testing insert
    # events
    empty_unpartitioned_tbl = ImpalaTestSuite.get_random_name("empty_unpart_tbl_")
    empty_partitioned_tbl = ImpalaTestSuite.get_random_name("empty_parttbl_")
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
        "comment on table {0}.{1} IS 'table level comment'".format(db_name, tbl_name),
        "comment on column {0}.{1}.C1 IS 'column level comment'".format(db_name,
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
        "compute incremental stats {0}.{1}".format(db_name, tbl_name),
        "drop stats {0}.{1}".format(db_name, tbl_name),
        # insert into a existing partition; generates INSERT self-event
        "insert into table {0}.{1} partition "
        "(year, month) select * from functional.alltypessmall where year=2009 "
        "and month=1".format(db_name, tbl_name),
        # insert overwrite query from Impala also generates a INSERT self-event
        "insert overwrite table {0}.{1} partition "
        "(year, month) select * from functional.alltypessmall where year=2009 "
        "and month=1".format(db_name, tbl_name),
        # events processor doesn't process delete column stats events currently,
        # however, in case of incremental stats, there could be alter table and
        # alter partition events which should be ignored. Hence we run compute stats
        # before to make sure that the truncate table command generated alter events
        # are ignored.
        "compute incremental stats {0}.{1}".format(db_name, tbl_name),
        "truncate table {0}.{1}".format(db_name, tbl_name)],
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
    tbl_name = ImpalaTestSuite.get_random_name("hive_test_tbl_")
    tbl2 = ImpalaTestSuite.get_random_name("hive_renamed_tbl_")
    view_name = ImpalaTestSuite.get_random_name("hive_view_")
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
        ImpalaTestSuite.get_random_name("view_")),
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
    tbls_refreshed, partitions_refreshed, \
      events_skipped = self.__get_self_event_metrics()
    last_synced_event = EventProcessorUtils.get_last_synced_event_id()
    logging.info("Running statement in {1}: {0}".format(stmt,
      "impala" if use_impala_client else "hive"))
    if not use_impala_client:
      self.run_stmt_in_hive(stmt)
    else:
      self.client.execute(stmt)

    EventProcessorUtils.wait_for_event_processing(self)
    tbls_refreshed_after, partitions_refreshed_after, \
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
