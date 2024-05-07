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
import logging
from os import getenv
import pytest


from beeswaxd.BeeswaxService import QueryState
from hive_metastore.ttypes import FireEventRequest
from hive_metastore.ttypes import FireEventRequestData
from hive_metastore.ttypes import InsertEventRequestData
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf, SkipIfFS
from tests.common.test_dimensions import add_exec_option_dimension
from tests.util.acid_txn import AcidTxn
from tests.util.hive_utils import HiveDbWrapper
from tests.util.event_processor_utils import EventProcessorUtils
from tests.util.filesystem_utils import WAREHOUSE
from tests.util.iceberg_util import IcebergCatalogs

HIVE_SITE_HOUSEKEEPING_ON =\
    getenv('IMPALA_HOME') + '/fe/src/test/resources/hive-site-housekeeping-on'
TRUNCATE_TBL_STMT = 'truncate table'


class TestEventProcessingCustomConfigsBase(CustomClusterTestSuite):
  """This the base class for other test classes in this file."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestEventProcessingCustomConfigsBase, cls).add_test_dimensions()

  def _run_self_events_test(self, db_name, options, use_impala):
    """Runs multiple queries which generate events and makes
    sure that tables and partitions are not refreshed the queries are run from Impala.
    If the queries are run from Hive, we make sure that the tables and partitions are
    refreshed"""
    recover_tbl_name = ImpalaTestSuite.get_random_name("tbl_")
    # create a table similar to alltypes so that we can recover the partitions on it
    # later in one of the test queries
    alltypes_tab_location = self._get_tbl_location("functional", "alltypes")
    self.execute_query(
      "create external table {0}.{1} like functional.alltypes location '{2}'"
      .format(db_name, recover_tbl_name, alltypes_tab_location), options)
    self.execute_query("refresh {0}.{1}".format(db_name, recover_tbl_name), options)
    if use_impala:
      queries = self._get_impala_test_queries(db_name, options, recover_tbl_name)
      # some queries do not trigger self-event evaluation (creates and drops) however,
      # its still good to confirm that we don't do unnecessary refreshes in such cases
      # For such queries we use a different metrics events-skipped to confirm that these
      # events are skipped.
      for stmt in queries[False]:
        self._exec_sql_and_check_selfevent_counter(stmt, options, use_impala, False)
      # All the queries with True key should confirm that the events-skipped counter
      # is also incremented
      for stmt in queries[True]:
        self._exec_sql_and_check_selfevent_counter(stmt, options, use_impala)
    else:
      queries = self._get_hive_test_queries(db_name, options, recover_tbl_name)
      for stmt in queries:
        self._exec_sql_and_check_selfevent_counter(stmt, options, use_impala)

  def _get_impala_test_queries(self, db_name, options, recover_tbl_name):
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
    self.execute_query(
      "create table {0}.{1} (c1 int)".format(db_name, empty_unpartitioned_tbl),
      options)
    self.execute_query(
      "create table {0}.{1} (c1 int) partitioned by (part int)"
      .format(db_name, empty_partitioned_tbl), options)
    acid_props = self._get_transactional_tblproperties(True)
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
        "{0} {1}.{2}".format(TRUNCATE_TBL_STMT, db_name, tbl_name)],
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
        "{0} {1}.{2}".format(TRUNCATE_TBL_STMT, db_name, acid_no_part_tbl_name),
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

  def _get_hive_test_queries(self, db_name, options, recover_tbl_name):
    tbl_name = ImpalaTestSuite.get_random_name("hive_test_tbl_")
    tbl2 = ImpalaTestSuite.get_random_name("hive_renamed_tbl_")
    view_name = ImpalaTestSuite.get_random_name("hive_view_")
    # we use a custom table schema to make it easier to change columns later in the
    # test_queries
    self.execute_query("create table {0}.{1} (key int) partitioned by "
                        "(part int) stored as parquet".format(db_name, tbl_name),
                        options)
    self.execute_query(
      "create view {0}.{1} as select * from functional.alltypessmall where year=2009"
        .format(db_name, view_name), options)
    # events-processor only refreshes loaded tables, hence its important to issue a
    # refresh here so that table is in loaded state
    self.execute_query("refresh {0}.{1}".format(db_name, tbl_name), options)
    self_event_test_queries = [
      # ALTER_DATABASE cases
      "alter database {0} set dbproperties ('comment'='self-event test "
      "database')".format(db_name),
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
  def _get_self_event_metrics():
    """
    Gets the tables-refreshed, partitions-refreshed and events-skipped metric values
    from Metastore EventsProcessor
    """
    tbls_refreshed_count = EventProcessorUtils.get_int_metric('tables-refreshed', 0)
    partitions_refreshed_count = EventProcessorUtils.get_int_metric(
      'partitions-refreshed', 0)
    events_skipped_count = EventProcessorUtils.get_int_metric('events-skipped', 0)
    return int(tbls_refreshed_count), int(partitions_refreshed_count), \
      int(events_skipped_count)

  def _exec_sql_and_check_selfevent_counter(self, stmt, options, use_impala_client,
      check_events_skipped_counter=True):
    """
    Method runs a given query statement using a impala client or hive client based on the
    argument use_impala_client and confirms if the self-event related counters are as
    expected based on whether we expect a self-event or not. If the
    check_self_event_counter is False it skips checking the events-skipped metric.
    """
    EventProcessorUtils.wait_for_event_processing(self)
    tbls_refreshed, partitions_refreshed, \
      events_skipped = self._get_self_event_metrics()
    last_synced_event = EventProcessorUtils.get_last_synced_event_id()
    logging.info("Running statement in {1}: {0}".format(stmt,
      "impala" if use_impala_client else "hive"))
    if not use_impala_client:
      self.run_stmt_in_hive(stmt)
    else:
      self.execute_query(stmt, options)

    EventProcessorUtils.wait_for_event_processing(self)
    tbls_refreshed_after, partitions_refreshed_after, \
      events_skipped_after = self._get_self_event_metrics()
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
      # if this is a self-event, no table or partitions should be refreshed,
      # except for "truncate table"
      if (TRUNCATE_TBL_STMT not in stmt):
        assert tbls_refreshed == tbls_refreshed_after, \
          "Failing query(impala={}): {}".format(use_impala_client, stmt)
      assert partitions_refreshed == partitions_refreshed_after, \
        "Failing query(impala={}): {}".format(use_impala_client, stmt)
    else:
      # hive was used to run the stmts, any events generated should not have been deemed
      # as self events unless there are empty partition add/drop events
      assert events_skipped <= events_skipped_after

  def _get_tbl_location(self, db_name, tbl_name):
    assert self.hive_client is not None
    return self.hive_client.get_table(db_name, tbl_name).sd.location

  def _get_transactional_tblproperties(self, is_transactional):
    """
    Util method to generate the tblproperties for transactional tables
    """
    tblproperties = ""
    if is_transactional:
      tblproperties = "tblproperties ('transactional'='true'," \
                      "'transactional_properties'='insert_only')"
    return tblproperties


@SkipIfFS.hive
class TestEventProcessingCustomConfigs(TestEventProcessingCustomConfigsBase):
  """This class contains tests that exercise the event processing mechanism in the
  catalog for non-default configurations"""

  @classmethod
  def add_test_dimensions(cls):
    super(TestEventProcessingCustomConfigs, cls).add_test_dimensions()

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
    create_metric_val_before = EventProcessorUtils.get_int_metric(create_metric_name, 0)
    removed_metric_val_before = EventProcessorUtils.get_int_metric(removed_metric_name, 0)
    events_skipped_before = EventProcessorUtils.get_int_metric('events-skipped', 0)
    num_iters = 100
    for iter in range(num_iters):
      for q in queries:
        try:
          self.execute_query_expect_success(self.create_impala_client(), q)
        except Exception as e:
          print("Failed in {} iterations. Error {}".format(iter, str(e)))
          raise
    EventProcessorUtils.wait_for_event_processing(self)
    create_metric_val_after = EventProcessorUtils.get_int_metric(create_metric_name, 0)
    removed_metric_val_after = EventProcessorUtils.get_int_metric(removed_metric_name, 0)
    events_skipped_after = EventProcessorUtils.get_int_metric('events-skipped', 0)
    num_delete_event_entries = EventProcessorUtils.\
        get_int_metric('delete-event-log-size', 0)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    # None of the queries above should actually trigger a add/remove object from events
    assert create_metric_val_after == create_metric_val_before
    assert removed_metric_val_after == removed_metric_val_before
    # each query set generates 2 events and both of them should be skipped
    assert events_skipped_after == num_iters * 2 + events_skipped_before
    # make sure that there are no more entries in the delete event log
    assert num_delete_event_entries == 0

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_self_events_with_hive(self, unique_database):
    """Runs multiple queries which generate events using hive as client"""
    self._run_self_events_test(unique_database, {}, use_impala=False)

  @CustomClusterTestSuite.with_args(
      catalogd_args="--hms_event_polling_interval_s=5"
                    " --enable_reload_events=true")
  def test_refresh_invalidate_events(self, unique_database):
    self.run_test_refresh_invalidate_events(unique_database, "reload_table")

  @CustomClusterTestSuite.with_args(
      catalogd_args="--hms_event_polling_interval_s=5"
                    " --enable_reload_events=true"
                    " --enable_sync_to_latest_event_on_ddls=true")
  def test_refresh_invalidate_events_enable_sync_to_latest_events(self, unique_database):
    self.run_test_refresh_invalidate_events(unique_database, "reload_table_sync", True)

  def run_test_refresh_invalidate_events(self, unique_database, test_reload_table,
    enable_sync_to_latest_event_on_ddls=False):
    """Test is to verify Impala-11808, refresh/invalidate commands should generate a
    Reload event in HMS and CatalogD's event processor should process this event.
    """
    self.client.execute(
      "create table {}.{} (i int) partitioned by (year int) "
        .format(unique_database, test_reload_table))
    self.client.execute(
      "insert into {}.{} partition (year=2022) values (1),(2),(3)"
        .format(unique_database, test_reload_table))
    self.client.execute(
      "insert into {}.{} partition (year=2023) values (1),(2),(3)"
        .format(unique_database, test_reload_table))
    EventProcessorUtils.wait_for_event_processing(self)

    def check_self_events(query):
      tbls_refreshed_before, partitions_refreshed_before, \
          events_skipped_before = self._get_self_event_metrics()
      last_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
      self.client.execute(query)
      # Check if there is a reload event fired after refresh query.
      events = EventProcessorUtils.get_next_notification(self.hive_client, last_event_id)
      assert len(events) == 1
      last_event = events[0]
      assert last_event.dbName == unique_database
      assert last_event.tableName == test_reload_table
      assert last_event.eventType == "RELOAD"
      EventProcessorUtils.wait_for_event_processing(self)
      tbls_refreshed_after, partitions_refreshed_after, \
          events_skipped_after = self._get_self_event_metrics()
      assert events_skipped_after > events_skipped_before

    check_self_events("refresh {}.{} partition(year=2022)"
        .format(unique_database, test_reload_table))
    check_self_events("refresh {}.{}".format(unique_database, test_reload_table))
    EventProcessorUtils.wait_for_event_processing(self)

    if enable_sync_to_latest_event_on_ddls:
      # Test to verify if older events are being skipped in event processor
      data = FireEventRequestData()
      data.refreshEvent = True
      req = FireEventRequest(True, data)
      req.dbName = unique_database
      req.tableName = test_reload_table
      # table level reload events
      tbl_events_skipped_before = EventProcessorUtils.get_num_skipped_events()
      for i in range(10):
        self.hive_client.fire_listener_event(req)
      EventProcessorUtils.wait_for_event_processing(self)
      tbl_events_skipped_after = EventProcessorUtils.get_num_skipped_events()
      assert tbl_events_skipped_after > tbl_events_skipped_before
      # partition level reload events
      EventProcessorUtils.wait_for_event_processing(self)
      part_events_skipped_before = EventProcessorUtils.get_num_skipped_events()
      req.partitionVals = ["2022"]
      for i in range(10):
        self.hive_client.fire_listener_event(req)
      EventProcessorUtils.wait_for_event_processing(self)
      part_events_skipped_after = EventProcessorUtils.get_num_skipped_events()
      assert part_events_skipped_after > part_events_skipped_before

    # Test to verify IMPALA-12213
    table = self.hive_client.get_table(unique_database, test_reload_table)
    table.dbName = unique_database
    table.tableName = "test_sequence_table"
    self.hive_client.create_table(table)
    data = FireEventRequestData()
    data.refreshEvent = True
    req = FireEventRequest(True, data)
    req.dbName = unique_database
    req.tableName = "test_sequence_table"
    self.hive_client.fire_listener_event(req)
    EventProcessorUtils.wait_for_event_processing(self)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  @CustomClusterTestSuite.with_args(
    catalogd_args="--enable_reload_events=true")
  def test_reload_events_with_transient_partitions(self, unique_database):
    tbl = unique_database + ".tbl"
    create_stmt = "create table {} (i int) partitioned by(p int)".format(tbl)
    add_part_stmt = "alter table {} add if not exists partition(p=0)".format(tbl)
    drop_part_stmt = "alter table {} drop if exists partition(p=0)".format(tbl)
    refresh_stmt = "refresh {} partition(p=0)".format(tbl)
    end_states = [self.client.QUERY_STATES['FINISHED'],
                  self.client.QUERY_STATES['EXCEPTION']]

    self.execute_query(create_stmt)
    self.execute_query(add_part_stmt)
    # Run REFRESH partition in the background so we can drop the partition concurrently.
    refresh_handle = self.client.execute_async(refresh_stmt)
    # Before IMPALA-12855, REFRESH usually fails in 2-3 rounds.
    for i in range(100):
      self.execute_query(drop_part_stmt)
      refresh_state = self.wait_for_any_state(refresh_handle, end_states, 10)
      assert refresh_state == self.client.QUERY_STATES['FINISHED'],\
          "REFRESH state: {}. Error log: {}".format(
              QueryState._VALUES_TO_NAMES[refresh_state],
              self.client.get_log(refresh_handle))
      self.execute_query(add_part_stmt)
      refresh_handle = self.client.execute_async(refresh_stmt)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--hms_event_polling_interval_s=10"
                  " --enable_skipping_older_events=true"
                  " --enable_sync_to_latest_event_on_ddls=true")
  def test_skipping_older_events(self, unique_database):
    """Test is to verify IMPALA-11535, event processor should ignore older events if the
    current event id is older than the lastRefreshEventId on the table/partition
    """
    test_old_table = "test_old_table"

    def verify_skipping_older_events(table_name, is_transactional, is_partitioned):
      query = " ".join(["create", "transactional" if is_transactional else '',
        "table {}.{} (i int)", "partitioned by (year int)" if is_partitioned else ''])
      self.run_stmt_in_hive(query.format(unique_database, table_name))
      values = "values (10),(20),(30)"
      EventProcessorUtils.wait_for_event_processing(self)

      def verify_skipping_hive_stmt_events(stmt, new_table_name):
        tbl_events_skipped_before = EventProcessorUtils.get_num_skipped_events()
        self.run_stmt_in_hive(stmt)
        self.client.execute(
          "refresh {}.{}".format(unique_database, new_table_name))
        tables_refreshed_before = EventProcessorUtils.get_int_metric("tables-refreshed")
        partitions_refreshed_before = \
          EventProcessorUtils.get_int_metric("partitions-refreshed")
        EventProcessorUtils.wait_for_event_processing(self)
        tbl_events_skipped_after = EventProcessorUtils.get_num_skipped_events()
        assert tbl_events_skipped_after > tbl_events_skipped_before
        tables_refreshed_after = EventProcessorUtils.get_int_metric("tables-refreshed")
        partitions_refreshed_after = \
          EventProcessorUtils.get_int_metric("partitions-refreshed")
        if is_partitioned:
          assert partitions_refreshed_after == partitions_refreshed_before
        else:
          assert tables_refreshed_after == tables_refreshed_before

      # test single insert event
      query = " ".join(["insert into `{}`.`{}`", "partition (year=2023)"
        if is_partitioned else '', values])
      verify_skipping_hive_stmt_events(
        query.format(unique_database, table_name), table_name)
      # test batch insert events
      query = " ".join(["insert into `{}`.`{}`", "partition (year=2023)"
        if is_partitioned else '', values, ";"])
      complete_query = ""
      for _ in range(3):
        complete_query += query.format(unique_database, table_name)
      verify_skipping_hive_stmt_events(complete_query, table_name)
      # Dynamic partitions test
      query = " ".join(["create", "table `{}`.`{}` (i int)",
        " partitioned by (year int) " if is_partitioned else '',
        self._get_transactional_tblproperties(is_transactional)])
      self.client.execute(query.format(unique_database, "new_table"))
      complete_query = "insert overwrite table `{db}`.`{tbl1}` " \
        "select * from `{db}`.`{tbl2}`"\
        .format(db=unique_database, tbl1="new_table", tbl2=table_name)
      verify_skipping_hive_stmt_events(complete_query, "new_table")
      # Drop the tables before running another test
      self.client.execute("drop table {}.{}".format(unique_database, table_name))
      self.client.execute("drop table {}.{}".format(unique_database, "new_table"))
    verify_skipping_older_events(test_old_table, False, False)
    verify_skipping_older_events(test_old_table, True, False)
    verify_skipping_older_events(test_old_table, False, True)
    verify_skipping_older_events(test_old_table, True, True)

  @CustomClusterTestSuite.with_args(
    catalogd_args="--enable_sync_to_latest_event_on_ddls=true "
                  "--debug_actions=catalogd_get_filtered_events_delay:SLEEP@3000 ")
  def test_skipping_batching_events(self, unique_database):
    """Test to verify IMPALA-10949, improving batching logic for partition events.
    Before batching the events, each event is checked if the event id is greater than
    table's lastSyncEventId then the event can be batched else it can be skipped."""
    # Print trace logs from DebugUtils.
    self.cluster.catalogd.set_jvm_log_level("org.apache.impala.util.DebugUtils", "trace")
    test_batch_table = "test_batch_table"
    self.client.execute(
      "create table {}.{} like functional.alltypes"
      .format(unique_database, test_batch_table))
    self.client.execute(
      "insert into {}.{} partition (year,month) select * from functional.alltypes"
      .format(unique_database, test_batch_table))
    # Generate batch ALTER_PARTITION events
    self.run_stmt_in_hive(
      "analyze table {}.{} compute statistics".format(unique_database, test_batch_table))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_metric = "batch-events-created"
    batch_events_1 = EventProcessorUtils.get_int_metric(batch_events_metric)
    prev_skipped_events = EventProcessorUtils.get_int_metric("events-skipped")
    self.run_stmt_in_hive(
      "analyze table {}.{} compute statistics".format(unique_database, test_batch_table))
    self.client.execute("refresh {0}.{1}".format(unique_database, test_batch_table))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_2 = EventProcessorUtils.get_int_metric(batch_events_metric)
    current_skipped_events = EventProcessorUtils.get_int_metric("events-skipped")
    # Make sure no new batch events are created
    assert batch_events_2 == batch_events_1
    assert current_skipped_events - prev_skipped_events >= 24

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_commit_compaction_events(self, unique_database):
    """Test is to verify Impala-11626, commit compaction events triggered in HMS would
    be consumed by CatalogD's event processor.
    """

    # Test scenario 1: partitioned table
    test_cc_part_table = "test_cc_partitioned_table"
    self.run_stmt_in_hive(
      "create transactional table {}.{} (i int) partitioned by (year int)"
      .format(unique_database, test_cc_part_table))
    for i in range(2):
        self.run_stmt_in_hive(
          "insert into {}.{} partition (year=2022) values (1),(2),(3)"
          .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    parts_refreshed_before_compaction = EventProcessorUtils.get_int_metric(
      "partitions-refreshed")
    self.client.execute(
      "select * from {}.{} limit 2"
      .format(unique_database, test_cc_part_table))
    self.run_stmt_in_hive(
      "alter table {}.{} partition(year=2022) compact 'minor' and wait"
      .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    parts_refreshed_after_compaction = EventProcessorUtils.get_int_metric(
      "partitions-refreshed")
    assert parts_refreshed_after_compaction > parts_refreshed_before_compaction

    # Test scenario 2:
    test_cc_unpart_tab = "test_cc_unpart_table"
    self.run_stmt_in_hive(
      "create transactional table {}.{} (i int)"
      .format(unique_database, test_cc_unpart_tab))
    for i in range(2):
        self.run_stmt_in_hive(
          "insert into {}.{} values (1),(2),(3)"
          .format(unique_database, test_cc_unpart_tab))
    EventProcessorUtils.wait_for_event_processing(self)
    tables_refreshed_before_compaction = EventProcessorUtils.get_int_metric(
      "tables-refreshed")
    self.client.execute(
      "select * from {}.{} limit 2"
      .format(unique_database, test_cc_unpart_tab))
    self.run_stmt_in_hive("alter table {}.{} compact 'minor' and wait"
      .format(unique_database, test_cc_unpart_tab))
    EventProcessorUtils.wait_for_event_processing(self)
    tables_refreshed_after_compaction = EventProcessorUtils.get_int_metric(
      "tables-refreshed")
    assert tables_refreshed_after_compaction > tables_refreshed_before_compaction

    # Test scenario 3: partitioned table has partition deleted
    test_cc_part_table = "test_cc_partitioned_table_error"
    self.run_stmt_in_hive(
      "create transactional table {}.{} (i int) partitioned by (year int)"
      .format(unique_database, test_cc_part_table))
    for i in range(2):
        self.run_stmt_in_hive(
          "insert into {}.{} partition (year=2022) values (1),(2),(3)"
          .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    self.client.execute(
      "select * from {}.{} limit 2"
      .format(unique_database, test_cc_part_table))
    self.run_stmt_in_hive(
      "alter table {}.{} partition(year=2022) compact 'minor' and wait"
      .format(unique_database, test_cc_part_table))
    self.run_stmt_in_hive("alter table {}.{} Drop if exists partition(year=2022)"
      .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    # Test scenario 4: process commit compaction for an unloaded table
    test_cc_part_table = "test_cc_table_unloaded"
    self.run_stmt_in_hive(
      "create transactional table {}.{} (i int) partitioned by (year int)"
      .format(unique_database, test_cc_part_table))
    for i in range(2):
        self.run_stmt_in_hive(
          "insert into {}.{} partition (year=2022) values (1),(2),(3)"
          .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    self.run_stmt_in_hive(
      "alter table {}.{} partition(year=2022) compact 'minor' and wait"
      .format(unique_database, test_cc_part_table))
    EventProcessorUtils.wait_for_event_processing(self)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_event_batching(self, unique_database):
    """Runs queries which generate multiple ALTER_PARTITION events which must be
    batched by events processor. Runs as a custom cluster test to isolate the metric
    values from other tests."""
    testtbl = "test_event_batching"
    test_acid_tbl = "test_event_batching_acid"
    acid_props = self._get_transactional_tblproperties(True)
    # create test tables
    self.client.execute(
      "create table {}.{} like functional.alltypes".format(unique_database, testtbl))
    self.client.execute(
      "insert into {}.{} partition (year,month) select * from functional.alltypes".format(
        unique_database, testtbl))
    self.client.execute(
      "create table {}.{} (id int) partitioned by (year int, month int) {}".format(
        unique_database, test_acid_tbl, acid_props))
    self.client.execute(
      "insert into {}.{} partition (year, month) "
      "select id, year, month from functional.alltypes".format(unique_database,
        test_acid_tbl))
    # run compute stats from impala; this should generate 24 ALTER_PARTITION events which
    # should be batched together into 1 or more number of events.
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_metric = "batch-events-created"
    batch_events_1 = EventProcessorUtils.get_int_metric(batch_events_metric)
    self.client.execute("compute stats {}.{}".format(unique_database, testtbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_2 = EventProcessorUtils.get_int_metric(batch_events_metric)
    assert batch_events_2 > batch_events_1
    # run analyze stats event from hive which generates ALTER_PARTITION event on each
    # partition of the table
    self.run_stmt_in_hive(
      "analyze table {}.{} compute statistics".format(unique_database, testtbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_3 = EventProcessorUtils.get_int_metric(batch_events_metric)
    assert batch_events_3 > batch_events_2
    # in case of transactional table since we batch the events together, the number of
    # tables refreshed must be far lower than number of events generated
    num_table_refreshes_1 = EventProcessorUtils.get_int_metric(
      "tables-refreshed")
    self.client.execute("compute stats {}.{}".format(unique_database, test_acid_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_4 = EventProcessorUtils.get_int_metric(batch_events_metric)
    num_table_refreshes_2 = EventProcessorUtils.get_int_metric(
      "tables-refreshed")
    # we should generate atleast 1 batch event if not more due to the 24 consecutive
    # ALTER_PARTITION events
    assert batch_events_4 > batch_events_3
    # table should not be refreshed since this is a self-event
    assert num_table_refreshes_2 == num_table_refreshes_1
    self.run_stmt_in_hive(
      "analyze table {}.{} compute statistics".format(unique_database, test_acid_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_5 = EventProcessorUtils.get_int_metric(batch_events_metric)
    assert batch_events_5 > batch_events_4
    num_table_refreshes_2 = EventProcessorUtils.get_int_metric("tables-refreshed")
    # the analyze table from hive generates 24 ALTER_PARTITION events which should be
    # batched into 1-2 batches (depending on timing of the event poll thread).
    assert num_table_refreshes_2 > num_table_refreshes_1
    assert int(num_table_refreshes_2) - int(num_table_refreshes_1) < 24
    EventProcessorUtils.wait_for_event_processing(self)
    # test for batching of insert events
    batch_events_insert = EventProcessorUtils.get_int_metric(batch_events_metric)
    tables_refreshed_insert = EventProcessorUtils.get_int_metric("tables-refreshed")
    partitions_refreshed_insert = EventProcessorUtils.get_int_metric(
      "partitions-refreshed")
    self.client.execute(
      "insert into {}.{} partition (year,month) select * from functional.alltypes".format(
        unique_database, testtbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_after_insert = EventProcessorUtils.get_int_metric(batch_events_metric)
    tables_refreshed_after_insert = EventProcessorUtils.get_int_metric("tables-refreshed")
    partitions_refreshed_after_insert = EventProcessorUtils.get_int_metric(
      "partitions-refreshed")
    # this is a self-event tables or partitions should not be refreshed
    assert batch_events_after_insert > batch_events_insert
    assert tables_refreshed_after_insert == tables_refreshed_insert
    assert partitions_refreshed_after_insert == partitions_refreshed_insert
    # run the insert from hive to make sure that batch event is refreshing all the
    # partitions
    self.run_stmt_in_hive(
      "SET hive.exec.dynamic.partition.mode=nonstrict; insert into {}.{} partition"
      " (year,month) select * from functional.alltypes".format(
        unique_database, testtbl))
    EventProcessorUtils.wait_for_event_processing(self)
    batch_events_after_hive = EventProcessorUtils.get_int_metric(batch_events_metric)
    partitions_refreshed_after_hive = EventProcessorUtils.get_int_metric(
      "partitions-refreshed")
    assert batch_events_after_hive > batch_events_insert
    # 24 partitions inserted and hence we must refresh 24 partitions once.
    assert int(partitions_refreshed_after_hive) == int(partitions_refreshed_insert) + 24

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=5")
  def test_event_processor_failure_extra_space(self, unique_database):
    """This test verifies that impala event processor is in active state after
    processing a couple of previously erroneous events"""
    test_table = "extra_space_table"
    # IMPALA-11939 -- create table event in HMS contains extra spaces in the db/table
    self.run_stmt_in_hive("create table ` {}`.`{} ` (i1 int) partitioned by (year int)"
      .format(unique_database, test_table))
    self.run_stmt_in_hive("alter table ` {}`.`{} ` add columns (i2 int)"
      .format(unique_database, test_table))
    EventProcessorUtils.wait_for_event_processing(self)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=5")
  def test_disable_hms_sync(self, unique_database):
    """This test verifies that impala event processor is in active state after
    processing an alter table event that re-enables hms sync"""
    # test 1: re-enable disableHmsSync config at table level
    test_table = "disable_hms_sync_table"
    self.client.execute(
      """create table {}.{} (i int) TBLPROPERTIES ('impala.disableHmsSync'='true')"""
      .format(unique_database, test_table))
    EventProcessorUtils.wait_for_event_processing(self)
    prev_events_skipped = EventProcessorUtils.get_int_metric('events-skipped', 0)
    self.run_stmt_in_hive(
      """ALTER TABLE {}.{} SET TBLPROPERTIES('somekey'='somevalue')"""
      .format(unique_database, test_table))
    self.client.execute(
      """ALTER TABLE {}.{} SET TBLPROPERTIES ('impala.disableHmsSync'='false')"""
      .format(unique_database, test_table))
    EventProcessorUtils.wait_for_event_processing(self)
    current_events_skipped = EventProcessorUtils.get_int_metric('events-skipped', 0)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    assert current_events_skipped >= prev_events_skipped + 1

    # test 2: re-enabling disableHmsSync config on a table shouldn't put event processor
    # in error state if the database is not loaded.
    try:
      test_db = "unloaded_db_sync"
      self.run_stmt_in_hive("""create database {}""".format(test_db))
      self.run_stmt_in_hive("""create table {}.{} (id int)
        TBLPROPERTIES ('impala.disableHmsSync'='true')""".format(test_db, test_table))
      self.run_stmt_in_hive(
        """ALTER TABLE {}.{} SET TBLPROPERTIES ('impala.disableHmsSync'='false')"""
        .format(test_db, test_table))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    finally:
      self.run_stmt_in_hive("""drop database {} cascade""".format(test_db))

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=10")
  def test_event_processor_dropped_partition(self, unique_database):
    """This test verifies that impala event processor is in active state after
    processing partitioned insert events of a dropped table"""
    # IMPALA-11768 -- Insert partition events should be ignored
    # if the table is dropped
    test_table = "partitioned_table"

    def is_event_processor_active(is_insert):
      self.run_stmt_in_hive("create table {}.{} (i1 int) partitioned by (year int)"
        .format(unique_database, test_table))
      EventProcessorUtils.wait_for_event_processing(self)
      self.client.execute("refresh {}.{}".format(unique_database, test_table))
      self.run_stmt_in_hive(
        "insert into {}.{} partition(year=2023) values (4),(5),(6)"
        .format(unique_database, test_table))
      data = FireEventRequestData()
      if is_insert:
        insert_data = InsertEventRequestData()
        insert_data.filesAdded = "/warehouse/mytable/b1"
        insert_data.replace = False
        data.insertData = insert_data
      else:
        data.refreshEvent = True
      req = FireEventRequest(True, data)
      req.dbName = unique_database
      req.tableName = test_table
      req.partitionVals = ["2023"]
      self.hive_client.fire_listener_event(req)
      self.run_stmt_in_hive(
        "drop table {}.{}".format(unique_database, test_table))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    is_event_processor_active(True)
    is_event_processor_active(False)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_iceberg_self_events(self, unique_database):
    """This test checks that Impala doesn't refresh Iceberg tables on self events."""
    tbl_name = unique_database + ".test_iceberg_events"
    iceberg_catalogs = IcebergCatalogs(unique_database)

    def check_self_events(query, skips_events=True):
      tbls_refreshed_before, partitions_refreshed_before, \
          events_skipped_before = self._get_self_event_metrics()
      self.client.execute(query)
      EventProcessorUtils.wait_for_event_processing(self)
      tbls_refreshed_after, partitions_refreshed_after, \
          events_skipped_after = self._get_self_event_metrics()
      assert tbls_refreshed_before == tbls_refreshed_after
      assert partitions_refreshed_before == partitions_refreshed_after
      if skips_events:
        assert events_skipped_after > events_skipped_before

    for catalog in iceberg_catalogs.get_iceberg_catalog_properties():
      is_hive_catalog = iceberg_catalogs.is_a_hive_catalog(catalog)
      self.client.execute("""
          CREATE TABLE {0} (i int) STORED AS ICEBERG
          TBLPROPERTIES ({1})""".format(tbl_name, catalog))

      check_self_events("INSERT OVERWRITE {0} VALUES (1)".format(tbl_name),
          skips_events=is_hive_catalog)
      check_self_events("ALTER TABLE {0} ADD COLUMN j INT".format(tbl_name))
      check_self_events("ALTER TABLE {0} DROP COLUMN i".format(tbl_name))
      check_self_events("ALTER TABLE {0} CHANGE COLUMN j j BIGINT".format(tbl_name))
      # SET PARTITION SPEC only updates HMS in case of HiveCatalog (which sets
      # table property 'metadata_location')
      check_self_events(
          "ALTER TABLE {0} SET PARTITION SPEC (truncate(2, j))".format(tbl_name),
          skips_events=is_hive_catalog)
      check_self_events(
          "ALTER TABLE {0} SET TBLPROPERTIES('key'='value')".format(tbl_name))
      check_self_events("ALTER TABLE {0} UNSET TBLPROPERTIES('key')".format(tbl_name))
      check_self_events("INSERT INTO {0} VALUES (2), (3), (4)".format(tbl_name),
          skips_events=is_hive_catalog)
      ctas_tbl = unique_database + ".ice_ctas"
      check_self_events("""CREATE TABLE {0} STORED AS ICEBERG
          TBLPROPERTIES ({1}) AS SELECT * FROM {2}""".format(ctas_tbl, catalog, tbl_name))
      check_self_events("DROP TABLE {0}".format(ctas_tbl))
      check_self_events("TRUNCATE TABLE {0}".format(tbl_name),
          skips_events=is_hive_catalog)

      self.client.execute("DROP TABLE {0}".format(tbl_name))

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=5")
  def test_stale_drop_partition_events(self, unique_database):
    """Regression Tests for IMPALA-12256. Verifies stale DROP_PARTITION events are
    skipped even if they are processed late after some other DDLs. Uses a higher polling
    interval to ensure late processing on the events"""
    self.client.execute(
        "create table %s.part(i int) partitioned by (p int) stored as textfile"
        % unique_database)
    self.client.execute(
        "insert into %s.part partition (p=0) values (0)" % unique_database)
    # These DDLs will reload the partition metadata. We will verify they don't lose
    # the create event ids after the reload.
    partition_ddls = [
      "compute stats %s.part" % unique_database,
      "compute incremental stats %s.part" % unique_database,
      "compute incremental stats %s.part partition(p=0)" % unique_database,
      "alter table %s.part partition(p=0) set row format"
      "  delimited fields terminated by ','" % unique_database,
      "alter table %s.part partition(p=0) set fileformat parquet" % unique_database,
      "alter table %s.part partition(p=0) set location '/tmp'" % unique_database,
      "alter table %s.part partition(p=0) set tblproperties('k'='v')" % unique_database,
      "refresh %s.part partition(p=0)" % unique_database,
      "refresh %s.part" % unique_database,
    ]
    # Wait until the events in preparing the table are consumed.
    EventProcessorUtils.wait_for_event_processing(self)
    parts_added_before = EventProcessorUtils.get_int_metric("partitions-added")
    parts_refreshed_before = EventProcessorUtils.get_int_metric("partitions-refreshed")
    parts_removed_before = EventProcessorUtils.get_int_metric("partitions-removed")
    for ddl in partition_ddls:
      events_skipped_before = EventProcessorUtils.get_int_metric("events-skipped")
      # Drop-create the partition and then runs a DDL on it. A DROP_PARTITION and an
      # ADD_PARTITION event will be generated and should be skipped. The 3rd DDL might
      # generate an ALTER_PARTITION event but it should be skipped as self-event.
      # Note that we don't perform self-event detection on ADD/DROP_PARTITION events.
      # They are skipped based on the partition level create event ids. So we should see
      # no partitions are added/removed/refreshed if we correctly track the create event
      # id (saved by the 2nd DDL that creates the partition).
      # For the DROP_PARTITION event, there are 3 cases:
      # 1) The DROP_PARTITION event is processed before the INSERT statement.
      #    It's skipped since the partition doesn't exist.
      # 2) The DROP_PARTITION event is processed after the INSERT statement
      #    and before the 3rd DDL. The INSERT statement creates the partition so saves
      #    the create event id which is higher than the id of the DROP_PARTITION event.
      #    Thus the DROP_PARTITION event is skipped.
      # 3) The DROP_PARTITION event is processed after the 3rd DDL. The reload triggered
      #    by the DDL should keep track of the create event id so the DROP_PARTITION event
      #    can be skipped.
      # This test sets hms_event_polling_interval_s to 5 which is long enough for the
      # 3 DDLs to finish. So it's more likely the 3rd case would happen, which is the
      # case of IMPALA-12256.
      self.client.execute(
          "alter table %s.part drop partition (p=0)" % unique_database)
      self.client.execute(
          "insert into %s.part partition(p=0) values (1),(2)" % unique_database)
      self.client.execute(ddl)
      EventProcessorUtils.wait_for_event_processing(self)
      events_skipped_after = EventProcessorUtils.get_int_metric("events-skipped")
      parts_added_after = EventProcessorUtils.get_int_metric("partitions-added")
      parts_refreshed_after = EventProcessorUtils.get_int_metric("partitions-refreshed")
      parts_removed_after = EventProcessorUtils.get_int_metric("partitions-removed")
      # Event-processor should not update any partitions since all events should be
      # skipped
      assert parts_removed_before == parts_removed_after
      assert parts_added_before == parts_added_after
      assert parts_refreshed_before == parts_refreshed_after
      assert events_skipped_after > events_skipped_before

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=5")
  def test_truncate_table_from_hive(self, unique_database):
    """IMPALA-12636: verify truncate table from hive reloads file metadata in Impala"""
    hive_tbl = "tbl_in_hive"
    values = "values (10),(20),(30)"

    def verify_truncate_op_in_hive(tbl_name, is_transactional, is_partitioned,
        is_batched):
      create_query = " ".join(["create", "table `{}`.`{}` (i int)",
        " partitioned by (year int) " if is_partitioned else '',
          self._get_transactional_tblproperties(is_transactional)])
      self.execute_query(create_query.format(unique_database, tbl_name))
      insert_query = " ".join(["insert into `{}`.`{}`", "partition (year=2024)"
        if is_partitioned else '', values])
      self.run_stmt_in_hive(insert_query.format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      self.client.execute("refresh {}.{}".format(unique_database, tbl_name))
      truncate_query = " ".join(["truncate table `{}`.`{}`", "partition (year=2024)"
        if is_partitioned else ''])
      self.run_stmt_in_hive(truncate_query.format(unique_database, tbl_name))
      if is_batched:
        self.run_stmt_in_hive(
          "insert into {}.{} partition (year=2024) values (1),(2)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      data = int(self.execute_scalar("select count(*) from {0}.{1}".format(
        unique_database, tbl_name)))
      assert data == 2 if is_batched else data == 0
      self.client.execute("drop table {}.{}".format(unique_database, tbl_name))
    # Case-I: truncate single partition
    verify_truncate_op_in_hive(hive_tbl, False, False, False)
    verify_truncate_op_in_hive(hive_tbl, True, False, False)
    verify_truncate_op_in_hive(hive_tbl, False, True, False)
    verify_truncate_op_in_hive(hive_tbl, False, True, True)
    verify_truncate_op_in_hive(hive_tbl, True, True, False)
    verify_truncate_op_in_hive(hive_tbl, True, True, True)

    # Case-II: truncate partition in multi partition
    hive_tbl = "multi_part_tbl"
    self.client.execute("create table {}.{} (i int) partitioned by "
      "(p int, q int)".format(unique_database, hive_tbl))
    self.client.execute("insert into {}.{} partition(p, q) values "
      "(0,0,0), (0,0,1), (0,0,2)".format(unique_database, hive_tbl))
    self.client.execute("insert into {}.{} partition(p, q) values "
      "(0,1,0), (0,1,1)".format(unique_database, hive_tbl))
    self.run_stmt_in_hive("truncate table {}.{} partition(p=0)"
      .format(unique_database, hive_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    data = int(self.execute_scalar("select count(*) from {0}.{1}".format(
      unique_database, hive_tbl)))
    assert data == 2
    self.run_stmt_in_hive("truncate table {}.{}"
      .format(unique_database, hive_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    data = int(self.execute_scalar("select count(*) from {0}.{1}".format(
      unique_database, hive_tbl)))
    assert data == 0

  @SkipIf.is_test_jdk
  @CustomClusterTestSuite.with_args(
      catalogd_args="--hms_event_polling_interval_s=100",
      hive_conf_dir=HIVE_SITE_HOUSEKEEPING_ON)
  def test_commit_compaction_with_abort_txn(self, unique_database):
    """Use a long enough polling interval to allow Hive statements to finish before
       the ABORT_TXN event is processed. In local tests, the Hive statements usually
       finish in 60s.
       TODO: improve this by adding commands to pause and resume the event-processor."""
    tbl = "part_table"
    fq_tbl = unique_database + '.' + tbl
    acid = AcidTxn(self.hive_client)
    self.run_stmt_in_hive(
        "create transactional table {} (i int) partitioned by (p int)".format(fq_tbl))

    # Allocate a write id on this table and abort the txn
    txn_id = acid.open_txns()
    acid.allocate_table_write_ids(txn_id, unique_database, tbl)
    acid.abort_txn(txn_id)

    # Insert some rows and trigger compaction
    for i in range(2):
      self.run_stmt_in_hive(
          "insert into {} partition(p=0) values (1),(2),(3)".format(fq_tbl))
    self.run_stmt_in_hive(
        "alter table {} partition(p=0) compact 'major' and wait".format(fq_tbl))

    # The CREATE_TABLE event hasn't been processed yet so we have to explictily invalidate
    # the table first.
    self.client.execute("invalidate metadata " + fq_tbl)
    # Reload the table so the latest valid writeIdList is loaded
    self.client.execute("refresh " + fq_tbl)
    # Process the ABORT_TXN event
    EventProcessorUtils.wait_for_event_processing(self, timeout=100)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    # Uncomment this once we can stop and resume the event-processor using commands.
    # Currently the test is flaky with it since the Hive statements could take longer to
    # finish than 100s (e.g. I saw a run of 5mins).
    # self.assert_catalogd_log_contains("INFO", "Not added ABORTED write id 1 since it's "
    #    + "not opened and might already be cleaned up")

  @CustomClusterTestSuite.with_args(
      catalogd_args="--hms_event_incremental_refresh_transactional_table=false")
  def test_no_hms_event_incremental_refresh_transactional_table(self, unique_database):
    """IMPALA-12835: Test that Impala notices inserts to acid tables when
       hms_event_incremental_refresh_transactional_table is false.
    """
    for partitioned in [False, True]:
      tbl = "part_tbl" if partitioned else "tbl"
      fq_tbl = unique_database + '.' + tbl
      part_create = " partitioned by (p int)" if partitioned else ""
      part_insert = " partition (p = 1)" if partitioned else ""

      self.run_stmt_in_hive(
          "create transactional table {} (i int){}".format(fq_tbl, part_create))
      EventProcessorUtils.wait_for_event_processing(self)

      # Load the table in Impala before INSERT
      self.client.execute("refresh " + fq_tbl)
      self.run_stmt_in_hive(
          "insert into {}{} values (1),(2),(3)".format(fq_tbl, part_insert))
      EventProcessorUtils.wait_for_event_processing(self)

      results = self.client.execute("select i from " + fq_tbl)
      assert results.data == ["1", "2", "3"]

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=5")
  def test_invalidate_better_create_event_id(self, unique_database):
    """This test should set better create event id for invalidate table"""
    test_tbl = "test_invalidate_table"
    self.client.execute("create table {}.{} (i int)".format(unique_database, test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    tables_removed_before = EventProcessorUtils.get_int_metric("tables-removed")
    self.client.execute("drop table {}.{}".format(unique_database, test_tbl))
    self.run_stmt_in_hive(
        "create table {}.{} (i int, j int)".format(unique_database, test_tbl))
    self.client.execute("invalidate metadata {}.{}".format(unique_database, test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    tables_removed_after = EventProcessorUtils.get_int_metric("tables-removed")
    assert tables_removed_after == tables_removed_before


@SkipIfFS.hive
class TestEventProcessingWithImpala(TestEventProcessingCustomConfigsBase):
  """This class contains tests that exercise the event processing mechanism in the
  catalog for non-default configurations"""

  @classmethod
  def add_test_dimensions(cls):
    super(TestEventProcessingWithImpala, cls).add_test_dimensions()

    # Inject 2s sleep at catalogd to interleave with 1s poll period of
    # HMS Event Processor.
    add_exec_option_dimension(
        cls, 'debug_action', [None, 'catalogd_load_metadata_delay:SLEEP@2000'])
    add_exec_option_dimension(cls, 'sync_ddl', [0, 1])

    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('sync_ddl') == 0 or v.get_value('debug_action') is not None)

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=1")
  def test_self_events_with_impala(self, vector, unique_database):
    """Runs multiple queries which generate events using impala as client"""
    # Print trace logs from DebugUtils.
    self.cluster.catalogd.set_jvm_log_level("org.apache.impala.util.DebugUtils", "trace")
    self._run_self_events_test(unique_database, vector.get_value('exec_option'),
        use_impala=True)
