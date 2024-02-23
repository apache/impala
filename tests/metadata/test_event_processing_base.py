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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfCatalogV2
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfFS.hive
@SkipIfCatalogV2.hms_event_polling_disabled()
class TestEventProcessingBase(ImpalaTestSuite):

  @classmethod
  def _run_test_insert_events_impl(cls, hive_client, impala_client, impala_cluster,
      unique_database, is_transactional=False):
    """Test for insert event processing. Events are created in Hive and processed in
    Impala. The following cases are tested :
    Insert into table --> for partitioned and non-partitioned table
    Insert overwrite table --> for partitioned and non-partitioned table
    Insert into partition --> for partitioned table
    """
    # Test table with no partitions.
    tbl_insert_nopart = 'tbl_insert_nopart'
    cls.run_stmt_in_hive(
      "drop table if exists %s.%s" % (unique_database, tbl_insert_nopart))
    tblproperties = ""
    if is_transactional:
      tblproperties = "tblproperties ('transactional'='true'," \
          "'transactional_properties'='insert_only')"
    cls.run_stmt_in_hive("create table %s.%s (id int, val int) %s"
        % (unique_database, tbl_insert_nopart, tblproperties))
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Test CTAS and insert by Impala with empty results (IMPALA-10765).
    cls.execute_query_expect_success(impala_client,
        "create table {db}.ctas_tbl {prop} as select * from {db}.{tbl}"
        .format(db=unique_database, tbl=tbl_insert_nopart, prop=tblproperties))
    cls.execute_query_expect_success(impala_client,
        "insert into {db}.ctas_tbl select * from {db}.{tbl}"
        .format(db=unique_database, tbl=tbl_insert_nopart))
    # Test insert into table, this will fire an insert event.
    cls.run_stmt_in_hive("insert into %s.%s values(101, 200)"
        % (unique_database, tbl_insert_nopart))
    # With MetastoreEventProcessor running, the insert event will be processed. Query the
    # table from Impala.
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Verify that the data is present in Impala.
    data = cls.execute_scalar_expect_success(impala_client, "select * from %s.%s" %
        (unique_database, tbl_insert_nopart))
    assert data.split('\t') == ['101', '200']

    # Test insert overwrite. Overwrite the existing value.
    cls.run_stmt_in_hive("insert overwrite table %s.%s values(101, 201)"
        % (unique_database, tbl_insert_nopart))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Verify that the data is present in Impala.
    data = cls.execute_scalar_expect_success(impala_client, "select * from %s.%s" %
        (unique_database, tbl_insert_nopart))
    assert data.split('\t') == ['101', '201']
    # Test insert overwrite by Impala with empty results (IMPALA-10765).
    cls.execute_query_expect_success(impala_client,
        "insert overwrite {db}.{tbl} select * from {db}.ctas_tbl"
        .format(db=unique_database, tbl=tbl_insert_nopart))
    result = cls.execute_query_expect_success(impala_client,
        "select * from {db}.{tbl}".format(db=unique_database, tbl=tbl_insert_nopart))
    assert len(result.data) == 0

    # Test partitioned table.
    tbl_insert_part = 'tbl_insert_part'
    cls.run_stmt_in_hive("drop table if exists %s.%s"
        % (unique_database, tbl_insert_part))
    cls.run_stmt_in_hive("create table %s.%s (id int, name string) "
        "partitioned by(day int, month int, year int) %s"
        % (unique_database, tbl_insert_part, tblproperties))
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Test insert overwrite by Impala with empty results (IMPALA-10765).
    cls.execute_query_expect_success(impala_client,
        "create table {db}.ctas_part partitioned by (day, month, year) {prop} as "
        "select * from {db}.{tbl}".format(db=unique_database, tbl=tbl_insert_part,
            prop=tblproperties))
    cls.execute_query_expect_success(impala_client,
        "insert into {db}.ctas_part partition(day=0, month=0, year=0) select id, "
        "name from {db}.{tbl}".format(db=unique_database, tbl=tbl_insert_part))
    # Insert data into partitions.
    cls.run_stmt_in_hive(
        "insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(101, 'x')" % (unique_database, tbl_insert_part))
    # Make sure the event has been processed.
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Verify that the data is present in Impala.
    data = cls.execute_scalar_expect_success(impala_client,
        "select * from %s.%s" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['101', 'x', '28', '3', '2019']

    # Test inserting into existing partitions.
    cls.run_stmt_in_hive(
        "insert into %s.%s partition(day=28, month=03, year=2019)"
        "values(102, 'y')" % (unique_database, tbl_insert_part))
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Verify that the data is present in Impala.
    data = cls.execute_scalar_expect_success(impala_client,
        "select count(*) from %s.%s where day=28 and month=3 "
        "and year=2019" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['2']
    # Test inserting into existing partitions by Impala with empty results
    # (IMPALA-10765).
    cls.execute_query_expect_success(impala_client,
        "insert into {db}.{tbl} partition(day=28, month=03, year=2019) "
        "select id, name from {db}.ctas_part"
        .format(db=unique_database, tbl=tbl_insert_part))

    # Test insert overwrite into existing partitions
    cls.run_stmt_in_hive(
        "insert overwrite table %s.%s partition(day=28, month=03, "
        "year=2019)" "values(101, 'z')" % (unique_database, tbl_insert_part))
    EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
    # Verify that the data is present in Impala.
    data = cls.execute_scalar_expect_success(impala_client,
        "select * from %s.%s where day=28 and month=3 and"
        " year=2019 and id=101" % (unique_database, tbl_insert_part))
    assert data.split('\t') == ['101', 'z', '28', '3', '2019']
    # Test insert overwrite into existing partitions by Impala with empty results
    # (IMPALA-10765).
    cls.execute_query_expect_success(impala_client, "insert overwrite {db}.{tbl} "
                       "partition(day=28, month=03, year=2019) "
                       "select id, name from {db}.ctas_part"
                       .format(db=unique_database, tbl=tbl_insert_part))
    result = cls.execute_query_expect_success(impala_client, "select * from {db}.{tbl} "
                                "where day=28 and month=3 and year=2019"
                                .format(db=unique_database, tbl=tbl_insert_part))
    assert len(result.data) == 0

  @classmethod
  def _run_event_based_replication_tests_impl(cls, hive_client, impala_client,
      impala_cluster, filesystem_client, transactional=True):
    """Hive Replication relies on the insert events generated on the tables.
    This test issues some basic replication commands from Hive and makes sure
    that the replicated table has correct data."""
    TBLPROPERTIES = cls._get_transactional_tblproperties(transactional)
    source_db = ImpalaTestSuite.get_random_name("repl_source_")
    target_db = ImpalaTestSuite.get_random_name("repl_target_")
    unpartitioned_tbl = "unpart_tbl"
    partitioned_tbl = "part_tbl"
    try:
      cls.run_stmt_in_hive("create database {0}".format(source_db))
      cls.run_stmt_in_hive(
        "alter database {0} set dbproperties ('repl.source.for'='xyz')"
        .format(source_db))
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      # explicit create table command since create table like doesn't allow tblproperties
      impala_client.execute("create table {0}.{1} (a string, b string) stored as parquet"
        " {2}".format(source_db, unpartitioned_tbl, TBLPROPERTIES))
      impala_client.execute(
        "create table {0}.{1} (id int, bool_col boolean, tinyint_col tinyint, "
        "smallint_col smallint, int_col int, bigint_col bigint, float_col float, "
        "double_col double, date_string string, string_col string, "
        "timestamp_col timestamp) partitioned by (year int, month int) stored as parquet"
        " {2}".format(source_db, partitioned_tbl, TBLPROPERTIES))

      # case I: insert
      # load the table with some data from impala, this also creates new partitions.
      impala_client.execute("insert into {0}.{1}"
        " select * from functional.tinytable".format(source_db,
          unpartitioned_tbl))
      impala_client.execute("insert into {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      rows_in_unpart_tbl = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(source_db, unpartitioned_tbl)).split('\t')[
        0])
      rows_in_part_tbl = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(source_db, partitioned_tbl))
        .split('\t')[0])
      assert rows_in_unpart_tbl > 0
      assert rows_in_part_tbl > 0
      # bootstrap the replication
      cls.run_stmt_in_hive("repl dump {0}".format(source_db))
      # create a target database where tables will be replicated
      impala_client.execute("create database {0}".format(target_db))
      # replicate the table from source to target
      cls.run_stmt_in_hive("repl load {0} into {1}".format(source_db,
        target_db))
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      assert unpartitioned_tbl in impala_client.execute(
        "show tables in {0}".format(target_db)).get_data()
      assert partitioned_tbl in impala_client.execute(
        "show tables in {0}".format(target_db)).get_data()
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl))
          .split('\t')[0])
      assert rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert rows_in_part_tbl == rows_in_part_tbl_target

      # case II: insert into existing partitions.
      impala_client.execute("insert into {0}.{1}"
        " select * from functional.tinytable".format(
          source_db, unpartitioned_tbl))
      impala_client.execute("insert into {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      cls.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      cls.run_stmt_in_hive("repl load {0} into {1}".format(source_db,
        target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl))
        .split('\t')[0])
      assert 2 * rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert 2 * rows_in_part_tbl == rows_in_part_tbl_target

      # Case III: insert overwrite
      # impala does a insert overwrite of the tables.
      impala_client.execute("insert overwrite table {0}.{1}"
        " select * from functional.tinytable".format(
          source_db, unpartitioned_tbl))
      impala_client.execute("insert overwrite table {0}.{1} partition(year,month)"
        " select * from functional_parquet.alltypessmall".format(
          source_db, partitioned_tbl))
      cls.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      cls.run_stmt_in_hive("repl load {0} into {1}".format(source_db,
        target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl))
        .split('\t')[0])
      assert rows_in_unpart_tbl == rows_in_unpart_tbl_target
      assert rows_in_part_tbl == rows_in_part_tbl_target

      # Case IV: CTAS which creates a transactional table.
      impala_client.execute(
        "create table {0}.insertonly_nopart_ctas {1} as "
        "select * from {0}.{2}".format(source_db, TBLPROPERTIES, unpartitioned_tbl))
      impala_client.execute(
        "create table {0}.insertonly_part_ctas partitioned by (year, month) {1}"
        " as select * from {0}.{2}".format(source_db, TBLPROPERTIES, partitioned_tbl))
      cls.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      cls.run_stmt_in_hive("repl load {0} into {1}".format(source_db,
        target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_source = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from "
        "{0}.insertonly_nopart_ctas".format(source_db)).split('\t')[0])
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from "
        "{0}.insertonly_nopart_ctas".format(target_db)).split('\t')[0])
      assert rows_in_unpart_tbl_source == rows_in_unpart_tbl_target
      rows_in_unpart_tbl_source = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from "
        "{0}.insertonly_part_ctas".format(source_db)).split('\t')[0])
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from "
        "{0}.insertonly_part_ctas".format(target_db)).split('\t')[0])
      assert rows_in_unpart_tbl_source == rows_in_unpart_tbl_target

      # Case V: truncate table
      # impala truncates both the tables. Make sure replication sees that.
      impala_client.execute("truncate table {0}.{1}".format(source_db,
        unpartitioned_tbl))
      impala_client.execute("truncate table {0}.{1}".format(source_db, partitioned_tbl))
      cls.run_stmt_in_hive("repl dump {0}".format(source_db))
      # replicate the table from source to target
      cls.run_stmt_in_hive("repl load {0} into {1}".format(source_db,
        target_db))
      # we wait until the events catch up in case repl command above did some HMS
      # operations.
      EventProcessorUtils.wait_for_event_processing_impl(hive_client, impala_cluster)
      # confirm the number of rows in target match with the source table.
      rows_in_unpart_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, unpartitioned_tbl))
          .split('\t')[0])
      rows_in_part_tbl_target = int(cls.execute_scalar_expect_success(impala_client,
        "select count(*) from {0}.{1}".format(target_db, partitioned_tbl))
        .split('\t')[0])
      assert rows_in_unpart_tbl_target == 0
      assert rows_in_part_tbl_target == 0
    finally:
      src_db = cls.__get_db_nothrow(source_db)
      target_db_obj = cls.__get_db_nothrow(target_db)
      if src_db is not None:
        cls.run_stmt_in_hive(
          "alter database {0} set dbproperties ('repl.source.for'='')".format(source_db))
        cls.run_stmt_in_hive("drop database if exists {0} cascade"
          .format(source_db))
      if target_db_obj is not None:
        cls.run_stmt_in_hive("drop database if exists {0} cascade"
          .format(target_db))
      # workaround for HIVE-24135. the managed db location doesn't get cleaned up
      if src_db is not None and src_db.managedLocationUri is not None:
        filesystem_client.delete_file_dir(src_db.managedLocationUri,
          True)
      if target_db_obj is not None and target_db_obj.managedLocationUri is not None:
        filesystem_client.delete_file_dir(
          target_db_obj.managedLocationUri, True)

  @classmethod
  def __get_db_nothrow(self, name):
    try:
      return self.hive_client.get_database(name)
    except Exception:
      return None

  @classmethod
  def _get_transactional_tblproperties(self, is_transactional):
    """
    Util method to generate the tblproperties for transactional tables
    """
    tblproperties = ""
    if is_transactional:
      tblproperties = "tblproperties ('transactional'='true'," \
          "'transactional_properties'='insert_only')"
    return tblproperties
