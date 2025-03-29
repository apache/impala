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
from subprocess import check_call
import logging
import pytest
import re
import time
import threading

from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    add_mandatory_exec_option)
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfHive2, SkipIfCatalogV2
from tests.common.test_vector import HS2
from tests.metadata.test_event_processing_base import TestEventProcessingBase
from tests.util.event_processor_utils import EventProcessorUtils

PROCESSING_TIMEOUT_S = 10
LOG = logging.getLogger(__name__)

@SkipIfFS.hive
@SkipIfCatalogV2.hms_event_polling_disabled()
class TestEventProcessing(ImpalaTestSuite):
  """This class contains tests that exercise the event processing mechanism in the
  catalog."""

  @classmethod
  def default_test_protocol(cls):
    return HS2

  @SkipIfHive2.acid
  def test_transactional_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for transactional tables.
    """
    TestEventProcessingBase._run_test_insert_events_impl(
        unique_database, is_transactional=True)

  def test_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    TestEventProcessingBase._run_test_insert_events_impl(unique_database)

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

  def test_hive_impala_iceberg_reloads(self, unique_database):
    test_tbl = unique_database + ".test_events"
    self.run_stmt_in_hive("create table {} (value string) \
        partitioned by (year int) stored by iceberg".format(test_tbl))
    EventProcessorUtils.wait_for_event_processing(self)
    self.execute_query("describe {}".format(test_tbl))

    self.run_stmt_in_hive("insert into {} values ('1', 2025)".format(test_tbl))
    self.run_stmt_in_hive("select * from {}".format(test_tbl))

    EventProcessorUtils.wait_for_event_processing(self)
    res = self.execute_query("select * from {}".format(test_tbl))

    assert ["1\t2025"] == res.data
    res = self.execute_query("refresh {}".format(test_tbl))
    assert "Iceberg table reload skipped as no change detected" in res.runtime_profile

  @SkipIfHive2.acid
  def test_empty_partition_events_transactional(self, unique_database):
    self._run_test_empty_partition_events(unique_database, True)

  def test_empty_partition_events(self, unique_database):
    self._run_test_empty_partition_events(unique_database, False)

  def test_event_based_replication(self):
    TestEventProcessingBase._run_event_based_replication_tests_impl(
        self.filesystem_client)

  def _run_test_empty_partition_events(self, unique_database, is_transactional):
    test_tbl = unique_database + ".test_events"
    TBLPROPERTIES = TestEventProcessingBase._get_transactional_tblproperties(
      is_transactional)
    self.run_stmt_in_hive("create table {0} (key string, value string) \
      partitioned by (year int) stored as parquet {1}".format(test_tbl, TBLPROPERTIES))
    self.client.set_configuration({
      "sync_hms_events_wait_time_s": PROCESSING_TIMEOUT_S,
      "sync_hms_events_strict_mode": True
    })
    self.client.execute("describe {0}".format(test_tbl))

    self.run_stmt_in_hive(
      "alter table {0} add partition (year=2019)".format(test_tbl))
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')

    self.run_stmt_in_hive(
      "alter table {0} add if not exists partition (year=2019)".format(test_tbl))
    assert [('2019',)] == self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop partition (year=2019)".format(test_tbl))
    assert ('2019') not in self.get_impala_partition_info(test_tbl, 'year')
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

    self.run_stmt_in_hive(
      "alter table {0} drop if exists partition (year=2019)".format(test_tbl))
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

  def test_transact_partition_location_change_from_hive(self, unique_database):
    """IMPALA-12356: Verify alter partition from hive on transactional table"""
    self.run_test_partition_location_change_from_hive(unique_database,
                                                      "transact_alter_part_hive", True)

  def test_partition_location_change_from_hive(self, unique_database):
    """IMPALA-12356: Verify alter partition from hive on non-transactional table"""
    self.run_test_partition_location_change_from_hive(unique_database, "alter_part_hive")

  def run_test_partition_location_change_from_hive(self, unique_database, tbl_name,
    is_transactional=False):
    fq_tbl_name = unique_database + "." + tbl_name
    TBLPROPERTIES = TestEventProcessingBase._get_transactional_tblproperties(
      is_transactional)
    # Create the table
    self.client.execute(
      "create table %s (i int) partitioned by(j int) stored as parquet %s"
      % (fq_tbl_name, TBLPROPERTIES))
    # Insert some data to a partition
    p1 = "j=1"
    self.client.execute("insert into table %s partition(%s) values (0),(1),(2)"
                        % (fq_tbl_name, p1))
    tbl_location = self._get_table_property("Location:", fq_tbl_name)
    partitions = self.get_impala_partition_info(fq_tbl_name, 'Location')
    assert [("{0}/{1}".format(tbl_location, p1),)] == partitions
    # Alter partition location from hive
    new_part_location = tbl_location + "/j=2"
    self.run_stmt_in_hive("alter table %s partition(%s) set location '%s'"
                          % (fq_tbl_name, p1, new_part_location))
    EventProcessorUtils.wait_for_event_processing(self)
    # Verify if the location is updated
    partitions = self.get_impala_partition_info(fq_tbl_name, 'Location')
    assert [(new_part_location,)] == partitions

  def _get_table_property(self, property_name, table_name):
    """Extract the table property value from output of DESCRIBE FORMATTED."""
    result = self.client.execute("describe formatted {0}".format(table_name))
    for row in result.data:
      if property_name in row:
        row = row.split('\t')
        if row[1] == 'NULL':
          break
        return row[1].rstrip()
    return None

  def _exec_and_check_ep_cmd(self, cmd, expected_status):
    cmd_output = self.execute_query(cmd).get_data()
    match = re.search(
      r"EventProcessor status: %s. LastSyncedEventId: \d+. LatestEventId: \d+." %
      expected_status,
      cmd_output)
    assert match, cmd_output
    assert EventProcessorUtils.get_event_processor_status() == expected_status
    return cmd_output

  @pytest.mark.execute_serially
  def test_event_processor_cmds(self, unique_database):
    ###########################################################################
    # 1. Test normal PAUSE and RESUME. Also check the STATUS command.
    self._exec_and_check_ep_cmd(":event_processor('pause')", "PAUSED")
    self._exec_and_check_ep_cmd(":event_processor('status')", "PAUSED")
    self._exec_and_check_ep_cmd(":event_processor('start')", "ACTIVE")
    self._exec_and_check_ep_cmd(":event_processor('status')", "ACTIVE")

    # Make sure the CREATE_DATABASE event for 'unique_database' is processed
    EventProcessorUtils.wait_for_event_processing(self)

    ###########################################################################
    # 2. Test failure of restarting at an older event id when status is ACTIVE
    last_synced_event_id = EventProcessorUtils.get_last_synced_event_id()
    e = self.execute_query_expect_failure(
        self.client, ":event_processor('start', %d)" % (last_synced_event_id / 2))
    assert "EventProcessor is active. Failed to set last synced event id from " +\
        str(last_synced_event_id) + " back to " + str(int(last_synced_event_id / 2)) +\
        ". Please pause EventProcessor first." in str(e)

    ###########################################################################
    # 3. Test restarting to the latest event id
    self._exec_and_check_ep_cmd(":event_processor('pause')", "PAUSED")
    # Create some HMS events
    for i in range(3):
      self.run_stmt_in_hive("create table %s.tbl_%d(i int)" % (unique_database, i))
    latest_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
    # Wait some time for EP to update its latest event id
    time.sleep(2)
    # Restart to the latest event id
    self._exec_and_check_ep_cmd(":event_processor('start', -1)", "ACTIVE")
    assert EventProcessorUtils.get_last_synced_event_id() == latest_event_id
    # Verify the new events are skipped so Impala queries should fail
    for i in range(3):
      self.execute_query_expect_failure(
          self.client, "describe %s.tbl_%d" % (unique_database, i))

    ###########################################################################
    # 4. Test setting back the last synced event id after pausing EP
    self._exec_and_check_ep_cmd(":event_processor('pause')", "PAUSED")
    # Restart to the previous last synced event id to process the missing HMS events
    self._exec_and_check_ep_cmd(
        ":event_processor('start', %d)" % last_synced_event_id, "ACTIVE")
    EventProcessorUtils.wait_for_event_processing(self)
    # Tables should be visible now
    for i in range(3):
      self.execute_query_expect_success(
          self.client, "describe %s.tbl_%d" % (unique_database, i))

    ###########################################################################
    # 5. Test unknown commands
    e = self.execute_query_expect_failure(self.client, ":event_processor('bad_cmd')")
    assert "Unknown command: BAD_CMD. Supported commands: PAUSE, START, STATUS" in str(e)

    ###########################################################################
    # 6. Test illegal event id
    e = self.execute_query_expect_failure(self.client, ":event_processor('start', -2)")
    assert "Illegal event id -2. Should be >= -1" in str(e)

    ###########################################################################
    # 7. Test restarting on a future event id
    cmd_output = self._exec_and_check_ep_cmd(
        ":event_processor('start', %d)" % (latest_event_id + 2), "ACTIVE")
    warning = ("Target event id %d is larger than the latest event id %d. Some future "
               "events will be skipped.") % (latest_event_id + 2, latest_event_id)
    assert warning in cmd_output
    # The cleanup method will drop 'unique_database' and tables in it, which generates
    # more than 2 self-events. It's OK for EP to skip them.


@SkipIfFS.hive
@SkipIfCatalogV2.hms_event_polling_disabled()
class TestEventSyncWaiting(ImpalaTestSuite):
  """Verify query option sync_hms_events_wait_time_s should protect the query by
     waiting until Impala sync the HMS changes."""

  @classmethod
  def get_workload(cls):
    return 'functional-planner'

  @classmethod
  def add_test_dimensions(cls):
    super(TestEventSyncWaiting, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    add_mandatory_exec_option(cls, 'sync_hms_events_wait_time_s', PROCESSING_TIMEOUT_S)
    add_mandatory_exec_option(cls, 'sync_hms_events_strict_mode', True)

  @pytest.mark.execute_serially
  def test_event_processor_pauses(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"

    # Create a table in Hive and submit a query on it when EP is paused.
    client.execute(":event_processor('pause')")
    self.run_stmt_in_hive("create table {} as select 1".format(tbl))

    # execute_async() is not really async that it returns after query planning finishes.
    # So we use execute_query_expect_success here and resume EP in a background thread.
    def resume_event_processor():
      time.sleep(2)
      client = self.create_impala_client_from_vector(vector)
      client.execute(":event_processor('start')")
    resume_ep_thread = threading.Thread(target=resume_event_processor)
    resume_ep_thread.start()
    res = self.execute_query_expect_success(client, "select * from " + tbl)
    assert res.data == ['1']

  def test_describe(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl_name = unique_database + ".tbl"
    # Test DESCRIBE on new table created in Hive
    self.run_stmt_in_hive(
        "create table {0} (i int) partitioned by (p int)".format(tbl_name))
    res = self.execute_query_expect_success(client, "describe " + tbl_name)
    assert res.data == ["i\tint\t", 'p\tint\t']
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

  def test_show_tables(self, vector, unique_database):
    # Test SHOW TABLES gets new tables created in Hive
    client = self.create_impala_client_from_vector(vector)
    tbl_name = unique_database + ".tbl"
    self.run_stmt_in_hive("create table {0} (i int)".format(tbl_name))
    res = self.execute_query_expect_success(client, "show tables in " + unique_database)
    assert res.data == ["tbl"]
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

    # Test SHOW VIEWS gets new views created in Hive
    self.run_stmt_in_hive(
        "create view {0}.v as select * from {1}".format(unique_database, tbl_name))
    res = self.execute_query_expect_success(client, "show views in " + unique_database)
    assert res.data == ["v"]
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

  def test_drop_created_table(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    self.run_stmt_in_hive("create table {0}.tbl(i int)".format(unique_database))
    self.execute_query_expect_success(
        client, "drop table {0}.tbl".format(unique_database))

  def test_insert_under_missing_db(self, vector, unique_name):
    client = self.create_impala_client_from_vector(vector)
    db = unique_name + "_db"
    try:
      # Create the table in Hive immediately after creating the db. So it's more likely
      # that when the INSERT is submitted, the db is still missing in Impala.
      self.run_stmt_in_hive("""create database {0};
          create table {0}.tbl(i int)""".format(db))
      self.execute_query_expect_success(
          client, "insert into {0}.tbl values (0)".format(db))
    finally:
      self.run_stmt_in_hive(
          "drop database if exists {0} cascade".format(db))

  def test_show_databases(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    res = self.execute_query_expect_success(client, "show databases")
    assert unique_database + "\t" in res.data
    assert unique_database + "_2\t" not in res.data
    self.__verify_profile_timeline(res.runtime_profile)

    # Create a new db in Hive
    self.run_stmt_in_hive("create database {0}_2".format(unique_database))
    res = self.execute_query_expect_success(client, "show databases")
    assert unique_database + "\t" in res.data
    assert unique_database + "_2\t" in res.data
    self.__verify_profile_timeline(res.runtime_profile)

    # Drop the new db in Hive
    self.run_stmt_in_hive("drop database {0}_2".format(unique_database))
    res = self.execute_query_expect_success(client, "show databases")
    assert unique_database + "\t" in res.data
    assert unique_database + "_2\t" not in res.data
    self.__verify_profile_timeline(res.runtime_profile)

  def test_drop_db(self, vector, unique_name):
    client = self.create_impala_client_from_vector(vector)
    db = unique_name + "_db"
    try:
      self.run_stmt_in_hive("create database {0}".format(db))
      self.execute_query_expect_success(client, "drop database {0}".format(db))
    finally:
      self.run_stmt_in_hive(
          "drop database if exists {0} cascade".format(db))

  def test_describe_db(self, vector, unique_name):
    client = self.create_impala_client_from_vector(vector)
    db = unique_name + "_db"
    try:
      self.run_stmt_in_hive("create database {0}".format(db))
      self.execute_query_expect_success(
          client, "describe database {0}".format(db))
    finally:
      self.run_stmt_in_hive("drop database if exists {0}".format(db))

  def test_show_functions(self, vector, unique_name):
    client = self.create_impala_client_from_vector(vector)
    db = unique_name + "_db"
    try:
      self.run_stmt_in_hive("create database {0}".format(db))
      self.execute_query_expect_success(
          client, "show functions in {0}".format(db))
    finally:
      self.run_stmt_in_hive("drop database if exists {0}".format(db))

  def test_hive_insert(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    # Create a partitioned table and DESCRIBE it to make it loaded.
    tbl_name = unique_database + ".tbl"
    self.execute_query("create table {0}(i int) partitioned by(p int)".format(tbl_name))
    self.execute_query("describe " + tbl_name)
    # Test SELECT gets new values inserted by Hive
    self.run_stmt_in_hive(
        "insert into table {0} partition (p=0) select 0".format(tbl_name))
    res = self.execute_query_expect_success(client, "select * from " + tbl_name)
    assert res.data == ["0\t0"]
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)
    # Same case but using INSERT OVERWRITE in Hive
    self.run_stmt_in_hive(
        "insert overwrite table {0} partition (p=0) select 1".format(tbl_name))
    res = self.execute_query_expect_success(client, "select * from " + tbl_name)
    assert res.data == ["1\t0"]
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

    # Test SHOW PARTITIONS gets new partitions created by Hive
    self.run_stmt_in_hive(
        "insert into table {0} partition (p=2) select 2".format(tbl_name))
    res = self.execute_query_expect_success(client, "show partitions " + tbl_name)
    assert self.has_value('p=0', res.data)
    assert self.has_value('p=2', res.data)
    # 3 result lines: 2 for partitions, 1 for total info
    assert len(res.data) == 3
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

  def test_create_dropped_db(self, vector, unique_name):
    """Test CREATE DATABASE on db dropped by Hive.
       Use unique_name instead of unique_database to avoid cleanup failure overwriting
       the real test failure, i.e. when the test fails, the db probably doesn't exist
       so cleanup of unique_database will also fail."""
    client = self.create_impala_client_from_vector(vector)
    db = unique_name + "_db"
    self.execute_query("create database " + db)
    try:
      self.run_stmt_in_hive("drop database " + db)
      res = self.execute_query_expect_success(client, "create database " + db)
      self.__verify_profile_timeline(res.runtime_profile)
    finally:
      self.execute_query("drop database if exists {} cascade".format(db))

  def test_create_dropped_table(self, vector, unique_database):
    """Test CREATE TABLE on table dropped by Hive"""
    client = self.create_impala_client_from_vector(vector)
    # Create a table and DESCRIBE it to make it loaded
    tbl_name = unique_database + ".tbl"
    self.execute_query_expect_success(client, "create table {0} (i int)".format(tbl_name))
    res = self.execute_query_expect_success(client, "describe " + tbl_name)
    assert res.data == ["i\tint\t"]
    # Drop it in Hive and re-create it in Impala using a new schema
    self.run_stmt_in_hive("drop table " + tbl_name)
    self.execute_query_expect_success(client, "create table {0} (j int)".format(tbl_name))
    res = self.execute_query_expect_success(client, "describe " + tbl_name)
    assert res.data == ["j\tint\t"]
    assert res.log == ''
    self.__verify_profile_timeline(res.runtime_profile)

  def __verify_profile_timeline(self, profile):
    self.verify_timeline_item(
        "Query Compilation", "Synced events from Metastore", profile)

  def test_multiple_tables(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    for i in range(3):
      self.execute_query("create table {0}.tbl{1} (i int)".format(unique_database, i))
    res = self.execute_query_expect_success(client, """
        select t1.i from {0}.tbl0 t0, {0}.tbl1 t1, {0}.tbl2 t2
        where t0.i = t1.i and t1.i = t2.i""".format(unique_database))
    assert len(res.data) == 0

    for i in range(3):
      self.run_stmt_in_hive("insert into table {0}.tbl{1} select 1".format(
          unique_database, i))
    res = self.execute_scalar_expect_success(client, """
        select t1.i from {0}.tbl0 t0, {0}.tbl1 t1, {0}.tbl2 t2
        where t0.i = t1.i and t1.i = t2.i""".format(unique_database))
    assert res == "1"

  def test_view(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    view = unique_database + ".foo_view"
    count_stmt = "select count(*) from {}".format(view)
    self.execute_query("create table {}(i int)".format(tbl))
    self.execute_query("create view {} as select * from {}".format(view, tbl))
    # Run a query to make the metadata loaded so they can be stale later.
    res = self.execute_scalar(count_stmt)
    assert res == '0'

    # Modify the table in Hive and read the view in Impala
    self.run_stmt_in_hive("insert into {} select 1".format(tbl))
    res = self.execute_query_expect_success(client, count_stmt)
    assert res.data[0] == '1'

    # Modify the view in Hive and read it in Impala
    self.run_stmt_in_hive(
        "alter view {} as select * from {} where i > 1".format(view, tbl))
    res = self.execute_query_expect_success(client, count_stmt)
    assert res.data[0] == '0'

  def test_view_partitioned(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    view = unique_database + ".foo_view"
    select_stmt = "select * from " + view
    self.execute_query("create table {}(i int) partitioned by(p int)".format(tbl))
    self.execute_query("create view {} as select * from {} where p>0".format(view, tbl))
    res = self.execute_query_expect_success(client, select_stmt)
    assert len(res.data) == 0

    # Ingest data in Hive and read the view in Impala
    # Add a new partition that will be filtered out by the view
    self.run_stmt_in_hive("insert into {} select 0, 0".format(tbl))
    res = self.execute_query_expect_success(client, select_stmt)
    assert len(res.data) == 0
    # Add a new partition that will show up in the view
    self.run_stmt_in_hive("insert into {} select 1, 1".format(tbl))
    res = self.execute_scalar_expect_success(client, select_stmt)
    assert res == '1\t1'
    # Add a new partition and alter the view to only show it
    self.run_stmt_in_hive("insert into {} select 2, 2".format(tbl))
    self.run_stmt_in_hive("alter view {} as select * from {} where p>1".format(view, tbl))
    res = self.execute_scalar_expect_success(client, select_stmt)
    assert res == '2\t2'

  def test_compute_stats(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    self.execute_query("create table {}(i int) partitioned by(p int)".format(tbl))
    # Add one partition in Hive and compute incremental stats on that partition in Impala
    self.run_stmt_in_hive("insert into {} select 0,0".format(tbl))
    res = self.execute_query_expect_success(
        client, "compute incremental stats {} partition(p=0)".format(tbl))
    assert res.data == ['Updated 1 partition(s) and 1 column(s).']
    # Add one partition in Hive and compute incremental stats on that table in Impala
    self.run_stmt_in_hive("insert into {} select 1,1 union all select 2,2".format(tbl))
    res = self.execute_query_expect_success(
      client, "compute incremental stats {}".format(tbl))
    assert res.data == ['Updated 2 partition(s) and 1 column(s).']
    # Drop two partitions in Hive and compute stats on that table in Impala. The
    # incremental stats will be replaced with non-incremental stats so the remaining
    # partition is updated.
    self.run_stmt_in_hive("alter table {} drop partition(p<2)".format(tbl))
    res = self.execute_query_expect_success(
        client, "compute stats {}".format(tbl))
    assert res.data == ['Updated 1 partition(s) and 1 column(s).']

  def test_ctas(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    tmp_tbl = unique_database + ".tmp"
    self.execute_query("create table {}(i int) partitioned by(p int)".format(tbl))
    # Add one partition in Hive and use the table in Impala
    self.run_stmt_in_hive("insert into {} select 0,0".format(tbl))
    res = self.execute_query_expect_success(
        client, "create table {} as select * from {}".format(tmp_tbl, tbl))
    assert res.data == ['Inserted 1 row(s)']
    # Insert one row into the same partition in Hive and use the table in Impala
    self.run_stmt_in_hive("insert into {} select 1,0".format(tbl))
    res = self.execute_query_expect_success(
        client, "create table {}_2 as select * from {}".format(tmp_tbl, tbl))
    assert res.data == ['Inserted 2 row(s)']
    # Truncate the table in Hive and use it in Impala
    self.run_stmt_in_hive("truncate table {}".format(tbl))
    res = self.execute_query_expect_success(
        client, "create table {}_3 as select * from {}".format(tmp_tbl, tbl))
    assert res.data == ['Inserted 0 row(s)']

    # Create a table in Hive before CTAS of it in Impala
    self.run_stmt_in_hive("create table {}_4(i int) partitioned by(p int)".format(tbl))
    exception = self.execute_query_expect_failure(
        client, "create table {}_4 as select 1,1".format(tbl))
    assert 'Table already exists: {}_4'.format(tbl) in str(exception)

  def test_impala_insert(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    tmp_tbl = unique_database + ".tmp"
    self.execute_query("create table {}(i int) partitioned by(p int)".format(tbl))
    self.execute_query("create table {}(i int) partitioned by(p int)".format(tmp_tbl))
    insert_stmt = "insert into {} partition (p) select * from {}".format(tmp_tbl, tbl)
    # Add one partition in Hive and use the table in INSERT in Impala
    self.run_stmt_in_hive("insert into {} select 0,0".format(tbl))
    res = self.execute_query_expect_success(client, insert_stmt)
    # Result rows are "partition_name: num_rows_inserted" for each modified partitions
    assert 'Partition: p=0\nNumModifiedRows: 1\n' in res.runtime_profile
    # Insert one row into the same partition in Hive and use the table in INSERT in Impala
    self.run_stmt_in_hive("insert into {} select 1,0".format(tbl))
    res = self.execute_query_expect_success(client, insert_stmt)
    assert 'Partition: p=0\nNumModifiedRows: 2\n' in res.runtime_profile
    # Add another new partition in Hive and use the table in INSERT in Impala
    self.run_stmt_in_hive("insert into {} select 2,2".format(tbl))
    res = self.execute_query_expect_success(client, insert_stmt)
    assert 'Partition: p=0\nNumModifiedRows: 2\n' in res.runtime_profile
    assert 'Partition: p=2\nNumModifiedRows: 1\n' in res.runtime_profile
    # Drop one partition in Hive and use the table in INSERT in Impala
    self.run_stmt_in_hive("alter table {} drop partition(p=0)".format(tbl))
    res = self.execute_query_expect_success(client, insert_stmt)
    assert 'Partition: p=2\nNumModifiedRows: 1\n' in res.runtime_profile
    # Truncate the table in Hive and use it in INSERT in Impala
    self.run_stmt_in_hive("truncate table {}".format(tbl))
    res = self.execute_query_expect_success(client, insert_stmt)
    assert 'NumModifiedRows:' not in res.runtime_profile

  def test_txn(self, vector, unique_database):
    client = self.create_impala_client_from_vector(vector)
    tbl = unique_database + ".foo"
    self.run_stmt_in_hive(
        "create transactional table {}(i int) partitioned by(p int)".format(tbl))
    # Load the table in Impala
    self.execute_query_expect_success(client, "describe " + tbl)
    # Insert the table in Hive and check it in Impala immediately
    self.run_stmt_in_hive("insert into {} select 0,0".format(tbl))
    res = self.execute_query_expect_success(client, "select * from " + tbl)
    assert res.data == ['0\t0']
    # Insert the table in Hive again and check number of rows in Impala
    self.run_stmt_in_hive("insert into {} select 1,0".format(tbl))
    res = self.execute_query_expect_success(client, "select count(*) from " + tbl)
    assert res.data == ['2']
    res = self.execute_query_expect_success(client, "show files in " + tbl)
    assert len(res.data) == 2
    # Trigger compaction in Hive
    self.run_stmt_in_hive(
        "alter table {} partition(p=0)compact 'minor' and wait".format(tbl))
    res = self.execute_query_expect_success(client, "show files in " + tbl)
    assert len(res.data) == 1

  def test_hms_event_sync_on_deletion(self, vector, unique_name):
    """Regression test for IMPALA-13829: TWaitForHmsEventResponse not able to collect
    removed objects due to their items in deleteLog being GCed."""
    client = self.create_impala_client_from_vector(vector)
    # Set a sleep time so catalogd has time to GC the deleteLog.
    client.execute("set debug_action='collect_catalog_results_delay:SLEEP@1000'")
    db = unique_name + "_db"
    tbl = db + ".foo"
    self.execute_query("drop database if exists {} cascade".format(db))
    self.execute_query("drop database if exists {}_2 cascade".format(db))
    self.execute_query("create database {}".format(db))
    self.execute_query("create database {}_2".format(db))
    # Create HMS Thrift clients to drop db/tables in the fastest way
    hive_clients = []
    hive_transports = []
    for _ in range(2):
      c, t = ImpalaTestSuite.create_hive_client()
      hive_clients.append(c)
      hive_transports.append(t)

    try:
      # Drop 2 dbs concurrently. So their DROP_DATABASE events are processed together (in
      # the same event batch). We need more than one db to be dropped so one of them in
      # catalogd's deleteLog can be GCed since its version < latest catalog version.
      # Note that this is no longer the way catalogd GCs the deleteLog after IMPALA-13829,
      # but it can be used to trigger the issue before this fix.
      def drop_db_in_hive(i, db_name):
        hive_clients[i].drop_database(db_name, deleteData=True, cascade=True)
        LOG.info("Dropped database {} in Hive".format(db_name))
      ts = [threading.Thread(target=drop_db_in_hive, args=params)
            for params in [[0, db], [1, db + "_2"]]]
      for t in ts:
        t.start()
      for t in ts:
        t.join()
      client.execute("create database " + db)

      self.execute_query("create table {}(i int)".format(tbl))
      self.execute_query("create table {}_2(i int)".format(tbl))

      # Drop 2 tables concurrently. So their DROP_TABLE events are processed together (in
      # the same event batch). We need more than one table to be dropped so one of them in
      # catalogd's deleteLog can be GCed since its version < latest catalog version.
      # Note that this is no longer the way catalogd GCs the deleteLog after IMPALA-13829,
      # but it can be used to trigger the issue before this fix.
      def drop_table_in_hive(i, tbl_name):
        hive_clients[i].drop_table(db, tbl_name, deleteData=True)
        LOG.info("Dropped table {}.{} in Hive".format(db, tbl_name))
      ts = [threading.Thread(target=drop_table_in_hive, args=params)
            for params in [[0, "foo"], [1, "foo_2"]]]
      for t in ts:
        t.start()
      for t in ts:
        t.join()
      client.execute("create table {}(i int)".format(tbl))
    finally:
      for t in hive_transports:
        t.close()
      self.execute_query("drop database if exists {} cascade".format(db))
      self.execute_query("drop database if exists {}_2 cascade".format(db))
