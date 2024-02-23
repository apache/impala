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
import pytest

from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS, SkipIfHive2, SkipIfCatalogV2
from tests.metadata.test_event_processing_base import TestEventProcessingBase
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
    TestEventProcessingBase._run_test_insert_events_impl(self.hive_client, self.client,
        ImpalaCluster.get_e2e_test_cluster(), unique_database, is_transactional=True)

  def test_insert_events(self, unique_database):
    """Executes 'run_test_insert_events' for non-transactional tables.
    """
    TestEventProcessingBase._run_test_insert_events_impl(self.hive_client, self.client,
        ImpalaCluster.get_e2e_test_cluster(), unique_database)

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
    TestEventProcessingBase._run_event_based_replication_tests_impl(self.hive_client,
        self.client, ImpalaCluster.get_e2e_test_cluster(), self.filesystem_client)

  def _run_test_empty_partition_events(self, unique_database, is_transactional):
    test_tbl = unique_database + ".test_events"
    TBLPROPERTIES = TestEventProcessingBase._get_transactional_tblproperties(
      is_transactional)
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
