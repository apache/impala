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

from hive_metastore.ttypes import FireEventRequest
from hive_metastore.ttypes import FireEventRequestData
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfCatalogV2, SkipIfFS
from tests.metadata.test_event_processing_base import TestEventProcessingBase
from tests.util.acid_txn import AcidTxn
from tests.util.event_processor_utils import EventProcessorUtils


@SkipIfCatalogV2.hms_event_polling_disabled()
class TestEventProcessingError(CustomClusterTestSuite):
  """
  Tests for verify event processor not going into error state whenever there are
  runtime exceptions while processing events.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
    impalad_args="--use_local_catalog=true",
    catalogd_args="--catalog_topic_mode=minimal "
                  "--invalidate_metadata_on_event_processing_failure=false "
                  "--inject_process_event_failure_event_types='ALTER_TABLE' "
                  "--hms_event_polling_interval_s=2")
  def test_event_processor_error_sanity_check(self, unique_database):
      """Tests event processor going into error state for alter table event"""
      tbl_name = "hive_alter_table"
      self.__create_table_and_load__(unique_database, tbl_name, False, False)
      self.run_stmt_in_hive(
          "alter table {}.{} set owner user `test-user`"
          .format(unique_database, tbl_name))
      try:
          EventProcessorUtils.wait_for_event_processing(self)
      except Exception:
          assert EventProcessorUtils.get_event_processor_status() == "ERROR"
          self.client.execute("INVALIDATE METADATA")
          assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ALTER_TABLE' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_alter_table(self, unique_database):
      """Tests event processor going into error state for alter table event"""
      tbl_name = "hive_alter_table"
      self.__create_table_and_load__(unique_database, tbl_name, False, False)
      self.run_stmt_in_hive(
          "alter table {}.{} set owner user `test-user`"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))
      self.verify_owner_property(result, 'test-user')

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ADD_PARTITION' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_add_partition(self, unique_database):
      """Tests event processor going into error state for add partition event"""
      tbl_name = "hive_table_add_partition"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      self.client.execute("describe {}.{}".format(unique_database, tbl_name))
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("show partitions {}.{}"
          .format(unique_database, tbl_name))
      # First line is the header. Only one partition should be shown so the
      # result has two lines.
      assert "hive_table_add_partition/year=2024" in result.get_data()
      assert len(result.data) == 2

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ALTER_PARTITION' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_alter_partition(self, unique_database):
      """Tests event processor going into error state for alter partition event"""
      tbl_name = "hive_table_alter_partition"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      self.run_stmt_in_hive(
          "analyze table {}.{} compute statistics"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("show partitions {}.{}"
          .format(unique_database, tbl_name))
      assert "2024" in result.get_data()
      assert len(result.data) == 2

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ALTER_PARTITIONS' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_alter_partitions(self, unique_database):
      """Tests event processor going into error state for batch alter partitions event"""
      tbl_name = "hive_table_alter_partitions"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      for i in range(5):
          self.client.execute(
              "alter table {}.{} add partition(year={})"
              .format(unique_database, tbl_name, i))
      self.run_stmt_in_hive(
          "analyze table {}.{} compute statistics"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("show partitions {}.{}"
          .format(unique_database, tbl_name))
      for i in range(5):
          assert str(i) in result.get_data()
      assert len(result.data) == 6

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='INSERT' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_insert_event(self, unique_database):
      """Tests event processor going into error state for insert event"""
      tbl_name = "hive_table_insert"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      for _ in range(2):
          self.client.execute(
              "insert into {}.{} partition(year=2024) values (1),(2),(3)"
              .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("select count(*) from {}.{}"
          .format(unique_database, tbl_name))
      assert result.data[0] == '6'

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='INSERT_PARTITIONS' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_insert_events(self, unique_database):
      """Tests event processor going into error state for insert partitions event"""
      tbl_name = "hive_table_insert_partitions"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      for _ in range(2):
          self.client.execute(
              "insert into {}.{} partition(year=2024) values (1),(2),(3)"
              .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("select count(*) from {}.{}"
          .format(unique_database, tbl_name))
      assert result.data[0] == '6'

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='DROP_PARTITION' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_drop_partition(self, unique_database):
      """Tests event processor going into error state for drop partitions event"""
      tbl_name = "hive_table_drop_partition"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      self.run_stmt_in_hive(
          "alter table {}.{} drop partition(year=2024)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("show partitions {}.{}"
          .format(unique_database, tbl_name))
      assert "2024" not in result.get_data()
      assert len(result.data) == 1

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='RELOAD' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_reload_event(self, unique_database):
      """Tests event processor going into error state for reload event"""
      tbl_name = "hive_table_reload_event"
      self.__create_table_and_load__(unique_database, tbl_name, False, True)
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      # Refresh at table level
      data = FireEventRequestData()
      data.refreshEvent = True
      req = FireEventRequest(True, data)
      req.dbName = unique_database
      req.tableName = tbl_name
      self.hive_client.fire_listener_event(req)
      EventProcessorUtils.wait_for_event_processing(self)
      # refresh at partition level
      req.partitionVals = ["2024"]
      self.hive_client.fire_listener_event(req)
      EventProcessorUtils.wait_for_event_processing(self)
      # invalidate at table level
      data.refreshEvent = False
      req = FireEventRequest(True, data)
      req.dbName = unique_database
      req.tableName = tbl_name
      self.hive_client.fire_listener_event(req)
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types="
                    "'COMMIT_COMPACTION_EVENT' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_commit_compaction_event(self, unique_database):
      """Tests event processor going into error state for commit compaction event"""
      tbl_name = "hive_table_commit_compaction"
      self.__create_table_and_load__(unique_database, tbl_name, True, False)
      for _ in range(2):
          self.run_stmt_in_hive(
              "insert into {}.{} values (1),(2),(3)"
              .format(unique_database, tbl_name))
      self.run_stmt_in_hive(
          "alter table {}.{} compact 'minor' and wait"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ALLOC_WRITE_ID_EVENT' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_alloc_write_id_event(self, unique_database):
      tbl_name = "hive_table_alloc_write_id"
      self.__create_table_and_load__(unique_database, tbl_name, True, True)
      acid = AcidTxn(self.hive_client)
      txn_id = acid.open_txns()
      acid.allocate_table_write_ids(txn_id, unique_database, tbl_name)
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='COMMIT_TXN' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_commit_txn(self, unique_database):
      tbl_name = "hive_table_commit_txn"
      self.__create_table_and_load__(unique_database, tbl_name, True, True)
      self.run_stmt_in_hive(
          "insert into {}.{} partition (year=2022) values (1),(2),(3)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_event_types='ABORT_TXN' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_abort_txn_event(self, unique_database):
      tbl_name = "hive_table_abort_txn"
      acid = AcidTxn(self.hive_client)
      self.__create_table_and_load__(unique_database, tbl_name, True, True)
      txn_id = acid.open_txns()
      acid.abort_txn(txn_id)
      EventProcessorUtils.wait_for_event_processing(self)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))

  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--invalidate_metadata_on_event_processing_failure=false "
                    "--invalidate_global_metadata_on_event_processing_failure=true "
                    "--inject_process_event_failure_event_types="
                    "'ALTER_TABLE, ADD_PARTITION' "
                    "--hms_event_polling_interval_s=2")
  def test_event_processor_error_global_invalidate(self, unique_database):
      """Test to verify that auto global invalidate put back EP to active
      when it goes into error state"""
      tbl_name = "hive_table_global_invalidate"
      self.__create_table_and_load__(unique_database, tbl_name, True, True)
      self.run_stmt_in_hive(
          "alter table {}.{} set owner user `test-user`"
          .format(unique_database, tbl_name))
      self.run_stmt_in_hive(
          "alter table {}.{} add partition(year=2024)"
          .format(unique_database, tbl_name))
      EventProcessorUtils.wait_for_event_processing(self, error_status_possible=True)
      assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
      result = self.client.execute("describe formatted {}.{}"
          .format(unique_database, tbl_name))
      self.verify_owner_property(result, 'test-user')
      assert "2024" in result.get_data()

  @SkipIfFS.hive
  @CustomClusterTestSuite.with_args(
      impalad_args="--use_local_catalog=true",
      catalogd_args="--catalog_topic_mode=minimal "
                    "--inject_process_event_failure_ratio=0.5 "
                    "--inject_process_event_failure_event_types="
                    "'ALTER_TABLE,ADD_PARTITION,"
                    "ALTER_PARTITION,INSERT,ABORT_TXN,COMMIT_TXN'")
  def test_event_processor_error_stress_test(self, unique_database):
    """Executes inserts for transactional tables and external tables. Also runs
    replication tests
    """
    # inserts on transactional tables
    TestEventProcessingBase._run_test_insert_events_impl(self.hive_client, self.client,
      self.cluster, unique_database, True)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    try:
        test_db = unique_database + "_no_transact"
        self.run_stmt_in_hive("""create database {}""".format(test_db))
        # inserts on external tables
        TestEventProcessingBase._run_test_insert_events_impl(self.hive_client,
          self.client, self.cluster, test_db, False)
        assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
    finally:
        self.run_stmt_in_hive("""drop database {} cascade""".format(test_db))
    # replication related tests
    TestEventProcessingBase._run_event_based_replication_tests_impl(self.hive_client,
      self.client, self.cluster, self.filesystem_client)
    assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"

  def __create_table_and_load__(self, db_name, table_name, is_transactional,
      is_partitioned):
    create_query = " ".join(["create", " transactional " if is_transactional else '',
      "table `{}`.`{}`(i int)", " partitioned by (year int) " if is_partitioned else ''])
    self.run_stmt_in_hive(create_query.format(db_name, table_name))
    EventProcessorUtils.wait_for_event_processing(self)
    # Make the table loaded
    self.client.execute("describe {}.{}".format(db_name, table_name))

  @staticmethod
  def verify_owner_property(result, user_name):
      match = False
      for row in result.data:
          fields = row.split("\t")
          if "Owner:" in fields[0]:
              assert user_name == fields[1].strip()
              match = True
      assert True, match
