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
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS, SkipIfLocal
from tests.util.event_processor_utils import EventProcessorUtils
from tests.util.filesystem_utils import IS_ISILON, WAREHOUSE
from tests.util.hdfs_util import (
    HdfsConfig,
    get_webhdfs_client,
    get_webhdfs_client_from_conf)

TEST_TBL = "insert_inherit_permission"


@SkipIfFS.hdfs_acls
@SkipIfLocal.hdfs_client
class TestInsertBehaviourCustomCluster(CustomClusterTestSuite):

  @classmethod
  def setup_class(cls):
    super(TestInsertBehaviourCustomCluster, cls).setup_class()
    if pytest.config.option.namenode_http_address is None:
      hdfs_conf = HdfsConfig(pytest.config.option.minicluster_xml_conf)
      cls.hdfs_client = get_webhdfs_client_from_conf(hdfs_conf)
    else:
      host, port = pytest.config.option.namenode_http_address.split(":")
      cls.hdfs_client = get_webhdfs_client(host, port)

  def _check_partition_perms(self, part, perms):
    ls = self.hdfs_client.get_file_dir_status("test-warehouse/%s/%s" % (TEST_TBL, part))
    assert ls['FileStatus']['permission'] == perms

  def _get_impala_client(self):
    impalad = self.cluster.get_any_impalad()
    return impalad.service.create_beeswax_client()

  def _create_test_tbl(self):
    client = self._get_impala_client()
    options = {'sync_ddl': '1'}
    try:
      self.execute_query_expect_success(client, "DROP TABLE IF EXISTS %s" % TEST_TBL,
                                        query_options=options)
      self.execute_query_expect_success(client,
                                        "CREATE TABLE {0} (col int) PARTITIONED"
                                        " BY (p1 int, p2 int, p3 int) location"
                                        " '{1}/{0}'".format(TEST_TBL, WAREHOUSE),
                                        query_options=options)
      self.execute_query_expect_success(client, "ALTER TABLE %s"
                                        " ADD PARTITION(p1=1, p2=1, p3=1)" % TEST_TBL,
                                        query_options=options)
    finally:
      client.close()

  def _drop_test_tbl(self):
    client = self._get_impala_client()
    self.execute_query_expect_success(client, "drop table if exists %s" % TEST_TBL)
    client.close()

  def setup_method(cls, method):
    super(TestInsertBehaviourCustomCluster, cls).setup_method(method)
    cls._create_test_tbl()

  def teardown_method(cls, method):
    cls._drop_test_tbl()
    super(TestInsertBehaviourCustomCluster, cls).teardown_method(method)

  @SkipIfLocal.root_path
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--insert_inherit_permissions=true")
  def test_insert_inherit_permission(self):
    """Create a table with three partition columns to test permission inheritance"""
    client = self._get_impala_client()
    try:
      self.hdfs_client.chmod("test-warehouse/%s/p1=1/" % TEST_TBL, "777")

      # 1. INSERT that creates two new directories gets permissions from parent
      self.execute_query_expect_success(client, "INSERT INTO %s"
                                        " PARTITION(p1=1, p2=2, p3=2) VALUES(1)" % TEST_TBL)
      self._check_partition_perms("p1=1/p2=2/", "777")
      self._check_partition_perms("p1=1/p2=2/p3=2/", "777")

      # 2. INSERT that creates one new directory gets permissions from parent
      self.execute_query_expect_success(client, "INSERT INTO %s"
                                        " PARTITION(p1=1, p2=2, p3=3) VALUES(1)" % TEST_TBL)
      self._check_partition_perms("p1=1/p2=2/p3=3/", "777")

      # 3. INSERT that creates no new directories keeps standard permissions
      self.hdfs_client.chmod("test-warehouse/%s/p1=1/p2=2" % TEST_TBL, "744")
      self.execute_query_expect_success(client, "INSERT INTO %s"
                                        " PARTITION(p1=1, p2=2, p3=3) VALUES(1)" % TEST_TBL)
      self._check_partition_perms("p1=1/p2=2/", "744")
      self._check_partition_perms("p1=1/p2=2/p3=3/", "777")
    finally:
      client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--insert_inherit_permissions=false")
  def test_insert_inherit_permission_disabled(self):
    """Check that turning off insert permission inheritance works correctly."""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    try:
      ls = self.hdfs_client.get_file_dir_status("test-warehouse/%s/p1=1/" % TEST_TBL)
      default_perms = ls['FileStatus']['permission']
      self.hdfs_client.chmod("test-warehouse/%s/p1=1/" % TEST_TBL, "777")

      self.execute_query_expect_success(client, "INSERT INTO %s"
                                        " PARTITION(p1=1, p2=3, p3=4) VALUES(1)" % TEST_TBL)
      # Would be 777 if inheritance was enabled
      if not IS_ISILON: # IMPALA-4221
        self._check_partition_perms("p1=1/p2=3/", default_perms)
      self._check_partition_perms("p1=1/p2=3/p3=4/", default_perms)
    finally:
       client.close()


@SkipIfFS.hive
class TestInsertUnSyncedPartition(CustomClusterTestSuite):

  @classmethod
  def setup_class(cls):
    super(TestInsertUnSyncedPartition, cls).setup_class()

  @CustomClusterTestSuite.with_args(catalogd_args="--hms_event_polling_interval_s=0")
  def test_insert_unsynced_partition(self, unique_database):
    """Regression test for IMPALA-12257. Tests with event-processing disabled so
    catalogd can easily have unsynced partition with HMS."""
    self._test_insert_on_unsynced_partition(unique_database, "part1", False, False)
    self._test_insert_on_unsynced_partition(unique_database, "part2", False, True)
    self._test_insert_on_unsynced_partition(unique_database, "txn_part1", True, False)
    self._test_insert_on_unsynced_partition(unique_database, "txn_part2", True, True)

  def _test_insert_on_unsynced_partition(self, db, tbl, is_transactional, is_overwrite):
    tbl_name = db + "." + tbl
    create_stmt = """
        create table {0} (i int) partitioned by (p int)
        stored as textfile""".format(tbl_name)
    if is_transactional:
      create_stmt += """ tblproperties(
        'transactional'='true',
        'transactional_properties'='insert_only')"""
    self.client.execute(create_stmt)
    # Run any query on the table to make it loaded in catalogd.
    self.client.execute("describe " + tbl_name)
    # Add the partition in Hive so catalogd is not aware of it.
    self.run_stmt_in_hive("""
        insert into {0} partition (p=0) values (0)""".format(tbl_name))
    # Track the last event id so we can fetch the generated events
    last_event_id = EventProcessorUtils.get_current_notification_id(self.hive_client)
    # Insert the new partition in Impala.
    self.client.execute("""
        insert {0} {1} partition(p=0) values (1)
        """.format("overwrite" if is_overwrite else "into", tbl_name))
    events = EventProcessorUtils.get_next_notification(self.hive_client, last_event_id)
    if is_transactional:
      assert len(events) > 2
      assert events[0].eventType == "OPEN_TXN"
      assert events[1].eventType == "ALLOC_WRITE_ID_EVENT"
      assert events[1].dbName == db
      assert events[1].tableName == tbl
      # There is an empty ADD_PARTITION event due to Impala invokes the add_partitions
      # HMS API but no new partitions are really added. This might change in future Hive
      # versions. Here we just verify whether the last event is COMMIT_TXN.
      assert events[len(events) - 1].eventType == "COMMIT_TXN"
    else:
      assert len(events) > 0
      last_event = events[len(events) - 1]
      assert last_event.dbName == db
      assert last_event.tableName == tbl
      assert last_event.eventType == "INSERT"

    res = self.client.execute("select * from " + tbl_name)
    if is_overwrite:
      assert res.data == ["1\t0"]
    else:
      assert "0\t0" in res.data
      assert "1\t0" in res.data
      assert len(res.data) == 2
