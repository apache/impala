# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import pytest
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfS3, SkipIfIsilon, SkipIfLocal
from tests.util.filesystem_utils import IS_ISILON, WAREHOUSE
from tests.util.hdfs_util import HdfsConfig, get_hdfs_client, get_hdfs_client_from_conf

TEST_TBL = "insert_inherit_permission"

@SkipIfS3.insert
class TestInsertBehaviourCustomCluster(CustomClusterTestSuite):

  @classmethod
  def setup_class(cls):
    if pytest.config.option.namenode_http_address is None:
      hdfs_conf = HdfsConfig(pytest.config.option.minicluster_xml_conf)
      cls.hdfs_client = get_hdfs_client_from_conf(hdfs_conf)
    else:
      host, port = pytest.config.option.namenode_http_address.split(":")
      cls.hdfs_client = get_hdfs_client(host, port)

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

  @SkipIfLocal.hdfs_client
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

  @SkipIfLocal.hdfs_client
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
      if not IS_ISILON: # CDH-27688
        self._check_partition_perms("p1=1/p2=3/", default_perms)
      self._check_partition_perms("p1=1/p2=3/p3=4/", default_perms)
    finally:
       client.close()
