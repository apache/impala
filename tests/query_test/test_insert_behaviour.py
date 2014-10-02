#!/usr/bin/env python
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

from tests.common.impala_test_suite import ImpalaTestSuite
import time
import pytest

class TestInsertBehaviour(ImpalaTestSuite):
  """Tests for INSERT behaviour that isn't covered by checking query results"""

  @pytest.mark.execute_serially
  def test_insert_removes_staging_files(self):
    insert_staging_dir = \
      "test-warehouse/functional.db/insert_overwrite_nopart/.impala_insert_staging"
    self.hdfs_client.delete_file_dir(insert_staging_dir, recursive=True)
    self.client.execute("""INSERT OVERWRITE
functional.insert_overwrite_nopart SELECT int_col FROM functional.tinyinttable""")
    ls = self.hdfs_client.list_dir(insert_staging_dir)
    assert len(ls['FileStatuses']['FileStatus']) == 0

  @pytest.mark.execute_serially
  def test_insert_preserves_hidden_files(self):
    """Test that INSERT OVERWRITE preserves hidden files in the root table directory"""
    table_dir = "test-warehouse/functional.db/insert_overwrite_nopart/"
    hidden_file_locations = [".hidden", "_hidden"]
    dir_locations = ["dir", ".hidden_dir"]
    for dir in dir_locations:
      self.hdfs_client.make_dir(table_dir + dir)
    for file in hidden_file_locations:
      self.hdfs_client.create_file(table_dir + file, '', overwrite=True)

    self.client.execute("""INSERT OVERWRITE
functional.insert_overwrite_nopart SELECT int_col FROM functional.tinyinttable""")

    for file in hidden_file_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file)
      except:
        err_msg = "Hidden file '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + file))

    for dir in dir_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file)
      except:
        err_msg = "Directory '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + dir))

  @pytest.mark.execute_serially
  def test_insert_alter_partition_location(self):
    """Test that inserts after changing the location of a partition work correctly,
    including the creation of a non-existant partition dir"""
    partition_dir = "tmp/test_insert_alter_partition_location"

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS "
                                      "functional.insert_alter_partition_location")
    self.hdfs_client.delete_file_dir(partition_dir, recursive=True)

    self.execute_query_expect_success(self.client,
"CREATE TABLE functional.insert_alter_partition_location (c int) PARTITIONED BY (p int)")
    self.execute_query_expect_success(self.client,
"ALTER TABLE functional.insert_alter_partition_location ADD PARTITION(p=1)")
    self.execute_query_expect_success(self.client,
"ALTER TABLE functional.insert_alter_partition_location PARTITION(p=1) SET LOCATION '/%s'"
      % partition_dir)
    self.execute_query_expect_success(self.client,
"INSERT OVERWRITE functional.insert_alter_partition_location PARTITION(p=1) VALUES(1)")

    result = self.execute_query_expect_success(self.client,
"SELECT COUNT(*) FROM functional.insert_alter_partition_location")
    assert int(result.get_data()) == 1

    # Should have created the partition dir, which should contain exactly one file (not in
    # a subdirectory)
    ls = self.hdfs_client.list_dir(partition_dir)
    assert len(ls['FileStatuses']['FileStatus']) == 1

  @pytest.mark.xfail(run=False, reason="Fails intermittently on test clusters")
  @pytest.mark.execute_serially
  def test_insert_inherit_acls(self):
    """Check that ACLs are inherited when we create new partitions"""

    ROOT_ACL = "default:group:dummy_group:rwx"
    TEST_ACL = "default:group:impala_test_users:r-x"
    def check_has_acls(part, acl=TEST_ACL):
      path = "test-warehouse/functional.db/insert_inherit_acls/" + part
      result = self.hdfs_client.getacl(path)
      assert acl in result['AclStatus']['entries']

    # Add a spurious ACL to functional.db directory
    self.hdfs_client.setacl("test-warehouse/functional.db", ROOT_ACL)

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS"
                                      " functional.insert_inherit_acls")
    self.execute_query_expect_success(self.client, "CREATE TABLE "
                                      "functional.insert_inherit_acls (col int)"
                                      " PARTITIONED BY (p1 int, p2 int, p3 int)")

    # Check that table creation inherited the ACL
    check_has_acls("", ROOT_ACL)

    self.execute_query_expect_success(self.client, "ALTER TABLE "
                                      "functional.insert_inherit_acls ADD PARTITION"
                                      "(p1=1, p2=1, p3=1)")

    check_has_acls("p1=1", ROOT_ACL)
    check_has_acls("p1=1/p2=1", ROOT_ACL)
    check_has_acls("p1=1/p2=1/p3=1", ROOT_ACL)

    self.hdfs_client.setacl(
      "test-warehouse/functional.db/insert_inherit_acls/p1=1/", TEST_ACL)

    self.execute_query_expect_success(self.client, "INSERT INTO "
                                      "functional.insert_inherit_acls "
                                      "PARTITION(p1=1, p2=2, p3=2) VALUES(1)")

    check_has_acls("p1=1/p2=2/")
    check_has_acls("p1=1/p2=2/p3=2")

    # Check that SETACL didn't cascade down to children (which is more to do with HDFS
    # than Impala, but worth asserting here)
    check_has_acls("p1=1/p2=1", ROOT_ACL)
    check_has_acls("p1=1/p2=1/p3=1", ROOT_ACL)

    # Change ACLs on p1=1,p2=2 and create a new leaf at p3=30
    self.hdfs_client.setacl(
      "test-warehouse/functional.db/insert_inherit_acls/p1=1/p2=2/",
      "default:group:new_leaf_group:-w-")

    self.execute_query_expect_success(self.client, "INSERT INTO "
                                      "functional.insert_inherit_acls "
                                      "PARTITION(p1=1, p2=2, p3=30) VALUES(1)")
    check_has_acls("p1=1/p2=2/p3=30", "default:group:new_leaf_group:-w-")

  def test_insert_acl_permissions(self):
    """Test that INSERT correctly respects ACLs"""
    TBL = "functional.insert_acl_permissions"
    TBL_PATH = "test-warehouse/functional.db/insert_acl_permissions"

    INSERT_QUERY = "INSERT INTO %s VALUES(1)" % TBL

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS"
                                      " functional.insert_acl_permissions")
    self.execute_query_expect_success(self.client, "CREATE TABLE "
                                      "functional.insert_acl_permissions (col int) ")

    # Check that a simple insert works
    self.execute_query_expect_success(self.client, INSERT_QUERY)

    # Remove the permission to write and confirm that INSERTs won't work
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Now add group access, still should fail (because the user will match and take
    # priority)
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,group::rwx,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Check that the mask correctly applies to the anonymous group ACL
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,group::rwx,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because mask applies to unnamed group and disables writing
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Check that the mask correctly applies to the user ACL
    self.hdfs_client.setacl(TBL_PATH,
                            "user::r-x,user:impala:rwx,group::r-x,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because mask applies to unnamed group and disables writing
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Now make the target directory non-writable with posix permissions, but writable with
    # ACLs (ACLs should take priority). Note: chmod affects ACLs (!) so it has to be done
    # first.
    self.hdfs_client.chmod(TBL_PATH, "000")
    self.hdfs_client.setacl(TBL_PATH, "user::rwx,group::r-x,other::r-x")

    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_success(self.client, INSERT_QUERY)

    # Finally, change the owner
    self.hdfs_client.chown(TBL_PATH, "another_user", "another_group")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because 'other' ACLs don't allow writes
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Give write perms back to 'other'
    self.hdfs_client.setacl(TBL_PATH, "user::rwx,group::r-x,other::rwx")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because 'other' ACLs don't allow writes
    self.execute_query_expect_success(self.client, INSERT_QUERY)

  def test_load_permissions(self):
    # We rely on test_insert_acl_permissions() to exhaustively check that ACL semantics
    # are correct. Here we just validate that LOADs can't be done when we cannot read from
    # or write to the src directory, or write to the dest directory.
    TBL = "functional.load_acl_permissions"
    TBL_PATH = "test-warehouse/functional.db/load_acl_permissions"
    FILE_PATH = "tmp/impala_load_test"
    FILE_NAME = "%s/impala_data_file" % FILE_PATH
    LOAD_FILE_QUERY = "LOAD DATA INPATH '/%s' INTO TABLE %s" % (FILE_NAME, TBL)
    LOAD_DIR_QUERY = "LOAD DATA INPATH '/%s' INTO TABLE %s" % (FILE_PATH, TBL)

    self.hdfs_client.make_dir(FILE_PATH)

    self.hdfs_client.setacl(FILE_PATH, "user::rwx,group::rwx,other::---")
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS"
                                      " functional.load_acl_permissions")
    self.execute_query_expect_success(self.client, "CREATE TABLE "
                                      "functional.load_acl_permissions (col int)")
    self.hdfs_client.delete_file_dir(FILE_NAME)
    self.hdfs_client.create_file(FILE_NAME, "1")

    self.execute_query_expect_success(self.client, LOAD_FILE_QUERY)

    # Now remove write perms from the source directory
    self.hdfs_client.create_file(FILE_NAME, "1")
    self.hdfs_client.setacl(FILE_PATH, "user::---,group::---,other::---")
    self.hdfs_client.setacl(TBL_PATH, "user::rwx,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.load_acl_permissions")
    self.execute_query_expect_failure(self.client, LOAD_FILE_QUERY)
    self.execute_query_expect_failure(self.client, LOAD_DIR_QUERY)

    # Remove write perms from target
    self.hdfs_client.setacl(FILE_PATH, "user::rwx,group::rwx,other::rwx")
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.load_acl_permissions")
    self.execute_query_expect_failure(self.client, LOAD_FILE_QUERY)
    self.execute_query_expect_failure(self.client, LOAD_DIR_QUERY)

    # Finally remove read perms from file itself
    self.hdfs_client.setacl(FILE_NAME, "user::-wx,group::rwx,other::rwx")
    self.hdfs_client.setacl(TBL_PATH, "user::rwx,group::rwx,other::rwx")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.load_acl_permissions")
    self.execute_query_expect_failure(self.client, LOAD_FILE_QUERY)
    # We expect this to succeed, it's not an error if all files in the dir cannot be read
    self.execute_query_expect_success(self.client, LOAD_DIR_QUERY)
