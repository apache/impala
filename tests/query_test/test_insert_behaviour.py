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

import getpass
import grp
import os
import pwd
import pytest
import time
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfS3, SkipIfIsilon, SkipIfLocal
from tests.util.filesystem_utils import WAREHOUSE, IS_DEFAULT_FS, get_fs_path

@SkipIfS3.insert
@SkipIfLocal.hdfs_client
class TestInsertBehaviour(ImpalaTestSuite):
  """Tests for INSERT behaviour that isn't covered by checking query results"""

  TEST_DB_NAME = "insert_empty_result_db"

  def setup_method(self, method):
    # cleanup and create a fresh test database
    if method.__name__ == "test_insert_select_with_empty_resultset":
      self.cleanup_db(self.TEST_DB_NAME)
      self.execute_query("create database if not exists {0} location '{1}/{0}.db'"
          .format(self.TEST_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    if method.__name__ == "test_insert_select_with_empty_resultset":
      self.cleanup_db(self.TEST_DB_NAME)

  @pytest.mark.execute_serially
  def test_insert_removes_staging_files(self):
    TBL_NAME = "insert_overwrite_nopart"
    insert_staging_dir = ("test-warehouse/functional.db/%s/"
        "_impala_insert_staging" % TBL_NAME)
    self.hdfs_client.delete_file_dir(insert_staging_dir, recursive=True)
    self.client.execute(("INSERT OVERWRITE functional.%s"
        " SELECT int_col FROM functional.tinyinttable" % TBL_NAME))
    ls = self.hdfs_client.list_dir(insert_staging_dir)
    assert len(ls['FileStatuses']['FileStatus']) == 0

  @pytest.mark.execute_serially
  def test_insert_preserves_hidden_files(self):
    """Test that INSERT OVERWRITE preserves hidden files in the root table directory"""
    TBL_NAME = "insert_overwrite_nopart"
    table_dir = "test-warehouse/functional.db/%s/" % TBL_NAME
    hidden_file_locations = [".hidden", "_hidden"]
    dir_locations = ["dir", ".hidden_dir"]
    for dir_ in dir_locations:
      self.hdfs_client.make_dir(table_dir + dir_)
    for file_ in hidden_file_locations:
      self.hdfs_client.create_file(table_dir + file_, '', overwrite=True)

    self.client.execute(("INSERT OVERWRITE functional.%s"
        " SELECT int_col FROM functional.tinyinttable" % TBL_NAME))

    for file_ in hidden_file_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file_)
      except:
        err_msg = "Hidden file '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + file_))

    for dir_ in dir_locations:
      try:
        self.hdfs_client.get_file_dir_status(table_dir + file_)
      except:
        err_msg = "Directory '%s' was unexpectedly deleted by INSERT OVERWRITE"
        pytest.fail(err_msg % (table_dir + dir_))

  @pytest.mark.execute_serially
  def test_insert_alter_partition_location(self):
    """Test that inserts after changing the location of a partition work correctly,
    including the creation of a non-existant partition dir"""
    PART_DIR = "tmp/test_insert_alter_partition_location"
    QUALIFIED_PART_DIR = get_fs_path('/' + PART_DIR)
    TBL_NAME = "functional.insert_alter_partition_location"

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME)
    self.hdfs_client.delete_file_dir(PART_DIR, recursive=True)

    self.execute_query_expect_success(self.client,
        "CREATE TABLE  %s (c int) PARTITIONED BY (p int)" % TBL_NAME)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s ADD PARTITION(p=1)" % TBL_NAME)
    self.execute_query_expect_success(self.client,
        "ALTER TABLE %s PARTITION(p=1) SET LOCATION '%s'" %
        (TBL_NAME, QUALIFIED_PART_DIR))
    self.execute_query_expect_success(self.client,
        "INSERT OVERWRITE %s PARTITION(p=1) VALUES(1)" % TBL_NAME)

    result = self.execute_query_expect_success(self.client,
        "SELECT COUNT(*) FROM %s" % TBL_NAME)
    assert int(result.get_data()) == 1

    # Should have created the partition dir, which should contain exactly one file (not in
    # a subdirectory)
    ls = self.hdfs_client.list_dir(PART_DIR)
    assert len(ls['FileStatuses']['FileStatus']) == 1

  @SkipIfIsilon.hdfs_acls
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

  @SkipIfIsilon.hdfs_acls
  @pytest.mark.execute_serially
  def test_insert_file_permissions(self):
    """Test that INSERT correctly respects file permission (minimum ACLs)"""
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
    # Should be writable because 'other' ACLs allow writes
    self.execute_query_expect_success(self.client, INSERT_QUERY)

  @SkipIfIsilon.hdfs_acls
  @pytest.mark.execute_serially
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

    USER = getpass.getuser()
    GROUP = grp.getgrgid(pwd.getpwnam(USER).pw_gid).gr_name
    # First, change the owner to someone other than user who runs impala service
    self.hdfs_client.chown(TBL_PATH, "another_user", GROUP)

    # Remove the permission to write and confirm that INSERTs won't work
    self.hdfs_client.setacl(TBL_PATH,
                            "user::r-x,user:" + USER + ":r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Add the permission to write. if we're not the owner of the file, INSERTs should work
    self.hdfs_client.setacl(TBL_PATH,
                            "user::r-x,user:" + USER + ":rwx,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_success(self.client, INSERT_QUERY)

    # Now add group access, still should fail (because the user will match and take
    # priority)
    self.hdfs_client.setacl(TBL_PATH,
                            "user::r-x,user:" + USER + ":r-x,group::rwx,other::r-x")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Check that the mask correctly applies to the anonymous group ACL
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,group::rwx,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because mask applies to unnamed group and disables writing
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Check that the mask correctly applies to the named user ACL
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,user:" + USER +
                            ":rwx,group::r-x,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because mask applies to named user and disables writing
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Now make the target directory non-writable with posix permissions, but writable with
    # ACLs (ACLs should take priority). Note: chmod affects ACLs (!) so it has to be done
    # first.
    self.hdfs_client.chmod(TBL_PATH, "000")
    self.hdfs_client.setacl(TBL_PATH, "user::rwx,user:foo:rwx,group::rwx,other::r-x")

    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    self.execute_query_expect_success(self.client, INSERT_QUERY)

    # Finally, change the owner/group
    self.hdfs_client.chown(TBL_PATH, "test_user", "invalid")

    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be unwritable because 'other' ACLs don't allow writes
    self.execute_query_expect_failure(self.client, INSERT_QUERY)

    # Give write perms back to 'other'
    self.hdfs_client.setacl(TBL_PATH, "user::r-x,user:foo:rwx,group::r-x,other::rwx")
    self.execute_query_expect_success(self.client,
                                      "REFRESH functional.insert_acl_permissions")
    # Should be writable because 'other' ACLs allow writes
    self.execute_query_expect_success(self.client, INSERT_QUERY)

  @SkipIfIsilon.hdfs_acls
  @pytest.mark.execute_serially
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

  @pytest.mark.execute_serially
  def test_insert_select_with_empty_resultset(self):
    """Test insert/select query won't trigger partition directory or zero size data file
    creation if the resultset of select is empty."""
    def check_path_exists(path, should_exist):
      fail = None
      try:
        self.hdfs_client.get_file_dir_status(path)
        if not should_exist:
          pytest.fail("file/dir '%s' unexpectedly exists" % path)
      except Exception, e:
        if should_exist:
          pytest.fail("file/dir '%s' does not exist" % path)

    db_path = "test-warehouse/%s.db/" % self.TEST_DB_NAME
    table_path = db_path + "test_insert_empty_result"
    partition_path = "{0}/year=2009/month=1".format(table_path)
    check_path_exists(table_path, False)

    table_name = self.TEST_DB_NAME + ".test_insert_empty_result"
    self.execute_query_expect_success(self.client, ("CREATE TABLE %s (id INT, col INT)"
        " PARTITIONED BY (year INT, month INT)" % table_name))
    check_path_exists(table_path, True)
    check_path_exists(partition_path, False)

    # Run an insert/select stmt that returns an empty resultset.
    insert_query = ("INSERT INTO TABLE {0} PARTITION(year=2009, month=1)"
        "select 1, 1 from {0} LIMIT 0".format(table_name))
    self.execute_query_expect_success(self.client, insert_query)
    # Partition directory should not be created
    check_path_exists(partition_path, False)

    # Insert one record
    insert_query_one_row = ("INSERT INTO TABLE %s PARTITION(year=2009, month=1) "
        "values(2, 2)" % table_name)
    self.execute_query_expect_success(self.client, insert_query_one_row)
    # Partition directory should be created with one data file
    check_path_exists(partition_path, True)
    ls = self.hdfs_client.list_dir(partition_path)
    assert len(ls['FileStatuses']['FileStatus']) == 1

    # Run an insert/select statement that returns an empty resultset again
    self.execute_query_expect_success(self.client, insert_query)
    # No new data file should be created
    new_ls = self.hdfs_client.list_dir(partition_path)
    assert len(new_ls['FileStatuses']['FileStatus']) == 1
    assert new_ls['FileStatuses'] == ls['FileStatuses']

    # Run an insert overwrite/select that returns an empty resultset
    insert_query = ("INSERT OVERWRITE {0} PARTITION(year=2009, month=1)"
                    " select 1, 1 from  {0} LIMIT 0".format(table_name))
    self.execute_query_expect_success(self.client, insert_query)
    # Data file should be deleted
    new_ls2 = self.hdfs_client.list_dir(partition_path)
    assert len(new_ls2['FileStatuses']['FileStatus']) == 0
    assert new_ls['FileStatuses'] != new_ls2['FileStatuses']

    # Test for IMPALA-2008 insert overwrite to an empty table with empty dataset
    empty_target_tbl = "test_overwrite_with_empty_target"
    create_table = "create table %s.%s (id INT, col INT)" % (self.TEST_DB_NAME,
                                                             empty_target_tbl)
    self.execute_query_expect_success(self.client, create_table)
    insert_query = ("INSERT OVERWRITE {0}.{1} select 1, 1 from  {0}.{1} LIMIT 0"
                    .format(self.TEST_DB_NAME, empty_target_tbl))
    self.execute_query_expect_success(self.client, insert_query)

    # Delete target table directory, query should fail with
    # "No such file or directory" error
    target_table_path = "%s%s" % (db_path, empty_target_tbl)
    self.hdfs_client.delete_file_dir(target_table_path, recursive=True)
    self.execute_query_expect_failure(self.client, insert_query)

  @SkipIfIsilon.hdfs_acls
  @pytest.mark.execute_serially
  def test_multiple_group_acls(self):
    """Test that INSERT correctly respects multiple group ACLs"""
    TBL = "functional.insert_group_acl_permissions"
    TBL_PATH = "test-warehouse/functional.db/insert_group_acl_permissions"
    INSERT_QUERY = "INSERT INTO %s VALUES(1)" % TBL

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS " + TBL)
    self.execute_query_expect_success(self.client, "CREATE TABLE %s (col int)" % TBL)

    USER = getpass.getuser()
    TEST_USER = "test_user"
    # Get the list of all groups of USER except the user's owning group.
    OWNINGROUP = grp.getgrgid(pwd.getpwnam(USER).pw_gid).gr_name
    GROUPS = [g.gr_name for g in grp.getgrall() if USER in g.gr_mem]
    if (len(GROUPS) < 1):
      pytest.xfail(reason="Cannot run test, user belongs to only one group.")

    # First, change the owner to someone other than user who runs impala service
    self.hdfs_client.chown(TBL_PATH, "another_user", OWNINGROUP)

    # Set two group ACLs, one contains requested permission, the other doesn't.
    self.hdfs_client.setacl(TBL_PATH,
        "user::r-x,user:{0}:r-x,group::---,group:{1}:rwx,other::r-x"
            .format(TEST_USER, GROUPS[0]))
    self.execute_query_expect_success(self.client, "REFRESH " + TBL)
    self.execute_query_expect_success(self.client, INSERT_QUERY)

    # Two group ACLs but with mask to deny the permission.
    self.hdfs_client.setacl(TBL_PATH,
        "user::r-x,group::r--,group:{0}:rwx,mask::r-x,other::---".format(GROUPS[0]))
    self.execute_query_expect_success(self.client, "REFRESH " + TBL)
    self.execute_query_expect_failure(self.client, INSERT_QUERY)
