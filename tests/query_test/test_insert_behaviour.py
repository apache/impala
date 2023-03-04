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
import getpass
import grp
import os
import pwd
import pytest
import re

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import (SkipIfFS, SkipIfLocal, SkipIfDockerizedCluster,
    SkipIfCatalogV2)
from tests.util.filesystem_utils import WAREHOUSE, IS_S3


@SkipIfLocal.hdfs_client
class TestInsertBehaviour(ImpalaTestSuite):
  """Tests for INSERT behaviour that isn't covered by checking query results"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  TEST_DB_NAME = "insert_empty_result_db"

  def setup_method(self, method):
    # cleanup and create a fresh test database
    if method.__name__ == "test_insert_select_with_empty_resultset":
      self.cleanup_db(self.TEST_DB_NAME)
      self.execute_query("create database if not exists {0} location "
                         "'{1}/{0}.db'".format(self.TEST_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    if method.__name__ == "test_insert_select_with_empty_resultset":
      self.cleanup_db(self.TEST_DB_NAME)

  @SkipIfFS.eventually_consistent
  @pytest.mark.execute_serially
  def test_insert_removes_staging_files(self):
    TBL_NAME = "insert_overwrite_nopart"
    insert_staging_dir = ("%s/functional.db/%s/"
        "_impala_insert_staging" % (WAREHOUSE, TBL_NAME))
    self.filesystem_client.delete_file_dir(insert_staging_dir, recursive=True)
    self.client.execute("INSERT OVERWRITE functional.%s "
                        "SELECT int_col FROM functional.tinyinttable" % TBL_NAME)
    ls = self.filesystem_client.ls(insert_staging_dir)
    assert len(ls) == 0

  @pytest.mark.execute_serially
  def test_insert_preserves_hidden_files(self):
    """Test that INSERT OVERWRITE preserves hidden files in the root table directory"""
    TBL_NAME = "insert_overwrite_nopart"
    table_dir = "%s/functional.db/%s/" % (WAREHOUSE, TBL_NAME)
    hidden_file_locations = [".hidden", "_hidden"]
    dir_locations = ["dir", ".hidden_dir"]

    for dir_ in dir_locations:
      self.filesystem_client.make_dir(table_dir + dir_)

    # We do this here because the above 'make_dir' call doesn't make a directory for S3.
    for dir_ in dir_locations:
      self.filesystem_client.create_file(
          table_dir + dir_ + '/' + hidden_file_locations[0] , '', overwrite=True)

    for file_ in hidden_file_locations:
      self.filesystem_client.create_file(table_dir + file_, '', overwrite=True)

    self.client.execute("INSERT OVERWRITE functional.%s"
                        " SELECT int_col FROM functional.tinyinttable" % TBL_NAME)

    for file_ in hidden_file_locations:
      assert self.filesystem_client.exists(table_dir + file_), "Hidden file {0} was " \
          "unexpectedly deleted by INSERT OVERWRITE".format(table_dir + file_)

    for dir_ in dir_locations:
      assert self.filesystem_client.exists(table_dir + dir_), "Directory {0} was " \
          "unexpectedly deleted by INSERT OVERWRITE".format(table_dir + dir_)

  def test_insert_ascii_nulls(self, unique_database):
    TBL_NAME = '`{0}`.`null_insert`'.format(unique_database)
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME)
    self.execute_query_expect_success(self.client, "create table %s as select '\0' s"
        % TBL_NAME)
    result = self.execute_query_expect_success(self.client,
        "SELECT LENGTH(s) FROM %s" % TBL_NAME)
    assert int(result.get_data()) == 1

  @UniqueDatabase.parametrize(name_prefix='test_insert_alter_partition_location_db')
  def test_insert_alter_partition_location(self, unique_database):
    """Test that inserts after changing the location of a partition work correctly,
    including the creation of a non-existant partition dir"""
    # Moved to WAREHOUSE for Ozone because it does not allow rename between buckets.
    work_dir = "%s/tmp" % WAREHOUSE
    self.filesystem_client.make_dir(work_dir)
    part_dir = os.path.join(work_dir, unique_database)
    table_name = "`{0}`.`insert_alter_partition_location`".format(unique_database)

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % table_name)
    self.filesystem_client.delete_file_dir(part_dir, recursive=True)

    self.execute_query_expect_success(
        self.client,
        "CREATE TABLE  %s (c int) PARTITIONED BY (p int)" % table_name)
    self.execute_query_expect_success(
        self.client,
        "ALTER TABLE %s ADD PARTITION(p=1)" % table_name)
    self.execute_query_expect_success(
        self.client,
        "ALTER TABLE %s PARTITION(p=1) SET LOCATION '%s'" % (table_name,
                                                             part_dir))
    self.execute_query_expect_success(
        self.client,
        "INSERT OVERWRITE %s PARTITION(p=1) VALUES(1)" % table_name)

    result = self.execute_query_expect_success(
        self.client,
        "SELECT COUNT(*) FROM %s" % table_name)
    assert int(result.get_data()) == 1

    # Should have created the partition dir, which should contain exactly one file (not in
    # a subdirectory)
    assert len(self.filesystem_client.ls(part_dir)) == 1

  @SkipIfFS.hdfs_acls
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

  @SkipIfFS.hdfs_acls
  @SkipIfCatalogV2.impala_7539()
  def test_insert_file_permissions(self, unique_database):
    """Test that INSERT correctly respects file permission (minimum ACLs)"""
    table = "`{0}`.`insert_acl_permissions`".format(unique_database)
    table_path = "test-warehouse/{0}.db/insert_acl_permissions".format(unique_database)

    insert_query = "INSERT INTO {0} VALUES(1)".format(table)
    refresh_query = "REFRESH {0}".format(table)

    self.execute_query_expect_success(self.client,
                                      "DROP TABLE IF EXISTS {0}".format(table))
    self.execute_query_expect_success(
        self.client, "CREATE TABLE {0} (col int)".format(table))

    # Check that a simple insert works
    self.execute_query_expect_success(self.client, insert_query)

    # Remove the permission to write and confirm that INSERTs won't work
    self.hdfs_client.setacl(table_path, "user::r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, insert_query)

    # Now add group access, still should fail (because the user will match and take
    # priority)
    self.hdfs_client.setacl(table_path, "user::r-x,group::rwx,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, insert_query)

    # Now make the target directory non-writable with posix permissions, but writable with
    # ACLs (ACLs should take priority). Note: chmod affects ACLs (!) so it has to be done
    # first.
    self.hdfs_client.chmod(table_path, "000")
    self.hdfs_client.setacl(table_path, "user::rwx,group::r-x,other::r-x")

    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_success(self.client, insert_query)

    # Finally, change the owner
    self.hdfs_client.chown(table_path, "another_user", "another_group")
    self.execute_query_expect_success(self.client, refresh_query)
    # Should be unwritable because 'other' ACLs don't allow writes
    self.execute_query_expect_failure(self.client, insert_query)

    # Give write perms back to 'other'
    self.hdfs_client.setacl(table_path, "user::rwx,group::r-x,other::rwx")
    self.execute_query_expect_success(self.client, refresh_query)
    # Should be writable because 'other' ACLs allow writes
    self.execute_query_expect_success(self.client, insert_query)

  @SkipIfFS.hdfs_acls
  @SkipIfCatalogV2.impala_7539()
  def test_mixed_partition_permissions(self, unique_database):
    """
    Test that INSERT and LOAD DATA into explicit partitions is allowed even
    if some partitions exist without appropriate permissions.
    """
    table = "`{0}`.`insert_partition_perms`".format(unique_database)
    table_path = "test-warehouse/{0}.db/insert_partition_perms".format(unique_database)

    def partition_path(partition):
      return "%s/p=%s" % (table_path, partition)

    def insert_query(partition):
      return "INSERT INTO {0} PARTITION (p='{1}') VALUES(1)".format(table, partition)

    def load_data(query_fn, partition):
      path = "tmp/{0}/data_file".format(unique_database)
      self.hdfs_client.delete_file_dir(path)
      self.hdfs_client.create_file(path, "1")
      q = "LOAD DATA INPATH '/{0}' INTO TABLE {1} PARTITION (p='{2}')".format(
          path, table, partition)
      return query_fn(self.client, q)

    insert_dynamic_partition = \
        "INSERT INTO {0} (col, p) SELECT col, p from {0}".format(table)

    insert_dynamic_partition_2 = \
        "INSERT INTO {0} PARTITION(p) SELECT col, p from {0}".format(table)

    # TODO(todd): REFRESH does not seem to refresh the permissions of existing
    # partitions -- need to fully reload the metadata to do so.
    invalidate_query = "INVALIDATE METADATA {0}".format(table)

    self.execute_query_expect_success(self.client,
                                      "DROP TABLE IF EXISTS {0}".format(table))
    self.execute_query_expect_success(
        self.client, "CREATE TABLE {0} (col int) PARTITIONED BY (p string)".format(table))

    # Check that a simple insert works
    self.execute_query_expect_success(self.client, insert_query("initial_part"))

    # Add a partition with no write permissions
    self.execute_query_expect_success(
        self.client,
        "ALTER TABLE {0} ADD PARTITION (p=\"no_write\")".format(table))
    self.hdfs_client.chown(partition_path("no_write"), "another_user", "another_group")
    self.execute_query_expect_success(self.client, invalidate_query)

    # Check that inserts into the new partition do not work.
    err = self.execute_query_expect_failure(self.client, insert_query("no_write"))
    assert re.search(r'Impala does not have WRITE access.*p=no_write', str(err))

    err = load_data(self.execute_query_expect_failure, "no_write")
    assert re.search(r'Impala does not have WRITE access.*p=no_write', str(err))

    # Inserts into the existing partition should still work
    self.execute_query_expect_success(self.client, insert_query("initial_part"))
    load_data(self.execute_query_expect_success, "initial_part")

    # Inserts into newly created partitions should work.
    self.execute_query_expect_success(self.client, insert_query("added_part"))

    # TODO(IMPALA-7313): loading data cannot currently add partitions, so this
    # is commented out. If that behavior is changed, uncomment this.
    # load_data(self.execute_query_expect_success, "added_part_load")

    # Inserts into dynamic partitions should fail.
    err = self.execute_query_expect_failure(self.client, insert_dynamic_partition)
    assert re.search(r'Impala does not have WRITE access.*p=no_write', str(err))
    err = self.execute_query_expect_failure(self.client, insert_dynamic_partition_2)
    assert re.search(r'Impala does not have WRITE access.*p=no_write', str(err))

    # If we make the top-level table directory read-only, we should still be able to
    # insert into existing writable partitions.
    self.hdfs_client.chown(table_path, "another_user", "another_group")
    self.execute_query_expect_success(self.client, insert_query("added_part"))
    load_data(self.execute_query_expect_success, "added_part")

  @SkipIfFS.hdfs_acls
  @SkipIfCatalogV2.impala_7539()
  def test_readonly_table_dir(self, unique_database):
    """
    Test that, if a partitioned table has a read-only base directory,
    the frontend rejects queries that may attempt to create new
    partitions.
    """
    table = "`{0}`.`insert_partition_perms`".format(unique_database)
    table_path = "test-warehouse/{0}.db/insert_partition_perms".format(unique_database)
    insert_dynamic_partition = \
        "INSERT INTO {0} (col, p) SELECT col, p from {0}".format(table)

    def insert_query(partition):
      return "INSERT INTO {0} PARTITION (p='{1}') VALUES(1)".format(table, partition)

    self.execute_query_expect_success(self.client,
                                      "DROP TABLE IF EXISTS {0}".format(table))
    self.execute_query_expect_success(
        self.client, "CREATE TABLE {0} (col int) PARTITIONED BY (p string)".format(table))

    # If we make the top-level table directory read-only, we should not be able
    # to create new partitions, either explicitly, or dynamically.
    self.hdfs_client.chown(table_path, "another_user", "another_group")
    self.execute_query_expect_success(self.client, "INVALIDATE METADATA %s" % table)
    err = self.execute_query_expect_failure(self.client, insert_dynamic_partition)
    assert re.search(r'Impala does not have WRITE access.*' + table_path, str(err))
    err = self.execute_query_expect_failure(self.client, insert_query("some_new_part"))
    assert re.search(r'Impala does not have WRITE access.*' + table_path, str(err))

  @SkipIfFS.hdfs_acls
  @SkipIfDockerizedCluster.insert_acls
  @SkipIfCatalogV2.impala_7539()
  def test_insert_acl_permissions(self, unique_database):
    """Test that INSERT correctly respects ACLs"""
    table = "`{0}`.`insert_acl_permissions`".format(unique_database)
    table_path = "test-warehouse/{0}.db/insert_acl_permissions".format(unique_database)

    insert_query = "INSERT INTO %s VALUES(1)" % table
    refresh_query = "REFRESH {0}".format(table)

    self.execute_query_expect_success(self.client,
                                      "DROP TABLE IF EXISTS {0}".format(table))
    self.execute_query_expect_success(self.client,
                                      "CREATE TABLE {0} (col int)".format(table))

    # Check that a simple insert works
    self.execute_query_expect_success(self.client, insert_query)

    user = getpass.getuser()
    group = grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
    # First, change the owner to someone other than user who runs impala service
    self.hdfs_client.chown(table_path, "another_user", group)

    # Remove the permission to write and confirm that INSERTs won't work
    self.hdfs_client.setacl(table_path,
                            "user::r-x,user:" + user + ":r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, insert_query)

    # Add the permission to write. if we're not the owner of the file, INSERTs should work
    self.hdfs_client.setacl(table_path,
                            "user::r-x,user:" + user + ":rwx,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_success(self.client, insert_query)

    # Now add group access, still should fail (because the user will match and take
    # priority)
    self.hdfs_client.setacl(table_path,
                            "user::r-x,user:" + user + ":r-x,group::rwx,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, insert_query)

    # Check that the mask correctly applies to the anonymous group ACL
    self.hdfs_client.setacl(table_path, "user::r-x,group::rwx,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client, refresh_query)
    # Should be unwritable because mask applies to unnamed group and disables writing
    self.execute_query_expect_failure(self.client, insert_query)

    # Check that the mask correctly applies to the named user ACL
    self.hdfs_client.setacl(table_path, "user::r-x,user:" + user +
                            ":rwx,group::r-x,other::rwx,mask::r--")
    self.execute_query_expect_success(self.client, refresh_query)
    # Should be unwritable because mask applies to named user and disables writing
    self.execute_query_expect_failure(self.client, insert_query)

    # Now make the target directory non-writable with posix permissions, but writable with
    # ACLs (ACLs should take priority). Note: chmod affects ACLs (!) so it has to be done
    # first.
    self.hdfs_client.chmod(table_path, "000")
    self.hdfs_client.setacl(table_path, "user::rwx,user:foo:rwx,group::rwx,other::r-x")

    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_success(self.client, insert_query)

    # Finally, change the owner/group
    self.hdfs_client.chown(table_path, "test_user", "invalid")

    self.execute_query_expect_success(self.client, refresh_query)
    # Should be unwritable because 'other' ACLs don't allow writes
    self.execute_query_expect_failure(self.client, insert_query)

    # Give write perms back to 'other'
    self.hdfs_client.setacl(table_path, "user::r-x,user:foo:rwx,group::r-x,other::rwx")
    self.execute_query_expect_success(self.client, refresh_query)
    # Should be writable because 'other' ACLs allow writes
    self.execute_query_expect_success(self.client, insert_query)

  @SkipIfFS.hdfs_acls
  @SkipIfCatalogV2.impala_7539()
  def test_load_permissions(self, unique_database):
    # We rely on test_insert_acl_permissions() to exhaustively check that ACL semantics
    # are correct. Here we just validate that LOADs can't be done when we cannot read from
    # or write to the src directory, or write to the dest directory.

    table = "`{0}`.`load_acl_permissions`".format(unique_database)
    table_path = "test-warehouse/{0}.db/load_acl_permissions".format(unique_database)

    file_path = "tmp/{0}".format(unique_database)
    file_name = "%s/impala_data_file" % file_path

    load_file_query = "LOAD DATA INPATH '/%s' INTO TABLE %s" % (file_name, table)
    load_dir_query = "LOAD DATA INPATH '/%s' INTO TABLE %s" % (file_path, table)

    refresh_query = "REFRESH {0}".format(table)

    self.hdfs_client.make_dir(file_path)

    self.hdfs_client.setacl(file_path, "user::rwx,group::rwx,other::---")
    self.execute_query_expect_success(self.client,
                                      "DROP TABLE IF EXISTS {0}".format(table))
    self.execute_query_expect_success(
        self.client, "CREATE TABLE {0} (col int)".format(table))
    self.hdfs_client.delete_file_dir(file_name)
    self.hdfs_client.create_file(file_name, "1")

    self.execute_query_expect_success(self.client, load_file_query)

    # Now remove write perms from the source directory
    self.hdfs_client.create_file(file_name, "1")
    self.hdfs_client.setacl(file_path, "user::---,group::---,other::---")
    self.hdfs_client.setacl(table_path, "user::rwx,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, load_file_query)
    self.execute_query_expect_failure(self.client, load_dir_query)

    # Remove write perms from target
    self.hdfs_client.setacl(file_path, "user::rwx,group::rwx,other::rwx")
    self.hdfs_client.setacl(table_path, "user::r-x,group::r-x,other::r-x")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, load_file_query)
    self.execute_query_expect_failure(self.client, load_dir_query)

    # Finally remove read perms from file itself
    self.hdfs_client.setacl(file_name, "user::-wx,group::rwx,other::rwx")
    self.hdfs_client.setacl(table_path, "user::rwx,group::rwx,other::rwx")
    self.execute_query_expect_success(self.client, refresh_query)
    self.execute_query_expect_failure(self.client, load_file_query)
    # We expect this to succeed, it's not an error if all files in the dir cannot be read
    self.execute_query_expect_success(self.client, load_dir_query)

  @SkipIfFS.eventually_consistent
  @pytest.mark.execute_serially
  def test_insert_select_with_empty_resultset(self):
    """Test insert/select query won't trigger partition directory or zero size data file
    creation if the resultset of select is empty."""
    def check_path_exists(path, should_exist):
      assert self.filesystem_client.exists(path) == should_exist, "file/dir '{0}' " \
          "should {1}exist but does {2}exist.".format(
              path, '' if should_exist else 'not ', 'not ' if should_exist else '')

    db_path = "%s/%s.db/" % (WAREHOUSE, self.TEST_DB_NAME)
    table_path = db_path + "test_insert_empty_result"
    partition_path = "{0}/year=2009/month=1".format(table_path)
    check_path_exists(table_path, False)

    table_name = self.TEST_DB_NAME + ".test_insert_empty_result"
    self.execute_query_expect_success(
        self.client,
        "CREATE TABLE %s (id INT, col INT) PARTITIONED BY (year INT, month "
        "INT)" % table_name)
    check_path_exists(table_path, True)
    check_path_exists(partition_path, False)

    # Run an insert/select stmt that returns an empty resultset.
    insert_query = ("INSERT INTO TABLE {0} PARTITION(year=2009, month=1)"
                    "select 1, 1 from {0} LIMIT 0".format(table_name))
    self.execute_query_expect_success(self.client, insert_query)
    # Partition directory is created
    check_path_exists(partition_path, True)

    # Insert one record
    insert_query_one_row = ("INSERT INTO TABLE %s PARTITION(year=2009, month=1) "
                            "values(2, 2)" % table_name)
    self.execute_query_expect_success(self.client, insert_query_one_row)
    # Partition directory should be created with one data file
    check_path_exists(partition_path, True)
    ls = (self.filesystem_client.ls(partition_path))
    assert len(ls) == 1

    # Run an insert/select statement that returns an empty resultset again
    self.execute_query_expect_success(self.client, insert_query)
    # No new data file should be created
    new_ls = self.filesystem_client.ls(partition_path)
    assert len(new_ls) == 1
    assert new_ls == ls

    # Run an insert overwrite/select that returns an empty resultset
    insert_query = ("INSERT OVERWRITE {0} PARTITION(year=2009, month=1)"
                    " select 1, 1 from  {0} LIMIT 0".format(table_name))
    self.execute_query_expect_success(self.client, insert_query)
    # Data file should be deleted
    new_ls2 = self.filesystem_client.ls(partition_path)
    assert len(new_ls2) == 0
    assert new_ls != new_ls2

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
    self.filesystem_client.delete_file_dir(target_table_path, recursive=True)
    self.execute_query_expect_failure(self.client, insert_query)

  @SkipIfFS.hdfs_acls
  @SkipIfDockerizedCluster.insert_acls
  @SkipIfCatalogV2.impala_7539()
  def test_multiple_group_acls(self, unique_database):
    """Test that INSERT correctly respects multiple group ACLs"""
    table = "`{0}`.`insert_group_acl_permissions`".format(unique_database)
    table_path = "test-warehouse/{0}.db/insert_group_acl_permissions".format(
        unique_database)
    insert_query = "INSERT INTO %s VALUES(1)" % table

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS " + table)
    self.execute_query_expect_success(self.client, "CREATE TABLE %s (col int)" % table)

    user = getpass.getuser()
    test_user = "test_user"
    # Get the list of all groups of user except the user's owning group.
    owning_group = grp.getgrgid(pwd.getpwnam(user).pw_gid).gr_name
    groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    if (len(groups) < 1):
      pytest.xfail(reason="Cannot run test, user belongs to only one group.")

    # First, change the owner to someone other than user who runs impala service
    self.hdfs_client.chown(table_path, "another_user", owning_group)

    # Set two group ACLs, one contains requested permission, the other doesn't.
    self.hdfs_client.setacl(
        table_path,
        "user::r-x,user:{0}:r-x,group::---,group:{1}:rwx,"
        "other::r-x".format(test_user, groups[0]))
    self.execute_query_expect_success(self.client, "REFRESH " + table)
    self.execute_query_expect_success(self.client, insert_query)

    # Two group ACLs but with mask to deny the permission.
    self.hdfs_client.setacl(
        table_path,
        "user::r-x,group::r--,group:{0}:rwx,mask::r-x,"
        "other::---".format(groups[0]))
    self.execute_query_expect_success(self.client, "REFRESH " + table)
    self.execute_query_expect_failure(self.client, insert_query)

  def test_clustered_partition_single_file(self, unique_database):
    """IMPALA-2523: Tests that clustered insert creates one file per partition, even when
    inserting over multiple row batches."""
    # On s3 this test takes about 220 seconds and we are unlikely to break it, so only run
    # it in exhaustive strategy.
    if self.exploration_strategy() != 'exhaustive' and IS_S3:
      pytest.skip("only runs in exhaustive")
    table = "{0}.insert_clustered".format(unique_database)
    table_path = "{1}/{0}.db/insert_clustered".format(unique_database, WAREHOUSE)

    create_stmt = """create table {0} like functional.alltypes""".format(table)
    self.execute_query_expect_success(self.client, create_stmt)

    set_location_stmt = """alter table {0} set location '{1}'""".format(
        table, table_path)
    self.execute_query_expect_success(self.client, set_location_stmt)

    # Setting a lower batch size will result in multiple row batches being written.
    self.execute_query_expect_success(self.client, "set batch_size=10")

    insert_stmt = """insert into {0} partition(year, month) /*+ clustered,shuffle */
                     select * from functional.alltypes""".format(table)
    self.execute_query_expect_success(self.client, insert_stmt)

    # We expect exactly one partition per year and month, since subsequent row batches of
    # a partition will be written into the same file.
    expected_partitions = \
        ["year=%s/month=%s" % (y, m) for y in [2009, 2010] for m in range(1,13)]

    for partition in expected_partitions:
      partition_path = "{0}/{1}".format(table_path, partition)
      files = self.filesystem_client.ls(partition_path)
      assert len(files) == 1, "%s: %s" % (partition, files)

  def test_clustered_partition_multiple_files(self, unique_database):
    """IMPALA-2523: Tests that clustered insert creates the right number of files per
    partition when inserting over multiple row batches."""
    # This test takes about 30 seconds and we are unlikely to break it, so only run it in
    # exhaustive strategy.
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("only runs in exhaustive")
    table = "{0}.insert_clustered".format(unique_database)
    table_path = "{1}/{0}.db/insert_clustered".format(unique_database, WAREHOUSE)

    create_stmt = """create table {0} (
                     l_orderkey BIGINT,
                     l_partkey BIGINT,
                     l_suppkey BIGINT,
                     l_linenumber INT,
                     l_quantity DECIMAL(12,2),
                     l_extendedprice DECIMAL(12,2),
                     l_discount DECIMAL(12,2),
                     l_tax DECIMAL(12,2),
                     l_linestatus STRING,
                     l_shipdate STRING,
                     l_commitdate STRING,
                     l_receiptdate STRING,
                     l_shipinstruct STRING,
                     l_shipmode STRING,
                     l_comment STRING)
                     partitioned by (l_returnflag STRING) stored as parquet
                     """.format(table)
    self.execute_query_expect_success(self.client, create_stmt)

    set_location_stmt = """alter table {0} set location '{1}'""".format(
        table, table_path)
    self.execute_query_expect_success(self.client, set_location_stmt)

    # Setting a lower parquet file size will result in multiple files being written.
    self.execute_query_expect_success(self.client, "set parquet_file_size=10485760")

    insert_stmt = """insert into {0} partition(l_returnflag) /*+ clustered,shuffle */
        select l_orderkey,
               l_partkey,
               l_suppkey,
               l_linenumber,
               l_quantity,
               l_extendedprice,
               l_discount,
               l_tax,
               l_linestatus,
               l_shipdate,
               l_commitdate,
               l_receiptdate,
               l_shipinstruct,
               l_shipmode,
               l_comment,
               l_returnflag
               from tpch_parquet.lineitem""".format(table)
    self.execute_query_expect_success(self.client, insert_stmt)

    expected_partition_files = [("l_returnflag=A", 3, 30),
                                ("l_returnflag=N", 3, 30),
                                ("l_returnflag=R", 3, 30)]

    for partition, min_files, max_files in expected_partition_files:
      partition_path = "{0}/{1}".format(table_path, partition)
      files = self.filesystem_client.ls(partition_path)
      assert min_files <= len(files) and len(files) <= max_files, \
          "%s: %s" % (partition, files)
