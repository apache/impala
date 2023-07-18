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
#
# Client tests for SQL statement authorization

from __future__ import absolute_import, division, print_function
from builtins import map, range
import os
import grp
import json
import pytest
import logging
import requests
from subprocess import check_call

from getpass import getuser
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import copy_files_to_hdfs_dir
from tests.common.skip import SkipIfFS, SkipIfHive2, SkipIf
from tests.common.test_dimensions import (create_client_protocol_dimension,
    create_exec_option_dimension, create_orc_dimension)
from tests.util.hdfs_util import NAMENODE
from tests.util.calculation_util import get_random_id
from tests.util.filesystem_utils import WAREHOUSE_PREFIX, WAREHOUSE
from tests.util.iceberg_util import get_snapshots

ADMIN = "admin"
OWNER_USER = getuser()
NON_OWNER = "non_owner"
ERROR_GRANT = "User doesn't have necessary permission to grant access"
ERROR_REVOKE = "User doesn't have necessary permission to revoke access"

RANGER_AUTH = ("admin", "admin")
RANGER_HOST = "http://localhost:6080"
REST_HEADERS = {"Content-Type": "application/json", "Accept": "application/json"}
IMPALAD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
               "--ranger_app_id=impala --authorization_provider=ranger"
CATALOGD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
                "--ranger_app_id=impala --authorization_provider=ranger"

LOCAL_CATALOG_IMPALAD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
    "--ranger_app_id=impala --authorization_provider=ranger --use_local_catalog=true"
LOCAL_CATALOG_CATALOGD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
    "--ranger_app_id=impala --authorization_provider=ranger --catalog_topic_mode=minimal"

LOG = logging.getLogger('impala_test_suite')


class TestRanger(CustomClusterTestSuite):
  """
  Tests for Apache Ranger integration with Apache Impala.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_revoke_with_catalog_v1(self, unique_name):
    """Tests grant/revoke with catalog v1."""
    self._test_grant_revoke(unique_name, [None, "invalidate metadata",
                                          "refresh authorization"])

  @pytest.mark.execute_serially
  @SkipIfFS.hdfs_acls
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_insert_with_catalog_v1(self, unique_name):
    """
    Test that when Ranger is the authorization provider in the legacy catalog mode,
    Impala does not throw an AnalysisException when an authorized user tries to execute
    an INSERT query against a partitioned table of which the respective table path and
    the partition path are not writable according to HDFS permission.
    """
    user = getuser()
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    partition_column = "year"
    partition_value = "2008"
    table_path = "test-warehouse/{0}.db/{1}".format(unique_database, unique_table)
    table_partition_path = "{0}/{1}={2}"\
        .format(table_path, partition_column, partition_value)
    insert_statement = "insert into {0}.{1} (name) partition ({2}) " \
        "values (\"Adam\", {3})".format(unique_database, unique_table, partition_column,
        partition_value)
    authz_err = "AuthorizationException: User '{0}' does not have privileges to " \
        "execute 'INSERT' on: {1}.{2}".format(user, unique_database, unique_table)
    try:
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create table {0}.{1} (name string) partitioned by ({2} int)"
          .format(unique_database, unique_table, partition_column), user=ADMIN)
      admin_client.execute("alter table {0}.{1} add partition ({2}={3})"
          .format(unique_database, unique_table, partition_column, partition_value),
          user=ADMIN)

      # Change the owner user and group of the HDFS paths corresponding to the table and
      # the partition so that according to Impala's FsPermissionChecker, the table is not
      # writable to the user that loads the table. This user usually is the one
      # representing the Impala service. Before IMPALA-11871, changing either the table
      # path or the partition path to non-writable would result in an AnalysisException.
      self.hdfs_client.chown(table_path, "another_user", "another_group")
      self.hdfs_client.chown(table_partition_path, "another_user", "another_group")
      # Invalidate the table metadata to force the catalog server to reload the HDFS
      # table and the related partition(s).
      admin_client.execute("invalidate metadata {0}.{1}"
          .format(unique_database, unique_table), user=ADMIN)

      # Verify that the INSERT statement fails with AuthorizationException because the
      # requesting user does not have the INSERT privilege on the table.
      result = self._run_query_as_user(insert_statement, user, False)
      assert authz_err in str(result)

      admin_client.execute("grant insert on table {0}.{1} to user {2}"
          .format(unique_database, unique_table, user), user=ADMIN)
      # Verify that the INSERT statement succeeds without AnalysisException.
      self._run_query_as_user(insert_statement, user, True)
    finally:
      admin_client.execute("revoke insert on table {0}.{1} from user {2}"
          .format(unique_database, unique_table, user))
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)

  @pytest.mark.execute_serially
  @SkipIfFS.hdfs_acls
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_load_data_with_catalog_v1(self, unique_name):
    """
    Test that when Ranger is the authorization provider in the legacy catalog mode,
    Impala does not throw an AnalysisException when an authorized user tries to execute
    a LOAD DATA query against a table partition of which the respective partition path is
    not writable according to Impala's FsPermissionChecker.
    """
    user = getuser()
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    partition_column = "year"
    partition_value = "2008"
    destination_table_path = "test-warehouse/{0}.db/{1}" \
        .format(unique_database, unique_table, )
    destination_table_partition_path = "{0}/{1}={2}"\
        .format(destination_table_path, partition_column, partition_value)
    file_name = "load_data_with_catalog_v1.txt"
    files_for_table = ["testdata/data/{0}".format(file_name)]
    source_hdfs_dir = "/tmp"
    load_data_statement = "load data inpath '{0}/{1}' into table {2}.{3} " \
        "partition ({4}={5})".format(source_hdfs_dir, file_name, unique_database,
        unique_table, partition_column, partition_value)
    authz_err = "AuthorizationException: User '{0}' does not have privileges to " \
        "execute 'INSERT' on: {1}.{2}".format(user, unique_database, unique_table)
    try:
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      copy_files_to_hdfs_dir(files_for_table, source_hdfs_dir)
      admin_client.execute("create table {0}.{1} (name string) partitioned by ({2} int) "
          "row format delimited fields terminated by ',' "
          "stored as textfile".format(unique_database, unique_table, partition_column),
          user=ADMIN)
      # We need to add the partition. Otherwise, the LOAD DATA statement can't create new
      # partitions.
      admin_client.execute("alter table {0}.{1} add partition ({2}={3})"
          .format(unique_database, unique_table, partition_column, partition_value),
          user=ADMIN)

      # Change the permissions of the HDFS path of the destination table partition.
      # Before IMPALA-11871, even we changed the table path to non-writable, loading
      # data into the partition was still allowed if the destination partition path
      # was writable according to Impala's FsPermissionChecker. But if the destination
      # partition path was not writable, an AnalysisException would be thrown.
      self.hdfs_client.chown(destination_table_partition_path, "another_user",
          "another_group")
      # Invalidate the table metadata to force the catalog server to reload the HDFS
      # table and the related partition(s).
      admin_client.execute("invalidate metadata {0}.{1}"
          .format(unique_database, unique_table), user=ADMIN)

      # To execute the LOAD DATA statement, a user has to be granted the ALL privilege
      # on the source HDFS path and the INSERT privilege on the destination table.
      # The following verifies the LOAD DATA statement fails with AuthorizationException
      # due to insufficient privileges.
      result = self._run_query_as_user(load_data_statement, user, False)
      assert authz_err in str(result)

      admin_client.execute("grant all on uri '{0}/{1}' to user {2}"
          .format(source_hdfs_dir, file_name, user), user=ADMIN)
      # The following verifies the ALL privilege on the source file alone is not
      # sufficient to execute the LOAD DATA statement.
      result = self._run_query_as_user(load_data_statement, user, False)
      assert authz_err in str(result)

      admin_client.execute("grant insert on table {0}.{1} to user {2}"
          .format(unique_database, unique_table, user), user=ADMIN)
      # Verify the LOAD DATA statement fails without AnalysisException.
      self._run_query_as_user(load_data_statement, user, True)
    finally:
      admin_client.execute("revoke all on uri '{0}/{1}' from user {2}"
          .format(source_hdfs_dir, file_name, user), user=ADMIN)
      admin_client.execute("revoke insert on table {0}.{1} from user {2}"
          .format(unique_database, unique_table, user), user=ADMIN)
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      self.filesystem_client.delete_file_dir("{0}/{1}"
          .format(source_hdfs_dir, file_name))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} {1}".format(IMPALAD_ARGS, "--use_local_catalog=true"),
    catalogd_args="{0} {1}".format(CATALOGD_ARGS, "--catalog_topic_mode=minimal"))
  def test_grant_revoke_with_local_catalog(self, unique_name):
    """Tests grant/revoke with catalog v2 (local catalog)."""
    self._test_grant_revoke(unique_name, [None, "invalidate metadata",
                                          "refresh authorization"])

  def _test_grant_revoke(self, unique_name, refresh_statements):
    user = getuser()
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    group = grp.getgrnam(getuser()).gr_name
    test_data = [(user, "USER"), (group, "GROUP")]

    for refresh_stmt in refresh_statements:
      for data in test_data:
        ident = data[0]
        kw = data[1]
        try:
          # Set-up temp database/table
          admin_client.execute("drop database if exists {0} cascade"
                               .format(unique_database), user=ADMIN)
          admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
          admin_client.execute("create table {0}.{1} (x int)"
                               .format(unique_database, unique_table), user=ADMIN)

          self.execute_query_expect_success(admin_client,
                                            "grant select on database {0} to {1} {2}"
                                            .format(unique_database, kw, ident),
                                            user=ADMIN)
          self._refresh_authorization(admin_client, refresh_stmt)
          result = self.execute_query("show grant {0} {1} on database {2}"
                                      .format(kw, ident, unique_database))
          TestRanger._check_privileges(result, [
              [kw, ident, unique_database, "", "", "", "", "", "*", "select", "false"],
              [kw, ident, unique_database, "*", "*", "", "", "", "", "select", "false"]])
          self.execute_query_expect_success(admin_client,
                                            "revoke select on database {0} from {1} "
                                            "{2}".format(unique_database, kw, ident),
                                            user=ADMIN)
          self._refresh_authorization(admin_client, refresh_stmt)
          result = self.execute_query("show grant {0} {1} on database {2}"
                                      .format(kw, ident, unique_database))
          TestRanger._check_privileges(result, [])
        finally:
          admin_client.execute("revoke select on database {0} from {1} {2}"
                               .format(unique_database, kw, ident), user=ADMIN)
          admin_client.execute("drop database if exists {0} cascade"
                               .format(unique_database), user=ADMIN)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_option(self, unique_name):
    user1 = getuser()
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"

    try:
      # Set-up temp database/table
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create table {0}.{1} (x int)"
                           .format(unique_database, unique_table), user=ADMIN)

      # Give user 1 the ability to grant select privileges on unique_database
      self.execute_query_expect_success(admin_client,
                                        "grant select on database {0} to user {1} with "
                                        "grant option".format(unique_database, user1),
                                        user=ADMIN)
      self.execute_query_expect_success(admin_client,
                                        "grant insert on database {0} to user {1} with "
                                        "grant option".format(unique_database, user1),
                                        user=ADMIN)

      # Verify user 1 has with_grant privilege on unique_database
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      TestRanger._check_privileges(result, [
          ["USER", user1, unique_database, "", "", "", "", "", "*", "insert", "true"],
          ["USER", user1, unique_database, "", "", "", "", "", "*", "select", "true"],
          ["USER", user1, unique_database, "*", "*", "", "", "", "", "insert", "true"],
          ["USER", user1, unique_database, "*", "*", "", "", "", "", "select", "true"]])

      # Revoke select privilege and check grant option is still present
      self.execute_query_expect_success(admin_client,
                                        "revoke select on database {0} from user {1}"
                                        .format(unique_database, user1), user=ADMIN)
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      # Revoking the select privilege also deprives the grantee of the permission to
      # transfer other privilege(s) on the same resource to other principals. This is a
      # current limitation of Ranger since privileges on the same resource share the same
      # delegateAdmin field in the corresponding RangerPolicyItem.
      TestRanger._check_privileges(result, [
          ["USER", user1, unique_database, "", "", "", "", "", "*", "insert", "false"],
          ["USER", user1, unique_database, "*", "*", "", "", "", "", "insert", "false"]])

      # Revoke privilege granting from user 1
      self.execute_query_expect_success(admin_client, "revoke grant option for insert "
                                                      "on database {0} from user {1}"
                                        .format(unique_database, user1), user=ADMIN)

      # User 1 can no longer grant privileges on unique_database
      # In ranger it is currently not possible to revoke grant for a single access type
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      TestRanger._check_privileges(result, [
          ["USER", user1, unique_database, "", "", "", "", "", "*", "insert", "false"],
          ["USER", user1, unique_database, "*", "*", "", "", "", "", "insert", "false"]])
    finally:
      admin_client.execute("revoke insert on database {0} from user {1}"
                           .format(unique_database, user1), user=ADMIN)
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)

  def _update_privileges_and_verify(self, admin_client, update_stmt, show_grant_stmt,
                                    expected_privileges):
    admin_client.execute(update_stmt)
    result = self.client.execute(show_grant_stmt)
    TestRanger._check_privileges(result, expected_privileges)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_show_grant(self, unique_name):
    user = getuser()
    group = grp.getgrnam(getuser()).gr_name
    test_data = [(user, "USER"), (group, "GROUP")]
    admin_client = self.create_impala_client()
    unique_db = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    udf = "identity"

    try:
      # Create test database/table
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_db), user=ADMIN)
      admin_client.execute("create table {0}.{1} (x int)"
                           .format(unique_db, unique_table), user=ADMIN)

      for data in test_data:
        # Test basic show grant functionality for user/group
        self._test_show_grant_basic(admin_client, data[1], data[0], unique_db,
                                    unique_table, udf)
        # Test that omitting ON <resource> results in failure
        self._test_show_grant_without_on(data[1], data[0])

        # Test inherited privileges (server privileges show for database, etc.)
        self._test_show_grant_inherited(admin_client, data[1], data[0], unique_db,
                                        unique_table)

      # Test ALL privilege hides other privileges
      self._test_show_grant_mask(admin_client, user)

      # Test ALL privilege on UDF hides other privileges
      self._test_show_grant_mask_on_udf(admin_client, data[1], data[0], unique_db, udf)

      # Test USER inherits privileges for their GROUP
      self._test_show_grant_user_group(admin_client, user, group, unique_db)
    finally:
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)

  def _test_show_grant_without_on(self, kw, ident):
    self.execute_query_expect_failure(self.client, "show grant {0} {1}".format(kw, ident))

  def _test_show_grant_user_group(self, admin_client, user, group, unique_db):
    try:
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))
      TestRanger._check_privileges(result, [])

      admin_client.execute("grant select on database {0} to group {1}"
                           .format(unique_db, group))
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))
      TestRanger._check_privileges(result, [
          ["GROUP", user, unique_db, "", "", "", "", "", "*", "select", "false"],
          ["GROUP", user, unique_db, "*", "*", "", "", "", "", "select", "false"]])
    finally:
      admin_client.execute("revoke select on database {0} from group {1}"
                           .format(unique_db, group))

  def _test_show_grant_mask(self, admin_client, user):
    privs_excl_rwstorage = ["select", "insert", "create", "alter", "drop", "refresh"]
    try:
      for privilege in privs_excl_rwstorage:
        admin_client.execute("grant {0} on server to user {1}".format(privilege, user))
      result = self.client.execute("show grant user {0} on server".format(user))
      TestRanger._check_privileges(result, [
          ["USER", user, "", "", "", "*", "", "", "", "alter", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "create", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "drop", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "insert", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "refresh", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "select", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "alter", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "create", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "drop", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "insert", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "refresh", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "select", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "alter", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "create", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "drop", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "insert", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "refresh", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "select", "false"]])

      # GRANT RWSTORAGE ON SERVER additionally grants the RWSTORAGE privilege on all
      # storage types and all storage URI's.
      admin_client.execute("grant rwstorage on server to user {0}".format(user))
      result = self.client.execute("show grant user {0} on server".format(user))
      TestRanger._check_privileges(result, [
        ["USER", user, "", "", "", "", "*", "*", "", "rwstorage", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "alter", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "create", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "drop", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "insert", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "refresh", "false"],
        ["USER", user, "", "", "", "*", "", "", "", "select", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "alter", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "create", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "drop", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "insert", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "refresh", "false"],
        ["USER", user, "*", "", "", "", "", "", "*", "select", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "alter", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "create", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "drop", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "insert", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "refresh", "false"],
        ["USER", user, "*", "*", "*", "", "", "", "", "select", "false"]])

      admin_client.execute("grant all on server to user {0}".format(user))
      result = self.client.execute("show grant user {0} on server".format(user))
      TestRanger._check_privileges(result, [
          ["USER", user, "", "", "", "", "*", "*", "", "rwstorage", "false"],
          ["USER", user, "", "", "", "*", "", "", "", "all", "false"],
          ["USER", user, "*", "", "", "", "", "", "*", "all", "false"],
          ["USER", user, "*", "*", "*", "", "", "", "", "all", "false"]])
    finally:
      admin_client.execute("revoke all on server from user {0}".format(user))
      admin_client.execute("revoke rwstorage on server from user {0}".format(user))
      for privilege in privs_excl_rwstorage:
        admin_client.execute("revoke {0} on server from user {1}".format(privilege, user))

  def _test_show_grant_mask_on_udf(self, admin_client, kw, id, unique_database, udf):
    try:
      # Grant the CREATE privilege and verify.
      self._update_privileges_and_verify(
          admin_client, "grant create on user_defined_fn {0}.{1} to {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [
              [kw, id, unique_database, "", "", "", "", "", udf, "create", "false"]])

      # Grant the DROP privilege and verify.
      self._update_privileges_and_verify(
          admin_client, "grant drop on user_defined_fn {0}.{1} to {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [
              [kw, id, unique_database, "", "", "", "", "", udf, "create", "false"],
              [kw, id, unique_database, "", "", "", "", "", udf, "drop", "false"]])

      # Grant the SELECT privilege and verify.
      self._update_privileges_and_verify(
          admin_client, "grant select on user_defined_fn {0}.{1} to {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [
              [kw, id, unique_database, "", "", "", "", "", udf, "create", "false"],
              [kw, id, unique_database, "", "", "", "", "", udf, "drop", "false"],
              [kw, id, unique_database, "", "", "", "", "", udf, "select", "false"]])

      # Grant the ALL privilege and verify other privileges are masked.
      self._update_privileges_and_verify(
          admin_client, "grant all on user_defined_fn {0}.{1} to {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [
              [kw, id, unique_database, "", "", "", "", "", udf, "all", "false"]])
    finally:
      admin_client.execute("revoke create on user_defined_fn {0}.{1} from {2} {3}"
                           .format(unique_database, udf, kw, id))
      admin_client.execute("revoke drop on user_defined_fn {0}.{1} from {2} {3}"
                           .format(unique_database, udf, kw, id))
      admin_client.execute("revoke select on user_defined_fn {0}.{1} from {2} {3}"
                           .format(unique_database, udf, kw, id))
      admin_client.execute("revoke all on user_defined_fn {0}.{1} from {2} {3}"
                           .format(unique_database, udf, kw, id))

  def _test_show_grant_basic(self, admin_client, kw, id, unique_database, unique_table,
                             udf):
    uri = WAREHOUSE_PREFIX + '/tmp'
    database = 'functional'
    table = 'alltypes'
    storagehandler_uri = 'kudu://localhost/impala::tpch_kudu.nation'
    try:
      # Grant server privileges and verify
      self._update_privileges_and_verify(
          admin_client, "grant all on server to {0} {1}".format(kw, id),
          "show grant {0} {1} on server".format(kw, id), [
              [kw, id, "", "", "", "", "*", "*", "", "rwstorage", "false"],
              [kw, id, "", "", "", "*", "", "", "", "all", "false"],
              [kw, id, "*", "", "", "", "", "", "*", "all", "false"],
              [kw, id, "*", "*", "*", "", "", "", "", "all", "false"]])

      # Revoke server privileges and verify
      self._update_privileges_and_verify(
          admin_client, "revoke all on server from {0} {1}".format(kw, id),
          "show grant {0} {1} on server".format(kw, id), [])

      # Grant uri privileges and verify
      self._update_privileges_and_verify(
          admin_client, "grant all on uri '{0}' to {1} {2}".format(uri, kw, id),
          "show grant {0} {1} on uri '{2}'".format(kw, id, uri), [
              [kw, id, "", "", "", "{0}{1}".format(NAMENODE, '/tmp'), "", "", "", "all",
               "false"]])

      # Revoke uri privileges and verify
      self._update_privileges_and_verify(
          admin_client, "revoke all on uri '{0}' from {1} {2}".format(uri, kw, id),
          "show grant {0} {1} on uri '{2}'".format(kw, id, uri), [])

      # Grant storage handler URI privilege and verify
      self._update_privileges_and_verify(
          admin_client, "grant rwstorage on storagehandler_uri '{0}' to {1} {2}"
          .format(storagehandler_uri, kw, id),
          "show grant {0} {1} on storagehandler_uri '{2}'"
          .format(kw, id, storagehandler_uri), [
              [kw, id, "", "", "", "", "kudu", "localhost/impala::tpch_kudu.nation", "",
               "rwstorage", "false"]])

      # Grant the rwstorage privilege on a database does not result in the grantee being
      # granted the privilege on the database.
      self._update_privileges_and_verify(
          admin_client,
          "grant rwstorage on database {0} to {1} {2}".format(database, kw, id),
          "show grant {0} {1} on database {2}".format(kw, id, database), [])

      # Grant the rwstorage privilege on a table does not result in the grantee being
      # granted the privilege on the table.
      self._update_privileges_and_verify(
          admin_client,
          "grant rwstorage on table {0}.{1} to {2} {3}".format(database, table, kw, id),
          "show grant {0} {1} on table {2}.{3}".format(kw, id, database, table), [])

      # Revoke storage handler URI privilege and verify
      self._update_privileges_and_verify(
          admin_client, "revoke rwstorage on storagehandler_uri '{0}' from {1} {2}"
          .format(storagehandler_uri, kw, id),
          "show grant {0} {1} on storagehandler_uri '{2}'"
          .format(kw, id, storagehandler_uri), [])

      # Grant database privileges and verify
      self._update_privileges_and_verify(
          admin_client, "grant select on database {0} to {1} {2}"
          .format(unique_database, kw, id),
          "show grant {0} {1} on database {2}".format(kw, id, unique_database), [
              [kw, id, unique_database, "", "", "", "", "", "*", "select", "false"],
              [kw, id, unique_database, "*", "*", "", "", "", "", "select", "false"]])

      # Revoke database privileges and verify
      self._update_privileges_and_verify(
          admin_client, "revoke select on database {0} from {1} {2}"
          .format(unique_database, kw, id),
          "show grant {0} {1} on database {2}".format(kw, id, unique_database), [])

      # Grant table privileges and verify
      self._update_privileges_and_verify(
          admin_client, "grant select on table {0}.{1} to {2} {3}"
          .format(unique_database, unique_table, kw, id),
          "show grant {0} {1} on table {2}.{3}"
          .format(kw, id, unique_database, unique_table), [
              [kw, id, unique_database, unique_table, "*", "", "", "", "", "select",
               "false"]])

      # Revoke table privileges and verify
      self._update_privileges_and_verify(
          admin_client, "revoke select on table {0}.{1} from {2} {3}"
          .format(unique_database, unique_table, kw, id),
          "show grant {0} {1} on table {2}.{3}"
          .format(kw, id, unique_database, unique_table), [])

      # Grant a privilege on a UDF with a wildcard for both database name and function
      # name.
      self._update_privileges_and_verify(
          admin_client, "grant select on user_defined_fn `*`.`*` to {0} {1}"
          .format(kw, id),
          "show grant {0} {1} on user_defined_fn `*`.`*`".format(kw, id), [
              [kw, id, "*", "", "", "", "", "", "*", "select", "false"]])

      # Revoke the granted privilege and verify.
      self._update_privileges_and_verify(
          admin_client,
          "revoke select on user_defined_fn `*`.`*` from {0} {1}".format(kw, id),
          "show grant {0} {1} on user_defined_fn `*`.`*`".format(kw, id), [])

      # Grant a privilege on a UDF with functional name in wildcard.
      self._update_privileges_and_verify(
          admin_client, "grant select on user_defined_fn {0}.`*` to {1} {2}"
          .format(unique_database, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.`*`"
          .format(kw, id, unique_database), [
              [kw, id, unique_database, "", "", "", "", "", "*", "select", "false"]])

      # Revoke the granted privilege and verify.
      self._update_privileges_and_verify(
          admin_client, "revoke select on user_defined_fn {0}.`*` from {1} {2}"
          .format(unique_database, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.`*`"
          .format(kw, id, unique_database), [])

      # Grant a privilege on a UDF with a wildcard for database name but a
      # non-wildcard for functional name.
      self._update_privileges_and_verify(
          admin_client,
          "grant select on user_defined_fn `*`.{0} to {1} {2}".format(udf, kw, id),
          "show grant {0} {1} on user_defined_fn `*`.{2}".format(kw, id, udf), [
              [kw, id, "*", "", "", "", "", "", udf, "select", "false"]])

      # Revoke the granted privilege and verify.
      self._update_privileges_and_verify(
          admin_client,
          "revoke select on user_defined_fn `*`.{0} from {1} {2}".format(udf, kw, id),
          "show grant {0} {1} on user_defined_fn `*`.{2}".format(kw, id, udf), [])

      # Grant a privilege on a UDF and verify.
      self._update_privileges_and_verify(
          admin_client, "grant select on user_defined_fn {0}.{1} to {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [
              [kw, id, unique_database, "", "", "", "", "", udf, "select", "false"]])

      # Revoke the granted privilege and verify.
      self._update_privileges_and_verify(
          admin_client, "revoke select on user_defined_fn {0}.{1} from {2} {3}"
          .format(unique_database, udf, kw, id),
          "show grant {0} {1} on user_defined_fn {2}.{3}"
          .format(kw, id, unique_database, udf), [])

      # Grant column privileges and verify
      self._update_privileges_and_verify(
          admin_client, "grant select(x) on table {0}.{1} to {2} {3}"
          .format(unique_database, unique_table, kw, id),
          "show grant {0} {1} on column {2}.{3}.x"
          .format(kw, id, unique_database, unique_table), [
              [kw, id, unique_database, unique_table, "x", "", "", "", "", "select",
               "false"]])

      # Revoke column privileges and verify
      self._update_privileges_and_verify(
          admin_client, "revoke select(x) on table {0}.{1} from {2} {3}"
          .format(unique_database, unique_table, kw, id),
          "show grant {0} {1} on column {2}.{3}.x"
          .format(kw, id, unique_database, unique_table), [])
    finally:
      admin_client.execute("revoke all on server from {0} {1}".format(kw, id))
      admin_client.execute("revoke all on uri '{0}' from {1} {2}"
                           .format(uri, kw, id))
      admin_client.execute("revoke rwstorage on storagehandler_uri '{0}' from {1} {2}"
                           .format(storagehandler_uri, kw, id))
      admin_client.execute("revoke select on database {0} from {1} {2}"
                           .format(unique_database, kw, id))
      admin_client.execute("revoke select on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      admin_client.execute("revoke select on user_defined_fn `*`.`*` from {0} {1}"
                           .format(kw, id))
      admin_client.execute("revoke select on user_defined_fn {0}.`*` from {1} {2}"
                           .format(unique_database, kw, id))
      admin_client.execute("revoke select on user_defined_fn `*`.{0} from {1} {2}"
                           .format(udf, kw, id))
      admin_client.execute("revoke select on user_defined_fn {0}.{1} from {2} {3}"
                           .format(unique_database, udf, kw, id))
      admin_client.execute("revoke select(x) on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))

  def _test_show_grant_inherited(self, admin_client, kw, id, unique_database,
                                 unique_table):
    try:
      # Grant the select privilege on server
      admin_client.execute("grant select on server to {0} {1}".format(kw, id), user=ADMIN)

      # Verify the privileges are correctly added
      result = self.client.execute("show grant {0} {1} on server".format(kw, id))
      TestRanger._check_privileges(result, [
          [kw, id, "", "", "", "*", "", "", "", "select", "false"],
          [kw, id, "*", "", "", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a database
      result = self.client.execute("show grant {0} {1} on database {2}"
          .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "", "", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a table
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a column
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"]])

      # Grant the create privilege on database and verify
      admin_client.execute("grant create on database {0} to {1} {2}"
                           .format(unique_database, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on database {2}"
                                   .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "", "", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"],
          [kw, id, unique_database, "", "", "", "", "", "*", "create", "false"],
          [kw, id, unique_database, "*", "*", "", "", "", "", "create", "false"]
      ])

      # Grant the insert privilege on table and verify
      admin_client.execute("grant insert on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "", "", "select", "false"],
          [kw, id, unique_database, "*", "*", "", "", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "", "", "insert", "false"]
      ])

      # Grant the select privilege on column and verify
      admin_client.execute("grant select(x) on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, "*", "*", "", "", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "", "", "insert", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "", "", "select", "false"]
      ])

      # The insert privilege on table masks the select privilege just added
      admin_client.execute("grant select on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, "*", "*", "", "", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "", "", "insert", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "", "", "select", "false"]
      ])

      # The all privilege on table masks the privileges of insert and select, but not the
      # select privilege on column.
      admin_client.execute("grant all on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, unique_table, "*", "", "", "", "", "all", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "", "", "select", "false"]
      ])

    finally:
      admin_client.execute("revoke select on server from {0} {1}".format(kw, id))
      admin_client.execute("revoke create on database {0} from {1} {2}"
                           .format(unique_database, kw, id))
      admin_client.execute("revoke insert on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      admin_client.execute("revoke select(x) on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      admin_client.execute("revoke select on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      admin_client.execute("revoke all on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_revoke_ranger_api(self, unique_name):
    user = getuser()
    admin_client = self.create_impala_client()
    unique_db = unique_name + "_db"
    resource = {
      "database": unique_db,
      "column": "*",
      "table": "*"
    }
    access = ["select", "create"]

    try:
      # Create the test database
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_db), user=ADMIN)

      # Grant privileges via Ranger REST API
      TestRanger._grant_ranger_privilege(user, resource, access)

      # Privileges should be stale before a refresh
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))
      TestRanger._check_privileges(result, [])

      # Refresh and check updated privileges
      admin_client.execute("refresh authorization")
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))

      TestRanger._check_privileges(result, [
          ["USER", user, unique_db, "*", "*", "", "", "", "", "create", "false"],
          ["USER", user, unique_db, "*", "*", "", "", "", "", "select", "false"]
      ])

      # Revoke privileges via Ranger REST API
      TestRanger._revoke_ranger_privilege(user, resource, access)

      # Privileges should be stale before a refresh
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))
      TestRanger._check_privileges(result, [
          ["USER", user, unique_db, "*", "*", "", "", "", "", "create", "false"],
          ["USER", user, unique_db, "*", "*", "", "", "", "", "select", "false"]
      ])

      # Refresh and check updated privileges
      admin_client.execute("refresh authorization")
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))

      TestRanger._check_privileges(result, [])
    finally:
      admin_client.execute("revoke all on database {0} from user {1}"
                           .format(unique_db, user))
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)

  @pytest.mark.execute_serially
  @SkipIf.is_test_jdk
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS, reset_ranger=True)
  def test_show_grant_hive_privilege(self, unique_name):
    user = getuser()
    admin_client = self.create_impala_client()
    unique_db = unique_name + "_db"
    resource = {
      "database": unique_db,
      "column": "*",
      "table": "*"
    }
    access = ["lock", "select"]

    try:
      TestRanger._grant_ranger_privilege(user, resource, access)
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_db), user=ADMIN)

      admin_client.execute("refresh authorization")
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))

      TestRanger._check_privileges(result, [
          ["USER", user, unique_db, "*", "*", "", "", "", "", "select", "false"]
      ])

      # Assert that lock, select privilege exists in Ranger server
      assert "lock" in TestRanger._get_ranger_privileges_db(user, unique_db)
      assert "select" in TestRanger._get_ranger_privileges_db(user, unique_db)

      admin_client.execute("revoke select on database {0} from user {1}"
                           .format(unique_db, user))

      # Assert that lock is still present and select is revoked in Ranger server
      assert "lock" in TestRanger._get_ranger_privileges_db(user, unique_db)
      assert "select" not in TestRanger._get_ranger_privileges_db(user, unique_db)

      admin_client.execute("refresh authorization")
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))

      TestRanger._check_privileges(result, [])
    finally:
      admin_client.execute("drop database if exists {0} cascade".format(unique_db),
                           user=ADMIN)
      TestRanger._revoke_ranger_privilege(user, resource, access)

  @staticmethod
  def _grant_ranger_privilege(user, resource, access):
    data = {
      "grantor": ADMIN,
      "grantorGroups": [],
      "resource": resource,
      "users": [user],
      "groups": [],
      "accessTypes": access,
      "delegateAdmin": "false",
      "enableAudit": "true",
      "replaceExistingPermissions": "false",
      "isRecursive": "false",
      "clusterName": "server1"
    }
    r = requests.post("{0}/service/plugins/services/grant/test_impala?pluginId=impala"
                      .format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=REST_HEADERS)
    assert 200 <= r.status_code < 300

  @staticmethod
  def _revoke_ranger_privilege(user, resource, access):
    data = {
      "grantor": ADMIN,
      "grantorGroups": [],
      "resource": resource,
      "users": [user],
      "groups": [],
      "accessTypes": access,
      "delegateAdmin": "false",
      "enableAudit": "true",
      "replaceExistingPermissions": "false",
      "isRecursive": "false",
      "clusterName": "server1"
    }
    r = requests.post("{0}/service/plugins/services/revoke/test_impala?pluginId=impala"
                      .format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=REST_HEADERS)
    assert 200 <= r.status_code < 300

  @staticmethod
  def _get_ranger_privileges_db(user, db):
    policies = TestRanger._get_ranger_privileges(user)
    result = []

    for policy in policies:
      resources = policy["resources"]
      if "database" in resources and db in resources["database"]["values"]:
        for policy_items in policy["policyItems"]:
          if user in policy_items["users"]:
            for access in policy_items["accesses"]:
              result.append(access["type"])

    return result

  @staticmethod
  def _get_ranger_privileges(user):
    r = requests.get("{0}/service/plugins/policies"
                     .format(RANGER_HOST),
                     auth=RANGER_AUTH, headers=REST_HEADERS)
    return json.loads(r.content)["policies"]

  def _add_ranger_user(self, user):
    data = {"name": user, "password": "Password123", "userRoleList": ["ROLE_USER"]}
    r = requests.post("{0}/service/xusers/secure/users".format(RANGER_HOST),
                      auth=RANGER_AUTH,
                      json=data, headers=REST_HEADERS)
    return json.loads(r.content)["id"]

  def _remove_ranger_user(self, id):
    r = requests.delete("{0}/service/xusers/users/{1}?forceDelete=true"
                        .format(RANGER_HOST, id), auth=RANGER_AUTH)
    assert 300 > r.status_code >= 200

  @staticmethod
  def _add_column_masking_policy(
      policy_name, user, db, table, column, mask_type, value_expr=None):
    """ Adds a column masking policy and returns the policy id"""
    data = {
      "name": policy_name,
      "policyType": 1,
      "serviceType": "hive",
      "service": "test_impala",
      "resources": {
        "database": {
          "values": [db],
          "isExcludes": False,
          "isRecursive": False
        },
        "table": {
          "values": [table],
          "isExcludes": False,
          "isRecursive": False
        },
        "column": {
          "values": [column],
          "isExcludes": False,
          "isRecursive": False
        }
      },
      "dataMaskPolicyItems": [
        {
          "accesses": [
            {
              "type": "select",
              "isAllowed": True
            }
          ],
          "users": [user],
          "dataMaskInfo": {
            "dataMaskType": mask_type,
            "valueExpr": value_expr
          }
        }
      ]
    }
    r = requests.post("{0}/service/public/v2/api/policy".format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=REST_HEADERS)
    assert 300 > r.status_code >= 200, r.content
    policy_id = json.loads(r.content)["id"]
    LOG.info("Added column masking policy " + str(policy_id))
    return policy_id

  @staticmethod
  def _add_row_filtering_policy(policy_name, user, db, table, filter_expr):
    """Adds a row filtering policy and returns the policy id"""
    TestRanger._add_multiuser_row_filtering_policy(policy_name, db, table, [user],
                                                   [filter_expr])

  @staticmethod
  def _add_multiuser_row_filtering_policy(policy_name, db, table, users, filters):
    """Adds a row filtering policy on 'db'.'table' and returns the policy id.
    If len(filters) == 1, all users use the same filter. Otherwise,
    users[0] has filters[0], users[1] has filters[1] and so on."""
    assert len(users) > 0
    assert len(filters) == 1 or len(users) == len(filters)
    items = []
    if len(filters) == 1:
      items.append({
        "accesses": [
          {
            "type": "select",
            "isAllowed": True
          }
        ],
        "users": users,
        "rowFilterInfo": {"filterExpr": filters[0]}
      })
    else:
      for i in range(len(users)):
        items.append({
            "accesses": [
              {
                "type": "select",
                "isAllowed": True
              }
            ],
            "users": [users[i]],
            "rowFilterInfo": {"filterExpr": filters[i]}
          })
    TestRanger._add_row_filtering_policy_with_items(policy_name, db, table, items)

  @staticmethod
  def _add_row_filtering_policy_with_items(policy_name, db, table, items):
    """ Adds a row filtering policy and returns the policy id"""
    policy_data = {
      "name": policy_name,
      "policyType": 2,
      "serviceType": "hive",
      "service": "test_impala",
      "resources": {
        "database": {
          "values": [db],
          "isExcludes": False,
          "isRecursive": False
        },
        "table": {
          "values": [table],
          "isExcludes": False,
          "isRecursive": False
        }
      },
      "rowFilterPolicyItems": items
    }
    r = requests.post("{0}/service/public/v2/api/policy".format(RANGER_HOST),
                      auth=RANGER_AUTH, json=policy_data, headers=REST_HEADERS)
    assert 300 > r.status_code >= 200, r.content
    LOG.info("Added row filtering policy on table {0}.{1} for using items {2}"
             .format(db, table, items))
    return json.loads(r.content)["id"]

  @staticmethod
  def _add_deny_policy(policy_name, user, db, table, column):
    """ Adds a deny policy and returns the policy id"""
    data = {
      "name": policy_name,
      "policyType": 0,
      "serviceType": "hive",
      "service": "test_impala",
      "resources": {
        "database": {
          "values": [db],
          "isExcludes": False,
          "isRecursive": False
        },
        "table": {
          "values": [table],
          "isExcludes": False,
          "isRecursive": False
        },
        "column": {
          "values": [column],
          "isExcludes": False,
          "isRecursive": False
        }
      },
      "policyItems": [],
      "denyPolicyItems": [
        {
          "accesses": [
            {
              "type": "select",
              "isAllowed": True
            }
          ],
          "users": [user],
        }
      ]
    }
    r = requests.post("{0}/service/public/v2/api/policy".format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=REST_HEADERS)
    assert 300 > r.status_code >= 200, r.content
    return json.loads(r.content)["id"]

  @staticmethod
  def _remove_policy(policy_name):
    r = requests.delete(
        "{0}/service/public/v2/api/policy?servicename=test_impala&policyname={1}".format(
            RANGER_HOST, policy_name),
        auth=RANGER_AUTH, headers=REST_HEADERS)
    assert 300 > r.status_code >= 200, r.content

  @staticmethod
  def _check_privileges(result, expected):
    def columns(row):
      cols = row.split("\t")
      return cols[0:len(cols) - 1]

    assert list(map(columns, result.data)) == expected

  def _refresh_authorization(self, client, statement):
    if statement is not None:
      self.execute_query_expect_success(client, statement)

  def _run_query_as_user(self, query, username, expect_success):
    """Helper to run an input query as a given user."""
    impala_client = self.create_impala_client()
    if expect_success:
      return self.execute_query_expect_success(
          impala_client, query, user=username, query_options={'sync_ddl': 1})
    return self.execute_query_expect_failure(impala_client, query, user=username)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_unsupported_sql(self):
    """Tests unsupported SQL statements when running with Ranger."""
    user = "admin"
    impala_client = self.create_impala_client()
    error_msg = "UnsupportedOperationException: SHOW GRANT is not supported without a " \
        "defined resource in Ranger."
    statement = "show grant role foo"
    result = self.execute_query_expect_failure(impala_client, statement, user=user)
    assert error_msg in str(result)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_revoke_invalid_principal(self):
    """Tests grant/revoke to/from invalid principal should return more readable
       error messages."""
    valid_user = "admin"
    invalid_user = "invalid_user"
    invalid_group = "invalid_group"
    # TODO(IMPALA-8640): Create two different Impala clients because the users to
    # workaround the bug.
    invalid_impala_client = self.create_impala_client()
    valid_impala_client = self.create_impala_client()
    for statement in ["grant select on table functional.alltypes to user {0}"
                      .format(getuser()),
                      "revoke select on table functional.alltypes from user {0}"
                      .format(getuser())]:
      result = self.execute_query_expect_failure(invalid_impala_client,
                                                 statement,
                                                 user=invalid_user)
      if "grant" in statement:
        assert "Error granting a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantor user invalid_user doesn't exist" in str(result)
      else:
        assert "Error revoking a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantor user invalid_user doesn't exist" in str(result)

    for statement in ["grant select on table functional.alltypes to user {0}"
                      .format(invalid_user),
                      "revoke select on table functional.alltypes from user {0}"
                      .format(invalid_user)]:
      result = self.execute_query_expect_failure(valid_impala_client,
                                                 statement,
                                                 user=valid_user)
      if "grant" in statement:
        assert "Error granting a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantee user invalid_user doesn't exist" in str(result)
      else:
        assert "Error revoking a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantee user invalid_user doesn't exist" in str(result)

    for statement in ["grant select on table functional.alltypes to group {0}"
                      .format(invalid_group),
                      "revoke select on table functional.alltypes from group {0}"
                      .format(invalid_group)]:
      result = self.execute_query_expect_failure(valid_impala_client,
                                                 statement,
                                                 user=valid_user)
      if "grant" in statement:
        assert "Error granting a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantee group invalid_group doesn't exist" in str(result)
      else:
        assert "Error revoking a privilege in Ranger. Ranger error message: " \
               "HTTP 403 Error: Grantee group invalid_group doesn't exist" in str(result)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_legacy_catalog_ownership(self):
      self._test_ownership()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS)
  def test_local_catalog_ownership(self):
      # getTableIfCached() in LocalCatalog loads a minimal incomplete table
      # that does not include the ownership information. Hence show tables
      # *never* show owned tables. TODO(bharathv): Fix in a follow up commit
      pytest.xfail("getTableIfCached() faulty behavior, known issue")
      self._test_ownership()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_revoke_by_owner_legacy_catalog(self, unique_name):
    self._test_grant_revoke_by_owner(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS)
  def test_grant_revoke_by_owner_local_catalog(self, unique_name):
    self._test_grant_revoke_by_owner(unique_name)

  def _test_grant_revoke_by_owner(self, unique_name):
    non_owner_user = NON_OWNER
    non_owner_group = NON_OWNER
    grantee_role = "grantee_role"
    resource_owner_role = OWNER_USER
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    table_name = "tbl"
    column_names = ["a", "b"]
    udf_uri = "/test-warehouse/impala-hive-udfs.jar"
    udf_name = "identity"
    test_data = [("USER", non_owner_user), ("GROUP", non_owner_group),
        ("ROLE", grantee_role)]
    privileges = ["alter", "drop", "create", "insert", "select", "refresh"]

    try:
      admin_client.execute("create role {}".format(grantee_role), user=ADMIN)
      admin_client.execute("create role {}".format(resource_owner_role), user=ADMIN)
      admin_client.execute("grant create on server to user {0}".format(OWNER_USER),
          user=ADMIN)
      # A user has to be granted the ALL privilege on the URI of the UDF as well to be
      # able to create a UDF.
      admin_client.execute("grant all on uri '{0}{1}' to user {2}"
          .format(os.getenv("FILESYSTEM_PREFIX"), udf_uri, OWNER_USER), user=ADMIN)
      self._run_query_as_user("create database {0}".format(unique_database), OWNER_USER,
          True)
      self._run_query_as_user("create table {0}.{1} ({2} int, {3} string)"
          .format(unique_database, table_name, column_names[0], column_names[1]),
          OWNER_USER, True)
      self._run_query_as_user("create function {0}.{1} "
          "location '{2}{3}' symbol='org.apache.impala.TestUdf'"
          .format(unique_database, udf_name, os.getenv("FILESYSTEM_PREFIX"), udf_uri),
          OWNER_USER, True)

      for data in test_data:
        grantee_type = data[0]
        grantee = data[1]
        for privilege in privileges:
          # Case 1: when the resource is a database.
          self._test_grant_revoke_by_owner_on_database(privilege, unique_database,
              grantee_type, grantee, resource_owner_role)

          # Case 2: when the resource is a table.
          self._test_grant_revoke_by_owner_on_table(privilege, unique_database,
              table_name, grantee_type, grantee, resource_owner_role)

          # Case 3: when the resource is a column.
          self._test_grant_revoke_by_owner_on_column(privilege, column_names,
              unique_database, table_name, grantee_type, grantee, resource_owner_role)

          # Case 4: when the resource is a UDF.
          self._test_grant_revoke_by_owner_on_udf(privilege, unique_database, udf_name,
              grantee_type, grantee)
    finally:
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
          user=ADMIN)
      admin_client.execute("drop role {}".format(grantee_role), user=ADMIN)
      admin_client.execute("drop role {}".format(resource_owner_role), user=ADMIN)
      admin_client.execute("revoke create on server from user {0}".format(OWNER_USER),
          user=ADMIN)
      admin_client.execute("revoke all on uri '{0}{1}' from user {2}"
          .format(os.getenv("FILESYSTEM_PREFIX"), udf_uri, OWNER_USER), user=ADMIN)

  def _test_grant_revoke_by_owner_on_database(self, privilege, unique_database,
      grantee_type, grantee, resource_owner_role):
    grant_database_stmt = "grant {0} on database {1} to {2} {3}"
    revoke_database_stmt = "revoke {0} on database {1} from {2} {3}"
    show_grant_database_stmt = "show grant {0} {1} on database {2}"
    set_database_owner_user_stmt = "alter database {0} set owner user {1}"
    set_database_owner_group_stmt = "alter database {0} set owner group {1}"
    set_database_owner_role_stmt = "alter database {0} set owner role {1}"
    resource_owner_group = OWNER_USER
    admin_client = self.create_impala_client()

    try:
      self._run_query_as_user(grant_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          True)
      result = admin_client.execute(show_grant_database_stmt
          .format(grantee_type, grantee, unique_database), user=ADMIN)
      TestRanger._check_privileges(result, [
          [grantee_type, grantee, unique_database, "", "", "", "", "", "*",
           privilege, "false"],
          [grantee_type, grantee, unique_database, "*", "*", "", "", "", "",
           privilege, "false"]])

      self._run_query_as_user(revoke_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          True)
      result = admin_client.execute(show_grant_database_stmt
          .format(grantee_type, grantee, unique_database), user=ADMIN)
      TestRanger._check_privileges(result, [])

      # Set the owner of the database to a group that has the same name as 'OWNER_USER'
      # and verify that the user 'OWNER_USER' is not able to grant or revoke a privilege
      # on the database.
      # We run the statement in Hive because currently
      # ALTER DATABASE SET OWNER GROUP is not supported in Impala. Note that we
      # need to force Impala to reload the metadata of database since the change
      # is made through Hive.
      self.run_stmt_in_hive(set_database_owner_group_stmt
          .format(unique_database, resource_owner_group))
      admin_client.execute("invalidate metadata", user=ADMIN)

      result = self._run_query_as_user(grant_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          False)
      assert ERROR_REVOKE in str(result)

      # Set the owner of the database to a role that has the same name as 'OWNER_USER'
      # and verify that the user 'OWNER_USER' is not able to grant or revoke a
      # privilege on the database.
      admin_client.execute(set_database_owner_role_stmt
          .format(unique_database, resource_owner_role), user=ADMIN)

      result = self._run_query_as_user(grant_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), OWNER_USER,
          False)
      assert ERROR_REVOKE in str(result)
      # Change the database owner back to the user 'OWNER_USER'.
      admin_client.execute(set_database_owner_user_stmt
          .format(unique_database, OWNER_USER), user=ADMIN)
    finally:
      # Revoke the privilege that was granted by 'OWNER_USER' in case any of the
      # REVOKE statements submitted by 'owner_user' failed to prevent this test
      # from interfering with other tests.
      admin_client.execute(revoke_database_stmt
          .format(privilege, unique_database, grantee_type, grantee), user=ADMIN)

  def _test_grant_revoke_by_owner_on_table(self, privilege, unique_database, table_name,
      grantee_type, grantee, resource_owner_role):
    # The CREATE privilege on a table is not supported.
    if privilege == "create":
      return
    grant_table_stmt = "grant {0} on table {1}.{2} to {3} {4}"
    revoke_table_stmt = "revoke {0} on table {1}.{2} from {3} {4}"
    show_grant_table_stmt = "show grant {0} {1} on table {2}.{3}"
    resource_owner_group = OWNER_USER
    admin_client = self.create_impala_client()
    set_table_owner_user_stmt = "alter table {0}.{1} set owner user {2}"
    set_table_owner_group_stmt = "alter table {0}.{1} set owner group {2}"
    set_table_owner_role_stmt = "alter table {0}.{1} set owner role {2}"

    try:
      self._run_query_as_user(grant_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, True)
      result = admin_client.execute(show_grant_table_stmt
          .format(grantee_type, grantee, unique_database, table_name),
          user=ADMIN)
      TestRanger._check_privileges(result, [
          [grantee_type, grantee, unique_database, table_name, "*", "", "", "",
          "", privilege, "false"]])

      self._run_query_as_user(revoke_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, True)
      result = admin_client.execute(show_grant_table_stmt
          .format(grantee_type, grantee, unique_database, table_name),
          user=ADMIN)
      TestRanger._check_privileges(result, [])

      # Set the owner of the table to a group that has the same name as
      # 'OWNER_USER' and verify that the user 'OWNER_USER' is not able to grant or
      # revoke a privilege on the table.
      # We run the statement in Hive because currently
      # ALTER TABLE SET OWNER GROUP is not supported in Impala. Note that we
      # need to force Impala to reload the metadata of table since the change
      # is made through Hive.
      self.run_stmt_in_hive(set_table_owner_group_stmt
          .format(unique_database, table_name, resource_owner_group))
      admin_client.execute("refresh {0}.{1}".format(unique_database, table_name),
          user=ADMIN)

      result = self._run_query_as_user(grant_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, False)
      assert ERROR_REVOKE in str(result)

      # Set the owner of the table to a role that has the same name as
      # 'OWNER_USER' and verify that the user 'OWNER_USER' is not able to grant or
      # revoke a privilege on the table.
      admin_client.execute(set_table_owner_role_stmt
          .format(unique_database, table_name, resource_owner_role), user=ADMIN)

      result = self._run_query_as_user(grant_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          OWNER_USER, False)
      assert ERROR_REVOKE in str(result)
      # Change the table owner back to the user 'OWNER_USER'.
      admin_client.execute(set_table_owner_user_stmt
          .format(unique_database, table_name, OWNER_USER), user=ADMIN)
    finally:
      # Revoke the privilege that was granted by 'OWNER_USER' in case any of the
      # REVOKE statements submitted by 'OWNER_USER' failed to prevent this test
      # from interfering with other tests.
      admin_client.execute(revoke_table_stmt
          .format(privilege, unique_database, table_name, grantee_type, grantee),
          user=ADMIN)

  def _test_grant_revoke_by_owner_on_column(self, privilege, column_names,
      unique_database, table_name, grantee_type, grantee, resource_owner_role):
    # For a column, only the SELECT privilege is allowed.
    if privilege != "select":
      return
    grant_column_stmt = "grant {0} ({1}) on table {2}.{3} to {4} {5}"
    revoke_column_stmt = "revoke {0} ({1}) on table {2}.{3} from {4} {5}"
    show_grant_column_stmt = "show grant {0} {1} on column {2}.{3}.{4}"
    resource_owner_group = OWNER_USER
    admin_client = self.create_impala_client()
    set_table_owner_user_stmt = "alter table {0}.{1} set owner user {2}"
    set_table_owner_group_stmt = "alter table {0}.{1} set owner group {2}"
    set_table_owner_role_stmt = "alter table {0}.{1} set owner role {2}"

    try:
      self._run_query_as_user(grant_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, True)
      result = admin_client.execute(show_grant_column_stmt
          .format(grantee_type, grantee, unique_database, table_name,
          column_names[0]), user=ADMIN)
      TestRanger._check_privileges(result, [
          [grantee_type, grantee, unique_database, table_name, column_names[0],
          "", "", "", "", privilege, "false"]])

      self._run_query_as_user(revoke_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, True)
      result = admin_client.execute(show_grant_column_stmt
          .format(grantee_type, grantee, unique_database, table_name,
          column_names[0]), user=ADMIN)
      TestRanger._check_privileges(result, [])

      # Set the owner of the table to a group that has the same name as 'OWNER_USER'
      # and verify that the user 'OWNER_USER' is not able to grant or revoke a
      # privilege on a column in the table.
      self.run_stmt_in_hive(set_table_owner_group_stmt
          .format(unique_database, table_name, resource_owner_group))
      admin_client.execute("refresh {0}.{1}".format(unique_database, table_name),
          user=ADMIN)

      result = self._run_query_as_user(grant_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, False)
      assert ERROR_REVOKE in str(result)

      # Set the owner of the table to a role that has the same name as 'OWNER_USER' and
      # verify that the user 'OWNER_USER' is not able to grant or revoke a privilege on
      # a column in the table.
      admin_client.execute(set_table_owner_role_stmt
          .format(unique_database, table_name, resource_owner_role), user=ADMIN)

      result = self._run_query_as_user(grant_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, False)
      assert ERROR_GRANT in str(result)
      result = self._run_query_as_user(revoke_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), OWNER_USER, False)
      assert ERROR_REVOKE in str(result)
      # Change the table owner back to the user 'owner_user'.
      admin_client.execute(set_table_owner_user_stmt
          .format(unique_database, table_name, OWNER_USER), user=ADMIN)
    finally:
      # Revoke the privilege that was granted by 'OWNER_USER' in case any of the
      # REVOKE statements submitted by 'OWNER_USER' failed to prevent this test
      # from interfering with other tests.
      admin_client.execute(revoke_column_stmt
          .format(privilege, column_names[0], unique_database, table_name,
          grantee_type, grantee), user=ADMIN)

  def _test_grant_revoke_by_owner_on_udf(self, privilege, unique_database, udf_name,
      grantee_type, grantee):
    # Due to IMPALA-11743 and IMPALA-12685, the owner of a UDF could not grant
    # or revoke the SELECT privilege.
    result = self._run_query_as_user("grant {0} on user_defined_fn "
        "{1}.{2} to {3} {4}".format(privilege, unique_database, udf_name,
        grantee_type, grantee), OWNER_USER, False)
    assert ERROR_GRANT in str(result)
    result = self._run_query_as_user("revoke {0} on user_defined_fn "
        "{1}.{2} from {3} {4}".format(privilege, unique_database, udf_name,
        grantee_type, grantee), OWNER_USER, False)
    assert ERROR_REVOKE in str(result)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_show_functions(self, unique_name):
    user1 = getuser()
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    privileges = ["ALTER", "DROP", "CREATE", "INSERT", "SELECT", "REFRESH"]
    fs_prefix = os.getenv("FILESYSTEM_PREFIX") or str()
    try:
      # Set-up temp database + function
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      self.execute_query_expect_success(admin_client, "create function {0}.foo() RETURNS"
                                      " int LOCATION '{1}/test-warehouse/libTestUdfs.so'"
                                      "SYMBOL='Fn'".format(unique_database, fs_prefix),
                                      user=ADMIN)
      # Check "show functions" with no privilege granted.
      result = self._run_query_as_user("show functions in {0}".format(unique_database),
          user1, False)
      err = "User '{0}' does not have privileges to access: {1}.*.*". \
          format(user1, unique_database)
      assert err in str(result)
      for privilege in privileges:
        try:
          # Grant privilege
          self.execute_query_expect_success(admin_client,
                                            "grant {0} on database {1} to user {2}"
                                            .format(privilege, unique_database, user1),
                                            user=ADMIN)
          # Check with current privilege
          result = self._run_query_as_user("show functions in {0}"
                                          .format(unique_database), user1, True)
          assert "foo()", str(result)
        finally:
          # Revoke privilege
          admin_client.execute("revoke {0} on database {1} from user {2}"
                              .format(privilege, unique_database, user1), user=ADMIN)
    finally:
      # Drop database
      self._run_query_as_user("drop database {0} cascade".format(unique_database),
                              ADMIN, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_select_function(self, unique_name):
    """Verifies that to execute a UDF in a database, a user has to be granted a) the
    SELECT privilege on the UDF, and b) any of the SELECT, INSERT, REFRESH privileges on
    all the tables, columns in the database."""
    test_user = "non_owner"
    admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    fs_prefix = os.getenv("FILESYSTEM_PREFIX") or str()
    try:
      # Create a temporary database and a user-defined function.
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create function {0}.identity(bigint) "
                           "RETURNS bigint "
                           "LOCATION "
                           "'{1}/test-warehouse/impala-hive-udfs.jar' "
                           "SYMBOL='org.apache.impala.TestUdf'"
                           .format(unique_database, fs_prefix), user=ADMIN)
      # Create a temporary table and grant 'test_user' the INSERT privilege on the table,
      # which is necessary for 'test_user' to insert values into a table.
      admin_client.execute("create table {0}.tbl (id bigint)".format(unique_database))
      admin_client.execute("grant insert on table {0}.tbl to user {1}"
                           .format(unique_database, test_user))

      stmts = ["select {0}.identity(1)".format(unique_database),
               "insert into {0}.tbl values ({0}.identity(1))".format(unique_database)]
      for stmt in stmts:
        # A user not granted any privilege is not allowed to execute the UDF.
        result = self._run_query_as_user(stmt, test_user, False)
        err = "User '{0}' does not have privileges to SELECT functions in: " \
              "{1}.identity".format(test_user, unique_database)
        assert err in str(result)

        view_metadata_privileges = ["select", "insert", "refresh"]
        for privilege_on_database in view_metadata_privileges:
          try:
            # A user is allowed to execute a UDF in a database if the user has been
            # granted the SELECT privilege on the database. Such a privilege covers all
            # the tables, columns, as well as UDFs in the database.
            self._update_privileges_and_verify(
                admin_client, "grant {0} on database {1} to user {2}"
                .format(privilege_on_database, unique_database, test_user),
                "show grant user {0} on database {1}"
                .format(test_user, unique_database), [
                    ["USER", test_user, unique_database, "", "", "", "", "", "*",
                     privilege_on_database, "false"],
                    ["USER", test_user, unique_database, "*", "*", "", "", "", "",
                     privilege_on_database, "false"]])
            # Query succeeds only if 'privilege_on_database' is "select".
            if privilege_on_database != "select":
              result = self._run_query_as_user(stmt, test_user, False)
              err = "User '{0}' does not have privileges to SELECT functions in: " \
                    "{1}.identity".format(test_user, unique_database)
              assert err in str(result)
            else:
              self._run_query_as_user(stmt, test_user, True)

            # A user not being granted the SELECT privilege on any UDF in the database is
            # not allowed to execute the UDF even though the user has
            # the 'privilege_on_database' privilege on all the tables, columns in the
            # database.
            self._update_privileges_and_verify(
                admin_client, "revoke {0} on user_defined_fn {1}.`*` from user {2}"
                .format(privilege_on_database, unique_database, test_user),
                "show grant user {0} on database {1}"
                .format(test_user, unique_database), [
                    ["USER", test_user, unique_database, "*", "*", "", "", "", "",
                     privilege_on_database, "false"]])
            result = self._run_query_as_user(stmt, test_user, False)
            err = "User '{0}' does not have privileges to SELECT functions in: " \
                  "{1}.identity".format(test_user, unique_database)
            assert err in str(result)

            # A user is allowed to execute the UDF if the user is explicitly granted the
            # SELECT privilege on the UDF.
            self._update_privileges_and_verify(
                admin_client, "grant select on user_defined_fn {0}.identity to user {1}"
                .format(unique_database, test_user),
                "show grant user {0} on user_defined_fn {1}.identity"
                .format(test_user, unique_database), [
                    ["USER", test_user, unique_database, "", "", "", "", "", "identity",
                     "select", "false"]])
            self._run_query_as_user(stmt, test_user, True)

            # Even though a user is explicitly granted the SELECT privilege on the UDF,
            # the user is not allowed to execute the UDF if the user is not granted any
            # of the SELECT, INSERT, or REFRESH privileges on all the tables and columns
            # in the database.
            self._update_privileges_and_verify(
                admin_client, "revoke {0} on database {1} from user {2}"
                .format(privilege_on_database, unique_database, test_user),
                "show grant user {0} on database {1}".format(test_user, unique_database),
                [])
            result = admin_client.execute("show grant user {0} "
                                          "on user_defined_fn {1}.identity"
                                          .format(test_user, unique_database),
                                          user=ADMIN)
            TestRanger._check_privileges(result, [
                ["USER", test_user, unique_database, "", "", "", "", "", "identity",
                 "select", "false"]])
            result = self._run_query_as_user(stmt, test_user, False)
            err = "User '{0}' does not have privileges to access: {1}"\
                  .format(test_user, unique_database)
            assert err in str(result)
          finally:
            # Revoke the granted privileges.
            admin_client.execute("revoke {0} on database {1} from user {2}"
                                 .format(privilege_on_database, unique_database,
                                         test_user), user=ADMIN)
            admin_client.execute("revoke select on user_defined_fn {0}.identity "
                                 "from user {1}"
                                 .format(unique_database, test_user), user=ADMIN)
    finally:
      # Revoke the granted privilege on the temporary table.
      self._run_query_as_user("revoke insert on table {0}.tbl from user {1}"
                              .format(unique_database, test_user),
                              ADMIN, True)
      # Drop the database.
      self._run_query_as_user("drop database {0} cascade".format(unique_database),
                              ADMIN, True)

  def _test_ownership(self):
    """Tests ownership privileges for databases and tables with ranger along with
    some known quirks in the implementation."""
    test_user = getuser()
    test_role = 'test_role'
    test_db = "test_ranger_ownership_" + get_random_id(5).lower()
    # Create a test database as "admin" user. Owner is set accordingly.
    self._run_query_as_user("create database {0}".format(test_db), ADMIN, True)
    try:
      # Try to create a table under test_db as current user. It should fail.
      self._run_query_as_user(
          "create table {0}.foo(a int)".format(test_db), test_user, False)

      # Change the owner of the database to the current user.
      self._run_query_as_user(
          "alter database {0} set owner user {1}".format(test_db, test_user), ADMIN, True)

      self._run_query_as_user("refresh authorization", ADMIN, True)

      # Create should succeed now.
      self._run_query_as_user(
          "create table {0}.foo(a int)".format(test_db), test_user, True)
      # Run show tables on the db. The resulting list should be empty. This happens
      # because the created table's ownership information is not aggressively cached
      # by the current Catalog implementations. Hence the analysis pass does not
      # have access to the ownership information to verify if the current session
      # user is actually the owner. We need to fix this by caching the HMS metadata
      # more aggressively when the table loads. TODO(IMPALA-8937).
      result = \
          self._run_query_as_user("show tables in {0}".format(test_db), test_user, True)
      assert len(result.data) == 0
      # Run a simple query that warms up the table metadata and repeat SHOW TABLES.
      self._run_query_as_user("select * from {0}.foo".format(test_db), test_user, True)
      result = \
          self._run_query_as_user("show tables in {0}".format(test_db), test_user, True)
      assert len(result.data) == 1
      assert "foo" in result.data
      # Change the owner of the db back to the admin user
      self._run_query_as_user(
          "alter database {0} set owner user {1}".format(test_db, ADMIN), ADMIN, True)
      result = self._run_query_as_user(
          "show tables in {0}".format(test_db), test_user, False)
      err = "User '{0}' does not have privileges to access: {1}.*.*". \
          format(test_user, test_db)
      assert err in str(result)
      # test_user is still the owner of the table, so select should work fine.
      self._run_query_as_user("select * from {0}.foo".format(test_db), test_user, True)
      # Change the table owner back to admin.
      self._run_query_as_user(
          "alter table {0}.foo set owner user {1}".format(test_db, ADMIN), ADMIN, True)
      # create role before test begin.
      self._run_query_as_user("CREATE ROLE {0}".format(test_role), ADMIN, True)
      # test alter table owner to role statement, expect success result.
      stmt = "alter table {0}.foo set owner role {1}".format(test_db, test_role)
      self._run_query_as_user(stmt, ADMIN, True)
      # drop the role.
      self._run_query_as_user("DROP ROLE {0}".format(test_role), ADMIN, True)
      # alter table to a non-exist role, expect error showing "role doesn't exist".
      stmt = "alter table {0}.foo set owner role {1}".format(test_db, test_role)
      result = self._run_query_as_user(stmt, ADMIN, False)
      err = "Role '{0}' does not exist.".format(test_role)
      assert err in str(result)
      # test_user should not be authorized to run the queries anymore.
      result = self._run_query_as_user(
          "select * from {0}.foo".format(test_db), test_user, False)
      err = ("AuthorizationException: User '{0}' does not have privileges to execute"
             + " 'SELECT' on: {1}.foo").format(test_user, test_db)
      assert err in str(result)
    finally:
      self._run_query_as_user("drop database {0} cascade".format(test_db), ADMIN, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_select_function_with_fallback_db(self, unique_name):
    """Verifies that Impala should not allow using functions in the fallback database
    unless the user has been granted sufficient privileges on the given database."""
    test_user = "non_owner"
    admin_client = self.create_impala_client()
    non_owner_client = self.create_impala_client()
    refresh_stmt = "refresh authorization"
    unique_database = unique_name + "_db"

    try:
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)
      admin_client.execute("create database %s" % unique_database, user=ADMIN)
      admin_client.execute("create function {0}.identity(bigint) "
                           "RETURNS bigint "
                           "LOCATION "
                           "'{1}/libTestUdfs.so' "
                           "SYMBOL='Identity'"
                           .format(unique_database, WAREHOUSE), user=ADMIN)
      # A user not granted any privilege is not allowed to execute the UDF.
      result = self._run_query_as_user("select identity(1)", test_user, False)
      err = "User '{0}' does not have privileges to SELECT functions in: " \
            "default.identity".format(test_user)
      assert err in str(result)

      admin_client.execute(
          "grant select on database default to user {0}".format(test_user),
          user=ADMIN)
      self._refresh_authorization(admin_client, refresh_stmt)

      result = self._run_query_as_user("select identity(1)", test_user, False)
      err = "default.identity() unknown for database default."
      assert err in str(result)

      # A user is not allowed to access fallback database if the user has no
      # privileges on it, whether the function exists or not.
      result = self.execute_query_expect_failure(
          non_owner_client, "select identity(1)", query_options={
              'FALLBACK_DB_FOR_FUNCTIONS': unique_database}, user=test_user)
      err = "User '{0}' does not have privileges to SELECT functions in: " \
            "{1}.identity".format(test_user, unique_database)
      assert err in str(result)

      result = self.execute_query_expect_failure(
          non_owner_client, "select fn()", query_options={
              'FALLBACK_DB_FOR_FUNCTIONS': unique_database}, user=test_user)
      err = "User '{0}' does not have privileges to SELECT functions in: " \
            "{1}.fn".format(test_user, unique_database)
      assert err in str(result)

      # A user has to be granted a) any of the INSERT, REFRESH, SELECT privileges on all
      # the tables and columns in the fallback database, and b) the SELECT privilege on
      # the UDF in the fallback database in order to execute the UDF.
      admin_client.execute(
          "grant insert on database {0} to user {1}".format(
              unique_database, test_user), user=ADMIN)
      admin_client.execute(
          "grant select on user_defined_fn {0}.identity to user {1}".format(
              unique_database, test_user), user=ADMIN)
      self._refresh_authorization(admin_client, refresh_stmt)

      # A user is allowed to use functions in the fallback database if the user is
      # explicitly granted the SELECT privilege.
      self.execute_query_expect_success(
          non_owner_client,
          "select identity(1)",
          query_options={'FALLBACK_DB_FOR_FUNCTIONS': unique_database},
          user=test_user)
    finally:
      # Revoke the granted privileges.
      admin_client.execute("revoke select on database default from user {0}"
                           .format(test_user), user=ADMIN)
      admin_client.execute("revoke insert on database {0} from user {1}"
                           .format(unique_database, test_user), user=ADMIN)
      admin_client.execute("revoke select on user_defined_fn {0}.identity from user {1}"
                           .format(unique_database, test_user), user=ADMIN)
      # Drop the database.
      self._run_query_as_user("drop database {0} cascade".format(unique_database),
                              ADMIN, True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_column_masking(self, vector, unique_name):
    user = getuser()
    unique_database = unique_name + '_db'
    # Create another client for admin user since current user doesn't have privileges to
    # create/drop databases or refresh authorization.
    admin_client = self.create_impala_client()
    admin_client.execute("drop database if exists %s cascade" % unique_database,
                         user=ADMIN)
    admin_client.execute("create database %s" % unique_database, user=ADMIN)
    # Grant CREATE on database to current user for tests on CTAS, CreateView etc.
    admin_client.execute("grant create on database %s to user %s"
                         % (unique_database, user))
    policy_cnt = 0
    try:
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "id",
        "CUSTOM", "id * 100")   # use column name 'id' directly
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "bool_col",
        "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "string_col",
        "CUSTOM", "concat({col}, 'aaa')")   # use column reference '{col}'
      policy_cnt += 1
      # Add policy to a view
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes_view", "string_col",
        "CUSTOM", "concat('vvv', {col})")
      policy_cnt += 1
      # Add policy to the table used in the view
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes", "id",
        "CUSTOM", "{col} * 100")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes", "string_col",
        "CUSTOM", "concat({col}, 'ttt')")
      policy_cnt += 1
      # Add policy to mask "bigint_col" using a subquery. It will hit IMPALA-10483.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypesagg", "bigint_col",
        "CUSTOM", "(select count(*) from functional.alltypestiny)")
      policy_cnt += 1
      # Add column masking policy for virtual column INPUT__FILE__NAME
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny",
        "input__file__name",
        "CUSTOM", "mask_show_last_n({col}, 10, 'x', 'x', 'x', -1, '1')")
      policy_cnt += 1
      # Add column masking policy to an Iceberg table.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional_parquet", "iceberg_partitioned",
        "id", "MASK_NULL")
      policy_cnt += 1
      # Add column masking policy to an Iceberg V2 table.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional_parquet",
        "iceberg_v2_delete_positional", "data", "MASK_NULL")
      policy_cnt += 1
      # Add invalid column masking policy to trigger an error during re-analyze.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional_parquet",
        "alltypessmall", "string_col", "CUSTOM", "concat(string_col, invalid_col)")
      policy_cnt += 1
      self.execute_query_expect_success(admin_client, "refresh authorization",
                                        user=ADMIN)
      self.run_test_case("QueryTest/ranger_column_masking", vector,
                         test_file_vars={'$UNIQUE_DB': unique_database})
      # Add a policy on a primitive column of a table which contains nested columns.
      for db in ['functional_parquet', 'functional_orc_def']:
        TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, db, "complextypestbl",
          "id", "CUSTOM", "100 * {col}")
        policy_cnt += 1
        # Add policies on a nested column though they won't be recognized (same as Hive).
        TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, db, "complextypestbl",
          "nested_struct.a", "CUSTOM", "100 * {col}")
        policy_cnt += 1
        TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, db, "complextypestbl",
          "int_array", "MASK_NULL")
        policy_cnt += 1
        self.execute_query_expect_success(admin_client, "refresh authorization",
                                          user=ADMIN)
        self.run_test_case("QueryTest/ranger_column_masking_complex_types", vector,
                           use_db=db)
    finally:
      admin_client.execute("revoke create on database %s from user %s"
                           % (unique_database, user))
      admin_client.execute("drop database %s cascade" % unique_database)
      for i in range(policy_cnt):
        TestRanger._remove_policy(unique_name + str(i))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_block_metadata_update(self, vector, unique_name):
    """Test that the metadata update operation on a table by a requesting user is denied
       if there exists a column masking policy defined on any column in the table for the
       requesting user even when the table metadata (e.g., list of columns) have been
       invalidated immediately before the requesting user tries to invalidate the table
       metadata again. This test would have failed if we did not load the table metadata
       for ResetMetadataStmt."""
    user = getuser()
    admin_client = self.create_impala_client()
    non_owner_client = self.create_impala_client()
    try:
      TestRanger._add_column_masking_policy(
          unique_name, user, "functional", "alltypestiny", "id",
          "CUSTOM", "id * 100")
      self.execute_query_expect_success(admin_client,
          "invalidate metadata functional.alltypestiny", user=ADMIN)
      admin_client.execute("grant all on server to user {0}".format(user))
      result = self.execute_query_expect_failure(
          non_owner_client, "invalidate metadata functional.alltypestiny", user=user)
      assert "User '{0}' does not have privileges to execute " \
          "'INVALIDATE METADATA/REFRESH' on: functional.alltypestiny".format(user) \
          in str(result)
    finally:
      TestRanger._remove_policy(unique_name)
      admin_client.execute("revoke all on server from user {0}".format(user))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} {1}".format(IMPALAD_ARGS,
                                  "--allow_catalog_cache_op_from_masked_users=true"),
    catalogd_args=CATALOGD_ARGS)
  def test_allow_metadata_update(self, unique_name):
    self.__test_allow_catalog_cache_op_from_masked_users(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} {1}".format(LOCAL_CATALOG_IMPALAD_ARGS,
                                  "--allow_catalog_cache_op_from_masked_users=true"),
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS)
  def test_allow_metadata_update_local_catalog(self, unique_name):
    self.__test_allow_catalog_cache_op_from_masked_users(unique_name)

  def __test_allow_catalog_cache_op_from_masked_users(self, unique_name):
    """Verify that catalog cache operations are allowed for masked users
    when allow_catalog_cache_op_from_masked_users=true."""
    user = getuser()
    admin_client = self.create_impala_client()
    non_admin_client = self.create_impala_client()
    try:
      # Create a column masking policy on 'user' which is also the owner of the table
      TestRanger._add_column_masking_policy(
          unique_name, user, "functional", "alltypestiny", "id",
          "CUSTOM", "id * 100")

      # At a cold start, the table is unloaded so its owner is unknown.
      # INVALIDATE METADATA <table> is denied since 'user' is not detected as the owner.
      result = self.execute_query_expect_failure(
          non_admin_client, "invalidate metadata functional.alltypestiny", user=user)
      assert "User '{0}' does not have privileges to execute " \
             "'INVALIDATE METADATA/REFRESH' on: functional.alltypestiny".format(user) \
             in str(result)
      # Verify catalogd never loads metadata of this table
      table_loaded_log = "Loaded metadata for: functional.alltypestiny"
      self.assert_catalogd_log_contains("INFO", table_loaded_log, expected_count=0)

      # Run a query to trigger metadata loading on the table
      self.execute_query_expect_success(
        non_admin_client, "describe functional.alltypestiny", user=user)
      # Verify catalogd loads metadata of this table
      self.assert_catalogd_log_contains("INFO", table_loaded_log, expected_count=1)

      # INVALIDATE METADATA <table> is allowed since 'user' is detected as the owner.
      self.execute_query_expect_success(
          non_admin_client, "invalidate metadata functional.alltypestiny", user=user)

      # Run a query to trigger metadata loading on the table
      self.execute_query_expect_success(
          non_admin_client, "describe functional.alltypestiny", user=user)
      # Verify catalogd loads metadata of this table
      self.assert_catalogd_log_contains("INFO", table_loaded_log, expected_count=2)

      # Verify REFRESH <table> is allowed since 'user' is detected as the owner.
      self.execute_query_expect_success(
          non_admin_client, "refresh functional.alltypestiny", user=user)
      self.execute_query_expect_success(
          non_admin_client,
          "refresh functional.alltypestiny partition(year=2009, month=1)", user=user)

      # Clear the catalog cache and grant 'user' enough privileges
      self.execute_query_expect_success(
          admin_client, "invalidate metadata functional.alltypestiny", user=ADMIN)
      admin_client.execute("grant refresh on table functional.alltypestiny to user {0}"
                           .format(user), user=ADMIN)
      try:
        # Now 'user' should be able to run REFRESH/INVALIDATE even if the table is
        # unloaded (not recognize it as the owner).
        self.execute_query_expect_success(
            non_admin_client, "invalidate metadata functional.alltypestiny", user=user)
        self.execute_query_expect_success(
            non_admin_client, "refresh functional.alltypestiny", user=user)
      finally:
        admin_client.execute(
            "revoke refresh on table functional.alltypestiny from user {0}".format(user))
    finally:
      TestRanger._remove_policy(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_masking_overload_coverage(self, vector, unique_name):
    """Test that we have cover all the overloads of the masking functions that could
       appear in using default policies."""
    user = getuser()
    policy_names = []
    # Create another client for admin user since current user doesn't have privileges to
    # create/drop databases or refresh authorization.
    admin_client = self.create_impala_client()
    try:
      for mask_type in ["MASK", "MASK_SHOW_LAST_4", "MASK_SHOW_FIRST_4", "MASK_HASH",
                        "MASK_NULL", "MASK_NONE", "MASK_DATE_SHOW_YEAR"]:
        LOG.info("Testing default mask type " + mask_type)
        # Add masking policies on functional.alltypestiny
        for col in ["bool_col", "tinyint_col", "smallint_col", "int_col", "bigint_col",
                    "float_col", "double_col", "date_string_col", "string_col",
                    "timestamp_col", "year"]:
          policy_name = "_".join((unique_name, col, mask_type))
          TestRanger._add_column_masking_policy(
              policy_name, user, "functional", "alltypestiny", col, mask_type)
          policy_names.append(policy_name)
        # Add masking policies on functional.date_tbl
        for col in ["date_col", "date_part"]:
          policy_name = "_".join((unique_name, col, mask_type))
          TestRanger._add_column_masking_policy(
              policy_name, user, "functional", "date_tbl", col, mask_type)
          policy_names.append(policy_name)
        # Add masking policies on functional.chars_tiny
        for col in ["cs", "cl", "vc"]:
          policy_name = "_".join((unique_name, col, mask_type))
          TestRanger._add_column_masking_policy(
              policy_name, user, "functional", "chars_tiny", col, mask_type)
          policy_names.append(policy_name)

        self.execute_query_expect_success(admin_client, "refresh authorization",
                                          user=ADMIN)
        self.run_test_case("QueryTest/ranger_alltypes_" + mask_type.lower(), vector)
        while policy_names:
          TestRanger._remove_policy(policy_names.pop())
    finally:
      while policy_names:
        TestRanger._remove_policy(policy_names.pop())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_row_filtering(self, vector, unique_name):
    user = getuser()
    unique_database = unique_name + '_db'
    # Create another client for admin user since current user doesn't have privileges to
    # create/drop databases or refresh authorization.
    admin_client = self.create_impala_client()
    admin_client.execute("drop database if exists %s cascade" % unique_database,
                         user=ADMIN)
    admin_client.execute("create database %s" % unique_database, user=ADMIN)
    # Grant CREATE on database to current user for tests on CTAS, CreateView etc.
    # Note that 'user' is the owner of the test tables. No additional GRANTs are required.
    admin_client.execute("grant create on database %s to user %s"
                         % (unique_database, user))
    policy_cnt = 0
    try:
      #######################################################
      # Test row filters on current user
      #######################################################
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional", "alltypestiny", "id % 2 = 0")
      policy_cnt += 1
      # Add a filter using builtin functions
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional", "alltypessmall",
          """(string_col = concat('0', '') and id <= 0) or
             (string_col = '1' and bool_col = true and id > 90)""")
      policy_cnt += 1
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional", "alltypes",
          "year = 2009 and month = 1")
      policy_cnt += 1
      # Add a row-filtering policy using a nonexisting column 'test_id'. Queries in this
      # table will fail in resolving the column.
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional_parquet", "alltypes",
          "test_id = id")
      policy_cnt += 1
      # Add an illegal row filter that could cause parsing error.
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional_parquet", "alltypessmall",
          "100 id = int_col")
      policy_cnt += 1
      # Add a row-filtering policy on a view. 'alltypes_view' is a view on table
      # 'alltypes' which also has a row-filtering policy. They will both be performed.
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional", "alltypes_view",
          "id < 5")
      policy_cnt += 1
      # Row-filtering expr using subquery on current table.
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional", "alltypesagg",
          "id = (select min(id) from functional.alltypesagg)")
      policy_cnt += 1
      # Row-filtering expr using subquery on other tables.
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional_parquet", "alltypesagg",
          "id in (select id from functional.alltypestiny)")
      policy_cnt += 1
      # Row-filtering expr on nested types
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional_parquet", "complextypestbl",
          "nested_struct.a is not NULL")
      policy_cnt += 1
      # Row-filtering expr on Iceberg table
      TestRanger._add_row_filtering_policy(
          unique_name + str(policy_cnt), user, "functional_parquet",
          "iceberg_v2_positional_not_all_data_files_have_delete_files",
          "i % 2 = 1")
      policy_cnt += 1
      admin_client.execute("refresh authorization")
      self.run_test_case("QueryTest/ranger_row_filtering", vector,
                         test_file_vars={'$UNIQUE_DB': unique_database})

      #######################################################
      # Test row filter policy on multiple users
      #######################################################
      TestRanger._add_multiuser_row_filtering_policy(
          unique_name + str(policy_cnt), "functional_parquet", "alltypestiny",
          [user, "non_owner", "non_owner_2"],
          ["id=0", "id=1", "id=2"])
      policy_cnt += 1
      admin_client.execute(
          "grant select on table functional_parquet.alltypestiny to user non_owner")
      admin_client.execute(
          "grant select on table functional_parquet.alltypestiny to user non_owner_2")
      admin_client.execute("refresh authorization")
      non_owner_client = self.create_impala_client()
      non_owner_2_client = self.create_impala_client()
      query = "select id from functional_parquet.alltypestiny"
      assert self.client.execute(query).get_data() == "0"
      assert non_owner_client.execute(query, user="non_owner").get_data() == "1"
      assert non_owner_2_client.execute(query, user="non_owner_2").get_data() == "2"
      query = "select max(id) from functional_parquet.alltypestiny"
      assert self.client.execute(query).get_data() == "0"
      assert non_owner_client.execute(query, user="non_owner").get_data() == "1"
      assert non_owner_2_client.execute(query, user="non_owner_2").get_data() == "2"

      #######################################################
      # Test row filters that contains complex subqueries
      #######################################################
      admin_client.execute("""create table %s.employee (
          e_id bigint,
          e_name string,
          e_nation string)
          stored as textfile""" % unique_database)
      admin_client.execute("""insert into %s.employee values
          (0, '%s', 'CHINA'),
          (1, 'non_owner', 'PERU'),
          (2, 'non_owner2', 'IRAQ')
          """ % (unique_database, user))
      admin_client.execute("grant select on table %s.employee to user %s"
                           % (unique_database, user))
      admin_client.execute("grant select on table %s.employee to user non_owner"
                           % unique_database)
      admin_client.execute("grant select on table %s.employee to user non_owner_2"
                           % unique_database)
      admin_client.execute("grant select on database tpch to user %s" % user)
      admin_client.execute("grant select on database tpch to user non_owner")
      admin_client.execute("grant select on database tpch to user non_owner_2")

      # Each employee can only see customers in the same nation.
      row_filter_tmpl = """c_nationkey in (
            select n_nationkey from tpch.nation
            where n_name in (
              select e_nation from {db}.employee
              where e_name = %s
            )
          )""".format(db=unique_database)
      TestRanger._add_multiuser_row_filtering_policy(
          unique_name + str(policy_cnt), "tpch", "customer",
          [user, 'non_owner', 'non_owner_2'], [row_filter_tmpl % "current_user()"])
      policy_cnt += 1
      admin_client.execute("refresh authorization")

      user_query = "select count(*) from tpch.customer"
      admin_query_tmpl = user_query + " where " + row_filter_tmpl
      self._verified_multiuser_results(
          admin_client, admin_query_tmpl, user_query,
          [user, 'non_owner', 'non_owner_2'],
          [self.client, non_owner_client, non_owner_2_client])

      tpch_q10_tmpl = """select c_custkey, c_name,
          sum(l_extendedprice * (1 - l_discount)) as revenue,
          c_acctbal, n_name, c_address, c_phone, c_comment
        from {customer_place_holder}, tpch.orders, tpch.lineitem, tpch.nation
        where
          c_custkey = o_custkey and l_orderkey = o_orderkey
          and o_orderdate >= '1993-10-01' and o_orderdate < '1994-01-01'
          and l_returnflag = 'R' and c_nationkey = n_nationkey
        group by c_custkey, c_name, c_acctbal, c_phone, n_name, c_address, c_comment
        order by revenue desc, c_custkey
        limit 20"""
      user_query = tpch_q10_tmpl.format(customer_place_holder="tpch.customer")
      admin_value = "(select * from tpch.customer where " + row_filter_tmpl + ") v"
      admin_query_tmpl = tpch_q10_tmpl.format(customer_place_holder=admin_value)
      self._verified_multiuser_results(
          admin_client, admin_query_tmpl, user_query,
          [user, 'non_owner', 'non_owner_2'],
          [self.client, non_owner_client, non_owner_2_client])
    finally:
      for i in range(policy_cnt):
        TestRanger._remove_policy(unique_name + str(i))
      cleanup_statements = [
        "revoke select on table functional_parquet.alltypestiny from user non_owner",
        "revoke select on table functional_parquet.alltypestiny from user non_owner_2",
        "revoke select on database tpch from user non_owner",
        "revoke select on database tpch from user non_owner_2",
        "revoke select on table %s.employee from user %s" % (unique_database, user),
        "revoke select on table %s.employee from user non_owner" % unique_database,
        "revoke select on table %s.employee from user non_owner_2" % unique_database,
      ]
      for statement in cleanup_statements:
        try:
          admin_client.execute(statement, user=ADMIN)
        except Exception as e:
          LOG.error("Ignored exception in cleanup: " + str(e))

  def _verified_multiuser_results(self, admin_client, admin_query_tmpl, user_query, users,
                                 user_clients):
    assert len(users) == len(user_clients)
    for i in range(len(users)):
      admin_res = admin_client.execute(admin_query_tmpl % ("'%s'" % users[i])).get_data()
      user_res = user_clients[i].execute(user_query).get_data()
      assert admin_res == user_res

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_column_masking_and_row_filtering(self, vector, unique_name):
    user = getuser()
    admin_client = self.create_impala_client()
    policy_cnt = 0
    try:
      # 3 column masking policies and 1 row filtering policy on functional.alltypestiny.
      # The row filtering policy will take effect first, then the column masking policies
      # mask the final results.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "id",
        "CUSTOM", "id + 100")   # use column name 'id' directly
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "string_col",
        "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny",
        "date_string_col", "MASK")
      policy_cnt += 1
      TestRanger._add_row_filtering_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypestiny", "id % 3 = 0")
      policy_cnt += 1
      # 2 column masking policies on functional.alltypes and 1 row filtering policy on
      # functional.alltypesview which is a view on functional.alltypes. The column masking
      # policies on functional.alltypes will take effect first, which affects the results
      # of functional.alltypesview. Then the row filtering policy of the view filters
      # out rows of functional.alltypesview.
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes", "id",
        "CUSTOM", "-id")   # use column name 'id' directly
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes",
        "date_string_col", "MASK")
      policy_cnt += 1
      TestRanger._add_row_filtering_policy(
        unique_name + str(policy_cnt), user, "functional", "alltypes_view",
        "id >= -8 and date_string_col = 'nn/nn/nn'")
      policy_cnt += 1

      self.execute_query_expect_success(admin_client, "refresh authorization",
                                        user=ADMIN)
      self.run_test_case("QueryTest/ranger_column_masking_and_row_filtering", vector)
    finally:
      for i in range(policy_cnt):
        TestRanger._remove_policy(unique_name + str(i))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_iceberg_time_travel_with_masking(self, unique_name):
    """When we do a time travel query on an iceberg table we will use the schema from
    the time of the snapshot. Make sure this works when column masking is being used."""
    user = getuser()
    admin_client = self.create_impala_client()
    short_table_name = "ice_1"
    unique_database = unique_name + "_db"
    tbl_name = unique_database + "." + short_table_name

    try:
      admin_client.execute("drop database if exists {0} cascade"
                           .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create table {0} (a int, b string, c int) stored as iceberg"
                           .format(tbl_name), user=ADMIN)
      admin_client.execute("insert into {0} values (1, 'one', 1)".format(tbl_name),
                           user=ADMIN)
      admin_client.execute("alter table {0} drop column a".format(tbl_name), user=ADMIN)
      admin_client.execute("insert into {0} values ('two', 2)".format(tbl_name),
                           user=ADMIN)
      admin_client.execute("grant select on database {0} to user {1} with "
                           "grant option".format(unique_database, user),
                           user=ADMIN)
      admin_client.execute("grant insert on database {0} to user {1} with "
                           "grant option".format(unique_database, user),
                           user=ADMIN)

      snapshots = get_snapshots(admin_client, tbl_name, expected_result_size=2)
      # Create two versions of a simple query based on the two snapshot ids.
      query = "select * from {0} FOR SYSTEM_VERSION AS OF {1}"
      # The first query is for the data after the first insert.
      first_time_travel_query = query.format(tbl_name, snapshots[0].get_snapshot_id())
      # The expected results of the first query.
      first_query_columns = ['A', 'B', 'C']
      first_results_unmasked = ['1\tone\t1']
      first_results_masked_b = ['1\tNULL\t1']  # Column 'B' is masked to NULL.
      first_results_masked_c = ['1\tone\tNULL']  # Column 'C' is masked to NULL.

      # Second query is for the data after the second insert, when the column has gone.
      second_time_travel_query = query.format(tbl_name, snapshots[1].get_snapshot_id())
      # The expected results of the second query, depending on masking.
      second_query_columns = ['B', 'C']
      second_results_unmasked = ['one\t1', 'two\t2']
      second_results_masked_b = ['NULL\t1', 'NULL\t2']  # Column 'B' masked to NULL.
      second_results_masked_c = ['one\tNULL', 'two\tNULL']  # Column 'C' masked to NULL.

      # Run queries without column masking.
      results = self.client.execute(first_time_travel_query)
      assert results.column_labels == first_query_columns
      assert results.data == first_results_unmasked

      results = self.client.execute(second_time_travel_query)
      assert results.column_labels == second_query_columns
      assert len(results.data) == len(second_results_unmasked)
      for row in second_results_unmasked:
        assert row in results.data

      try:
        # Mask column C to null.
        TestRanger._add_column_masking_policy(
          unique_name, user, unique_database, short_table_name, "C", "MASK_NULL")
        admin_client.execute("refresh authorization", user=ADMIN)

        # Run the time travel queries again, time travel should work, but column
        # 'C' is masked.
        results = self.client.execute(first_time_travel_query)
        assert results.column_labels == first_query_columns
        assert results.data == first_results_masked_c

        results = self.client.execute(second_time_travel_query)
        assert results.column_labels == second_query_columns
        assert len(results.data) == len(second_results_masked_c)
        for row in second_results_masked_c:
          assert row in results.data
      finally:
        # Remove the masking policy.
        TestRanger._remove_policy(unique_name)
        admin_client.execute("refresh authorization", user=ADMIN)

      # Run the queries again without masking as we are here.
      results = self.client.execute(first_time_travel_query)
      assert results.column_labels == first_query_columns
      assert results.data == first_results_unmasked

      results = self.client.execute(second_time_travel_query)
      assert results.column_labels == second_query_columns
      assert len(results.data) == len(second_results_unmasked)
      for row in second_results_unmasked:
        assert row in results.data

      try:
        # Mask column B to null.
        TestRanger._add_column_masking_policy(
          unique_name, user, unique_database, short_table_name, "B", "MASK_NULL")
        admin_client.execute("refresh authorization", user=ADMIN)

        # Run the time travel queries again, time travel should work, but column
        # 'B' is masked.
        results = self.client.execute(first_time_travel_query)
        assert results.column_labels == first_query_columns
        assert results.data == first_results_masked_b

        results = self.client.execute(second_time_travel_query)
        assert results.column_labels == second_query_columns
        for row in second_results_masked_b:
          assert row in results.data
      finally:
        TestRanger._remove_policy(unique_name)
    finally:
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_convert_table_to_iceberg(self, unique_name):
    """Test that autorization is taken into account when performing a table migration to
    Iceberg."""
    user = getuser()
    admin_client = self.create_impala_client()
    non_admin_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    tbl_name = unique_database + "." + "hive_tbl_to_convert"

    try:
      admin_client.execute("drop database if exists {0} cascade"
                           .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)

      # create table using admin user.
      admin_client.execute("create table {0} (a int, b string) stored as parquet".format(
          tbl_name), user=ADMIN)
      admin_client.execute("insert into {0} values (1, 'one')".format(tbl_name),
                           user=ADMIN)

      try:
        # non-admin user can't convert table by default.
        result = self.execute_query_expect_failure(
            non_admin_client, "alter table {0} convert to iceberg".format(tbl_name),
            user=user)
        assert "User '{0}' does not have privileges to access: {1}".format(
            user, unique_database) in str(result)

        # Grant ALL privileges on the table for non-admin user. Even with this the query
        # should fail as we expect DB level ALL privileges for table migration. Once
        # https://issues.apache.org/jira/browse/IMPALA-12190 is fixed, this should also
        # pass with table-level ALL privileges.
        admin_client.execute("grant all on table {0} to user {1}".format(tbl_name, user),
            user=ADMIN)
        result = self.execute_query_expect_failure(
            non_admin_client, "alter table {0} convert to iceberg".format(tbl_name),
            user=user)
        assert "User '{0}' does not have privileges to access: {1}".format(
            user, unique_database) in str(result)

        # After granting ALL privileges on the DB, the table migration should succeed.
        admin_client.execute("grant all on database {0} to user {1}"
            .format(unique_database, user), user=ADMIN)
        self.execute_query_expect_success(
            non_admin_client, "alter table {0} convert to iceberg".format(tbl_name),
            user=user)

        result = non_admin_client.execute("describe formatted {0}".format(tbl_name),
            user=user)
        assert "org.apache.iceberg.mr.hive.HiveIcebergSerDe" in str(result)
        assert "org.apache.iceberg.mr.hive.HiveIcebergInputFormat" in str(result)
        assert "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat" in str(result)
      finally:
        # Revoke privileges
        admin_client.execute("revoke all on table {0} from user {1}"
                            .format(tbl_name, user), user=ADMIN)
        admin_client.execute("revoke all on database {0} from user {1}"
                            .format(unique_database, user), user=ADMIN)

      tbl_name2 = unique_database + "." + "hive_tbl_to_convert2"
      # create table using admin user.
      admin_client.execute("create table {0} (a int, b string) stored as parquet".format(
          tbl_name2), user=ADMIN)
      admin_client.execute("insert into {0} values (1, 'one')".format(tbl_name2),
                           user=ADMIN)

      try:
        admin_client.execute("grant all on table {0} to user {1}"
                             .format(tbl_name2, user), user=ADMIN)
        result = self.execute_query_expect_success(
            non_admin_client, "select count(*) from {0}".format(tbl_name2), user=user)
        assert result.get_data() == "1"

        # Migrates the table by admin and checks if the non-admin usert still has
        # privileges.
        self.execute_query_expect_success(
            admin_client, "alter table {0} convert to iceberg".format(tbl_name2),
            user=ADMIN)

        result = self.execute_query_expect_success(
            non_admin_client, "select count(*) from {0}".format(tbl_name2), user=user)
        assert result.get_data() == "1"
      finally:
        # Revoke privileges
        admin_client.execute("revoke all on table {0} from user {1}"
                            .format(tbl_name2, user), user=ADMIN)

    finally:
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_iceberg_metadata_table_privileges(self, unique_name):
    user = getuser()
    admin_client = self.create_impala_client()
    non_admin_client = self.create_impala_client()
    short_table_name = "ice_1"
    unique_database = unique_name + "_db"
    tbl_name = unique_database + "." + short_table_name

    try:
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create table {0} (a int) stored as iceberg"
          .format(tbl_name), user=ADMIN)

      # At this point, non-admin user without select privileges cannot query the metadata
      # tables
      result = self.execute_query_expect_failure(non_admin_client,
          "select * from {0}.history".format(tbl_name), user=user)
      assert "User '{0}' does not have privileges to execute 'SELECT' on: {1}".format(
          user, unique_database) in str(result)

      # Grant 'user' select privilege on the table
      admin_client.execute("grant select on table {0} to user {1}".format(tbl_name, user),
          user=ADMIN)
      result = non_admin_client.execute("select * from {0}.history".format(tbl_name),
          user=user)
      assert result.success is True

    finally:
      admin_client.execute("revoke select on table {0} from user {1}"
          .format(tbl_name, user), user=ADMIN)
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
          user=ADMIN)

  @pytest.mark.execute_serially
  @SkipIf.is_test_jdk
  @SkipIfFS.hive
  @SkipIfHive2.ranger_auth
  @CustomClusterTestSuite.with_args()
  def test_hive_with_ranger_setup(self, vector):
    """Test for setup of Hive-Ranger integration. Make sure future upgrades on
    Hive/Ranger won't break the tool."""
    script = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin/run-hive-server.sh')
    try:
      # Add the policy before restarting Hive. So it can take effect immediately after
      # HiveServer2 starts.
      TestRanger._add_column_masking_policy(
          "col_mask_for_hive", getuser(), "functional", "alltypestiny", "id", "CUSTOM",
          "{col} * 100")
      check_call([script, '-with_ranger'])
      self.run_test_case("QueryTest/hive_ranger_integration", vector)
    finally:
      check_call([script])
      TestRanger._remove_policy("col_mask_for_hive")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_profile_protection(self, vector):
    """Test that a requesting user is able to access the runtime profile or execution
    summary of a query involving a view only if the user is granted the privileges on all
    the underlying tables of the view. Recall that the view functional.complex_view we
    use here is created based on the tables functional.alltypesagg and
    functional.alltypestiny."""
    admin_client = self.create_impala_client()
    non_owner_client = self.create_impala_client()
    test_db = "functional"
    test_view = "complex_view"
    grantee_user = "non_owner"
    try:
      admin_client.execute(
          "grant select on table {0}.{1} to user {2}"
          .format(test_db, test_view, grantee_user), user=ADMIN)

      admin_client.execute("refresh authorization")
      # Recall that in a successful execution, result.exec_summary and
      # result.runtime_profile store the execution summary and runtime profile,
      # respectively. But when the requesting user does not have the privileges
      # on the underlying tables, an exception will be thrown from
      # ImpalaBeeswaxClient.get_runtime_profile().
      result = self.execute_query_expect_failure(
          non_owner_client, "select count(*) from {0}.{1}".format(test_db, test_view),
          user=grantee_user)
      assert "User {0} is not authorized to access the runtime profile or " \
          "execution summary".format(grantee_user) in str(result)

      admin_client.execute(
          "grant select on table {0}.alltypesagg to user {1}"
          .format(test_db, grantee_user), user=ADMIN)

      admin_client.execute("refresh authorization")
      self.execute_query_expect_failure(
          non_owner_client, "select count(*) from {0}.{1}".format(test_db, test_view),
          user=grantee_user)
      assert "User {0} is not authorized to access the runtime profile or " \
          "execution summary".format(grantee_user) in str(result)

      admin_client.execute(
          "grant select on table {0}.alltypestiny to user {1}"
          .format(test_db, grantee_user), user=ADMIN)

      admin_client.execute("refresh authorization")
      self.execute_query_expect_success(
          non_owner_client, "select count(*) from {0}.{1}".format(test_db, test_view),
          user=grantee_user)
    finally:
      cleanup_statements = [
          "revoke select on table {0}.{1} from user {2}"
          .format(test_db, test_view, grantee_user),
          "revoke select on table {0}.alltypesagg from user {1}"
          .format(test_db, grantee_user),
          "revoke select on table {0}.alltypestiny from user {1}"
          .format(test_db, grantee_user)
      ]

      for statement in cleanup_statements:
        admin_client.execute(statement, user=ADMIN)

  @pytest.mark.execute_serially
  @SkipIfFS.incorrent_reported_ec
  @CustomClusterTestSuite.with_args(
    # We additionally provide impalad and catalogd with the customized user-to-groups
    # mapper since some test cases in grant_revoke.test require Impala to retrieve the
    # groups a given user belongs to and such users might not exist in the underlying
    # OS in the testing environment, e.g., the user 'non_owner'.
    impalad_args="{0} {1}".format(IMPALAD_ARGS,
                                  "--use_customized_user_groups_mapper_for_ranger"),
    catalogd_args="{0} {1}".format(CATALOGD_ARGS,
                                   "--use_customized_user_groups_mapper_for_ranger"))
  def test_grant_revoke_with_role(self, vector):
    """Test grant/revoke with role."""
    admin_client = self.create_impala_client()
    try:
      self.run_test_case('QueryTest/grant_revoke', vector, use_db="default")
    finally:
      # Below are the statements that need to be executed in order to clean up the
      # privileges granted to the test roles as well as the test roles themselves.
      # Note that we need to revoke those previously granted privileges so that each role
      # is not referenced by any policy before we delete those roles.
      # Moreover, we need to revoke the privilege on the database 'grant_rev_db' before
      # dropping 'grant_rev_db'. Otherwise, the revocation would fail due to an
      # AnalysisException thrown because 'grant_rev_db' does not exist.
      cleanup_statements = [
        "revoke all on database grant_rev_db from grant_revoke_test_ALL_TEST_DB",
        "revoke all on server from grant_revoke_test_ALL_SERVER",
        "revoke all on table functional.alltypes from grant_revoke_test_NON_OWNER",
        "revoke grant option for all on database functional "
        "from grant_revoke_test_NON_OWNER",
        "REVOKE SELECT (a, b, c, d, e, x, y) ON TABLE grant_rev_db.test_tbl3 "
        "FROM grant_revoke_test_ALL_SERVER",
        "REVOKE ALL ON DATABASE functional FROM grant_revoke_test_NON_OWNER",
        "REVOKE SELECT ON TABLE grant_rev_db.test_tbl3 FROM grant_revoke_test_NON_OWNER",
        "REVOKE GRANT OPTION FOR SELECT (a, c) ON TABLE grant_rev_db.test_tbl3 "
        "FROM grant_revoke_test_ALL_SERVER",
        "REVOKE SELECT ON TABLE grant_rev_db.test_tbl1 "
        "FROM grant_revoke_test_SELECT_INSERT_TEST_TBL",
        "REVOKE INSERT ON TABLE grant_rev_db.test_tbl1 "
        "FROM grant_revoke_test_SELECT_INSERT_TEST_TBL",
        "REVOKE SELECT ON TABLE grant_rev_db.test_tbl3 "
        "FROM grant_revoke_test_NON_OWNER",
        "REVOKE SELECT (a) ON TABLE grant_rev_db.test_tbl3 "
        "FROM grant_revoke_test_NON_OWNER",
        "REVOKE SELECT (a, c, e) ON TABLE grant_rev_db.test_tbl3 "
        "FROM grant_revoke_test_ALL_SERVER",
        "revoke all on server server1 from grant_revoke_test_ALL_SERVER1",
        "revoke select(col1) on table grant_rev_db.test_tbl4 "
        "from role grant_revoke_test_COLUMN_PRIV",
        "{0}{1}{2}".format("revoke all on uri '",
                           os.getenv("FILESYSTEM_PREFIX"),
                           "/test-warehouse/grant_rev_test_tbl2'"
                           "from grant_revoke_test_ALL_URI"),
        "{0}{1}{2}".format("revoke all on uri '",
                           os.getenv("FILESYSTEM_PREFIX"),
                           "/test-warehouse/GRANT_REV_TEST_TBL3'"
                           "from grant_revoke_test_ALL_URI"),
        "{0}{1}{2}".format("revoke all on uri '",
                           os.getenv("FILESYSTEM_PREFIX"),
                           "/test-warehouse/grant_rev_test_prt'"
                           "from grant_revoke_test_ALL_URI"),
        "drop role grant_revoke_test_ALL_TEST_DB",
        "drop role grant_revoke_test_ALL_SERVER",
        "drop role grant_revoke_test_SELECT_INSERT_TEST_TBL",
        "drop role grant_revoke_test_ALL_URI",
        "drop role grant_revoke_test_NON_OWNER",
        "drop role grant_revoke_test_ALL_SERVER1",
        "drop role grant_revoke_test_COLUMN_PRIV",
        "drop database grant_rev_db cascade"
      ]

      for statement in cleanup_statements:
        try:
          admin_client.execute(statement, user=ADMIN)
        except Exception:
          # There could be an exception thrown due to the non-existence of the role or
          # resource involved in a statement that aims to revoke the privilege on a
          # resource from a role, but we do not have to handle such an exception. We only
          # need to make sure in the case when the role and the corresponding resource
          # exist, the granted privilege is revoked. The same applies to the case when we
          # drop a role.
          pass

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_select_view_created_by_non_superuser_with_catalog_v1(self, unique_name):
    self._test_select_view_created_by_non_superuser(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS)
  def test_select_view_created_by_non_superuser_with_local_catalog(self, unique_name):
    self._test_select_view_created_by_non_superuser(unique_name)

  def _test_select_view_created_by_non_superuser(self, unique_name):
    """Test that except for the necessary privileges on the view, the requesting user
    has to be granted the necessary privileges on the underlying tables as well in order
    to access a view with its table property of 'Authorized' set to false."""
    admin_client = self.create_impala_client()
    non_owner_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    test_tbl_1 = unique_name + "_tbl_1"
    test_tbl_2 = unique_name + "_tbl_2"
    test_view = unique_name + "_view"
    grantee_user = "non_owner"

    try:
      # Set up temp database, tables, and the view.
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      admin_client.execute("create database {0}".format(unique_database), user=ADMIN)
      admin_client.execute("create table {0}.{1} (id int, bigint_col bigint)"
          .format(unique_database, test_tbl_1), user=ADMIN)
      admin_client.execute("create table {0}.{1} (id int, string_col string)"
          .format(unique_database, test_tbl_2), user=ADMIN)
      admin_client.execute("create view {0}.{1} (abc, xyz) as "
          "select count(a.bigint_col), b.string_col "
          "from {0}.{2} a inner join {0}.{3} b on a.id = b.id "
          "group by b.string_col having count(a.bigint_col) > 1"
          .format(unique_database, test_view, test_tbl_1, test_tbl_2), user=ADMIN)
      # Add this table property to simulate a view created by a non-superuser.
      self.run_stmt_in_hive("alter view {0}.{1} "
          "set tblproperties ('Authorized' = 'false')"
          .format(unique_database, test_view))

      admin_client.execute("grant select(abc) on table {0}.{1} to user {2}"
          .format(unique_database, test_view, grantee_user), user=ADMIN)
      admin_client.execute("grant select(xyz) on table {0}.{1} to user {2}"
          .format(unique_database, test_view, grantee_user), user=ADMIN)
      admin_client.execute("grant select on table {0}.{1} to user {2}"
          .format(unique_database, test_tbl_2, grantee_user), user=ADMIN)
      admin_client.execute("refresh authorization")

      result = self.execute_query_expect_failure(non_owner_client,
          "select * from {0}.{1}".format(unique_database, test_view), user=grantee_user)
      assert "User '{0}' does not have privileges to execute 'SELECT' on: " \
          "{1}.{2}".format(grantee_user, unique_database, test_tbl_1) in str(result)

      admin_client.execute("grant select(id) on table {0}.{1} to user {2}"
          .format(unique_database, test_tbl_1, grantee_user), user=ADMIN)
      admin_client.execute("refresh authorization")
      # The query is not authorized since the SELECT privilege on the column 'bigint_col'
      # in the table 'test_tbl_1' is also required.
      result = self.execute_query_expect_failure(non_owner_client,
          "select * from {0}.{1}".format(unique_database, test_view), user=grantee_user)
      assert "User '{0}' does not have privileges to execute 'SELECT' on: " \
          "{1}.{2}".format(grantee_user, unique_database, test_tbl_1) in str(result)

      admin_client.execute("grant select(bigint_col) on table {0}.{1} to user {2}"
          .format(unique_database, test_tbl_1, grantee_user), user=ADMIN)
      admin_client.execute("refresh authorization")

      # The query is authorized successfully once sufficient privileges are granted.
      self.execute_query_expect_success(non_owner_client,
          "select * from {0}.{1}".format(unique_database, test_view), user=grantee_user)

      # Add a deny policy to prevent 'grantee_user' from accessing the column 'id' in
      # the table 'test_tbl_2', on which 'grantee_user' had been granted the SELECT
      # privilege.
      TestRanger._add_deny_policy(unique_name, grantee_user, unique_database, test_tbl_2,
          "id")
      admin_client.execute("refresh authorization")

      # The query is not authorized since the SELECT privilege on the column 'id' in the
      # table 'test_tbl_2' has been denied in the policy above.
      result = self.execute_query_expect_failure(non_owner_client,
          "select * from {0}.{1}".format(unique_database, test_view), user=grantee_user)
      assert "User '{0}' does not have privileges to execute 'SELECT' on: " \
          "{1}.{2}".format(grantee_user, unique_database, test_tbl_2) in str(result)
    finally:
      admin_client.execute("revoke select(abc) on table {0}.{1} from user {2}"
          .format(unique_database, test_view, grantee_user), user=ADMIN)
      admin_client.execute("revoke select(xyz) on table {0}.{1} from user {2}"
          .format(unique_database, test_view, grantee_user), user=ADMIN)
      admin_client.execute("revoke select(id) on table {0}.{1} from user {2}"
           .format(unique_database, test_tbl_1, grantee_user), user=ADMIN)
      admin_client.execute("revoke select(bigint_col) on table {0}.{1} from user {2}"
           .format(unique_database, test_tbl_1, grantee_user), user=ADMIN)
      admin_client.execute("revoke select on table {0}.{1} from user {2}"
          .format(unique_database, test_tbl_2, grantee_user), user=ADMIN)
      admin_client.execute("drop database if exists {0} cascade"
          .format(unique_database), user=ADMIN)
      TestRanger._remove_policy(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS,
    # We additionally set 'reset_ranger' to True, to reset all the policies in the
    # Ranger service, so even if there were roles before this test, they will be
    # deleted when this test runs. since the Ranger policies are reset before this
    # test, we do not have to worry about there could be existing roles when the test
    # is running.
    reset_ranger=True)
  def test_no_exception_in_show_roles_if_no_roles_in_ranger(self, unique_name):
    self._test_no_exception_in_show_roles_if_no_roles_in_ranger(unique_name)

  def _test_no_exception_in_show_roles_if_no_roles_in_ranger(self, unique_name):
    """
    Ensure that no exception should throw for show roles statement
    if there are no roles in ranger.
    """
    admin_client = self.create_impala_client()
    show_roles_statements = [
      "SHOW ROLES",
      "SHOW CURRENT ROLES",
      "SHOW ROLE GRANT GROUP admin"
    ]
    for statement in show_roles_statements:
      result = self.execute_query_expect_success(admin_client, statement,
                                                 user=ADMIN)
      assert len(result.data) == 0


class TestRangerColumnMaskingTpchNested(CustomClusterTestSuite):
  """
  Tests for Apache Ranger column masking policies on tpch nested tables.
  """

  @classmethod
  def get_workload(cls):
    return 'tpch_nested'

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Do not call the super() implementation because this class needs to relax the
    # set of constraints.
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_tpch_nested_column_masking(self, vector):
    """Test column masking on nested tables"""
    user = getuser()
    db = "tpch_nested_parquet"
    # Mask PII columns: name, phone, address
    tbl_cols = {
      "customer": ["c_name", "c_phone", "c_address"],
      "supplier": ["s_name", "s_phone", "s_address"],
      "part": ["p_name"],
    }
    # Create another client for admin user since current user doesn't have privileges to
    # create/drop databases or refresh authorization.
    admin_client = self.create_impala_client()
    try:
      for tbl in tbl_cols:
        for col in tbl_cols[tbl]:
          policy_name = "%s_%s_mask" % (tbl, col)
          # Q22 requires showing the first 2 chars of the phone column.
          mask_type = "MASK_SHOW_FIRST_4" if col.endswith("phone") else "MASK"
          TestRanger._add_column_masking_policy(
            policy_name, user, db, tbl, col, mask_type)
      self.execute_query_expect_success(admin_client, "refresh authorization",
                                        user=ADMIN)
      same_result_queries = ["q1", "q3", "q4", "q5", "q6", "q7", "q8", "q11", "q12",
                             "q13", "q14", "q16", "q17", "q19", "q22"]
      result_masked_queries = ["q9", "q10", "q15", "q18", "q20", "q21", "q2"]
      for q in same_result_queries:
        self.run_test_case("tpch_nested-" + q, vector, use_db=db)
      for q in result_masked_queries:
        self.run_test_case("masked-tpch_nested-" + q, vector, use_db=db)
    finally:
      for tbl in tbl_cols:
        for col in tbl_cols[tbl]:
          policy_name = "%s_%s_mask" % (tbl, col)
          TestRanger._remove_policy(policy_name)


class TestRangerColumnMaskingComplexTypesInSelectList(CustomClusterTestSuite):
  """
  Tests Ranger policies when complex types are given in the select list. The reason
  this is a separate class is that directly querying complex types works only on HS2
  while some tests in TestRanger needs Beeswax interface otherwise some of them fails.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRangerColumnMaskingComplexTypesInSelectList, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_orc_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') == 'hs2')

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Do not call the super() implementation, because this class needs to relax
    # the set of constraints. The usual constraints only run on uncompressed text.
    # This disables that constraint to let us run against only ORC.
    return

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_column_masking_with_structs_in_select_list(self, vector, unique_name):
    user = getuser()
    db = "functional_orc_def"
    # Create another client for admin user since current user doesn't have privileges to
    # create/drop databases or refresh authorization.
    admin_client = self.create_impala_client()
    policy_cnt = 0
    try:
      # Add a policy on a primitive column of a table which contains nested columns.
      TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, "functional_orc_def",
          "complextypes_structs", "str", "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, "functional_orc_def",
          "complextypes_structs", "tiny_struct", "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, "functional_orc_def",
          "complextypestbl", "id", "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, "functional_orc_def",
          "complextypestbl", "int_array_array", "MASK_NULL")
      policy_cnt += 1
      TestRanger._add_column_masking_policy(
          unique_name + str(policy_cnt), user, "functional_orc_def",
          "complextypestbl", "int_array_map", "MASK_NULL")
      policy_cnt += 1
      self.execute_query_expect_success(admin_client, "refresh authorization",
          user=ADMIN)
      self.run_test_case("QueryTest/ranger_column_masking_struct_in_select_list", vector,
          use_db=db)
    finally:
      for i in range(policy_cnt):
        TestRanger._remove_policy(unique_name + str(i))
