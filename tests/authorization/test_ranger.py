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

import os
import grp
import json
import pytest
import requests
from subprocess import check_call

from getpass import getuser
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import (SkipIfS3, SkipIfABFS, SkipIfADLS, SkipIfIsilon,
                               SkipIfLocal, SkipIfHive2)
from tests.util.hdfs_util import NAMENODE
from tests.util.calculation_util import get_random_id

ADMIN = "admin"
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
              [kw, ident, unique_database, "", "", "", "*", "select", "false"],
              [kw, ident, unique_database, "*", "*", "", "", "select", "false"]])
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
          ["USER", user1, unique_database, "", "", "", "*", "insert", "true"],
          ["USER", user1, unique_database, "", "", "", "*", "select", "true"],
          ["USER", user1, unique_database, "*", "*", "", "", "insert", "true"],
          ["USER", user1, unique_database, "*", "*", "", "", "select", "true"]])

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
          ["USER", user1, unique_database, "", "", "", "*", "insert", "false"],
          ["USER", user1, unique_database, "*", "*", "", "", "insert", "false"]])

      # Revoke privilege granting from user 1
      self.execute_query_expect_success(admin_client, "revoke grant option for insert "
                                                      "on database {0} from user {1}"
                                        .format(unique_database, user1), user=ADMIN)

      # User 1 can no longer grant privileges on unique_database
      # In ranger it is currently not possible to revoke grant for a single access type
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      TestRanger._check_privileges(result, [
          ["USER", user1, unique_database, "", "", "", "*", "insert", "false"],
          ["USER", user1, unique_database, "*", "*", "", "", "insert", "false"]])
    finally:
      admin_client.execute("revoke insert on database {0} from user {1}"
                           .format(unique_database, user1), user=ADMIN)
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=ADMIN)

  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_show_grant(self, unique_name):
    user = getuser()
    group = grp.getgrnam(getuser()).gr_name
    test_data = [(user, "USER"), (group, "GROUP")]
    admin_client = self.create_impala_client()
    unique_db = unique_name + "_db"
    unique_table = unique_name + "_tbl"

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
                                    unique_table)
        # Test that omitting ON <resource> results in failure
        self._test_show_grant_without_on(data[1], data[0])

        # Test inherited privileges (server privileges show for database, etc.)
        self._test_show_grant_inherited(admin_client, data[1], data[0], unique_db,
                                        unique_table)

      # Test ALL privilege hides other privileges
      self._test_show_grant_mask(admin_client, user)

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
          ["GROUP", user, unique_db, "", "", "", "*", "select", "false"],
          ["GROUP", user, unique_db, "*", "*", "", "", "select", "false"]])
    finally:
      admin_client.execute("revoke select on database {0} from group {1}"
                           .format(unique_db, group))

  def _test_show_grant_mask(self, admin_client, user):
    privileges = ["select", "insert", "create", "alter", "drop", "refresh"]
    try:
      for privilege in privileges:
        admin_client.execute("grant {0} on server to user {1}".format(privilege, user))
      result = self.client.execute("show grant user {0} on server".format(user))
      TestRanger._check_privileges(result, [
          ["USER", user, "", "", "", "*", "", "alter", "false"],
          ["USER", user, "", "", "", "*", "", "create", "false"],
          ["USER", user, "", "", "", "*", "", "drop", "false"],
          ["USER", user, "", "", "", "*", "", "insert", "false"],
          ["USER", user, "", "", "", "*", "", "refresh", "false"],
          ["USER", user, "", "", "", "*", "", "select", "false"],
          ["USER", user, "*", "", "", "", "*", "alter", "false"],
          ["USER", user, "*", "", "", "", "*", "create", "false"],
          ["USER", user, "*", "", "", "", "*", "drop", "false"],
          ["USER", user, "*", "", "", "", "*", "insert", "false"],
          ["USER", user, "*", "", "", "", "*", "refresh", "false"],
          ["USER", user, "*", "", "", "", "*", "select", "false"],
          ["USER", user, "*", "*", "*", "", "", "alter", "false"],
          ["USER", user, "*", "*", "*", "", "", "create", "false"],
          ["USER", user, "*", "*", "*", "", "", "drop", "false"],
          ["USER", user, "*", "*", "*", "", "", "insert", "false"],
          ["USER", user, "*", "*", "*", "", "", "refresh", "false"],
          ["USER", user, "*", "*", "*", "", "", "select", "false"]])

      admin_client.execute("grant all on server to user {0}".format(user))
      result = self.client.execute("show grant user {0} on server".format(user))
      TestRanger._check_privileges(result, [
          ["USER", user, "", "", "", "*", "", "all", "false"],
          ["USER", user, "*", "", "", "", "*", "all", "false"],
          ["USER", user, "*", "*", "*", "", "", "all", "false"]])
    finally:
      admin_client.execute("revoke all on server from user {0}".format(user))
      for privilege in privileges:
        admin_client.execute("revoke {0} on server from user {1}".format(privilege, user))

  def _test_show_grant_basic(self, admin_client, kw, id, unique_database, unique_table):
    uri = '/tmp'
    try:
      # Grant server privileges and verify
      admin_client.execute("grant all on server to {0} {1}".format(kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on server".format(kw, id))
      TestRanger._check_privileges(result, [
          [kw, id, "", "", "", "*", "", "all", "false"],
          [kw, id, "*", "", "", "", "*", "all", "false"],
          [kw, id, "*", "*", "*", "", "", "all", "false"]])

      # Revoke server privileges and verify
      admin_client.execute("revoke all on server from {0} {1}".format(kw, id))
      result = self.client.execute("show grant {0} {1} on server".format(kw, id))
      TestRanger._check_privileges(result, [])

      # Grant uri privileges and verify
      admin_client.execute("grant all on uri '{0}' to {1} {2}"
                           .format(uri, kw, id))
      result = self.client.execute("show grant {0} {1} on uri '{2}'"
                                   .format(kw, id, uri))
      TestRanger._check_privileges(result, [
          [kw, id, "", "", "", "{0}{1}".format(NAMENODE, uri), "", "all", "false"]])

      # Revoke uri privileges and verify
      admin_client.execute("revoke all on uri '{0}' from {1} {2}"
                           .format(uri, kw, id))
      result = self.client.execute("show grant {0} {1} on uri '{2}'"
                                   .format(kw, id, uri))
      TestRanger._check_privileges(result, [])

      # Grant database privileges and verify
      admin_client.execute("grant select on database {0} to {1} {2}"
                           .format(unique_database, kw, id))
      result = self.client.execute("show grant {0} {1} on database {2}"
                                   .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, "", "", "", "*", "select", "false"],
          [kw, id, unique_database, "*", "*", "", "", "select", "false"]])

      # Revoke database privileges and verify
      admin_client.execute("revoke select on database {0} from {1} {2}"
                           .format(unique_database, kw, id))
      result = self.client.execute("show grant {0} {1} on database {2}"
                                   .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [])

      # Grant table privileges and verify
      admin_client.execute("grant select on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, unique_table, "*", "", "", "select", "false"]])

      # Revoke table privileges and verify
      admin_client.execute("revoke select on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [])

      # Grant column privileges and verify
      admin_client.execute("grant select(x) on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, unique_table, "x", "", "", "select", "false"]])

      # Revoke column privileges and verify
      admin_client.execute("revoke select(x) on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [])
    finally:
      admin_client.execute("revoke all on server from {0} {1}".format(kw, id))
      admin_client.execute("revoke all on uri '{0}' from {1} {2}"
                           .format(uri, kw, id))
      admin_client.execute("revoke select on database {0} from {1} {2}"
                           .format(unique_database, kw, id))
      admin_client.execute("revoke select on table {0}.{1} from {2} {3}"
                           .format(unique_database, unique_table, kw, id))
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
          [kw, id, "", "", "", "*", "", "select", "false"],
          [kw, id, "*", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a database
      result = self.client.execute("show grant {0} {1} on database {2}"
          .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a table
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "select", "false"]])

      # Verify the highest level of resource that contains the specified resource could
      # be computed when the specified resource is a column
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "select", "false"]])

      # Grant the create privilege on database and verify
      admin_client.execute("grant create on database {0} to {1} {2}"
                           .format(unique_database, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on database {2}"
                                   .format(kw, id, unique_database))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "", "", "", "*", "select", "false"],
          [kw, id, "*", "*", "*", "", "", "select", "false"],
          [kw, id, unique_database, "", "", "", "*", "create", "false"],
          [kw, id, unique_database, "*", "*", "", "", "create", "false"]
      ])

      # Grant the insert privilege on table and verify
      admin_client.execute("grant insert on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on table {2}.{3}"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, "*", "*", "*", "", "", "select", "false"],
          [kw, id, unique_database, "*", "*", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "insert", "false"]
      ])

      # Grant the select privilege on column and verify
      admin_client.execute("grant select(x) on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, "*", "*", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "insert", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "select", "false"]
      ])

      # The insert privilege on table masks the select privilege just added
      admin_client.execute("grant select on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, "*", "*", "", "", "create", "false"],
          [kw, id, unique_database, unique_table, "*", "", "", "insert", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "select", "false"]
      ])

      # The all privilege on table masks the privileges of insert and select, but not the
      # select privilege on column.
      admin_client.execute("grant all on table {0}.{1} to {2} {3}"
                           .format(unique_database, unique_table, kw, id), user=ADMIN)
      result = self.client.execute("show grant {0} {1} on column {2}.{3}.x"
                                   .format(kw, id, unique_database, unique_table))
      TestRanger._check_privileges(result, [
          [kw, id, unique_database, unique_table, "*", "", "", "all", "false"],
          [kw, id, unique_database, unique_table, "x", "", "", "select", "false"]
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
          ["USER", user, unique_db, "*", "*", "", "", "create", "false"],
          ["USER", user, unique_db, "*", "*", "", "", "select", "false"]
      ])

      # Revoke privileges via Ranger REST API
      TestRanger._revoke_ranger_privilege(user, resource, access)

      # Privileges should be stale before a refresh
      result = self.client.execute("show grant user {0} on database {1}"
                                   .format(user, unique_db))
      TestRanger._check_privileges(result, [
          ["USER", user, unique_db, "*", "*", "", "", "create", "false"],
          ["USER", user, unique_db, "*", "*", "", "", "select", "false"]
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

  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
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
          ["USER", user, unique_db, "*", "*", "", "", "select", "false"]
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
    data = {"name": user, "password": "password123", "userRoleList": ["ROLE_USER"]}
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
    return json.loads(r.content)["id"]

  @staticmethod
  def _remove_column_masking_policy(policy_name):
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

    assert map(columns, result.data) == expected

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

  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_unsupported_sql(self):
    """Tests unsupported SQL statements when running with Ranger."""
    user = "admin"
    impala_client = self.create_impala_client()
    error_msg = "UnsupportedFeatureException: {0} is not supported by Ranger."
    for statement in [("show roles", error_msg.format("SHOW ROLES")),
                      ("show current roles", error_msg.format("SHOW CURRENT ROLES")),
                      ("create role foo", error_msg.format("CREATE ROLE")),
                      ("drop role foo", error_msg.format("DROP ROLE")),
                      ("grant select on database functional to role foo",
                       error_msg.format("GRANT <privilege> TO ROLE")),
                      ("revoke select on database functional from role foo",
                       error_msg.format("REVOKE <privilege> FROM ROLE")),
                      ("show grant role foo", error_msg.format("SHOW GRANT ROLE")),
                      ("show role grant group foo",
                       error_msg.format("SHOW ROLE GRANT GROUP"))]:
      result = self.execute_query_expect_failure(impala_client, statement[0], user=user)
      assert statement[1] in str(result)

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

  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_legacy_catalog_ownership(self):
      self._test_ownership()

  @CustomClusterTestSuite.with_args(impalad_args=LOCAL_CATALOG_IMPALAD_ARGS,
    catalogd_args=LOCAL_CATALOG_CATALOGD_ARGS)
  def test_local_catalog_ownership(self):
      # getTableIfCached() in LocalCatalog loads a minimal incomplete table
      # that does not include the ownership information. Hence show tables
      # *never* show owned tables. TODO(bharathv): Fix in a follow up commit
      pytest.xfail("getTableIfCached() faulty behavior, known issue")
      self._test_ownership()

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

  def _test_ownership(self):
    """Tests ownership privileges for databases and tables with ranger along with
    some known quirks in the implementation."""
    test_user = getuser()
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
      # test_user should not be authorized to run the queries anymore.
      result = self._run_query_as_user(
          "select * from {0}.foo".format(test_db), test_user, False)
      err = ("AuthorizationException: User '{0}' does not have privileges to execute" +
             " 'SELECT' on: {1}.foo").format(test_user, test_db)
      assert err in str(result)
    finally:
      self._run_query_as_user("drop database {0} cascade".format(test_db), ADMIN, True)

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
        TestRanger._remove_column_masking_policy(unique_name + str(i))

  @SkipIfABFS.hive
  @SkipIfADLS.hive
  @SkipIfIsilon.hive
  @SkipIfLocal.hive
  @SkipIfS3.hive
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
      TestRanger._remove_column_masking_policy("col_mask_for_hive")


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
          TestRanger._remove_column_masking_policy(policy_name)
