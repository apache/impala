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

import grp
import json
import pytest
import requests

from getpass import getuser
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.hdfs_util import NAMENODE

ADMIN = "admin"
RANGER_AUTH = ("admin", "admin")
RANGER_HOST = "http://localhost:6080"
IMPALAD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
               "--ranger_app_id=impala --authorization_provider=ranger"
CATALOGD_ARGS = "--server-name=server1 --ranger_service_type=hive " \
                "--ranger_app_id=impala --authorization_provider=ranger"


class TestRanger(CustomClusterTestSuite):
  """
  Tests for Apache Ranger integration with Apache Impala.
  """

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
    catalogd_args="{0} {1}".format(CATALOGD_ARGS,
                                   "--use_local_catalog=true "
                                   "--catalog_topic_mode=minimal"))
  def test_grant_revoke_with_local_catalog(self, unique_name):
    """Tests grant/revoke with catalog v2 (local catalog)."""
    # Catalog v2 does not support global invalidate metadata.
    self._test_grant_revoke(unique_name, [None, "refresh authorization"])

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

      # Verify user 1 has with_grant privilege on unique_database
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      TestRanger._check_privileges(result, [
        ["USER", user1, unique_database, "", "", "", "*", "select", "true"],
        ["USER", user1, unique_database, "*", "*", "", "", "select", "true"]])

      # Revoke privilege granting from user 1
      self.execute_query_expect_success(admin_client, "revoke grant option for select "
                                                      "on database {0} from user {1}"
                                        .format(unique_database, user1), user=ADMIN)

      # User 1 can no longer grant privileges on unique_database
      result = self.execute_query("show grant user {0} on database {1}"
                                  .format(user1, unique_database))
      TestRanger._check_privileges(result, [])
    finally:
      admin_client.execute("revoke grant option for select on database {0} from user {1}"
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

      # Test ALL privilege hides other privileges
      self._test_show_grant_mask(admin_client, user)

      # Test USER inherits privileges for their GROUP
      self._test_show_grant_user_group(admin_client, user, group, unique_db)

      # Test that show grant without ON a resource fails

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

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post("{0}/service/plugins/services/grant/test_impala?pluginId=impala"
                      .format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=headers)
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

    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.post("{0}/service/plugins/services/revoke/test_impala?pluginId=impala"
                      .format(RANGER_HOST),
                      auth=RANGER_AUTH, json=data, headers=headers)
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
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    r = requests.get("{0}/service/plugins/policies"
                     .format(RANGER_HOST),
                     auth=RANGER_AUTH, headers=headers)
    return json.loads(r.content)["policies"]

  def _add_ranger_user(self, user):
    data = {"name": user, "password": "password123", "userRoleList": ["ROLE_USER"]}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    r = requests.post("{0}/service/xusers/secure/users".format(RANGER_HOST),
                      auth=RANGER_AUTH,
                      json=data, headers=headers)
    return json.loads(r.content)["id"]

  def _remove_ranger_user(self, id):
    r = requests.delete("{0}/service/xusers/users/{1}?forceDelete=true"
                        .format(RANGER_HOST, id), auth=RANGER_AUTH)
    assert 300 > r.status_code >= 200

  @staticmethod
  def _check_privileges(result, expected):
    def columns(row):
      cols = row.split("\t")
      return cols[0:len(cols) - 1]

    assert map(columns, result.data) == expected

  def _refresh_authorization(self, client, statement):
    if statement is not None:
      self.execute_query_expect_success(client, statement)

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
