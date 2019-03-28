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
import time

from getpass import getuser
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

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
    self._test_grant_revoke(unique_name)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="{0} {1}".format(IMPALAD_ARGS, "--use_local_catalog=true"),
      catalogd_args="{0} {1}".format(CATALOGD_ARGS,
                                     "--use_local_catalog=true "
                                     "--catalog_topic_mode=minimal"))
  def test_grant_revoke_with_local_catalog(self, unique_name):
    """Tests grant/revoke with catalog v2 (local catalog)."""
    self._test_grant_revoke(unique_name)

  def _test_grant_revoke(self, unique_name):
    user = getuser()
    admin = "admin"
    admin_client = self.create_impala_client()
    user_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    group = grp.getgrnam(getuser()).gr_name
    test_data = [(user, "user"), (group, "group")]

    for data in test_data:
      ident = data[0]
      kw = data[1]

      try:
        # Set-up temp database/table
        admin_client.execute("drop database if exists {0} cascade"
                             .format(unique_database), user=admin)
        admin_client.execute("create database {0}".format(unique_database), user=admin)
        admin_client.execute("create table {0}.{1} (x int)"
                             .format(unique_database, unique_table), user=admin)

        self.execute_query_expect_success(admin_client,
                                          "grant select on database {0} to {1} {2}"
                                          .format(unique_database, kw, ident), user=admin)
        # TODO: IMPALA-8293 use refresh authorization
        time.sleep(10)
        self.execute_query_expect_success(user_client, "show tables in {0}"
                                          .format(unique_database), user=user)
        self.execute_query_expect_success(admin_client,
                                          "revoke select on database {0} from {1} "
                                          "{2}".format(unique_database, kw, ident),
                                          user=admin)
        # TODO: IMPALA-8293 use refresh authorization
        time.sleep(10)
        self.execute_query_expect_failure(user_client, "show tables in {0}"
                                          .format(unique_database))
      finally:
        admin_client.execute("revoke select on database {0} from {1} {2}"
                             .format(unique_database, kw, ident), user=admin)
        admin_client.execute("drop database if exists {0} cascade"
                             .format(unique_database), user=admin)

  @CustomClusterTestSuite.with_args(
      impalad_args=IMPALAD_ARGS, catalogd_args=CATALOGD_ARGS)
  def test_grant_option(self, unique_name):
    user1 = getuser()
    user2 = unique_name + "_user"
    admin = "admin"
    admin_client = self.create_impala_client()
    user1_client = self.create_impala_client()
    user2_client = self.create_impala_client()
    unique_database = unique_name + "_db"
    unique_table = unique_name + "_tbl"
    id = self._add_ranger_user(user2)

    try:
      # Set-up temp database/table
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=admin)
      admin_client.execute("create database {0}".format(unique_database), user=admin)
      admin_client.execute("create table {0}.{1} (x int)"
                           .format(unique_database, unique_table), user=admin)

      # Give user 1 the ability to grant select privileges on unique_database
      self.execute_query_expect_success(admin_client,
                                        "grant select on database {0} to user {1} with "
                                        "grant option".format(unique_database, user1),
                                        user=admin)
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(10)
      # User 1 grants select privilege to user 2
      self.execute_query_expect_success(user1_client,
                                        "grant select on database {0} to user {1}"
                                        .format(unique_database, user2), user=user1)
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(10)
      # User 2 exercises select privilege
      self.execute_query_expect_success(user2_client, "show tables in {0}"
                                        .format(unique_database), user=user2)
      # User 1 revokes select privilege from user 2
      self.execute_query_expect_success(user1_client,
                                        "revoke select on database {0} from user "
                                        "{1}".format(unique_database, user2), user=user1)
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(10)
      # User 2 can no longer select because the privilege was revoked
      self.execute_query_expect_failure(user2_client, "show tables in {0}"
                                        .format(unique_database))
      # Revoke privilege granting from user 1
      self.execute_query_expect_success(admin_client, "revoke grant option for select "
                                        "on database {0} from user {1}"
                                        .format(unique_database, user1), user=admin)
      # TODO: IMPALA-8293 use refresh authorization
      time.sleep(10)
      # User 1 can no longer grant privileges on unique_database
      self.execute_query_expect_failure(user1_client,
                                        "grant select on database {0} to user {1}"
                                        .format(unique_database, user2), user=user1)
    finally:
      admin_client.execute("revoke select on database {0} from user {1}"
                           .format(unique_database, user2), user=admin)
      admin_client.execute("revoke grant option for select on database {0} from user {1}"
                           .format(unique_database, user1), user=admin)
      admin_client.execute("drop database if exists {0} cascade".format(unique_database),
                           user=admin)
      self._remove_ranger_user(id)

  def _add_ranger_user(self, user):
    data = {"name": user, "password": "password123", "userRoleList": ["ROLE_USER"]}
    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    r = requests.post("{0}/service/xusers/secure/users".format(RANGER_HOST),
                      auth=RANGER_AUTH,
                      json=data, headers=headers)
    return json.loads(r.content)['id']

  def _remove_ranger_user(self, id):
    r = requests.delete("{0}/service/xusers/users/{1}?forceDelete=true"
                        .format(RANGER_HOST, id), auth=RANGER_AUTH)
    assert r.status_code < 300 and r.status_code >= 200
