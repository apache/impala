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

import grp
import pytest
import os
from getpass import getuser

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.sentry_cache_test_suite import SentryCacheTestSuite

SENTRY_CONFIG_DIR = os.getenv('IMPALA_HOME') + '/fe/src/test/resources/'
SENTRY_CONFIG_FILE = SENTRY_CONFIG_DIR + 'sentry-site.xml'


class TestSentry(CustomClusterTestSuite):
  """This class contains Sentry specific authorization tests."""
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0}".format(SENTRY_CONFIG_FILE),
      catalogd_args="--sentry_config={0}".format(SENTRY_CONFIG_FILE))
  def test_sentry_admin(self, unique_role, unique_name):
    """
    Tests that only admin user can execute certain authorization statements.
    """
    admin = getuser()
    non_admin = unique_name
    admin_client = self.create_impala_client()
    non_admin_client = self.create_impala_client()
    try:
      self.execute_query_expect_success(admin_client,
                                        "create role {0}".format(unique_role),
                                        user=admin)

      # show current roles is always allowed.
      self.execute_query_expect_success(admin_client, "show current roles", user=admin)
      self.execute_query_expect_success(non_admin_client, "show current roles",
                                        user=non_admin)

      for statement in ["show roles",
                        "show grant role {0}".format(unique_role)]:
        self.execute_query_expect_success(admin_client, statement, user=admin)
        result = self.execute_query_expect_failure(non_admin_client, statement,
                                                   user=non_admin)
        assert "AuthorizationException: User '{0}' does not have privileges to access " \
               "the requested policy metadata.".format(non_admin) in str(result)
    finally:
      admin_client.execute("drop role {0}".format(unique_role))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1",
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
    sentry_config=SENTRY_CONFIG_FILE)
  def test_grant_revoke_invalid_role(self, unique_name):
    role_name = "foobar"
    group = grp.getgrnam(getuser()).gr_name
    try:
      # This will create "foobar" role catalog object.
      self.client.execute("create role {0}".format(role_name))
      self.client.execute("grant all on server to {0}".format(role_name))
      self.client.execute("grant role {0} to group `{1}`".format(role_name, group))
      self.client.execute("create database {0}".format(unique_name))

      ex = self.execute_query_expect_failure(
        self.client, "grant all on database {0} to role non_role".format(unique_name))
      assert "Role 'non_role' does not exist." in str(ex)

      ex = self.execute_query_expect_failure(
        self.client, "revoke all on database {0} from role non_role".format(unique_name))
      assert "Role 'non_role' does not exist." in str(ex)

      ex = self.execute_query_expect_failure(self.client, "show grant role non_role")
      assert "Role 'non_role' does not exist." in str(ex)

      ex = self.execute_query_expect_failure(
        self.client, "grant role non_role to group `{0}`".format(group))
      assert "Role 'non_role' does not exist." in str(ex)

      ex = self.execute_query_expect_failure(self.client, "drop role non_role")
      assert "Role 'non_role' does not exist." in str(ex)

      ex = self.execute_query_expect_failure(self.client,
                                             "create role {0}".format(role_name))
      assert "Role '{0}' already exists.".format(role_name) in str(ex)
    finally:
      self.client.execute("drop database {0}".format(unique_name))
      self.client.execute("drop role {0}".format(role_name))
