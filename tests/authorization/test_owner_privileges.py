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
# Client tests to ensure object ownership functionality
#
# The tests uses two clients, one for oo_user1 and one for 'oo_user2'. oo_user2 is
# used as a user to transfer ownership to. Additionally, in these tests we run all
# the tests twice. Once with just using cache, hence the long sentry poll, and once
# by ensuring refreshes happen from Sentry.

import grp
import pytest
from getpass import getuser
from os import getenv
from time import sleep, time

from tests.common.sentry_cache_test_suite import SentryCacheTestSuite, TestObject
from tests.common.test_dimensions import create_uncompressed_text_dimension

# The polling frequency used by catalogd to refresh Sentry privileges.
# The long polling is so we can check updates just for the cache.
# The other polling is so we can get the updates without having to wait.
SENTRY_LONG_POLLING_FREQUENCY_S = 60
SENTRY_POLLING_FREQUENCY_S = 1
# The timeout, in seconds, when waiting for a refresh of Sentry privileges.
SENTRY_REFRESH_TIMEOUT_S = SENTRY_POLLING_FREQUENCY_S * 2

SENTRY_CONFIG_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/'
SENTRY_BASE_LOG_DIR = getenv('IMPALA_CLUSTER_LOGS_DIR') + "/sentry"
SENTRY_CONFIG_FILE_OO = SENTRY_CONFIG_DIR + 'sentry-site_oo.xml'
SENTRY_CONFIG_FILE_OO_NOGRANT = SENTRY_CONFIG_DIR + 'sentry-site_oo_nogrant.xml'
SENTRY_CONFIG_FILE_NO_OO = SENTRY_CONFIG_DIR + 'sentry-site_no_oo.xml'


class TestOwnerPrivileges(SentryCacheTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestOwnerPrivileges, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  def teardown_class(self):
    super(self)

  def setup_method(self, method):
    super(TestOwnerPrivileges, self).setup_method(method)
    self._test_cleanup()
    # Base roles for enabling tests.
    self.execute_query("create role owner_priv_test_oo_user1")
    # Role for verifying grant.
    self.execute_query("create role owner_priv_test_all_role")
    # Role for verifying transfer to role.
    self.execute_query("create role owner_priv_test_owner_role")
    self.execute_query("grant role owner_priv_test_oo_user1 to group oo_group1")
    self.execute_query("grant role owner_priv_test_owner_role to group oo_group1")
    self.execute_query("grant create on server to owner_priv_test_oo_user1")
    self.execute_query("grant select on database functional to owner_priv_test_oo_user1")

  def teardown_method(self, method):
    self._test_cleanup()
    self.execute_query("drop role owner_priv_admin")
    super(TestOwnerPrivileges, self).teardown_method(method)

  @staticmethod
  def count_user_privileges(result):
    """
    This method returns a new list of privileges that only contain user privileges.
    """
    # results should have the following columns
    # principal_name, principal_type, scope, database, table, column, uri, privilege,
    # grant_option, create_time
    total = 0
    for row in result.data:
      col = row.split('\t')
      if col[0] == 'USER':
        total += 1
    return total

  def _validate_user_privilege_count(self, client, query, user, delay_s, count):
    result = self.user_query(client, query, user=user, delay_s=delay_s)
    return self.count_user_privileges(result) == count

  def _test_cleanup(self):
    # Admin for manipulation and cleaning up.
    try:
      self.execute_query("drop role owner_priv_admin")
    except Exception:
      # Ignore in case it wasn't created yet.
      pass
    self.execute_query("create role owner_priv_admin")
    self.execute_query("grant all on server to owner_priv_admin with grant option")
    group_name = grp.getgrnam(getuser()).gr_name
    self.execute_query("grant role owner_priv_admin to group `%s`" % group_name)
    # Clean up the test artifacts.
    try:
      self.cleanup_db("owner_priv_db", sync_ddl=0)
    except Exception:
      # Ignore this if we can't show tables.
      pass

    # Clean up any old roles created by this test
    for role_name in self.execute_query("show roles").data:
      if "owner_priv_test" in role_name:
        self.execute_query("drop role %s" % role_name)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider "
                   .format(SENTRY_CONFIG_FILE_OO),
      catalogd_args="--sentry_config={0} --authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    " --sentry_catalog_polling_frequency_s={1}"
                    .format(SENTRY_CONFIG_FILE_OO, str(SENTRY_LONG_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_OO,
      sentry_log_dir="{0}/test_owner_privileges_with_grant_log_poll"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_with_grant_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests(TestObject(TestObject.DATABASE, "owner_priv_db",
        grant=True))
    self.__execute_owner_privilege_tests(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl", grant=True))
    self.__execute_owner_privilege_tests(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view", grant=True))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_OO, str(SENTRY_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_OO,
      sentry_log_dir="{0}/test_owner_privileges_with_grant"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_with_grant(self, vector, unique_database):
    self.__execute_owner_privilege_tests(TestObject(TestObject.DATABASE, "owner_priv_db",
        grant=True), sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl", grant=True),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view", grant=True),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests(self, test_obj, sentry_refresh_timeout_s=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure oo_user1 gets owner privileges.
    self.oo_user1_impalad_client = self.create_impala_client()
    self.oo_user2_impalad_client = self.create_impala_client()
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="oo_user1")
    self.validate_privileges(self.oo_user1_impalad_client, "show grant user oo_user1",
        test_obj, sentry_refresh_timeout_s, "oo_user1")

    # Ensure grant works.
    self.user_query(self.oo_user1_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1")
    self.user_query(self.oo_user1_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1")

    # Change the database owner and ensure oo_user1 does not have owner privileges.
    # Use a delay to avoid cache consistency issue that could occur after create.
    self.user_query(self.oo_user1_impalad_client, "alter %s %s set owner user oo_user2"
        % (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
        delay_s=sentry_refresh_timeout_s)
    assert self._validate_user_privilege_count(self.oo_user1_impalad_client,
        "show grant user oo_user1", "oo_user1", sentry_refresh_timeout_s, 0)

    # Ensure oo_user1 cannot drop database after owner change.
    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s" % (test_obj.obj_type,
        test_obj.obj_name), user="oo_user1", delay_s=sentry_refresh_timeout_s,
        error_msg="does not have privileges to execute 'DROP'")

    # oo_user2 should have privileges for object now.
    self.validate_privileges(self.oo_user2_impalad_client, "show grant user oo_user2",
        test_obj, sentry_refresh_timeout_s, "oo_user2")

    # Change the owner to a role and ensure oo_user2 doesn't have privileges.
    # Set the owner back to oo_user1 since for views, oo_user2 doesn't have select
    # privileges on the underlying table.
    self.execute_query("alter %s %s set owner user oo_user1" % (test_obj.obj_type,
        test_obj.obj_name))
    assert self._validate_user_privilege_count(self.oo_user2_impalad_client,
        "show grant user oo_user2", "oo_user2", sentry_refresh_timeout_s, 0)
    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client,
        "alter %s %s set owner role owner_priv_test_owner_role"
        % (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
        delay_s=sentry_refresh_timeout_s)
    # Ensure oo_user1 does not have user privileges.
    assert self._validate_user_privilege_count(self.oo_user1_impalad_client,
        "show grant user oo_user1", "oo_user1", sentry_refresh_timeout_s, 0)

    # Ensure role has owner privileges.
    self.validate_privileges(self.oo_user1_impalad_client,
        "show grant role owner_priv_test_owner_role", test_obj, sentry_refresh_timeout_s,
        "oo_user1")

    # Drop the object and ensure no role privileges.
    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="oo_user1", delay_s=sentry_refresh_timeout_s)
    assert self._validate_user_privilege_count(self.oo_user1_impalad_client,
        "show grant user oo_user1", "oo_user1", sentry_refresh_timeout_s, 0)

    # Ensure user privileges are gone after drop.
    # Use a delay to avoid cache consistency issue that could occur after drop.
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="oo_user1", delay_s=sentry_refresh_timeout_s)
    # Use a delay to avoid cache consistency issue that could occur after create.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="oo_user1", delay_s=sentry_refresh_timeout_s)
    assert self._validate_user_privilege_count(self.oo_user1_impalad_client,
        "show grant user oo_user1", "oo_user1", sentry_refresh_timeout_s, 0)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider "
                   .format(SENTRY_CONFIG_FILE_NO_OO),
      catalogd_args="--sentry_config={0} --authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    " --sentry_catalog_polling_frequency_s={1}"
                    .format(SENTRY_CONFIG_FILE_NO_OO,
                    str(SENTRY_LONG_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_NO_OO,
      sentry_log_dir="{0}/test_owner_privileges_disabled_log_poll"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_disabled_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.DATABASE,
        "owner_priv_db"))
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"))
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_NO_OO),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_NO_OO, str(SENTRY_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_NO_OO,
      sentry_log_dir="{0}/test_owner_privileges_disabled"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_disabled(self, vector, unique_database):
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.DATABASE,
        "owner_priv_db"), sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests_no_oo(self, test_obj, sentry_refresh_timeout_s=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure oo_user1 gets owner privileges.
    self.oo_user1_impalad_client = self.create_impala_client()
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="oo_user1")

    # Ensure grant doesn't work.
    self.user_query(self.oo_user1_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.oo_user1_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    # Ensure changing the database owner doesn't work.
    self.user_query(self.oo_user1_impalad_client, "alter %s %s set owner user oo_user2"
        % (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges with 'GRANT OPTION'")

    # Ensure oo_user1 cannot drop database.
    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s" % (test_obj.obj_type,
        test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges to execute 'DROP'",
        delay_s=sentry_refresh_timeout_s)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO_NOGRANT),
      catalogd_args="--sentry_config={0} --authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    " --sentry_catalog_polling_frequency_s={1}"
                    .format(SENTRY_CONFIG_FILE_OO_NOGRANT,
                    str(SENTRY_LONG_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT,
      sentry_log_dir="{0}/test_owner_privileges_without_grant_log_poll"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_without_grant_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.DATABASE,
        "owner_priv_db"))
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"))
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO_NOGRANT),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.service.CustomClusterResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_OO_NOGRANT,
                    str(SENTRY_POLLING_FREQUENCY_S)),
      sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT,
      sentry_log_dir="{0}/test_owner_privileges_without_grant"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_without_grant(self, vector, unique_database):
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.DATABASE,
        "owner_priv_db"), sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"),
        sentry_refresh_timeout_s=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests_oo_nogrant(self, test_obj,
      sentry_refresh_timeout_s=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure oo_user1 gets owner privileges.
    self.oo_user1_impalad_client = self.create_impala_client()
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="oo_user1")
    self.validate_privileges(self.oo_user1_impalad_client, "show grant user oo_user1",
        test_obj, sentry_refresh_timeout_s, "oo_user1")

    # Ensure grant doesn't work.
    self.user_query(self.oo_user1_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.oo_user1_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
        error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    # Use a delay to avoid cache consistency issue that could occur after create.
    self.user_query(self.oo_user1_impalad_client, "alter %s %s set owner user oo_user2"
        % (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
        delay_s=sentry_refresh_timeout_s,
        error_msg="does not have privileges with 'GRANT OPTION'")

    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="oo_user1", delay_s=sentry_refresh_timeout_s)
    assert self._validate_user_privilege_count(self.oo_user1_impalad_client,
        "show grant user oo_user1", "oo_user1", sentry_refresh_timeout_s, 0)
