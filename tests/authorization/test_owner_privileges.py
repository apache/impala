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
# These tests run with using the user 'root' since we need a user on the system that
# has a group that we can grant role to. The tests uses two clients, one for the root
# user and one for 'test_user'. test_user is used as a user to transfer ownership to
# but does not need a group for role grants. Additionally, in these tests we run all
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
    # Using user 'root' for these tests since that user and the group 'root' should
    # exist on all systems running these tests which provides a group that can be
    # granted to a role.
    super(TestOwnerPrivileges, self).setup_method(method)
    self._test_cleanup()
    # Base roles for enabling tests.
    self.execute_query("create role owner_priv_test_ROOT")
    # Role for verifying grant.
    self.execute_query("create role owner_priv_test_all_role")
    # Role for verifying transfer to role.
    self.execute_query("create role owner_priv_test_owner_role")
    self.execute_query("grant role owner_priv_test_ROOT to group root")
    self.execute_query("grant role owner_priv_test_owner_role to group root")
    self.execute_query("grant create on server to owner_priv_test_ROOT")
    self.execute_query("grant select on database functional to owner_priv_test_ROOT")

  def teardown_method(self, method):
    self._test_cleanup()
    self.execute_query("drop role owner_priv_admin")
    super(TestOwnerPrivileges, self).teardown_method(method)

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
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_LONG_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO)
  def test_owner_privileges_with_grant_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests(TestObject(TestObject.DATABASE, "owner_priv_db",
        grant=True))
    self.__execute_owner_privilege_tests(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl", grant=True))
    self.__execute_owner_privilege_tests(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view", grant=True))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO)
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
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.test_user_impalad_client = self.create_impala_client()
    self.user_query(self.root_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="root")
    self.validate_privileges(self.root_impalad_client, "show grant user root", test_obj,
        sentry_refresh_timeout_s, "root")

    # Ensure grant works.
    self.user_query(self.root_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root")
    self.user_query(self.root_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root")

    # Change the database owner and ensure root does not have owner privileges.
    self.user_query(self.root_impalad_client, "alter %s %s set owner user test_user"
        % (test_obj.obj_type, test_obj.obj_name), user="root")
    result = self.user_query(self.root_impalad_client, "show grant user root",
        user="root", delay_s=sentry_refresh_timeout_s)
    assert len(result.data) == 0

    # Ensure root cannot drop database after owner change.
    self.user_query(self.root_impalad_client, "drop %s %s" % (test_obj.obj_type,
        test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute 'DROP'")

    # test_user should have privileges for object now.
    self.validate_privileges(self.test_user_impalad_client, "show grant user test_user",
        test_obj, sentry_refresh_timeout_s, "test_user")

    # Change the owner to a role and ensure test_user doesn't have privileges.
    # Set the owner back to root since for views, test_user doesn't have select
    # privileges on the underlying table.
    self.execute_query("alter %s %s set owner user root" % (test_obj.obj_type,
        test_obj.obj_name))
    result = self.user_query(self.test_user_impalad_client,
        "show grant user test_user", user="test_user", delay_s=sentry_refresh_timeout_s)
    assert len(result.data) == 0
    self.user_query(self.root_impalad_client,
        "alter %s %s set owner role owner_priv_test_owner_role"
        % (test_obj.obj_type, test_obj.obj_name), user="root")
    # Ensure root does not have user privileges.
    result = self.user_query(self.root_impalad_client, "show grant user root",
        user="root", delay_s=sentry_refresh_timeout_s)
    assert len(result.data) == 0

    # Ensure role has owner privileges.
    self.validate_privileges(self.root_impalad_client,
        "show grant role owner_priv_test_owner_role", test_obj, sentry_refresh_timeout_s,
        "root")

    # Drop the object and ensure no role privileges.
    self.user_query(self.root_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="root")
    result = self.user_query(self.root_impalad_client, "show grant role " +
        "owner_priv_test_owner_role", user="root", delay_s=sentry_refresh_timeout_s)
    assert len(result.data) == 0

    # Ensure user privileges are gone after drop.
    self.user_query(self.root_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="root")
    self.user_query(self.root_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="root")
    result = self.user_query(self.root_impalad_client, "show grant user root",
        user="root")
    assert len(result.data) == 0

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_NO_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_NO_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_LONG_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_NO_OO)
  def test_owner_privileges_disabled_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.DATABASE,
        "owner_priv_db"))
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"))
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_NO_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_NO_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_NO_OO)
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
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.user_query(self.root_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="root")
    # For user privileges, if the user has no privileges, the user will not exist
    # in the privileges list.
    self.user_query(self.root_impalad_client, "show grant user root",
        user="root", error_msg="User 'root' does not exist")

    # Ensure grant doesn't work.
    self.user_query(self.root_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.root_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    # Ensure changing the database owner doesn't work.
    self.user_query(self.root_impalad_client, "alter %s %s set owner user test_user"
        % (test_obj.obj_type, test_obj.obj_name), user="root",
        error_msg="does not have privileges with 'GRANT OPTION'")

    # Ensure root cannot drop database.
    self.user_query(self.root_impalad_client, "drop %s %s" % (test_obj.obj_type,
        test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute 'DROP'",
        delay_s=sentry_refresh_timeout_s)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_LONG_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT)
  def test_owner_privileges_without_grant_long_poll(self, vector, unique_database):
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.DATABASE,
        "owner_priv_db"))
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"))
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"))

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT)
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
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.user_query(self.root_impalad_client, "create %s if not exists %s %s %s"
        % (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
        test_obj.view_select), user="root")
    self.validate_privileges(self.root_impalad_client, "show grant user root", test_obj,
        sentry_refresh_timeout_s, "root")

    # Ensure grant doesn't work.
    self.user_query(self.root_impalad_client,
        "grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.root_impalad_client,
        "revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name), user="root",
        error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    self.user_query(self.root_impalad_client, "alter %s %s set owner user test_user"
        % (test_obj.obj_type, test_obj.obj_name), user="root",
        error_msg="does not have privileges with 'GRANT OPTION'")

    self.user_query(self.root_impalad_client, "drop %s %s " % (test_obj.obj_type,
        test_obj.obj_name), user="root")
    result = self.user_query(self.root_impalad_client, "show grant user root",
        user="root")
    assert len(result.data) == 0
