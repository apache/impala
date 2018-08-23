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

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_uncompressed_text_dimension

# The polling frequency used by catalogd to refresh Sentry privileges.
# The long polling is so we can check updates just for the cache.
# The other polling is so we can get the updates without having to wait.
SENTRY_LONG_POLLING_FREQUENCY_S = 60
SENTRY_POLLING_FREQUENCY_S = 3
# The timeout, in seconds, when waiting for a refresh of Sentry privileges.
SENTRY_REFRESH_TIMEOUT_S = 9

SENTRY_CONFIG_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/'
SENTRY_BASE_LOG_DIR = getenv('IMPALA_CLUSTER_LOGS_DIR') + "/sentry"
SENTRY_CONFIG_FILE_OO = SENTRY_CONFIG_DIR + 'sentry-site_oo.xml'
SENTRY_CONFIG_FILE_OO_NOGRANT = SENTRY_CONFIG_DIR + 'sentry-site_oo_nogrant.xml'
SENTRY_CONFIG_FILE_NO_OO = SENTRY_CONFIG_DIR + 'sentry-site_no_oo.xml'


class TestObject():
  DATABASE = "database"
  TABLE = "table"
  VIEW = "view"

  def __init__(self, obj_type, obj_name, grant=False):
    self.obj_name = obj_name
    self.obj_type = obj_type
    parts = obj_name.split(".")
    self.db_name = parts[0]
    self.table_name = None
    self.table_def = ""
    self.view_select = ""
    if len(parts) > 1:
      self.table_name = parts[1]
    if obj_type == TestObject.VIEW:
      self.grant_name = TestObject.TABLE
      self.view_select = "as select * from functional.alltypes"
    elif obj_type == TestObject.TABLE:
      self.grant_name = TestObject.TABLE
      self.table_def = "(col1 int)"
    else:
      self.grant_name = obj_type
    self.grant = grant


class TestOwnerPrivileges(CustomClusterTestSuite):
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
  @CustomClusterTestSuite.with_args(
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
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO)
  def test_owner_privileges_with_grant(self, vector, unique_database):
    self.__execute_owner_privilege_tests(TestObject(TestObject.DATABASE, "owner_priv_db",
        grant=True), sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl", grant=True),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view", grant=True),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests(self, test_obj, sentry_refresh_timeout_sec=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.test_user_impalad_client = self.create_impala_client()
    self.__root_query("create %s if not exists %s %s %s" % (test_obj.obj_type,
        test_obj.obj_name, test_obj.table_def, test_obj.view_select))
    self.__validate_privileges(self.root_impalad_client, "root", "show grant user root",
        test_obj, sentry_refresh_timeout_sec)

    # Ensure grant works.
    self.__root_query("grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name))
    self.__root_query("revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name))

    # Change the database owner and ensure root does not have owner privileges.
    self.__root_query("alter %s %s set owner user test_user" % (test_obj.obj_type,
        test_obj.obj_name))
    result = self.__root_query("show grant user root")
    assert len(result.data) == 0

    # Ensure root cannot drop database after owner change.
    self.__root_query_fail("drop %s %s" % (test_obj.obj_type, test_obj.obj_name),
        "does not have privileges to execute 'DROP'")

    # test_user should have privileges for object now.
    self.__validate_privileges(self.test_user_impalad_client, "test_user",
        "show grant user test_user", test_obj, sentry_refresh_timeout_sec)

    # Change the owner to a role and ensure test_user doesn't have privileges.
    # Set the owner back to root since for views, test_user doesn't have select
    # privileges on the underlying table.
    self.execute_query("alter %s %s set owner user root" % (test_obj.obj_type,
        test_obj.obj_name))
    result = self.__test_user_query("show grant user test_user")
    assert len(result.data) == 0
    self.__root_query("alter %s %s set owner role owner_priv_test_owner_role"
        % (test_obj.obj_type, test_obj.obj_name))
    # Ensure root does not have user privileges.
    result = self.__root_query("show grant user root")
    assert len(result.data) == 0

    # Ensure role has owner privileges.
    self.__validate_privileges(self.root_impalad_client, "root",
        "show grant role owner_priv_test_owner_role", test_obj,
        sentry_refresh_timeout_sec)

    # Drop the object and ensure no role privileges.
    self.__root_query("drop %s %s " % (test_obj.obj_type, test_obj.obj_name))
    result = self.__root_query("show grant role owner_priv_test_owner_role")
    assert len(result.data) == 0

    # Ensure user privileges are gone after drop.
    self.__root_query("create %s if not exists %s %s %s" % (test_obj.obj_type,
        test_obj.obj_name, test_obj.table_def, test_obj.view_select))
    self.__root_query("drop %s %s " % (test_obj.obj_type, test_obj.obj_name))
    result = self.__root_query("show grant user root")
    assert len(result.data) == 0

  def __str_to_bool(self, val):
    if val.lower() == 'true':
      return True
    return False

  def __check_privileges(self, result, test_obj, null_create_date=True):
    # results should have the following columns
    # scope, database, table, column, uri, privilege, grant_option, create_time
    for row in result.data:
      col = row.split('\t')
      assert col[0] == test_obj.grant_name
      assert col[1] == test_obj.db_name
      if test_obj.table_name is not None and len(test_obj.table_name) > 0:
        assert col[2] == test_obj.table_name
      assert self.__str_to_bool(col[6]) == test_obj.grant
      if not null_create_date:
        assert str(col[7]) != 'NULL'

  def __root_query(self, query):
    return self.execute_query_expect_success(self.root_impalad_client, query, user="root")

  def __root_query_fail(self, query, error_msg):
    e = self.execute_query_expect_failure(self.root_impalad_client, query, user="root")
    self.__verify_exceptions(error_msg, str(e))

  def __validate_privileges(self, client, user, query, test_obj, timeout_sec):
    """Validate privileges. If timeout_sec is > 0 then retry until create_date is not null
    or the timeout_sec is reached.
    """
    if timeout_sec is None or timeout_sec <= 0:
      self.__check_privileges(self.execute_query_expect_success(client, query,
            user=user), test_obj)
    else:
      start_time = time()
      while time() - start_time < timeout_sec:
        result = self.execute_query_expect_success(client, query, user=user)
        try:
          self.__check_privileges(result, test_obj, null_create_date=False)
          return True
        except Exception:
          pass
        sleep(1)
      return False

  def __test_user_query(self, query):
    return self.execute_query_expect_success(self.test_user_impalad_client, query,
        user="test_user")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
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
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_NO_OO,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_NO_OO +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_NO_OO)
  def test_owner_privileges_disabled(self, vector, unique_database):
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.DATABASE,
        "owner_priv_db"), sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_no_oo(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests_no_oo(self, test_obj, sentry_refresh_timeout_sec=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.test_user_impalad_client = self.create_impala_client()
    self.__root_query("create %s if not exists %s %s %s" % (test_obj.obj_type,
        test_obj.obj_name, test_obj.table_def, test_obj.view_select))
    # For user privileges, if the user has no privileges, the user will not exist
    # in the privileges list.
    self.__root_query_fail("show grant user root", "User 'root' does not exist")

    # Ensure grant doesn't work.
    self.__root_query_fail("grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name),
        "does not have privileges to execute: GRANT_PRIVILEGE")

    self.__root_query_fail("revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name),
        "does not have privileges to execute: REVOKE_PRIVILEGE")

    # Ensure changing the database owner doesn't work.
    self.__root_query_fail("alter %s %s set owner user test_user"
        % (test_obj.obj_type, test_obj.obj_name),
        "does not have privileges with 'GRANT OPTION'")

    # Ensure root cannot drop database.
    self.__root_query_fail("drop %s %s" % (test_obj.obj_type, test_obj.obj_name),
        "does not have privileges to execute 'DROP'")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
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
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT,
    catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE_OO_NOGRANT +
    " --sentry_catalog_polling_frequency_s=" + str(SENTRY_POLLING_FREQUENCY_S),
    sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT)
  def test_owner_privileges_without_grant(self, vector, unique_database):
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.DATABASE,
        "owner_priv_db"), sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.TABLE,
        unique_database + ".owner_priv_tbl"),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)
    self.__execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.VIEW,
        unique_database + ".owner_priv_view"),
        sentry_refresh_timeout_sec=SENTRY_REFRESH_TIMEOUT_S)

  def __execute_owner_privilege_tests_oo_nogrant(self, test_obj,
      sentry_refresh_timeout_sec=0):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure root gets owner privileges.
    self.root_impalad_client = self.create_impala_client()
    self.test_user_impalad_client = self.create_impala_client()
    self.__root_query("create %s if not exists %s %s %s" % (test_obj.obj_type,
        test_obj.obj_name, test_obj.table_def, test_obj.view_select))
    self.__validate_privileges(self.root_impalad_client, "root", "show grant user root",
        test_obj, sentry_refresh_timeout_sec)

    # Ensure grant doesn't work.
    self.__root_query_fail("grant all on %s %s to role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name),
        "does not have privileges to execute: GRANT_PRIVILEGE")

    self.__root_query_fail("revoke all on %s %s from role owner_priv_test_all_role"
        % (test_obj.grant_name, test_obj.obj_name),
        "does not have privileges to execute: REVOKE_PRIVILEGE")

    self.__root_query_fail("alter %s %s set owner user test_user"
        % (test_obj.obj_type, test_obj.obj_name),
        "does not have privileges with 'GRANT OPTION'")

    self.__root_query("drop %s %s " % (test_obj.obj_type, test_obj.obj_name))
    result = self.__root_query("show grant user root")
    assert len(result.data) == 0

  def __verify_exceptions(self, expected_str, actual_str):
    """
    Verifies that 'expected_str' is a substring of the actual exception string
    'actual_str'.
    """
    actual_str = actual_str.replace('\n', '')
    # Strip newlines so we can split error message into multiple lines
    expected_str = expected_str.replace('\n', '')
    if expected_str in actual_str: return
    assert False, 'Unexpected exception string. Expected: %s\nNot found in actual: %s' % \
      (expected_str, actual_str)
