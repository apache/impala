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
# Client tests to ensure object ownership functionality.

import grp
import pytest
from getpass import getuser
from os import getenv

from tests.common.sentry_cache_test_suite import SentryCacheTestSuite, TestObject
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.skip import SkipIfHive3
from tests.util.filesystem_utils import FILESYSTEM_PREFIX

# Sentry long polling frequency to make Sentry refresh not run.
SENTRY_LONG_POLLING_FREQUENCY_S = 3600

SENTRY_CONFIG_DIR = getenv('IMPALA_HOME') + '/fe/src/test/resources/'
SENTRY_BASE_LOG_DIR = getenv('IMPALA_CLUSTER_LOGS_DIR') + "/sentry"
SENTRY_CONFIG_FILE_OO = SENTRY_CONFIG_DIR + 'sentry-site_oo.xml'
SENTRY_CONFIG_FILE_OO_NOGRANT = SENTRY_CONFIG_DIR + 'sentry-site_oo_nogrant.xml'
SENTRY_CONFIG_FILE_NO_OO = SENTRY_CONFIG_DIR + 'sentry-site_no_oo.xml'


@SkipIfHive3.sentry_not_supported
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
    self._setup_admin()

  def teardown_method(self, method):
    self._cleanup_admin()
    super(TestOwnerPrivileges, self).teardown_method(method)

  def _setup_ownership_test(self):
    self._cleanup_ownership_test()
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

  def _cleanup_ownership_test(self):
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

  def _validate_no_user_privileges(self, client, user, refresh_authorization):
    if refresh_authorization: self.execute_query("refresh authorization")
    result = self.user_query(client, "show grant user %s" % user, user=user)
    return TestOwnerPrivileges.count_user_privileges(result) == 0

  def _setup_admin(self):
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

  def _cleanup_admin(self):
    self.execute_query("drop role owner_priv_admin")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_OO, SENTRY_LONG_POLLING_FREQUENCY_S),
      sentry_config=SENTRY_CONFIG_FILE_OO,
      sentry_log_dir="{0}/test_drop_table_if_exists".format(SENTRY_BASE_LOG_DIR))
  def test_drop_if_exists(self, vector, unique_database):
    try:
      # we need to use a separate db for testing the drop database if exists case
      # For others we can rely on using unique_database which gets cleaned up
      # automatically
      test_db = "test_drop_if_exists_db"
      self._setup_drop_if_exist_test(unique_database, test_db)
      self._execute_drop_if_exists(TestObject(TestObject.DATABASE, test_db))
      self._execute_drop_if_exists(TestObject(TestObject.TABLE, unique_database +
          ".test_table"))
      self._execute_drop_if_exists(TestObject(TestObject.VIEW, unique_database +
          ".test_view"))
      self._execute_drop_if_exists(TestObject(TestObject.FUNCTION, unique_database +
          ".test_func()"))
    finally:
      self._cleanup_drop_if_exist_test(test_db)

  def _setup_drop_if_exist_test(self, unique_database, test_db):
    # Cleanup test_db, as the previous test run may have not been able to clean it up
    self.execute_query("drop database if exists %s" % test_db)

    # create a role which can create objects on this server
    # for the purposes of this test oo_user1 will have privileges
    # to create (and drop) objects while oo_user_2 will not have drop privileges
    # oo_user_3 will have privileges to select on the server but not to drop
    for role_name in self.execute_query("show roles").data:
      if role_name in ['oo_user1_role', 'oo_user2_role', 'oo_user3_role']:
        self.execute_query("drop role {0}".format(role_name))

    self.execute_query("create role oo_user1_role")
    self.execute_query("create role oo_user2_role")
    self.execute_query("create role oo_user3_role")
    # grant create permissions to oo_user_1 so that they can create database/table
    # or functions
    self.execute_query("grant create on server to oo_user1_role")
    # grant select permissions to oo_user_3 so that they can use database/table
    # or functions, but cannot drop them
    self.execute_query("grant select on server to oo_user3_role")
    # oo_user1 needs permissions to create a view based of functional.alltypes
    self.execute_query("grant select on database functional to oo_user1_role")
    # oo_user1 needs permissions to create a function based of libTestUdfs.so
    self.execute_query("grant all on uri '{0}/test-warehouse/libTestUdfs.so' to"
        " oo_user1_role".format(FILESYSTEM_PREFIX))
    # We need to provide explicit privileges to drop functions
    self.execute_query("grant drop on database {0} to "
        "oo_user1_role".format(unique_database))
    self.execute_query("grant role oo_user1_role to group oo_group1")
    self.execute_query("grant role oo_user2_role to group oo_group2")
    self.execute_query("grant role oo_user3_role to group oo_group3")
    self.execute_query("refresh authorization")

  def _cleanup_drop_if_exist_test(self, test_db):
    self.execute_query("revoke role oo_user1_role from group oo_group1")
    self.execute_query("revoke role oo_user2_role from group oo_group2")
    self.execute_query("revoke role oo_user3_role from group oo_group3")
    self.execute_query("drop role oo_user1_role")
    self.execute_query("drop role oo_user2_role")
    self.execute_query("drop role oo_user3_role")
    self.execute_query("refresh authorization")
    self.execute_query("drop database if exists {0}".format(test_db))

  def _execute_drop_if_exists(self, test_obj):
    self._execute_drop_if_exists_inner(test_obj, True)
    if test_obj.obj_type != TestObject.DATABASE:
      self._execute_drop_if_exists_inner(test_obj, False)

  def _execute_drop_if_exists_inner(self, test_obj, use_qualified_name):
    """
    Executes a drop table if exists on a given object type and makes sure that
    there is no authorization exception when the user has enough privileges. If the user
    does not have correct privileges the test confirms that error is thrown
    """
    self.oo_user1_impalad_client = self.create_impala_client()
    self.oo_user2_impalad_client = self.create_impala_client()
    self.oo_user3_impalad_client = self.create_impala_client()

    fq_obj_name = test_obj.obj_name
    obj_name = fq_obj_name if use_qualified_name else test_obj.table_name
    if not use_qualified_name:
       # Call "use db" and omit "db." in the queries of oo_user1 and oo_user3
       # oo_user2 has no privileges for the database so it will always use qualified name
       self.user_query(self.oo_user1_impalad_client, "use %s" % test_obj.db_name,
                       user="oo_user1")
       self.user_query(self.oo_user3_impalad_client, "use %s" % test_obj.db_name,
                       user="oo_user3")

    self.user_query(self.oo_user1_impalad_client, "create %s %s %s %s %s" %
                    (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
                     test_obj.view_select, test_obj.func_def), user="oo_user1")

    access_error_msg = \
        "does not have privileges to ANY" if test_obj.obj_type == TestObject.FUNCTION \
        else "does not have privileges to access"
    drop_error_msg = \
        "does not have privileges to DROP" if test_obj.obj_type == TestObject.FUNCTION \
        else "does not have privileges to execute 'DROP'"

    # Try to DROP IF EXISTS an existing object without DROP privileges
    self.user_query(self.oo_user2_impalad_client, "drop %s if exists %s" %
                    (test_obj.obj_type, fq_obj_name), user="oo_user2,",
                    error_msg=access_error_msg)
    self.user_query(self.oo_user3_impalad_client, "drop %s if exists %s" %
                    (test_obj.obj_type, obj_name), user="oo_user3",
                    error_msg=drop_error_msg)

    # Try to DROP (without IF EXISTS) an existing object
    self.user_query(self.oo_user2_impalad_client, "drop %s %s" %
                    (test_obj.obj_type, fq_obj_name), user="oo_user2",
                    error_msg=drop_error_msg)
    self.user_query(self.oo_user3_impalad_client, "drop %s %s" %
                    (test_obj.obj_type, obj_name), user="oo_user3",
                    error_msg=drop_error_msg)
    # oo_user1 has the privileges to drop the object, so the next query drops it
    self.user_query(self.oo_user1_impalad_client, "drop %s %s" %
                    (test_obj.obj_type, obj_name), user="oo_user1")

    # a drop if exists on a non-existing object should not error out if the user has
    # minimum set of privileges required to list those object types
    self.user_query(self.oo_user1_impalad_client, "drop %s if exists %s" %
                    (test_obj.obj_type, obj_name), user="oo_user1")
    self.user_query(self.oo_user3_impalad_client, "drop %s if exists %s" %
                    (test_obj.obj_type, obj_name), user="oo_user3")
    # oo_user2 does not have privileges on this object and hence should receive a
    # authorization error.
    error_msg = \
        "does not have privileges to ANY" if test_obj.obj_type == TestObject.FUNCTION \
        else "does not have privileges to access"
    self.user_query(self.oo_user2_impalad_client, "drop %s if exists %s" %
                    (test_obj.obj_type, fq_obj_name), user="oo_user2",
                    error_msg=error_msg)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_OO, SENTRY_LONG_POLLING_FREQUENCY_S),
      sentry_config=SENTRY_CONFIG_FILE_OO,
      sentry_log_dir="{0}/test_owner_privileges_with_grant".format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_with_grant(self, vector, unique_database):
    """Tests owner privileges with grant on database, table, and view.
    - refresh_authorization=True: With Sentry refresh to make sure privileges are really
                                  stored in Sentry.
    - refresh_authorization=False: No Sentry refresh to make sure user can use owner
                                   privileges right away without a Sentry refresh."""
    for refresh in [True, False]:
      try:
        self._setup_ownership_test()
        self._execute_owner_privilege_tests(TestObject(TestObject.DATABASE,
                                                       "owner_priv_db",
                                                       grant=True),
                                            refresh_authorization=refresh)
        self._execute_owner_privilege_tests(TestObject(TestObject.TABLE,
                                                       unique_database +
                                                       ".owner_priv_tbl",
                                                       grant=True),
                                            refresh_authorization=refresh)
        self._execute_owner_privilege_tests(TestObject(TestObject.VIEW,
                                                       unique_database +
                                                       ".owner_priv_view",
                                                       grant=True),
                                            refresh_authorization=refresh)
      finally:
        self._cleanup_ownership_test()

  def _execute_owner_privilege_tests(self, test_obj, refresh_authorization):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure oo_user1 gets owner privileges.
    self.oo_user1_impalad_client = self.create_impala_client()
    # oo_user2 is only used for transferring ownership.
    self.oo_user2_impalad_client = self.create_impala_client()
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s" %
                    (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
                     test_obj.view_select), user="oo_user1")
    self.validate_privileges(self.oo_user1_impalad_client, "show grant user oo_user1",
                             test_obj, user="oo_user1",
                             refresh_authorization=refresh_authorization)

    # Ensure grant works.
    self.user_query(self.oo_user1_impalad_client,
                    "grant all on %s %s to role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1")
    self.user_query(self.oo_user1_impalad_client,
                    "revoke all on %s %s from role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1")

    # Change the database owner and ensure oo_user1 does not have owner privileges.
    self.user_query(self.oo_user1_impalad_client, "alter %s %s set owner user oo_user2" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1")
    assert self._validate_no_user_privileges(self.oo_user1_impalad_client,
                                             user="oo_user1",
                                             refresh_authorization=refresh_authorization)

    # Ensure oo_user1 cannot drop database after owner change.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute 'DROP'")

    # oo_user2 should have privileges for object now.
    self.validate_privileges(self.oo_user2_impalad_client, "show grant user oo_user2",
                             test_obj, user="oo_user2",
                             refresh_authorization=refresh_authorization)

    # Change the owner to a role and ensure oo_user2 doesn't have privileges.
    # Set the owner back to oo_user1 since for views, oo_user2 doesn't have select
    # privileges on the underlying table.
    self.execute_query("alter %s %s set owner user oo_user1" %
                       (test_obj.obj_type, test_obj.obj_name),
                       query_options={"sync_ddl": 1})
    assert self._validate_no_user_privileges(self.oo_user2_impalad_client,
                                             user="oo_user2",
                                             refresh_authorization=refresh_authorization)
    self.user_query(self.oo_user1_impalad_client,
                    "alter %s %s set owner role owner_priv_test_owner_role" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1")
    # Ensure oo_user1 does not have user privileges.
    assert self._validate_no_user_privileges(self.oo_user1_impalad_client,
                                             user="oo_user1",
                                             refresh_authorization=refresh_authorization)

    # Ensure role has owner privileges.
    self.validate_privileges(self.oo_user1_impalad_client,
                             "show grant role owner_priv_test_owner_role", test_obj,
                             user="oo_user1", refresh_authorization=refresh_authorization)

    # Drop the object and ensure no role privileges.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1")
    assert self._validate_no_user_privileges(self.oo_user1_impalad_client,
                                             user="oo_user1",
                                             refresh_authorization=refresh_authorization)

    # Ensure user privileges are gone after drop.
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s" %
                    (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
                     test_obj.view_select), user="oo_user1")
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1")
    assert self._validate_no_user_privileges(self.oo_user1_impalad_client,
                                             user="oo_user1",
                                             refresh_authorization=refresh_authorization)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_NO_OO),
      catalogd_args="--sentry_config={0} --authorization_policy_provider_class="
                    "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_NO_OO),
      sentry_config=SENTRY_CONFIG_FILE_NO_OO,
      sentry_log_dir="{0}/test_owner_privileges_disabled".format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_disabled(self, vector, unique_database):
    """Tests that there should not be owner privileges."""
    try:
      self._setup_ownership_test()
      self._execute_owner_privilege_tests_no_oo(TestObject(TestObject.DATABASE,
                                                           "owner_priv_db"))
      self._execute_owner_privilege_tests_no_oo(TestObject(TestObject.TABLE,
                                                           unique_database +
                                                           ".owner_priv_tbl"))
      self._execute_owner_privilege_tests_no_oo(TestObject(TestObject.VIEW,
                                                           unique_database +
                                                           ".owner_priv_view"))
    finally:
      self._cleanup_ownership_test()

  def _execute_owner_privilege_tests_no_oo(self, test_obj):
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
                    "grant all on %s %s to role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.oo_user1_impalad_client,
                    "revoke all on %s %s from role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    # Ensure changing the database owner doesn't work.
    self.user_query(self.oo_user1_impalad_client,
                    "alter %s %s set owner user oo_user2" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges with 'GRANT OPTION'")

    # Ensure oo_user1 cannot drop database.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute 'DROP'")

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config={0} "
                   "--authorization_policy_provider_class="
                   "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                   .format(SENTRY_CONFIG_FILE_OO_NOGRANT),
      catalogd_args="--sentry_config={0} --sentry_catalog_polling_frequency_s={1} "
                    "--authorization_policy_provider_class="
                    "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                    .format(SENTRY_CONFIG_FILE_OO_NOGRANT,
                            SENTRY_LONG_POLLING_FREQUENCY_S),
      sentry_config=SENTRY_CONFIG_FILE_OO_NOGRANT,
      sentry_log_dir="{0}/test_owner_privileges_without_grant"
                     .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_without_grant(self, vector, unique_database):
    """Tests owner privileges without grant on database, table, and view.
    - refresh_authorization=True: With Sentry refresh to make sure privileges are really
                                  stored in Sentry.
    - refresh_authorization=False: No Sentry refresh to make sure user can use owner
                                   privileges right away without a Sentry refresh."""
    for refresh in [True, False]:
      try:
        self._setup_ownership_test()
        self._execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.DATABASE,
                                                                  "owner_priv_db"),
                                                       refresh_authorization=refresh)
        self._execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.TABLE,
                                                                  unique_database +
                                                                  ".owner_priv_tbl"),
                                                       refresh_authorization=refresh)
        self._execute_owner_privilege_tests_oo_nogrant(TestObject(TestObject.VIEW,
                                                                  unique_database +
                                                                  ".owner_priv_view"),
                                                       refresh_authorization=refresh)
      finally:
        self._cleanup_ownership_test()

  def _execute_owner_privilege_tests_oo_nogrant(self, test_obj, refresh_authorization):
    """
    Executes all the statements required to validate owner privileges work correctly
    for a specific database, table, or view.
    """
    # Create object and ensure oo_user1 gets owner privileges.
    self.oo_user1_impalad_client = self.create_impala_client()
    self.user_query(self.oo_user1_impalad_client, "create %s if not exists %s %s %s" %
                    (test_obj.obj_type, test_obj.obj_name, test_obj.table_def,
                     test_obj.view_select), user="oo_user1")
    self.validate_privileges(self.oo_user1_impalad_client, "show grant user oo_user1",
                             test_obj, user="oo_user1",
                             refresh_authorization=refresh_authorization)

    # Ensure grant doesn't work.
    self.user_query(self.oo_user1_impalad_client,
                    "grant all on %s %s to role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute: GRANT_PRIVILEGE")

    self.user_query(self.oo_user1_impalad_client,
                    "revoke all on %s %s from role owner_priv_test_all_role" %
                    (test_obj.grant_name, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges to execute: REVOKE_PRIVILEGE")

    self.user_query(self.oo_user1_impalad_client, "alter %s %s set owner user oo_user2" %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1",
                    error_msg="does not have privileges with 'GRANT OPTION'")

    # Use a delay to avoid cache consistency issue that could occur after alter.
    self.user_query(self.oo_user1_impalad_client, "drop %s %s " %
                    (test_obj.obj_type, test_obj.obj_name), user="oo_user1")
    assert self._validate_no_user_privileges(self.oo_user1_impalad_client,
                                             user="oo_user1",
                                             refresh_authorization=refresh_authorization)

  @pytest.mark.execute_serially
  @SentryCacheTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config={0} "
                 "--authorization_policy_provider_class="
                 "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                 .format(SENTRY_CONFIG_FILE_OO),
    catalogd_args="--sentry_config={0} "
                  "--authorization_policy_provider_class="
                  "org.apache.impala.testutil.TestSentryResourceAuthorizationProvider"
                  .format(SENTRY_CONFIG_FILE_OO),
    sentry_config=SENTRY_CONFIG_FILE_OO,
    sentry_log_dir="{0}/test_owner_privileges_different_cases"
                   .format(SENTRY_BASE_LOG_DIR))
  def test_owner_privileges_different_cases(self, vector, unique_database):
    """IMPALA-7742: Tests that only user names that differ only in case are not
    authorized to access the database/table/view unless the user is the owner."""
    # Use two different clients so that the sessions will use two different user names.
    foobar_impalad_client = self.create_impala_client()
    FOOBAR_impalad_client = self.create_impala_client()
    role_name = "owner_priv_diff_cases_role"
    try:
      self.execute_query("create role %s" % role_name)
      self.execute_query("grant role %s to group foobar" % role_name)
      self.execute_query("grant all on server to role %s" % role_name)

      self.user_query(foobar_impalad_client, "create database %s_db" %
                      unique_database, user="foobar")
      # FOOBAR user should not be allowed to create a table in the foobar's database.
      self.user_query(FOOBAR_impalad_client, "create table %s_db.test_tbl(i int)" %
                      unique_database, user="FOOBAR",
                      error_msg="User 'FOOBAR' does not have privileges to execute "
                                "'CREATE' on: %s_db" % unique_database)

      self.user_query(foobar_impalad_client, "create table %s.owner_case_tbl(i int)" %
                      unique_database, user="foobar")
      # FOOBAR user should not be allowed to select foobar's table.
      self.user_query(FOOBAR_impalad_client, "select * from %s.owner_case_tbl" %
                      unique_database, user="FOOBAR",
                      error_msg="User 'FOOBAR' does not have privileges to execute "
                                "'SELECT' on: %s.owner_case_tbl" % unique_database)

      self.user_query(foobar_impalad_client,
                      "create view %s.owner_case_view as select 1" % unique_database,
                      user="foobar")
      # FOOBAR user should not be allowed to select foobar's view.
      self.user_query(FOOBAR_impalad_client, "select * from %s.owner_case_view" %
                      unique_database, user="FOOBAR",
                      error_msg="User 'FOOBAR' does not have privileges to execute "
                                "'SELECT' on: %s.owner_case_view" % unique_database)

      # FOOBAR user should not be allowed to see foobar's privileges.
      self.user_query(FOOBAR_impalad_client, "show grant user foobar", user="FOOBAR",
                      error_msg="User 'FOOBAR' does not have privileges to access the "
                                "requested policy metadata")
    finally:
      self.user_query(foobar_impalad_client, "drop database %s_db cascade" %
                      unique_database, user="foobar")
      self.execute_query("drop role %s" % role_name)
