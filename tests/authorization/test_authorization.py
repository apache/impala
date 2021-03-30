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
import pytest
import tempfile
import grp
import re
import random
import sys
import subprocess
import urllib

from getpass import getuser
from ImpalaService import ImpalaHiveServer2Service
from TCLIService import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import assert_file_in_dir_contains,\
    assert_no_files_in_dir_contain
from tests.common.skip import SkipIf

SENTRY_CONFIG_DIR = os.getenv('IMPALA_HOME') + '/fe/src/test/resources/'
SENTRY_BASE_LOG_DIR = os.getenv('IMPALA_CLUSTER_LOGS_DIR') + "/sentry"
SENTRY_CONFIG_FILE = SENTRY_CONFIG_DIR + 'sentry-site.xml'
SENTRY_CONFIG_FILE_OO = SENTRY_CONFIG_DIR + 'sentry-site_oo.xml'
PRIVILEGES = ['all', 'alter', 'drop', 'insert', 'refresh', 'select']
ADMIN = "admin"

class TestAuthorization(CustomClusterTestSuite):
  def setup(self):
    host, port = (self.cluster.impalads[0].service.hostname,
                  self.cluster.impalads[0].service.hs2_port)
    self.socket = TSocket(host, port)
    self.transport = TBufferedTransport(self.socket)
    self.transport.open()
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    self.hs2_client = ImpalaHiveServer2Service.Client(self.protocol)

  def teardown(self):
    if self.socket:
      self.socket.close()

  def __execute_hs2_stmt(self, statement, verify=True):
    """
    Executes an hs2 statement

    :param statement: the statement to execute
    :param verify: If set to true, will thrown an exception on a failed hs2 execution
    :return: the result of execution
    """
    from tests.hs2.test_hs2 import TestHS2
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = statement
    result = self.hs2_client.ExecuteStatement(execute_statement_req)
    if verify:
      TestHS2.check_response(result)
    return result

  def __open_hs2(self, user, configuration, verify=True):
    """
    Open a session with hs2

    :param user: the user to open the session
    :param configuration: the configuration for the session
    :param verify: If set to true, will thrown an exception on failed session open
    :return: the result of opening the session
    """
    from tests.hs2.test_hs2 import TestHS2
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = user
    open_session_req.configuration = configuration
    resp = self.hs2_client.OpenSession(open_session_req)
    if verify:
      TestHS2.check_response(resp)
    return resp

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--server_name=server1 "
                   "--authorized_proxy_user_config=hue={0}".format(getuser()),
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
      sentry_config=SENTRY_CONFIG_FILE)
  def test_access_runtime_profile(self, unique_role, unique_name):
    unique_db = unique_name + "_db"

    try:
      self.session_handle = self.__open_hs2(getuser(), dict()).sessionHandle
      self.__execute_hs2_stmt("create role {0}".format(unique_role))
      self.__execute_hs2_stmt("grant create on server to role {0}".format(unique_role))
      self.__execute_hs2_stmt("grant all on database tpch to role {0}"
                              .format(unique_role))
      self.__execute_hs2_stmt("grant select on table functional.complex_view to role {0}"
                              .format(unique_role))
      self.__execute_hs2_stmt("grant role {0} to group {1}"
                              .format(unique_role, grp.getgrnam(getuser()).gr_name))
      # Create db with permissions
      self.__execute_hs2_stmt("create database {0}".format(unique_db))
      self.__execute_hs2_stmt("grant all on database {0} to role {1}"
                              .format(unique_db, unique_role))

      # Current user can't access view's underlying tables
      bad_resp = self.__execute_hs2_stmt("explain select * from functional.complex_view",
                                         False)
      assert 'User \'%s\' does not have privileges to EXPLAIN' % getuser() in \
             str(bad_resp)
      # User should not have access to the runtime profile
      self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
                                                False, False)
      self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
                                                False, True)

      # Repeat as a delegated user
      self.session_handle = \
          self.__open_hs2('hue', {'impala.doas.user': getuser()}).sessionHandle
      # User should not have access to the runtime profile
      self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
                                                False, False)
      self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
                                                False, True)

      # Create a view for which the user has access to the underlying tables.
      self.session_handle = self.__open_hs2(getuser(), dict()).sessionHandle
      self.__execute_hs2_stmt(
          "create view if not exists {0}.customer_view as select * from tpch.customer "
          "limit 1".format(unique_db))

      # User should be able to run EXPLAIN
      self.__execute_hs2_stmt("explain select * from {0}.customer_view"
                              .format(unique_db))

      # User should have access to the runtime profile and exec summary
      self.__run_stmt_and_verify_profile_access("select * from {0}.customer_view"
                                                .format(unique_db), True, False)
      self.__run_stmt_and_verify_profile_access("select * from {0}.customer_view"
                                                .format(unique_db), True, True)

      # Repeat as a delegated user
      self.session_handle = \
          self.__open_hs2('hue', {'impala.doas.user': getuser()}).sessionHandle
      # Delegated user is the current user
      self.__run_stmt_and_verify_profile_access("select * from {0}.customer_view"
                                                .format(unique_db), True, False)
      self.__run_stmt_and_verify_profile_access("select * from {0}.customer_view"
                                                .format(unique_db), True, True)
    finally:
      self.__execute_hs2_stmt("grant all on server to role {0}".format(unique_role))
      self.__execute_hs2_stmt("drop view if exists {0}.customer_view".format(unique_db))
      self.__execute_hs2_stmt("drop table if exists {0}.customer".format(unique_db))
      self.__execute_hs2_stmt("drop database if exists {0}".format(unique_db))
      self.__execute_hs2_stmt("drop role {0}".format(unique_role))

  def __run_stmt_and_verify_profile_access(self, stmt, has_access, close_operation):
    """Runs 'stmt' and retrieves the runtime profile and exec summary. If
      'has_access' is true, it verifies that no runtime profile or exec summary are
      returned. If 'close_operation' is true, make sure the operation is closed before
      retrieving the profile and exec summary."""
    from tests.hs2.test_hs2 import TestHS2
    execute_statement_resp = self.__execute_hs2_stmt(stmt, False)

    if close_operation:
      close_operation_req = TCLIService.TCloseOperationReq()
      close_operation_req.operationHandle = execute_statement_resp.operationHandle
      TestHS2.check_response(self.hs2_client.CloseOperation(close_operation_req))

    get_profile_req = ImpalaHiveServer2Service.TGetRuntimeProfileReq()
    get_profile_req.operationHandle = execute_statement_resp.operationHandle
    get_profile_req.sessionHandle = self.session_handle
    get_profile_resp = self.hs2_client.GetRuntimeProfile(get_profile_req)

    if has_access:
      TestHS2.check_response(get_profile_resp)
      assert "Plan: " in get_profile_resp.profile
    else:
      assert "User %s is not authorized to access the runtime profile or "\
          "execution summary." % (getuser()) in str(get_profile_resp)

    exec_summary_req = ImpalaHiveServer2Service.TGetExecSummaryReq()
    exec_summary_req.operationHandle = execute_statement_resp.operationHandle
    exec_summary_req.sessionHandle = self.session_handle
    exec_summary_resp = self.hs2_client.GetExecSummary(exec_summary_req)

    if has_access:
      TestHS2.check_response(exec_summary_resp)
    else:
      assert "User %s is not authorized to access the runtime profile or "\
          "execution summary." % (getuser()) in str(exec_summary_resp)

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config=" + SENTRY_CONFIG_FILE,
      catalogd_args="--sentry_config=" + SENTRY_CONFIG_FILE,
      impala_log_dir=tempfile.mkdtemp(prefix="test_deprecated_none_",
      dir=os.getenv("LOG_DIR")))
  def test_deprecated_flag_doesnt_show(self):
    assert_no_files_in_dir_contain(self.impala_log_dir, "Ignoring removed flag "
                                                        "authorization_policy_file")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--server_name=server1\
      --authorization_policy_file=ignored_file",
      impala_log_dir=tempfile.mkdtemp(prefix="test_deprecated_",
      dir=os.getenv("LOG_DIR")))
  def test_deprecated_flags(self):
    assert_file_in_dir_contains(self.impala_log_dir, "Ignoring removed flag "
                                                     "authorization_policy_file")

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s" % SENTRY_CONFIG_FILE,
    catalogd_args="--sentry_config=%s" % SENTRY_CONFIG_FILE,
    impala_log_dir=tempfile.mkdtemp(prefix="test_catalog_restart_",
                                    dir=os.getenv("LOG_DIR")))
  def test_catalog_restart(self, unique_role):
    """IMPALA-7713: Tests that a catalogd restart when authorization is enabled should
    reset the previous privileges stored in impalad's catalog to avoid stale privilege
    data in the impalad's catalog."""
    def assert_privileges():
      result = self.client.execute("show grant role %s_foo" % unique_role)
      TestAuthorization._check_privileges(result, [["database", "functional",
                                                    "", "", "", "all", "false"]])

      result = self.client.execute("show grant role %s_bar" % unique_role)
      TestAuthorization._check_privileges(result, [["database", "functional_kudu",
                                                    "", "", "", "all", "false"]])

      result = self.client.execute("show grant role %s_baz" % unique_role)
      TestAuthorization._check_privileges(result, [["database", "functional_avro",
                                                    "", "", "", "all", "false"]])

    self.role_cleanup(unique_role)
    try:
      self.client.execute("create role %s_foo" % unique_role)
      self.client.execute("create role %s_bar" % unique_role)
      self.client.execute("create role %s_baz" % unique_role)
      self.client.execute("grant all on database functional to role %s_foo" %
                          unique_role)
      self.client.execute("grant all on database functional_kudu to role %s_bar" %
                          unique_role)
      self.client.execute("grant all on database functional_avro to role %s_baz" %
                          unique_role)

      assert_privileges()
      self._start_impala_cluster(["--catalogd_args=--sentry_config=%s" %
                                  SENTRY_CONFIG_FILE, "--restart_catalogd_only"])
      assert_privileges()
    finally:
      self.role_cleanup(unique_role)

  def role_cleanup(self, role_name_match):
    """Cleans up any roles that match the given role name."""
    for role_name in self.client.execute("show roles").data:
      if role_name_match in role_name:
        self.client.execute("drop role %s" % role_name)

  @staticmethod
  def _check_privileges(result, expected):
    def columns(row):
      cols = row.split("\t")
      return cols[0:len(cols) - 1]
    assert map(columns, result.data) == expected

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s" % SENTRY_CONFIG_FILE,
    catalogd_args="--sentry_config=%s" % SENTRY_CONFIG_FILE,
    impala_log_dir=tempfile.mkdtemp(prefix="test_catalog_restart_",
                                    dir=os.getenv("LOG_DIR")))
  def test_catalog_object(self, unique_role):
    """IMPALA-7721: Tests /catalog_object web API for principal and privilege"""
    self.role_cleanup(unique_role)
    try:
      self.client.execute("create role %s" % unique_role)
      self.client.execute("grant select on database functional to role %s" % unique_role)
      for service in [self.cluster.catalogd.service,
                      self.cluster.get_first_impalad().service]:
        obj_dump = service.get_catalog_object_dump("PRINCIPAL", "%s.ROLE" % unique_role)
        assert "catalog_version" in obj_dump

        # Get the privilege associated with that principal ID.
        principal_id = re.search(r"principal_id \(i32\) = (\d+)", obj_dump)
        assert principal_id is not None
        obj_dump = service.get_catalog_object_dump("PRIVILEGE", urllib.quote(
            "server=server1->db=functional->action=select->grantoption=false.%s.ROLE" %
            principal_id.group(1)))
        assert "catalog_version" in obj_dump

        # Get the principal that does not exist.
        obj_dump = service.get_catalog_object_dump("PRINCIPAL", "doesnotexist.ROLE")
        assert "CatalogException" in obj_dump

        # Get the privilege that does not exist.
        obj_dump = service.get_catalog_object_dump("PRIVILEGE", urllib.quote(
            "server=server1->db=doesntexist->action=select->grantoption=false.%s.ROLE" %
            principal_id.group(1)))
        assert "CatalogException" in obj_dump
    finally:
      self.role_cleanup(unique_role)

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s" % SENTRY_CONFIG_FILE,
    catalogd_args="--sentry_config=%s --sentry_catalog_polling_frequency_s=3600" %
                  SENTRY_CONFIG_FILE,
    impala_log_dir=tempfile.mkdtemp(prefix="test_invalidate_metadata_sentry_unavailable_",
                                    dir=os.getenv("LOG_DIR")))
  def test_invalidate_metadata_sentry_unavailable(self, unique_role):
    """IMPALA-7824: Tests that running INVALIDATE METADATA when Sentry is unavailable
    should not cause Impala to hang."""
    self.role_cleanup(unique_role)
    try:
      group_name = grp.getgrnam(getuser()).gr_name
      self.client.execute("create role %s" % unique_role)
      self.client.execute("grant all on server to role %s" % unique_role)
      self.client.execute("grant role %s to group `%s`" % (unique_role, group_name))

      self._stop_sentry_service()
      # Calling INVALIDATE METADATA when Sentry is unavailable should return an error.
      result = self.execute_query_expect_failure(self.client, "invalidate metadata")
      result_str = str(result)
      assert "MESSAGE: CatalogException: Error refreshing authorization policy:" \
             in result_str
      assert "CAUSED BY: ImpalaRuntimeException: Error refreshing authorization policy." \
             " Sentry is unavailable. Ensure Sentry is up:" in result_str

      self._start_sentry_service(SENTRY_CONFIG_FILE)
      # Calling INVALIDATE METADATA after Sentry is up should not return an error.
      self.execute_query_expect_success(self.client, "invalidate metadata")
    finally:
      self.role_cleanup(unique_role)

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--server_name=server1 --sentry_config=%s" % SENTRY_CONFIG_FILE,
      catalogd_args="--sentry_config=%s --sentry_catalog_polling_frequency_s=3600 " %
                    SENTRY_CONFIG_FILE,
      impala_log_dir=tempfile.mkdtemp(prefix="test_refresh_authorization_",
                                      dir=os.getenv("LOG_DIR")))
  def test_refresh_authorization(self, unique_role):
    """Tests refresh authorization statement by adding and removing roles and privileges
       externally. The long Sentry polling is used so that any authorization metadata
       updated externally does not get polled by Impala in order to test an an explicit
       call to refresh authorization statement."""
    group_name = grp.getgrnam(getuser()).gr_name
    self.role_cleanup(unique_role)
    for sync_ddl in [1, 0]:
      query_options = {'sync_ddl': sync_ddl}
      clients = []
      if sync_ddl:
        # When sync_ddl is True, we want to ensure the changes are propagated to all
        # coordinators.
        for impalad in self.cluster.impalads:
          clients.append(impalad.service.create_beeswax_client())
      else:
        clients.append(self.client)
      try:
        self.client.execute("create role %s" % unique_role)
        self.client.execute("grant role %s to group `%s`" % (unique_role, group_name))
        self.client.execute("grant refresh on server to %s" % unique_role)

        self.validate_refresh_authorization_roles(unique_role, query_options, clients)
        self.validate_refresh_authorization_privileges(unique_role, query_options,
                                                       clients)
      finally:
        self.role_cleanup(unique_role)

  def validate_refresh_authorization_roles(self, unique_role, query_options, clients):
    """This method tests refresh authorization statement by adding and removing
       roles externally."""
    try:
      # Create two roles inside Impala.
      self.client.execute("create role %s_internal1" % unique_role)
      self.client.execute("create role %s_internal2" % unique_role)
      # Drop an existing role (_internal1) outside Impala.
      role = "%s_internal1" % unique_role
      subprocess.check_call(
        ["/bin/bash", "-c",
         "%s/bin/sentryShell --conf %s/sentry-site.xml -dr -r %s" %
         (os.getenv("SENTRY_HOME"), os.getenv("SENTRY_CONF_DIR"), role)],
        stdout=sys.stdout, stderr=sys.stderr)

      result = self.execute_query_expect_success(self.client, "show roles")
      assert any(role in x for x in result.data)
      self.execute_query_expect_success(self.client, "refresh authorization",
                                        query_options=query_options)
      for client in clients:
        result = self.execute_query_expect_success(client, "show roles")
        assert not any(role in x for x in result.data)

      # Add a new role outside Impala.
      role = "%s_external" % unique_role
      subprocess.check_call(
          ["/bin/bash", "-c",
           "%s/bin/sentryShell --conf %s/sentry-site.xml -cr -r %s" %
           (os.getenv("SENTRY_HOME"), os.getenv("SENTRY_CONF_DIR"), role)],
          stdout=sys.stdout, stderr=sys.stderr)

      result = self.execute_query_expect_success(self.client, "show roles")
      assert not any(role in x for x in result.data)
      self.execute_query_expect_success(self.client, "refresh authorization",
                                        query_options=query_options)
      for client in clients:
        result = self.execute_query_expect_success(client, "show roles")
        assert any(role in x for x in result.data)
    finally:
      for suffix in ["internal1", "internal2", "external"]:
        self.role_cleanup("%s_%s" % (unique_role, suffix))

  def validate_refresh_authorization_privileges(self, unique_role, query_options,
                                                clients):
    """This method tests refresh authorization statement by adding and removing
       privileges externally."""
    # Grant select privilege outside Impala.
    subprocess.check_call(
        ["/bin/bash", "-c",
         "%s/bin/sentryShell --conf %s/sentry-site.xml -gpr -p "
         "'server=server1->db=functional->table=alltypes->action=select' -r %s" %
         (os.getenv("SENTRY_HOME"), os.getenv("SENTRY_CONF_DIR"), unique_role)],
        stdout=sys.stdout, stderr=sys.stderr)

    # Before refresh authorization, there should only be one refresh privilege.
    result = self.execute_query_expect_success(self.client, "show grant role %s" %
                                               unique_role)
    assert len(result.data) == 1
    assert any("refresh" in x for x in result.data)

    for client in clients:
      self.execute_query_expect_failure(client,
                                        "select * from functional.alltypes limit 1")

    self.execute_query_expect_success(self.client, "refresh authorization",
                                      query_options=query_options)

    for client in clients:
      # Ensure select privilege was granted after refresh authorization.
      result = self.execute_query_expect_success(client, "show grant role %s" %
                                                 unique_role)
      assert len(result.data) == 2
      assert any("select" in x for x in result.data)
      assert any("refresh" in x for x in result.data)
      self.execute_query_expect_success(client,
                                        "select * from functional.alltypes limit 1")

  @staticmethod
  def _verify_show_dbs(result, unique_name, visibility_privileges=PRIVILEGES):
    """ Helper function for verifying the results of SHOW DATABASES below.
    Only show databases with privileges implying any of the visibility_privileges.
    """
    for priv in PRIVILEGES:
      # Result lines are in the format of "db_name\tdb_comment"
      db_name = 'db_%s_%s\t' % (unique_name, priv)
      if priv != 'all' and priv not in visibility_privileges:
        assert db_name not in result.data
      else:
        assert db_name in result.data

  def _test_sentry_show_stmts_helper(self, unique_role, unique_name,
                                     visibility_privileges):
    unique_db = unique_name + "_db"
    # TODO: can we create and use a temp username instead of using root?
    another_user = 'root'
    another_user_grp = 'root'
    self.role_cleanup(unique_role)
    try:
      self.client.execute("create role %s" % unique_role)
      self.client.execute("grant create on server to role %s" % unique_role)
      self.client.execute("grant drop on server to role %s" % unique_role)
      self.client.execute("grant role %s to group %s" %
                          (unique_role, grp.getgrnam(getuser()).gr_name))

      self.client.execute("drop database if exists %s cascade" % unique_db)
      self.client.execute("create database %s" % unique_db)
      for priv in PRIVILEGES:
        self.client.execute("create database db_%s_%s" % (unique_name, priv))
        self.client.execute("grant {0} on database db_{1}_{2} to role {3}"
                            .format(priv, unique_name, priv, unique_role))
        self.client.execute("create table %s.tbl_%s (i int)" % (unique_db, priv))
        self.client.execute("grant {0} on table {1}.tbl_{2} to role {3}"
                            .format(priv, unique_db, priv, unique_role))
      self.client.execute("grant role %s to group %s" %
                          (unique_role, another_user_grp))

      # Owner (current user) can still see all the owned databases and tables
      result = self.client.execute("show databases")
      TestAuthorization._verify_show_dbs(result, unique_name)
      result = self.client.execute("show tables in %s" % unique_db)
      assert result.data == ["tbl_%s" % p for p in PRIVILEGES]

      # Check SHOW DATABASES and SHOW TABLES using another username
      # Create another client so we can user another username
      root_impalad_client = self.create_impala_client()
      result = self.execute_query_expect_success(
          root_impalad_client, "show databases", user=another_user)
      TestAuthorization._verify_show_dbs(result, unique_name, visibility_privileges)
      result = self.execute_query_expect_success(
          root_impalad_client, "show tables in %s" % unique_db, user=another_user)
      # Only show tables with privileges implying any of the visibility privileges
      assert 'tbl_all' in result.data   # ALL can imply to any privilege
      for p in visibility_privileges:
        assert 'tbl_%s' % p in result.data
    finally:
      self.client.execute("drop database if exists %s cascade" % unique_db)
      for priv in PRIVILEGES:
        self.client.execute(
            "drop database if exists db_%s_%s cascade" % (unique_name, priv))
      self.role_cleanup(unique_role)

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s "
                 "--authorized_proxy_user_config=%s=* "
                 "--min_privilege_set_for_show_stmts=select" %
                 (SENTRY_CONFIG_FILE, getuser()),
    catalogd_args="--sentry_config={0}".format(SENTRY_CONFIG_FILE),
    sentry_config=SENTRY_CONFIG_FILE_OO,  # Enable Sentry Object Ownership
    sentry_log_dir="{0}/test_sentry_show_stmts_with_select".format(SENTRY_BASE_LOG_DIR))
  def test_sentry_show_stmts_with_select(self, unique_role, unique_name):
    self._test_sentry_show_stmts_helper(unique_role, unique_name, ['select'])

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s "
                 "--authorized_proxy_user_config=%s=* "
                 "--min_privilege_set_for_show_stmts=select,insert" %
                 (SENTRY_CONFIG_FILE, getuser()),
    catalogd_args="--sentry_config={0}".format(SENTRY_CONFIG_FILE),
    sentry_config=SENTRY_CONFIG_FILE_OO,  # Enable Sentry Object Ownership
    sentry_log_dir="{0}/test_sentry_show_stmts_with_select_insert"
                   .format(SENTRY_BASE_LOG_DIR))
  def test_sentry_show_stmts_with_select_insert(self, unique_role, unique_name):
    self._test_sentry_show_stmts_helper(unique_role, unique_name,
                                        ['select', 'insert'])

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s "
                 "--authorized_proxy_user_config=%s=* "
                 "--min_privilege_set_for_show_stmts=any" %
                 (SENTRY_CONFIG_FILE, getuser()),
    catalogd_args="--sentry_config={0}".format(SENTRY_CONFIG_FILE),
    sentry_config=SENTRY_CONFIG_FILE_OO,  # Enable Sentry Object Ownership
    sentry_log_dir="{0}/test_sentry_show_stmts_with_any".format(SENTRY_BASE_LOG_DIR))
  def test_sentry_show_stmts_with_any(self, unique_role, unique_name):
    self._test_sentry_show_stmts_helper(unique_role, unique_name, PRIVILEGES)

  def _test_ranger_show_stmts_helper(self, unique_name, visibility_privileges):
    unique_db = unique_name + "_db"
    admin_client = self.create_impala_client()
    try:
      admin_client.execute("drop database if exists %s cascade" % unique_db, user=ADMIN)
      admin_client.execute("create database %s" % unique_db, user=ADMIN)
      for priv in PRIVILEGES:
        admin_client.execute("create database db_%s_%s" % (unique_name, priv))
        admin_client.execute("grant {0} on database db_{1}_{2} to user {3}"
                             .format(priv, unique_name, priv, getuser()))
        admin_client.execute("create table %s.tbl_%s (i int)" % (unique_db, priv))
        admin_client.execute("grant {0} on table {1}.tbl_{2} to user {3}"
                             .format(priv, unique_db, priv, getuser()))

      # Admin can still see all the databases and tables
      result = admin_client.execute("show databases")
      TestAuthorization._verify_show_dbs(result, unique_name)
      result = admin_client.execute("show tables in %s" % unique_db)
      assert result.data == ["tbl_%s" % p for p in PRIVILEGES]

      # Check SHOW DATABASES and SHOW TABLES using another username
      result = self.client.execute("show databases")
      TestAuthorization._verify_show_dbs(result, unique_name, visibility_privileges)
      result = self.client.execute("show tables in %s" % unique_db)
      # Only show tables with privileges implying any of the visibility privileges
      assert 'tbl_all' in result.data   # ALL can imply to any privilege
      for p in visibility_privileges:
        assert 'tbl_%s' % p in result.data
    finally:
      admin_client.execute("drop database if exists %s cascade" % unique_db)
      for priv in PRIVILEGES:
        admin_client.execute(
            "drop database if exists db_%s_%s cascade" % (unique_name, priv))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger "
                 "--min_privilege_set_for_show_stmts=select",
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger")
  def test_ranger_show_stmts_with_select(self, unique_name):
    self._test_ranger_show_stmts_helper(unique_name, ['select'])

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger "
                 "--min_privilege_set_for_show_stmts=select,insert",
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger")
  def test_ranger_show_stmts_with_select_insert(self, unique_name):
    self._test_ranger_show_stmts_helper(unique_name, ['select', 'insert'])

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger "
                 "--min_privilege_set_for_show_stmts=any",
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger")
  def test_ranger_show_stmts_with_any(self, unique_name):
    self._test_ranger_show_stmts_helper(unique_name, PRIVILEGES)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server-name=server1 --ranger_service_type=hive "
                 "--ranger_app_id=impala --authorization_provider=ranger "
                 "--num_check_authorization_threads=%d" % (random.randint(2, 128)),
    catalogd_args="--server-name=server1 --ranger_service_type=hive "
                  "--ranger_app_id=impala --authorization_provider=ranger")
  def test_num_check_authorization_threads_with_ranger(self, unique_name):
    self._test_ranger_show_stmts_helper(unique_name, PRIVILEGES)

  @SkipIf.sentry_disabled
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--server_name=server1 --sentry_config=%s "
                 "--authorized_proxy_user_config=%s=* "
                 "--num_check_authorization_threads=%d" %
                 (SENTRY_CONFIG_FILE, getuser(), random.randint(2, 128)),
    catalogd_args="--sentry_config={0}".format(SENTRY_CONFIG_FILE),
    sentry_config=SENTRY_CONFIG_FILE_OO,  # Enable Sentry Object Ownership
    sentry_log_dir="{0}/test_num_check_authorization_threads_with_sentry"
                   .format(SENTRY_BASE_LOG_DIR))
  def test_num_check_authorization_threads_with_sentry(self, unique_role, unique_name):
    self._test_sentry_show_stmts_helper(unique_role, unique_name, PRIVILEGES)
