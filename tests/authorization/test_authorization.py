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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--server_name=server1\
      --authorization_policy_file=ignored_file",
      impala_log_dir=tempfile.mkdtemp(prefix="test_deprecated_",
      dir=os.getenv("LOG_DIR")))
  def test_deprecated_flags(self):
    assert_file_in_dir_contains(self.impala_log_dir, "Ignoring removed flag "
                                                     "authorization_policy_file")

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
