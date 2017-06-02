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
import shutil
import tempfile
import json
from time import sleep, time
from getpass import getuser
from ImpalaService import ImpalaHiveServer2Service
from TCLIService import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.hs2.hs2_test_suite import operation_id_to_query_id
from tests.util.filesystem_utils import WAREHOUSE

AUTH_POLICY_FILE = "%s/authz-policy.ini" % WAREHOUSE

class TestAuthorization(CustomClusterTestSuite):
  AUDIT_LOG_DIR = tempfile.mkdtemp(dir=os.getenv('LOG_DIR'))

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
    shutil.rmtree(self.AUDIT_LOG_DIR, ignore_errors=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--server_name=server1\
      --authorization_policy_file=%s\
      --authorization_policy_provider_class=%s" %\
      (AUTH_POLICY_FILE,
       "org.apache.sentry.provider.file.LocalGroupResourceAuthorizationProvider"))
  def test_custom_authorization_provider(self):
    from tests.hs2.test_hs2 import TestHS2
    open_session_req = TCLIService.TOpenSessionReq()
    # User is 'test_user' (defined in the authorization policy file)
    open_session_req.username = 'test_user'
    open_session_req.configuration = dict()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Try to query a table we are not authorized to access.
    self.session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch_seq.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    assert 'User \'%s\' does not have privileges to access' % 'test_user' in\
        str(execute_statement_resp)

    # Now try the same operation on a table we are authorized to access.
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)


  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--server_name=server1\
      --authorization_policy_file=%s\
      --authorized_proxy_user_config=hue=%s" % (AUTH_POLICY_FILE, getuser()))
  def test_access_runtime_profile(self):
    from tests.hs2.test_hs2 import TestHS2
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = getuser()
    open_session_req.configuration = dict()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Current user can't access view's underlying tables
    self.session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "explain select * from functional.complex_view"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    assert 'User \'%s\' does not have privileges to EXPLAIN' % getuser() in\
        str(execute_statement_resp)
    # User should not have access to the runtime profile
    self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
        False, False)
    self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
        False, True)

    # Repeat as a delegated user
    open_session_req.username = 'hue'
    open_session_req.configuration = dict()
    # Delegated user is the current user
    open_session_req.configuration['impala.doas.user'] = getuser()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)
    self.session_handle = resp.sessionHandle
    # User should not have access to the runtime profile
    self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
        False, False)
    self.__run_stmt_and_verify_profile_access("select * from functional.complex_view",
        False, True)

    # Create a view for which the user has access to the underlying tables.
    open_session_req.username = getuser()
    open_session_req.configuration = dict()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)
    self.session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = """create view if not exists tpch.customer_view as
        select * from tpch.customer limit 1"""
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    # User should be able to run EXPLAIN
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = """explain select * from tpch.customer_view"""
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    # User should have access to the runtime profile and exec summary
    self.__run_stmt_and_verify_profile_access("select * from tpch.customer_view", True,
        False)
    self.__run_stmt_and_verify_profile_access("select * from tpch.customer_view", True,
        True)

    # Repeat as a delegated user
    open_session_req.username = 'hue'
    open_session_req.configuration = dict()
    # Delegated user is the current user
    open_session_req.configuration['impala.doas.user'] = getuser()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)
    self.session_handle = resp.sessionHandle
    # User should have access to the runtime profile and exec summary
    self.__run_stmt_and_verify_profile_access("select * from tpch.customer_view",
        True, False)
    self.__run_stmt_and_verify_profile_access("select * from tpch.customer_view",
        True, True)

    # Clean up
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "drop view if exists tpch.customer_view"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--server_name=server1\
      --authorization_policy_file=%s\
      --authorized_proxy_user_config=hue=%s\
      --abort_on_failed_audit_event=false\
      --audit_event_log_dir=%s" % (AUTH_POLICY_FILE, getuser(), AUDIT_LOG_DIR))
  def test_impersonation(self):
    """End-to-end impersonation + authorization test. Expects authorization to be
    configured before running this test"""
    # TODO: To reuse the HS2 utility code from the TestHS2 test suite we need to import
    # the module within this test function, rather than as a top-level import. This way
    # the tests in that module will not get pulled when executing this test suite. The fix
    # is to split the utility code out of the TestHS2 class and support HS2 as a first
    # class citizen in our test framework.
    from tests.hs2.test_hs2 import TestHS2
    open_session_req = TCLIService.TOpenSessionReq()
    # Connected user is 'hue'
    open_session_req.username = 'hue'
    open_session_req.configuration = dict()
    # Delegated user is the current user
    open_session_req.configuration['impala.doas.user'] = getuser()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Try to query a table we are not authorized to access.
    self.session_handle = resp.sessionHandle
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch_seq.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    assert 'User \'%s\' does not have privileges to access' % getuser() in\
        str(execute_statement_resp)

    assert self.__wait_for_audit_record(user=getuser(), impersonator='hue'),\
        'No matching audit event recorded in time window'

    # Now try the same operation on a table we are authorized to access.
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = "describe tpch.lineitem"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)

    TestHS2.check_response(execute_statement_resp)

    # Verify the correct user information is in the runtime profile
    query_id = operation_id_to_query_id(
        execute_statement_resp.operationHandle.operationId)
    profile_page = self.cluster.impalads[0].service.read_query_profile_page(query_id)
    self.__verify_profile_user_fields(profile_page, effective_user=getuser(),
        delegated_user=getuser(), connected_user='hue')

    # Try to user we are not authorized to delegate to.
    open_session_req.configuration['impala.doas.user'] = 'some_user'
    resp = self.hs2_client.OpenSession(open_session_req)
    assert 'User \'hue\' is not authorized to delegate to \'some_user\'' in str(resp)

    # Create a new session which does not have a do_as_user.
    open_session_req.username = 'hue'
    open_session_req.configuration = dict()
    resp = self.hs2_client.OpenSession(open_session_req)
    TestHS2.check_response(resp)

    # Run a simple query, which should succeed.
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = resp.sessionHandle
    execute_statement_req.statement = "select 1"
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

    # Verify the correct user information is in the runtime profile. Since there is
    # no do_as_user the Delegated User field should be empty.
    query_id = operation_id_to_query_id(
        execute_statement_resp.operationHandle.operationId)

    profile_page = self.cluster.impalads[0].service.read_query_profile_page(query_id)
    self.__verify_profile_user_fields(profile_page, effective_user='hue',
        delegated_user='', connected_user='hue')

    self.socket.close()
    self.socket = None

  def __verify_profile_user_fields(self, profile_str, effective_user, connected_user,
      delegated_user):
    """Verifies the given runtime profile string contains the specified values for
    User, Connected User, and Delegated User"""
    assert '\n    User: %s\n' % effective_user in profile_str
    assert '\n    Connected User: %s\n' % connected_user in profile_str
    assert '\n    Delegated User: %s\n' % delegated_user in profile_str

  def __wait_for_audit_record(self, user, impersonator, timeout_secs=30):
    """Waits until an audit log record is found that contains the given user and
    impersonator, or until the timeout is reached.
    """
    # The audit event might not show up immediately (the audit logs are flushed to disk
    # on regular intervals), so poll the audit event logs until a matching record is
    # found.
    start_time = time()
    while time() - start_time < timeout_secs:
      for audit_file_name in os.listdir(self.AUDIT_LOG_DIR):
        if self.__find_matching_audit_record(audit_file_name, user, impersonator):
          return True
      sleep(1)
    return False

  def __find_matching_audit_record(self, audit_file_name, user, impersonator):
    with open(os.path.join(self.AUDIT_LOG_DIR, audit_file_name)) as audit_log_file:
      for line in audit_log_file.readlines():
          json_dict = json.loads(line)
          if len(json_dict) == 0: continue
          if json_dict[min(json_dict)]['user'] == user and\
              json_dict[min(json_dict)]['impersonator'] == impersonator:
            return True
    return False

  def __run_stmt_and_verify_profile_access(self, stmt, has_access, close_operation):
    """Runs 'stmt' and retrieves the runtime profile and exec summary. If
      'has_access' is true, it verifies that no runtime profile or exec summary are
      returned. If 'close_operation' is true, make sure the operation is closed before
      retrieving the profile and exec summary."""
    from tests.hs2.test_hs2 import TestHS2
    execute_statement_req = TCLIService.TExecuteStatementReq()
    execute_statement_req.sessionHandle = self.session_handle
    execute_statement_req.statement = stmt
    execute_statement_resp = self.hs2_client.ExecuteStatement(execute_statement_req)
    TestHS2.check_response(execute_statement_resp)

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
      assert exec_summary_resp.summary.nodes is not None
    else:
      assert "User %s is not authorized to access the runtime profile or "\
          "execution summary." % (getuser()) in str(exec_summary_resp)
