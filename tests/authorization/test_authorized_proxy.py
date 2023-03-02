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

from __future__ import absolute_import, division, print_function
import pytest
import os
import grp
import time
import json
import tempfile
import shutil

from getpass import getuser
from ImpalaService import ImpalaHiveServer2Service
from TCLIService import TCLIService
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIf
from tests.hs2.hs2_test_suite import operation_id_to_query_id

AUDIT_LOG_DIR = tempfile.mkdtemp(dir=os.getenv("LOG_DIR"))

RANGER_IMPALAD_ARGS = "--server-name=server1 " \
                      "--ranger_service_type=hive " \
                      "--ranger_app_id=impala " \
                      "--authorization_provider=ranger " \
                      "--abort_on_failed_audit_event=false " \
                      "--audit_event_log_dir={0}".format(AUDIT_LOG_DIR)
RANGER_CATALOGD_ARGS = "--server-name=server1 " \
                       "--ranger_service_type=hive " \
                       "--ranger_app_id=impala " \
                       "--authorization_provider=ranger"
RANGER_ADMIN_USER = "admin"


class TestAuthorizedProxy(CustomClusterTestSuite):
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
    shutil.rmtree(AUDIT_LOG_DIR, ignore_errors=True)

  def _execute_hs2_stmt(self, statement, verify=True):
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

  def _open_hs2(self, user, configuration, verify=True):
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
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} --authorized_proxy_user_config=foo=bar;hue=non_owner "
                 .format(RANGER_IMPALAD_ARGS),
    catalogd_args=RANGER_CATALOGD_ARGS)
  def test_authorized_proxy_user_with_ranger(self):
    """Tests authorized proxy user with Ranger using HS2."""
    self._test_authorized_proxy_with_ranger(self._test_authorized_proxy, "non_owner",
                                            False)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} --authorized_proxy_user_config=hue=non_owner "
                 "--authorized_proxy_group_config=foo=bar;hue=non_owner "
                 "--use_customized_user_groups_mapper_for_ranger"
                 .format(RANGER_IMPALAD_ARGS),
    catalogd_args=RANGER_CATALOGD_ARGS)
  def test_authorized_proxy_group_with_ranger(self):
    """Tests authorized proxy group with Ranger using HS2."""
    self._test_authorized_proxy_with_ranger(self._test_authorized_proxy, "non_owner",
                                            True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="{0} --authorized_proxy_user_config=foo=bar "
                 "--authorized_proxy_group_config=foo=bar".format(RANGER_IMPALAD_ARGS),
    catalogd_args=RANGER_CATALOGD_ARGS)
  def test_no_matching_user_and_group_authorized_proxy_with_ranger(self):
    self._test_no_matching_user_and_group_authorized_proxy()

  def _test_no_matching_user_and_group_authorized_proxy(self):
    open_session_req = TCLIService.TOpenSessionReq()
    open_session_req.username = "hue"
    open_session_req.configuration = dict()
    open_session_req.configuration["impala.doas.user"] = "abc"
    resp = self.hs2_client.OpenSession(open_session_req)
    assert "User 'hue' is not authorized to delegate to 'abc'" in str(resp)

  def _test_authorized_proxy_with_ranger(self, test_func, delegated_user,
                                         delegated_to_group):
    try:
      self.session_handle = self._open_hs2(RANGER_ADMIN_USER, dict()).sessionHandle
      if not delegated_to_group:
        self._execute_hs2_stmt("grant all on table tpch.lineitem to user non_owner")
      else:
        self._execute_hs2_stmt("grant all on table tpch.lineitem to group non_owner")
      test_func(delegated_user)
    finally:
      self.session_handle = self._open_hs2(RANGER_ADMIN_USER, dict()).sessionHandle
      if not delegated_to_group:
        self._execute_hs2_stmt("revoke all on table tpch.lineitem from user non_owner")
      else:
        self._execute_hs2_stmt("revoke all on table tpch.lineitem from group non_owner")

  def _test_authorized_proxy(self, delegated_user):
    """End-to-end impersonation + authorization test. Expects authorization to be
       configured before running this test"""
    # TODO: To reuse the HS2 utility code from the TestHS2 test suite we need to import
    # the module within this test function, rather than as a top-level import. This way
    # the tests in that module will not get pulled when executing this test suite. The fix
    # is to split the utility code out of the TestHS2 class and support HS2 as a first
    # class citizen in our test framework.
    from tests.hs2.test_hs2 import TestHS2

    # Try to query a table we are not authorized to access.
    self.session_handle = self._open_hs2("hue",
                                         {"impala.doas.user": delegated_user})\
        .sessionHandle
    bad_resp = self._execute_hs2_stmt("describe tpch_seq.lineitem", False)
    assert "User '%s' does not have privileges to access" % delegated_user in \
           str(bad_resp)

    assert self._wait_for_audit_record(user=delegated_user, impersonator="hue"), \
           "No matching audit event recorded in time window"

    # Now try the same operation on a table we are authorized to access.
    good_resp = self._execute_hs2_stmt("describe tpch.lineitem")
    TestHS2.check_response(good_resp)

    # Verify the correct user information is in the runtime profile.
    query_id = operation_id_to_query_id(good_resp.operationHandle.operationId)
    profile_page = self.cluster.impalads[0].service.read_query_profile_page(query_id)
    self._verify_profile_user_fields(profile_page, effective_user=delegated_user,
                                     delegated_user=delegated_user, connected_user="hue")

    # Try to delegate a user we are not authorized to delegate to.
    resp = self._open_hs2("hue", {"impala.doas.user": "some_user"}, False)
    assert "User 'hue' is not authorized to delegate to 'some_user'" in str(resp)

    # Create a new session which does not have a do_as_user and run a simple query.
    self.session_handle = self._open_hs2("hue", dict()).sessionHandle
    resp = self._execute_hs2_stmt("select 1")

    # Verify the correct user information is in the runtime profile. Since there is
    # no do_as_user the Delegated User field should be empty.
    query_id = operation_id_to_query_id(resp.operationHandle.operationId)

    profile_page = self.cluster.impalads[0].service.read_query_profile_page(query_id)
    self._verify_profile_user_fields(profile_page, effective_user="hue",
                                     delegated_user="", connected_user="hue")

  def _verify_profile_user_fields(self, profile_str, effective_user, connected_user,
                                  delegated_user):
    """Verifies the given runtime profile string contains the specified values for
       User, Connected User, and Delegated User"""
    assert "\n    User: {0}\n".format(effective_user) in profile_str
    assert "\n    Connected User: {0}\n".format(connected_user) in profile_str
    assert "\n    Delegated User: {0}\n".format(delegated_user) in profile_str

  def _wait_for_audit_record(self, user, impersonator, timeout_secs=30):
    """Waits until an audit log record is found that contains the given user and
       impersonator, or until the timeout is reached.
    """
    # The audit event might not show up immediately (the audit logs are flushed to disk
    # on regular intervals), so poll the audit event logs until a matching record is
    # found.
    start_time = time.time()
    while time.time() - start_time < timeout_secs:
      for audit_file_name in os.listdir(AUDIT_LOG_DIR):
        if self._find_matching_audit_record(audit_file_name, user, impersonator):
          return True
      time.sleep(1)
    return False

  def _find_matching_audit_record(self, audit_file_name, user, impersonator):
    with open(os.path.join(AUDIT_LOG_DIR, audit_file_name)) as audit_log_file:
      for line in audit_log_file.readlines():
        json_dict = json.loads(line)
        if len(json_dict) == 0: continue
        if json_dict[min(json_dict)]["user"] == user and \
            json_dict[min(json_dict)]["impersonator"] == impersonator:
          return True
    return False
