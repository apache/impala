#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
#
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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from tests.shell.util import create_impala_shell_executable_dimension
from tests.shell.util import run_impala_shell_cmd
from k5test import K5Realm


class TestImpalaShellKerberosAuth(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    """Overrides all other add_dimension methods in super classes up the entire class
    hierarchy ensuring that each test in this class only get run once
    on different python versions."""
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())
    cls.ImpalaTestMatrix.add_dimension(create_impala_shell_executable_dimension())

  @pytest.mark.execute_serially
  def test_kerberos_host_fqdn_option(self, vector):
    """
    This test checks whether impala-shell uses the hostname specified in
    the kerberos_host_fqdn option when looking for a service principal
    for Kerberos authentication.

    Note: Since the Kerberos authentication is not enabled in the python
    test environment, the connection will fail for sure, but the Kerberos
    log can be used to check if the correct service principal is used.
    """
    realm = None
    try:
      realm = self._create_kerberos_realm_and_user("testuser", "password")
      env = {
        "KRB5CCNAME": "FILE:" + realm.ccache,  # Ticket cache created by kinit
        "KRB5_TRACE": "/dev/stderr",           # Krb log to validate the principals
      }
      result = run_impala_shell_cmd(vector, ['--kerberos',
                                    '--connect_max_tries=1',
                                    '--protocol=hs2-http',
                                    '--kerberos_host_fqdn=any.host',
                                    '--quiet'], env=env)

      assert "testuser@KRBTEST.COM" in result.stderr, \
        "Principal 'testuser@KRBTEST.COM' should be in the Kerberos log"
      assert "impala/any.host@KRBTEST.COM" in result.stderr, \
        "Principal 'impala/any.host@KRBTEST.COM' should be in the Kerberos log"
    finally:
      realm.stop_kdc()

  def _create_kerberos_realm_and_user(self, principal, password):
    """
    Initializes a test Kerberos realm, creates a new user principal,
    and runs kinit to get a Kerberos ticket.

    Args:
      principal (str): Name of the new Kerberos user principal.
      password (str): Password of the new Kerberos user principal.

    Returns:
      realm (K5Realm): The Kerberos realm.
    """
    realm = K5Realm(create_host=False, get_creds=False)
    realm.addprinc(principal, password)
    realm.kinit(principal, password)
    return realm
