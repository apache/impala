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
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from tests.shell.util import run_impala_shell_cmd


class TestImpalaShellJwtAuth(ImpalaTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    """Overrides all other add_dimension methods in super classes up the entire class
    hierarchy ensuring that each test in this class only get run once."""
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())

  def test_jwt_cmd_without_jwt_auth(self, vector):
    """Asserts the jwt_cmd arg is only allowed when JWT auth is enabled."""
    result = run_impala_shell_cmd(vector, ['--jwt_cmd=echo', '--protocol=hs2-http',
                                  '--auth_creds_ok_in_clear'], expect_success=False)
    assert "Option --jwt_cmd requires using JWT authentication mechanism (-j)" \
           in result.stderr

  def test_jwt_cmd_invalid(self, vector):
    """Asserts an invalid jwt_cmd arg value produces an explanatory error message."""
    result = run_impala_shell_cmd(vector, ['-j', '--protocol=hs2-http',
                                  '--auth_creds_ok_in_clear', '--jwt_cmd=idontexist'],
                                  expect_success=False)
    assert "Error retrieving JWT" in result.stderr
    assert "command was: 'idontexist'" in result.stderr

  def test_jwt_auth_without_ssl_creds_in_clear(self, vector):
    """Asserts that JWTs do not get sent over insecure network connections if the user
    does not provide the auth_creds_ok_in_clear arg."""
    result = run_impala_shell_cmd(vector, ['-j', '--protocol=hs2-http'],
                                  expect_success=False)
    assert "JWTs may not be sent over insecure connections. Enable SSL or " \
           "set --auth_creds_ok_in_clear" in result.stderr

  def test_jwt_auth_protocol_beeswax(self, vector):
    """Asserts that JWT auth does not work with the beeswax protocol."""
    result = run_impala_shell_cmd(vector, ['-j', '--protocol=beeswax'],
                                  expect_success=False)
    assert "Invalid protocol 'beeswax'. JWT authentication requires using the " \
           "'hs2-http' protocol" in result.stderr

  def test_jwt_auth_protocol_hs2_no_http(self, vector):
    """Asserts that JWT auth does not work with the plain hs2 protocol."""
    result = run_impala_shell_cmd(vector, ['-j', '--protocol=hs2'], expect_success=False)
    assert "Invalid protocol 'hs2'. JWT authentication requires using the " \
           "'hs2-http' protocol" in result.stderr

  def test_jwt_auth_protocol_strict_hs2(self, vector):
    """Asserts that JWT auth does not work when strict hs2 is enabled."""
    result = run_impala_shell_cmd(vector, ['-j', '--protocol=hs2-http',
                                           '--strict_hs2_protocol'],
                                           expect_success=False)
    assert "JWT authentication is not supported when using strict hs2." in result.stderr

  def test_multiple_auth_ldap_jwt(self, vector):
    """Asserts that ldap and jwt auth cannot both be enabled."""
    result = run_impala_shell_cmd(vector, ['-l', '-j'], expect_success=False)
    assert "Please specify at most one authentication mechanism (-k, -l, or -j)" \
           in result.stderr

  def test_multiple_auth_ldap_kerberos(self, vector):
    """Asserts that ldap and kerberos auth cannot both be enabled."""
    result = run_impala_shell_cmd(vector, ['-l', '-k'], expect_success=False)
    assert "Please specify at most one authentication mechanism (-k, -l, or -j)" \
           in result.stderr

  def test_multiple_auth_jwt_kerberos(self, vector):
    """Asserts that jwt and kerberos auth cannot both be enabled."""
    result = run_impala_shell_cmd(vector, ['-j', '-k'], expect_success=False)
    assert "Please specify at most one authentication mechanism (-k, -l, or -j)" \
           in result.stderr

  def test_multiple_auth_ldap_jwt_kerberos(self, vector):
    """Asserts ldap, jwt, and kerberos auth cannot all be enabled."""
    result = run_impala_shell_cmd(vector, ['-l', '-j', '-k'], expect_success=False)
    assert "Please specify at most one authentication mechanism (-k, -l, or -j)" \
           in result.stderr
