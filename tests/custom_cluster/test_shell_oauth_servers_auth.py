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

import json
import os
import re
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from tests.shell.util import run_impala_shell_cmd


class TestImpalaShellOAuthServersAuth(CustomClusterTestSuite):
  """End-to-end tests for JWT/OAuth authentication configured via --oauth_servers."""

  JWKS_JWTS_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata', 'jwt')
  JWKS_JSON_PATH = os.path.join(JWKS_JWTS_DIR, 'jwks_signing.json')
  JWT_SIGNED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_signed')
  OAUTH_SIGNED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_signed')

  _OAUTH_SERVERS_JSON = json.dumps([{
      'jwksFilePath': JWKS_JSON_PATH,
      'usernameClaim': 'sub',
  }], separators=(',', ':'))
  # CustomClusterTestSuite applies str.format() to impalad_args; escape JSON braces.
  # Single quotes preserve JSON double quotes through shlex/gflags parsing.
  _OAUTH_SERVERS_JSON_ESCAPED = _OAUTH_SERVERS_JSON.replace('{', '{{').replace('}', '}}')
  OAUTH_SERVERS_FLAG = "-oauth_servers='{0}'".format(_OAUTH_SERVERS_JSON_ESCAPED)

  JWT_IMPALAD_ARGS = ('-v 2 -jwt_token_auth=true -jwt_allow_without_tls=true {0}'
                      .format(OAUTH_SERVERS_FLAG))

  OAUTH_IMPALAD_ARGS = ('-v 2 -oauth_token_auth=true -oauth_allow_without_tls=true {0}'
                        .format(OAUTH_SERVERS_FLAG))

  OAUTH_LEGACY_FLAGS_IMPALAD_ARGS = (
      '-v 2 -oauth_token_auth=true -oauth_allow_without_tls=true '
      '-oauth_jwks_file_path={0} -oauth_jwt_custom_claim_username=sub'
      .format(JWKS_JSON_PATH))

  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=JWT_IMPALAD_ARGS,
    impala_log_dir="{oauth_servers_jwt_success}",
    tmp_dir_placeholders=["oauth_servers_jwt_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwt_auth_with_oauth_servers(self, vector):
    """JWT auth succeeds when JWKS is configured through --oauth_servers."""
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(self.JWT_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        r'--oauth_servers=.*' + re.escape(self.JWKS_JSON_PATH), expected_count=1)
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=OAUTH_IMPALAD_ARGS,
    impala_log_dir="{oauth_servers_oauth_success}",
    tmp_dir_placeholders=["oauth_servers_oauth_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_oauth_auth_with_oauth_servers(self, vector):
    """OAuth auth succeeds when JWKS is configured through --oauth_servers."""
    args = ['--protocol', vector.get_value('protocol'), '-a', '--oauth_cmd',
            'cat {0}'.format(self.OAUTH_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        r'--oauth_servers=.*' + re.escape(self.JWKS_JSON_PATH), expected_count=1)
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=OAUTH_LEGACY_FLAGS_IMPALAD_ARGS,
    impala_log_dir="{oauth_legacy_flags_success}",
    tmp_dir_placeholders=["oauth_legacy_flags_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_oauth_auth_with_legacy_oauth_flags(self, vector):
    """OAuth auth succeeds with legacy oauth_* flags."""
    args = ['--protocol', vector.get_value('protocol'), '-a', '--oauth_cmd',
            'cat {0}'.format(self.OAUTH_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    self._stop_impala_cluster()
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout
