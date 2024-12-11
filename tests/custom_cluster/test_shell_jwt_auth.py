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
import os
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from tests.shell.util import run_impala_shell_cmd


class TestImpalaShellJWTAuth(CustomClusterTestSuite):
  """Tests the Impala shell JWT authentication functionality by first standing up an
  Impala cluster with specific startup flags to enable JWT authentication support.
  Then, the Impala shell is launched in a separate process with authentication done using
  JWTs.  Assertions are done by scanning the shell output and Impala server logs for
  expected strings.

  These tests require a JWKS and three JWT files to be present in the 'testdata/jwt'
  directory. The 'testdata/bin/jwt-generate.sh' script can be run to set up the
  necessary files. Since the JWKS/JWT files are committed to the git repo, this script
  should not need to be executed again.
  """

  JWKS_JWTS_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata', 'jwt')
  JWKS_JSON_PATH = os.path.join(JWKS_JWTS_DIR, 'jwks_signing.json')
  JWT_SIGNED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_signed')
  JWT_EXPIRED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_expired')
  JWT_INVALID_JWK = os.path.join(JWKS_JWTS_DIR, 'jwt_signed_untrusted')

  IMPALAD_ARGS = ("-v 2 -jwks_file_path={0} -jwt_custom_claim_username=sub "
                  "-jwt_token_auth=true -jwt_allow_without_tls=true "
                  .format(JWKS_JSON_PATH))

  # Name of the Impala metric containing the total count of hs2-http connections opened.
  HS2_HTTP_CONNS = "impala.thrift-server.hiveserver2-http-frontend.total-connections"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    """Overrides all other add_dimension methods in super classes up the entire class
    hierarchy ensuring that each test in this class run using the hs2-http protocol."""
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_http_transport())

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS,
    impala_log_dir="{jwt_auth_success}",
    tmp_dir_placeholders=["jwt_auth_success"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwt_auth_valid(self, vector):
    """Asserts the Impala shell can authenticate to Impala using JWT authentication.
    Also executes a query to ensure the authentication was successful."""
    before_rpc_count = self.__get_rpc_count()

    # Run a query and wait for it to complete.
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)
    self.cluster.get_first_impalad().service.wait_for_metric_value(
        "impala-server.backend-num-queries-executed", 1, timeout=15)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    query_rpc_count = self.__get_rpc_count() - before_rpc_count
    self.__assert_success_fail_metric(success_count=query_rpc_count)

    # Shut down cluster to ensure logs flush to disk.
    self._stop_impala_cluster()

    # Ensure JWT auth was enabled by checking the coordinator startup flags logged
    # in the coordinator's INFO logfile
    self.assert_impalad_log_contains("INFO",
        '--jwks_file_path={0}'.format(self.JWKS_JSON_PATH), expected_count=1)
    # Ensure JWT auth was successful by checking impala coordinator logs
    self.assert_impalad_log_contains("INFO",
        'effective username: test-user', expected_count=1)
    self.assert_impalad_log_contains("INFO",
        r'connected_user \(string\) = "test-user"', expected_count=1)

    # Ensure the query ran successfully.
    assert "version()" in result.stdout
    assert "impalad version" in result.stdout

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS,
    impala_log_dir="{jwt_auth_fail}",
    tmp_dir_placeholders=["jwt_auth_fail"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwt_auth_expired(self, vector):
    """Asserts the Impala shell fails to authenticate when it presents a JWT that has a
    valid signature but is expired."""
    before_rpc_count = self.__get_rpc_count()

    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_EXPIRED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args, expect_success=False)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    self.__wait_for_rpc_count(before_rpc_count + 1)
    query_rpc_count = self.__get_rpc_count() - before_rpc_count
    self.__assert_success_fail_metric(fail_count=query_rpc_count)

    # Shut down cluster to ensure logs flush to disk.
    self._stop_impala_cluster()

    # Ensure JWT auth was enabled by checking the coordinator startup flags logged
    # in the coordinator's INFO logfile
    expected_string = '--jwks_file_path={0}'.format(self.JWKS_JSON_PATH)
    self.assert_impalad_log_contains("INFO", expected_string)

    # Ensure JWT auth failed by checking impala coordinator logs
    expected_string = (
      'Error verifying JWT token'
      '.*'
      'Error verifying JWT Token: Verification failed, error: token expired'
    )
    self.assert_impalad_log_contains("ERROR", expected_string, expected_count=-1)

    # Ensure the shell login failed.
    assert "HttpError" in result.stderr
    assert "HTTP code 401: Unauthorized" in result.stderr
    assert "Not connected to Impala, could not execute queries." in result.stderr

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args=IMPALAD_ARGS,
    impala_log_dir="{jwt_auth_invalid_jwk}",
    tmp_dir_placeholders=["jwt_auth_invalid_jwk"],
    disable_log_buffering=True,
    cluster_size=1)
  def test_jwt_auth_invalid_jwk(self, vector):
    """Asserts the Impala shell fails to authenticate when it presents a JWT that has a
    valid signature but is expired."""
    before_rpc_count = self.__get_rpc_count()

    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_INVALID_JWK),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args, expect_success=False)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    self.__wait_for_rpc_count(before_rpc_count + 1)
    query_rpc_count = self.__get_rpc_count() - before_rpc_count
    self.__assert_success_fail_metric(fail_count=query_rpc_count)

    # Shut down cluster to ensure logs flush to disk.
    self._stop_impala_cluster()

    # Ensure JWT auth was enabled by checking the coordinator startup flags logged
    # in the coordinator's INFO logfile
    expected_string = '--jwks_file_path={0}'.format(self.JWKS_JSON_PATH)
    self.assert_impalad_log_contains("INFO", expected_string)

    # Ensure JWT auth failed by checking impala coordinator logs
    expected_string = (
      'Error verifying JWT token'
      '.*'
      'Error verifying JWT Token: Invalid JWK ID in the JWT token'
    )
    self.assert_impalad_log_contains("ERROR", expected_string, expected_count=-1)

    # Ensure the shell login failed.
    assert "HttpError" in result.stderr
    assert "HTTP code 401: Unauthorized" in result.stderr
    assert "Not connected to Impala, could not execute queries." in result.stderr

  def __assert_success_fail_metric(self, success_count=0, fail_count=0):
    """Impala emits metrics that count the number of successful and failed JWT
    authentications. This function asserts the JWT auth success/fail counters from the
    coordinator match the expected values."""
    actual = self.cluster.get_first_impalad().service.get_metric_values([
        "impala.thrift-server.hiveserver2-http-frontend.total-jwt-token-auth-success",
        "impala.thrift-server.hiveserver2-http-frontend.total-jwt-token-auth-failure"])

    assert actual[0] == success_count, "Expected JWT auth success count to be '{}' but " \
        "was '{}'".format(success_count, actual[0])
    assert actual[1] == fail_count, "Expected JWT auth failure count to be '{}' but " \
        "was '{}'".format(fail_count, actual[1])

  def __get_rpc_count(self):
    return self.cluster.get_first_impalad().service.get_metric_value(self.HS2_HTTP_CONNS)

  def __wait_for_rpc_count(self, expected_count):
    self.cluster.get_first_impalad().service.wait_for_metric_value(self.HS2_HTTP_CONNS,
        expected_count, allow_greater=True)
