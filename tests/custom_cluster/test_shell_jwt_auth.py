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
import tempfile

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_http_transport
from time import sleep
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

  LOG_DIR_JWT_AUTH_SUCCESS = tempfile.mkdtemp(prefix="jwt_auth_success")
  LOG_DIR_JWT_AUTH_FAIL = tempfile.mkdtemp(prefix="jwt_auth_fail")
  LOG_DIR_JWT_AUTH_INVALID_JWK = tempfile.mkdtemp(prefix="jwt_auth_invalid_jwk")

  JWKS_JWTS_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata', 'jwt')
  JWKS_JSON_PATH = os.path.join(JWKS_JWTS_DIR, 'jwks_signing.json')
  JWT_SIGNED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_signed')
  JWT_EXPIRED_PATH = os.path.join(JWKS_JWTS_DIR, 'jwt_expired')
  JWT_INVALID_JWK = os.path.join(JWKS_JWTS_DIR, 'jwt_signed_untrusted')

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
    impala_log_dir=LOG_DIR_JWT_AUTH_SUCCESS,
    impalad_args="-v 2 -jwks_file_path={0} -jwt_custom_claim_username=sub "
    "-jwt_token_auth=true -jwt_allow_without_tls=true"
    .format(JWKS_JSON_PATH))
  def test_jwt_auth_valid(self, vector):
    """Asserts the Impala shell can authenticate to Impala using JWT authentication.
    Also executes a query to ensure the authentication was successful."""
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_SIGNED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    sleep(5)
    self.__assert_success_fail_metric(success_count_min=15, success_count_max=16)

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
    impala_log_dir=LOG_DIR_JWT_AUTH_FAIL,
    impalad_args="-v 2 -jwks_file_path={0} -jwt_custom_claim_username=sub "
    "-jwt_token_auth=true -jwt_allow_without_tls=true"
    .format(JWKS_JSON_PATH))
  def test_jwt_auth_expired(self, vector):
    """Asserts the Impala shell fails to authenticate when it presents a JWT that has a
    valid signature but is expired."""
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_EXPIRED_PATH),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args, expect_success=False)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    sleep(5)
    self.__assert_success_fail_metric(failure_count_min=4, failure_count_max=4)

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
    assert "Error connecting: HttpError" in result.stderr
    assert "HTTP code 401: Unauthorized" in result.stderr
    assert "Not connected to Impala, could not execute queries." in result.stderr

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impala_log_dir=LOG_DIR_JWT_AUTH_INVALID_JWK,
    impalad_args="-v 2 -jwks_file_path={0} -jwt_custom_claim_username=sub "
    "-jwt_token_auth=true -jwt_allow_without_tls=true"
    .format(JWKS_JSON_PATH))
  def test_jwt_auth_invalid_jwk(self, vector):
    """Asserts the Impala shell fails to authenticate when it presents a JWT that has a
    valid signature but is expired."""
    args = ['--protocol', vector.get_value('protocol'), '-j', '--jwt_cmd',
            'cat {0}'.format(TestImpalaShellJWTAuth.JWT_INVALID_JWK),
            '-q', 'select version()', '--auth_creds_ok_in_clear']
    result = run_impala_shell_cmd(vector, args, expect_success=False)

    # Ensure the Impala coordinator is correctly reporting the jwt auth metrics
    # must be done before the cluster shuts down since it calls to the coordinator
    sleep(5)
    self.__assert_success_fail_metric(failure_count_min=4, failure_count_max=4)

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
    assert "Error connecting: HttpError" in result.stderr
    assert "HTTP code 401: Unauthorized" in result.stderr
    assert "Not connected to Impala, could not execute queries." in result.stderr

  def __assert_success_fail_metric(self, success_count_min=0, success_count_max=0,
                                   failure_count_min=0, failure_count_max=0):
    """Impala emits metrics that count the number of successful and failed JWT
    authentications. This function asserts the JWT auth success/fail counters from the
    coordinator are within the specified ranges."""
    self.__assert_counter(
      "impala.thrift-server.hiveserver2-http-frontend.total-jwt-token-auth-success",
      success_count_min, success_count_max)
    self.__assert_counter(
      "impala.thrift-server.hiveserver2-http-frontend.total-jwt-token-auth-failure",
      failure_count_min, failure_count_max)

  def __assert_counter(self, counter_name, expected_count_min, expected_count_max):
    """Asserts the value of the specifed counter metric from the coordinator falls
    within the specified min and max (inclusive)."""
    counter_val = self.cluster.impalads[0].service.get_metric_value(counter_name)

    assert counter_val >= expected_count_min and counter_val <= expected_count_max, \
           "expected counter '{0}' to have a value between '{1}' and '{2}' inclusive " \
           "but its value was {3}" \
          .format(counter_name, expected_count_min, expected_count_max, counter_val)
