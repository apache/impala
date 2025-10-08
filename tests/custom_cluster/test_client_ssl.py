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

from __future__ import absolute_import, division, print_function
import json
import logging
import os
import pytest
import re
import requests
import signal
import socket
import sys
import time


from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_service import ImpaladService
from tests.common.network import SKIP_SSL_MSG, REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.common.test_vector import BEESWAX
from tests.shell.util import run_impala_shell_cmd, run_impala_shell_cmd_no_expect, \
    ImpalaShell, create_impala_shell_executable_dimension


CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']

# Due to THRIFT-792, SSL errors are suppressed when using OpenSSL hostname verification.
# This is the only option on Python 3.12+, using ssl.PROTOCOL_TLS_CLIENT.
CERT_ERR = ["doesn't match", "certificate verify failed", "Could not connect"]


class TestClientSsl(CustomClusterTestSuite):
  """Tests for a client using SSL (particularly, the Impala Shell) """

  SSL_ENABLED = "SSL is enabled"
  CONNECTED = "Connected to"
  FETCHED = "Fetched 1 row"
  SAN_UNSUPPORTED_ERROR = ("Certificate error with remote host: hostname "
                          "'localhost' doesn't match u'badCN'")

  # Deprecation warnings should not be seen.
  DEPRECATION_WARNING = "DeprecationWarning"

  SSL_WILDCARD_ARGS = ("--ssl_client_ca_certificate={0}/wildcardCA.pem "
                       "--ssl_server_certificate={0}/wildcard-cert.pem "
                       "--ssl_private_key={0}/wildcard-cert.key "
                       "--hostname={1} "
                       "--state_store_host={1} "
                       "--catalog_service_host={1} "
                       ).format(CERT_DIR, "ip4.impala.test")

  SSL_WILDCARD_SAN_ARGS = ("--ssl_client_ca_certificate={0}/wildcardCA.pem "
                           "--ssl_server_certificate={0}/wildcard-san-cert.pem "
                           "--ssl_private_key={0}/wildcard-san-cert.key "
                           "--hostname={1} "
                           "--state_store_host={1} "
                           "--catalog_service_host={1} "
                           ).format(CERT_DIR, "ip4.impala.test")

  SSL_ARGS = ("--ssl_client_ca_certificate={0}/server-cert.pem "
              "--ssl_server_certificate={0}/server-cert.pem "
              "--ssl_private_key={0}/server-key.pem "
              "--hostname=localhost " # Required to match hostname in certificate
              ).format(CERT_DIR)

  @classmethod
  def setup_class(cls):
    if sys.version_info < REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12:
      pytest.skip("Python version does not support tls 1.2")
    super(TestClientSsl, cls).setup_class()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_ARGS, statestored_args=SSL_ARGS,
                                    catalogd_args=SSL_ARGS)
  def test_ssl(self, vector):
    self._verify_negative_cases(vector, "%s/server-cert.pem" % CERT_DIR)
    # TODO: This is really two different tests, but the custom cluster takes too long to
    # start. Make it so that custom clusters can be specified across test suites.
    self._validate_positive_cases(vector, "%s/server-cert.pem" % CERT_DIR)

    # No certificate checking: will accept any cert.
    self._validate_positive_cases(vector, )

    # Test cancelling a query
    impalad = ImpaladService(socket.getfqdn())
    assert impalad.wait_for_num_in_flight_queries(0)
    impalad.wait_for_metric_value('impala-server.backend-num-queries-executing', 0)
    p = ImpalaShell(vector, args=["--ssl"])
    p.send_cmd("SET DEBUG_ACTION=0:OPEN:WAIT")
    p.send_cmd("select count(*) from functional.alltypes")
    # Wait until the query has been planned and started executing, at which point it
    # should be cancellable.
    impalad.wait_for_metric_value('impala-server.backend-num-queries-executing', 1,
            timeout=60)

    LOG = logging.getLogger('test_client_ssl')
    LOG.info("Cancelling query")
    num_tries = 0
    # In practice, sending SIGINT to the shell process doesn't always seem to get caught
    # (and a search shows up some bugs in Python where SIGINT might be ignored). So retry
    # for 30s until one signal takes.
    while impalad.get_num_in_flight_queries() == 1:
      time.sleep(1)
      LOG.info("Sending signal...")
      os.kill(p.pid(), signal.SIGINT)
      num_tries += 1
      assert num_tries < 30, ("SIGINT was not caught by shell within 30s. Queries: " +
              json.dumps(impalad.get_queries_json(), indent=2))

    p.send_cmd("profile")
    result = p.get_result()

    print(result.stderr)
    assert "Query Status: Cancelled" in result.stdout
    assert impalad.wait_for_num_in_flight_queries(0)
    self.check_connections()

  WEBSERVER_SSL_ARGS = ("--webserver_certificate_file=%(cert_dir)s/server-cert.pem "
                        "--webserver_private_key_file=%(cert_dir)s/server-key.pem "
                        "--hostname=localhost "  # Must match hostname in certificate
                        "--webserver_interface=localhost "
                        % {'cert_dir': CERT_DIR})

  @classmethod
  def add_test_dimensions(cls):
    super(TestClientSsl, cls).add_test_dimensions()
    # Limit the test dimensions to avoid long run times. This runs hs2 and hs2-http
    # with only the dev shells (python2 and python3) for a total of up to 4
    # dimensions.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_impala_shell_executable_dimension(dev_only=True))
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') != BEESWAX)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=WEBSERVER_SSL_ARGS,
                                    statestored_args=WEBSERVER_SSL_ARGS,
                                    catalogd_args=WEBSERVER_SSL_ARGS)
  def test_webserver_ssl(self):
    "Tests that the debug web pages are reachable when run with ssl."
    self._verify_ssl_webserver()

  # Test that the shell can connect to a ECDH only cluster.
  TLS_ECDH_ARGS = ("--ssl_client_ca_certificate=%(cert_dir)s/server-cert.pem "
                   "--ssl_server_certificate=%(cert_dir)s/server-cert.pem "
                   "--ssl_private_key=%(cert_dir)s/server-key.pem "
                   "--hostname=localhost "  # Must match hostname in certificate
                   "--ssl_cipher_list=ECDHE-RSA-AES128-GCM-SHA256 "
                   "--webserver_certificate_file=%(cert_dir)s/server-cert.pem "
                   "--webserver_private_key_file=%(cert_dir)s/server-key.pem "
                   "--webserver_interface=localhost "
                   % {'cert_dir': CERT_DIR})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=TLS_ECDH_ARGS,
                                    statestored_args=TLS_ECDH_ARGS,
                                    catalogd_args=TLS_ECDH_ARGS)
  @pytest.mark.skipif(SKIP_SSL_MSG is not None, reason=SKIP_SSL_MSG)
  def test_tls_ecdh(self, vector):
    self._verify_negative_cases(vector, "%s/server-cert.pem" % CERT_DIR)
    self._validate_positive_cases(vector, "%s/server-cert.pem" % CERT_DIR)
    self._verify_ssl_webserver()

  # Sanity check that the shell can still connect when we set a min ssl version of 1.0
  TLS_V10_ARGS = ("--ssl_client_ca_certificate=%s/server-cert.pem "
                  "--ssl_server_certificate=%s/server-cert.pem "
                  "--ssl_private_key=%s/server-key.pem "
                  "--hostname=localhost " # Required to match hostname in certificate"
                  "--ssl_minimum_version=tlsv1"
                  % (CERT_DIR, CERT_DIR, CERT_DIR))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=TLS_V10_ARGS,
                                    statestored_args=TLS_V10_ARGS,
                                    catalogd_args=TLS_V10_ARGS)
  @pytest.mark.skipif(SKIP_SSL_MSG is not None, reason=SKIP_SSL_MSG)
  def test_tls_v10(self, vector):
    self._validate_positive_cases(vector, "%s/server-cert.pem" % CERT_DIR)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_WILDCARD_ARGS,
                                    statestored_args=SSL_WILDCARD_ARGS,
                                    catalogd_args=SSL_WILDCARD_ARGS)
  @pytest.mark.skipif(SKIP_SSL_MSG is not None, reason=SKIP_SSL_MSG)
  def test_wildcard_ssl(self, vector):
    """ Test for IMPALA-3159: Test with a certificate which has a wildcard for the
    CommonName.
    """
    self._verify_negative_cases(vector, "%s/server-cert.pem" % CERT_DIR,
                                host="ip4.impala.test")

    self._validate_positive_cases(vector, "%s/wildcardCA.pem" % CERT_DIR,
                                  host="ip4.impala.test")
    self.check_connections()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_WILDCARD_SAN_ARGS,
                                    statestored_args=SSL_WILDCARD_SAN_ARGS,
                                    catalogd_args=SSL_WILDCARD_SAN_ARGS)
  @pytest.mark.skipif(SKIP_SSL_MSG is not None, reason=SKIP_SSL_MSG)
  def test_wildcard_san_ssl(self, vector):
    """ Test for IMPALA-3159: Test with a certificate which has a wildcard as a SAN. """
    # This block of code is the same as _validate_positive_cases() but we want to check
    # if retrieving the SAN is supported first.
    args = ["--ssl", "-q", "select 1 + 2", "--ca_cert=%s/wildcardCA.pem" % CERT_DIR]
    result = run_impala_shell_cmd_no_expect(vector, args)
    if self.SAN_UNSUPPORTED_ERROR in result.stderr:
      pytest.xfail("Running with a RHEL/Python combination that has a bug where Python "
          "cannot retrieve SAN from certificate: "
          "https://bugzilla.redhat.com/show_bug.cgi?id=928390")

    self._verify_negative_cases(vector, "%s/server-cert.pem" % CERT_DIR,
                                host="ip4.impala.test")

    self._validate_positive_cases(vector, "%s/wildcardCA.pem" % CERT_DIR,
                                  host="ip4.impala.test")
    self.check_connections()

  def _is_cert_error(self, stderr):
    for err in CERT_ERR:
      if err in stderr:
        return True
    return False

  def _verify_negative_cases(self, vector, ca_cert, host=""):
    # Expect the shell to not start successfully if we connect to an endpoint that
    # doesn't match the certificate.
    invalid_host = "localhost" if host else "127.0.0.1"
    args = ["--ssl", "-q", "select 1 + 2", "-i", invalid_host, "--ca_cert=%s" % ca_cert]
    result = run_impala_shell_cmd(vector, args, expect_success=False)
    assert self._is_cert_error(result.stderr), result.stderr

    # Expect the shell to not start successfully if we point --ca_cert to an incorrect
    # certificate.
    args = ["--ssl", "-q", "select 1 + 2",
            "--ca_cert=%s/incorrect-commonname-cert.pem" % CERT_DIR]
    if host:
      args.extend(["-i", host])
    run_impala_shell_cmd(vector, args, expect_success=False)

    # Expect the shell to not start successfully if we don't specify the --ssl option
    args = ["-q", "select 1 + 2"]
    if host:
      args.extend(["-i", host])
    run_impala_shell_cmd(vector, args, expect_success=False)

  def _validate_positive_cases(self, vector, ca_cert="", host=None):
    python3_10_version_re = re.compile(r"using Python 3\.1[0-9]")
    shell_options = ["--ssl", "-q", "select 1 + 2"]
    if host:
      shell_options.extend(["-i", host])
    result = run_impala_shell_cmd(vector, shell_options)
    for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
      assert msg in result.stderr
    # Python >3.10 has deprecated ssl.PROTOCOL_TLS and impala-shell currently emits a
    # DeprecationWarning for that version. As a temporary workaround, this skips the
    # assert about deprecation for Python 3.10 or above. This can be removed when
    # IMPALA-12219 is fixed.
    # Note: This is the version that impala-shell uses, not the version pytests uses.
    if not python3_10_version_re.search(result.stderr):
      assert self.DEPRECATION_WARNING not in result.stderr

    if ca_cert != "":
      shell_options = shell_options + ["--ca_cert=%s" % ca_cert]
      result = run_impala_shell_cmd(vector, shell_options)
      for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
        assert msg in result.stderr
      if not python3_10_version_re.search(result.stderr):
        assert self.DEPRECATION_WARNING not in result.stderr

  def _verify_ssl_webserver(self):
    for port in ["25000", "25010", "25020"]:
      url = "https://localhost:%s" % port
      response = requests.get(url, verify="%s/server-cert.pem" % CERT_DIR)
      assert response.status_code == requests.codes.ok, url


# Run when the python version is too low to support TLS 1.2, to check that impala-shell
# returns the expected warning.
class TestClientSslUnsupported(CustomClusterTestSuite):
  @classmethod
  def setup_class(cls):
    if sys.version_info >= REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12:
      pytest.skip("This test is only run with older versions of python")
    super(TestClientSslUnsupported, cls).setup_class()

  SSL_ARGS = ("--ssl_client_ca_certificate=%s/server-cert.pem "
              "--ssl_server_certificate=%s/server-cert.pem "
              "--ssl_private_key=%s/server-key.pem "
              "--hostname=localhost "  # Required to match hostname in certificate
              % (CERT_DIR, CERT_DIR, CERT_DIR))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_ARGS,
                                    statestored_args=SSL_ARGS,
                                    catalogd_args=SSL_ARGS)
  @pytest.mark.skipif(SKIP_SSL_MSG is not None, reason=SKIP_SSL_MSG)
  def test_shell_warning(self, vector):
    result = run_impala_shell_cmd_no_expect(vector, ["--ssl", "-q", "select 1 + 2"])
    assert "Warning: TLSv1.2 is not supported for Python < 2.7.9" in result.stderr, \
      result.stderr
