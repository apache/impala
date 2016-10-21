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

import logging
import os
import pytest
import signal
import socket
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_service import ImpaladService
from tests.shell.util import run_impala_shell_cmd, run_impala_shell_cmd_no_expect, \
    ImpalaShell

class TestClientSsl(CustomClusterTestSuite):
  """Tests for a client using SSL (particularly, the Impala Shell) """

  CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']

  SSL_ENABLED = "SSL is enabled"
  CONNECTED = "Connected to"
  FETCHED = "Fetched 1 row"
  SAN_UNSUPPORTED_ERROR = ("Certificate error with remote host: hostname "
                          "'localhost' doesn't match u'badCN'")

  SSL_WILDCARD_ARGS = ("--ssl_client_ca_certificate=%s/wildcardCA.pem "
                      "--ssl_server_certificate=%s/wildcard-cert.pem "
                      "--ssl_private_key=%s/wildcard-cert.key"
                      % (CERT_DIR, CERT_DIR, CERT_DIR))

  SSL_WILDCARD_SAN_ARGS = ("--ssl_client_ca_certificate=%s/wildcardCA.pem "
                          "--ssl_server_certificate=%s/wildcard-san-cert.pem "
                          "--ssl_private_key=%s/wildcard-san-cert.key"
                          % (CERT_DIR, CERT_DIR, CERT_DIR))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--ssl_server_certificate=%s/server-cert.pem "
                                    "--ssl_private_key=%s/server-key.pem"
                                    % (CERT_DIR, CERT_DIR))
  def test_ssl(self, vector):

    self._verify_negative_cases()
    # TODO: This is really two different tests, but the custom cluster takes too long to
    # start. Make it so that custom clusters can be specified across test suites.
    self._validate_positive_cases("%s/server-cert.pem" % self.CERT_DIR)

    # No certificate checking: will accept any cert.
    self._validate_positive_cases()

    # Test cancelling a query
    impalad = ImpaladService(socket.getfqdn())
    impalad.wait_for_num_in_flight_queries(0)
    p = ImpalaShell(args="--ssl")
    p.send_cmd("SET DEBUG_ACTION=0:OPEN:WAIT")
    p.send_cmd("select count(*) from functional.alltypes")
    impalad.wait_for_num_in_flight_queries(1)

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
      assert num_tries < 30, "SIGINT was not caught by shell within 30s"

    p.send_cmd("profile")
    result = p.get_result()

    print result.stderr
    assert result.rc == 0
    assert "Query Status: Cancelled" in result.stdout
    assert impalad.wait_for_num_in_flight_queries(0)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_WILDCARD_ARGS,
                                    statestored_args=SSL_WILDCARD_ARGS,
                                    catalogd_args=SSL_WILDCARD_ARGS)
  @pytest.mark.xfail(run=True, reason="IMPALA-4295 on Centos6")
  def test_wildcard_ssl(self, vector):
    """ Test for IMPALA-3159: Test with a certificate which has a wildcard for the
    CommonName.
    """
    self._verify_negative_cases()

    self._validate_positive_cases("%s/wildcardCA.pem" % self.CERT_DIR)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=SSL_WILDCARD_SAN_ARGS,
                                    statestored_args=SSL_WILDCARD_SAN_ARGS,
                                    catalogd_args=SSL_WILDCARD_SAN_ARGS)
  @pytest.mark.xfail(run=True, reason="IMPALA-4295")
  def test_wildcard_san_ssl(self, vector):
    """ Test for IMPALA-3159: Test with a certificate which has a wildcard as a SAN. """

    # This block of code is the same as _validate_positive_cases() but we want to check
    # if retrieving the SAN is supported first.
    args = "--ssl -q 'select 1 + 2' --ca_cert=%s/wildcardCA.pem" \
        % self.CERT_DIR
    result = run_impala_shell_cmd_no_expect(args)
    if self.SAN_UNSUPPORTED_ERROR in result.stderr:
      pytest.xfail("Running with a RHEL/Python combination that has a bug where Python "
          "cannot retrieve SAN from certificate: "
          "https://bugzilla.redhat.com/show_bug.cgi?id=928390")

    self._verify_negative_cases()

    self._validate_positive_cases("%s/wildcardCA.pem" % self.CERT_DIR)

  def _verify_negative_cases(self):
    # Expect the shell to not start successfully if we point --ca_cert to an incorrect
    # certificate.
    args = "--ssl -q 'select 1 + 2' --ca_cert=%s/incorrect-commonname-cert.pem" \
        % self.CERT_DIR
    run_impala_shell_cmd(args, expect_success=False)

    # Expect the shell to not start successfully if we don't specify the --ssl option
    args = "-q 'select 1 + 2'"
    run_impala_shell_cmd(args, expect_success=False)

  def _validate_positive_cases(self, ca_cert=""):
    shell_options = "--ssl -q 'select 1 + 2'"

    result = run_impala_shell_cmd(shell_options)
    for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
      assert msg in result.stderr

    if ca_cert != "":
      shell_options = shell_options + (" --ca_cert=%s" % ca_cert)
      result = run_impala_shell_cmd(shell_options)
      for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
        assert msg in result.stderr
