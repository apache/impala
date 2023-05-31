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
import ssl
import sys
import time

# This import is the actual ImpalaShell class from impala_shell.py.
# We rename it to ImpalaShellClass here because we later import another
# class called ImpalaShell from tests/shell/util.py, and we don't want
# to mask it.
from shell.impala_shell import ImpalaShell as ImpalaShellClass

from tests.common.environ import IS_REDHAT_DERIVATIVE
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_vector import ImpalaTestVector
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.shell.util import ImpalaShell

REQUIRED_MIN_OPENSSL_VERSION = 0x10001000
# Python supports TLSv1.2 from 2.7.9 officially but on Red Hat/CentOS Python2.7.5
# with newer python-libs (eg python-libs-2.7.5-77) supports TLSv1.2 already
if IS_REDHAT_DERIVATIVE:
  REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12 = (2, 7, 5)
else:
  REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12 = (2, 7, 9)
_openssl_version_number = getattr(ssl, "OPENSSL_VERSION_NUMBER", None)
if _openssl_version_number is None:
  SKIP_SSL_MSG = "Legacy OpenSSL module detected"
elif _openssl_version_number < REQUIRED_MIN_OPENSSL_VERSION:
  SKIP_SSL_MSG = "Only have OpenSSL version %X, but test requires %X" % (
    ssl.OPENSSL_VERSION_NUMBER, REQUIRED_MIN_OPENSSL_VERSION)
else:
  SKIP_SSL_MSG = None
CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']

SSL_ARGS = ("--ssl_client_ca_certificate=%s/server-cert.pem "
            "--ssl_server_certificate=%s/server-cert.pem "
            "--ssl_private_key=%s/server-key.pem "
            "--hostname=localhost "  # Required to match hostname in certificate
            % (CERT_DIR, CERT_DIR, CERT_DIR))

# IMPALA-12114 was an issue that occurred when the number of idle polls exceeded a limit
# and lead to disconnection. This uses a very short poll period to make these tests
# do more polls and verify the fix for this issue.
IDLE_ARGS = " --idle_client_poll_period_s=1 -v=2"


class TestThriftSocket(CustomClusterTestSuite):
  """ Check if thrift timeout errors are detected properly """

  @classmethod
  def setup_class(cls):
    if sys.version_info < REQUIRED_MIN_PYTHON_VERSION_FOR_TLSV12:
      pytest.skip("Python version does not support tls 1.2")
    super(TestThriftSocket, cls).setup_class()

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=IDLE_ARGS, cluster_size=1)
  def test_peek_timeout_no_ssl(self):
    # Iterate over test vector within test function to avoid restarting cluster.
    for protocol_dim in create_client_protocol_dimension():
      for vector in [ImpalaTestVector([protocol_dim])]:
        shell_args = ["-Q", "idle_session_timeout=1800"]
        # This uses a longer idle time to verify IMPALA-12114, see comment above.
        self._run_idle_shell(vector, shell_args, 12)
    self.assert_impalad_log_contains('INFO',
        r'Socket read or peek timeout encountered.*THRIFT_EAGAIN \(timed out\)',
        expected_count=-1)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(statestored_args=SSL_ARGS,
                                    catalogd_args=SSL_ARGS,
                                    impalad_args=(SSL_ARGS + IDLE_ARGS),
                                    cluster_size=1)
  def test_peek_timeout_ssl(self):
    # Iterate over test vector within test function to avoid restarting cluster.
    for protocol_dim in create_client_protocol_dimension():
      for vector in [ImpalaTestVector([protocol_dim])]:
        shell_args = ["-Q", "idle_session_timeout=1800", "--ssl"]
        # This uses a longer idle time to verify IMPALA-12114, see comment above.
        self._run_idle_shell(vector, shell_args, 12)
    self.assert_impalad_log_contains('INFO',
        r'Socket read or peek timeout encountered.*THRIFT_POLL \(timed out\)',
        expected_count=-1)

  def _run_idle_shell(self, vector, args, idle_time):
    p = ImpalaShell(vector, args)
    p.send_cmd("USE functional")
    # Running different statements before and after idle allows for more
    # precise asserts that can distinguish output from before vs after.
    p.send_cmd("SHOW DATABASES")
    time.sleep(idle_time)
    p.send_cmd("SHOW TABLES")

    result = p.get_result()
    # Output from before the idle period
    assert "functional_parquet" in result.stdout, result.stdout
    # Output from after the idle period
    assert "alltypesaggmultifilesnopart" in result.stdout, result.stdout
    # Impala shell reconnects automatically, so we need to be sure that
    # it didn't lose the connection.
    assert ImpalaShellClass.CONNECTION_LOST_MESSAGE not in result.stderr, result.stderr
