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
import logging
import os
import pytest
import re
import subprocess
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.common.test_vector import ImpalaTestVector
from tests.shell.util import run_impala_shell_cmd, get_impalad_port

LOG = logging.getLogger('test_client_keepalive')


class TestClientKeepalive(CustomClusterTestSuite):
  """Tests for enabling server-side keepalive for client connections.
     The mechanism is slightly different SSL and non-SSL, so this tests both."""

  CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']
  SSL_ARGS = ("--ssl_client_ca_certificate=%s/server-cert.pem "
              "--ssl_server_certificate=%s/server-cert.pem "
              "--ssl_private_key=%s/server-key.pem "
              "--hostname=localhost "  # Required to match hostname in certificate
              % (CERT_DIR, CERT_DIR, CERT_DIR))

  KEEPALIVE_ARGS = ("--client_keepalive_probe_period_s=600")

  def get_ss_command(self):
    # HACK: Most systems have ss on the PATH, but sometimes the PATH is misconfigured
    # while ss is still available in /usr/sbin. This tries the PATH and then falls back
    # to trying /usr/sbin/ss.
    possible_ss_commands = ['ss', '/usr/sbin/ss']
    with open(os.devnull, "w") as devnull:
      for ss_command in possible_ss_commands:
        try:
          retcode = subprocess.call([ss_command], stdout=devnull, stderr=devnull)
          LOG.info("{0} returns {1}".format(ss_command, retcode))
          if retcode == 0:
            return ss_command
        except Exception as e:
          LOG.info(e)
          pass

    raise Exception("No valid ss executable. Tried: {0}".format(possible_ss_commands))

  def check_keepalive(self, vector, ssl):
    ss = self.get_ss_command()
    impalad_port = get_impalad_port(vector)
    # Sleep 1 second to make sure the connection is idle, then use the ss utility
    # to print information about keepalive.
    # -H disables the header
    # -t limits it to TCP connections
    # -o prints the timer information which includes keepalive
    # -n uses numeric addresses to avoid DNS lookups
    # sport X - limit to connections for the impalad port that we are using
    ss_command = "sleep 1 && {0} -Hton sport = {1}".format(ss, impalad_port)
    LOG.info("Command: {0}".format(ss_command))
    shell_options = ["-q", "shell {0}".format(ss_command)]
    if ssl:
      shell_options.append("--ssl")
    result = run_impala_shell_cmd(vector, shell_options)
    LOG.info("STDOUT: {0} STDERR: {1}".format(result.stdout, result.stderr))

    # The message is of the form "timer:(keepalive,$TIME,$NUM_RETRIES)"
    # e.g. "timer:(keepalive,9min58sec,0)" or "timer:(keepalive,10min,0)"
    KEEPALIVE_REGEX = r"timer:\(keepalive,([0-9]+)min([0-9]+sec)?,([0-9])\)"
    match = re.search(KEEPALIVE_REGEX, result.stdout)
    assert match, "Could not find keepalive information in {0}".format(result.stdout)
    num_minutes = int(match.group(1))
    num_retries = int(match.group(3))
    assert num_minutes == 9 or num_minutes == 10
    assert num_retries == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=(SSL_ARGS + KEEPALIVE_ARGS),
                                    statestored_args=SSL_ARGS,
                                    catalogd_args=SSL_ARGS)
  def test_ssl_keepalive(self, vector):
    # Keepalive applies to all client ports / protocols, so test all protocols
    # Iterate over test vector within test function to avoid restart cluster
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      self.check_keepalive(vector, ssl=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=KEEPALIVE_ARGS)
  def test_nonssl_keepalive(self, vector):
    # Keepalive applies to all client ports / protocols, so test all protocols
    # Iterate over test vector within test function to avoid restart cluster
    for vector in\
        [ImpalaTestVector([value]) for value in create_client_protocol_dimension()]:
      self.check_keepalive(vector, ssl=False)
