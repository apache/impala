# Copyright (c) 2016 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging
import os
import pytest
import signal
import socket
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_service import ImpaladService
from tests.shell.util import run_impala_shell_cmd, ImpalaShell

class TestClientSsl(CustomClusterTestSuite):
  """Tests for a client using SSL (particularly, the Impala Shell) """

  CERT_DIR = "%s/be/src/testutil" % os.environ['IMPALA_HOME']

  SSL_ENABLED = "SSL is enabled"
  CONNECTED = "Connected to"
  FETCHED = "Fetched 1 row"

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--ssl_server_certificate=%s/server-cert.pem "
                                    "--ssl_private_key=%s/server-key.pem"
                                    % (CERT_DIR, CERT_DIR))
  def test_ssl(self, vector):
    # TODO: This is really two different tests, but the custom cluster takes too long to
    # start. Make it so that custom clusters can be specified across test suites.
    result = run_impala_shell_cmd("--ssl --ca_cert=%s/server-cert.pem -q 'select 1 + 2'"
                                  % self.CERT_DIR)
    for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
      assert msg in result.stderr

    # No certificate checking: will accept any cert.
    result = run_impala_shell_cmd("--ssl -q 'select 1 + 2'")
    for msg in [self.SSL_ENABLED, self.CONNECTED, self.FETCHED]:
      assert msg in result.stderr

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
