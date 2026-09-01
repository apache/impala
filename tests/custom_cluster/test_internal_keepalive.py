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

import logging
import re
import subprocess
import time
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_cluster import (DEFAULT_STATE_STORE_SUBSCRIBER_PORT)

LOG = logging.getLogger('test_internal_keepalive')

INTERNAL_KEEPALIVE_ENABLED_ARGS = '--internal_keepalive_probe_period_s=1'
INTERNAL_KEEPALIVE_DISABLED_ARGS = '--internal_keepalive_probe_period_s=0'
SLOW_STATESTORE_ARGS = ('--statestore_update_frequency_ms=5000 '
                        '--statestore_priority_update_frequency_ms=5000 '
                        '--statestore_heartbeat_frequency_ms=5000')
SS_POLL_ATTEMPTS = 20
SS_POLL_INTERVAL_S = 0.25
KEEPALIVE_RE = re.compile(r"timer:\(keepalive,[^)]+\)")


class TestInternalKeepalive(CustomClusterTestSuite):
  """IMPALA-15257: verify that TCP keepalive is applied to internal cluster
  Thrift connections. The impalad<->statestored connection (port: 24000) is a
  long-lived internal connection, so we sample it with 'ss -Htno' while the
  connection is idle(heartbeats slowed to 5s) and check for the kernel's
  keepalive timer."""

  def _ss_lines_for_port(self, ss_cmd, port):
    """Return the lines from ss output that match the given port."""
    cmd = [ss_cmd, "-Htno", "( sport = :{0} or dport = :{0} )".format(port)]
    out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode()
    return [line for line in out.splitlines() if line.strip()]

  def _test_internal_keepalive(self, enabled):
    """Poll ss and return once we see the expected state,
    or fail after SS_POLL_ATTEMPTS."""

    ss = self.get_ss_command()
    lines = None
    for i in range(SS_POLL_ATTEMPTS):
      lines = self._ss_lines_for_port(ss, DEFAULT_STATE_STORE_SUBSCRIBER_PORT)

      assert lines, ("No 'ss' output for statestore port {0}. internal Thrift "
                     "connection missing".format(DEFAULT_STATE_STORE_SUBSCRIBER_PORT))
      any_keepalive = any(KEEPALIVE_RE.search(line) for line in lines)
      if enabled and any_keepalive:
        LOG.info("Found keepalive in ss output: {0}".format(lines))
        return
      if not enabled:
        if any_keepalive:
          raise Exception(
            "Found keepalive in ss output when it should be disabled: {0}".format(lines))
        else:
          # Do a few extra polls to be sure that a keepalive timer isn't hiding
          # The disabled case must be quiet across the whole window.
          if i >= 8:
            LOG.info("No keepalive in ss output: {0}".format(lines))
            return
      time.sleep(SS_POLL_INTERVAL_S)
    assert False, (
      "Expected presence of keepalive timer on port {0} but polling exhausted. Last ss "
      "output:\n{1}".format(DEFAULT_STATE_STORE_SUBSCRIBER_PORT, "\n".join(lines or [])))

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=INTERNAL_KEEPALIVE_ENABLED_ARGS,
                                    statestored_args=INTERNAL_KEEPALIVE_ENABLED_ARGS
                                    + " " + SLOW_STATESTORE_ARGS,
                                    catalogd_args=INTERNAL_KEEPALIVE_ENABLED_ARGS)
  def test_internal_keepalive_enabled(self, vector):  # noqa: U100
    """With keepalive enabled, verify that the internal Thrift connection has a
     keepalive timer."""
    self._test_internal_keepalive(enabled=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args=INTERNAL_KEEPALIVE_DISABLED_ARGS,
                                    statestored_args=INTERNAL_KEEPALIVE_DISABLED_ARGS
                                    + " " + SLOW_STATESTORE_ARGS,
                                    catalogd_args=INTERNAL_KEEPALIVE_DISABLED_ARGS)
  def test_internal_keepalive_disabled(self, vector):  # noqa: U100
    """With keepalive disabled, verify that the internal Thrift connection does not
    check_keepalive a keepalive timer."""
    self._test_internal_keepalive(enabled=False)
