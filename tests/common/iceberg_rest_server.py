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
import subprocess
import signal
import socket
import sys
import time

REST_SERVER_PORT = 9084
IMPALA_HOME = os.environ['IMPALA_HOME']
LOG = logging.getLogger('impala_test_suite')


class IcebergRestServer(object):
  """
  Utility class for starting and stopping our minimal Iceberg REST server.
  """

  def start_rest_server(self, timeout_s):
    self.process = subprocess.Popen('testdata/bin/run-iceberg-rest-server.sh',
        stdout=sys.stdout, stderr=sys.stderr, shell=True,
        preexec_fn=os.setsid, cwd=IMPALA_HOME)
    self._wait_for_rest_server_to_start(timeout_s)

  def stop_rest_server(self, timeout_s):
    if self.process:
      os.killpg(self.process.pid, signal.SIGTERM)
      self._wait_for_rest_server_to_be_killed(timeout_s)

  def _wait_for_rest_server_to_start(self, timeout_s):
    sleep_interval_s = 0.5
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      if s.connect_ex(('localhost', REST_SERVER_PORT)) == 0:
        LOG.info("Iceberg REST server is available.")
        return
      s.close()
      time.sleep(sleep_interval_s)
    raise Exception(
        "Webserver did not become available within {} seconds.".format(timeout_s))

  def _wait_for_rest_server_to_be_killed(self, timeout_s):
    sleep_interval_s = 0.5
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      if s.connect_ex(('localhost', REST_SERVER_PORT)) != 0:
        LOG.info("Iceberg REST server has stopped.")
        return
      s.close()
      time.sleep(sleep_interval_s)
    # Let's not throw an exception as this is typically invoked during cleanup, and we
    # want the rest of the cleanup code to be executed.
    LOG.info("Iceberg REST server hasn't stopped in time.")
