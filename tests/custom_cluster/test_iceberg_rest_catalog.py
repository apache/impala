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
import socket
import time
import pytest
import signal
import subprocess
import sys

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

REST_SERVER_PORT = 9084
IMPALA_HOME = os.environ['IMPALA_HOME']
START_ARGS = 'start_args'
IMPALAD_ARGS = """--use_local_catalog=true --catalogd_deployed=false
    --catalog_config_dir={}/testdata/configs/catalog_configs/iceberg_rest_config"""\
        .format(IMPALA_HOME)


class TestIcebergRestCatalog(CustomClusterTestSuite):

  def setup_method(self, method):
    # Invoke start-impala-cluster.py with '--no_catalogd'
    start_args = "--no_catalogd"
    if START_ARGS in method.__dict__:
      start_args = method.__dict__[START_ARGS] + " " + start_args
    method.__dict__[START_ARGS] = start_args

    try:
      self._start_rest_server()
      self._wait_for_rest_server(300)
      super(TestIcebergRestCatalog, self).setup_method(method)
    except Exception as e:
      self._stop_rest_server()
      raise e

  def teardown_method(self, method):
    self._stop_rest_server()
    super(TestIcebergRestCatalog, self).teardown_method(method)

  def _start_rest_server(self):
    self.process = subprocess.Popen(
        'testdata/bin/run-iceberg-rest-server.sh',
        stdout=sys.stdout, stderr=sys.stderr, shell=True,
        preexec_fn=os.setsid, cwd=IMPALA_HOME)

  def _stop_rest_server(self):
    if self.process:
      os.killpg(self.process.pid, signal.SIGTERM)

  def _wait_for_rest_server(self, timeout_s):
    sleep_interval_s = 0.5
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
      try:
        if s.connect_ex(('localhost', REST_SERVER_PORT)) == 0:
          print("Iceberg REST server is available.")
          return
      finally:
        s.close()
      time.sleep(sleep_interval_s)
    raise Exception(
        "Webserver did not become available within {} seconds.".format(timeout_s))

  @CustomClusterTestSuite.with_args(
     impalad_args=IMPALAD_ARGS)
  @pytest.mark.execute_serially
  def test_rest_catalog_basic(self, vector):
    self.run_test_case('QueryTest/iceberg-rest-catalog', vector, use_db="ice")
