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
# Verifier for common impalad metrics

from __future__ import absolute_import, division, print_function
import logging
from time import time, sleep

LOG = logging.getLogger('test_verify_metrics')
LOG.setLevel(level=logging.DEBUG)

# List of metrics that should be equal to zero when there are no outstanding queries.
METRIC_LIST = [
               "impala-server.num-queries-registered",
               "impala-server.num-fragments-in-flight",
               "impala-server.io-mgr.num-open-files",
               "impala-server.num-files-open-for-insert",
               "impala-server.scan-ranges.num-missing-volume-id",
               # Buffer pool pages belong to specific queries. Therefore there should be
               # no clean pages if there are no queries running.
               "buffer-pool.clean-pages",
               "buffer-pool.clean-page-bytes",
               "impala-server.num-open-beeswax-sessions",
               "impala-server.num-open-hiveserver2-sessions",
               "tmp-file-mgr.scratch-space-bytes-used",
               "tmp-file-mgr.scratch-space-bytes-used.dir-0"]

class MetricVerifier(object):
  """Reuseable class that can verify common metrics"""
  def __init__(self, impalad_service):
    """Initialize module given an ImpalaService object"""
    self.impalad_service = impalad_service

  def verify_metrics_are_zero(self, timeout=60):
    """Test that all the metric in METRIC_LIST are 0"""
    for metric in METRIC_LIST:
      self.wait_for_metric(metric, 0, timeout)

  def verify_no_open_files(self, timeout=60):
    """Tests there are no open files"""
    self.wait_for_metric("impala-server.num-files-open-for-insert", 0, timeout)
    self.wait_for_metric("impala-server.io-mgr.num-open-files", 0, timeout)

  def verify_num_unused_buffers(self, timeout=60):
    """Test that all buffers are unused"""
    buffers =\
        self.impalad_service.get_metric_value("impala-server.io-mgr.num-buffers")
    self.wait_for_metric("impala-server.io-mgr.num-unused-buffers", buffers,
        timeout)

  def wait_for_metric(self, metric_name, expected_value, timeout=60):
    self.impalad_service.wait_for_metric_value(metric_name, expected_value, timeout)

  def wait_for_backend_admission_control_state(self, timeout=60):
    """Wait for the admission-control-related values on the /backends page to go to
    zero (i.e. consistent with an idle cluster)."""
    start_time = time()
    while time() - start_time < timeout:
      try:
        self.__assert_backend_ac_value_are_zero()
        break  # Success!
      except AssertionError as e:
        LOG.info("Not yet quiesced: %s", str(e))
        sleep(0.1)

  def __assert_backend_ac_value_are_zero(self):
    response = self.impalad_service.get_debug_webpage_json("/backends")
    for backend in response['backends']:
      for key in ['num_admitted', 'admission_slots_in_use', 'mem_admitted']:
        assert str(backend[key]) == '0'
