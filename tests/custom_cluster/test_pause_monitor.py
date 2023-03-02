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
import signal
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestPauseMonitor(CustomClusterTestSuite):
  """Class for pause monitor tests."""

  @CustomClusterTestSuite.with_args("--logbuflevel=-1")
  def test_jvm_pause_monitor_logs_entries(self):
    """This test injects a non-GC pause and confirms that that the JVM pause
    monitor detects and logs it."""
    impalad = self.cluster.get_first_impalad()
    # Send a SIGSTOP for the process and block it for 5s.
    impalad.kill(signal.SIGSTOP)
    time.sleep(5)
    impalad.kill(signal.SIGCONT)
    # Wait for over a second for the cache metrics to expire.
    time.sleep(2)
    # Check that the pause is detected.
    self.assert_impalad_log_contains('INFO', "Detected pause in JVM or host machine")
    # Check that the metrics we have for this updated as well
    assert impalad.service.get_metric_value("jvm.gc_num_info_threshold_exceeded") > 0
    assert impalad.service.get_metric_value("jvm.gc_total_extra_sleep_time_millis") > 0
