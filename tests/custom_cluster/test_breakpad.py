# Copyright 2016 Cloudera Inc.
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

import glob
import os
import pytest
import shutil
import tempfile
import time

from signal import SIGSEGV

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

DAEMONS = ['impalad', 'statestored', 'catalogd']
DAEMON_ARGS = ['impalad_args', 'state_store_args', 'catalogd_args']

class TestBreakpad(CustomClusterTestSuite):
  """Check that breakpad integration into the daemons works as expected. This includes
  writing minidump files on unhandled signals and rotating old minidumps on startup. The
  tests kill the daemons by sending a SIGSEGV signal.
  """
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip()
    # Override parent
    # The temporary directory gets removed in teardown_method() after each test.
    self.tmp_dir = tempfile.mkdtemp()

  def teardown_method(self, method):
    # Override parent
    # Stop the cluster to prevent future accesses to self.tmp_dir.
    self._stop_impala_cluster()
    assert self.tmp_dir
    shutil.rmtree(self.tmp_dir)

  @classmethod
  def teardown_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      return
    # Start default cluster for subsequent tests (verify_metrics).
    cls._start_impala_cluster([])

  def start_cluster(self):
    cluster_options = ["""--%s='-minidump_path=%s -max_minidumps=2'"""
                       % (arg, self.tmp_dir) for arg in DAEMON_ARGS]
    self._start_impala_cluster(cluster_options)

  def start_cluster_without_minidumps(self):
    cluster_options = ["""--%s='-minidump_path= -max_minidumps=2'"""
                       % arg for arg in DAEMON_ARGS]
    self._start_impala_cluster(cluster_options)

  def kill_cluster(self, signal):
    cluster = self.cluster
    for impalad in cluster.impalads:
      impalad.kill(signal)
    cluster.statestored.kill(signal)
    cluster.catalogd.kill(signal)
    # Wait for daemons to finish writing minidumps
    time.sleep(1)
    self.assert_all_processes_killed()

  def assert_all_processes_killed(self):
    self.cluster.refresh()
    assert not self.cluster.impalads
    assert not self.cluster.statestored
    assert not self.cluster.catalogd

  def count_minidumps(self, daemon):
    path = os.path.join(self.tmp_dir, daemon)
    return len(glob.glob("%s/*.dmp" % path))

  def count_all_minidumps(self):
    return sum((self.count_minidumps(daemon) for daemon in DAEMONS))

  def assert_num_logfile_entries(self, expected_count):
    self.assert_impalad_log_contains('INFO', 'Wrote minidump to ',
        expected_count=expected_count)
    self.assert_impalad_log_contains('ERROR', 'Wrote minidump to ',
        expected_count=expected_count)

  @pytest.mark.execute_serially
  def test_minidump_creation(self):
    """Check that when a daemon crashes it writes a minidump file."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    assert self.count_all_minidumps() == 0
    cluster_size = len(self.cluster.impalads)
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(1)
    assert self.count_minidumps('impalad') == cluster_size
    assert self.count_minidumps('statestored') == 1
    assert self.count_minidumps('catalogd') == 1

  @pytest.mark.execute_serially
  def test_minidump_cleanup(self):
    """Check that a limited number of minidumps is preserved during startup."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(1)
    self.start_cluster()
    expected_impalads = min(len(self.cluster.impalads), 2)
    assert self.count_minidumps('impalad') == expected_impalads
    assert self.count_minidumps('statestored') == 1
    assert self.count_minidumps('catalogd') == 1

  @pytest.mark.execute_serially
  def test_disable_minidumps(self):
    """Check that setting the minidump_path to an empty value disables minidump creation.
    """
    assert self.count_all_minidumps() == 0
    self.start_cluster_without_minidumps()
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(0)
