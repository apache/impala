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

import glob
import os
import psutil
import pytest
import shutil
import tempfile
import time
from resource import setrlimit, RLIMIT_CORE, RLIM_INFINITY
from signal import SIGSEGV, SIGKILL, SIGUSR1
from subprocess import CalledProcessError

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType

DAEMONS = ['impalad', 'statestored', 'catalogd']
DAEMON_ARGS = ['impalad_args', 'state_store_args', 'catalogd_args']

class TestBreakpad(CustomClusterTestSuite):
  """Check that breakpad integration into the daemons works as expected. This includes
  writing minidump files on unhandled signals and rotating old minidumps on startup. The
  tests kill the daemons by sending a SIGSEGV signal.
  """
  # Limit for the number of minidumps that gets passed to the daemons as a startup flag.
  MAX_MINIDUMPS = 2

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
    self.kill_cluster(SIGKILL)
    assert self.tmp_dir
    shutil.rmtree(self.tmp_dir)

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('breakpad tests only run in exhaustive')
    # Disable core dumps for this test
    setrlimit(RLIMIT_CORE, (0, RLIM_INFINITY))

  @classmethod
  def teardown_class(cls):
    # Re-enable core dumps
    setrlimit(RLIMIT_CORE, (RLIM_INFINITY, RLIM_INFINITY))
    # Start default cluster for subsequent tests (verify_metrics).
    cls._start_impala_cluster([])

  def start_cluster_with_args(self, **kwargs):
    cluster_options = []
    for daemon_arg in DAEMON_ARGS:
      daemon_options = " ".join("-%s=%s" % i for i in kwargs.iteritems())
      cluster_options.append("""--%s='%s'""" % (daemon_arg, daemon_options))
    self._start_impala_cluster(cluster_options)

  def start_cluster(self):
    self.start_cluster_with_args(minidump_path=self.tmp_dir,
                                 max_minidumps=self.MAX_MINIDUMPS)

  def start_cluster_without_minidumps(self):
    self.start_cluster_with_args(minidump_path='', max_minidumps=self.MAX_MINIDUMPS)

  def kill_cluster(self, signal):
    self.cluster.refresh()
    processes = self.cluster.impalads + [self.cluster.catalogd, self.cluster.statestored]
    processes = filter(None, processes)
    self.kill_processes(processes, signal)
    signal is SIGUSR1 or self.assert_all_processes_killed()

  def kill_processes(self, processes, signal):
    for process in processes:
      process.kill(signal)
    signal is SIGUSR1 or self.wait_for_all_processes_dead(processes)

  def wait_for_all_processes_dead(self, processes, timeout=300):
    for process in processes:
      try:
        pid = process.get_pid()
        if not pid:
          continue
        psutil_process = psutil.Process(pid)
        psutil_process.wait(timeout)
      except psutil.TimeoutExpired:
        raise RuntimeError("Unable to kill %s (pid %d) after %d seconds." %
            (psutil_process.name, psutil_process.pid, timeout))

  def get_num_processes(self, daemon):
    self.cluster.refresh()
    if daemon == 'impalad':
      return len(self.cluster.impalads)
    elif daemon == 'catalogd':
      return self.cluster.catalogd and 1 or 0
    elif daemon == 'statestored':
      return self.cluster.statestored and 1 or 0
    raise RuntimeError("Unknown daemon name: %s" % daemon)

  def wait_for_num_processes(self, daemon, num_expected, timeout=60):
    end = time.time() + timeout
    self.cluster.refresh()
    num_processes = self.get_num_processes(daemon)
    while num_processes != num_expected and time.time() <= end:
      time.sleep(1)
      num_processes = self.get_num_processes(daemon)
    return num_processes

  def assert_all_processes_killed(self):
    self.cluster.refresh()
    assert not self.cluster.impalads
    assert not self.cluster.statestored
    assert not self.cluster.catalogd

  def count_minidumps(self, daemon, base_dir=None):
    base_dir = base_dir or self.tmp_dir
    path = os.path.join(base_dir, daemon)
    return len(glob.glob("%s/*.dmp" % path))

  def count_all_minidumps(self, base_dir=None):
    return sum((self.count_minidumps(daemon, base_dir) for daemon in DAEMONS))

  def assert_num_minidumps_for_all_daemons(self, base_dir=None):
    self.assert_num_logfile_entries(1)
    # IMPALA-3794 / Breakpad-681: Weak minidump ID generation can lead to name conflicts,
    # so that one process overwrites the minidump of others. See IMPALA-3794 for more
    # information.
    # TODO: Change this here and elsewhere in this file to expect 'cluster_size' minidumps
    # once Breakpad-681 has been fixed.
    assert self.count_minidumps('impalad', base_dir) >= 1
    assert self.count_minidumps('statestored', base_dir) == 1
    assert self.count_minidumps('catalogd', base_dir) == 1


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
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGSEGV)
    self.assert_num_minidumps_for_all_daemons()

  @pytest.mark.execute_serially
  def test_sigusr1_writes_minidump(self):
    """Check that when a daemon receives SIGUSR1 it writes a minidump file."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    assert self.count_all_minidumps() == 0
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGUSR1)
    # Breakpad forks to write its minidump files, wait for all the clones to terminate.
    assert self.wait_for_num_processes('impalad', cluster_size, 5) == cluster_size
    assert self.wait_for_num_processes('catalogd', 1, 5) == 1
    assert self.wait_for_num_processes('statestored', 1, 5) == 1
    # Make sure impalad still answers queries.
    client = self.create_impala_client()
    self.execute_query_expect_success(client, "SELECT COUNT(*) FROM functional.alltypes")
    # Kill the cluster. Sending SIGKILL will not trigger minidumps to be written.
    self.kill_cluster(SIGKILL)
    self.assert_num_minidumps_for_all_daemons()

  @pytest.mark.execute_serially
  def test_minidump_relative_path(self):
    """Check that setting 'minidump_path' to a relative value results in minidump files
    written to 'log_dir'.
    """
    minidump_base_dir = os.path.join(os.environ.get('LOG_DIR', '/tmp'), 'minidumps')
    shutil.rmtree(minidump_base_dir)
    # Omitting minidump_path as a parameter to the cluster will choose the default
    # configuration, which is a FLAGS_log_dir/minidumps.
    self.start_cluster_with_args()
    assert self.count_all_minidumps(minidump_base_dir) == 0
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGSEGV)
    self.assert_num_minidumps_for_all_daemons(minidump_base_dir)
    shutil.rmtree(minidump_base_dir)

  @pytest.mark.execute_serially
  def test_minidump_cleanup(self):
    """Check that a limited number of minidumps is preserved during startup."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(1)
    self.start_cluster()
    expected_impalads = min(self.get_num_processes('impalad'), self.MAX_MINIDUMPS)
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

  def trigger_single_minidump_and_get_size(self):
    """Kill a single impalad with SIGSEGV to make it write a minidump. Kill the rest of
    the cluster. Clean up the single minidump file and return its size.
    """
    self.cluster.refresh()
    assert self.get_num_processes('impalad') > 0
    # Make one impalad write a minidump.
    self.kill_processes(self.cluster.impalads[:1], SIGSEGV)
    # Kill the rest of the cluster.
    self.kill_cluster(SIGKILL)
    assert self.count_minidumps('impalad') == 1
    # Get file size of that miniump.
    path = os.path.join(self.tmp_dir, 'impalad')
    minidump_file = glob.glob("%s/*.dmp" % path)[0]
    minidump_size = os.path.getsize(minidump_file)
    os.remove(minidump_file)
    assert self.count_all_minidumps() == 0
    return minidump_size

  @pytest.mark.execute_serially
  def test_limit_minidump_size(self):
    """Check that setting the 'minidump_size_limit_hint_kb' to a small value will reduce
    the minidump file size.
    """
    assert self.count_all_minidumps() == 0
    # Generate minidump with default settings.
    self.start_cluster()
    full_minidump_size = self.trigger_single_minidump_and_get_size()
    # Start cluster with limited minidump file size, we use a very small value, to ensure
    # the resulting minidump will be as small as possible.
    self.start_cluster_with_args(minidump_path=self.tmp_dir,
        minidump_size_limit_hint_kb=1)
    reduced_minidump_size = self.trigger_single_minidump_and_get_size()
    # Check that the minidump file size has been reduced.
    assert reduced_minidump_size < full_minidump_size

  @SkipIfBuildType.not_dev_build
  @pytest.mark.execute_serially
  def test_dcheck_writes_minidump(self):
    """Check that hitting a DCHECK macro writes a minidump."""
    assert self.count_all_minidumps() == 0
    failed_to_start = False
    try:
      self.start_cluster_with_args(minidump_path=self.tmp_dir,
          beeswax_port=1)
    except CalledProcessError:
      failed_to_start = True
    assert failed_to_start
    assert self.count_minidumps('impalad') > 0
