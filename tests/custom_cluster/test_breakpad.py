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
from builtins import range
import glob
import os
import psutil
import pytest
import shutil
import tempfile
import time
from resource import setrlimit, RLIMIT_CORE, RLIM_INFINITY
from signal import SIGSEGV, SIGKILL, SIGUSR1, SIGTERM
from subprocess import CalledProcessError

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfBuildType

DAEMONS = ['impalad', 'statestored', 'catalogd']
DAEMON_ARGS = ['impalad_args', 'state_store_args', 'catalogd_args']

class TestBreakpadBase(CustomClusterTestSuite):
  """Base class with utility methods for all breakpad tests."""
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def setup_method(self, method):
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
    super(TestBreakpadBase, cls).setup_class()
    # Disable core dumps for this test
    setrlimit(RLIMIT_CORE, (0, RLIM_INFINITY))

  @classmethod
  def teardown_class(cls):
    # Re-enable core dumps
    setrlimit(RLIMIT_CORE, (RLIM_INFINITY, RLIM_INFINITY))

  def start_cluster_with_args(self, **kwargs):
    cluster_options = []
    for daemon_arg in DAEMON_ARGS:
      daemon_options = " ".join("-{0}={1}".format(k, v) for k, v in kwargs.items())
      cluster_options.append("--{0}={1}".format(daemon_arg, daemon_options))
    self._start_impala_cluster(cluster_options)

  def start_cluster(self):
    self.start_cluster_with_args(minidump_path=self.tmp_dir)

  def kill_cluster(self, signal):
    self.cluster.refresh()
    processes = self.cluster.impalads + [self.cluster.catalogd, self.cluster.statestored]
    processes = [_f for _f in processes if _f]
    self.kill_processes(processes, signal)
    signal is SIGUSR1 or self.assert_all_processes_killed()

  def kill_processes(self, processes, signal):
    for process in processes:
      process.kill(signal)
    signal is SIGUSR1 or self.wait_for_all_processes_dead(processes)

  def wait_for_all_processes_dead(self, processes, timeout=300):
    for process in processes:
      try:
        # For every process in the list we might see the original Impala process plus a
        # forked off child that is writing the minidump. We need to catch both.
        for pid in process.get_pids():
          print("Checking pid %s" % pid)
          psutil_process = psutil.Process(pid)
          psutil_process.wait(timeout)
      except psutil.NoSuchProcess:
        # Process has exited in the meantime
        pass
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

  def wait_for_num_processes(self, daemon, num_expected, timeout=30):
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

  def assert_num_minidumps_for_all_daemons(self, cluster_size, base_dir=None):
    self.assert_num_logfile_entries(1)
    assert self.count_minidumps('impalad', base_dir) == cluster_size
    assert self.count_minidumps('statestored', base_dir) == 1
    assert self.count_minidumps('catalogd', base_dir) == 1

  def assert_num_logfile_entries(self, expected_count):
    self.assert_impalad_log_contains('INFO', 'Wrote minidump to ',
        expected_count=expected_count)
    self.assert_impalad_log_contains('ERROR', 'Wrote minidump to ',
        expected_count=expected_count)
    self.assert_impalad_log_contains('INFO', 'Minidump with no thread info available.',
        expected_count=expected_count)
    self.assert_impalad_log_contains('ERROR', 'Minidump with no thread info available.',
        expected_count=expected_count)


class TestBreakpadCore(TestBreakpadBase):
  """Core tests to check that the breakpad integration into the daemons works as
  expected. This includes writing minidump when the daemons call abort(). Add tests here
  that depend on functionality of Impala other than the breakpad integration itself.
  """
  @pytest.mark.execute_serially
  def test_abort_writes_minidump(self):
    """Check that abort() (e.g. hitting a DCHECK macro) writes a minidump."""
    assert self.count_all_minidumps() == 0
    failed_to_start = False
    try:
      # Calling with an unresolvable hostname will abort.
      self.start_cluster_with_args(minidump_path=self.tmp_dir,
          hostname="jhzvlthd")
    except CalledProcessError:
      failed_to_start = True
    assert failed_to_start
    # Don't check for minidumps until all processes have gone away so that
    # the state of the cluster is not in flux.
    self.wait_for_num_processes('impalad', 0)
    assert self.count_minidumps('impalad') > 0


class TestBreakpadExhaustive(TestBreakpadBase):
  """Exhaustive tests to check that the breakpad integration into the daemons works as
  expected. This includes writing minidump files on unhandled signals and rotating old
  minidumps on startup.
  """
  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('These breakpad tests only run in exhaustive')
    super(TestBreakpadExhaustive, cls).setup_class()

  @pytest.mark.execute_serially
  def test_minidump_creation(self):
    """Check that when a daemon crashes, it writes a minidump file."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    assert self.count_all_minidumps() == 0
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGSEGV)
    self.assert_num_minidumps_for_all_daemons(cluster_size)

  @pytest.mark.execute_serially
  def test_sigusr1_writes_minidump(self):
    """Check that when a daemon receives SIGUSR1, it writes a minidump file."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    assert self.count_all_minidumps() == 0
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGUSR1)
    # Breakpad forks to write its minidump files, wait for all the clones to terminate.
    assert self.wait_for_num_processes('impalad', cluster_size) == cluster_size
    assert self.wait_for_num_processes('catalogd', 1) == 1
    assert self.wait_for_num_processes('statestored', 1) == 1
    # Make sure impalad still answers queries.
    client = self.create_impala_client()
    self.execute_query_expect_success(client, "SELECT COUNT(*) FROM functional.alltypes")
    # Kill the cluster. Sending SIGKILL will not trigger minidumps to be written.
    self.kill_cluster(SIGKILL)
    self.assert_num_minidumps_for_all_daemons(cluster_size)

  @pytest.mark.execute_serially
  def test_sigusr1_doesnt_kill(self):
    """Check that when minidumps are disabled and a daemon receives SIGUSR1, it does not
    die.
    """
    assert self.count_all_minidumps() == 0
    self.start_cluster_with_args(enable_minidumps=False)
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGUSR1)
    # Check that no minidumps have been written.
    self.assert_num_logfile_entries(0)
    assert self.count_all_minidumps() == 0
    # Check that all daemons are still alive.
    assert self.get_num_processes('impalad') == cluster_size
    assert self.get_num_processes('catalogd') == 1
    assert self.get_num_processes('statestored') == 1

  @pytest.mark.execute_serially
  def test_sigterm_no_minidumps(self):
    """Check that when a SIGTERM is caught, no minidump file is written.
    After receiving SIGTERM there should be no impalad/catalogd/statestored
    running.
    """
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    cluster_size = self.get_num_processes('impalad')
    assert self.count_all_minidumps() == 0

    # impalad/catalogd/statestored should be running.
    assert cluster_size > 0
    assert self.get_num_processes('catalogd') == 1
    assert self.get_num_processes('statestored') == 1
    # There should be no SIGTERM message in the log
    # when the system starts.
    self.assert_impalad_log_contains('INFO', 'Caught signal: SIGTERM. Daemon will exit',
        expected_count=0)

    self.kill_cluster(SIGTERM)

    # There should be no impalad/catalogd/statestored running.
    # There should be no minidump generated.
    assert self.get_num_processes('impalad') == 0
    assert self.get_num_processes('catalogd') == 0
    assert self.get_num_processes('statestored') == 0
    assert self.count_all_minidumps() == 0
    uid = os.getuid()
    # There should be a SIGTERM message in the log now
    # since we raised one above.
    log_str = 'Caught signal: SIGTERM. Daemon will exit.'
    self.assert_impalad_log_contains('INFO', log_str, expected_count=1)

  @pytest.mark.execute_serially
  def test_minidump_relative_path(self):
    """Check that setting 'minidump_path' to a relative value results in minidump files
    written to 'log_dir'.
    """
    minidump_base_dir = os.path.join(os.environ.get('LOG_DIR', '/tmp'), 'minidumps')
    shutil.rmtree(minidump_base_dir, ignore_errors=True)
    # Omitting minidump_path as a parameter to the cluster will choose the default
    # configuration, which is a FLAGS_log_dir/minidumps.
    self.start_cluster_with_args()
    assert self.count_all_minidumps(minidump_base_dir) == 0
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGSEGV)
    self.assert_num_minidumps_for_all_daemons(cluster_size, minidump_base_dir)
    shutil.rmtree(minidump_base_dir)

  @pytest.mark.execute_serially
  def test_minidump_cleanup(self):
    """Check that a limited number of minidumps is preserved during startup."""
    assert self.count_all_minidumps() == 0
    self.start_cluster()
    cluster_size = self.get_num_processes('impalad')
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(1)
    # Maximum number of minidumps that the impalads should keep for this test.
    max_minidumps = 2
    self.start_cluster_with_args(minidump_path=self.tmp_dir,
                                 max_minidumps=max_minidumps,
                                 logbufsecs=1)
    # Wait for log maintenance thread to clean up minidumps asynchronously.
    start = time.time()
    expected_impalad_minidumps = min(cluster_size, max_minidumps)
    while (self.count_minidumps('impalad') != expected_impalad_minidumps
        and time.time() - start < 10):
      time.sleep(0.1)
    assert self.count_minidumps('impalad') == expected_impalad_minidumps
    assert self.count_minidumps('statestored') == 1
    assert self.count_minidumps('catalogd') == 1

  @pytest.mark.execute_serially
  def test_minidump_cleanup_thread(self):
    """Check that periodic rotation preserves a limited number of minidumps."""
    assert self.count_all_minidumps() == 0
    # Maximum number of minidumps that the impalads should keep for this test.
    max_minidumps = 2
    # Sleep interval for the log rotation thread.
    rotation_interval = 1
    self.start_cluster_with_args(minidump_path=self.tmp_dir,
                                 max_minidumps=max_minidumps,
                                 logbufsecs=rotation_interval)
    cluster_size = self.get_num_processes('impalad')
    # We trigger several rounds of minidump creation to make sure that all daemons wrote
    # enough files to trigger rotation.
    for i in range(max_minidumps + 1):
      self.kill_cluster(SIGUSR1)
      # Breakpad forks to write its minidump files, sleep briefly to allow the forked
      # processes to start.
      time.sleep(1)
      # Wait for all the clones to terminate.
      assert self.wait_for_num_processes('impalad', cluster_size) == cluster_size
      assert self.wait_for_num_processes('catalogd', 1) == 1
      assert self.wait_for_num_processes('statestored', 1) == 1
      self.assert_num_logfile_entries(i + 1)
    # Sleep long enough for log cleaning to take effect.
    time.sleep(rotation_interval + 1)
    assert self.count_minidumps('impalad') == min(cluster_size, max_minidumps)
    assert self.count_minidumps('statestored') == max_minidumps
    assert self.count_minidumps('catalogd') == max_minidumps

  @pytest.mark.execute_serially
  def test_disable_minidumps(self):
    """Check that setting enable_minidumps to false disables minidump creation."""
    assert self.count_all_minidumps() == 0
    self.start_cluster_with_args(enable_minidumps=False)
    self.kill_cluster(SIGSEGV)
    self.assert_num_logfile_entries(0)

  @pytest.mark.execute_serially
  def test_empty_minidump_path_disables_breakpad(self):
    """Check that setting the minidump_path to an empty value disables minidump creation.
    """
    assert self.count_all_minidumps() == 0
    self.start_cluster_with_args(minidump_path='')
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


class TestLoggingBase(TestBreakpadBase):
  _default_max_log_files = 2

  @classmethod
  def setup_class(cls):
    super(TestLoggingBase, cls).setup_class()

  def start_cluster_with_args(self, cluster_size, log_dir, **kwargs):
    cluster_options = []
    for daemon_arg in DAEMON_ARGS:
      daemon_options = " ".join("-{0}={1}".format(k, v) for k, v in kwargs.items())
      cluster_options.append("--{0}={1}".format(daemon_arg, daemon_options))
    self._start_impala_cluster(cluster_options, cluster_size=cluster_size,
                               expected_num_impalads=cluster_size,
                               impala_log_dir=log_dir,
                               ignore_pid_on_log_rotation=True)

  def assert_logs(self, daemon, max_count, max_bytes, match_pid=True):
    """Assert that there are at most 'max_count' of INFO + ERROR log files for the
    specified daemon and the individual file size does not exceed 'max_bytes'.
    Also assert that stdout/stderr are redirected to correct file on each rotation."""
    log_dir = self.tmp_dir
    log_paths = glob.glob("%s/%s*log.ERROR.*" % (log_dir, daemon)) \
                + glob.glob("%s/%s*log.INFO.*" % (log_dir, daemon))
    assert len(log_paths) <= max_count

    # group log_paths by kind and pid (if match_pid).
    log_group = {}
    for path in sorted(log_paths):
      tok = path.split('.')
      key = tok[-1] + '.' + tok[-3] if match_pid else tok[-3]
      if key in log_group:
        log_group[key].append(path)
      else:
        log_group[key] = [path]

    for key, paths in log_group.items():
      for i in range(0, len(paths)):
        try:
          curr_path = paths[i]
          # check log size
          log_size = os.path.getsize(curr_path)
          assert log_size <= max_bytes, "{} exceed {} bytes".format(curr_path, max_bytes)

          if i < len(paths) - 1:
            # check that we print the next_path in last line of this log file
            next_path = paths[i + 1]
            with open(curr_path, 'rb') as f:
              f.seek(-2, os.SEEK_END)
              while f.read(1) != b'\n':
                f.seek(-2, os.SEEK_CUR)
              last_line = f.readline().decode()
              assert next_path in last_line
        except OSError:
          # The daemon might delete the log in the middle of assertion.
          # In that case, do nothing and move on.
          pass

  def silent_remove(self, filename):
    try:
      os.remove(filename)
    except OSError:
      pass

  def start_excessive_cerr_cluster(self, test_cluster_size=1, remove_symlink=False,
                                   match_pid=True, max_log_count_begin=0,
                                   max_log_count_end=None):
    """Check that impalad log is kept being rotated when most writing activity is coming
    from stderr stream.
    Along with LogFaultInjectionThread in init.cc, this test will fill impalad error logs
    with approximately 128kb error messages per second."""
    test_logbufsecs = 3
    test_max_log_files = self._default_max_log_files
    test_max_log_size = 1  # 1 MB
    test_error_msg = ('123456789abcde_' * 64)  # 1 KB error message
    test_debug_actions = 'LOG_MAINTENANCE_STDERR:FAIL@1.0@' + test_error_msg
    daemon = 'impalad'
    os.chmod(self.tmp_dir, 0o744)

    expected_log_max_bytes = int(1.2 * 1024**2)  # 1.2 MB
    self.assert_logs(daemon, max_log_count_begin, expected_log_max_bytes)
    self.start_cluster_with_args(test_cluster_size, self.tmp_dir,
                                 logbufsecs=test_logbufsecs,
                                 max_log_files=test_max_log_files,
                                 max_log_size=test_max_log_size,
                                 debug_actions=test_debug_actions,
                                 log_rotation_match_pid=('1' if match_pid else '0'))
    self.wait_for_num_processes(daemon, test_cluster_size, 30)
    # Count both INFO and ERROR logs
    if max_log_count_end is None:
      max_log_count_end = test_max_log_files * test_cluster_size * 2
    # Wait for log maintenance thread to flush and rotate the logs asynchronously.
    duration = test_logbufsecs * 10
    start = time.time()
    while (time.time() - start < duration):
      time.sleep(1)
      self.assert_logs(daemon, max_log_count_end, expected_log_max_bytes)
      if (remove_symlink):
        pattern = self.tmp_dir + '/' + daemon + '*'
        symlinks = glob.glob(pattern + '.INFO') + glob.glob(pattern + '.ERROR')
        for symlink in symlinks:
          self.silent_remove(symlink)


class TestLogging(TestLoggingBase):
  """Core tests to check that impala log is rolled periodically, obeying
  max_log_size, max_log_files, and log_rotation_match_pid even in the presence of heavy
  stderr writing.
  """

  @classmethod
  def setup_class(cls):
    super(TestLogging, cls).setup_class()

  @pytest.mark.execute_serially
  def test_excessive_cerr_ignore_pid(self):
    """Test excessive cerr activity twice with restart in between and no PID matching."""
    self.start_excessive_cerr_cluster(test_cluster_size=1, remove_symlink=False,
                                      match_pid=False)
    self.kill_cluster(SIGTERM)
    # There should be no impalad/catalogd/statestored running.
    assert self.get_num_processes('impalad') == 0
    assert self.get_num_processes('catalogd') == 0
    assert self.get_num_processes('statestored') == 0
    max_count = self._default_max_log_files * 2
    self.start_excessive_cerr_cluster(test_cluster_size=1, remove_symlink=False,
                                      match_pid=False,
                                      max_log_count_begin=max_count,
                                      max_log_count_end=max_count)


class TestLoggingExhaustive(TestLoggingBase):
  """Exhaustive tests to check that impala log is rolled periodically, obeying
  max_log_size, max_log_files, and log_rotation_match_pid even in the presence of heavy
  stderr writing.
  """

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('TestLoggingExhaustive only run in exhaustive')
    super(TestLoggingExhaustive, cls).setup_class()

  @pytest.mark.execute_serially
  def test_excessive_cerr(self):
    """Test excessive cerr activity with single node cluster."""
    self.start_excessive_cerr_cluster()

  @pytest.mark.execute_serially
  def test_excessive_cerr_no_symlink(self):
    """Test excessive cerr activity with two node cluster and missing log symlinks."""
    self.start_excessive_cerr_cluster(test_cluster_size=2, remove_symlink=False)

  @pytest.mark.execute_serially
  def test_excessive_cerr_match_pid(self):
    """Test excessive cerr activity twice with restart in between and PID matching."""
    self.start_excessive_cerr_cluster(1, remove_symlink=False, match_pid=True)
    self.kill_cluster(SIGTERM)
    # There should be no impalad/catalogd/statestored running.
    assert self.get_num_processes('impalad') == 0
    assert self.get_num_processes('catalogd') == 0
    assert self.get_num_processes('statestored') == 0
    max_count_begin = self._default_max_log_files * 2
    max_count_end = max_count_begin * 2
    self.start_excessive_cerr_cluster(test_cluster_size=1, remove_symlink=False,
                                      match_pid=True,
                                      max_log_count_begin=max_count_begin,
                                      max_log_count_end=max_count_end)
