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
# Superclass for all tests that need a custom cluster.
# TODO: Configure cluster size and other parameters.

from __future__ import absolute_import, division, print_function
import inspect
import logging
import os
import os.path
import pipes
import pytest
import subprocess

from glob import glob
from impala_py_lib.helpers import find_all_files, is_core_dump
from re import search
from subprocess import check_call
from tests.common.file_utils import cleanup_tmp_test_dir, make_tmp_test_dir
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.util.filesystem_utils import IS_LOCAL
from tests.util.workload_management import QUERY_TBL_LOG_NAME, QUERY_TBL_LIVE_NAME
from time import sleep, time

LOG = logging.getLogger(__name__)

IMPALA_HOME = os.environ['IMPALA_HOME']
DEFAULT_CLUSTER_SIZE = 3
NUM_COORDINATORS = DEFAULT_CLUSTER_SIZE
# Marks that with_args was specified on a method, as opposed to the whole class.
WITH_ARGS_METHOD = 'with_args_method'

# Additional args passed to respective daemon command line.
IMPALAD_ARGS = 'impalad_args'
STATESTORED_ARGS = 'state_store_args'
CATALOGD_ARGS = 'catalogd_args'
ADMISSIOND_ARGS = 'admissiond_args'
KUDU_ARGS = 'kudu_args'
# Additional args passed to the start-impala-cluster script.
START_ARGS = 'start_args'
JVM_ARGS = 'jvm_args'
CUSTOM_CORE_SITE_DIR = 'custom_core_site_dir'
HIVE_CONF_DIR = 'hive_conf_dir'
CLUSTER_SIZE = "cluster_size"
# Default query options passed to the impala daemon command line. Handled separately from
# other impala daemon arguments to allow merging multiple defaults into a single list.
DEFAULT_QUERY_OPTIONS = 'default_query_options'
IMPALA_LOG_DIR = 'impala_log_dir'
NUM_EXCLUSIVE_COORDINATORS = 'num_exclusive_coordinators'
STATESTORED_TIMEOUT_S = 'statestored_timeout_s'
IMPALAD_TIMEOUT_S = 'impalad_timeout_s'
EXPECT_CORES = 'expect_cores'
# Additional arg to determine whether we should reset the Ranger policy repository.
RESET_RANGER = 'reset_ranger'
# Decorator key to support temporary dir creation.
TMP_DIR_PLACEHOLDERS = 'tmp_dir_placeholders'
# Indicates if a failure to start is acceptable or not. If set to `True` and the cluster
# startup fails with "num_known_live_backends did not reach expected value in time", then
# the test passes. Any other exception is raised.
EXPECT_STARTUP_FAIL = 'expect_startup_fail'
# If True, add '--logbuflevel=-1' into all impala daemon args.
DISABLE_LOG_BUFFERING = 'disable_log_buffering'
# If True, resolves the actual files for all the log symlinks and outputs the resolved
# paths to stderr.
LOG_SYMLINKS = 'log_symlinks'
WORKLOAD_MGMT = 'workload_mgmt'
FORCE_RESTART = 'force_restart'

# Args that accept additional formatting to supply temporary dir path.
ACCEPT_FORMATTING = set([IMPALAD_ARGS, CATALOGD_ARGS, IMPALA_LOG_DIR])

# Run with fast topic updates by default to reduce time to first query running.
DEFAULT_STATESTORE_ARGS = ('--statestore_update_frequency_ms=50 '
                           '--statestore_priority_update_frequency_ms=50 '
                           '--statestore_heartbeat_frequency_ms=50')

# Additional flags appended to impalad_args if workload_mgmt=True.
# IMPALA-13051: Add faster default graceful shutdown options before processing
# explicit args. Impala doesn't start graceful shutdown until the grace period
# has passed, and most tests that use graceful shutdown are testing flushing
# the query log, which doesn't start until after the grace period has passed.
WORKLOAD_MGMT_IMPALAD_FLAGS = (
  '--enable_workload_mgmt=true --query_log_write_interval_s=1 '
  '--shutdown_grace_period_s=0 --shutdown_deadline_s=60 ')

PREVIOUS_CMD_STR = ""

class CustomClusterTestSuite(ImpalaTestSuite):
  """Runs tests with a custom Impala cluster. There are two modes:
  - If a @with_args decorator is specified on the class, all tests in the class share a
    single Impala cluster. @with_args decorators on methods are ignored.
  - Otherwise every test runs starts a new cluster, and @with_args decorators on a test
    method can be used to customize that cluster."""

  # Central place to keep all temporary dirs referred by a custom cluster test method.
  # setup_method() will populate this using make_tmp_dir(), and then teardown_method()
  # will clean up using clear_tmp_dirs().
  TMP_DIRS = dict()

  # Args for cluster startup/teardown when sharing a single cluster for the entire class.
  SHARED_CLUSTER_ARGS = {}

  # The currently executing test method. setup_method() populates this and tear_method()
  # clears it. This is used to set the log directory location when a test manually
  # restarts the cluster during the test. This is left unset for tests that use a single
  # cluster for multiple tests (i.e. with SHARED_CLUSTER_ARGS), as the logs will be
  # shared across multiple tests. Since this is used from @classmethod functions, this is
  # set and accessed via @classmethod functions set/get_current_test_method(). This is
  # awkward, but it should work because custom cluster tests are single threaded.
  CURRENT_TEST_METHOD_NAME = None

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.add_custom_cluster_constraints()

  @classmethod
  def add_custom_cluster_constraints(cls):
    # Defines constraints for custom cluster tests, called by add_test_dimensions.
    # By default, custom cluster tests only run on text/none and with a limited set of
    # exec options. Subclasses may override this to relax these default constraints.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text'
        and v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['batch_size'] == 0
        and v.get_value('exec_option')['disable_codegen'] is False
        and v.get_value('exec_option')['num_nodes'] == 0)

  @classmethod
  def setup_class(cls):
    # IMPALA-3614: @SkipIfLocal.multiple_impalad workaround
    # IMPALA-2943 TODO: When pytest is upgraded, see if this explicit skip can be
    # removed in favor of the class-level SkipifLocal.multiple_impalad decorator.
    if IS_LOCAL:
      pytest.skip("multiple impalads needed")

    if cls.SHARED_CLUSTER_ARGS:
      cls.cluster_setup(cls.SHARED_CLUSTER_ARGS)

  @classmethod
  def teardown_class(cls):
    if cls.SHARED_CLUSTER_ARGS:
      cls.cluster_teardown(cls.__name__, cls.SHARED_CLUSTER_ARGS)

  @classmethod
  def with_args(cls, impalad_args=None, statestored_args=None, catalogd_args=None,
      start_args=None, default_query_options=None, jvm_args=None,
      impala_log_dir=None, hive_conf_dir=None, cluster_size=None,
      num_exclusive_coordinators=None, kudu_args=None, statestored_timeout_s=None,
      impalad_timeout_s=None, expect_cores=None, reset_ranger=False,
      tmp_dir_placeholders=[],
      expect_startup_fail=False, disable_log_buffering=False, log_symlinks=False,
      workload_mgmt=False, force_restart=False, custom_core_site_dir=None):
    """Records arguments to be passed to a cluster by adding them to the decorated
    method's func_dict"""
    args = dict()
    if impalad_args is not None:
      args[IMPALAD_ARGS] = impalad_args
    if statestored_args is not None:
      args[STATESTORED_ARGS] = statestored_args
    if catalogd_args is not None:
      args[CATALOGD_ARGS] = catalogd_args
    if start_args is not None:
      args[START_ARGS] = start_args
    if jvm_args is not None:
      args[JVM_ARGS] = jvm_args
    if custom_core_site_dir is not None:
      args[CUSTOM_CORE_SITE_DIR] = custom_core_site_dir
    if hive_conf_dir is not None:
      args[HIVE_CONF_DIR] = hive_conf_dir
    if kudu_args is not None:
      args[KUDU_ARGS] = kudu_args
    if default_query_options is not None:
      args[DEFAULT_QUERY_OPTIONS] = default_query_options
    if impala_log_dir is not None:
      args[IMPALA_LOG_DIR] = impala_log_dir
    if cluster_size is not None:
      args[CLUSTER_SIZE] = cluster_size
    if num_exclusive_coordinators is not None:
      args[NUM_EXCLUSIVE_COORDINATORS] = num_exclusive_coordinators
    if statestored_timeout_s is not None:
      args[STATESTORED_TIMEOUT_S] = statestored_timeout_s
    if impalad_timeout_s is not None:
      args[IMPALAD_TIMEOUT_S] = impalad_timeout_s
    if expect_cores is not None:
      args[EXPECT_CORES] = expect_cores
    if reset_ranger:
      args[RESET_RANGER] = True
    if tmp_dir_placeholders:
      args[TMP_DIR_PLACEHOLDERS] = tmp_dir_placeholders
    if expect_startup_fail:
      args[EXPECT_STARTUP_FAIL] = True
    if disable_log_buffering:
      args[DISABLE_LOG_BUFFERING] = True
    if log_symlinks:
      args[LOG_SYMLINKS] = True
    if workload_mgmt:
      args[WORKLOAD_MGMT] = True
    if force_restart or cls.pytest_config().option.shard_tests:
      # When sharding tests, always restart the cluster to avoid issues with tests
      # that depend on a specific test order within a shard.
      args[FORCE_RESTART] = True

    def merge_args(args_first, args_last):
      result = args_first.copy()
      for key in args_last:
        if key not in result:
          result[key] = args_last[key]
        else:
          if key in (
              IMPALAD_ARGS,
              STATESTORED_ARGS,
              CATALOGD_ARGS,
              START_ARGS,
              JVM_ARGS,
              KUDU_ARGS
          ):
            # Let the server decide.
            result[key] = " ".join((result[key], args_last[key]))
          else:
            # Last writer wins.
            result[key] = args_last[key]
      return result

    def decorate(obj):
      """If obj is a class, set SHARED_CLUSTER_ARGS for setup/teardown_class. Otherwise
      add to the function __dict__ for setup/teardown_method."""
      if inspect.isclass(obj):
        obj.SHARED_CLUSTER_ARGS = merge_args(obj.SHARED_CLUSTER_ARGS, args)
      else:
        obj.__dict__[WITH_ARGS_METHOD] = True
        obj.__dict__ = merge_args(obj.__dict__, args)
      return obj
    return decorate

  @classmethod
  def make_tmp_dir(cls, name):
    """Create a temporary directory and register it."""
    assert name not in cls.TMP_DIRS
    cls.TMP_DIRS[name] = make_tmp_test_dir(name)
    LOG.info("Created temporary dir {}".format(cls.TMP_DIRS[name]))
    return cls.TMP_DIRS[name]

  def get_tmp_dir(self, name):
    """Get the path of temporary directory that was registered with given 'name'."""
    return self.TMP_DIRS[name]

  @classmethod
  def clear_tmp_dirs(cls):
    """Clear all temporary dirs."""
    for tmp_dir in cls.TMP_DIRS.values():
      LOG.info("Removing temporary dir {}".format(tmp_dir))
      cleanup_tmp_test_dir(tmp_dir)
    cls.TMP_DIRS.clear()

  def clear_tmp_dir(self, name):
    """Clear temporary dir 'name'."""
    assert name in self.TMP_DIRS
    LOG.info("Removing temporary dir {}".format(self.TMP_DIRS[name]))
    cleanup_tmp_test_dir(self.TMP_DIRS[name])
    del self.TMP_DIRS[name]

  @classmethod
  def set_current_test_method_name(cls, test_method_name):
    cls.CURRENT_TEST_METHOD_NAME = test_method_name

  @classmethod
  def get_current_test_method_name(self):
    return self.CURRENT_TEST_METHOD_NAME

  @classmethod
  def cluster_setup(cls, args):
    cluster_args = list()
    disable_log_buffering = args.get(DISABLE_LOG_BUFFERING, False)
    cls._warn_assert_log = not disable_log_buffering

    if TMP_DIR_PLACEHOLDERS in args:
      # Create all requested temporary dirs.
      for name in args[TMP_DIR_PLACEHOLDERS]:
        if name in cls.TMP_DIRS:
          LOG.warning("Tmp dir '{0}' referring to '{1}' not been cleanup. It will "
                      "be erased from TMP_DIRS map, but actual path might stay in "
                      "filesystem. Custom cluster test before this might have not "
                      "teardown cleanly.".format(name, cls.TMP_DIRS[name]))
          del cls.TMP_DIRS[name]
        cls.make_tmp_dir(name)

    impala_daemons = [IMPALAD_ARGS, STATESTORED_ARGS, CATALOGD_ARGS, ADMISSIOND_ARGS]
    for arg in (impala_daemons + [JVM_ARGS]):
      val = ''
      if args.get(WORKLOAD_MGMT, False):
        if arg == CATALOGD_ARGS:
          val += '--enable_workload_mgmt=true '
        if arg == IMPALAD_ARGS:
          val += WORKLOAD_MGMT_IMPALAD_FLAGS
      if arg in impala_daemons and disable_log_buffering:
        val += '--logbuflevel=-1 '
      # append this the very last so it can override anything above.
      if arg in args:
        val += (args[arg] if arg not in ACCEPT_FORMATTING
               else args[arg].format(**cls.TMP_DIRS))
      if val:
        cluster_args.append("--%s=%s " % (arg, val))
    if START_ARGS in args:
      cluster_args.extend(args[START_ARGS].split())

    custom_class_path_val = ""
    if CUSTOM_CORE_SITE_DIR in args:
      custom_class_path_val = args[CUSTOM_CORE_SITE_DIR]

    if HIVE_CONF_DIR in args:
      cls._start_hive_service(args[HIVE_CONF_DIR])
      custom_class_path_val += ":" + args[HIVE_CONF_DIR]

    if custom_class_path_val:
      # Should let Impala adopt the custom Hadoop configuration. The only way is to add it
      # in the beginning of the CLASSPATH. Because there's already Hadoop site xml files
      # in the default CLASSPATH (see bin/set-classpath.sh).
      cluster_args.append(
          '--env_vars=CUSTOM_CLASSPATH=%s ' % custom_class_path_val)

    if KUDU_ARGS in args:
      cls._restart_kudu_service(args[KUDU_ARGS])

    if RESET_RANGER in args:
      cls._reset_ranger_policy_repository()

    cluster_size = DEFAULT_CLUSTER_SIZE
    if CLUSTER_SIZE in args:
      cluster_size = args[CLUSTER_SIZE]

    use_exclusive_coordinators = False
    num_coordinators = cluster_size
    if NUM_EXCLUSIVE_COORDINATORS in args:
      num_coordinators = args[NUM_EXCLUSIVE_COORDINATORS]
      use_exclusive_coordinators = True

    # Start a clean new cluster before each test
    kwargs = {
      "cluster_size": cluster_size,
      "num_coordinators": num_coordinators,
      "expected_num_impalads": cluster_size,
      DEFAULT_QUERY_OPTIONS: args.get(DEFAULT_QUERY_OPTIONS),
      "use_exclusive_coordinators": use_exclusive_coordinators
    }
    if IMPALA_LOG_DIR in args:
      kwargs[IMPALA_LOG_DIR] = args[IMPALA_LOG_DIR].format(**cls.TMP_DIRS)
    if STATESTORED_TIMEOUT_S in args:
      kwargs[STATESTORED_TIMEOUT_S] = args[STATESTORED_TIMEOUT_S]
    if IMPALAD_TIMEOUT_S in args:
      kwargs[IMPALAD_TIMEOUT_S] = args[IMPALAD_TIMEOUT_S]
    if FORCE_RESTART in args:
      kwargs[FORCE_RESTART] = args[FORCE_RESTART]
      if args[FORCE_RESTART] is True and not cls.pytest_config().option.shard_tests:
        LOG.warning("Test uses force_restart=True to avoid restarting the cluster. "
                    "Test reorganization/assertion rewrite is needed")
    else:
      # Default to False to ensure that the cluster is not restarted for every test.
      kwargs[FORCE_RESTART] = False

    if args.get(EXPECT_CORES, False):
      # Make a note of any core files that already exist
      possible_cores = find_all_files('*core*')
      cls.pre_test_cores = set([f for f in possible_cores if is_core_dump(f)])

      # Explicitly allow startup to exception, since startup is expected to fail
      try:
        cls._start_impala_cluster(cluster_args, **kwargs)
        pytest.fail("cluster startup should have failed")
      except Exception:
        cls._stop_impala_cluster()
    else:
      try:
        cls._start_impala_cluster(cluster_args, **kwargs)

        # Fail test if cluster startup succeeded when it was supposed to fail.
        assert not args.get(EXPECT_STARTUP_FAIL, False), \
            "Expected cluster startup to fail, but startup succeeded."

        super(CustomClusterTestSuite, cls).setup_class()
      except AssertionError as e:
        if args.get(EXPECT_STARTUP_FAIL, False):
          assert e.msg == "num_known_live_backends did not reach expected value " \
              "in time", "Unexpected exception: {}".format(e)
        else:
          raise e
      except subprocess.CalledProcessError as e:
        if args.get(EXPECT_STARTUP_FAIL, False):
          assert search(r"returned non-zero exit status", str(e)), \
              "Unexpected exception: {}".format(e)
        else:
          raise e

  def setup_method(self, method):
    if not self.SHARED_CLUSTER_ARGS:
      # Store the test method name so that we can put logs in different directories for
      # different tests. This only applies if the cluster is being restarted per test
      # method. If this cluster is used for multiple test methods, leave this unset.
      self.set_current_test_method_name(method.__name__)
      self.cluster_setup(method.__dict__)
    elif method.__dict__.get(WITH_ARGS_METHOD):
      pytest.fail("Cannot specify with_args on both class and methods")

  @classmethod
  def cluster_teardown(cls, name, args):
    if args.get(WORKLOAD_MGMT, False):
      cls.close_impala_clients()
      cls.cluster.graceful_shutdown_impalads()

    cls.clear_tmp_dirs()

    if HIVE_CONF_DIR in args:
      cls._start_hive_service(None)  # Restart Hive Service using default configs

    if args.get(EXPECT_CORES, False):
      # The core dumps expected to be generated by this test should be cleaned up
      possible_cores = find_all_files('*core*')
      post_test_cores = set([f for f in possible_cores if is_core_dump(f)])

      for f in (post_test_cores - cls.pre_test_cores):
        LOG.info("Cleaned up {core} created by {name}".format(
          core=f, name=name))
        os.remove(f)
      # Skip teardown_class as setup was skipped.
    elif not args.get(EXPECT_STARTUP_FAIL, False):
      # Skip teardown (which closes all open clients) if a startup failure is expected
      # since no clients will have been created.
      super(CustomClusterTestSuite, cls).teardown_class()

  def teardown_method(self, method):
    if not self.SHARED_CLUSTER_ARGS:
      self.cluster_teardown(method.__name__, method.__dict__)
    self.set_current_test_method_name(None)

  def wait_for_wm_init_complete(self, timeout_s=180):
    """
    Waits for the catalog to report the workload management initialization process has
    completed and the workload management tables to be in the local catalog of
    all coordinators.
    """
    end_time = time() + timeout_s
    self.assert_catalogd_ha_contains("INFO", r'Completed workload management '
        r'initialization', timeout_s=(end_time - time()))

    # Wait until table is available in the coordinator's catalog cache.
    for coord in self.cluster.get_all_coordinators():
      success = False
      wm_tables = list()
      while (not success and time() < end_time):
        wm_tables = [QUERY_TBL_LIVE_NAME, QUERY_TBL_LOG_NAME]
        catalog_objs = coord.service.read_debug_webpage(
            "catalog?json", timeout=(end_time - time()))
        for tbl in list(wm_tables):
          if tbl in catalog_objs:
            wm_tables.remove(tbl)
        success = (len(wm_tables) == 0)
        if not success:
          sleep(0.5)
      assert success, (
          "Did not find table '{}' in local catalog of coordinator '{}:{}'.").format(
              str(wm_tables), coord.hostname, coord.get_webserver_port())

  def wait_for_wm_idle(self, coordinators=[], timeout_s=370):
    """Wait until workload management worker in each coordinator becomes idle.
    The 'timeout_s' is applied on each coordinator wait. Default 'timeout_s' is:
      query_log_dml_exec_timeout_s * query_log_max_insert_attempts + 10 = 370
    It is intentionally set high to avoid HMS deadlock in the event of slow insert
    followed by ungraceful shutdown (IMPALA-13842)."""
    if not coordinators:
      # Refresh cluster in case cluster has changed.
      self.cluster.refresh()
      coordinators = self.cluster.get_all_coordinators()
    for coord in coordinators:
      coord.service.wait_for_metric_value(
        "impala-server.completed-queries.queued", 0, timeout=timeout_s, interval=1)

  @classmethod
  def _stop_impala_cluster(cls):
    # TODO: Figure out a better way to handle case where processes are just starting
    # / cleaning up so that sleeps are not needed.
    sleep(2)
    check_call([os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'), '--kill_only'])
    sleep(2)

  @staticmethod
  def _restart_kudu_service(kudu_args=None):
    kudu_env = dict(os.environ)
    # On some platforms, Kudu can get stuck when HEAPCHECK is set (even to an empty
    # value). Drop any HEAPCHECK variable from the environment.
    if "HEAPCHECK" in kudu_env:
      del kudu_env["HEAPCHECK"]
    if kudu_args is not None:
      kudu_env["IMPALA_KUDU_STARTUP_FLAGS"] = kudu_args
    call = subprocess.Popen(
        ['/bin/bash', '-c', os.path.join(IMPALA_HOME,
                                         'testdata/cluster/admin restart kudu')],
        env=kudu_env)
    call.wait()
    if call.returncode != 0:
      raise RuntimeError("Unable to restart Kudu")

  @staticmethod
  def _start_hive_service(hive_conf_dir):
    hive_env = dict(os.environ)
    if hive_conf_dir is not None:
      hive_env['HIVE_CONF_DIR'] = hive_conf_dir
    call = subprocess.Popen(
      ['/bin/bash', '-c', os.path.join(IMPALA_HOME, 'testdata/bin/run-hive-server.sh')],
      env=hive_env)
    call.wait()
    if call.returncode != 0:
      raise RuntimeError("Unable to start Hive")

  @staticmethod
  def _stop_hive_service():
    subprocess.check_call([os.path.join(IMPALA_HOME,
                                        "testdata/bin/kill-hive-server.sh")],
                          close_fds=True)

  @staticmethod
  def _reset_ranger_policy_repository():
    script_kill_ranger = os.path.join(os.environ['IMPALA_HOME'],
                                      'testdata/bin/kill-ranger-server.sh')
    script_run_ranger = os.path.join(os.environ['IMPALA_HOME'],
                                     'testdata/bin/run-ranger-server.sh')
    script_create_test_config = os.path.join(os.environ['IMPALA_HOME'],
                                             'bin/create-test-configuration.sh')
    script_setup_ranger = os.path.join(os.environ['IMPALA_HOME'],
                                       'testdata/bin/setup-ranger.sh')
    check_call([script_kill_ranger])
    check_call([script_create_test_config, '-create_ranger_policy_db'])
    check_call([script_run_ranger])
    check_call([script_setup_ranger])

  @classmethod
  def _log_symlinks(cls, logdir=None, log=None,
      patterns=["*.INFO", "*.WARNING", "*.ERROR", "*.FATAL"]):
    """
     Resolves all symlinks in the specified logdir and print out their actual paths.
     If the 'logdir' parameter is None, will use the value in 'cls.impala_log_dir'.
     If the 'log' parameter is None, will call the 'print' function to output the resolved
     paths, otherwise will call 'log.info' to output the resolved paths.
    """
    if logdir is None:
      logdir = cls.impala_log_dir

    file_list = "Log Files for Test:\n"
    for pattern in patterns:
      matching_files = glob(os.path.join(logdir, pattern))
      for matching_file in sorted(matching_files):
        file_list += "  * {} - {}\n".format(matching_file.split(os.path.sep)[-1],
            os.path.realpath(matching_file))

    if log is None:
      print(file_list)
    else:
      log.info(file_list)

  @classmethod
  def _start_impala_cluster(cls,
                            options,
                            impala_log_dir=None,
                            cluster_size=DEFAULT_CLUSTER_SIZE,
                            num_coordinators=NUM_COORDINATORS,
                            use_exclusive_coordinators=False,
                            add_executors=False,
                            add_impalads=False,
                            log_level=1,
                            expected_num_impalads=DEFAULT_CLUSTER_SIZE,
                            expected_subscribers=0,
                            default_query_options=None,
                            statestored_timeout_s=60,
                            impalad_timeout_s=60,
                            ignore_pid_on_log_rotation=False,
                            wait_for_backends=True,
                            log_symlinks=False,
                            force_restart=True):
    if impala_log_dir:
      # If the test gave a specific location, use it, as the test may be parsing the logs
      # to find certain output.
      cls.impala_log_dir = impala_log_dir
    else:
      # The test didn't customize the log dir, so calculate a reasonable base directory
      # To find the log directory, we proceed in this order:
      # 1. LOG_DIR environment variable (used in test scripts for Jenkins jobs, etc)
      # 2. IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR - set impala-config.sh (used in devenvs)
      # 3. /tmp/ - This probably shouldn't happen, but at least the logs can go somewhere
      impala_base_log_dir = os.getenv("LOG_DIR",
          os.getenv("IMPALA_CUSTOM_CLUSTER_TEST_LOGS_DIR", "/tmp/"))

      # To make it easier to find logs across multiple custom cluster tests, organize
      # them into subdirectories based on their test class and their test method name
      # (where applicable).
      impala_log_dir_per_test = os.path.join(impala_base_log_dir, cls.__name__)
      # The CURRENT_TEST_METHOD_NAME will be None when using SHARED_CLUSTER_ARGS as the
      # cluster is not restarted for each test method
      if cls.CURRENT_TEST_METHOD_NAME:
        impala_log_dir_per_test = os.path.join(impala_log_dir_per_test,
            cls.CURRENT_TEST_METHOD_NAME)

      if not os.path.isdir(impala_log_dir_per_test):
        os.makedirs(impala_log_dir_per_test)
      cls.impala_log_dir = impala_log_dir_per_test

    # We ignore TEST_START_CLUSTER_ARGS here. Custom cluster tests specifically test that
    # certain custom startup arguments work and we want to keep them independent of dev
    # environments.
    cmd = [os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'),
           '--state_store_args=%s' % DEFAULT_STATESTORE_ARGS,
           '--cluster_size=%d' % cluster_size,
           '--num_coordinators=%d' % num_coordinators,
           '--log_dir=%s' % cls.impala_log_dir,
           '--log_level=%s' % log_level]

    if ignore_pid_on_log_rotation:
      # IMPALA-12595: Ignore PID on log rotation for all custom cluster tests.
      # Most of test in custom_cluster need to match PID, except some test for logging.
      cmd.append('--ignore_pid_on_log_rotation')

    if use_exclusive_coordinators:
      cmd.append("--use_exclusive_coordinators")

    if add_executors:
      cmd.append("--add_executors")

    if add_impalads:
      cmd.append("--add_impalads")

    if cls.pytest_config().option.use_local_catalog:
      cmd.append("--impalad_args=--use_local_catalog=1")
      cmd.append("--catalogd_args=--catalog_topic_mode=minimal")

    default_query_option_kvs = []
    # Put any defaults first, then any arguments after that so they can override defaults.
    if default_query_options is not None:
      default_query_option_kvs.extend(default_query_options)
    # Add the default query options after any arguments. This will override any default
    # options set in --impalad_args by design to force tests to pass default_query_options
    # into this function directly.
    options.append("--impalad_args=--default_query_options={0}".format(
        ','.join(["{0}={1}".format(k, v) for k, v in default_query_option_kvs])))

    cmd_str = " ".join(pipes.quote(arg) for arg in cmd + options)

    # If the cluster is already started, we don't need to start it again, unless
    # force_restart is set to True. NOTE: reordering tests into classes with class-level
    # 'with_args' decorators is a more preferable way to avoid restarting the cluster.
    global PREVIOUS_CMD_STR
    if PREVIOUS_CMD_STR == cmd_str and not force_restart:
        LOG.info("Reusing existing cluster with command: %s" % cmd_str)
        cls.cluster = ImpalaCluster.get_e2e_test_cluster()
        try:
          cls._verify_cluster(expected_num_impalads, options, wait_for_backends,
                              expected_subscribers, 5, 5)
          return
        except Exception as e:
          LOG.info("Failed to reuse running cluster: %s" % e)
          pass
        finally:
          cls.cluster = ImpalaCluster.get_e2e_test_cluster()
          cls.impalad_test_service = cls.create_impala_service()

    LOG.info("Starting cluster with command: %s" % cmd_str)
    try:
      check_call(cmd + options, close_fds=True)
    finally:
      if log_symlinks:
        cls._log_symlinks(log=LOG)

      # Failure tests expect cluster to be initialised even if start-impala-cluster fails.
      cls.cluster = ImpalaCluster.get_e2e_test_cluster()
      cls.impalad_test_service = cls.create_impala_service()

    PREVIOUS_CMD_STR = cmd_str

    cls._verify_cluster(expected_num_impalads, options, wait_for_backends,
                        expected_subscribers, statestored_timeout_s, impalad_timeout_s)

  @classmethod
  def _verify_cluster(cls, expected_num_impalads, options, wait_for_backends,
                   expected_subscribers, statestored_timeout_s, impalad_timeout_s):
    """
    Verifies the cluster by checking the number of live subscribers and backends.
    Raises exception if verification fails.
    """
    statestored = cls.cluster.statestored
    if statestored is None:
      raise Exception("statestored was not found")

    # The number of statestore subscribers is
    #     cluster_size (# of impalad) + 1 (for catalogd)
    #     + 1 (for admissiond if enable_admission_service is set in the options)
    #     + 1 (for catalogd if enable_catalogd_ha is set in the options).
    if expected_subscribers == 0:
      expected_subscribers = expected_num_impalads + 1
      if "--enable_admission_service" in options:
        expected_subscribers += 1
      if "--enable_catalogd_ha" in options:
        expected_subscribers += 1
      elif "--no_catalogd" in options:
        expected_subscribers -= 1

    if wait_for_backends:
      statestored.service.wait_for_live_subscribers(expected_subscribers,
                                                    timeout=statestored_timeout_s)
      for impalad in cls.cluster.impalads:
        impalad.service.wait_for_num_known_live_backends(expected_num_impalads,
                                                         timeout=impalad_timeout_s)

  @classmethod
  def create_impala_service(cls):
    """Override ImpalaTestSuite to return 1st impalad of custom cluster.
    Returns None if no impalad was started."""
    return cls.cluster.impalads[0].service if cls.cluster.impalads else None

  def query_id_from_ui(self, section, match_func=None, match_query=None, coord_idx=0,
      max_attempts=30, sleep_time_s=1):
    """
      Calls to the debug UI's queries page and loops over all queries in the specified
      section calling the provided func for each query, Returns the string id of the
      first query that matches or None if no query matches.

      Parameters:
        section:     UI section where the query is located, valid values are
                     "completed_queries" or "in_flight_queries"
        match_func:  Function that takes a single argument (a dictionary) and returns
                     True if the query matches, False otherwise. The dictionary parameter
                     keys mirror the JSON returned by calling the /queries?json endpoint
                     on a coordinator. If specified, this is used instead of match_query.
        match_query: String of the exact query statement to match. If specified, this
                     is used instead of match_func.
        coord_idx:   Index of the Impalad to use as the coordinator. This is used to
                     determine which impalad's UI to query.
        max_attempts: Number of times to poll the queries page before giving up.
        sleep_time_s: Number of seconds to sleep between polls of the queries page.

      Returns:
        String of the query id of the first matching query or None if no query matches.
        Fails an assert if no matching query is found.
    """
    assert section == "completed_queries" or section == "in_flight_queries"
    assert match_func is not None or match_query is not None, \
        "Must specify either match_func or match_query"
    assert match_func is None or match_query is None, \
        "Cannot specify both match_func and match_query"

    if match_query is not None:
      match_query = match_query.lower().strip()

    attempts = 0

    while attempts < max_attempts:
      service = self.cluster.impalads[coord_idx].service
      queries_json = service.get_debug_webpage_json('/queries')

      for query in queries_json[section]:
        if (match_query is not None and query["stmt"].lower() == match_query.lower()) \
            or (match_func is not None and match_func(query)):
          query_id = query['query_id']
          return query_id, service.read_query_profile_page(query_id)

      attempts += 1
      sleep(sleep_time_s)

    assert False, "No matching query found in section '{}' after {} " \
        "attempts".format(section, max_attempts)

  def query_profile_from_ui(self, query_id, coord_idx=0):
    """
      Wrapper function around ImpaladService.read_query_profile_page() to fetch the query
      profile for a given query id from the UI of the specified coordinator.

      Parameters:
        query_id:  String id of the query to fetch the profile for.
        coord_idx: Index of the Impalad to use as the coordinator. This is used to
                   determine which impalad's UI to query.

      Returns:
        String of the query profile.
    """
    service = self.cluster.impalads[coord_idx].service
    return service.read_query_profile_page(query_id)
