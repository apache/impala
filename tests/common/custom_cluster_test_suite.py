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
import logging
import os
import os.path
import pipes
import pytest
import subprocess
from impala_py_lib.helpers import find_all_files, is_core_dump
from signal import SIGRTMIN
from subprocess import check_call
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.impala_cluster import ImpalaCluster
from tests.util.filesystem_utils import IS_LOCAL
from time import sleep

IMPALA_HOME = os.environ['IMPALA_HOME']
DEFAULT_CLUSTER_SIZE = 3
NUM_COORDINATORS = DEFAULT_CLUSTER_SIZE

# Additional args passed to respective daemon command line.
IMPALAD_ARGS = 'impalad_args'
STATESTORED_ARGS = 'state_store_args'
CATALOGD_ARGS = 'catalogd_args'
ADMISSIOND_ARGS = 'admissiond_args'
KUDU_ARGS = 'kudu_args'
# Additional args passed to the start-impala-cluster script.
START_ARGS = 'start_args'
JVM_ARGS = 'jvm_args'
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
# By default, Impalad processes are left running at the end of the test. The next test run
# terminates the processes when calling the `bin/start-impala-cluster.py`` script. Setting
# this to `True` causes Impalad processes to be sent the SIGRTMIN signal at the end of the
# test which runs the Impalad shutdown steps instead of abruptly ending the process.
IMPALAD_GRACEFUL_SHUTDOWN = 'impalad_graceful_shutdown'

# Run with fast topic updates by default to reduce time to first query running.
DEFAULT_STATESTORE_ARGS = '--statestore_update_frequency_ms=50 \
    --statestore_priority_update_frequency_ms=50 \
    --statestore_heartbeat_frequency_ms=50'


class CustomClusterTestSuite(ImpalaTestSuite):
  """Every test in a test suite deriving from this class gets its own Impala cluster.
  Custom arguments may be passed to the cluster by using the @with_args decorator."""
  @classmethod
  def get_workload(cls):
    return 'tpch'

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
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('exec_option')['batch_size'] == 0 and
        v.get_value('exec_option')['disable_codegen'] == False and
        v.get_value('exec_option')['num_nodes'] == 0)

  @classmethod
  def setup_class(cls):
    # Explicit override of ImpalaTestSuite.setup_class(). For custom cluster, the
    # ImpalaTestSuite.setup_class() procedure needs to happen on a per-method basis.
    # IMPALA-3614: @SkipIfLocal.multiple_impalad workaround
    # IMPALA-2943 TODO: When pytest is upgraded, see if this explicit skip can be
    # removed in favor of the class-level SkipifLocal.multiple_impalad decorator.
    if IS_LOCAL:
      pytest.skip("multiple impalads needed")

  @classmethod
  def teardown_class(cls):
    # Explicit override of ImpalaTestSuite.teardown_class(). For custom cluster, the
    # ImpalaTestSuite.teardown_class() procedure needs to happen on a per-method basis.
    pass

  @staticmethod
  def with_args(impalad_args=None, statestored_args=None, catalogd_args=None,
      start_args=None, default_query_options=None, jvm_args=None,
      impala_log_dir=None, hive_conf_dir=None, cluster_size=None,
      num_exclusive_coordinators=None, kudu_args=None, statestored_timeout_s=None,
      impalad_timeout_s=None, expect_cores=None, reset_ranger=False,
      impalad_graceful_shutdown=False):
    """Records arguments to be passed to a cluster by adding them to the decorated
    method's func_dict"""
    def decorate(func):
      if impalad_args is not None:
        func.__dict__[IMPALAD_ARGS] = impalad_args
      func.__dict__[STATESTORED_ARGS] = statestored_args
      if catalogd_args is not None:
        func.__dict__[CATALOGD_ARGS] = catalogd_args
      if start_args is not None:
        func.__dict__[START_ARGS] = start_args.split()
      if jvm_args is not None:
        func.__dict__[JVM_ARGS] = jvm_args
      if hive_conf_dir is not None:
        func.__dict__[HIVE_CONF_DIR] = hive_conf_dir
      if kudu_args is not None:
        func.__dict__[KUDU_ARGS] = kudu_args
      if default_query_options is not None:
        func.__dict__[DEFAULT_QUERY_OPTIONS] = default_query_options
      if impala_log_dir is not None:
        func.__dict__[IMPALA_LOG_DIR] = impala_log_dir
      if cluster_size is not None:
        func.__dict__[CLUSTER_SIZE] = cluster_size
      if num_exclusive_coordinators is not None:
        func.__dict__[NUM_EXCLUSIVE_COORDINATORS] = num_exclusive_coordinators
      if statestored_timeout_s is not None:
        func.__dict__[STATESTORED_TIMEOUT_S] = statestored_timeout_s
      if impalad_timeout_s is not None:
        func.__dict__[IMPALAD_TIMEOUT_S] = impalad_timeout_s
      if expect_cores is not None:
        func.__dict__[EXPECT_CORES] = expect_cores
      if reset_ranger is not False:
        func.__dict__[RESET_RANGER] = True
      if impalad_graceful_shutdown is not False:
        func.__dict__[IMPALAD_GRACEFUL_SHUTDOWN] = True
      return func
    return decorate

  def setup_method(self, method):
    cluster_args = list()
    if method.__dict__.get(IMPALAD_GRACEFUL_SHUTDOWN, False):
      # IMPALA-13051: Add faster default graceful shutdown options before processing
      # explicit args. Impala doesn't start graceful shutdown until the grace period has
      # passed, and most tests that use graceful shutdown are testing flushing the query
      # log, which doesn't start until after the grace period has passed.
      cluster_args.append(
          "--impalad=--shutdown_grace_period_s=0 --shutdown_deadline_s=15")
    for arg in [IMPALAD_ARGS, STATESTORED_ARGS, CATALOGD_ARGS, ADMISSIOND_ARGS, JVM_ARGS]:
      if arg in method.__dict__:
        cluster_args.append("--%s=%s " % (arg, method.__dict__[arg]))
    if START_ARGS in method.__dict__:
      cluster_args.extend(method.__dict__[START_ARGS])

    if HIVE_CONF_DIR in method.__dict__:
      self._start_hive_service(method.__dict__[HIVE_CONF_DIR])
      # Should let Impala adopt the same hive-site.xml. The only way is to add it in the
      # beginning of the CLASSPATH. Because there's already a hive-site.xml in the
      # default CLASSPATH (see bin/set-classpath.sh).
      cluster_args.append(
        '--env_vars=CUSTOM_CLASSPATH=%s ' % method.__dict__[HIVE_CONF_DIR])

    if KUDU_ARGS in method.__dict__:
      self._restart_kudu_service(method.__dict__[KUDU_ARGS])

    if RESET_RANGER in method.__dict__:
      self._reset_ranger_policy_repository()

    cluster_size = DEFAULT_CLUSTER_SIZE
    if CLUSTER_SIZE in method.__dict__:
      cluster_size = method.__dict__[CLUSTER_SIZE]

    use_exclusive_coordinators = False
    num_coordinators = cluster_size
    if NUM_EXCLUSIVE_COORDINATORS in method.__dict__:
      num_coordinators = method.__dict__[NUM_EXCLUSIVE_COORDINATORS]
      use_exclusive_coordinators = True

    # Start a clean new cluster before each test
    kwargs = {
      "cluster_size": cluster_size,
      "num_coordinators": num_coordinators,
      "expected_num_impalads": cluster_size,
      "default_query_options": method.__dict__.get(DEFAULT_QUERY_OPTIONS),
      "use_exclusive_coordinators": use_exclusive_coordinators
    }
    if IMPALA_LOG_DIR in method.__dict__:
      kwargs["impala_log_dir"] = method.__dict__[IMPALA_LOG_DIR]
    if STATESTORED_TIMEOUT_S in method.__dict__:
      kwargs["statestored_timeout_s"] = method.__dict__[STATESTORED_TIMEOUT_S]
    if IMPALAD_TIMEOUT_S in method.__dict__:
      kwargs["impalad_timeout_s"] = method.__dict__[IMPALAD_TIMEOUT_S]

    if method.__dict__.get(EXPECT_CORES, False):
      # Make a note of any core files that already exist
      possible_cores = find_all_files('*core*')
      self.pre_test_cores = set([f for f in possible_cores if is_core_dump(f)])

      # Explicitly allow startup to exception, since startup is expected to fail
      try:
        self._start_impala_cluster(cluster_args, **kwargs)
        pytest.fail("cluster startup should have failed")
      except Exception:
        self._stop_impala_cluster()
    else:
      self._start_impala_cluster(cluster_args, **kwargs)
      super(CustomClusterTestSuite, self).setup_class()

  def teardown_method(self, method):
    if method.__dict__.get(IMPALAD_GRACEFUL_SHUTDOWN, False):
      for impalad in self.cluster.impalads:
        impalad.kill(SIGRTMIN)
      for impalad in self.cluster.impalads:
        impalad.wait_for_exit()

    if HIVE_CONF_DIR in method.__dict__:
      self._start_hive_service(None)  # Restart Hive Service using default configs

    if method.__dict__.get(EXPECT_CORES, False):
      # The core dumps expected to be generated by this test should be cleaned up
      possible_cores = find_all_files('*core*')
      post_test_cores = set([f for f in possible_cores if is_core_dump(f)])

      for f in (post_test_cores - self.pre_test_cores):
        logging.info(
            "Cleaned up {core} created by {name}".format(core=f, name=method.__name__))
        os.remove(f)
      # Skip teardown_class as setup was skipped.
    else:
      super(CustomClusterTestSuite, self).teardown_class()

  @classmethod
  def _stop_impala_cluster(cls):
    # TODO: Figure out a better way to handle case where processes are just starting
    # / cleaning up so that sleeps are not needed.
    sleep(2)
    check_call([os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'), '--kill_only'])
    sleep(2)

  @classmethod
  def _restart_kudu_service(cls, kudu_args=None):
    kudu_env = dict(os.environ)
    if kudu_args is not None:
      kudu_env["IMPALA_KUDU_STARTUP_FLAGS"] = kudu_args
    call = subprocess.Popen(
        ['/bin/bash', '-c', os.path.join(IMPALA_HOME,
                                         'testdata/cluster/admin restart kudu')],
        env=kudu_env)
    call.wait()
    if call.returncode != 0:
      raise RuntimeError("Unable to restart Kudu")

  @classmethod
  def _start_hive_service(cls, hive_conf_dir):
    hive_env = dict(os.environ)
    if hive_conf_dir is not None:
      hive_env['HIVE_CONF_DIR'] = hive_conf_dir
    call = subprocess.Popen(
      ['/bin/bash', '-c', os.path.join(IMPALA_HOME, 'testdata/bin/run-hive-server.sh')],
      env=hive_env)
    call.wait()
    if call.returncode != 0:
      raise RuntimeError("Unable to start Hive")

  @classmethod
  def _stop_hive_service(cls):
    subprocess.check_call([os.path.join(IMPALA_HOME,
                                        "testdata/bin/kill-hive-server.sh")],
                          close_fds=True)

  @classmethod
  def _reset_ranger_policy_repository(cls):
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
  def _start_impala_cluster(cls,
                            options,
                            impala_log_dir=os.getenv('LOG_DIR', "/tmp/"),
                            cluster_size=DEFAULT_CLUSTER_SIZE,
                            num_coordinators=NUM_COORDINATORS,
                            use_exclusive_coordinators=False,
                            add_executors=False,
                            log_level=1,
                            expected_num_impalads=DEFAULT_CLUSTER_SIZE,
                            expected_subscribers=0,
                            default_query_options=None,
                            statestored_timeout_s=60,
                            impalad_timeout_s=60,
                            ignore_pid_on_log_rotation=False):
    cls.impala_log_dir = impala_log_dir
    # We ignore TEST_START_CLUSTER_ARGS here. Custom cluster tests specifically test that
    # certain custom startup arguments work and we want to keep them independent of dev
    # environments.
    cmd = [os.path.join(IMPALA_HOME, 'bin/start-impala-cluster.py'),
           '--state_store_args=%s' % DEFAULT_STATESTORE_ARGS,
           '--cluster_size=%d' % cluster_size,
           '--num_coordinators=%d' % num_coordinators,
           '--log_dir=%s' % impala_log_dir,
           '--log_level=%s' % log_level]

    if ignore_pid_on_log_rotation:
      # IMPALA-12595: Ignore PID on log rotation for all custom cluster tests.
      # Most of test in custom_cluster need to match PID, except some test for logging.
      cmd.append('--ignore_pid_on_log_rotation')

    if use_exclusive_coordinators:
      cmd.append("--use_exclusive_coordinators")

    if add_executors:
      cmd.append("--add_executors")

    if pytest.config.option.use_local_catalog:
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

    logging.info("Starting cluster with command: %s" %
                 " ".join(pipes.quote(arg) for arg in cmd + options))
    try:
      check_call(cmd + options, close_fds=True)
    finally:
      # Failure tests expect cluster to be initialised even if start-impala-cluster fails.
      cls.cluster = ImpalaCluster.get_e2e_test_cluster()
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

    statestored.service.wait_for_live_subscribers(expected_subscribers,
                                                  timeout=statestored_timeout_s)
    for impalad in cls.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(expected_num_impalads,
                                                       timeout=impalad_timeout_s)
