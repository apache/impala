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

import logging
import os
import os.path
import pipes
import pytest
import subprocess
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
KUDU_ARGS = 'kudu_args'
# Additional args passed to the start-impala-cluster script.
START_ARGS = 'start_args'
HIVE_CONF_DIR = 'hive_conf_dir'
CLUSTER_SIZE = "cluster_size"
# Default query options passed to the impala daemon command line. Handled separately from
# other impala daemon arguments to allow merging multiple defaults into a single list.
DEFAULT_QUERY_OPTIONS = 'default_query_options'
IMPALA_LOG_DIR = 'impala_log_dir'
NUM_EXCLUSIVE_COORDINATORS = 'num_exclusive_coordinators'
STATESTORED_TIMEOUT_S = 'statestored_timeout_s'
IMPALAD_TIMEOUT_S = 'impalad_timeout_s'

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
    super(CustomClusterTestSuite, cls).add_test_dimensions()
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
      start_args=None, default_query_options=None,
      impala_log_dir=None, hive_conf_dir=None, cluster_size=None,
      num_exclusive_coordinators=None, kudu_args=None, statestored_timeout_s=None,
      impalad_timeout_s=None):
    """Records arguments to be passed to a cluster by adding them to the decorated
    method's func_dict"""
    def decorate(func):
      if impalad_args is not None:
        func.func_dict[IMPALAD_ARGS] = impalad_args
      func.func_dict[STATESTORED_ARGS] = statestored_args
      if catalogd_args is not None:
        func.func_dict[CATALOGD_ARGS] = catalogd_args
      if start_args is not None:
        func.func_dict[START_ARGS] = start_args.split()
      if hive_conf_dir is not None:
        func.func_dict[HIVE_CONF_DIR] = hive_conf_dir
      if kudu_args is not None:
        func.func_dict[KUDU_ARGS] = kudu_args
      if default_query_options is not None:
        func.func_dict[DEFAULT_QUERY_OPTIONS] = default_query_options
      if impala_log_dir is not None:
        func.func_dict[IMPALA_LOG_DIR] = impala_log_dir
      if cluster_size is not None:
        func.func_dict[CLUSTER_SIZE] = cluster_size
      if num_exclusive_coordinators is not None:
        func.func_dict[NUM_EXCLUSIVE_COORDINATORS] = num_exclusive_coordinators
      if statestored_timeout_s is not None:
        func.func_dict[STATESTORED_TIMEOUT_S] = statestored_timeout_s
      if impalad_timeout_s is not None:
        func.func_dict[IMPALAD_TIMEOUT_S] = impalad_timeout_s
      return func
    return decorate

  def setup_method(self, method):
    cluster_args = list()
    for arg in [IMPALAD_ARGS, STATESTORED_ARGS, CATALOGD_ARGS]:
      if arg in method.func_dict:
        cluster_args.append("--%s=%s " % (arg, method.func_dict[arg]))
    if START_ARGS in method.func_dict:
      cluster_args.extend(method.func_dict[START_ARGS])

    if HIVE_CONF_DIR in method.func_dict:
      self._start_hive_service(method.func_dict[HIVE_CONF_DIR])
      # Should let Impala adopt the same hive-site.xml. The only way is to add it in the
      # beginning of the CLASSPATH. Because there's already a hive-site.xml in the
      # default CLASSPATH (see bin/set-classpath.sh).
      cluster_args.append(
        '--env_vars=CUSTOM_CLASSPATH=%s ' % method.func_dict[HIVE_CONF_DIR])

    if KUDU_ARGS in method.func_dict:
      self._restart_kudu_service(method.func_dict[KUDU_ARGS])

    cluster_size = DEFAULT_CLUSTER_SIZE
    if CLUSTER_SIZE in method.func_dict:
      cluster_size = method.func_dict[CLUSTER_SIZE]

    use_exclusive_coordinators = False
    num_coordinators = cluster_size
    if NUM_EXCLUSIVE_COORDINATORS in method.func_dict:
      num_coordinators = method.func_dict[NUM_EXCLUSIVE_COORDINATORS]
      use_exclusive_coordinators = True

    # Start a clean new cluster before each test
    kwargs = {
      "cluster_size": cluster_size,
      "num_coordinators": num_coordinators,
      "expected_num_executors": cluster_size,
      "default_query_options": method.func_dict.get(DEFAULT_QUERY_OPTIONS),
      "use_exclusive_coordinators": use_exclusive_coordinators
    }
    if IMPALA_LOG_DIR in method.func_dict:
      kwargs["impala_log_dir"] = method.func_dict[IMPALA_LOG_DIR]
    if STATESTORED_TIMEOUT_S in method.func_dict:
      kwargs["statestored_timeout_s"] = method.func_dict[STATESTORED_TIMEOUT_S]
    if IMPALAD_TIMEOUT_S in method.func_dict:
      kwargs["impalad_timeout_s"] = method.func_dict[IMPALAD_TIMEOUT_S]
    self._start_impala_cluster(cluster_args, **kwargs)
    super(CustomClusterTestSuite, self).setup_class()

  def teardown_method(self, method):
    if HIVE_CONF_DIR in method.func_dict:
      self._start_hive_service(None)  # Restart Hive Service using default configs
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
  def _start_impala_cluster(cls,
                            options,
                            impala_log_dir=os.getenv('LOG_DIR', "/tmp/"),
                            cluster_size=DEFAULT_CLUSTER_SIZE,
                            num_coordinators=NUM_COORDINATORS,
                            use_exclusive_coordinators=False,
                            add_executors=False,
                            log_level=1,
                            expected_num_executors=DEFAULT_CLUSTER_SIZE,
                            expected_subscribers=0,
                            default_query_options=None,
                            statestored_timeout_s=60,
                            impalad_timeout_s=60):
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

    if use_exclusive_coordinators:
      cmd.append("--use_exclusive_coordinators")

    if add_executors:
      cmd.append("--add_executors")

    if pytest.config.option.use_local_catalog:
      cmd.append("--impalad_args=--use_local_catalog=1")
      cmd.append("--catalogd_args=--catalog_topic_mode=minimal")

    default_query_option_kvs = []
    # Put any defaults first, then any arguments after that so they can override defaults.
    if os.environ.get("ERASURE_CODING") == "true":
      default_query_option_kvs.append(("allow_erasure_coded_files", "true"))
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
    # cluster_size (# of impalad) + 1 (for catalogd).
    if expected_subscribers == 0:
      expected_subscribers = expected_num_executors + 1

    statestored.service.wait_for_live_subscribers(expected_subscribers,
                                                  timeout=statestored_timeout_s)
    for impalad in cls.cluster.impalads:
      impalad.service.wait_for_num_known_live_backends(expected_num_executors,
                                                       timeout=impalad_timeout_s)
