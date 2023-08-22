#!/usr/bin/env impala-python
#
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
# Runs the Impala query tests, first executing the tests that cannot be run in parallel
# (the serial tests), then executing the stress tests, and then
# executing the remaining tests in parallel. To run only some of
# these, use --skip-serial, --skip-stress, or --skip-parallel.
# All additional command line options are passed to py.test.
from __future__ import absolute_import, division, print_function
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_service import ImpaladService
from tests.conftest import configure_logging
import itertools
import json
import multiprocessing
import os
import pytest
import sys
from _pytest.main import EXIT_NOTESTSCOLLECTED
from _pytest.config import FILE_OR_DIR

# We whitelist valid test directories. If a new test directory is added, update this.
VALID_TEST_DIRS = ['failure', 'query_test', 'stress', 'unittests', 'aux_query_tests',
                   'shell', 'hs2', 'catalog_service', 'metadata', 'data_errors',
                   'statestore', 'infra', 'observability', 'webserver']

# A list of helper directories that do not contain any tests. The purpose of this
# additional list is to prevent devs from adding a new test dir, but not adding the
# new dir to the list of valid test dirs above. All dirs unders tests/ must be placed
# into one of these lists, otherwise the script will throw an error. This list can be
# removed once IMPALA-4417 has been resolved.
TEST_HELPER_DIRS = ['aux_parquet_data_load', 'comparison', 'benchmark',
                     'custom_cluster', 'util', 'experiments', 'verifiers', 'common',
                     'performance', 'beeswax', 'aux_custom_cluster_tests',
                     'authorization', 'test-hive-udfs']

TEST_DIR = os.path.join(os.environ['IMPALA_HOME'], 'tests')
RESULT_DIR = os.path.join(os.environ['IMPALA_EE_TEST_LOGS_DIR'], 'results')

# Arguments that control output logging. If additional default arguments are needed they
# should go in the pytest.ini file.
LOGGING_ARGS = {'--junitxml': 'TEST-impala-{0}.xml',
                '--resultlog': 'TEST-impala-{0}.log'}

# Default the number of concurrent tests defaults to the cpu cores in the system.
# This can be overridden by setting the NUM_CONCURRENT_TESTS environment variable.
NUM_CONCURRENT_TESTS = multiprocessing.cpu_count()
if 'NUM_CONCURRENT_TESTS' in os.environ:
  NUM_CONCURRENT_TESTS = int(os.environ['NUM_CONCURRENT_TESTS'])

# Default the number of stress clinets to 4x the number of CPUs (but not exceeding the
# default max # of concurrent connections)
# This can be overridden by setting the NUM_STRESS_CLIENTS environment variable.
# TODO: fix the stress test so it can start more clients than available connections
# without deadlocking (e.g. close client after each test instead of on test class
# teardown).
NUM_STRESS_CLIENTS = min(multiprocessing.cpu_count() * 4, 64)
if 'NUM_STRESS_CLIENTS' in os.environ:
  NUM_STRESS_CLIENTS = int(os.environ['NUM_STRESS_CLIENTS'])

class TestCounterPlugin(object):
  """ Custom pytest plugin to count the number of tests
  collected and executed over multiple pytest runs

  tests_collected is set of nodeids for collected tests
  tests_executed is set of nodeids for executed tests
  """
  def __init__(self):
    self.tests_collected = set()
    self.tests_executed = set()

  # pytest hook to handle test collection when xdist is used (parallel tests)
  # https://github.com/pytest-dev/pytest-xdist/pull/35/commits (No official documentation available)
  def pytest_xdist_node_collection_finished(self, node, ids):
      self.tests_collected.update(set(ids))

  # link to pytest_collection_modifyitems
  # https://docs.pytest.org/en/2.9.2/writing_plugins.html#_pytest.hookspec.pytest_collection_modifyitems
  def pytest_collection_modifyitems(self, items):
      for item in items:
          self.tests_collected.add(item.nodeid)

  # link to pytest_runtest_logreport
  # https://docs.pytest.org/en/2.9.2/_modules/_pytest/hookspec.html#pytest_runtest_logreport
  def pytest_runtest_logreport(self, report):
    if report.passed:
       self.tests_executed.add(report.nodeid)

class TestExecutor(object):
  def __init__(self, exit_on_error=True):
    self._exit_on_error = exit_on_error
    self.tests_failed = False
    self.total_executed = 0

  def run_tests(self, args):
    testcounterplugin = TestCounterPlugin()

    try:
      pytest_exit_code = pytest.main(args, plugins=[testcounterplugin])
    except:
      sys.stderr.write("Unexpected exception with pytest {0}".format(args))
      raise

    if '--collect-only' in args:
      for test in testcounterplugin.tests_collected:
        print(test)

    self.total_executed += len(testcounterplugin.tests_executed)

    if 0 < pytest_exit_code < EXIT_NOTESTSCOLLECTED and self._exit_on_error:
      sys.exit(pytest_exit_code)
    self.tests_failed = 0 < pytest_exit_code < EXIT_NOTESTSCOLLECTED or self.tests_failed

def build_test_args(base_name, valid_dirs=VALID_TEST_DIRS):
  """
  Prepare the list of arguments that will be passed to pytest.main().

  Args:
    base_name: the base name for the log file to write
    valid_dirs: a white list of sub-directories with desired tests (i.e, those
      that will not get flagged with --ignore before py.test is called.)

  Return:
    a list of command line arguments

  For most test stages (e.g., serial, parallel), we augment the given command
  line arguments with a list of directories to ignore. However, when running the
  metric verification tests at the end of the test run:

  - verifiers.test_verify_metrics.TestValidateMetrics.test_metrics_are_zero
  - verifiers.test_verify_metrics.TestValidateMetrics.test_num_unused_buffers

  then we instead need to filter out args that specifiy other tests (otherwise,
  they will be run again), but still retain the basic config args.
  """

  # When building the list of command line args, in order to correctly filter
  # them as needed (see issue IMPALA-4510) we should account for the fact that
  # '--foo bar' and '--foo=bar' might be supplied by the user. We also need to
  # be able identify any other arbitrary options. E.g., if the user specified
  # the following on the command line:
  #
  #   'run-tests.py --arg1 value1 --random_opt --arg2=value2'
  #
  # we want an iterable that, if unpacked as a list, would look like:
  #
  #   [arg1, value1, random_opt, arg2, value2]
  #
  commandline_args = itertools.chain(*[arg.split('=') for arg in sys.argv[1:]])

  ignored_dirs = build_ignore_dir_arg_list(valid_dirs=valid_dirs)
  logging_args = []
  for arg, log in LOGGING_ARGS.items():
    logging_args.extend([arg, os.path.join(RESULT_DIR, log.format(base_name))])

  if valid_dirs != ['verifiers']:
    # This isn't the metrics verification stage yet, so we don't need to filter.
    test_args = ignored_dirs + logging_args + list(commandline_args)
  else:
    # For metrics verification, we only want to run the verifier tests, so we need
    # to filter out any command line args that specify other test modules, classes,
    # and functions. The list of these can be found by calling
    #
    #    pytest.config.getoption(FILE_OR_DIR)
    #
    # For example, with the following command line invocation:
    #
    # $ ./run-tests.py query_test/test_limit.py::TestLimit::test_limit \
    #   query_test/test_queries.py::TestHdfsQueries --verbose -n 4 \
    #   --table_formats=parquet/none --exploration_strategy core
    #
    # then pytest.config.getoption(FILE_OR_DIR) will return a list of two elements:
    #
    # ['query_test/test_limit.py::TestLimit::test_limit',
    #  'query_test/test_queries.py::TestHdfsQueries']
    #
    explicit_tests = pytest.config.getoption(FILE_OR_DIR)
    config_options = [arg for arg in commandline_args if arg not in explicit_tests]
    # We also want to strip out any --shard_tests option and its corresponding value.
    while "--shard_tests" in config_options:
      i = config_options.index("--shard_tests")
      del config_options[i:i+2]
    test_args = ignored_dirs + logging_args + config_options

  return test_args


def build_ignore_dir_arg_list(valid_dirs):
  """ Builds a list of directories to ignore

  Return:
    a list ['--ignore', 'dir1', '--ignore', 'dir2', etc...]

  Because we have several non-test directories and files in our tests/ path, pytest
  can have auto-discovery problems -- i.e., pytest may try to execute some non-test
  code as though it contained tests, resulting in misleading warnings or failures.
  (There is a JIRA filed to restructure this: IMPALA-4417.)
  """
  subdirs = [subdir for subdir in os.listdir(TEST_DIR)
      if os.path.isdir(subdir) and not subdir.startswith(".")]
  for subdir in subdirs:
      assert subdir in VALID_TEST_DIRS or subdir in TEST_HELPER_DIRS,\
        "Unexpected test dir '%s' is not in the list of valid or helper test dirs"\
        % subdir
  ignored_dir_list = []
  for subdir in (set(subdirs) - set(valid_dirs)):
    ignored_dir_list += ['--ignore', subdir]
  return ignored_dir_list


def print_metrics(substring):
  """Prints metrics with the given substring in the name"""
  for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
    print(">" * 80)
    port = impalad.get_webserver_port()
    cert = impalad._get_webserver_certificate_file()
    print("connections metrics for impalad at port {0}:".format(port))
    debug_info = json.loads(ImpaladService(impalad.hostname, webserver_port=port,
        webserver_certificate_file=cert).read_debug_webpage('metrics?json'))
    for metric in debug_info['metric_group']['metrics']:
      if substring in metric['name']:
        print(json.dumps(metric, indent=1))
    print("<" * 80)


def detect_and_remove_flag(flag):
  """Find any usage of 'flag' in sys.argv and remove them. Return true if the
     flag is found. Return false otherwise."""
  flag_exists = False
  # Handle multiple occurrences of the same flag
  while flag in sys.argv:
    flag_exists = True
    sys.argv.remove(flag)
  return flag_exists


if __name__ == "__main__":
  # Ensure that logging is configured for the 'run-test.py' wrapper itself.
  configure_logging()
  exit_on_error = '-x' in sys.argv or '--exitfirst' in sys.argv
  skip_serial = detect_and_remove_flag('--skip-serial')
  skip_stress = detect_and_remove_flag('--skip-stress')
  skip_parallel = detect_and_remove_flag('--skip-parallel')
  test_executor = TestExecutor(exit_on_error=exit_on_error)

  # If the user is just asking for --help, just print the help test and then exit.
  if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
    test_executor.run_tests(sys.argv[1:])
    sys.exit(0)

  def run(args):
    """Helper to print out arguments of test_executor before invoking."""
    print("Running TestExecutor with args: %s" % (args,))
    test_executor.run_tests(args)

  os.chdir(TEST_DIR)

  # Create the test result directory if it doesn't already exist.
  if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)

  # If you like to avoid verbose output the following
  # adding -p no:terminal to --collect-only will suppress
  # pytest warnings/messages and displays collected tests

  if '--collect-only' in sys.argv:
    run(sys.argv[1:])
  else:
    print_metrics('connections')

    # If using sharding, it is useful to include it in the output filenames so that
    # different shards don't overwrite each other. If not using sharding, use the
    # normal filenames. This does not validate the shard_tests argument.
    shard_identifier = ""
    shard_arg = None
    for idx, arg in enumerate(sys.argv):
      # This deliberately does not stop at the first occurrence. It continues through
      # all the arguments to find the last occurrence of shard_tests.
      if arg == "--shard_tests":
        # Form 1: --shard_tests N/M (space separation => grab next argument)
        assert idx + 1 < len(sys.argv), "shard_args expects an argument"
        shard_arg = sys.argv[idx + 1]
      elif "--shard_tests=" in arg:
        # Form 2: --shard_tests=N/M
        shard_arg = arg.replace("--shard_tests=", "")

    if shard_arg:
      # The shard argument is "N/M" where N <= M. Convert to a string that can be used
      # in a filename.
      shard_identifier = "_shard_{0}".format(shard_arg.replace("/", "_"))

    # First run query tests that need to be executed serially
    if not skip_serial:
      base_args = ['-m', 'execute_serially']
      run(base_args + build_test_args("serial{0}".format(shard_identifier)))
      print_metrics('connections')

    # Run the stress tests
    if not skip_stress:
      base_args = ['-m', 'stress', '-n', NUM_STRESS_CLIENTS]
      run(base_args + build_test_args("stress{0}".format(shard_identifier)))
      print_metrics('connections')

    # Run the remaining query tests in parallel
    if not skip_parallel:
      base_args = ['-m', 'not execute_serially and not stress', '-n', NUM_CONCURRENT_TESTS]
      run(base_args + build_test_args("parallel{0}".format(shard_identifier)))

    # The total number of tests executed at this point is expected to be >0
    # If it is < 0 then the script needs to exit with a non-zero
    # error code indicating an error in test execution
    if test_executor.total_executed == 0:
      sys.exit(1)

    # Finally, validate impalad/statestored metrics.
    args = build_test_args(base_name="verify-metrics{0}".format(shard_identifier),
                           valid_dirs=['verifiers'])
    args.append('verifiers/test_verify_metrics.py')
    run(args)

  if test_executor.tests_failed:
    sys.exit(1)
