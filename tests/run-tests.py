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
# and then executing the remaining tests in parallel. All additional command line options
# are passed to py.test.
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_service import ImpaladService
import itertools
import json
import multiprocessing
import os
import pytest
import sys

from _pytest.config import FILE_OR_DIR

# We whitelist valid test directories. If a new test directory is added, update this.
VALID_TEST_DIRS = ['failure', 'query_test', 'stress', 'unittests', 'aux_query_tests',
                   'shell', 'hs2', 'catalog_service', 'metadata', 'data_errors',
                   'statestore']

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


class TestExecutor:
  def __init__(self, exit_on_error=True):
    self._exit_on_error = exit_on_error
    self.tests_failed = False

  def run_tests(self, args):
    try:
      exit_code = pytest.main(args)
    except:
      sys.stderr.write("Unexpected exception with pytest {0}".format(args))
      raise
    if exit_code != 0 and self._exit_on_error:
      sys.exit(exit_code)
    self.tests_failed = exit_code != 0 or self.tests_failed


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
  for arg, log in LOGGING_ARGS.iteritems():
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
  subdirs = [subdir for subdir in os.listdir(TEST_DIR) if os.path.isdir(subdir)]
  ignored_dir_list = []
  for subdir in (set(subdirs) - set(valid_dirs)):
    ignored_dir_list += ['--ignore', subdir]
  return ignored_dir_list


def print_metrics(substring):
  """Prints metrics with the given substring in the name"""
  for impalad in ImpalaCluster().impalads:
    print ">" * 80
    port = impalad._get_webserver_port()
    print "connections metrics for impalad at port {0}:".format(port)
    debug_info = json.loads(ImpaladService(
            impalad.hostname,
            webserver_port=port)
            .open_debug_webpage('metrics?json').read())
    for metric in debug_info['metric_group']['metrics']:
      if substring in metric['name']:
        print json.dumps(metric, indent=1)
    print "<" * 80


if __name__ == "__main__":
  exit_on_error = '-x' in sys.argv or '--exitfirst' in sys.argv
  test_executor = TestExecutor(exit_on_error=exit_on_error)

  # If the user is just asking for --help, just print the help test and then exit.
  if '-h' in sys.argv[1:] or '--help' in sys.argv[1:]:
    test_executor.run_tests(sys.argv[1:])
    sys.exit(0)

  os.chdir(TEST_DIR)

  # Create the test result directory if it doesn't already exist.
  if not os.path.exists(RESULT_DIR):
    os.makedirs(RESULT_DIR)

  # First run query tests that need to be executed serially
  base_args = ['-m', 'execute_serially']
  test_executor.run_tests(base_args + build_test_args('serial'))

  # Run the stress tests tests
  if '--collect-only' not in sys.argv:
    print_metrics('connections')
  base_args = ['-m', 'stress', '-n', NUM_STRESS_CLIENTS]
  test_executor.run_tests(base_args + build_test_args('stress'))
  if '--collect-only' not in sys.argv:
    print_metrics('connections')

  # Run the remaining query tests in parallel
  base_args = ['-m', 'not execute_serially and not stress', '-n', NUM_CONCURRENT_TESTS]
  test_executor.run_tests(base_args + build_test_args('parallel'))

  # Finally, validate impalad/statestored metrics.
  args = build_test_args(base_name='verify-metrics', valid_dirs=['verifiers'])
  args.append('verifiers/test_verify_metrics.py')
  test_executor.run_tests(args)

  if test_executor.tests_failed:
    sys.exit(1)
