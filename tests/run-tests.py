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

# We whitelist valid test directories. If a new test directory is added, update this.
VALID_TEST_DIRS = ['failure', 'query_test', 'stress', 'unittests', 'aux_query_tests',
                   'shell', 'hs2', 'catalog_service', 'metadata', 'data_errors',
                   'statestore']

TEST_DIR = os.path.join(os.environ['IMPALA_HOME'], 'tests')
TEST_RESULT_DIR = os.path.join(os.environ['IMPALA_EE_TEST_LOGS_DIR'], 'results')

# Arguments that control output logging. If additional default arguments are needed they
# should go in the pytest.ini file.
LOGGING_ARGS = '--junitxml=%(result_dir)s/TEST-impala-%(log_name)s.xml '\
               '--resultlog=%(result_dir)s/TEST-impala-%(log_name)s.log'

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
      sys.stderr.write("Unexpected exception with pytest {}".format(args))
      raise
    if exit_code != 0 and self._exit_on_error:
      sys.exit(exit_code)
    self.tests_failed = exit_code != 0 or self.tests_failed


def build_test_args(log_base_name, valid_dirs):
  """
  Modify and return the command line arguments that will be passed to py.test.

  Args:
    log_base_name: the base name for the log file to write
    valid_dirs: a white list of sub-directories with desired tests (i.e, those
      that will not get flagged with --ignore before py.test is called.)

  Return:
    a modified command line for py.test

  For most test stages (e.g., serial, parallel), we augment the given command
  line arguments with a list of directories to ignore. However, when running the
  metric verification tests at the end of the test run:

  - verifiers.test_verify_metrics.TestValidateMetrics.test_metrics_are_zero
  - verifiers.test_verify_metrics.TestValidateMetrics.test_num_unused_buffers

  then we instead need to filter out args that specifiy other tests (otherwise,
  they will be run again), but still retain the basic config args.
  """
  logging_args = LOGGING_ARGS % {'result_dir': TEST_RESULT_DIR,
                                 'log_name': log_base_name}

  # The raw command line arguments need to be modified because of the way our
  # repo is organized. We have several non-test directories and files in our
  # tests/ path, which causes auto-discovery problems for pytest -- i.e., pytest
  # will futiley try to execute them as tests, resulting in misleading failures.
  # (There is a JIRA filed to restructure this: IMPALA-4417.)
  #
  # e.g. --ignore="comparison" --ignore="util" --ignore=etc...
  ignored_dirs = build_ignore_dir_arg_list(valid_dirs=valid_dirs)

  if valid_dirs != ['verifiers']:
    # This isn't the metrics verification stage yet, so after determining the
    # logging params and which sub-directories within tests/ to ignore, just tack
    # on any other args from sys.argv -- excluding sys.argv[0], which of course
    # is the script name
    test_args = '%s %s %s' % (ignored_dirs, logging_args, ' '.join(sys.argv[1:]))
  else:
    # When filtering, we need to account for the fact that '--foo bar' and
    # '--foo=bar' might be supplied by the user, as well as random options. E.g.,
    # if the user specified the following on the command line:
    #
    #   'run-tests.py --arg1 value1 --random_opt --arg2=value2'
    #
    # we want an iterable that, if unpacked as a list, would look like:
    #
    #   [arg1, value1, random_opt, arg2, value2]
    #
    raw_args = itertools.chain(*[arg.split('=') for arg in sys.argv[1:]])
    kept_args = []

    for arg in raw_args:
      try:
        pytest.config.getvalue(arg.strip('-'))  # Raises ValueError if invalid arg
        kept_args += [arg, str(raw_args.next())]
      except ValueError:
        # For any arg that's not a required pytest config arg, we can filter it out
        continue
    test_args = '%s %s %s' % (ignored_dirs, logging_args, ' '.join(kept_args))

  return test_args


def build_ignore_dir_arg_list(valid_dirs):
  """ Builds a list of directories to ignore """
  subdirs = [subdir for subdir in os.listdir(TEST_DIR) if os.path.isdir(subdir)]
  # In bash, in single-quoted strings, single quotes cannot appear - not even escaped!
  # Instead, one must close the string with a single-quote, insert a literal single-quote
  # (escaped, so bash doesn't think you're starting a new string), then start your
  # single-quoted string again. That works out to the four-character sequence '\''.
  return ' '.join(["--ignore='%s'" % d.replace("'", "'\''")
                   for d in set(subdirs) - set(valid_dirs)])


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
  if not os.path.exists(TEST_RESULT_DIR):
    os.makedirs(TEST_RESULT_DIR)

  # First run query tests that need to be executed serially
  args = '-m "execute_serially" %s' % build_test_args('serial', VALID_TEST_DIRS)
  test_executor.run_tests(args)

  # Run the stress tests tests
  print_metrics('connections')
  args = '-m "stress" -n %d %s' %\
      (NUM_STRESS_CLIENTS, build_test_args('stress', VALID_TEST_DIRS))
  test_executor.run_tests(args)
  print_metrics('connections')

  # Run the remaining query tests in parallel
  args = '-m "not execute_serially and not stress"  -n %d %s' %\
      (NUM_CONCURRENT_TESTS, build_test_args('parallel', VALID_TEST_DIRS))
  test_executor.run_tests(args)

  # Finally, validate impalad/statestored metrics.
  args = build_test_args(log_base_name='verify-metrics', valid_dirs=['verifiers'])
  args += ' verifiers/test_verify_metrics.py'
  test_executor.run_tests(args)

  if test_executor.tests_failed:
    sys.exit(1)
