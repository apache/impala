#!/usr/bin/env impala-python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
#
# Runs the Impala query tests, first executing the tests that cannot be run in parallel
# and then executing the remaining tests in parallel. All additional command line options
# are passed to py.test.
import multiprocessing
import os
import pytest
import sys

# We whitelist valid test directories. If a new test directory is added, update this.
VALID_TEST_DIRS = ['failure', 'query_test', 'stress', 'unittests', 'aux_query_tests',
                   'shell', 'hs2', 'catalog_service', 'metadata', 'data_errors',
                   'statestore']

TEST_DIR = os.path.join(os.environ['IMPALA_HOME'], 'tests')
TEST_RESULT_DIR = os.path.join(TEST_DIR, 'results')

# Arguments that control output logging. If additional default arguments are needed they
# should go in the pytest.ini file.
LOGGING_ARGS = '--junitxml=%(result_dir)s/TEST-impala-%(log_name)s.xml '\
               '--resultlog=%(result_dir)s/TEST-impala-%(log_name)s.log'

# Default the number of concurrent tests defaults to the cpu cores in the system.
# This can be overridden by setting the NUM_CONCURRENT_TESTS environment variable.
NUM_CONCURRENT_TESTS = multiprocessing.cpu_count()
if 'NUM_CONCURRENT_TESTS' in os.environ:
  NUM_CONCURRENT_TESTS = int(os.environ['NUM_CONCURRENT_TESTS'])

# Default the number of stress clinets to 4x the number of CPUs
# This can be overridden by setting the NUM_STRESS_CLIENTS environment variable.
NUM_STRESS_CLIENTS = multiprocessing.cpu_count() * 4
if 'NUM_STRESS_CLIENTS' in os.environ:
  NUM_STRESS_CLIENTS = int(os.environ['NUM_STRESS_CLIENTS'])

class TestExecutor:
  def __init__(self, exit_on_error=True):
    self._exit_on_error = exit_on_error
    self.tests_failed = False

  def run_tests(self, args):
    exit_code = pytest.main(args)
    if exit_code != 0 and self._exit_on_error:
      sys.exit(exit_code)
    self.tests_failed = exit_code != 0 or self.tests_failed

def build_test_args(log_base_name, valid_dirs, include_cmdline_args=True):
  logging_args = LOGGING_ARGS % {'result_dir': TEST_RESULT_DIR, 'log_name': log_base_name}
  args = '%s %s' % (build_ignore_dir_arg_list(valid_dirs=valid_dirs), logging_args)

  if include_cmdline_args:
    # sys.argv[0] is always the script name, so exclude it
    return '%s %s' % (args, ' '.join(sys.argv[1:]))
  return args

def build_ignore_dir_arg_list(valid_dirs):
  """ Builds a list of directories to ignore """
  subdirs = [subdir for subdir in os.listdir(TEST_DIR) if os.path.isdir(subdir)]
  return ' '.join(['--ignore="%s"' % d for d in set(subdirs) - set(valid_dirs)])

if __name__ == "__main__":
  os.chdir(TEST_DIR)

  # Create the test result directory if it doesn't already exist.
  if not os.path.exists(TEST_RESULT_DIR):
    os.makedirs(TEST_RESULT_DIR)

  exit_on_error = '-x' in sys.argv or '--exitfirst' in sys.argv
  test_executor = TestExecutor(exit_on_error=exit_on_error)

  # First run query tests that need to be executed serially
  args = '-m "execute_serially" %s' % build_test_args('serial', VALID_TEST_DIRS)
  test_executor.run_tests(args)

  # Run the stress tests tests
  args = '-m "stress" -n %d %s' %\
      (NUM_STRESS_CLIENTS, build_test_args('stress', VALID_TEST_DIRS))
  test_executor.run_tests(args)

  # Run the remaining query tests in parallel
  args = '-m "not execute_serially and not stress"  -n %d %s' %\
      (NUM_CONCURRENT_TESTS, build_test_args('parallel', VALID_TEST_DIRS))
  test_executor.run_tests(args)

  # Finally, validate impalad/statestored metrics.
  # Do not include any command-line arguments when invoking verifiers because it
  # can lead to tests being run multiple times should someone pass in a .py file.
  args = build_test_args(log_base_name='verify-metrics', valid_dirs=['verifiers'],
      include_cmdline_args=False)
  args += ' verifiers/test_verify_metrics.py'
  test_executor.run_tests(args)

  if test_executor.tests_failed:
    sys.exit(1)
