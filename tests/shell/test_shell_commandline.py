#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
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

import os
import logging
import pytest
import shlex
from subprocess import Popen, PIPE, call

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
DEFAULT_QUERY = 'select 1'
TEST_DB = "tmp_shell"
TEST_TBL = "tbl1"
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')


class TestImpalaShell(object):
  """A set of sanity tests for the Impala shell commandiline parameters.

  The tests need to maintain Python 2.4 compatibility as a sub-goal of having
  shell tests is to ensure that it's not broken in systems running Python 2.4.
  The tests need a running impalad instance in order to execute queries.

  TODO:
     * Test individual modules.
     * Test the shell in interactive mode.
     * Add a test for a kerberized impala.
  """

  @classmethod
  def setup_class(cls):
    cls.__create_shell_data()

  @classmethod
  def teardown_class(cls):
    run_impala_shell_cmd('-q "drop table if exists %s.%s"' % (TEST_DB, TEST_TBL))
    run_impala_shell_cmd('-q "drop database if exists %s"' % TEST_DB)

  @classmethod
  def __create_shell_data(cls):
    # Create a temporary table and populate it with test data.
    stmts = ['create database if not exists %s' % TEST_DB,
             'create table if not exists %s.%s (i integer, s string)' % (TEST_DB,\
                                                                         TEST_TBL),
             "insert into %s.%s values (1, 'a'),(1, 'b'),(3, 'b')" % (TEST_DB, TEST_TBL)
            ]
    args = '-q "%s"' % (';'.join(stmts))
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_no_args(self):
    args = '-q "%s"' % DEFAULT_QUERY
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_multiple_queries(self):
    queries = ';'.join([DEFAULT_QUERY] * 3)
    args = '-q "%s" -B' % queries
    run_impala_shell_cmd(args)

  @pytest.mark.execute_serially
  def test_default_db(self):
    args = '-d %s -q "describe %s" --quiet' % (TEST_DB, TEST_TBL)
    run_impala_shell_cmd(args)
    args = '-q "describe %s"' % TEST_TBL
    run_impala_shell_cmd(args, expect_success=False)

  @pytest.mark.execute_serially
  def test_refresh_on_connect(self):
    args = '-r -q "%s"' % DEFAULT_QUERY
    result = run_impala_shell_cmd(args)
    assert 'Invalidating Metadata' in result.stderr, result.stderr

  @pytest.mark.execute_serially
  def test_print_header(self):
    args = '--print_header -B --output_delim="," -q "select * from %s.%s"' % (TEST_DB,
                                                                              TEST_TBL)
    result = run_impala_shell_cmd(args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 4
    assert result_rows[0].split(',') == ['i', 's']
    args = '-B --output_delim="," -q "select * from %s.%s"' % (TEST_DB, TEST_TBL)
    result = run_impala_shell_cmd(args)
    result_rows = result.stdout.strip().split('\n')
    assert len(result_rows) == 3

  @pytest.mark.execute_serially
  @pytest.mark.xfail(run=False,
      reason='kerberos options not working as expected on all machines')
  def test_kerberos_option(self):
    args = "-k -q '%s'" % DEFAULT_QUERY
    # The command with fail because we're trying to connect to a kerberized impalad.
    results = run_impala_shell_cmd(args, expect_success=False)
    # Check that impala is using the right service name.
    assert "Using service name 'impala' for kerberos" in results.stderr
    # Change the service name
    args += " -s foobar"
    results = run_impala_shell_cmd(args, expect_success=False)
    assert "Using service name 'foobar' for kerberos" in results.stderr

  @pytest.mark.execute_serially
  def test_continue_on_error(self):
    args = '-c -q "select foo; select bar;"'
    run_impala_shell_cmd(args)
    # Should fail
    args = '-q "select foo; select bar;"'
    run_impala_shell_cmd(args, expect_success=False)

  @pytest.mark.execute_serially
  def test_execute_queries_from_file(self):
    args = '-f %s/test_file_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    output = result.stdout
    args = '-f %s/test_file_no_comments.sql --quiet -B' % QUERY_FILE_PATH
    result = run_impala_shell_cmd(args)
    assert output == result.stdout, "Queries with comments not parsed correctly"

  @pytest.mark.execute_serially
  def test_output_format(self):
    expected_output = ['1'] * 3
    args = '-q "select 1,1,1" -B --quiet'
    result = run_impala_shell_cmd(args)
    actual_output = [r.strip() for r in result.stdout.split('\t')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(args + ' --output_delim="|"')
    actual_output = [r.strip() for r in result.stdout.split('|')]
    assert actual_output == expected_output
    result = run_impala_shell_cmd(args + ' --output_delim="||"',
                                  expect_success=False)
    assert "Illegal delimiter" in result.stderr


class ImpalaShellResult(object):
  def __init__(self):
    self.rc = 0
    self.stdout = str()
    self.stderr = str()

def run_impala_shell_cmd(shell_args, expect_success=True):
  """Runs the Impala shell on the commandline.

  shell_args is a string which represents the commandline options.
  returns the process return code, stdout and stderr.
  """
  cmd = "%s %s" % (SHELL_CMD, shell_args)
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)
  result = ImpalaShellResult()
  result.stdout, result.stderr = p.communicate()
  result.rc = p.returncode
  if expect_success:
    assert result.rc == 0, "Command %s was expected to succeed: %s" % (cmd,
                                                                       result.stderr)
  else:
    assert result.rc != 0, "Command %s was expected to fail" % cmd
  return result
