#!/usr/bin/env impala-python
# encoding=utf-8
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

import os
import pexpect
import pytest
import re
import shutil
import signal
import socket
import sys
from time import sleep

from tests.common.impala_service import ImpaladService
from tests.common.skip import SkipIfLocal
from util import assert_var_substitution, ImpalaShell

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
SHELL_HISTORY_FILE = os.path.expanduser("~/.impalahistory")
TMP_HISTORY_FILE = os.path.expanduser("~/.impalahistorytmp")
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

class TestImpalaShellInteractive(object):
  """Test the impala shell interactively"""

  @classmethod
  def setup_class(cls):
    if os.path.exists(SHELL_HISTORY_FILE):
      shutil.move(SHELL_HISTORY_FILE, TMP_HISTORY_FILE)

  @classmethod
  def teardown_class(cls):
    if os.path.exists(TMP_HISTORY_FILE): shutil.move(TMP_HISTORY_FILE, SHELL_HISTORY_FILE)

  def _expect_with_cmd(self, proc, cmd, expectations=()):
    """Executes a command on the expect process instance and verifies a set of
    assertions defined by the expections."""
    proc.sendline(cmd + ";")
    proc.expect(":21000] >")
    if not expectations: return
    for e in expectations:
      assert e in proc.before

  @pytest.mark.execute_serially
  def test_local_shell_options(self):
    """Test that setting the local shell options works"""
    proc = pexpect.spawn(SHELL_CMD)
    proc.expect(":21000] >")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: False", "LIVE_SUMMARY: False"))
    self._expect_with_cmd(proc, "set live_progress=true")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: True", "LIVE_SUMMARY: False"))
    self._expect_with_cmd(proc, "set live_summary=1")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: True", "LIVE_SUMMARY: True"))

  @pytest.mark.execute_serially
  def test_compute_stats_with_live_progress_options(self):
    """Test that setting LIVE_PROGRESS options won't cause COMPUTE STATS query fail"""
    p = ImpalaShell()
    p.send_cmd("set live_progress=True")
    p.send_cmd("set live_summary=True")
    p.send_cmd('create table test_live_progress_option(col int);')
    try:
      p.send_cmd('compute stats test_live_progress_option;')
    finally:
      p.send_cmd('drop table if exists test_live_progress_option;')
    result = p.get_result()
    assert "Updated 1 partition(s) and 1 column(s)" in result.stdout

  @pytest.mark.execute_serially
  def test_escaped_quotes(self):
    """Test escaping quotes"""
    # test escaped quotes outside of quotes
    result = run_impala_shell_interactive("select \\'bc';")
    assert "Unexpected character" in result.stderr
    result = run_impala_shell_interactive("select \\\"bc\";")
    assert "Unexpected character" in result.stderr
    # test escaped quotes within quotes
    result = run_impala_shell_interactive("select 'ab\\'c';")
    assert "Fetched 1 row(s)" in result.stderr
    result = run_impala_shell_interactive("select \"ab\\\"c\";")
    assert "Fetched 1 row(s)" in result.stderr

  @pytest.mark.execute_serially
  def test_cancellation(self):
    impalad = ImpaladService(socket.getfqdn())
    impalad.wait_for_num_in_flight_queries(0)
    command = "select sleep(10000);"
    p = ImpalaShell()
    p.send_cmd(command)
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelled" not in result.stderr
    assert impalad.wait_for_num_in_flight_queries(0)

  @pytest.mark.execute_serially
  def test_unicode_input(self):
    "Test queries containing non-ascii input"
    # test a unicode query spanning multiple lines
    unicode_text = u'\ufffd'
    args = "select '%s'\n;" % unicode_text.encode('utf-8')
    result = run_impala_shell_interactive(args)
    assert "Fetched 1 row(s)" in result.stderr

  @pytest.mark.execute_serially
  def test_welcome_string(self):
    """Test that the shell's welcome message is only printed once
    when the shell is started. Ensure it is not reprinted on errors.
    Regression test for IMPALA-1153
    """
    result = run_impala_shell_interactive('asdf;')
    assert result.stdout.count("Welcome to the Impala shell") == 1
    result = run_impala_shell_interactive('select * from non_existent_table;')
    assert result.stdout.count("Welcome to the Impala shell") == 1

  @pytest.mark.execute_serially
  def test_bash_cmd_timing(self):
    """Test existence of time output in bash commands run from shell"""
    args = "! ls;"
    result = run_impala_shell_interactive(args)
    assert "Executed in" in result.stderr

  @SkipIfLocal.multiple_impalad
  @pytest.mark.execute_serially
  def test_reconnect(self):
    """Regression Test for IMPALA-1235

    Verifies that a connect command by the user is honoured.
    """

    def get_num_open_sessions(impala_service):
      """Helper method to retrieve the number of open sessions"""
      return impala_service.get_metric_value('impala-server.num-open-beeswax-sessions')

    hostname = socket.getfqdn()
    initial_impala_service = ImpaladService(hostname)
    target_impala_service = ImpaladService(hostname, webserver_port=25001,
        beeswax_port=21001, be_port=22001)
    # Get the initial state for the number of sessions.
    num_sessions_initial = get_num_open_sessions(initial_impala_service)
    num_sessions_target = get_num_open_sessions(target_impala_service)
    # Connect to localhost:21000 (default)
    p = ImpalaShell()
    sleep(2)
    # Make sure we're connected <hostname>:21000
    assert get_num_open_sessions(initial_impala_service) == num_sessions_initial + 1, \
        "Not connected to %s:21000" % hostname
    p.send_cmd("connect %s:21001" % hostname)
    # Wait for a little while
    sleep(2)
    # The number of sessions on the target impalad should have been incremented.
    assert get_num_open_sessions(target_impala_service) == num_sessions_target + 1, \
        "Not connected to %s:21001" % hostname
    # The number of sessions on the initial impalad should have been decremented.
    assert get_num_open_sessions(initial_impala_service) == num_sessions_initial, \
        "Connection to %s:21000 should have been closed" % hostname

  @pytest.mark.execute_serially
  def test_ddl_queries_are_closed(self):
    """Regression test for IMPALA-1317

    The shell does not call close() for alter, use and drop queries, leaving them in
    flight. This test issues those queries in interactive mode, and checks the debug
    webpage to confirm that they've been closed.
    TODO: Add every statement type.
    """

    TMP_DB = 'inflight_test_db'
    TMP_TBL = 'tmp_tbl'
    MSG = '%s query should be closed'
    NUM_QUERIES = 'impala-server.num-queries'

    impalad = ImpaladService(socket.getfqdn())
    p = ImpalaShell()
    try:
      start_num_queries = impalad.get_metric_value(NUM_QUERIES)
      p.send_cmd('create database if not exists %s' % TMP_DB)
      p.send_cmd('use %s' % TMP_DB)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 2)
      assert impalad.wait_for_num_in_flight_queries(0), MSG % 'use'
      p.send_cmd('create table %s(i int)' % TMP_TBL)
      p.send_cmd('alter table %s add columns (j int)' % TMP_TBL)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 4)
      assert impalad.wait_for_num_in_flight_queries(0), MSG % 'alter'
      p.send_cmd('drop table %s' % TMP_TBL)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 5)
      assert impalad.wait_for_num_in_flight_queries(0), MSG % 'drop'
    finally:
      run_impala_shell_interactive("drop table if exists %s.%s;" % (TMP_DB, TMP_TBL))
      run_impala_shell_interactive("drop database if exists foo;")

  @pytest.mark.execute_serially
  def test_multiline_queries_in_history(self):
    """Test to ensure that multiline queries with comments are preserved in history

    Ensure that multiline queries are preserved when they're read back from history.
    Additionally, also test that comments are preserved.
    """
    # regex for pexpect, a shell prompt is expected after each command..
    prompt_regex = '.*%s:2100.*' % socket.getfqdn()
    # readline gets its input from tty, so using stdin does not work.
    child_proc = pexpect.spawn(SHELL_CMD)
    queries = ["select\n1--comment;",
        "select /*comment*/\n1;",
        "select\n/*comm\nent*/\n1;"]
    for query in queries:
      child_proc.expect(prompt_regex)
      child_proc.sendline(query)
    child_proc.expect(prompt_regex)
    child_proc.sendline('quit;')
    p = ImpalaShell()
    p.send_cmd('history')
    result = p.get_result()
    for query in queries:
      assert query in result.stderr, "'%s' not in '%s'" % (query, result.stderr)

  @pytest.mark.execute_serially
  def test_rerun(self):
    """Smoke test for the 'rerun' command"""
    # Clear history first.
    if os.path.exists(SHELL_HISTORY_FILE):
      os.remove(SHELL_HISTORY_FILE)
    assert not os.path.exists(SHELL_HISTORY_FILE)
    child_proc = pexpect.spawn(SHELL_CMD)
    child_proc.expect(":21000] >")
    self._expect_with_cmd(child_proc, "@1", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "rerun -1", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "select 'first_command'", ("first_command"))
    self._expect_with_cmd(child_proc, "rerun 1", ("first_command"))
    self._expect_with_cmd(child_proc, "@ -1", ("first_command"))
    self._expect_with_cmd(child_proc, "select 'second_command'", ("second_command"))
    child_proc.sendline('history;')
    child_proc.expect(":21000] >")
    assert '[1]: select \'first_command\';' in child_proc.before;
    assert '[2]: select \'second_command\';' in child_proc.before;
    assert '[3]: history;' in child_proc.before;
    # Rerunning command should not add an entry into history.
    assert '[4]' not in child_proc.before;
    self._expect_with_cmd(child_proc, "@0", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "rerun   4", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "@-4", ("Command index out of range"))
    self._expect_with_cmd(child_proc, " @ 3 ", ("second_command"))
    self._expect_with_cmd(child_proc, "@-3", ("first_command"))
    self._expect_with_cmd(child_proc, "@",
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "@1foo",
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "@1 2",
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "rerun1", ("Syntax error"))
    child_proc.sendline('quit;')

  @pytest.mark.execute_serially
  def test_tip(self):
    """Smoke test for the TIP command"""
    # Temporarily add impala_shell module to path to get at TIPS list for verification
    sys.path.append("%s/shell/" % os.environ['IMPALA_HOME'])
    try:
      import impala_shell
    finally:
      sys.path = sys.path[:-1]
    result = run_impala_shell_interactive("tip;")
    for t in impala_shell.TIPS:
      if t in result.stderr: return
    assert False, "No tip found in output %s" % result.stderr

  @pytest.mark.execute_serially
  def test_var_substitution(self):
    cmds = open(os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql')).read()
    args = '''--var=foo=123 --var=BAR=456 --delimited "--output_delimiter= " '''
    result = run_impala_shell_interactive(cmds, shell_args=args)
    assert_var_substitution(result)

  @pytest.mark.execute_serially
  def test_source_file(self):
    cwd = os.getcwd()
    try:
      # Change working dir so that SOURCE command in shell.cmds can find shell2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      # IMPALA-5416: Test that a command following 'source' won't be run twice.
      result = run_impala_shell_interactive("source shell.cmds;select \"second command\";")
      assert "Query: use FUNCTIONAL" in result.stderr
      assert "Query: show TABLES" in result.stderr
      assert "alltypes" in result.stdout
      # This is from shell2.cmds, the result of sourcing a file from a sourced file.
      assert "select VERSION()" in result.stderr
      assert "version()" in result.stdout
      assert len(re.findall("'second command'", result.stdout)) == 1
      # IMPALA-5416: Test that two source commands on a line won't crash the shell.
      result = run_impala_shell_interactive("source shell.cmds;source shell.cmds;")
      assert len(re.findall("version\(\)", result.stdout)) == 2
    finally:
      os.chdir(cwd)

  @pytest.mark.execute_serially
  def test_source_file_with_errors(self):
    full_path = "%s/tests/shell/shell_error.cmds" % os.environ['IMPALA_HOME']
    result = run_impala_shell_interactive("source %s;" % full_path)
    assert "Could not execute command: use UNKNOWN_DATABASE" in result.stderr
    assert "Query: use FUNCTIONAL" not in result.stderr

    result = run_impala_shell_interactive("source %s;" % full_path, '-c')
    assert "Could not execute command: use UNKNOWN_DATABASE" in result.stderr
    assert "Query: use FUNCTIONAL" in result.stderr
    assert "Query: show TABLES" in result.stderr
    assert "alltypes" in result.stdout

  @pytest.mark.execute_serially
  def test_source_missing_file(self):
    full_path = "%s/tests/shell/doesntexist.cmds" % os.environ['IMPALA_HOME']
    result = run_impala_shell_interactive("source %s;" % full_path)
    assert "No such file or directory" in result.stderr

  @pytest.mark.execute_serially
  def test_zero_row_fetch(self):
    # IMPALA-4418: DROP and USE are generally exceptional statements where
    # the client does not fetch. However, when preceded by a comment, the
    # Impala shell treats them like any other statement and will try to
    # fetch - receiving 0 rows. For statements returning 0 rows we do not
    # want an empty line in stdout.
    result = run_impala_shell_interactive("-- foo \n use default;")
    assert "Fetched 0 row(s)" in result.stderr
    assert re.search('> \[', result.stdout)
    result = run_impala_shell_interactive("select * from functional.alltypes limit 0;")
    assert "Fetched 0 row(s)" in result.stderr
    assert re.search('> \[', result.stdout)

def run_impala_shell_interactive(input_lines, shell_args=None):
  """Runs a command in the Impala shell interactively."""
  # if argument "input_lines" is a string, makes it into a list
  if type(input_lines) is str:
    input_lines = [input_lines]
  # workaround to make Popen environment 'utf-8' compatible
  # since piping defaults to ascii
  my_env = os.environ
  my_env['PYTHONIOENCODING'] = 'utf-8'
  p = ImpalaShell(shell_args, env=my_env)
  for line in input_lines:
    p.send_cmd(line)
  return p.get_result()
