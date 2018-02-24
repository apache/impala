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
import signal
import socket
import sys
import tempfile
from time import sleep

from tests.common.impala_service import ImpaladService
from tests.common.skip import SkipIfLocal
from util import assert_var_substitution, ImpalaShell
from util import move_shell_history, restore_shell_history

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
SHELL_HISTORY_FILE = os.path.expanduser("~/.impalahistory")
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

class TestImpalaShellInteractive(object):
  """Test the impala shell interactively"""

  @classmethod
  def setup_class(cls):
    cls.tempfile_name = tempfile.mktemp()
    move_shell_history(cls.tempfile_name)

  @classmethod
  def teardown_class(cls):
    restore_shell_history(cls.tempfile_name)

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
    prompt_regex = '\[{0}:2100[0-9]\]'.format(socket.getfqdn())
    # readline gets its input from tty, so using stdin does not work.
    child_proc = pexpect.spawn(SHELL_CMD)
    # List of (input query, expected text in output).
    # The expected output is usually the same as the input with a number prefix, except
    # where the shell strips newlines before a semicolon.
    queries = [
        ("select\n1;--comment", "[1]: select\n1;--comment"),
        ("select 1 --comment\n;", "[2]: select 1 --comment;"),
        ("select 1 --comment\n\n\n;", "[3]: select 1 --comment;"),
        ("select /*comment*/\n1;", "[4]: select /*comment*/\n1;"),
        ("select\n/*comm\nent*/\n1;", "[5]: select\n/*comm\nent*/\n1;")]
    for query, _ in queries:
      child_proc.expect(prompt_regex)
      child_proc.sendline(query)
      child_proc.expect("Fetched 1 row\(s\) in .*s")
    child_proc.expect(prompt_regex)
    child_proc.sendline('quit;')
    p = ImpalaShell()
    p.send_cmd('history')
    result = p.get_result()
    for _, history_entry in queries:
      assert history_entry in result.stderr, "'%s' not in '%s'" % (history_entry, result.stderr)

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
  def test_query_option_configuration(self):
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'impalarc_with_query_options')
    args = '-Q MT_dop=1 --query_option=MAX_ERRORS=200 --config_file="%s"' % rcfile_path
    cmds = "set all;"
    result = run_impala_shell_interactive(cmds, shell_args=args)
    assert "\tMT_DOP: 1" in result.stdout
    assert "\tMAX_ERRORS: 200" in result.stdout
    assert "\tEXPLAIN_LEVEL: 2" in result.stdout
    assert "INVALID_QUERY_OPTION is not supported for the impalad being "
    "connected to, ignoring." in result.stdout

  @pytest.mark.execute_serially
  def test_source_file(self):
    cwd = os.getcwd()
    try:
      # Change working dir so that SOURCE command in shell.cmds can find shell2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      # IMPALA-5416: Test that a command following 'source' won't be run twice.
      result = run_impala_shell_interactive("source shell.cmds;select \"second command\";")
      assert "Query: USE FUNCTIONAL" in result.stderr
      assert "Query: SHOW TABLES" in result.stderr
      assert "alltypes" in result.stdout
      # This is from shell2.cmds, the result of sourcing a file from a sourced file.
      assert "SELECT VERSION()" in result.stderr
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
    assert "Could not execute command: USE UNKNOWN_DATABASE" in result.stderr
    assert "Query: USE FUNCTIONAL" not in result.stderr

    result = run_impala_shell_interactive("source %s;" % full_path, '-c')
    assert "Could not execute command: USE UNKNOWN_DATABASE" in result.stderr
    assert "Query: USE FUNCTIONAL" in result.stderr
    assert "Query: SHOW TABLES" in result.stderr
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

  @pytest.mark.execute_serially
  def test_set_and_set_all(self):
    """IMPALA-2181. Tests the outputs of SET and SET ALL commands. SET should contain the
    REGULAR and ADVANCED options only. SET ALL should contain all the options grouped by
    display level."""
    shell1 = ImpalaShell()
    shell1.send_cmd("set")
    result = shell1.get_result()
    assert "Query options (defaults shown in []):" in result.stdout
    assert "ABORT_ON_ERROR" in result.stdout
    assert "Advanced Query Options:" in result.stdout
    assert "APPX_COUNT_DISTINCT" in result.stdout
    assert "SUPPORT_START_OVER" in result.stdout
    # Development, deprecated and removed options should not be shown.
    assert "Development Query Options:" not in result.stdout
    assert "DEBUG_ACTION" not in result.stdout
    assert "Deprecated Query Options:" not in result.stdout
    assert "ALLOW_UNSUPPORTED_FORMATS" not in result.stdout
    assert "MAX_IO_BUFFERS" not in result.stdout

    shell2 = ImpalaShell()
    shell2.send_cmd("set all")
    result = shell2.get_result()
    assert "Query options (defaults shown in []):" in result.stdout
    assert "Advanced Query Options:" in result.stdout
    assert "Development Query Options:" in result.stdout
    assert "Deprecated Query Options:" in result.stdout
    advanced_part_start_idx = result.stdout.find("Advanced Query Options")
    development_part_start_idx = result.stdout.find("Development Query Options")
    deprecated_part_start_idx = result.stdout.find("Deprecated Query Options")
    advanced_part = result.stdout[advanced_part_start_idx:development_part_start_idx]
    development_part = result.stdout[development_part_start_idx:deprecated_part_start_idx]
    assert "ABORT_ON_ERROR" in result.stdout[:advanced_part_start_idx]
    assert "APPX_COUNT_DISTINCT" in advanced_part
    assert "SUPPORT_START_OVER" in advanced_part
    assert "DEBUG_ACTION" in development_part
    assert "ALLOW_UNSUPPORTED_FORMATS" in result.stdout[deprecated_part_start_idx:]
    # Removed options should not be shown.
    assert "MAX_IO_BUFFERS" not in result.stdout

  def check_command_case_sensitivity(self, command, expected):
    shell = ImpalaShell()
    shell.send_cmd(command)
    assert expected in shell.get_result().stderr

  @pytest.mark.execute_serially
  def test_unexpected_conversion_for_literal_string_to_lowercase(self):
    # IMPALA-4664: Impala shell can accidentally convert certain literal
    # strings to lowercase. Impala shell splits each command into tokens
    # and then converts the first token to lowercase to figure out how it
    # should execute the command. The splitting is done by spaces only.
    # Thus, if the user types a TAB after the SELECT, the first token after
    # the split becomes the SELECT plus whatever comes after it.
    result = run_impala_shell_interactive("select'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)
    result = run_impala_shell_interactive("select\t'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)
    result = run_impala_shell_interactive("select\n'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)

  @pytest.mark.execute_serially
  def test_case_sensitive_command(self):
    # IMPALA-2640: Make a given command case-sensitive
    cwd = os.getcwd()
    try:
      self.check_command_case_sensitivity("sElEcT VERSION()", "Query: sElEcT")
      self.check_command_case_sensitivity("sEt VaR:FoO=bOo", "Variable FOO")
      self.check_command_case_sensitivity("sHoW tables", "Query: sHoW")
      # Change working dir so that SOURCE command in shell_case_sensitive.cmds can
      # find shell_case_sensitive2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      result = run_impala_shell_interactive(
        "sOuRcE shell_case_sensitive.cmds; SeLeCt 'second command'")
      print result.stderr
      assert "Query: uSe FUNCTIONAL" in result.stderr
      assert "Query: ShOw TABLES" in result.stderr
      assert "alltypes" in result.stdout
      # This is from shell_case_sensitive2.cmds, the result of sourcing a file
      # from a sourced file.
      print result.stderr
      assert "SeLeCt 'second command'" in result.stderr
    finally:
      os.chdir(cwd)

  @pytest.mark.execute_serially
  def test_line_ends_with_comment(self):
    # IMPALA-5269: Test lines that end with a comment.
    queries = ['select 1 + 1; --comment',
               'select 1 + 1 --comment\n;']
    for query in queries:
      result = run_impala_shell_interactive(query)
      assert '| 1 + 1 |' in result.stdout
      assert '| 2     |' in result.stdout

    queries = ['select \'some string\'; --comment',
               'select \'some string\' --comment\n;']
    for query in queries:
      result = run_impala_shell_interactive(query)
      assert '| \'some string\' |' in result.stdout
      assert '| some string   |' in result.stdout

    queries = ['select "--"; -- "--"',
               'select \'--\'; -- "--"',
               'select "--" -- "--"\n;',
               'select \'--\' -- "--"\n;']
    for query in queries:
      result = run_impala_shell_interactive(query)
      assert '| \'--\' |' in result.stdout
      assert '| --   |' in result.stdout

    query = ('select * from (\n' +
             'select count(*) from functional.alltypes\n' +
             ') v; -- Incomplete SQL statement in this line')
    result = run_impala_shell_interactive(query)
    assert '| count(*) |' in result.stdout

    query = ('select id from functional.alltypes\n' +
             'order by id; /*\n' +
             '* Multi-line comment\n' +
             '*/')
    result = run_impala_shell_interactive(query)
    assert '| id   |' in result.stdout

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
