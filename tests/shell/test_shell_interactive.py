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
from time import sleep

# This import is the actual ImpalaShell class from impala_shell.py.
# We rename it to ImpalaShellClass here because we later import another
# class called ImpalaShell from tests/shell/util.py, and we don't want
# to mask it.
from shell.impala_shell import ImpalaShell as ImpalaShellClass

from tempfile import NamedTemporaryFile
from tests.common.impala_service import ImpaladService
from tests.common.skip import SkipIfLocal
from util import assert_var_substitution, ImpalaShell

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

# Regex to match the interactive shell prompt that is expected after each command.
PROMPT_REGEX = r'\[[^:]+:2100[0-9]\]'


@pytest.fixture
def tmp_history_file(request):
  """
  Test fixture which uses a temporary file as the path for the shell
  history.
  """
  tmp = NamedTemporaryFile()
  old_path = os.environ.get('IMPALA_HISTFILE')
  os.environ['IMPALA_HISTFILE'] = tmp.name

  def cleanup():
    if old_path is not None:
      os.environ['IMPALA_HISTFILE'] = old_path
    else:
      del os.environ['IMPALA_HISTFILE']

  request.addfinalizer(cleanup)
  return tmp.name


class TestImpalaShellInteractive(object):
  """Test the impala shell interactively"""

  def _expect_with_cmd(self, proc, cmd, expectations=(), db="default"):
    """Executes a command on the expect process instance and verifies a set of
    assertions defined by the expectations."""
    proc.sendline(cmd + ";")
    proc.expect(":21000] {db}>".format(db=db))
    if not expectations: return
    for e in expectations:
      assert e in proc.before

  def test_local_shell_options(self):
    """Test that setting the local shell options works"""
    proc = pexpect.spawn(SHELL_CMD)
    proc.expect(":21000] default>")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: False", "LIVE_SUMMARY: False"))
    self._expect_with_cmd(proc, "set live_progress=true")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: True", "LIVE_SUMMARY: False"))
    self._expect_with_cmd(proc, "set live_summary=1")
    self._expect_with_cmd(proc, "set", ("LIVE_PROGRESS: True", "LIVE_SUMMARY: True"))
    self._expect_with_cmd(proc, "set", ("WRITE_DELIMITED: False", "VERBOSE: True"))
    self._expect_with_cmd(proc, "set", ("DELIMITER: \\t", "OUTPUT_FILE: None"))
    self._expect_with_cmd(proc, "set write_delimited=true")
    self._expect_with_cmd(proc, "set", ("WRITE_DELIMITED: True", "VERBOSE: True"))
    self._expect_with_cmd(proc, "set DELIMITER=,")
    self._expect_with_cmd(proc, "set", ("DELIMITER: ,", "OUTPUT_FILE: None"))
    self._expect_with_cmd(proc, "set output_file=/tmp/clmn.txt")
    self._expect_with_cmd(proc, "set", ("DELIMITER: ,", "OUTPUT_FILE: /tmp/clmn.txt"))

  @pytest.mark.execute_serially
  def test_write_delimited(self):
    """Test output rows in delimited mode"""
    p = ImpalaShell()
    p.send_cmd("use tpch")
    p.send_cmd("set write_delimited=true")
    p.send_cmd("select * from nation")
    result = p.get_result()
    assert "+----------------+" not in result.stdout
    assert "21\tVIETNAM\t2" in result.stdout

  @pytest.mark.execute_serially
  def test_change_delimiter(self):
    """Test change output delimiter if delimited mode is enabled"""
    p = ImpalaShell()
    p.send_cmd("use tpch")
    p.send_cmd("set write_delimited=true")
    p.send_cmd("set delimiter=,")
    p.send_cmd("select * from nation")
    result = p.get_result()
    assert "21,VIETNAM,2" in result.stdout

  @pytest.mark.execute_serially
  def test_print_to_file(self):
    """Test print to output file and unset"""
    # test print to file
    p1 = ImpalaShell()
    p1.send_cmd("use tpch")
    local_file = NamedTemporaryFile(delete=True)
    p1.send_cmd("set output_file=%s" % local_file.name)
    p1.send_cmd("select * from nation")
    result = p1.get_result()
    assert "VIETNAM" not in result.stdout
    with open(local_file.name, "r") as fi:
      # check if the results were written to the file successfully
      result = fi.read()
      assert "VIETNAM" in result
    # test unset to print back to stdout
    p2 = ImpalaShell()
    p2.send_cmd("use tpch")
    p2.send_cmd("set output_file=%s" % local_file.name)
    p2.send_cmd("unset output_file")
    p2.send_cmd("select * from nation")
    result = p2.get_result()
    assert "VIETNAM" in result.stdout

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
    assert impalad.wait_for_num_in_flight_queries(0)
    command = "select sleep(10000);"
    p = ImpalaShell()
    p.send_cmd(command)
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelled" not in result.stderr
    assert impalad.wait_for_num_in_flight_queries(0)

    p = ImpalaShell()
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "^C" in result.stderr

  def test_unicode_input(self):
    "Test queries containing non-ascii input"
    # test a unicode query spanning multiple lines
    unicode_text = u'\ufffd'
    args = "select '%s'\n;" % unicode_text.encode('utf-8')
    result = run_impala_shell_interactive(args)
    assert "Fetched 1 row(s)" in result.stderr

  def test_welcome_string(self):
    """Test that the shell's welcome message is only printed once
    when the shell is started. Ensure it is not reprinted on errors.
    Regression test for IMPALA-1153
    """
    result = run_impala_shell_interactive('asdf;')
    assert result.stdout.count("Welcome to the Impala shell") == 1
    result = run_impala_shell_interactive('select * from non_existent_table;')
    assert result.stdout.count("Welcome to the Impala shell") == 1

  def test_disconnected_shell(self):
    """Test that the shell presents a disconnected prompt if it can't connect
    """
    result = run_impala_shell_interactive('asdf;', shell_args='-i foo',
                                          wait_until_connected=False)
    assert ImpalaShellClass.DISCONNECTED_PROMPT in result.stdout

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

    def wait_for_num_open_sessions(impala_service, num, err):
      """Helper method to wait for the number of open sessions to reach 'num'."""
      assert impala_service.wait_for_metric_value(
          'impala-server.num-open-beeswax-sessions', num) == num, err

    hostname = socket.getfqdn()
    initial_impala_service = ImpaladService(hostname)
    target_impala_service = ImpaladService(hostname, webserver_port=25001,
        beeswax_port=21001, be_port=22001)
    # This test is running serially, so there shouldn't be any open sessions, but wait
    # here in case a session from a previous test hasn't been fully closed yet.
    wait_for_num_open_sessions(
        initial_impala_service, 0, "21000 should not have any remaining open sessions.")
    wait_for_num_open_sessions(
        target_impala_service, 0, "21001 should not have any remaining open sessions.")
    # Connect to localhost:21000 (default)
    p = ImpalaShell()

    # Make sure we're connected <hostname>:21000
    wait_for_num_open_sessions(
        initial_impala_service, 1, "Not connected to %s:21000" % hostname)
    p.send_cmd("connect %s:21001" % hostname)

    # The number of sessions on the target impalad should have been incremented.
    wait_for_num_open_sessions(
        target_impala_service, 1, "Not connected to %s:21001" % hostname)
    assert "[%s:21001] default>" % hostname in p.get_result().stdout

    # The number of sessions on the initial impalad should have been decremented.
    wait_for_num_open_sessions(initial_impala_service, 0,
        "Connection to %s:21000 should have been closed" % hostname)

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

  def test_multiline_queries_in_history(self, tmp_history_file):
    """Test to ensure that multiline queries with comments are preserved in history

    Ensure that multiline queries are preserved when they're read back from history.
    Additionally, also test that comments are preserved.
    """
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
      child_proc.expect(PROMPT_REGEX)
      child_proc.sendline(query)
      child_proc.expect("Fetched 1 row\(s\) in [0-9]+\.?[0-9]*s")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline('quit;')
    p = ImpalaShell()
    p.send_cmd('history')
    result = p.get_result()
    for _, history_entry in queries:
      assert history_entry in result.stderr, "'%s' not in '%s'" % (history_entry,
                                                                   result.stderr)

  def test_history_file_option(self, tmp_history_file):
    """
    Setting the 'tmp_history_file' fixture above means that the IMPALA_HISTFILE
    environment will be overridden. Here we override that environment by passing
    the --history_file command line option, ensuring that the history ends up
    in the appropriate spot.
    """
    with NamedTemporaryFile() as new_hist:
      child_proc = pexpect.spawn(
          SHELL_CMD,
          args=["--history_file=%s" % new_hist.name])
      child_proc.expect(":21000] default>")
      self._expect_with_cmd(child_proc, "select 'hi'", ('hi'))
      child_proc.sendline('exit;')
      child_proc.expect(pexpect.EOF)
      history_contents = file(new_hist.name).read()
      assert "select 'hi'" in history_contents

  def test_rerun(self, tmp_history_file):
    """Smoke test for the 'rerun' command"""
    child_proc = pexpect.spawn(SHELL_CMD)
    child_proc.expect(":21000] default>")
    self._expect_with_cmd(child_proc, "@1", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "rerun -1", ("Command index out of range"))
    self._expect_with_cmd(child_proc, "select 'first_command'", ("first_command"))
    self._expect_with_cmd(child_proc, "rerun 1", ("first_command"))
    self._expect_with_cmd(child_proc, "@ -1", ("first_command"))
    self._expect_with_cmd(child_proc, "select 'second_command'", ("second_command"))
    child_proc.sendline('history;')
    child_proc.expect(":21000] default>")
    assert '[1]: select \'first_command\';' in child_proc.before
    assert '[2]: select \'second_command\';' in child_proc.before
    assert '[3]: history;' in child_proc.before
    # Rerunning command should not add an entry into history.
    assert '[4]' not in child_proc.before
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

  def test_var_substitution(self):
    cmds = open(os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql')).read()
    args = '''--var=foo=123 --var=BAR=456 --delimited "--output_delimiter= " '''
    result = run_impala_shell_interactive(cmds, shell_args=args)
    assert_var_substitution(result)

  def test_query_option_configuration(self):
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'impalarc_with_query_options')
    args = '-Q MT_dop=1 --query_option=MAX_ERRORS=200 --config_file="%s"' % rcfile_path
    cmds = "set all;"
    result = run_impala_shell_interactive(cmds, shell_args=args)
    assert "\tMT_DOP: 1" in result.stdout
    assert "\tMAX_ERRORS: 200" in result.stdout
    assert "\tEXPLAIN_LEVEL: 2" in result.stdout
    assert "INVALID_QUERY_OPTION is not supported for the impalad being connected to, "\
           "ignoring." in result.stdout

  def test_source_file(self):
    cwd = os.getcwd()
    try:
      # Change working dir so that SOURCE command in shell.cmds can find shell2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      # IMPALA-5416: Test that a command following 'source' won't be run twice.
      result = run_impala_shell_interactive("source shell.cmds;select \"second "
                                            "command\";")
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

  def test_source_missing_file(self):
    full_path = "%s/tests/shell/doesntexist.cmds" % os.environ['IMPALA_HOME']
    result = run_impala_shell_interactive("source %s;" % full_path)
    assert "No such file or directory" in result.stderr

  def test_zero_row_fetch(self):
    # IMPALA-4418: DROP and USE are generally exceptional statements where
    # the client does not fetch. For statements returning 0 rows we do not
    # want an empty line in stdout.
    result = run_impala_shell_interactive("-- foo \n use default;")
    assert re.search('> \[', result.stdout)
    result = run_impala_shell_interactive("select * from functional.alltypes limit 0;")
    assert "Fetched 0 row(s)" in result.stderr
    assert re.search('> \[', result.stdout)

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
    # Note: there are currently no deprecated options
    assert "Development Query Options:" not in result.stdout
    assert "DEBUG_ACTION" not in result.stdout  # Development option.
    assert "MAX_IO_BUFFERS" not in result.stdout  # Removed option.

    shell2 = ImpalaShell()
    shell2.send_cmd("set all")
    result = shell2.get_result()
    assert "Query options (defaults shown in []):" in result.stdout
    assert "Advanced Query Options:" in result.stdout
    assert "Development Query Options:" in result.stdout
    assert "Deprecated Query Options:" not in result.stdout
    advanced_part_start_idx = result.stdout.find("Advanced Query Options")
    development_part_start_idx = result.stdout.find("Development Query Options")
    deprecated_part_start_idx = result.stdout.find("Deprecated Query Options")
    advanced_part = result.stdout[advanced_part_start_idx:development_part_start_idx]
    development_part = result.stdout[development_part_start_idx:deprecated_part_start_idx]
    assert "ABORT_ON_ERROR" in result.stdout[:advanced_part_start_idx]
    assert "APPX_COUNT_DISTINCT" in advanced_part
    assert "SUPPORT_START_OVER" in advanced_part
    assert "DEBUG_ACTION" in development_part
    # Removed options should not be shown.
    assert "MAX_IO_BUFFERS" not in result.stdout

  def check_command_case_sensitivity(self, command, expected):
    shell = ImpalaShell()
    shell.send_cmd(command)
    assert expected in shell.get_result().stderr

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

  def test_line_with_leading_comment(self):
    # IMPALA-2195: A line with a comment produces incorrect command.
    try:
      run_impala_shell_interactive('drop table if exists leading_comment;')
      run_impala_shell_interactive('create table leading_comment (i int);')
      result = run_impala_shell_interactive('-- comment\n'
                                            'insert into leading_comment values(1);')
      assert 'Modified 1 row(s)' in result.stderr
      result = run_impala_shell_interactive('-- comment\n'
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr
      result = run_impala_shell_interactive('--한글\n'
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr
      result = run_impala_shell_interactive('/* 한글 */\n'
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr
      result = run_impala_shell_interactive('/* comment */\n'
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr

      result = run_impala_shell_interactive('/* comment1 */\n'
                                            '-- comment2\n'
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr

      result = run_impala_shell_interactive('/* comment1\n'
                                            'comment2 */ select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr

      result = run_impala_shell_interactive('/* select * from leading_comment */ '
                                            'select * from leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr

      result = run_impala_shell_interactive('/* comment */ help use')
      assert 'Executes a USE... query' in result.stdout

      result = run_impala_shell_interactive('-- comment\n'
                                            ' help use;')
      assert 'Executes a USE... query' in result.stdout

      result = run_impala_shell_interactive('/* comment1 */\n'
                                            '-- comment2\n'
                                            'desc leading_comment;')
      assert 'Fetched 1 row(s)' in result.stderr

      result = run_impala_shell_interactive('/* comment1 */\n'
                                            '-- comment2\n'
                                            'help use;')
      assert 'Executes a USE... query' in result.stdout
    finally:
      run_impala_shell_interactive('drop table if exists leading_comment;')

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

  def test_fix_infinite_loop(self):
    # IMPALA-6337: Fix infinite loop.
    result = run_impala_shell_interactive("select 1 + 1; \"\n;\";")
    assert '| 2     |' in result.stdout
    result = run_impala_shell_interactive("select '1234'\";\n;\n\";")
    assert '| 1234 |' in result.stdout
    result = run_impala_shell_interactive("select 1 + 1; \"\n;\"\n;")
    assert '| 2     |' in result.stdout
    result = run_impala_shell_interactive("select '1\\'23\\'4'\";\n;\n\";")
    assert '| 1\'23\'4 |' in result.stdout
    result = run_impala_shell_interactive("select '1\"23\"4'\";\n;\n\";")
    assert '| 1"23"4 |' in result.stdout

  def test_comment_with_quotes(self):
    # IMPALA-2751: Comment does not need to have matching quotes
    queries = [
      "select -- '\n1;",
      'select -- "\n1;',
      "select -- \"'\n 1;",
      "select /*'\n*/ 1;",
      'select /*"\n*/ 1;',
      "select /*\"'\n*/ 1;",
      "with a as (\nselect 1\n-- '\n) select * from a",
      'with a as (\nselect 1\n-- "\n) select * from a',
      "with a as (\nselect 1\n-- '\"\n) select * from a",
    ]
    for query in queries:
      result = run_impala_shell_interactive(query)
      assert '| 1 |' in result.stdout

  def test_shell_prompt(self):
    proc = pexpect.spawn(SHELL_CMD)
    proc.expect(":21000] default>")
    self._expect_with_cmd(proc, "use foo", (), 'default')
    self._expect_with_cmd(proc, "use functional", (), 'functional')
    self._expect_with_cmd(proc, "use foo", (), 'functional')
    self._expect_with_cmd(proc, 'use `tpch`', (), 'tpch')
    self._expect_with_cmd(proc, 'use ` tpch `', (), 'tpch')

    proc = pexpect.spawn(SHELL_CMD, ['-d', 'functional'])
    proc.expect(":21000] functional>")
    self._expect_with_cmd(proc, "use foo", (), 'functional')
    self._expect_with_cmd(proc, "use tpch", (), 'tpch')
    self._expect_with_cmd(proc, "use foo", (), 'tpch')

    proc = pexpect.spawn(SHELL_CMD, ['-d', ' functional '])
    proc.expect(":21000] functional>")

    proc = pexpect.spawn(SHELL_CMD, ['-d', '` functional `'])
    proc.expect(":21000] functional>")

    # Start an Impala shell with an invalid DB.
    proc = pexpect.spawn(SHELL_CMD, ['-d', 'foo'])
    proc.expect(":21000] default>")
    self._expect_with_cmd(proc, "use foo", (), 'default')
    self._expect_with_cmd(proc, "use functional", (), 'functional')
    self._expect_with_cmd(proc, "use foo", (), 'functional')

  def test_strip_leading_comment(self):
    """Test stripping leading comments from SQL statements"""
    assert ('--delete\n', 'select 1') == \
        ImpalaShellClass.strip_leading_comment('--delete\nselect 1')
    assert ('--delete\n', 'select --do not delete\n1') == \
        ImpalaShellClass.strip_leading_comment('--delete\nselect --do not delete\n1')
    assert (None, 'select --do not delete\n1') == \
        ImpalaShellClass.strip_leading_comment('select --do not delete\n1')

    assert ('/*delete*/\n', 'select 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete*/\nselect 1')
    assert ('/*delete\nme*/\n', 'select 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete\nme*/\nselect 1')
    assert ('/*delete\nme*/\n', 'select 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete\nme*/\nselect 1')
    assert ('/*delete*/', 'select 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete*/select 1')
    assert ('/*delete*/ ', 'select /*do not delete*/ 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete*/ select /*do not delete*/ 1')
    assert ('/*delete1*/ \n/*delete2*/ \n--delete3 \n', 'select /*do not delete*/ 1') == \
        ImpalaShellClass.strip_leading_comment('/*delete1*/ \n'
                                               '/*delete2*/ \n'
                                               '--delete3 \n'
                                               'select /*do not delete*/ 1')
    assert (None, 'select /*do not delete*/ 1') == \
        ImpalaShellClass.strip_leading_comment('select /*do not delete*/ 1')
    assert ('/*delete*/\n', 'select c1 from\n'
                            'a\n'
                            'join -- +SHUFFLE\n'
                            'b') == \
        ImpalaShellClass.strip_leading_comment('/*delete*/\n'
                                               'select c1 from\n'
                                               'a\n'
                                               'join -- +SHUFFLE\n'
                                               'b')
    assert ('/*delete*/\n', 'select c1 from\n'
                            'a\n'
                            'join /* +SHUFFLE */\n'
                            'b') == \
        ImpalaShellClass.strip_leading_comment('/*delete*/\n'
                                               'select c1 from\n'
                                               'a\n'
                                               'join /* +SHUFFLE */\n'
                                               'b')

    assert (None, 'select 1') == \
        ImpalaShellClass.strip_leading_comment('select 1')

  def test_malformed_query(self):
    """Test the handling of malformed query without closing quotation"""
    shell = ImpalaShell()
    query = "with v as (select 1) \nselect foo('\\\\'), ('bar \n;"
    shell.send_cmd(query)
    result = shell.get_result()
    assert "ERROR: ParseException: Unmatched string literal" in result.stderr

  def test_timezone_validation(self):
    """Test that query option TIMEZONE is validated when executing a query.

       Query options are not sent to the coordinator immediately, so the error checking
       will only happen when running a query.
    """
    p = ImpalaShell()
    p.send_cmd('set timezone=BLA;')
    p.send_cmd('select 1;')
    results = p.get_result()
    assert "Fetched 1 row" not in results.stderr
    assert "ERROR: Errors parsing query options" in results.stderr
    assert "Invalid timezone name 'BLA'" in results.stderr

  def test_with_clause(self):
    # IMPALA-7939: Fix issue where CTE that contains "insert", "upsert", "update", or
    # "delete" is categorized as a DML statement.
    for keyword in ["insert", "upsert", "update", "delete", "\\'insert\\'",
                    "\\'upsert\\'", "\\'update\\'", "\\'delete\\'"]:
      p = ImpalaShell()
      p.send_cmd("with foo as "
                 "(select * from functional.alltypestiny where string_col='%s') "
                 "select * from foo limit 1" % keyword)
      result = p.get_result()
      assert "Fetched 0 row" in result.stderr


def run_impala_shell_interactive(input_lines, shell_args=None, wait_until_connected=True):
  """Runs a command in the Impala shell interactively."""
  # if argument "input_lines" is a string, makes it into a list
  if type(input_lines) is str:
    input_lines = [input_lines]
  # workaround to make Popen environment 'utf-8' compatible
  # since piping defaults to ascii
  my_env = os.environ
  my_env['PYTHONIOENCODING'] = 'utf-8'
  p = ImpalaShell(args=shell_args, env=my_env, wait_until_connected=wait_until_connected)
  for line in input_lines:
    p.send_cmd(line)
  return p.get_result()
