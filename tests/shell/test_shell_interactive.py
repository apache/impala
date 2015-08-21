#!/usr/bin/env impala-python
# encoding=utf-8
# Copyright 2014 Cloudera Inc.
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
import pexpect
import pytest
import re
import shutil
import socket
import signal
import sys

from impala_shell_results import get_shell_cmd_result
from subprocess import Popen, PIPE
from tests.common.impala_service import ImpaladService
from tests.common.skip import SkipIfLocal
from time import sleep
from test_shell_common import assert_var_substitution

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']
SHELL_HISTORY_FILE = os.path.expanduser("~/.impalahistory")
TMP_HISTORY_FILE = os.path.expanduser("~/.impalahistorytmp")
QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

class TestImpalaShellInteractive(object):
  """Test the impala shell interactively"""

  def _send_cmd_to_shell(self, p, cmd):
    """Given an open shell process, write a cmd to stdin

    This method takes care of adding the delimiter and EOL, callers should send the raw
    command.
    """
    p.stdin.write("%s;\n" % cmd)
    p.stdin.flush()

  def _start_new_shell_process(self):
    """Starts a shell process and returns the process handle"""
    return Popen([SHELL_CMD], stdout=PIPE, stdin=PIPE, stderr=PIPE)

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
    impalad = ImpaladService(socket.getfqdn())
    p = self._start_new_shell_process()
    self._send_cmd_to_shell(p, "set live_progress=True")
    self._send_cmd_to_shell(p, "set live_summary=True")
    self._send_cmd_to_shell(p, 'create table test_live_progress_option(col int);')
    try:
      self._send_cmd_to_shell(p, 'compute stats test_live_progress_option;')
    finally:
      self._send_cmd_to_shell(p, 'drop table if exists test_live_progress_option;')
    result = get_shell_cmd_result(p)
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
    p = self._start_new_shell_process()
    self._send_cmd_to_shell(p, command)
    sleep(3)
    os.kill(p.pid, signal.SIGINT)
    result = get_shell_cmd_result(p)
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
    p = self._start_new_shell_process()
    sleep(2)
    # Make sure we're connected <hostname>:21000
    assert get_num_open_sessions(initial_impala_service) == num_sessions_initial + 1, \
        "Not connected to %s:21000" % hostname
    self._send_cmd_to_shell(p, "connect %s:21001" % hostname)
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
    p = self._start_new_shell_process()
    try:
      start_num_queries = impalad.get_metric_value(NUM_QUERIES)
      self._send_cmd_to_shell(p, 'create database if not exists %s' % TMP_DB)
      self._send_cmd_to_shell(p, 'use %s' % TMP_DB)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 2)
      assert impalad.wait_for_num_in_flight_queries(0), MSG % 'use'
      self._send_cmd_to_shell(p, 'create table %s(i int)' % TMP_TBL)
      self._send_cmd_to_shell(p, 'alter table %s add columns (j int)' % TMP_TBL)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 4)
      assert impalad.wait_for_num_in_flight_queries(0), MSG % 'alter'
      self._send_cmd_to_shell(p, 'drop table %s' % TMP_TBL)
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
    p = self._start_new_shell_process()
    self._send_cmd_to_shell(p, 'history')
    result = get_shell_cmd_result(p)
    for query in queries:
      assert query in result.stderr, "'%s' not in '%s'" % (query, result.stderr)

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
    cmds = open(os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql')).read().\
      split('\n')
    args = '--var=foo=123 --var=BAR=456 --delimited --output_delimiter=" "'
    result = run_impala_shell_interactive(cmds, shell_args=args)
    assert_var_substitution(result)


def run_impala_shell_interactive(input_lines, shell_args=''):
  """Runs a command in the Impala shell interactively."""
  cmd = "%s %s" % (SHELL_CMD, shell_args)
  # if argument "input_lines" is a string, makes it into a list
  if type(input_lines) is str:
    input_lines = [input_lines]
  # workaround to make Popen environment 'utf-8' compatible
  # since piping defaults to ascii
  my_env = os.environ
  my_env['PYTHONIOENCODING'] = 'utf-8'
  p = Popen(cmd, shell=True, stdout=PIPE,
            stdin=PIPE, stderr=PIPE, env=my_env)
  for line in input_lines:
    p.stdin.write(line + "\n")
    p.stdin.flush()
  return get_shell_cmd_result(p)
