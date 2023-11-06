#!/usr/bin/env impala-python
# -*- coding: utf-8 -*-
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

from __future__ import absolute_import, division, print_function
import http.client
import http.server
import logging
import os
import pexpect
import pytest
import re
import signal
import socket
import socketserver
import sys
import threading
from time import sleep

# This import is the actual ImpalaShell class from impala_shell.py.
# We rename it to ImpalaShellClass here because we later import another
# class called ImpalaShell from tests/shell/util.py, and we don't want
# to mask it.
from shell.impala_shell import ImpalaShell as ImpalaShellClass

from tempfile import NamedTemporaryFile
from tests.common.impala_service import ImpaladService
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal
from tests.common.test_dimensions import (
  create_client_protocol_dimension, create_client_protocol_strict_dimension,
  create_uncompressed_text_dimension, create_single_exec_option_dimension)
from tests.shell.util import (assert_var_substitution, ImpalaShell, get_impalad_port,
  get_shell_cmd, get_open_sessions_metric, spawn_shell, get_unused_port,
  create_impala_shell_executable_dimension, get_impala_shell_executable)

QUERY_FILE_PATH = os.path.join(os.environ['IMPALA_HOME'], 'tests', 'shell')

# Regex to match the interactive shell prompt that is expected after each command.
# Examples: hostname:21000, hostname:21050, hostname:28000
PROMPT_REGEX = r'\[[^:]+:2(1|8)0[0-9][0-9]\]'

LOG = logging.getLogger('test_shell_interactive')


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


class RequestHandler503(http.server.SimpleHTTPRequestHandler):
  """A custom http handler that checks for duplicate 'Host' headers from the most
  recent http request, and always returns a 503 http code."""

  def __init__(self, request, client_address, server):
    http.server.SimpleHTTPRequestHandler.__init__(self, request, client_address,
                                                   server)

  def should_send_body_text(self):
    # in RequestHandler503 we do not send any body text
    return False

  def do_POST(self):
    # Ensure that a 'Host' header is contained in the request before responding.
    assert "Host" in self.headers

    # Respond with 503.
    self.send_response(code=http.client.SERVICE_UNAVAILABLE,
                       message="Service Unavailable")
    # The Python 3 version of SimpleHTTPRequestHandler requires this to be called
    # explicitly
    self.end_headers()
    if self.should_send_body_text():
      # Optionally send body text with 503 message.
      self.wfile.write("EXTRA")


class RequestHandler503Extra(RequestHandler503):
  """"Override RequestHandler503 so as to send body text with the 503 message."""

  def __init__(self, request, client_address, server):
    RequestHandler503.__init__(self, request, client_address, server)

  def should_send_body_text(self):
    # in RequestHandler503Extra we will send body text
    return True


class TestHTTPServer503(object):
  def __init__(self, clazz):
    self.HOST = "localhost"
    self.PORT = get_unused_port()
    self.httpd = socketserver.TCPServer((self.HOST, self.PORT), clazz)

    self.http_server_thread = threading.Thread(target=self.httpd.serve_forever)
    self.http_server_thread.start()


def shutdown_server(server):
  """Helper method to shutdown a http server."""
  if server.httpd is not None:
    server.httpd.shutdown()
  if server.http_server_thread is not None:
    server.http_server_thread.join()


@pytest.yield_fixture
def http_503_server():
  """A fixture that creates an http server that returns a 503 http code."""
  server = TestHTTPServer503(RequestHandler503)
  yield server

  # Cleanup after test.
  shutdown_server(server)


@pytest.yield_fixture
def http_503_server_extra():
  """A fixture that creates an http server that returns a 503 http code with extra
  body text."""
  server = TestHTTPServer503(RequestHandler503Extra)
  yield server

  # Cleanup after test.
  shutdown_server(server)


class TestImpalaShellInteractive(ImpalaTestSuite):
  """Test the impala shell interactively"""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestImpalaShellInteractive, cls).add_test_dimensions()
    # Limit to uncompressed text with default exec options
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Run with both beeswax and HS2 to ensure that behaviour is the same.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_strict_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('protocol') != 'beeswax' or not v.get_value('strict_hs2_protocol'))
    # Test combination of Python versions and tarball/PyPI
    cls.ImpalaTestMatrix.add_dimension(create_impala_shell_executable_dimension())

  def _expect_with_cmd(self, proc, cmd, vector, expectations=(), db="default"):
    """Executes a command on the expect process instance and verifies a set of
    assertions defined by the expectations."""
    proc.sendline(cmd + ";")
    proc.expect(":{0}] {1}>".format(get_impalad_port(vector), db))
    if not expectations: return
    for e in expectations:
      assert e in proc.before

  def _wait_for_num_open_sessions(self, vector, impala_service, expected, err):
    """Helper method to wait for the number of open sessions to reach 'expected'."""
    metric_name = get_open_sessions_metric(vector)
    try:
      actual = impala_service.wait_for_metric_value(metric_name, expected)
    except AssertionError:
      LOG.exception("Error: %s" % err)
      raise
    assert actual == expected, err

  def test_local_shell_options(self, vector):
    """Test that setting the local shell options works"""
    proc = spawn_shell(get_shell_cmd(vector))
    proc.expect(":{0}] default>".format(get_impalad_port(vector)))
    if vector.get_value('strict_hs2_protocol'):
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: False", "LIVE_SUMMARY: False"))
      self._expect_with_cmd(proc, "set live_progress=true", vector)
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: False", "LIVE_SUMMARY: False"))
      self._expect_with_cmd(proc, "set live_summary=1", vector)
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: False", "LIVE_SUMMARY: False"))
    else:
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: True", "LIVE_SUMMARY: False"))
      self._expect_with_cmd(proc, "set live_progress=true", vector)
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: True", "LIVE_SUMMARY: False"))
      self._expect_with_cmd(proc, "set live_summary=1", vector)
      self._expect_with_cmd(proc, "set", vector,
          ("LIVE_PROGRESS: True", "LIVE_SUMMARY: True"))
    self._expect_with_cmd(proc, "set", vector,
        ("WRITE_DELIMITED: False", "VERBOSE: True"))
    self._expect_with_cmd(proc, "set", vector,
        ("DELIMITER: \\t", "OUTPUT_FILE: None"))
    self._expect_with_cmd(proc, "set write_delimited=true", vector)
    self._expect_with_cmd(proc, "set", vector, ("WRITE_DELIMITED: True", "VERBOSE: True"))
    self._expect_with_cmd(proc, "set DELIMITER=,", vector)
    self._expect_with_cmd(proc, "set", vector, ("DELIMITER: ,", "OUTPUT_FILE: None"))
    self._expect_with_cmd(proc, "set output_file=/tmp/clmn.txt", vector)
    self._expect_with_cmd(proc, "set", vector,
        ("DELIMITER: ,", "OUTPUT_FILE: /tmp/clmn.txt"))
    proc.sendeof()
    proc.wait()

  @pytest.mark.execute_serially
  def test_write_delimited(self, vector):
    """Test output rows in delimited mode"""
    p = ImpalaShell(vector)
    p.send_cmd("use tpch")
    p.send_cmd("set write_delimited=true")
    p.send_cmd("select * from nation")
    result = p.get_result()
    assert "+----------------+" not in result.stdout
    assert "21\tVIETNAM\t2" in result.stdout, result.stdout

  @pytest.mark.execute_serially
  def test_change_delimiter(self, vector):
    """Test change output delimiter if delimited mode is enabled"""
    p = ImpalaShell(vector)
    p.send_cmd("use tpch")
    p.send_cmd("set write_delimited=true")
    p.send_cmd("set delimiter=,")
    p.send_cmd("select * from nation")
    result = p.get_result()
    assert "21,VIETNAM,2" in result.stdout

  def test_vertical(self, vector):
    """Test output rows in vertical mode"""
    p = ImpalaShell(vector)
    p.send_cmd("use tpch")
    p.send_cmd("set vertical=true")
    p.send_cmd("select N_NATIONKEY, N_NAME from nation limit 1")
    result = p.get_result()
    assert "+----------------+" not in result.stdout
    assert "************************************** " \
      "1.row **************************************" in result.stdout, result.stdout
    assert "n_nationkey: " in result.stdout, result.stdout
    assert "n_name: " in result.stdout, result.stdout

  @pytest.mark.execute_serially
  def test_print_to_file(self, vector):
    """Test print to output file and unset"""
    # test print to file
    p1 = ImpalaShell(vector)
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
    p2 = ImpalaShell(vector)
    p2.send_cmd("use tpch")
    p2.send_cmd("set output_file=%s" % local_file.name)
    p2.send_cmd("unset output_file")
    p2.send_cmd("select * from nation")
    result = p2.get_result()
    assert "VIETNAM" in result.stdout

  def test_compute_stats_with_live_progress_options(self, vector, unique_database):
    """Test that setting LIVE_PROGRESS options won't cause COMPUTE STATS query fail"""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Live progress not supported in strict hs2 mode.")
    p = ImpalaShell(vector)
    p.send_cmd("set live_progress=True")
    p.send_cmd("set live_summary=True")
    table = "{0}.live_progress_option".format(unique_database)
    p.send_cmd('create table {0}(col int);'.format(table))
    try:
      p.send_cmd('compute stats {0};'.format(table))
    finally:
      p.send_cmd('drop table if exists {0};'.format(table))
    result = p.get_result()
    assert "Updated 1 partition(s) and 1 column(s)" in result.stdout

  def test_escaped_quotes(self, vector):
    """Test escaping quotes"""
    # test escaped quotes outside of quotes
    result = run_impala_shell_interactive(vector, "select \\'bc';")
    if vector.get_value('strict_hs2_protocol'):
      assert "ParseException" in result.stderr
    else:
      assert "Unexpected character" in result.stderr
    result = run_impala_shell_interactive(vector, "select \\\"bc\";")
    if vector.get_value('strict_hs2_protocol'):
      assert "ParseException" in result.stderr
    else:
      assert "Unexpected character" in result.stderr
    # test escaped quotes within quotes
    result = run_impala_shell_interactive(vector, "select 'ab\\'c';")
    assert "Fetched 1 row(s)" in result.stderr
    result = run_impala_shell_interactive(vector, "select \"ab\\\"c\";")
    assert "Fetched 1 row(s)" in result.stderr

  @pytest.mark.execute_serially
  def test_cancellation(self, vector):
    impalad = ImpaladService(socket.getfqdn())
    assert impalad.wait_for_num_in_flight_queries(0)
    command = "select sleep(10000);"
    p = ImpalaShell(vector)
    p.send_cmd(command)
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "Cancelled" not in result.stderr
    assert impalad.wait_for_num_in_flight_queries(0)

    p = ImpalaShell(vector)
    sleep(3)
    os.kill(p.pid(), signal.SIGINT)
    result = p.get_result()
    assert "^C" in result.stderr

  @pytest.mark.execute_serially
  def test_cancellation_mid_command(self, vector):
    """Test that keyboard interrupt cancels multiline query strings"""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Cancellation infrastructure does not " +
          "work in strict hs2 mode.")
    shell_cmd = get_shell_cmd(vector)
    multiline_query = ["select column_1\n", "from table_1\n", "where ..."]
    # Test keyboard interrupt mid line
    child_proc = spawn_shell(shell_cmd)
    child_proc.expect(PROMPT_REGEX)
    for query_line in multiline_query:
      child_proc.send(query_line)
    child_proc.sendintr()
    child_proc.expect("\^C")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline('quit;')
    child_proc.wait()
    # Test keyboard interrupt in new line
    child_proc = spawn_shell(shell_cmd)
    child_proc.expect(PROMPT_REGEX)
    for query_line in multiline_query:
      child_proc.send(query_line)
    child_proc.send("\n")
    child_proc.expect(">")
    child_proc.sendintr()
    child_proc.expect("> \^C")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline('quit;')
    child_proc.wait()

  def test_unicode_input(self, vector):
    """Test queries containing non-ascii input"""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Failed, need to investigate.")
    # test a unicode query spanning multiple lines
    unicode_bytes = u'\ufffd'.encode('utf-8')
    args = "select '{0}'\n;".format(unicode_bytes)
    result = run_impala_shell_interactive(vector, args)
    assert "Fetched 1 row(s)" in result.stderr, result.stderr
    # IMPALA-10415: Multi queries in a line containing unicode chars.
    args = "select '{0}'; select '{0}'".format(unicode_bytes)
    result = run_impala_shell_interactive(vector, args)
    assert "Fetched 1 row(s)" in result.stderr, result.stderr
    # IMPALA-10415: Multiline query with history enabled and unicode chars.
    # readline gets its input from tty, so using stdin does not work.
    shell_cmd = get_shell_cmd(vector)
    child_proc = spawn_shell(shell_cmd)
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline("select '{0}'\n;".format(unicode_bytes))
    child_proc.expect("Fetched 1 row\(s\) in [0-9]+\.?[0-9]*s")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline('quit;')
    child_proc.wait()

  def test_unicode_column(self, vector, unique_database):
    """Tests unicode column name support"""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Failed, need to investigate.")
    args = ("create table {0}.test_tbl(`세율중분류구분코드` int, s string COMMENT 'String col')"
            " STORED AS TEXTFILE;".format(unique_database))
    result = run_impala_shell_interactive(vector, args)
    assert "Fetched 1 row(s)" in result.stderr, result.stderr
    args = ("describe {0}.test_tbl;"
            .format(unique_database))
    result = run_impala_shell_interactive(vector, args)
    assert "Fetched 2 row(s)" in result.stderr, result.stderr
    assert "세율중분류구분코드" in result.stdout
    args = ("insert into table {0}.test_tbl values(1, 'Alice');"
            .format(unique_database))
    result = run_impala_shell_interactive(vector, args)
    assert "Modified 1 row(s)" in result.stderr, result.stderr
    args = ("select * from {0}.test_tbl;"
            .format(unique_database))
    result = run_impala_shell_interactive(vector, args)
    assert "Fetched 1 row(s)" in result.stderr, result.stderr
    assert "세율중분류구분코드" in result.stdout


  def test_welcome_string(self, vector):
    """Test that the shell's welcome message is only printed once
    when the shell is started. Ensure it is not reprinted on errors.
    Regression test for IMPALA-1153
    """
    result = run_impala_shell_interactive(vector, 'asdf;')
    assert result.stdout.count("Welcome to the Impala shell") == 1
    result = run_impala_shell_interactive(vector, 'select * from non_existent_table;')
    assert result.stdout.count("Welcome to the Impala shell") == 1

  def test_disconnected_shell(self, vector):
    """Test that the shell presents a disconnected prompt if it can't connect
    """
    result = run_impala_shell_interactive(vector, 'asdf;', shell_args=['-ifoo'],
                                          wait_until_connected=False)
    assert ImpalaShellClass.DISCONNECTED_PROMPT in result.stdout, result.stderr

  def test_quit_no_reconnect(self, vector):
    """Test that a disconnected shell does not try to reconnect if quitting"""
    result = run_impala_shell_interactive(vector, 'quit;', shell_args=['-ifoo'],
                                          wait_until_connected=False)
    assert "reconnect" not in result.stderr

    result = run_impala_shell_interactive(vector, 'exit;', shell_args=['-ifoo'],
                                          wait_until_connected=False)
    assert "reconnect" not in result.stderr

    # Null case: This is not quitting, so it will result in an attempt to reconnect.
    result = run_impala_shell_interactive(vector, 'show tables;', shell_args=['-ifoo'],
                                          wait_until_connected=False)
    assert "reconnect" in result.stderr

  def test_bash_cmd_timing(self, vector):
    """Test existence of time output in bash commands run from shell"""
    args = ["! ls;"]
    result = run_impala_shell_interactive(vector, args)
    assert "Executed in" in result.stderr

  @SkipIfLocal.multiple_impalad
  @pytest.mark.execute_serially
  def test_reconnect(self, vector):
    """Regression Test for IMPALA-1235

    Verifies that a connect command by the user is honoured.
    """
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Connect command not supported in strict hs2 mode")
    try:
      # Disconnect existing clients so there are no open sessions.
      self.close_impala_clients()

      hostname = socket.getfqdn()
      initial_impala_service = ImpaladService(hostname)
      target_impala_service = ImpaladService(hostname, webserver_port=25001,
          beeswax_port=21001, hs2_port=21051, hs2_http_port=28001)
      protocol = vector.get_value("protocol").lower()
      if protocol == "hs2":
        target_port = 21051
      elif protocol == "hs2-http":
        target_port = 28001
      else:
        assert protocol == "beeswax"
        target_port = 21001
      # This test is running serially, so there shouldn't be any open sessions, but wait
      # here in case a session from a previous test hasn't been fully closed yet.
      self._wait_for_num_open_sessions(vector, initial_impala_service, 0,
          "first impalad should not have any remaining open sessions.")
      self._wait_for_num_open_sessions(vector, target_impala_service, 0,
          "second impalad should not have any remaining open sessions.")
      # Connect to the first impalad
      p = ImpalaShell(vector)

      # Make sure we're connected <hostname>:<port>
      self._wait_for_num_open_sessions(vector, initial_impala_service, 1,
          "Not connected to %s:%d" % (hostname, get_impalad_port(vector)))
      p.send_cmd("connect %s:%d" % (hostname, target_port))

      # The number of sessions on the target impalad should have been incremented.
      self._wait_for_num_open_sessions(vector,
          target_impala_service, 1, "Not connected to %s:%d" % (hostname, target_port))
      assert "[%s:%d] default>" % (hostname, target_port) in p.get_result().stdout

      # The number of sessions on the initial impalad should have been decremented.
      self._wait_for_num_open_sessions(vector, initial_impala_service, 0,
          "Connection to %s:%d should have been closed" % (
            hostname, get_impalad_port(vector)))

    finally:
      self.create_impala_clients()

  @pytest.mark.execute_serially
  def test_ddl_queries_are_closed(self, vector):
    """Regression test for IMPALA-1317

    The shell does not call close() for alter, use and drop queries, leaving them in
    flight. This test issues those queries in interactive mode, and checks the debug
    webpage to confirm that they've been closed.
    TODO: Add every statement type.
    """
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Inflight query support does not exist in strict hs2 mode.")
    # IMPALA-10312: deflake the test by increasing timeout above the default
    TIMEOUT_S = 30
    # Disconnect existing clients so there are no open sessions.
    self.close_impala_clients()

    TMP_DB = 'inflight_test_db'
    TMP_TBL = 'tmp_tbl'
    MSG = '%s query should be closed'
    NUM_QUERIES = 'impala-server.num-queries'

    impalad = ImpaladService(socket.getfqdn())
    self._wait_for_num_open_sessions(vector, impalad, 0,
        "Open sessions found after closing all clients.")
    p = ImpalaShell(vector)
    try:
      start_num_queries = impalad.get_metric_value(NUM_QUERIES)
      p.send_cmd('create database if not exists %s' % TMP_DB)
      p.send_cmd('use %s' % TMP_DB)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 2)
      assert impalad.wait_for_num_in_flight_queries(0, TIMEOUT_S), MSG % 'use'
      p.send_cmd('create table %s(i int)' % TMP_TBL)
      p.send_cmd('alter table %s add columns (j int)' % TMP_TBL)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 4)
      assert impalad.wait_for_num_in_flight_queries(0, TIMEOUT_S), MSG % 'alter'
      p.send_cmd('drop table %s' % TMP_TBL)
      impalad.wait_for_metric_value(NUM_QUERIES, start_num_queries + 5)
      assert impalad.wait_for_num_in_flight_queries(0, TIMEOUT_S), MSG % 'drop'
    finally:
      # get_result() must be called to exit the shell.
      p.get_result()
      self._wait_for_num_open_sessions(vector, impalad, 0,
          "shell should close sessions.")
      run_impala_shell_interactive(vector, "drop table if exists %s.%s;" % (
          TMP_DB, TMP_TBL))
      run_impala_shell_interactive(vector, "drop database if exists foo;")
      self.create_impala_clients()

  def test_multiline_queries_in_history(self, vector, tmp_history_file):
    """Test to ensure that multiline queries with comments are preserved in history

    Ensure that multiline queries are preserved when they're read back from history.
    Additionally, also test that comments are preserved.
    """
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("History command not supported in strict hs2 mode.")
    # readline gets its input from tty, so using stdin does not work.
    child_proc = spawn_shell(get_shell_cmd(vector))
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
    child_proc.wait()
    p = ImpalaShell(vector)
    p.send_cmd('history')
    result = p.get_result()
    for _, history_entry in queries:
      assert history_entry in result.stderr, "'%s' not in '%s'" % (history_entry,
                                                                   result.stderr)

  def test_history_does_not_duplicate_on_interrupt(self, vector, tmp_history_file):
    """This test verifies that once the cmdloop is broken the history file will not be
    re-read. The cmdloop can be broken when the user sends a SIGINT or exceptions
    occur."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("History command not supported in strict hs2 mode.")
    # readline gets its input from tty, so using stdin does not work.
    shell_cmd = get_shell_cmd(vector)
    child_proc = spawn_shell(shell_cmd)

    # initialize history
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline("select 1;")
    child_proc.expect("Fetched 1 row\(s\) in [0-9]+\.?[0-9]*s")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline("quit;")
    child_proc.wait()

    # create a new shell and send SIGINT
    child_proc = spawn_shell(shell_cmd)
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendintr()
    child_proc.expect("\^C")
    child_proc.sendline("select 2;")
    child_proc.expect("Fetched 1 row\(s\) in [0-9]+\.?[0-9]*s")
    child_proc.expect(PROMPT_REGEX)
    child_proc.sendline("quit;")
    child_proc.wait()

    # check history in a new shell instance
    p = ImpalaShell(vector)
    p.send_cmd('history')
    result = p.get_result().stderr.splitlines()
    assert "[1]: select 1;" == result[1]
    assert "[2]: quit;" == result[2]
    assert "[3]: select 2;" == result[3]
    assert "[4]: quit;" == result[4]

  def test_history_file_option(self, vector, tmp_history_file):
    """
    Setting the 'tmp_history_file' fixture above means that the IMPALA_HISTFILE
    environment will be overridden. Here we override that environment by passing
    the --history_file command line option, ensuring that the history ends up
    in the appropriate spot.
    """
    with NamedTemporaryFile() as new_hist:
      shell_cmd = get_shell_cmd(vector) + ["--history_file=%s" % new_hist.name]
      child_proc = spawn_shell(shell_cmd)
      child_proc.expect(":{0}] default>".format(get_impalad_port(vector)))
      self._expect_with_cmd(child_proc, "select 'hi'", vector, ('hi'))
      child_proc.sendline('exit;')
      child_proc.expect(pexpect.EOF)
      history_contents = open(new_hist.name).read()
      assert "select 'hi'" in history_contents

  def test_rerun(self, vector, tmp_history_file):
    """Smoke test for the 'rerun' command"""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Rerun not supported in strict hs2 mode.")
    child_proc = spawn_shell(get_shell_cmd(vector))
    child_proc.expect(":{0}] default>".format(get_impalad_port(vector)))
    self._expect_with_cmd(child_proc, "@1", vector, ("Command index out of range"))
    self._expect_with_cmd(child_proc, "rerun -1", vector,
        ("Command index out of range"))
    self._expect_with_cmd(child_proc, "select 'first_command'", vector,
        ("first_command"))
    self._expect_with_cmd(child_proc, "rerun 1", vector, ("first_command"))
    self._expect_with_cmd(child_proc, "@ -1", vector, ("first_command"))
    self._expect_with_cmd(child_proc, "select 'second_command'", vector,
        ("second_command"))
    child_proc.sendline('history;')
    child_proc.expect(":{0}] default>".format(get_impalad_port(vector)))
    assert '[1]: select \'first_command\';' in child_proc.before
    assert '[2]: select \'second_command\';' in child_proc.before
    assert '[3]: history;' in child_proc.before
    # Rerunning command should not add an entry into history.
    assert '[4]' not in child_proc.before
    self._expect_with_cmd(child_proc, "@0", vector, ("Command index out of range"))
    self._expect_with_cmd(child_proc, "rerun   4", vector, ("Command index out of range"))
    self._expect_with_cmd(child_proc, "@-4", vector, ("Command index out of range"))
    self._expect_with_cmd(child_proc, " @ 3 ", vector, ("second_command"))
    self._expect_with_cmd(child_proc, "@-3", vector, ("first_command"))
    self._expect_with_cmd(child_proc, "@", vector,
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "@1foo", vector,
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "@1 2", vector,
                          ("Command index to be rerun must be an integer."))
    self._expect_with_cmd(child_proc, "rerun1", vector, ("Syntax error"))
    child_proc.sendline('quit;')
    child_proc.wait()

  def test_tip(self, vector):
    """Smoke test for the TIP command"""
    # Temporarily add impala_shell module to path to get at TIPS list for verification
    sys.path.append("%s/shell/" % os.environ['IMPALA_HOME'])
    try:
      import impala_shell
    finally:
      sys.path = sys.path[:-1]
    result = run_impala_shell_interactive(vector, "tip;")
    for t in impala_shell.TIPS:
      if t in result.stderr: return
    assert False, "No tip found in output %s" % result.stderr

  def test_var_substitution(self, vector):
    cmds = open(os.path.join(QUERY_FILE_PATH, 'test_var_substitution.sql')).read()
    args = ["--var=foo=123", "--var=BAR=456", "--delimited", "--output_delimiter= "]
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert_var_substitution(result)

  def test_query_option_configuration(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Set command not supported in strict hs2 mode.")
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'impalarc_with_query_options')
    args = ['-Q', 'MT_dop=1', '--query_option=MAX_ERRORS=200',
            '--config_file=%s' % rcfile_path]
    cmds = "set all;"
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tMT_DOP: 1" in result.stdout
    assert "\tMAX_ERRORS: 200" in result.stdout
    assert "\tEXPLAIN_LEVEL: 2" in result.stdout
    assert "INVALID_QUERY_OPTION is not supported for the impalad being connected to, "\
           "ignoring." in result.stdout
    # Verify that query options under [impala] override those under [impala.query_options]
    assert "\tDEFAULT_FILE_FORMAT: avro" in result.stdout

    # unset all query options
    cmds = "unset all;set all;"
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tMT_DOP: " in result.stdout
    assert "\tMAX_ERRORS: [100]" in result.stdout
    assert "\tEXPLAIN_LEVEL: [STANDARD]" in result.stdout
    assert "INVALID_QUERY_OPTION is not supported for the impalad being connected to, "\
           "ignoring." in result.stdout
    assert "\tDEFAULT_FILE_FORMAT: [TEXT]" in result.stdout

  def test_commandline_flag_disable_live_progress(self, vector):
    """Test the command line flag disable_live_progress with live_progress."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Live option not supported in strict hs2 mode.")
    # By default, shell option live_progress is set to True in the interactive mode.
    cmds = "set all;"
    result = run_impala_shell_interactive(vector, cmds)
    assert "\tLIVE_PROGRESS: True" in result.stdout
    # override the default option through command line argument.
    args = ['--disable_live_progress']
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tLIVE_PROGRESS: False" in result.stdout
    # set live_progress as True with config file.
    # override the option in config file through command line argument.
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'good_impalarc3')
    args = ['--disable_live_progress', '--config_file=%s' % rcfile_path]
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tLIVE_PROGRESS: False" in result.stdout

  def test_commandline_flag_strict_hs2_protocol(self, vector):
    """Test the command line flag strict_hs2_protocol that it disables
       live_progress and live_summary"""
    if not vector.get_value('strict_hs2_protocol'):
      pytest.skip("Test only applies to strict_hs2_protocol.")

    cmds = "set all;"
    # override the default option through command line argument.
    args = ['--strict_hs2_protocol']
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tLIVE_PROGRESS: False" in result.stdout
    assert "\tLIVE_SUMMARY: False" in result.stdout

  def test_live_option_configuration(self, vector):
    """Test the optional configuration file with live_progress and live_summary."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Live option not supported in strict hs2 mode.")
    # Positive tests
    # set live_summary and live_progress as True with config file
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'good_impalarc3')
    cmd_line_args = ['--config_file=%s' % rcfile_path]
    cmds = "set all;"
    result = run_impala_shell_interactive(vector, cmds, shell_args=cmd_line_args)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    assert "\tLIVE_SUMMARY: True" in result.stdout
    assert "\tLIVE_PROGRESS: True" in result.stdout

    # set live_summary and live_progress as False with config file
    rcfile_path = os.path.join(QUERY_FILE_PATH, 'good_impalarc4')
    args = ['--config_file=%s' % rcfile_path]
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert 'WARNING:' not in result.stderr, \
      "A valid config file should not trigger any warning: {0}".format(result.stderr)
    assert "\tLIVE_SUMMARY: False" in result.stdout
    assert "\tLIVE_PROGRESS: False" in result.stdout
    # override options in config file through command line arguments
    args = ['--live_progress', '--live_summary', '--config_file=%s' % rcfile_path]
    result = run_impala_shell_interactive(vector, cmds, shell_args=args)
    assert "\tLIVE_SUMMARY: True" in result.stdout
    assert "\tLIVE_PROGRESS: True" in result.stdout

  def test_source_file(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Version not supported in strict hs2 mode.")
    cwd = os.getcwd()
    try:
      # Change working dir so that SOURCE command in shell.cmds can find shell2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      # IMPALA-5416: Test that a command following 'source' won't be run twice.
      result = run_impala_shell_interactive(vector, "source shell.cmds;select \"second "
                                            "command\";")
      assert "Query: USE FUNCTIONAL" in result.stderr
      assert "Query: SHOW TABLES" in result.stderr
      assert "alltypes" in result.stdout
      # This is from shell2.cmds, the result of sourcing a file from a sourced file.
      assert "SELECT VERSION()" in result.stderr
      assert "version()" in result.stdout
      assert len(re.findall("'second command'", result.stdout)) == 1
      # IMPALA-5416: Test that two source commands on a line won't crash the shell.
      result = run_impala_shell_interactive(
          vector, "source shell.cmds;source shell.cmds;")
      assert len(re.findall("version\(\)", result.stdout)) == 2
    finally:
      os.chdir(cwd)

  def test_source_file_with_errors(self, vector):
    full_path = "%s/tests/shell/shell_error.cmds" % os.environ['IMPALA_HOME']
    result = run_impala_shell_interactive(vector, "source %s;" % full_path)
    assert "Could not execute command: USE UNKNOWN_DATABASE" in result.stderr
    assert "Query: USE FUNCTIONAL" not in result.stderr

    result = run_impala_shell_interactive(vector, "source %s;" % full_path, ['-c'])
    assert "Could not execute command: USE UNKNOWN_DATABASE" in result.stderr,\
        result.stderr
    assert "Query: USE FUNCTIONAL" in result.stderr, result.stderr
    assert "Query: SHOW TABLES" in result.stderr, result.stderr
    assert "alltypes" in result.stdout, result.stdout

  def test_source_missing_file(self, vector):
    full_path = "%s/tests/shell/doesntexist.cmds" % os.environ['IMPALA_HOME']
    result = run_impala_shell_interactive(vector, "source %s;" % full_path)
    assert "No such file or directory" in result.stderr

  def test_zero_row_fetch(self, vector):
    # IMPALA-4418: DROP and USE are generally exceptional statements where
    # the client does not fetch. For statements returning 0 rows we do not
    # want an empty line in stdout.
    result = run_impala_shell_interactive(vector, "-- foo \n use default;")
    assert re.search('> \[', result.stdout)
    result = run_impala_shell_interactive(vector,
        "select * from functional.alltypes limit 0;")
    assert "Fetched 0 row(s)" in result.stderr
    assert re.search('> \[', result.stdout)

  def test_set_and_set_all(self, vector):
    """IMPALA-2181. Tests the outputs of SET and SET ALL commands. SET should contain the
    REGULAR and ADVANCED options only. SET ALL should contain all the options grouped by
    display level."""
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Set command not supported in strict hs2 mode.")
    shell1 = ImpalaShell(vector)
    shell1.send_cmd("set")
    result = shell1.get_result()
    assert "Query options (defaults shown in []):" in result.stdout
    assert "ABORT_ON_ERROR" in result.stdout
    assert "Advanced Query Options:" in result.stdout
    assert "APPX_COUNT_DISTINCT" in result.stdout
    assert vector.get_value("protocol") in ("hs2", "hs2-http")\
        or "SUPPORT_START_OVER" in result.stdout
    # Development, deprecated and removed options should not be shown.
    # Note: there are currently no deprecated options
    assert "Development Query Options:" not in result.stdout
    assert "DEBUG_ACTION" not in result.stdout  # Development option.
    assert "MAX_IO_BUFFERS" not in result.stdout  # Removed option.

    shell2 = ImpalaShell(vector)
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
    assert vector.get_value("protocol") in ("hs2", "hs2-http")\
        or "SUPPORT_START_OVER" in advanced_part
    assert "DEBUG_ACTION" in development_part
    # Removed options should not be shown.
    assert "MAX_IO_BUFFERS" not in result.stdout

  def check_command_case_sensitivity(self, vector, command, expected):
    shell = ImpalaShell(vector)
    shell.send_cmd(command)
    assert expected in shell.get_result().stderr

  def test_unexpected_conversion_for_literal_string_to_lowercase(self, vector):
    # IMPALA-4664: Impala shell can accidentally convert certain literal
    # strings to lowercase. Impala shell splits each command into tokens
    # and then converts the first token to lowercase to figure out how it
    # should execute the command. The splitting is done by spaces only.
    # Thus, if the user types a TAB after the SELECT, the first token after
    # the split becomes the SELECT plus whatever comes after it.
    result = run_impala_shell_interactive(vector, "select'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)
    result = run_impala_shell_interactive(vector, "select\t'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)
    result = run_impala_shell_interactive(vector, "select\n'MUST_HAVE_UPPER_STRING'")
    assert re.search('MUST_HAVE_UPPER_STRING', result.stdout)

  def test_case_sensitive_command(self, vector):
    # IMPALA-2640: Make a given command case-sensitive
    cwd = os.getcwd()
    try:
      self.check_command_case_sensitivity(vector, "sElEcT VERSION()", "Query: sElEcT")
      self.check_command_case_sensitivity(vector, "sEt VaR:FoO=bOo", "Variable FOO")
      self.check_command_case_sensitivity(vector, "sHoW tables", "Query: sHoW")
      # Change working dir so that SOURCE command in shell_case_sensitive.cmds can
      # find shell_case_sensitive2.cmds.
      os.chdir("%s/tests/shell/" % os.environ['IMPALA_HOME'])
      result = run_impala_shell_interactive(vector,
        "sOuRcE shell_case_sensitive.cmds; SeLeCt 'second command'")
      print(result.stderr)

      assert "Query: uSe FUNCTIONAL" in result.stderr
      assert "Query: ShOw TABLES" in result.stderr
      assert "alltypes" in result.stdout
      # This is from shell_case_sensitive2.cmds, the result of sourcing a file
      # from a sourced file.
      print(result.stderr)
      assert "SeLeCt 'second command'" in result.stderr
    finally:
      os.chdir(cwd)

  def test_line_with_leading_comment(self, vector, unique_database):
    # IMPALA-2195: A line with a comment produces incorrect command.
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Leading comments not supported in strict hs2 mode.")
    table = "{0}.leading_comment".format(unique_database)
    run_impala_shell_interactive(vector, 'create table {0} (i int);'.format(table))
    result = run_impala_shell_interactive(vector, '-- comment\n'
                                          'insert into {0} values(1);'.format(table))
    assert 'Modified 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '-- comment\n'
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '--한글\n'
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* 한글 */\n'
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* comment */\n'
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* comment1 */\n'
                                          '-- comment2\n'
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* comment1\n'
                                          'comment2 */ select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* select * from {0} */ '
                                          'select * from {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* comment */ help use')
    assert 'Executes a USE... query' in result.stdout
    result = run_impala_shell_interactive(vector, '-- comment\n'
                                          ' help use;')
    assert 'Executes a USE... query' in result.stdout
    result = run_impala_shell_interactive(vector, '/* comment1 */\n'
                                          '-- comment2\n'
                                          'desc {0};'.format(table))
    assert 'Fetched 1 row(s)' in result.stderr
    result = run_impala_shell_interactive(vector, '/* comment1 */\n'
                                          '-- comment2\n'
                                          'help use;')
    assert 'Executes a USE... query' in result.stdout

  def test_line_ends_with_comment(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("IMPALA-10827: Not working in strict hs2 mode.")
    # IMPALA-5269: Test lines that end with a comment.
    queries = ['select 1 + 1; --comment',
               'select 1 + 1 --comment\n;']
    for query in queries:
      result = run_impala_shell_interactive(vector, query)
      assert '| 1 + 1 |' in result.stdout
      assert '| 2     |' in result.stdout

    queries = ['select \'some string\'; --comment',
               'select \'some string\' --comment\n;']
    for query in queries:
      result = run_impala_shell_interactive(vector, query)
      assert '| \'some string\' |' in result.stdout
      assert '| some string   |' in result.stdout

    queries = ['select "--"; -- "--"',
               'select \'--\'; -- "--"',
               'select "--" -- "--"\n;',
               'select \'--\' -- "--"\n;']
    for query in queries:
      result = run_impala_shell_interactive(vector, query)
      assert '| \'--\' |' in result.stdout
      assert '| --   |' in result.stdout

    query = ('select * from (\n' +
             'select count(*) from functional.alltypes\n' +
             ') v; -- Incomplete SQL statement in this line')
    result = run_impala_shell_interactive(vector, query)
    assert '| count(*) |' in result.stdout

    query = ('select id from functional.alltypes\n' +
             'order by id; /*\n' +
             '* Multi-line comment\n' +
             '*/')
    result = run_impala_shell_interactive(vector, query)
    assert '| id   |' in result.stdout

  def test_fix_infinite_loop(self, vector):
    # IMPALA-6337: Fix infinite loop.

    result = run_impala_shell_interactive(vector, "select 1 + 1; \"\n;\";")
    if vector.get_value('strict_hs2_protocol'):
      assert '| 2   |' in result.stdout
    else:
      assert '| 2     |' in result.stdout
    result = run_impala_shell_interactive(vector, "select '1234'\";\n;\n\";")
    assert '| 1234 |' in result.stdout
    result = run_impala_shell_interactive(vector, "select 1 + 1; \"\n;\"\n;")
    if vector.get_value('strict_hs2_protocol'):
      assert '| 2   |' in result.stdout
    else:
      assert '| 2     |' in result.stdout
    result = run_impala_shell_interactive(vector, "select '1\\'23\\'4'\";\n;\n\";")
    assert '| 1\'23\'4 |' in result.stdout
    result = run_impala_shell_interactive(vector, "select '1\"23\"4'\";\n;\n\";")
    assert '| 1"23"4 |' in result.stdout

  def test_comment_with_quotes(self, vector):
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
      result = run_impala_shell_interactive(vector, query)
      assert '| 1 ' in result.stdout

  def test_shell_prompt(self, vector):
    shell_cmd = get_shell_cmd(vector)
    proc = spawn_shell(shell_cmd)
    proc.expect(":{0}] default>".format(get_impalad_port(vector)))
    self._expect_with_cmd(proc, "use foo", vector, (), 'default')
    self._expect_with_cmd(proc, "use functional", vector, (), 'functional')
    self._expect_with_cmd(proc, "use foo", vector, (), 'functional')
    self._expect_with_cmd(proc, 'use `tpch`', vector, (), 'tpch')
    self._expect_with_cmd(proc, 'use ` tpch `', vector, (), 'tpch')

    proc = spawn_shell(shell_cmd + ['-d', 'functional'])
    proc.expect(":{0}] functional>".format(get_impalad_port(vector)))
    self._expect_with_cmd(proc, "use foo", vector, (), 'functional')
    self._expect_with_cmd(proc, "use tpch", vector, (), 'tpch')
    self._expect_with_cmd(proc, "use foo", vector, (), 'tpch')

    proc = spawn_shell(shell_cmd + ['-d', ' functional '])
    proc.expect(":{0}] functional>".format(get_impalad_port(vector)))

    proc = spawn_shell(shell_cmd + ['-d', '` functional `'])
    proc.expect(":{0}] functional>".format(get_impalad_port(vector)))

    # Start an Impala shell with an invalid DB.
    proc = spawn_shell(shell_cmd + ['-d', 'foo'])
    proc.expect(":{0}] default>".format(get_impalad_port(vector)))
    self._expect_with_cmd(proc, "use foo", vector, (), 'default')
    self._expect_with_cmd(proc, "use functional", vector, (), 'functional')
    self._expect_with_cmd(proc, "use foo", vector, (), 'functional')
    proc.sendeof()
    proc.wait()

  def test_strip_leading_comment(self, vector):
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

  def test_malformed_query(self, vector):
    """Test the handling of malformed query without closing quotation"""
    shell = ImpalaShell(vector)
    query = "with v as (select 1) \nselect foo('\\\\'), ('bar \n;"
    shell.send_cmd(query)
    result = shell.get_result()
    if vector.get_value('strict_hs2_protocol'):
      assert "ParseException" in result.stderr,\
             result.stderr
    else:
      assert "ERROR: ParseException: Unmatched string literal" in result.stderr,\
             result.stderr

  def test_utf8_error_message(self, vector):
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("The now() function is Impala specific.")
    """Test UTF-8 error messages are shown correctly"""
    shell = ImpalaShell(vector)
    query = "select cast(now() as string format 'yyyy年MM月dd日')"
    shell.send_cmd(query)
    result = shell.get_result()
    assert "ERROR: Bad date/time conversion format: yyyy年MM月dd日" in result.stderr,\
           result.stderr

  def test_timezone_validation(self, vector):
    """Test that query option TIMEZONE is validated when executing a query.

       Query options are not sent to the coordinator immediately, so the error checking
       will only happen when running a query.
    """
    if vector.get_value('strict_hs2_protocol'):
      pytest.skip("Set command not supported in strict hs2 mode.")
    p = ImpalaShell(vector)
    p.send_cmd('set timezone=BLA;')
    p.send_cmd('select 1;')
    results = p.get_result()
    assert "Fetched 1 row" not in results.stderr
    # assert "ERROR: Errors parsing query options" in results.stderr, results.stderr
    assert "Invalid timezone name 'BLA'" in results.stderr, results.stderr

  def test_with_clause(self, vector):
    # IMPALA-7939: Fix issue where CTE that contains "insert", "upsert", "update", or
    # "delete" is categorized as a DML statement.
    for keyword in ["insert", "upsert", "update", "delete", "\\'insert\\'",
                    "\\'upsert\\'", "\\'update\\'", "\\'delete\\'"]:
      p = ImpalaShell(vector)
      cmd = ("with foo as "
             "(select * from functional.alltypestiny where string_col='%s') "
             "select * from foo limit 1" % keyword)
      p.send_cmd(cmd)
      result = p.get_result()
      assert "Fetched 0 row" in result.stderr

  def test_quotes_in_with_clause(self, vector):
    # IMPALA-10051: This test verifies that the fix prevents ValueErrors caused by
    # shlex library when quotes and whitespace characters are mixed.
    p = ImpalaShell(vector)
    cmd = ("with foo as "
           "(select *, regexp_replace(string_col,\"[a-zA-Z]\",\"+ \") "
           "from functional.alltypestiny) "
           "select * from foo limit 1")
    p.send_cmd(cmd)
    result = p.get_result()
    assert "Fetched 1 row" in result.stderr

  def test_http_interactions(self, vector, http_503_server):
    """Test interactions with the http server when using hs2-http protocol.
    Check that the shell prints a good message when the server returns a 503 error."""
    protocol = vector.get_value("protocol")
    if protocol != 'hs2-http':
      pytest.skip()

    # Check that we get a message about the 503 error when we try to connect.
    impala_shell_executable = get_impala_shell_executable(vector)
    shell_args = ["--protocol={0}".format(protocol),
                  "-i{0}:{1}".format(http_503_server.HOST, http_503_server.PORT)]
    shell_proc = spawn_shell(impala_shell_executable + shell_args)
    shell_proc.expect("HTTP code 503", timeout=10)

  def test_http_interactions_extra(self, vector, http_503_server_extra):
    """Test interactions with the http server when using hs2-http protocol.
    Check that the shell prints a good message when the server returns a 503 error,
    including the body text from the message."""
    protocol = vector.get_value("protocol")
    if protocol != 'hs2-http':
      pytest.skip()

    # Check that we get a message about the 503 error when we try to connect.
    impala_shell_executable = get_impala_shell_executable(vector)
    shell_args = ["--protocol={0}".format(protocol),
                  "-i{0}:{1}".format(http_503_server_extra.HOST,
                                     http_503_server_extra.PORT)]
    shell_proc = spawn_shell(impala_shell_executable + shell_args)
    shell_proc.expect("HTTP code 503: Service Unavailable \[EXTRA\]", timeout=10)


def run_impala_shell_interactive(vector, input_lines, shell_args=None,
                                 wait_until_connected=True):
  """Runs a command in the Impala shell interactively."""
  # if argument "input_lines" is a string, makes it into a list
  if type(input_lines) is str:
    input_lines = [input_lines]
  # workaround to make Popen environment 'utf-8' compatible
  # since piping defaults to ascii
  my_env = os.environ
  my_env['PYTHONIOENCODING'] = 'utf-8'
  p = ImpalaShell(vector, args=shell_args, env=my_env,
      wait_until_connected=wait_until_connected)
  for line in input_lines:
    LOG.info("Running %s" % line)
    p.send_cmd(line)
  return p.get_result()
