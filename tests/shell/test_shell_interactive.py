#!/usr/bin/env python
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
import pytest
import shlex
import signal

from impala_shell_results import get_shell_cmd_result, cancellation_helper
from subprocess import Popen, PIPE
from time import sleep

SHELL_CMD = "%s/bin/impala-shell.sh" % os.environ['IMPALA_HOME']

class TestImpalaShellInteractive(object):
  """Test the impala shell interactively"""
  # TODO: Add cancellation tests
  @pytest.mark.execute_serially
  def test_escaped_quotes(self):
    """Test escaping quotes"""
    # test escaped quotes outside of quotes
    result = run_impala_shell_interactive("select \\'bc';")
    assert "could not match input" in result.stderr
    result = run_impala_shell_interactive("select \\\"bc\";")
    assert "could not match input" in result.stderr
    # test escaped quotes within quotes
    result = run_impala_shell_interactive("select 'ab\\'c';")
    assert "Fetched 1 row(s)" in result.stderr
    result = run_impala_shell_interactive("select \"ab\\\"c\";")
    assert "Fetched 1 row(s)" in result.stderr

  @pytest.mark.execute_serially
  def test_cancellation(self):
    command = "select sleep(10000);"
    p = Popen(shlex.split(SHELL_CMD), shell=True,
              stdout=PIPE, stdin=PIPE, stderr=PIPE)
    p.stdin.write(command + "\n")
    p.stdin.flush()
    sleep(1)
    # iterate through all processes with psutil
    shell_pid = cancellation_helper()
    sleep(2)
    os.kill(shell_pid, signal.SIGINT)
    result = get_shell_cmd_result(p)
    assert "Cancelling Query" in result.stderr

  @pytest.mark.execute_serially
  def test_unicode_input(self):
    "Test queries containing non-ascii input"
    # test a unicode query spanning multiple lines
    unicode_text = u'\ufffd'
    args = "select '%s'\n;" % unicode_text.encode('utf-8')
    result = run_impala_shell_interactive(args)
    assert "Fetched 1 row(s)" in result.stderr


def run_impala_shell_interactive(command, shell_args=''):
  """Runs a command in the Impala shell interactively."""
  cmd = "%s %s" % (SHELL_CMD, shell_args)
  # workaround to make Popen environment 'utf-8' compatible
  # since piping defaults to ascii
  my_env = os.environ
  my_env['PYTHONIOENCODING'] = 'utf-8'
  p = Popen(shlex.split(cmd), shell=True, stdout=PIPE,
            stdin=PIPE, stderr=PIPE, env=my_env)
  p.stdin.write(command + "\n")
  p.stdin.flush()
  return get_shell_cmd_result(p)
