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

import shlex
import os
from subprocess import Popen, PIPE
import pytest
from impala_shell_results import get_shell_cmd_result

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
    assert "Returned 1 row(s)" in result.stderr
    result = run_impala_shell_interactive("select \"ab\\\"c\";")
    assert "Returned 1 row(s)" in result.stderr

def run_impala_shell_interactive(command, shell_args=''):
  """Runs a command in the Impala shell interactively."""
  cmd = "%s %s" % (SHELL_CMD, shell_args)
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stdin=PIPE, stderr=PIPE)
  p.stdin.write(command + "\n")
  p.stdin.flush()
  return get_shell_cmd_result(p)
