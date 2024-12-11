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
import logging
import os
import socket
from contextlib import closing

import pexpect
import pytest
import re
import shlex
import sys
import time
from subprocess import Popen, PIPE

# This import is the actual ImpalaShell class from impala_shell.py.
# We rename it to ImpalaShellClass here because we later import another
# class called ImpalaShell from tests/shell/util.py, and we don't want
# to mask it.
from shell.impala_shell import ImpalaShell as ImpalaShellClass

from tests.common.environ import (IMPALA_LOCAL_BUILD_VERSION,
                                  ImpalaTestClusterProperties)
from tests.common.impala_service import ImpaladService
from tests.common.impala_test_suite import (IMPALAD_BEESWAX_HOST_PORT,
    IMPALAD_HS2_HOST_PORT, IMPALAD_HS2_HTTP_HOST_PORT,
    STRICT_HS2_HOST_PORT, STRICT_HS2_HTTP_HOST_PORT)
from tests.common.test_vector import ImpalaTestDimension

LOG = logging.getLogger('tests/shell/util.py')
LOG.addHandler(logging.StreamHandler())

SHELL_HISTORY_FILE = os.path.expanduser("~/.impalahistory")
IMPALA_HOME = os.environ['IMPALA_HOME']

def build_shell_env(env=None):
  """ Construct the environment for the shell to run in based on 'env', or the current
  process's environment if env is None."""
  if not env: env = os.environ
  # Don't inherit PYTHONPATH or LD_LIBRARY_PATH - the shell launch script must set
  # these to include dependencies. Copy 'env' to avoid mutating argument or os.environ.
  env = dict(env)
  if "PYTHONPATH" in env:
    del env["PYTHONPATH"]
  if "LD_LIBRARY_PATH" in env:
    del env["LD_LIBRARY_PATH"]
  return env


def assert_var_substitution(result):
  assert_pattern(r'\bfoo_number=.*$', 'foo_number= 123123', result.stdout, \
    'Numeric values not replaced correctly')
  assert_pattern(r'\bfoo_string=.*$', 'foo_string=123', result.stdout, \
    'String values not replaced correctly')
  assert_pattern(r'\bVariables:[\s\n]*BAR:\s*[0-9]*\n\s*FOO:\s*[0-9]*', \
    'Variables:\n\tBAR: 456\n\tFOO: 123', result.stdout, \
    "Set variable not listed correctly by the first SET command")
  assert_pattern(r'\bError: Unknown variable FOO1$', \
    'Error: Unknown variable FOO1', result.stderr, \
    'Missing variable FOO1 not reported correctly')
  assert_pattern(r'\bmulti_test=.*$', 'multi_test=456_123_456_123', \
    result.stdout, 'Multiple replaces not working correctly')
  assert_pattern(r'\bError:\s*Unknown\s*substitution\s*syntax\s*' +
                 r'\(RANDOM_NAME\). Use \${VAR:var_name}', \
    'Error: Unknown substitution syntax (RANDOM_NAME). Use ${VAR:var_name}', \
    result.stderr, "Invalid variable reference")
  assert_pattern(r'"This should be not replaced: \${VAR:foo} \${HIVEVAR:bar}"',
    '"This should be not replaced: ${VAR:foo} ${HIVEVAR:bar}"', \
    result.stdout, "Variable escaping not working")
  assert_pattern(r'\bVariable MYVAR set to.*$', 'Variable MYVAR set to foo123',
    result.stderr, 'No evidence of MYVAR variable being set.')
  assert_pattern(r'\bVariables:[\s\n]*BAR:.*[\s\n]*FOO:.*[\s\n]*MYVAR:.*$',
    'Variables:\n\tBAR: 456\n\tFOO: 123\n\tMYVAR: foo123', result.stdout,
    'Set variables not listed correctly by the second SET command')
  assert_pattern(r'\bUnsetting variable FOO$', 'Unsetting variable FOO',
    result.stdout, 'No evidence of variable FOO being unset')
  assert_pattern(r'\bUnsetting variable BAR$', 'Unsetting variable BAR',
    result.stdout, 'No evidence of variable BAR being unset')
  assert_pattern(r'\bVariables:[\s\n]*No variables defined\.$', \
    'Variables:\n\tNo variables defined.', result.stdout, \
    'Unset variables incorrectly listed by third SET command.')
  assert_pattern(r'\bNo variable called NONEXISTENT is set', \
    'No variable called NONEXISTENT is set', result.stdout, \
    'Problem unsetting non-existent variable.')
  assert_pattern(r'\bVariable COMMENT_TYPE1 set to.*$',
    'Variable COMMENT_TYPE1 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE1 variable being set.')
  assert_pattern(r'\bVariable COMMENT_TYPE2 set to.*$',
    'Variable COMMENT_TYPE2 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE2 variable being set.')
  assert_pattern(r'\bVariable COMMENT_TYPE3 set to.*$',
    'Variable COMMENT_TYPE3 set to ok', result.stderr,
    'No evidence of COMMENT_TYPE3 variable being set.')
  assert_pattern(r'\bVariables:[\s\n]*COMMENT_TYPE1:.*[\s\n]*' + \
    'COMMENT_TYPE2:.*[\s\n]*COMMENT_TYPE3:.*$',
    'Variables:\n\tCOMMENT_TYPE1: ok\n\tCOMMENT_TYPE2: ok\n\tCOMMENT_TYPE3: ok', \
    result.stdout, 'Set variables not listed correctly by the SET command')

def assert_pattern(pattern, result, text, message):
  """Asserts that the pattern, when applied to text, returns the expected result"""
  m = re.search(pattern, text, re.MULTILINE)
  assert m and m.group(0) == result, message


def run_impala_shell_cmd(vector, shell_args, env=None, expect_success=True,
                         stdin_input=None, wait_until_connected=True,
                         stdout_file=None, stderr_file=None):
  """Runs the Impala shell on the commandline.

  'shell_args' is a string which represents the commandline options.
  Returns a ImpalaShellResult.
  """
  result = run_impala_shell_cmd_no_expect(vector, shell_args, env, stdin_input,
                                          expect_success and wait_until_connected,
                                          stdout_file=stdout_file,
                                          stderr_file=stderr_file)
  if expect_success:
    assert result.rc == 0, "Cmd %s was expected to succeed: %s" % (shell_args,
                                                                   result.stderr)
  else:
    assert result.rc != 0, "Cmd %s was expected to fail" % shell_args
  return result


def run_impala_shell_cmd_no_expect(vector, shell_args, env=None, stdin_input=None,
                                   wait_until_connected=True, stdout_file=None,
                                   stderr_file=None):
  """Runs the Impala shell on the commandline.

  'shell_args' is a string which represents the commandline options.
  Returns a ImpalaShellResult.

  Does not assert based on success or failure of command.
  """
  p = ImpalaShell(vector, shell_args, env=env, wait_until_connected=wait_until_connected,
                  stdout_file=stdout_file, stderr_file=stderr_file)
  result = p.get_result(stdin_input)
  return result


def get_impalad_host_port(vector):
  """Get host and port to connect to based on test vector provided."""
  protocol = vector.get_value("protocol")
  strict = vector.get_value_with_default("strict_hs2_protocol", False)
  if protocol == 'hs2':
    if strict:
      return STRICT_HS2_HOST_PORT
    else:
      return IMPALAD_HS2_HOST_PORT
  elif protocol == 'hs2-http':
    if strict:
      return STRICT_HS2_HTTP_HOST_PORT
    else:
      return IMPALAD_HS2_HTTP_HOST_PORT
  else:
    assert protocol == 'beeswax', protocol
    return IMPALAD_BEESWAX_HOST_PORT


def get_impalad_port(vector):
  """Get integer port to connect to based on test vector provided."""
  return int(get_impalad_host_port(vector).split(":")[1])


def get_shell_cmd(vector):
  """Get the basic shell command to start the shell, given the provided test vector.
  Returns the command as a list of string arguments."""
  impala_shell_executable = get_impala_shell_executable(vector)
  if vector.get_value_with_default("strict_hs2_protocol", False):
    protocol = vector.get_value("protocol")
    return impala_shell_executable + [
            "--protocol={0}".format(protocol),
            "--strict_hs2_protocol",
            "--use_ldap_test_password",
            "-i{0}".format(get_impalad_host_port(vector))]
  else:
    return impala_shell_executable + [
            "--protocol={0}".format(vector.get_value("protocol")),
            "-i{0}".format(get_impalad_host_port(vector))]


def spawn_shell(shell_cmd):
  """Spawn a shell process with the provided command line. Returns the Pexpect object."""
  return pexpect.spawn(shell_cmd[0], shell_cmd[1:], env=build_shell_env())


def get_open_sessions_metric(vector):
  """Get the name of the vector that tracks open sessions for the protocol in vector."""
  protocol = vector.get_value("protocol")
  if protocol in ('hs2', 'hs2-http'):
    return 'impala-server.num-open-hiveserver2-sessions'
  else:
    assert protocol == 'beeswax', protocol
    return 'impala-server.num-open-beeswax-sessions'

class ImpalaShellResult(object):
  def __init__(self):
    self.rc = 0
    self.stdout = str()
    self.stderr = str()


class ImpalaShell(object):
  """A single instance of the Impala shell. The process is started when this object is
     constructed, and then users should repeatedly call send_cmd(), followed eventually by
     get_result() to retrieve the process output. This constructor will wait until
     Impala shell is connected for the specified timeout unless wait_until_connected is
     set to False or --quiet is passed into the args."""
  def __init__(self, vector, args=None, env=None, wait_until_connected=True, timeout=60,
               stdout_file=None, stderr_file=None):
    self.shell_process = self._start_new_shell_process(vector, args, env=env,
                                                       stdout_file=stdout_file,
                                                       stderr_file=stderr_file)
    # When --quiet option is passed to Impala shell, we should not wait until we see
    # "Connected to" because it will never be printed to stderr. The same is true
    # if stderr is redirected.
    if wait_until_connected and (args is None or "--quiet" not in args) and \
       stderr_file is None:
      # We don't want to hang waiting for input. So, here are the scenarios
      # we need to handle:
      # 1. Shell process exits
      # 2. Shell fails to connect. This can lead to an interactive prompt
      #    that blocks for input forever. The two messages to look for are
      #    "Error connecting" and "Socket error"
      # 3. Process successfully connecting: "Connected to"
      # Cases 1 and 2 should lead to an assert.
      start_time = time.time()
      process_status = None
      connection_err = None
      connected = False
      while time.time() - start_time < timeout:
        # Condition 1: check if the shell process has exited
        # poll() returns None until the process exits
        process_status = self.shell_process.poll()
        if process_status is not None:
          break
        # readline() can block forever, so the timeout logic may not be effective
        # if something gets stuck here.
        line = self.shell_process.stderr.readline()
        # Condition 2: check for errors connecting
        if ImpalaShellClass.ERROR_CONNECTING_MESSAGE in line or \
           ImpalaShellClass.SOCKET_ERROR_MESSAGE in line:
          connection_err = line
          break

        # Condition 3: check if the connection is successful
        connected = ImpalaShellClass.CONNECTED_TO_MESSAGE in line
        if connected:
          break

      assert process_status is None, \
          "Impala shell exited with return code {0}".format(process_status)
      assert connection_err is None, connection_err
      assert connected, "Impala shell is not connected"

  def pid(self):
    return self.shell_process.pid

  def send_cmd(self, cmd):
    """Send a single command to the shell. This method adds the end-of-query
       terminator (';'). """
    self.shell_process.stdin.write("%s;\n" % cmd)
    self.shell_process.stdin.flush()
    # Allow fluent-style chaining of commands
    return self

  def wait_for_query_start(self):
    """If this shell was started with the '-q' option, this mathod will block until the
    query has started running"""
    # readline() will block until a line is actually printed, so this loop should always
    # read something like:
    #   Starting Impala Shell without Kerberos authentication
    #   Connected to localhost:21000
    #   Server version: impalad version...
    #   Query: select sleep(10)
    #   Query submitted at:...
    #   Query state can be monitored at:...
    # We stop at 10 iterations to prevent an infinite loop if somehting goes wrong.
    iters = 0
    while "Query state" not in self.shell_process.stderr.readline() and iters < 10:
      iters += 1

  def get_result(self, stdin_input=None):
    """Returns an ImpalaShellResult produced by the shell process on exit. After this
       method returns, send_cmd() no longer has any effect."""
    result = ImpalaShellResult()
    result.stdout, result.stderr = self.shell_process.communicate(input=stdin_input)
    # We need to close STDIN if we gave it an input, in order to send an EOF that will
    # allow the subprocess to exit.
    if stdin_input is not None: self.shell_process.stdin.close()
    result.rc = self.shell_process.returncode
    return result

  def _start_new_shell_process(self, vector, args=None, env=None, stdout_file=None,
                               stderr_file=None):
    """Starts a shell process and returns the process handle"""
    cmd = get_shell_cmd(vector)
    if args is not None: cmd += args
    stdout_arg = stdout_file if stdout_file is not None else PIPE
    stderr_arg = stderr_file if stderr_file is not None else PIPE
    return Popen(cmd, shell=False, stdout=stdout_arg, stdin=PIPE, stderr=stderr_arg,
                 env=build_shell_env(env))


def get_unused_port():
  """ Find an unused port http://stackoverflow.com/questions/1365265 """
  with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
    s.bind(('', 0))
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    return s.getsockname()[1]


def wait_for_query_state(vector, stmt, state, max_retry=15):
  """Waits for the given query 'stmt' to reach the given query 'state'. The state of the
  query is taken from the debug web ui. Polls the state of 'stmt' every second until the
  query gets to a state given via 'state' or a maximum retry count is reached.
  Restriction: Only works if there is only one in flight query."""
  impalad_service = ImpaladService(get_impalad_host_port(vector).split(':')[0])
  if not impalad_service.wait_for_num_in_flight_queries(1):
    raise Exception("No in flight query found")

  retry_count = 0
  while retry_count <= max_retry:
    query_info = impalad_service.get_in_flight_queries()[0]
    print(str(query_info))
    if query_info['stmt'] != stmt:
      exc_text = "The found in flight query is not the one under test: " + \
          query_info['stmt']
      raise Exception(exc_text)
    if query_info['state'] == state:
      return
    retry_count += 1
    time.sleep(1.0)
  raise Exception("Query didn't reach desired state: " + state)


# Returns shell executable, and whether to include pypi variants
def get_dev_impala_shell_executable():
  # Note that pytest.config.getoption is deprecated usage. We use this
  # in a couple of other places. Ultimately, it needs to be addressed if
  # we ever want to get off of pytest 2.9.2.
  impala_shell_executable = pytest.config.getoption('shell_executable')

  if impala_shell_executable is not None:
    return impala_shell_executable, False

  if ImpalaTestClusterProperties.get_instance().is_remote_cluster():
    # With remote cluster testing, we cannot assume that the shell was built locally.
    return os.path.join(IMPALA_HOME, "bin/impala-shell.sh"), False
  else:
    # Test the locally built shell distribution.
    return os.path.join(IMPALA_HOME, "shell/build",
        "impala-shell-" + IMPALA_LOCAL_BUILD_VERSION, "impala-shell"), True

# TODO: ambari-python-wrap testing
def create_impala_shell_executable_dimension(dev_only=False):
  _, include_pypi = get_dev_impala_shell_executable()
  dimensions = []
  if os.getenv("IMPALA_SYSTEM_PYTHON2"):
    dimensions.append('dev')
  if os.getenv("IMPALA_SYSTEM_PYTHON3"):
    dimensions.append('dev3')
  if include_pypi and not dev_only:
    if os.getenv("IMPALA_SYSTEM_PYTHON2"):
      dimensions.append('python2')
    if os.getenv("IMPALA_SYSTEM_PYTHON3"):
      dimensions.append('python3')
  return ImpalaTestDimension('impala_shell', *dimensions)


def get_impala_shell_executable(vector):
  # impala-shell is invoked some places where adding a test vector may not make sense;
  # use 'dev' as the default.
  impala_shell_executable, _ = get_dev_impala_shell_executable()
  return {
    'dev': [impala_shell_executable],
    'dev3': ['env', 'IMPALA_PYTHON_EXECUTABLE=python3', impala_shell_executable],
    'python2': [os.path.join(IMPALA_HOME, 'shell/build/python2_venv/bin/impala-shell')],
    'python3': [os.path.join(IMPALA_HOME, 'shell/build/python3_venv/bin/impala-shell')]
  }[vector.get_value_with_default('impala_shell', 'dev')]
