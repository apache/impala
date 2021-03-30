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

import logging
import os
import pytest
import re
import shlex
import sys
import time
from subprocess import Popen, PIPE

from tests.common.environ import (IMPALA_LOCAL_BUILD_VERSION,
                                  ImpalaTestClusterProperties)
from tests.common.impala_test_suite import (IMPALAD_BEESWAX_HOST_PORT,
    IMPALAD_HS2_HOST_PORT, IMPALAD_HS2_HTTP_HOST_PORT)

logging.basicConfig()
LOG = logging.getLogger('tests/shell/util.py')
LOG.addHandler(logging.StreamHandler())

SHELL_HISTORY_FILE = os.path.expanduser("~/.impalahistory")
IMPALA_HOME = os.environ['IMPALA_HOME']

# Note that pytest.config.getoption is deprecated usage. We use this
# in a couple of other places. Ultimately, it needs to be addressed if
# we ever want to get off of pytest 2.9.2.
IMPALA_SHELL_EXECUTABLE = pytest.config.getoption('shell_executable')

if IMPALA_SHELL_EXECUTABLE is None:
  if ImpalaTestClusterProperties.get_instance().is_remote_cluster():
    # With remote cluster testing, we cannot assume that the shell was built locally.
    IMPALA_SHELL_EXECUTABLE = os.path.join(IMPALA_HOME, "bin/impala-shell.sh")
  else:
    # Test the locally built shell distribution.
    IMPALA_SHELL_EXECUTABLE = os.path.join(
        IMPALA_HOME, "shell/build", "impala-shell-" + IMPALA_LOCAL_BUILD_VERSION,
        "impala-shell")


def get_python_version_for_shell_env():
  """
  Return the version of python belonging to the tested IMPALA_SHELL_EXECUTABLE.

  We need this because some tests behave differently based on the version of
  python being used to execute the impala-shell. However, since the test
  framework itself is still being run with python2.7.x, sys.version_info
  alone can't help us to determine the python version for the environment of
  the shell executable. Instead, we have to invoke the shell, and then parse
  the python version from the output. This information is present even in the
  case of a fatal shell exception, e.g., not being unable to establish a
  connection to an impalad.

  Moreover, if the python version for the shell being executed is 3 or higher,
  and USE_THRIFT11_GEN_PY is not True, the test run should exit. Using older
  versions of thrift with python 3 will cause hard-to-triage failures because
  older thrift gen-py files are not py3 compatible.
  """
  version_check = Popen([IMPALA_SHELL_EXECUTABLE, '-q', 'version()'],
                        stdout=PIPE, stderr=PIPE)
  stdout, stderr = version_check.communicate()

  if "No module named \'ttypes\'" in stderr:
    # If USE_THRIFT11_GEN_PY is not true, and the shell executable is from a
    # python 3 (or higher) environment, this is the specific error message that
    # we'll encounter, related to python3 not allowing implicit relative imports.
    if os.getenv("USE_THRIFT11_GEN_PY") != "true":
      sys.exit("If tested shell environment has python 3 or higher, "
               "the USE_THRIFT11_GEN_PY env variable must be 'true'")

  # e.g. Starting Impala with Kerberos authentication using Python 3.7.6
  start_msg_line = stderr.split('\n')[0]
  py_version = start_msg_line.split()[-1]   # e.g. 3.7.6
  try:
    major_version, minor_version, micro_version = py_version.split('.')
    ret_val = int(major_version)
  except (ValueError, UnboundLocalError) as e:
    LOG.error(stderr)
    sys.exit("Could not determine python version in shell env: {}".format(str(e)))

  return ret_val


# Since both test_shell_commandline and test_shell_interactive import from
# this file, this check will be forced before any tests are run.
SHELL_IS_PYTHON_2 = True if (get_python_version_for_shell_env() == 2) else False


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
                         stdin_input=None, wait_until_connected=True):
  """Runs the Impala shell on the commandline.

  'shell_args' is a string which represents the commandline options.
  Returns a ImpalaShellResult.
  """
  result = run_impala_shell_cmd_no_expect(vector, shell_args, env, stdin_input,
                                          expect_success and wait_until_connected)
  if expect_success:
    assert result.rc == 0, "Cmd %s was expected to succeed: %s" % (shell_args,
                                                                   result.stderr)
  else:
    assert result.rc != 0, "Cmd %s was expected to fail" % shell_args
  return result


def run_impala_shell_cmd_no_expect(vector, shell_args, env=None, stdin_input=None,
                                   wait_until_connected=True):
  """Runs the Impala shell on the commandline.

  'shell_args' is a string which represents the commandline options.
  Returns a ImpalaShellResult.

  Does not assert based on success or failure of command.
  """
  p = ImpalaShell(vector, shell_args, env=env, wait_until_connected=wait_until_connected)
  result = p.get_result(stdin_input)
  return result


def get_impalad_host_port(vector):
  """Get host and port to connect to based on test vector provided."""
  protocol = vector.get_value("protocol")
  if protocol == 'hs2':
    return IMPALAD_HS2_HOST_PORT
  elif protocol == 'hs2-http':
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
  return [IMPALA_SHELL_EXECUTABLE,
          "--protocol={0}".format(vector.get_value("protocol")),
          "-i{0}".format(get_impalad_host_port(vector))]


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
  """A single instance of the Impala shell. The proces is started when this object is
     constructed, and then users should repeatedly call send_cmd(), followed eventually by
     get_result() to retrieve the process output. This constructor will wait until
     Impala shell is connected for the specified timeout unless wait_until_connected is
     set to False or --quiet is passed into the args."""
  def __init__(self, vector, args=None, env=None, wait_until_connected=True, timeout=60):
    self.shell_process = self._start_new_shell_process(vector, args, env=env)
    # When --quiet option is passed to Impala shell, we should not wait until we see
    # "Connected to" because it will never be printed to stderr.
    if wait_until_connected and (args is None or "--quiet" not in args):
      start_time = time.time()
      connected = False
      while time.time() - start_time < timeout and not connected:
        connected = "Connected to" in self.shell_process.stderr.readline()
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
    #   Query progress can be monitored at:...
    # We stop at 10 iterations to prevent an infinite loop if somehting goes wrong.
    iters = 0
    while "Query progress" not in self.shell_process.stderr.readline() and iters < 10:
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

  def _start_new_shell_process(self, vector, args=None, env=None):
    """Starts a shell process and returns the process handle"""
    cmd = get_shell_cmd(vector)
    if args is not None: cmd += args
    if not env: env = os.environ
    # Don't inherit PYTHONPATH - the shell launch script should set up PYTHONPATH
    # to include dependencies. Copy 'env' to avoid mutating argument or os.environ.
    env = dict(env)
    if "PYTHONPATH" in env:
      del env["PYTHONPATH"]
    return Popen(cmd, shell=False, stdout=PIPE, stdin=PIPE, stderr=PIPE,
                 env=env)
