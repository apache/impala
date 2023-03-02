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
#
# Utility functions related to executing shell commands.

from __future__ import absolute_import, division, print_function
import logging
import os
import shlex
from select import select
from subprocess import PIPE, Popen, STDOUT, call
from textwrap import dedent
from time import sleep, time

from tests.common.errors import Timeout

LOG = logging.getLogger('shell_util')


def dump_server_stacktraces():
  LOG.debug('Dumping stacktraces of running servers')
  call([os.path.join(os.environ['IMPALA_HOME'], "bin/dump-stacktraces.sh")])

def exec_process(cmd):
  """Executes a subprocess, waiting for completion. The process exit code, stdout and
  stderr are returned as a tuple."""
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.  The first element is the command,
  # with the rest being arguments.
  p = exec_process_async(cmd)
  stdout, stderr = p.communicate()
  rc = p.returncode
  return rc, stdout, stderr

def exec_process_async(cmd):
  """Executes a subprocess, returning immediately. The process object is returned for
  later retrieval of the exit code etc. """
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.  The first element is the command,
  # with the rest being arguments.
  return Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)

def shell(cmd, cmd_prepend="set -euo pipefail\n", stdout=PIPE, stderr=STDOUT,
    timeout_secs=None, **popen_kwargs):
  """Executes a command and returns its output. If the command's return code is non-zero
     or the command times out, an exception is raised.
  """
  cmd = dedent(cmd.strip())
  if cmd_prepend:
    cmd = cmd_prepend + cmd
  LOG.debug("Running command with %s timeout: %s" % (
    "no" if timeout_secs is None else ("%s second" % timeout_secs), cmd))
  process = Popen(cmd, shell=True, executable="/bin/bash", stdout=stdout, stderr=stderr,
      **popen_kwargs)

  stdout_fileno = process.stdout and process.stdout.fileno()
  stderr_fileno = process.stderr and process.stderr.fileno()
  remaining_fds = list()
  if stdout_fileno is not None:
    remaining_fds.append(stdout_fileno)
  if stderr_fileno is not None:
    remaining_fds.append(stderr_fileno)
  stdout = list()
  stderr = list()
  def _read_available_output():
    while True:
      available_fds, _, _ = select(remaining_fds, [], [], 0)
      if not available_fds:
        return
      for fd in available_fds:
        data = os.read(fd, 4096)
        if fd == stdout_fileno:
          if not data:
            del remaining_fds[0]
          else:
            stdout.append(data)
        elif fd == stderr_fileno:
          if not data:
            del remaining_fds[-1]
          else:
            stderr.append(data)

  deadline = time() + timeout_secs if timeout_secs is not None else None
  while True:
    # The subprocess docs indicate that stdout/err need to be drained while waiting
    # if the PIPE option is used.
    _read_available_output()
    retcode = process.poll()
    if retcode is not None or (deadline and time() > deadline):
      break
    sleep(0.1)
  _read_available_output()

  output = "".join(stdout)
  if retcode == 0:
    return output

  if not output:
    output = "(No stdout)"
  err = "".join(stderr) if stderr else "(No stderr)"
  if retcode is None:
    raise Timeout("Command timed out after %s seconds\ncmd: %s\nstdout: %s\nstderr: %s"
        % (timeout_secs, cmd, output, err))
  raise Exception(("Command returned non-zero exit code: %s"
      "\ncmd: %s\nstdout: %s\nstderr: %s") % (retcode, cmd, output, err))
