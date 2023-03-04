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
from builtins import range
import atexit
import logging
import os
try:
  import paramiko
except ImportError as e:
  raise Exception(
      "Please run impala-pip install -r $IMPALA_HOME/infra/python/deps/extended-test-"
      "requirements.txt:\n{0}".format(str(e)))
import textwrap
import time

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

from tests.common.errors import Timeout  # noqa:E402


class SshClient(paramiko.SSHClient):
  """A paramiko SSH client modified to:

     1) Ignore host key checking.
     2) Return a popen-like object representing the execution of a remote process.
     3) Enable connection keep-alive.

     The client can execute multiple commands without the need to reconnect. This is
     important because creating connections frequently can be flaky.
  """

  def __init__(self):
    paramiko.SSHClient.__init__(self)
    self.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    self.host_name = None
    self.connect_args = None
    self.connect_kwargs = None

  def connect(self, host_name, retries=3, **kwargs):
    """Connect to the host. 'kwargs' is the same as paramiko's connect() kwargs. By
       default user name and ssh key auto-detection will be the same as 'ssh' from the
       command line, except ~/.ssh/config will not be used.
    """
    self.host_name = host_name
    if "timeout" not in kwargs:
      kwargs["timeout"] = 5 * 60   # 5 min TCP timeout
    self.connect_kwargs = kwargs

    for retry in range(retries):
      if retry:
        time.sleep(3)
      try:
        super(SshClient, self).connect(host_name, **self.connect_kwargs)
        break
      except paramiko.ssh_exception.AuthenticationException:
        raise
      except Exception as e:
        LOG.warn("Error connecting to %s" % host_name, exc_info=True)
        if retry >= retries - 1:
          LOG.error("Failed to ssh to %s" % host_name)
          raise e

    self.get_transport().set_keepalive(10)

    # Work around https://github.com/paramiko/paramiko/issues/17 -- python doesn't
    # shutdown properly if connections are open.
    atexit.register(self.close)

  def shell(self, cmd, cmd_prepend="set -euo pipefail\n", timeout_secs=None):
    """Executes a command and returns its output. If the command's return code is
       non-zero or the command times out, an exception is raised.
    """
    cmd = textwrap.dedent(cmd.strip())
    if cmd_prepend:
      cmd = cmd_prepend + cmd
    LOG.debug("Running command via ssh on %s:\n%s" % (self.host_name, cmd))
    transport = self.get_transport()
    for is_first_attempt in (True, False):
      try:
        channel = transport.open_session()
        break
      except Exception as e:
        if is_first_attempt:
          LOG.warn("Error opening ssh session: %s" % e)
          self.close()
          self.connect(self.host_name, **self.connect_kwargs)
        else:
          raise Exception("Unable to open ssh session to %s: %s" % (self.host_name, e))
    channel.set_combine_stderr(True)
    channel.exec_command(cmd)
    process = RemoteProcess(channel)

    deadline = time.time() + timeout_secs if timeout_secs is not None else None
    while True:
      retcode = process.poll()
      if retcode is not None or (deadline and time.time() > deadline):
        break
      time.sleep(0.1)

    if retcode == 0:
      return process.stdout.read().decode("utf-8").encode("ascii", errors="ignore")
    if retcode is None:
      if process.channel.recv_ready():
        output = process.channel.recv(None)
      else:
        output = ""
      if process.channel.recv_stderr_ready():
        err = process.channel.recv_stderr(None)
      else:
        err = ""
    else:
      output = process.stdout.read()
      err = process.stderr.read()
    if output:
      output = output.decode("utf-8").encode("ascii", errors="ignore")
    else:
      output = "(No stdout)"
    if err:
      err = err.decode("utf-8").encode("ascii", errors="ignore")
    else:
      err = "(No stderr)"
    if retcode is None:
      raise Timeout("Command timed out after %s seconds\ncmd: %s\nstdout: %s\nstderr: %s"
          % (timeout_secs, cmd, output, err))
    raise Exception(("Command returned non-zero exit code: %s"
        "\ncmd: %s\nstdout: %s\nstderr: %s") % (retcode, cmd, output, err))

  def __del__(self):
    self.close()


class RemoteProcess(object):

  def __init__(self, channel):
    """This constructor should not be called from outside this module. The 'channel'
       is created by the SSH client.
    """
    self.channel = channel
    self.stdout = channel.makefile("rb")
    self.stderr = channel.makefile_stderr("rb")

  def poll(self):
    """Returns the exit status of the process if the processes has completed, returns
       None otherwise.
    """
    if self.channel.exit_status_ready():
      return self.channel.recv_exit_status()

  def wait(self):
    """Wait for the process to complete."""
    while self.poll() is None:
      time.sleep(0.1)

  def communicate(self):
    self.wait()
    return self.stdout.read(), self.stderr.read()

  @property
  def returncode(self):
    return self.poll()

  def __del__(self):
    self.channel.close()
