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
try:
  import fabric.decorators
  from fabric.context_managers import hide, settings
  from fabric.operations import local, run, sudo
  from fabric.tasks import execute
except ImportError as e:
  raise Exception(
      "Please run impala-pip install -r $IMPALA_HOME/infra/python/deps/extended-test-"
      "requirements.txt:\n{0}".format(str(e)))
import logging
import os
from contextlib import contextmanager
from textwrap import dedent

LOG = logging.getLogger('cluster_controller')


class ClusterController(object):
  """A convenience wrapper around fabric."""

  def __init__(self, ssh_user=os.environ.get('FABRIC_SSH_USER'),
      ssh_key_path=os.environ.get('FABRIC_SSH_KEY'), host_names=(),
      host_names_path=os.environ.get('FABRIC_HOST_FILE'), ssh_timeout_secs=60,
      ssh_port=22):
    """If no hosts are given, command execution will be done locally.

       If the FABRIC_HOST_FILE environment variable is used, it should be a path to a
       text file with a list of fully qualified host names each terminated by an EOL.
    """
    self.ssh_user = ssh_user
    self.ssh_key_path = ssh_key_path
    if host_names:
      self.hosts = host_names
    elif host_names_path:
      with open(host_names_path, 'r') as host_file:
        self.hosts = [h.strip('\n') for h in host_file.readlines()]
    self.ssh_port = ssh_port
    self.ssh_timeout_secs = ssh_timeout_secs

  @contextmanager
  def _task_settings(self, _use_deprecated_mode=False):
    settings_args = [hide("running", "stdout")]
    settings_kwargs = {
        "abort_on_prompts": True,
        "connection_attempts": 10,
        "disable_known_hosts": True,
        "keepalive": True,
        "key_filename": self.ssh_key_path,
        "parallel": True,
        "timeout": self.ssh_timeout_secs,
        "use_ssh_config": True,
        "user": self.ssh_user}
    if _use_deprecated_mode:
      settings_kwargs["warn_only"] = True
    else:
      settings_args += [hide("warnings", "stderr")]
      settings_kwargs["abort_exception"] = Exception
    with settings(*settings_args, **settings_kwargs):
      yield

  def run_cmd(self, cmd, cmd_prefix="set -euo pipefail", hosts=(),
      _use_deprecated_mode=False):
    """Runs the given command and blocks until it completes then returns a dictionary
       containing the command output keyed by host name. 'cmd_prefix' will be prepended
       to the cmd if it is set.

       If '_use_deprecated_mode' is enabled:
         1) Sudo will be used. (Deprecated because it breaks the remote/local
            transparency.)
         2) Command failures will generate warning but not raise exceptions. (Deprecated
            since runtime behavior is not reliable.)
         3) The 'cmd_prefix' argument will be ignored.
         4) Additional runtime information about command execution will be sent to stdout.
    """
    with self._task_settings(_use_deprecated_mode=_use_deprecated_mode):
      if not _use_deprecated_mode and cmd_prefix:
        cmd = cmd_prefix + "\n" + cmd
      cmd = dedent(cmd)
      cmd_hosts = hosts or self.hosts
      if cmd_hosts:
        @fabric.decorators.hosts(cmd_hosts)
        def task():
          if _use_deprecated_mode:
            return sudo(cmd)
          else:
            return run(cmd)
        return execute(task)
      else:
        return {"localhost": local(cmd)}

  def run_task(self, task, hosts=(), *task_args, **task_kwargs):
    with self._task_settings():
      cmd_hosts = hosts or self.hosts
      return execute(fabric.decorators.hosts(cmd_hosts)(task), *task_args, **task_kwargs)

  def deprecated_run_cmd(self, cmd, hosts=()):
    """No new code should use this."""
    return self.run_cmd(cmd, cmd_prefix=None, hosts=hosts, _use_deprecated_mode=True)
