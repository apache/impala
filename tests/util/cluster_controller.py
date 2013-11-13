#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

import logging
import os
from fabric.api import sudo, local, run, execute, parallel
from fabric.api import hide, env as fabric_env

# Setup logging for this module.
LOG = logging.getLogger('cluster_controller')
LOG.setLevel(logging.INFO)
# Set paramiko's logging level to ERROR, to supress its log spew.
logging.getLogger("paramiko").setLevel(logging.ERROR)


class ClusterController(object):
  """Responsible for running remote commands on a cluster.

  Reads three environment variables in order to connect to a remote cluster:
    * FABRIC_SSH_KEY: The full path of the user's private key.
    * FABRIC_SSH_USER: The username corresponding to the key
    * FABRIC_HOST_FILE: A text file with a list of fully qualified hostnames
                        terminated by an EOL

  If the host file does not exist, the command will be run locally.
  """
  hosts = []
  def __init__(self, *args, **kwargs):
    self.ssh_key = os.environ.get('FABRIC_SSH_KEY')
    self.user = os.environ.get('FABRIC_SSH_USER')
    self.local = True
    self.cmd = str()
    self.__get_cluster_hosts()
    if not self.local:
      self.__set_fabric_env(kwargs)
      self.__validate()

  def __validate(self):
    """Validate that commands can be issued

    TODO: Validate that the connections are successfull.
    """

    # We do not need to validate the connection for a local run
    pass

  def __set_fabric_env(self, kwargs):
    """Set fabric global environment variables"""
    fabric_env.hosts = ClusterController.hosts
    fabric_env.warn_only = kwargs.get('continue_on_error', True)
    # This sets the fabric timeout
    # TODO: Find optimal timeout value
    fabric_env.timeout = kwargs.get('timeout', 60)
    if self.user:
      fabric_env.user = self.user
    if self.ssh_key:
      fabric_env.key_filename = self.ssh_key

  def change_fabric_hosts(self, hosts):
    """Change fabric hosts

    This method can be called if an operation needs to only be run on specific machines.
    """
    fabric_env.hosts = hosts

  def reset_fabric_hosts(self):
    """Reset the fabric hosts to the ClusterController default

    This method can be called after an operation on specifc hosts. It's generally called
    after an operation requiring change_fabric_hosts"""
    fabric_env.hosts = ClusterController.hosts

  def __get_cluster_hosts(self):
    """Get the host list from the environment variable

    Hosts are specified in a text file pointed to by $HOST_FILE
    """
    try:
      host_file = open(os.environ['FABRIC_HOST_FILE'], 'r+')
      ClusterController.hosts = [h.strip('\n') for h in host_file.readlines()]
      host_file.close()
      msg = "Hosts read from FABRIC_HOST_FILE: %s" % '\n'.join(ClusterController.hosts)
      LOG.debug(msg)
      self.local = False
    except Exception as e:
      LOG.error("No host file specified, running command on localhost")
      LOG.error("Error: %s" % e)

  def run_cmd(self, cmd, serial=False):
    """Run commands locally or remotely.

    If in local mode, the command is run locally. When not local,
    the command is run with superuser privileges on remote hosts
    in parallel. The user can override running the command in parallel
    by explicity invoking run_cmd to run serially.
    The method returns a dictionary. key = hostname, value = the results of the
    command.
    TODO: Make this cleaner. Remove superuser restriction and make it an option.
    """
    self.cmd = cmd
    if self.local:
      local(self.cmd)
      return
    if serial:
      return execute(self.__run_cmd_serial)
    else:
      return execute(self.__run_cmd_parallel)

  # The 'hide' context manager allows for selective muting of fabric's logging when
  # running remote commands.
  @parallel
  def __run_cmd_parallel(self):
    with hide('stdout', 'running'):
      return sudo(self.cmd, combine_stderr=True)

  def __run_cmd_serial(self):
    with hide('stdout', 'running'):
      return sudo(self.cmd, combine_stderr=True)
