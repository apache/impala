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
#
# Basic object model of an Impala cluster (set of Impala processes).
#
import json
import logging
import os
import psutil
import socket
import sys
import urllib

from collections import defaultdict
from HTMLParser import HTMLParser
from random import choice, shuffle
from tests.common.impala_service import *
from tests.util.shell_util import exec_process_async, exec_process
from time import sleep, time

logging.basicConfig(level=logging.ERROR, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_cluster')
LOG.setLevel(level=logging.DEBUG)

# Represents a set of Impala processes. Each Impala process must be created with
# a basic set of command line options (beeswax_port, webserver_port, etc)
class ImpalaCluster(object):
  def __init__(self):
    self.__impalads, self.__statestoreds, self.__catalogd =\
        self.__build_impala_process_lists()
    LOG.info("Found %d impalad/%d statestored/%d catalogd process(es)" %\
        (len(self.__impalads), len(self.__statestoreds), 1 if self.__catalogd else 0))

  def refresh(self):
    """ Re-loads the impalad/statestored/catalogd processes if they exist.

    Helpful to confirm that processes have been killed.
    """
    self.__impalads, self.__statestoreds, self.__catalogd =\
        self.__build_impala_process_lists()

  @property
  def statestored(self):
    """
    Returns the statestore process

    Note: Currently we expectly a single statestore process, in the future this might
    change in which case this should return the "active" statestore.
    """
    # If no statestored process exists, return an empty list.
    return self.__statestoreds[0] if len(self.__statestoreds) > 0 else list()

  @property
  def impalads(self):
    """Returns a list of the known impalad processes"""
    return self.__impalads

  @property
  def catalogd(self):
    """Returns the catalogd process, or None if no catalogd process was found"""
    return self.__catalogd

  def get_first_impalad(self):
    return self.impalads[0]

  def get_any_impalad(self):
    """Selects a random impalad from the list of known processes"""
    return choice(self.impalads)

  def get_different_impalad(self, other_impalad):
    """Selects an impalad that is different from the given impalad"""
    if len(self.impalads) <= 1:
      assert 0, "Only %d impalads available to choose from" % len(self.impalads)
    LOG.info("other_impalad: " + str(other_impalad))
    LOG.info("Cluster: " + str(len(self.impalads)))
    LOG.info("Cluster: " + str(self.impalads))
    return choice([impalad for impalad in self.impalads if impalad != other_impalad])

  def __build_impala_process_lists(self):
    """
    Gets all the running Impala procs (with start arguments) on the machine.

    Note: This currently only works for the local case. To support running in a cluster
    environment this would need to enumerate each machine in the cluster.
    """
    impalads = list()
    statestored = list()
    catalogd = None
    # TODO: Consider using process_iter() here
    for pid in psutil.get_pid_list():
      try:
        process = psutil.Process(pid)
      except psutil.NoSuchProcess, e:
        # A process from get_pid_list() no longer exists, continue.
        LOG.info(e)
        continue
      if process.name == 'impalad' and len(process.cmdline) >= 1:
        impalads.append(ImpaladProcess(process.cmdline))
      elif process.name == 'statestored' and len(process.cmdline) >= 1:
        statestored.append(StateStoreProcess(process.cmdline))
      elif process.name == 'catalogd' and len(process.cmdline) >=1:
        catalogd = CatalogdProcess(process.cmdline)
    return impalads, statestored, catalogd

# Represents a process running on a machine and common actions that can be performed
# on a process such as restarting or killing.
class Process(object):
  def __init__(self, cmd):
    self.cmd = cmd
    assert cmd is not None and len(cmd) >= 1,\
        'Process object must be created with valid command line argument list'

  def get_pid(self):
    """Gets the pid of the process. Returns None if the PID cannot be determined"""
    LOG.info("Attempting to find PID for %s" % ' '.join(self.cmd))
    for pid in psutil.get_pid_list():
      try:
        process = psutil.Process(pid)
        if set(self.cmd) == set(process.cmdline):
          return pid
      except psutil.NoSuchProcess, e:
        # A process from get_pid_list() no longer exists, continue.
        LOG.info(e)
    LOG.info("No PID found for process cmdline: %s. Process is dead?" % self.cmd)
    return None

  def start(self):
    LOG.info("Starting process: %s" % ' '.join(self.cmd))
    self.process = exec_process_async(' '.join(self.cmd))

  def wait(self):
    """Wait until the current process has exited, and returns
    (return code, stdout, stderr)"""
    LOG.info("Waiting for process: %s" % ' '.join(self.cmd))
    stdout, stderr = self.process.communicate()
    return self.process.returncode, stdout, stderr

  def kill(self):
    """
    Kills the given processes.

    Returns the PID that was killed or None of no PID was found (process not running)
    """
    pid = self.get_pid()
    if pid is None:
      assert 0, "No processes %s found" % self.cmd
    LOG.info('Killing: %s (PID: %d)'  % (' '.join(self.cmd), pid))
    exec_process("kill -9 %d" % pid)
    return pid

  def restart(self):
    """Kills and restarts the process"""
    self.kill()
    # Wait for a bit so the ports will be released.
    sleep(1)
    self.start()

  def __str__(self):
    return "Command: %s PID: %s" % (self.cmd, self.get_pid())


# Base class for all Impala processes
class BaseImpalaProcess(Process):
  def __init__(self, cmd, hostname):
    super(BaseImpalaProcess, self).__init__(cmd)
    self.hostname = hostname

  def _get_webserver_port(self, default=None):
    return int(self._get_arg_value('webserver_port', default))

  def _get_arg_value(self, arg_name, default=None):
    """Gets the argument value for given argument name"""
    for arg in self.cmd:
      if ('%s=' % arg_name) in arg.strip().lstrip('-'):
        return arg.split('=')[1]
    if default is None:
      assert 0, "No command line argument '%s' found." % arg_name
    return default


# Represents an impalad process
class ImpaladProcess(BaseImpalaProcess):
  def __init__(self, cmd):
    super(ImpaladProcess, self).__init__(cmd, socket.gethostname())
    self.service = ImpaladService(self.hostname,
        self._get_webserver_port(default=25000), self.__get_beeswax_port(default=21000),
        self.__get_be_port(default=22000))

  def __get_beeswax_port(self, default=None):
    return int(self._get_arg_value('beeswax_port', default))

  def __get_be_port(self, default=None):
    return int(self._get_arg_value('be_port', default))

  def start(self, wait_until_ready=True):
    """Starts the impalad and waits until the service is ready to accept connections."""
    super(ImpaladProcess, self).start()
    self.service.wait_for_metric_value('impala-server.ready',
        expected_value=1, timeout=30)


# Represents a statestored process
class StateStoreProcess(BaseImpalaProcess):
  def __init__(self, cmd):
    super(StateStoreProcess, self).__init__(cmd, socket.gethostname())
    self.service =\
        StateStoredService(self.hostname, self._get_webserver_port(default=25010))


# Represents a catalogd process
class CatalogdProcess(BaseImpalaProcess):
  def __init__(self, cmd):
    super(CatalogdProcess, self).__init__(cmd, socket.gethostname())
    self.service = CatalogdService(self.hostname,
        self._get_webserver_port(default=25020), self.__get_port(default=26000))

  def __get_port(self, default=None):
    return int(self._get_arg_value('catalog_service_port', default))
