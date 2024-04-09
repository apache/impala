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
# Basic object model of an Impala cluster (set of Impala processes).

from __future__ import absolute_import, division, print_function
from builtins import map, range
import json
import logging
import os
import pipes
import psutil
import socket
import time
import requests
from getpass import getuser
from random import choice
from signal import SIGKILL
from subprocess import check_call, check_output
from time import sleep

import tests.common.environ
from tests.common.impala_service import (
    AdmissiondService,
    CatalogdService,
    ImpaladService,
    StateStoredService)
from tests.util.shell_util import exec_process

LOG = logging.getLogger('impala_cluster')
LOG.setLevel(level=logging.DEBUG)

IMPALA_HOME = os.environ['IMPALA_HOME']
START_DAEMON_PATH = os.path.join(IMPALA_HOME, 'bin/start-daemon.sh')

DEFAULT_BEESWAX_PORT = 21000
DEFAULT_HS2_PORT = 21050
DEFAULT_EXTERNAL_FE_PORT = 21150
DEFAULT_HS2_HTTP_PORT = 28000
DEFAULT_KRPC_PORT = 27000
DEFAULT_CATALOG_SERVICE_PORT = 26000
DEFAULT_STATE_STORE_SUBSCRIBER_PORT = 23000
DEFAULT_CATALOGD_STATE_STORE_SUBSCRIBER_PORT = 23020
DEFAULT_STATESTORE_SERVICE_PORT = 24000
DEFAULT_STATESTORE_HA_SERVICE_PORT = 24020
DEFAULT_PEER_STATESTORE_HA_SERVICE_PORT = 24021
DEFAULT_IMPALAD_WEBSERVER_PORT = 25000
DEFAULT_STATESTORED_WEBSERVER_PORT = 25010
DEFAULT_CATALOGD_WEBSERVER_PORT = 25020
DEFAULT_ADMISSIOND_WEBSERVER_PORT = 25030

DEFAULT_IMPALAD_JVM_DEBUG_PORT = 30000
DEFAULT_CATALOGD_JVM_DEBUG_PORT = 30030

# Timeout to use when waiting for a cluster to start up. Set quite high to avoid test
# flakiness.
CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 240

# Url format used to set JVM log level.
SET_JAVA_LOGLEVEL_URL = "http://{0}:{1}/set_java_loglevel"


def post_data(url, data):
  """Helper method to post data to a url."""
  response = requests.head(url)
  assert response.status_code == requests.codes.ok, "URL: {0} Resp:{1}".format(
    url, response.text)
  response = requests.post(url, data=data)
  assert response.status_code == requests.codes.ok, "URL: {0} Resp:{1}".format(
    url, response.text)
  return response


# Represents a set of Impala processes.
# Handles two cases:
# * The traditional minicluster with many processes running as the current user on
#   the local system. In this case various settings are detected based on command
#   line options(beeswax_port, webserver_port, etc)
# * The docker minicluster with one container per process connected to a user-defined
#   bridge network.
class ImpalaCluster(object):
  def __init__(self, docker_network=None, use_admission_service=False):
    self.docker_network = docker_network
    self.use_admission_service = use_admission_service
    self.refresh()

  @classmethod
  def get_e2e_test_cluster(cls):
    """Within end-to-end tests, get the cluster under test with settings detected from
    the environment."""
    return ImpalaCluster(docker_network=tests.common.environ.docker_network)

  def refresh(self):
    """ Re-loads the impalad/statestored/catalogd processes if they exist.

    Helpful to confirm that processes have been killed.
    """
    if self.docker_network is None:
      self.__impalads, self.__statestoreds, self.__catalogds, self.__admissiond =\
          self.__build_impala_process_lists()
    else:
      self.__impalads, self.__statestoreds, self.__catalogds, self.__admissiond =\
          self.__find_docker_containers()
    admissiond_str = ""
    if self.use_admission_service:
      admissiond_str = "/%d admissiond" % (1 if self.__admissiond else 0)

    LOG.debug("Found %d impalad/%d statestored/%d catalogd%s process(es)" %
        (len(self.__impalads), len(self.__statestoreds), len(self.__catalogds),
         admissiond_str))

  @property
  def statestored(self):
    """
    Returns the statestore process

    Note: Currently we expectly a single statestore process, in the future this might
    change in which case this should return the "active" statestore.
    """
    # If no statestored process exists, return None.
    return self.__statestoreds[0] if len(self.__statestoreds) > 0 else None

  def statestoreds(self):
    """Returns a list of the known statestored processes"""
    return self.__statestoreds

  def get_first_statestored(self):
    return self.statestoreds[0]

  @property
  def impalads(self):
    """Returns a list of the known impalad processes"""
    return self.__impalads

  @property
  def catalogd(self):
    # If no catalogd process exists, return None. Otherwise, return first catalogd
    return self.__catalogds[0] if len(self.__catalogds) > 0 else None

  def catalogds(self):
    """Returns a list of the known catalogd processes"""
    return self.__catalogds

  def get_first_catalogd(self):
    return self.catalogds[0]

  @property
  def admissiond(self):
    """Returns the admisisond process, or None if no admissiond process was found"""
    return self.__admissiond

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

  def num_responsive_coordinators(self):
    """Find the number of impalad coordinators that can evaluate a test query."""
    n = 0
    for impalad in self.impalads:
      try:
        client = impalad.service.create_beeswax_client()
        result = client.execute("select 1")
        assert result.success
        ++n
      except Exception as e: print(e)
      finally:
        client.close()
    return n

  def wait_until_ready(self, expected_num_impalads=1, expected_num_ready_impalads=None):
    """Waits for this 'cluster' to be ready to submit queries.

      A cluster is deemed "ready" if:
        - expected_num_impalads impala processes are up (or, if not specified, at least
          one impalad is up).
        - expected_num_ready_impalads backends are registered with the statestore.
          expected_num_ready_impalads defaults to expected_num_impalads.
        - All impalads knows about all other ready impalads.
        - Each coordinator impalad's catalog cache is ready.
      This information is retrieved by querying the statestore debug webpage
      and each individual impalad's metrics webpage.
    """
    self.wait_for_num_impalads(expected_num_impalads)

    # TODO: fix this for coordinator-only nodes as well.
    if expected_num_ready_impalads is None:
      expected_num_ready_impalads = len(self.impalads)

    def check_processes_still_running():
      """Check that the processes we waited for above (i.e. impalads, statestored,
      catalogd) are still running. Throw an exception otherwise."""
      self.refresh()
      # The number of impalad processes may temporarily increase if breakpad forked a
      # process to write a minidump.
      assert len(self.impalads) >= expected_num_impalads
      assert self.statestored is not None
      assert self.catalogd is not None

    for impalad in self.impalads:
      impalad.service.wait_for_num_known_live_backends(expected_num_ready_impalads,
          timeout=CLUSTER_WAIT_TIMEOUT_IN_SECONDS, interval=2,
          early_abort_fn=check_processes_still_running)
      if (impalad._get_arg_value("is_coordinator", default="true") == "true"
         and impalad._get_arg_value("stress_catalog_init_delay_ms", default=0) == 0):
        impalad.wait_for_catalog()

  def wait_for_num_impalads(self, num_impalads, retries=10):
    """Checks that at least 'num_impalads' impalad processes are running, along with
    the statestored and catalogd.

    Refresh until the number running impalad processes reaches the expected
    number based on num_impalads, or the retry limit is hit. Failing this, raise a
    RuntimeError.
    """
    for i in range(retries):
      if len(self.impalads) < num_impalads or not self.statestored or not self.catalogd:
        sleep(1)
        self.refresh()
    msg = ""
    if len(self.impalads) < num_impalads:
      msg += "Expected {expected_num} impalad(s), only {actual_num} found\n".format(
          expected_num=num_impalads, actual_num=len(self.impalads))
    if not self.statestored:
      msg += "statestored failed to start.\n"
    if not self.catalogd:
      msg += "catalogd failed to start.\n"
    if msg:
      raise RuntimeError(msg)

  def __build_impala_process_lists(self):
    """
    Gets all the running Impala procs (with start arguments) on the machine.

    Note: This currently only works for the local case. To support running in a cluster
    environment this would need to enumerate each machine in the cluster.
    """
    impalads = list()
    statestoreds = list()
    catalogds = list()
    admissiond = None
    daemons = ['impalad', 'catalogd', 'statestored', 'admissiond']
    for binary, process in find_user_processes(daemons):
      # IMPALA-6889: When a process shuts down and becomes a zombie its cmdline becomes
      # empty for a brief moment, before it gets reaped by its parent (see man proc). We
      # copy the cmdline to prevent it from changing between the following checks and
      # the construction of the *Process objects.
      cmdline = ''
      try:
        cmdline = process.cmdline()
      except psutil.NoSuchProcess:
        # IMPALA-8320: psutil.Process.cmdline is a property and the process could have
        # disappeared between the time we built the process list and now.
        continue
      if len(cmdline) == 0:
        continue
      if binary == 'impalad':
        impalads.append(ImpaladProcess(cmdline))
      elif binary == 'statestored':
        statestoreds.append(StateStoreProcess(cmdline))
      elif binary == 'catalogd':
        catalogds.append(CatalogdProcess(cmdline))
      elif binary == 'admissiond':
        admissiond = AdmissiondProcess(cmdline)

    self.__sort_impalads(impalads)
    self.__sort_statestoreds(statestoreds)
    self.__sort_catalogds(catalogds)
    return impalads, statestoreds, catalogds, admissiond

  def __find_docker_containers(self):
    """
    Gets all the running Impala containers on self.docker_network.
    """
    impalads = []
    statestoreds = []
    catalogds = []
    admissiond = None
    output = check_output(["docker", "network", "inspect", self.docker_network],
                          universal_newlines=True)
    # Only one network should be present in the top level array.
    for container_id in json.loads(output)[0]["Containers"]:
      container_info = get_container_info(container_id)
      if container_info["State"]["Status"] != "running":
        # Skip over stopped containers.
        continue
      args = container_info["Args"]
      executable = os.path.basename(args[0])
      port_map = {}
      for k, v in container_info["NetworkSettings"]["Ports"].items():
        # Key looks like "25000/tcp"..
        port = int(k.split("/")[0])
        # Value looks like { "HostPort": "25002", "HostIp": "" }.
        host_port = int(v[0]["HostPort"])
        port_map[port] = host_port
      if executable == 'impalad':
        impalads.append(ImpaladProcess(args, container_id=container_id,
                                       port_map=port_map))
      elif executable == 'statestored':
        statestoreds.append(StateStoreProcess(args, container_id=container_id,
                                              port_map=port_map))
      elif executable == 'catalogd':
        catalogds.append(CatalogdProcess(args, container_id=container_id,
                                         port_map=port_map))
      elif executable == 'admissiond':
        assert admissiond is None
        admissiond = AdmissiondProcess(args, container_id=container_id,
                                       port_map=port_map)
    self.__sort_impalads(impalads)
    self.__sort_statestoreds(statestoreds)
    self.__sort_catalogds(catalogds)
    return impalads, statestoreds, catalogds, admissiond

  def __sort_impalads(self, impalads):
    """Does an in-place sort of a list of ImpaladProcess objects into a canonical order.
    We order them by their HS2 port, so that get_first_impalad() always returns the
    first one. We need to use a port that is exposed and mapped to a host port for
    the containerised cluster."""
    impalads.sort(key=lambda i: i.service.hs2_port)

  def __sort_statestoreds(self, statestoreds):
    """Does an in-place sort of a list of StateStoredService objects into a canonical
    order. We order them by their service port, so that get_first_statestored() always
    returns the first one. We need to use a port that is exposed and mapped to a host
    port for the containerised cluster."""
    statestoreds.sort(key=lambda i: i.service.service_port)

  def __sort_catalogds(self, catalogds):
    """Does an in-place sort of a list of CatalogdProcess objects into a canonical order.
    We order them by their service port, so that get_first_catalogd() always returns the
    first one. We need to use a port that is exposed and mapped to a host port for
    the containerised cluster."""
    catalogds.sort(key=lambda i: i.service.service_port)


# Represents a process running on a machine and common actions that can be performed
# on a process such as restarting or killing. The process may be the main process in
# a Docker container, if the cluster is containerised (in this case container_id must
# be provided). Note that containerised processes are really just processes running
# on the local system with some additional virtualisation, so some operations are
# the same for both containerised and non-containerised cases.
#
# For containerised processes, 'port_map' should be provided to map from the container's
# ports to ports on the host. Methods from this class always return the host port.
class Process(object):
  def __init__(self, cmd, container_id=None, port_map=None):
    assert cmd is not None and len(cmd) >= 1,\
        'Process object must be created with valid command line argument list'
    assert container_id is None or port_map is not None,\
        "Must provide port_map for containerised process"
    self.cmd = cmd
    self.container_id = container_id
    self.port_map = port_map

  def __class_name(self):
    return self.__class__.__name__

  def __str__(self):
    return "<%s PID: %s (%s)>" % (self.__class_name(), self.__get_pid(),
                                  ' '.join(self.cmd))

  def __repr__(self):
    return str(self)

  def get_pid(self):
    """Gets the PID of the process. Returns None if the PID cannot be determined"""
    pid = self.__get_pid()
    if pid:
      LOG.info("Found PID %s for %s" % (pid, " ".join(self.cmd)))
    else:
      LOG.info("No PID found for process cmdline: %s. Process is dead?" %
               " ".join(self.cmd))
    return pid

  def get_pids(self):
    """Gets the PIDs of the process. In some circumstances, a process can run multiple
    times, e.g. when it forks in the Breakpad crash handler. Returns an empty list if no
    PIDs can be determined."""
    pids = [proc['pid'] for proc in self.__get_procs()]
    if pids:
      LOG.info("Found PIDs %s for %s" % (", ".join(map(str, pids)), " ".join(self.cmd)))
    else:
      LOG.info("No PID found for process cmdline: %s. Process is dead?" %
               " ".join(self.cmd))
    return pids

  def __procs_str(self, procs):
    return "\n".join([str(proc) for proc in procs])

  def __get_pid(self):
    procs = self.__get_procs()
    # Return early for containerized environments
    if len(procs) == 1:
      return procs[0]['pid']

    result = None
    # In some circumstances - notably ubsan tests - child processes can be slow to exit.
    # Only return the original process, i.e. one who's parent has a different cmd.
    pids = [proc['pid'] for proc in procs]
    for process in procs:
      if process['ppid'] not in pids:
        assert result is None,\
            "Multiple non-child processes:\n%s" % self.__procs_str(procs)
        result = process['pid']
      else:
        LOG.info("Child process active:\n%s" % self.__procs_str(procs))

    return result

  def __get_procs(self):
    """
    Returns a list of dicts containing {pid, ppid, cmdline} for related processes.
    """
    if self.container_id is not None:
      container_info = get_container_info(self.container_id)
      if container_info["State"]["Status"] != "running":
        return []
      return [{'pid': container_info["State"]["Pid"], 'ppid': 0, 'cmdline': self.cmd}]

    # In non-containerised case, search for process based on matching command lines.
    procs = []
    for process in psutil.process_iter(['pid', 'ppid', 'cmdline']):
      # Use info because it won't throw NoSuchProcess exceptions.
      if set(self.cmd) == set(process.info['cmdline']):
        procs.append(process.info)
    return procs

  def kill(self, signal=SIGKILL):
    """
    Kills the given processes.
    """
    if self.container_id is None:
      pid = self.get_pid()
      assert pid is not None, "No processes for %s" % self
      LOG.info('Killing %s with signal %s' % (self, signal))
      exec_process("kill -%d %d" % (signal, pid))
    else:
      LOG.info("Stopping container: {0}".format(self.container_id))
      check_call(["docker", "container", "stop", self.container_id])

  def start(self):
    """Start the process with the same arguments after it was stopped."""
    if self.container_id is None:
      binary = os.path.basename(self.cmd[0])
      restart_args = self.cmd[1:]
      LOG.info("Starting {0} with arguments {1}".format(binary, restart_args))
      run_daemon(binary, restart_args)
    else:
      LOG.info("Starting container: {0}".format(self.container_id))
      check_call(["docker", "container", "start", self.container_id])

  def restart(self):
    """Kills and restarts the process"""
    self.kill()
    self.wait_for_exit()
    self.start()

  def wait_for_exit(self):
    """Wait until the process exits (or return immediately if it already has exited."""
    LOG.info('Waiting for exit: {0} (PID: {1})'.format(
        ' '.join(self.cmd), self.get_pid()))
    while self.__get_pid() is not None:
      sleep(0.01)

  def kill_and_wait_for_exit(self, signal=SIGKILL):
    """Kill the process and wait for it to exit"""
    self.kill(signal)
    self.wait_for_exit()

  def modify_argument(self, argument, new_value):
    """Modify the 'argument' in start args with new_value.
    If no 'argument' in start args, add it.
    If new_value is None, add or remove 'argument'."""
    for i in range(1, len(self.cmd)):
      if self.cmd[i].split('=')[0] == argument:
        if new_value is None:
          del self.cmd[i]
        else:
          self.cmd[i] = (argument + '=' + new_value)
        return
    self.cmd.append(argument if new_value is None else (argument + '=' + new_value))


# Base class for all Impala processes
class BaseImpalaProcess(Process):
  def __init__(self, cmd, container_id=None, port_map=None):
    super(BaseImpalaProcess, self).__init__(cmd, container_id, port_map)
    self.hostname = self._get_hostname()
    self.webserver_interface = self._get_webserver_interface()

  def get_webserver_port(self):
    """Return the port for the webserver of this process."""
    return int(self._get_port('webserver_port', self._get_default_webserver_port()))

  def set_jvm_log_level(self, class_name, level="info"):
    """Helper method to set JVM log level for certain class name.
    Some daemon might not have JVM in it."""
    raise NotImplementedError()

  def _get_default_webserver_port(self):
    """Different daemons have different defaults. Subclasses must override."""
    raise NotImplementedError()

  def _get_webserver_certificate_file(self):
    # TODO: if this is containerised, the path will likely not be the same on the host.
    return self._get_arg_value("webserver_certificate_file", "")

  def _get_hostname(self):
    return self._get_arg_value("hostname", socket.gethostname())

  def _get_webserver_interface(self):
    return self._get_arg_value("webserver_interface", socket.gethostname())

  def _get_arg_value(self, arg_name, default=None):
    """Gets the argument value for given argument name"""
    for arg in self.cmd:
      if arg.strip().lstrip('-').startswith('%s=' % arg_name):
        return arg.split('=')[1]
    if default is None:
      assert 0, "Argument '{0}' not found in cmd '{1}'.".format(arg_name, self.cmd)
    return default

  def _get_port(self, arg_name, default):
    """Return the host port for the specified by the command line argument 'arg_name'.
    If 'self.port_map' is set, maps from container ports to host ports."""
    port = int(self._get_arg_value(arg_name, default))
    if self.port_map is not None:
      port = self.port_map.get(port, port)
    return port


# Represents an impalad process
class ImpaladProcess(BaseImpalaProcess):
  def __init__(self, cmd, container_id=None, port_map=None):
    super(ImpaladProcess, self).__init__(cmd, container_id, port_map)
    self.service = ImpaladService(self.hostname, self.webserver_interface,
        self.get_webserver_port(), self.__get_beeswax_port(),
        self.__get_krpc_port(), self.__get_hs2_port(), self.__get_hs2_http_port(),
        self._get_webserver_certificate_file())

  def _get_default_webserver_port(self):
    return DEFAULT_IMPALAD_WEBSERVER_PORT

  def __get_beeswax_port(self):
    return int(self._get_port('beeswax_port', DEFAULT_BEESWAX_PORT))

  def __get_krpc_port(self):
    return int(self._get_port('krpc_port', DEFAULT_KRPC_PORT))

  def __get_hs2_port(self):
    return int(self._get_port('hs2_port', DEFAULT_HS2_PORT))

  def __get_hs2_http_port(self):
    return int(self._get_port('hs2_http_port', DEFAULT_HS2_HTTP_PORT))

  def start(self, wait_until_ready=True, timeout=30):
    """Starts the impalad and waits until the service is ready to accept connections.
    'timeout' is the amount of time to wait for the Impala server to be in the
    ready state."""
    restart_args = self.cmd[1:]
    LOG.info("Starting Impalad process with args: {0}".format(restart_args))
    run_daemon("impalad", restart_args)
    if wait_until_ready:
      self.service.wait_for_metric_value('impala-server.ready',
                                         expected_value=1, timeout=timeout)

  def wait_for_catalog(self):
    """Waits for a catalog copy to be received by the impalad. When its received,
       additionally waits for client ports to be opened."""
    start_time = time.time()
    beeswax_port_is_open = False
    hs2_port_is_open = False
    num_dbs = 0
    num_tbls = 0
    while ((time.time() - start_time < CLUSTER_WAIT_TIMEOUT_IN_SECONDS)
        and not (beeswax_port_is_open and hs2_port_is_open)):
      try:
        num_dbs, num_tbls = self.service.get_metric_values(
            ["catalog.num-databases", "catalog.num-tables"])
        beeswax_port_is_open = self.service.beeswax_port_is_open()
        hs2_port_is_open = self.service.hs2_port_is_open()
      except Exception:
        LOG.exception(("Client services not ready. Waiting for catalog cache: "
            "({num_dbs} DBs / {num_tbls} tables). Trying again ...").format(
                num_dbs=num_dbs,
                num_tbls=num_tbls))
      sleep(0.5)

    if not hs2_port_is_open or not beeswax_port_is_open:
      raise RuntimeError(
          "Unable to open client ports within {num_seconds} seconds.".format(
              num_seconds=CLUSTER_WAIT_TIMEOUT_IN_SECONDS))

  def set_jvm_log_level(self, class_name, level):
    """Helper method to set JVM log level for certain class name."""
    url = SET_JAVA_LOGLEVEL_URL.format(
      self.webserver_interface, self.get_webserver_port())
    return post_data(url, {"class": class_name, "level": level})


# Represents a statestored process
class StateStoreProcess(BaseImpalaProcess):
  def __init__(self, cmd, container_id=None, port_map=None):
    super(StateStoreProcess, self).__init__(cmd, container_id, port_map)
    self.service = StateStoredService(self.hostname, self.webserver_interface,
        self.get_webserver_port(), self._get_webserver_certificate_file(),
        self.__get_port())

  def _get_default_webserver_port(self):
    return DEFAULT_STATESTORED_WEBSERVER_PORT

  def __get_port(self):
    return int(self._get_port('state_store_port', DEFAULT_STATESTORE_SERVICE_PORT))

  def start(self, wait_until_ready=True, additional_args=None):
    """Starts statestored and waits until the service is started and ready to accept
    connections."""
    restart_args = self.cmd[1:]
    if additional_args:
      restart_args = restart_args + [additional_args]
    LOG.info("Starting Statestored process: {0}".format(restart_args))
    run_daemon("statestored", restart_args)
    if wait_until_ready:
      self.service.wait_for_metric_value('statestore.service-started',
                                         expected_value=1, timeout=30)


# Represents a catalogd process
class CatalogdProcess(BaseImpalaProcess):
  def __init__(self, cmd, container_id=None, port_map=None):
    super(CatalogdProcess, self).__init__(cmd, container_id, port_map)
    self.service = CatalogdService(self.hostname, self.webserver_interface,
        self.get_webserver_port(), self._get_webserver_certificate_file(),
        self.__get_port())

  def _get_default_webserver_port(self):
    return DEFAULT_CATALOGD_WEBSERVER_PORT

  def __get_port(self):
    return int(self._get_port('catalog_service_port', DEFAULT_CATALOG_SERVICE_PORT))

  def start(self, wait_until_ready=True, additional_args=None):
    """Starts catalogd and waits until the service is ready to accept connections."""
    restart_args = self.cmd[1:]
    if additional_args:
      restart_args = restart_args + [additional_args]
    LOG.info("Starting Catalogd process: {0}".format(restart_args))
    run_daemon("catalogd", restart_args)
    if wait_until_ready:
      self.service.wait_for_metric_value('statestore-subscriber.connected',
                                         expected_value=1, timeout=30)

  def set_jvm_log_level(self, class_name, level):
    """Helper method to set JVM log level for certain class name."""
    url = SET_JAVA_LOGLEVEL_URL.format(
      self.webserver_interface, self.get_webserver_port())
    return post_data(url, {"class": class_name, "level": level})


# Represents an admission control process.
class AdmissiondProcess(BaseImpalaProcess):
  def __init__(self, cmd, container_id=None, port_map=None):
    super(AdmissiondProcess, self).__init__(cmd, container_id, port_map)
    self.service = AdmissiondService(self.hostname, self.webserver_interface,
        self.get_webserver_port(), self._get_webserver_certificate_file())

  def _get_default_webserver_port(self):
    return DEFAULT_ADMISSIOND_WEBSERVER_PORT


def find_user_processes(binaries):
  """Returns an iterator over all processes owned by the current user with a matching
  binary name from the provided list. Return a iterable of tuples, with each tuple
  containing the binary name and the psutil.Process object."""
  for pid in psutil.pids():
    try:
      process = psutil.Process(pid)
      cmdline = process.cmdline()
      if process.username() != getuser() or len(cmdline) == 0:
        continue
      # IMPALA-8820 - sometimes the process name does not reflect the executed binary
      # because the process can change its own name at runtime. Checking the command
      # line is more robust.
      binary_name = os.path.basename(cmdline[0])
      if binary_name in binaries:
        yield binary_name, process
    except KeyError as e:
      if "uid not found" not in str(e):
        raise
    except psutil.NoSuchProcess as e:
      # Ignore the case when a process no longer exists.
      pass


def run_daemon(daemon_binary, args, build_type="latest", env_vars={}, output_file=None):
  """Starts up an impalad with the specified command line arguments. args must be a list
  of strings. An optional build_type parameter can be passed to determine the build type
  to use for the impalad instance.  Any values in the env_vars override environment
  variables inherited from this process. If output_file is specified, stdout and stderr
  are redirected to that file.
  """
  bin_path = os.path.join(IMPALA_HOME, "be", "build", build_type, "service",
                          daemon_binary)
  redirect = ""
  if output_file is not None:
    redirect = "1>{0} 2>&1".format(output_file)
  cmd = [START_DAEMON_PATH, bin_path] + args
  # Use os.system() to start 'cmd' in the background via a shell so its parent will be
  # init after the shell exits. Otherwise, the parent of 'cmd' will be py.test and we
  # cannot cleanly kill it until py.test exits. In theory, Popen(shell=True) should
  # achieve the same thing but it doesn't work on some platforms for some reasons.
  sys_cmd = ("{set_cmds} {cmd} {redirect} &".format(
      set_cmds=''.join(["export {0}={1};".format(k, pipes.quote(v))
                         for k, v in env_vars.items()]),
      cmd=' '.join([pipes.quote(tok) for tok in cmd]),
      redirect=redirect))
  os.system(sys_cmd)


def get_container_info(container_id):
  """Get the output of "docker container inspect" as a python data structure."""
  containers = json.loads(check_output(["docker", "container", "inspect", container_id],
                                       universal_newlines=True))
  # Only one container should be present in the top level array.
  assert len(containers) == 1, json.dumps(containers, indent=4)
  return containers[0]
