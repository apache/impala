# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Basic object model for an Impala cluster. Basic model is an "Impala Service" which
# represents a collection of ImpalaD processes and a State Store service. The Impala
# service is associated with an ImpalaCluster which has information on all the hosts
# / machines in the cluster along with the different services available (currently
# only Impala.
# To authenticate remote operation over SSH set the IMPALA_SSH_PRIVATE_KEY,
# IMPALA_SSH_PRIVATE_KEY_PASSWORD (if applicable), and IMPALA_SSH_USER environment
# variables. If not set the current user and default keys will be used.
#
# Dependencies:
# paramiko - Used to perform remote SSH commands. To install run 'easy_install paramiko'
# cm_api - Used to interact with cluster environment - Visit cloudera.github.com/cm_api/
# for installation details.
import cmd
import logging
import time
import os
import sys
import json
import paramiko
import urllib
from collections import defaultdict
from cm_api.api_client import ApiResource
from datetime import datetime
from optparse import OptionParser
from paramiko import PKey

logging.basicConfig(level=logging.ERROR, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_cluster')
LOG.setLevel(level=logging.DEBUG)

# Environment variables that control how to execute commands on remote machines
IMPALA_SSH_PRIVATE_KEY = os.environ.get('IMPALA_PRIVATE_KEY', None)
IMPALA_SSH_PRIVATE_KEY_PASSWORD = os.environ.get('IMPALA_PRIVATE_KEY_PASSWORD', str())
IMPALA_SSH_USER = os.environ.get('IMPALA_SSH_USER', 'impala')

# Represents a set of Impala services, processes, and  machines they are running on
class ImpalaCluster(object):
  def __init__(self, cm_host, cm_cluster_name, username, password):
    self.cm_api = ApiResource(cm_host, username=username, password=password)
    self.hosts = dict()
    self.services = list()
    self.cluster = self.cm_api.get_cluster(cm_cluster_name)
    if self.cluster is None:
      raise RuntimeError, 'Cluster name "%s" not found' % cm_cluster_name

    self.__load_hosts()
    self.__impala_service = ImpalaService(self)

  def _get_all_services(self):
    return self.cluster.get_all_services()

  def get_impala_service(self):
    return self.__impala_service

  def __load_hosts(self):
    self.hosts = dict()
    # Search for all hosts that are in the target cluster.
    # There is no API that provides the list of host in a given cluster, so to find them
    # we must loop through all the hosts and check the cluster name matches.
    for host_info in self.cm_api.get_all_hosts():
      # host_info doesn't include a link to the roleRef so need to do another lookup
      # based on the hostId.
      host = self.cm_api.get_host(host_info.hostId)
      for roleRef.get('clusterName') == self.cluster_name:
        self.hosts[host_info.hostId] = Host(host)
          break


# Base class for Cluster service objects
class ClusterService(object):
  def __init__(self):
    pass

  def start(self):
    raise NotImplementedError, 'This method is NYI'

  def stop(self):
    raise NotImplementedError, 'This method is NYI'

  def restart(self):
    raise NotImplementedError, 'This method is NYI'


# Represents an Impala service - a set of ImpalaD processes and a statestore.
class ImpalaService(ClusterService):
  def __init__(self, cluster):
    self.__parent_cluster = cluster
    self.__state_store_process = None
    self.__impalad_processes = list()
    self.__impala_service = self.__get_impala_service_internal()
    if self.__impala_service is None:
      raise RuntimeError, 'No Impala service found on cluster'

    # For each service, CM has a set of roles. A role is a lightweight object
    # that provides a link between a physical host machine and a logical service.
    # Here that information is used to determine where all the impala processes
    # are actually located (what machines).
    for role in self.__impala_service.get_all_roles():
      if 'STATESTORE' in role.name:
        self.__state_store_process = ImpalaStateStoreProcess(self,
            self.__parent_cluster.hosts[role.hostRef.hostId], role)
      elif 'IMPALAD' in role.name:
        self.__impalad_processes.append(ImpaladProcess(
            self.__parent_cluster.hosts[role.hostRef.hostId], role))
      else:
        raise RuntimeError, 'Unknown Impala role type'

  def get_state_store_process(self):
    """ Returns the state store process """
    return self.__state_store_process

  def get_impalad_process(self, hostname):
    """ Returns the impalad process running on the specified hostname """
    return first(self.__impalad_processes,
                 lambda impalad: impalad.hostname == hostname)

  def get_all_impalad_processes(self):
    return self.__impalad_processes

  def __get_impala_service_internal(self):
    return first(self.__parent_cluster._get_all_services(),
                 lambda service: 'impala' in service.name)

  def set_process_auto_restart_config(self, value):
    """ Sets the process_auto_restart configuration value.

        If set, Impala processes will automatically restart if the process dies
    """
    self.__update_configuration('process_auto_restart', str(value).lower())

  def __update_configuration(self, name, value):
    for role in self.__impala_service.get_all_roles():
      role.update_config({name: value})
      LOG.debug('Updated Config Value: %s/%s' % (role.name, role.get_config()))

  def start(self):
    """ Starts all roles/processes of the service """
    LOG.debug("Starting ImpalaService")
    self.__impala_service.start()
    self.__wait_for_service_state('STARTED')

  def restart(self):
    """ Restarts all roles/processes of the service """
    LOG.debug("Restarting ImpalaService")
    self.__impala_service.restart()
    self.__wait_for_service_state('STARTED')

  def stop(self):
    """ Stops all roles/processes of the service """
    LOG.debug("Stopping ImpalaService")
    self.__impala_service.stop()
    self.__wait_for_service_state('STOPPED')

  def get_health_summary(self):
    return self.__get_impala_service_internal().healthSummary

  def state(self):
    """
    Gets the current state of the service (a string value).

    Possible values are STOPPED, STOPPING, STARTED, STARTING, UNKNOWN
    """
    return self.__get_impala_service_internal().serviceState

  def __wait_for_service_state(self, desired_state, timeout=0):
    """ Waits for the service to reach the specified state within the given time(secs) """
    current_state = self.state()
    start_time = datetime.now()
    while current_state.upper() != desired_state.upper():
      LOG.debug('Current Impala Service State: %s Waiting For: %s' % (current_state,
                                                                      desired_state))
      # Sleep for a bit to give the serivce time to reach the target state.
      time.sleep(1)
      # Get the current service state.
      current_state = self.state()
      if timeout != 0 and (datetime.now() - start_time).seconds > timeout:
        raise RuntimeError, 'Did not reach desired state within %d seconds.' % timeout


# Represents one host/machine in the cluster.
class Host(object):
  def __init__(self, cm_host):
    self.cm_host = cm_host
    self.hostname = cm_host.hostname
    self.ssh_client = paramiko.SSHClient()
    self.ssh_client.load_system_host_keys()
    self.ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

  def exec_cmd(self, cmd):
    """ Executes a command on the machine using SSH """
    self.ssh_client.connect(hostname=self.hostname, username=IMPALA_SSH_USER)
    LOG.debug('Executing on host: %s Command: "%s"' % (self.hostname, cmd))
    rsa_key = None
    # TODO: Support other key types besides RSA
    if IMPALA_SSH_PRIVATE_KEY is not None:
      paramiko.RSAKey.from_private_key_file(filename=IMPALA_SSH_PRIVATE_KEY,
                                            password=IMPALA_SSH_PRIVATE_KEY_PASSWORD)
    stdin, stdout, stderr = self.ssh_client.exec_command(cmd, rsa_key)
    stdout_str = stdout.read()
    stderr_str = stderr.read()
    if stdout_str: LOG.debug(stdout_str.strip())
    if stderr_str: LOG.debug(stderr_str.strip())
    stdout.close()
    stderr.close()
    self.ssh_client.close()
    return stdout_str, stderr_str


# Represents a single process running on a machine
class Process(object):
  def __init__(self, host, process_name):
    self.name = process_name
    self.host = host
    self.hostname = host.hostname
    self.__pid = None

  def kill(self):
    """ Kill the process if it is running, if not running this will be a no-op """
    pid = self.get_pid()
    if pid is not None and pid > 0:
      self.host.exec_cmd('sudo kill -9 %d' % self.get_pid())
    else:
      LOG.debug('Skipping kill of pid: %s on host: %s' % (pid, self.hostname))

  def get_pid(self):
    """ Returns the process' current pid """
    stdout, stderr = self.host.exec_cmd('sudo /sbin/pidof %s' % self.name)
    pids = [pid.strip() for pid in stdout.split()]

    # Note: This is initialized to -2 instead of -1 because 'kill -1' kills all processes.
    self.__pid = -2
    if len(pids) > 1:
      raise RuntimeError, 'Error - %d PIDs detected. Expected 1' % len(pids)
    elif len(pids) == 1:
      self.__pid = int(pids[0])
    return self.__pid

  def is_running(self):
    return self.get_pid() > 0


# Represents a single Impala statestore process
class ImpalaStateStoreProcess(Process):
  def __init__(self, parent_service, host, cm_role, metrics_port=9190):
    Process.__init__(self, host, 'impala-statestore');
    self.metrics_port = metrics_port
    self.role = cm_role

  def get_impala_backend(self, hostname):
    """Returns the impala backend on the specified host."""
    return first(self.get_live_impala_backends(),
                 lambda backend: backend.split(':')[0] == hostname)

  def get_live_impala_backends(self):
    """Returns a list of host:be_port strings of live impalad instances."""
    metrics_page = urllib.urlopen("http://%s:%d/jsonmetrics" %\
        (self.host, int(self.metrics_port)))
    return json.loads(metrics_page.read())['statestore.live.backends.list']

  def __str__(self):
    return 'Name: %s Host: %s' % (self.name, self.hostname)


# Represents a single Impalad process
class ImpaladProcess(Process):
  def __init__(self, host, cm_role, be_port=22000, beeswax_port=21000):
    Process.__init__(self, host, 'impalad');
    self.role = cm_role
    self.host = host
    self.be_port = be_port
    self.beeswax_port = beeswax_port
    self.__pid = None

  def get_pid(self):
    try:
      self.__pid = super(ImpaladProcess, self).get_pid()
    except RuntimeError, e:
      # There could be multiple ImpalaD instances running on the same
      # machine (local testing case). Fall back to this method for getting the pid.
      LOG.info('Multiple PIDs found for Impalad service. Attempting to get PID based on '\
               'the the be_port: %s', e)
      stdout, stderr = self.host.exec_cmd(
          'lsof -i:%d | awk \'{print $2}\' | tail -n 1' % self.be_port)
      self.__pid = int(stdout) if stdout else -1
    return self.__pid

  def __str__(self):
    return 'Name: %s, Host: %s BE Port: %d Beeswax Port: %d PID: %s'\
        % (self.name, self.hostname, self.be_port, self.beeswax_port, self.__pid)

def first(collection, match_function):
  """ Returns the first item in the collection that satisfies the match function """
  return next((item for item in collection if match_function(item)), None)
