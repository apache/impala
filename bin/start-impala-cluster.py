#!/usr/bin/env impala-python
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

# Starts up an Impala cluster (ImpalaD + State Store) with the specified number of
# ImpalaD instances. Each ImpalaD runs on a different port allowing this to be run
# on a single machine.
import os
import psutil
import sys
from getpass import getuser
from time import sleep, time
from optparse import OptionParser, SUPPRESS_HELP
from testdata.common import cgroups

KUDU_MASTER_HOSTS = os.getenv('KUDU_MASTER_HOSTS', '127.0.0.1')
DEFAULT_IMPALA_MAX_LOG_FILES = os.environ.get('IMPALA_MAX_LOG_FILES', 10)


# Options
parser = OptionParser()
parser.add_option("-s", "--cluster_size", type="int", dest="cluster_size", default=3,
                  help="Size of the cluster (number of impalad instances to start).")
parser.add_option("-c", "--num_coordinators", type="int", dest="num_coordinators",
                  default=3, help="Number of coordinators.")
parser.add_option("--use_exclusive_coordinators", dest="use_exclusive_coordinators",
                  action="store_true", default=False, help="If true, coordinators only "
                  "coordinate queries and execute coordinator fragments. If false, "
                  "coordinators also act as executors.")
parser.add_option("--build_type", dest="build_type", default= 'latest',
                  help="Build type to use - debug / release / latest")
parser.add_option("--impalad_args", dest="impalad_args", action="append", type="string",
                  default=[],
                  help="Additional arguments to pass to each Impalad during startup")
parser.add_option("--state_store_args", dest="state_store_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to State Store during startup")
parser.add_option("--catalogd_args", dest="catalogd_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to the Catalog Service at startup")
parser.add_option("--disable_krpc", dest="disable_krpc", action="store_true",
                  default=False, help="Disable KRPC DataStream service during startup.")
parser.add_option("--kill", "--kill_only", dest="kill_only", action="store_true",
                  default=False, help="Instead of starting the cluster, just kill all"
                  " the running impalads and the statestored.")
parser.add_option("--force_kill", dest="force_kill", action="store_true", default=False,
                  help="Force kill impalad and statestore processes.")
parser.add_option("-r", "--restart_impalad_only", dest="restart_impalad_only",
                  action="store_true", default=False,
                  help="Restarts only the impalad processes")
parser.add_option("--in-process", dest="inprocess", action="store_true", default=False,
                  help="Start all Impala backends and state store in a single process.")
parser.add_option("--log_dir", dest="log_dir",
                  default=os.environ['IMPALA_CLUSTER_LOGS_DIR'],
                  help="Directory to store output logs to.")
parser.add_option('--max_log_files', default=DEFAULT_IMPALA_MAX_LOG_FILES,
                  help='Max number of log files before rotation occurs.')
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                  help="Prints all output to stderr/stdout.")
parser.add_option("--log_level", type="int", dest="log_level", default=1,
                   help="Set the impalad backend logging level")
parser.add_option("--jvm_args", dest="jvm_args", default="",
                  help="Additional arguments to pass to the JVM(s) during startup.")
parser.add_option("--kudu_master_hosts", default=KUDU_MASTER_HOSTS,
                  help="The host name or address of the Kudu master. Multiple masters "
                      "can be specified using a comma separated list.")

# For testing: list of comma-separated delays, in milliseconds, that delay impalad catalog
# replica initialization. The ith delay is applied to the ith impalad.
parser.add_option("--catalog_init_delays", dest="catalog_init_delays", default="",
                  help=SUPPRESS_HELP)

options, args = parser.parse_args()

IMPALA_HOME = os.environ['IMPALA_HOME']
KNOWN_BUILD_TYPES = ['debug', 'release', 'latest']
IMPALAD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(IMPALA_HOME,
    'bin/start-statestored.sh -build_type=%s' % options.build_type)
CATALOGD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-catalogd.sh -build_type=%s' % options.build_type)
MINI_IMPALA_CLUSTER_PATH = IMPALAD_PATH + " -in-process"

IMPALA_SHELL = os.path.join(IMPALA_HOME, 'bin/impala-shell.sh')
IMPALAD_PORTS = ("-beeswax_port=%d -hs2_port=%d  -be_port=%d -krpc_port=%d "
                 "-state_store_subscriber_port=%d -webserver_port=%d")
JVM_ARGS = "-jvm_debug_port=%s -jvm_args=%s"
BE_LOGGING_ARGS = "-log_filename=%s -log_dir=%s -v=%s -logbufsecs=5 -max_log_files=%s"
CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 240
# Kills have a timeout to prevent automated scripts from hanging indefinitely.
# It is set to a high value to avoid failing if processes are slow to shut down.
KILL_TIMEOUT_IN_SECONDS = 240

def find_user_processes(binaries):
  """Returns an iterator over all processes owned by the current user with a matching
  binary name from the provided list."""
  for pid in psutil.get_pid_list():
    try:
      process = psutil.Process(pid)
      if process.username == getuser() and process.name in binaries: yield process
    except KeyError, e:
      if "uid not found" not in str(e):
        raise
    except psutil.NoSuchProcess, e:
      # Ignore the case when a process no longer exists.
      pass

def check_process_exists(binary, attempts=1):
  """Checks if a process exists given the binary name. The `attempts` count allows us to
  control the time a process needs to settle until it becomes available. After each try
  the script will sleep for one second and retry. Returns True if it exists and False
  otherwise.
  """
  for _ in range(attempts):
    for proc in find_user_processes([binary]):
      return True
    sleep(1)
  return False

def exec_impala_process(cmd, args, stderr_log_file_path):
  redirect_output = str()
  if options.verbose:
    args += ' -logtostderr=1'
  else:
    redirect_output = "1>%s" % stderr_log_file_path
  cmd = '%s %s %s 2>&1 &' % (cmd, args, redirect_output)
  os.system(cmd)

def kill_cluster_processes(force=False):
  binaries = ['catalogd', 'impalad', 'statestored']
  kill_matching_processes(binaries, force)

def kill_matching_processes(binary_names, force=False):
  """Kills all processes with the given binary name, waiting for them to exit"""
  # Send all the signals before waiting so that processes can clean up in parallel.
  processes = list(find_user_processes(binary_names))
  for process in processes:
    try:
      if force:
        process.kill()
      else:
        process.terminate()
    except psutil.NoSuchProcess:
      pass

  for process in processes:
    try:
      process.wait(KILL_TIMEOUT_IN_SECONDS)
    except psutil.TimeoutExpired:
      raise RuntimeError("Unable to kill %s (pid %d) after %d seconds." % (process.name,
          process.pid, KILL_TIMEOUT_IN_SECONDS))

def start_statestore():
  print "Starting State Store logging to %s/statestored.INFO" % options.log_dir
  stderr_log_file_path = os.path.join(options.log_dir, "statestore-error.log")
  args = "%s %s" % (build_impalad_logging_args(0, "statestored"),
                    " ".join(options.state_store_args))
  exec_impala_process(STATE_STORE_PATH, args, stderr_log_file_path)
  if not check_process_exists("statestored", 10):
    raise RuntimeError("Unable to start statestored. Check log or file permissions"
                       " for more details.")

def start_catalogd():
  print "Starting Catalog Service logging to %s/catalogd.INFO" % options.log_dir
  stderr_log_file_path = os.path.join(options.log_dir, "catalogd-error.log")
  args = "%s %s %s" % (build_impalad_logging_args(0, "catalogd"),
                       " ".join(options.catalogd_args),
                       build_jvm_args(options.cluster_size))
  exec_impala_process(CATALOGD_PATH, args, stderr_log_file_path)
  if not check_process_exists("catalogd", 10):
    raise RuntimeError("Unable to start catalogd. Check log or file permissions"
                       " for more details.")

def build_impalad_port_args(instance_num):
  BASE_BEESWAX_PORT = 21000
  BASE_HS2_PORT = 21050
  BASE_BE_PORT = 22000
  BASE_KRPC_PORT = 27000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 25000
  return IMPALAD_PORTS % (BASE_BEESWAX_PORT + instance_num, BASE_HS2_PORT + instance_num,
                          BASE_BE_PORT + instance_num,
                          BASE_KRPC_PORT + instance_num,
                          BASE_STATE_STORE_SUBSCRIBER_PORT + instance_num,
                          BASE_WEBSERVER_PORT + instance_num)

def build_impalad_logging_args(instance_num, service_name):
  return BE_LOGGING_ARGS % (service_name, options.log_dir, options.log_level,
                            options.max_log_files)

def build_jvm_args(instance_num):
  BASE_JVM_DEBUG_PORT = 30000
  return JVM_ARGS % (BASE_JVM_DEBUG_PORT + instance_num, options.jvm_args)

def start_impalad_instances(cluster_size, num_coordinators, use_exclusive_coordinators):
  """Start 'cluster_size' impalad instances. The first 'num_coordinator' instances will
    act as coordinators. 'use_exclusive_coordinators' specifies whether the coordinators
    will only execute coordinator fragments."""
  if cluster_size == 0:
    # No impalad instances should be started.
    return

  # The default memory limit for an impalad is 80% of the total system memory. On a
  # mini-cluster with 3 impalads that means 240%. Since having an impalad be OOM killed
  # is very annoying, the mem limit will be reduced. This can be overridden using the
  # --impalad_args flag. virtual_memory().total returns the total physical memory.
  mem_limit = int(0.8 * psutil.virtual_memory().total / cluster_size)

  delay_list = []
  if options.catalog_init_delays != "":
    delay_list = [delay.strip() for delay in options.catalog_init_delays.split(",")]

  # Start each impalad instance and optionally redirect the output to a log file.
  for i in range(cluster_size):
    if i == 0:
      # The first impalad always logs to impalad.INFO
      service_name = "impalad"
    else:
      service_name = "impalad_node%s" % i
      # Sleep between instance startup: simultaneous starts hurt the minikdc
      # Yes, this is a hack, but it's easier than modifying the minikdc...
      # TODO: is this really necessary?
      sleep(1)

    print "Starting Impala Daemon logging to %s/%s.INFO" % (options.log_dir,
        service_name)

    # impalad args from the --impalad_args flag. Also replacing '#ID' with the instance.
    param_args = (" ".join(options.impalad_args)).replace("#ID", str(i))
    args = "--mem_limit=%s %s %s %s %s" %\
          (mem_limit,  # Goes first so --impalad_args will override it.
           build_impalad_logging_args(i, service_name), build_jvm_args(i),
           build_impalad_port_args(i), param_args)
    if options.kudu_master_hosts:
      # Must be prepended, otherwise the java options interfere.
      args = "-kudu_master_hosts %s %s" % (options.kudu_master_hosts, args)

    if i >= num_coordinators:
      args = "-is_coordinator=false %s" % (args)
    elif use_exclusive_coordinators:
      # Coordinator instance that doesn't execute non-coordinator fragments
      args = "-is_executor=false %s" % (args)

    if i < len(delay_list):
      args = "-stress_catalog_init_delay_ms=%s %s" % (delay_list[i], args)

    if options.disable_krpc:
      args = "-use_krpc=false %s" % (args)

    stderr_log_file_path = os.path.join(options.log_dir, '%s-error.log' % service_name)
    exec_impala_process(IMPALAD_PATH, args, stderr_log_file_path)

def wait_for_impala_process_count(impala_cluster, retries=10):
  """Checks that the desired number of impalad/statestored processes are running.

  Refresh until the number running impalad/statestored processes reaches the expected
  number based on CLUSTER_SIZE, or the retry limit is hit. Failing this, raise a
  RuntimeError.
  """
  for i in range(retries):
    if len(impala_cluster.impalads) < options.cluster_size or \
        not impala_cluster.statestored or not impala_cluster.catalogd:
          sleep(1)
          impala_cluster.refresh()
  msg = str()
  if len(impala_cluster.impalads) < options.cluster_size:
    impalads_found = len(impala_cluster.impalads)
    msg += "Expected %d impalad(s), only %d found\n" %\
        (options.cluster_size, impalads_found)
  if not impala_cluster.statestored:
    msg += "statestored failed to start.\n"
  if not impala_cluster.catalogd:
    msg += "catalogd failed to start.\n"
  if msg:
    raise RuntimeError(msg)

def wait_for_cluster_web(timeout_in_seconds=CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Checks if the cluster is "ready"

  A cluster is deemed "ready" if:
    - All backends are registered with the statestore.
    - Each impalad knows about all other impalads.
    - Each coordinator impalad's catalog cache is ready.
  This information is retrieved by querying the statestore debug webpage
  and each individual impalad's metrics webpage.
  """
  impala_cluster = ImpalaCluster()
  # impalad processes may take a while to come up.
  wait_for_impala_process_count(impala_cluster)

  # TODO: fix this for coordinator-only nodes as well.
  expected_num_backends = options.cluster_size
  if options.catalog_init_delays != "":
    for delay in options.catalog_init_delays.split(","):
      if int(delay.strip()) != 0: expected_num_backends -= 1

  for impalad in impala_cluster.impalads:
    impalad.service.wait_for_num_known_live_backends(expected_num_backends,
        timeout=CLUSTER_WAIT_TIMEOUT_IN_SECONDS, interval=2)
    if impalad._get_arg_value('is_coordinator', default='true') == 'true' and \
       impalad._get_arg_value('stress_catalog_init_delay_ms', default=0) == 0:
      wait_for_catalog(impalad)

def wait_for_catalog(impalad, timeout_in_seconds=CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Waits for a catalog copy to be received by the impalad. When its received,
     additionally waits for client ports to be opened."""
  start_time = time()
  client_beeswax = None
  client_hs2 = None
  num_dbs = 0
  num_tbls = 0
  while (time() - start_time < timeout_in_seconds):
    try:
      num_dbs = impalad.service.get_metric_value('catalog.num-databases')
      num_tbls = impalad.service.get_metric_value('catalog.num-tables')
      client_beeswax = impalad.service.create_beeswax_client()
      client_hs2 = impalad.service.create_hs2_client()
      break
    except Exception as e:
      print 'Client services not ready.'
      print 'Waiting for catalog cache: (%s DBs / %s tables). Trying again ...' %\
        (num_dbs, num_tbls)
    finally:
      if client_beeswax is not None: client_beeswax.close()
    sleep(0.5)

  if client_beeswax is None or client_hs2 is None:
    raise RuntimeError('Unable to open client ports within %s seconds.'\
                       % timeout_in_seconds)

def wait_for_cluster_cmdline(timeout_in_seconds=CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Checks if the cluster is "ready" by executing a simple query in a loop"""
  start_time = time()
  while os.system('%s -i localhost:21000 -q "%s"' %  (IMPALA_SHELL, 'select 1')) != 0:
    if time() - timeout_in_seconds > start_time:
      raise RuntimeError('Cluster did not start within %d seconds' % timeout_in_seconds)
    print 'Cluster not yet available. Sleeping...'
    sleep(2)

if __name__ == "__main__":
  if options.kill_only:
    kill_cluster_processes(force=options.force_kill)
    sys.exit(0)

  if options.build_type not in KNOWN_BUILD_TYPES:
    print 'Invalid build type %s' % options.build_type
    print 'Valid values: %s' % ', '.join(KNOWN_BUILD_TYPES)
    sys.exit(1)

  if options.cluster_size < 0:
    print 'Please specify a cluster size >= 0'
    sys.exit(1)

  if options.num_coordinators <= 0:
    print 'Please specify a valid number of coordinators > 0'
    sys.exit(1)

  if options.use_exclusive_coordinators and options.num_coordinators >= options.cluster_size:
    print 'Cannot start an Impala cluster with no executors'
    sys.exit(1)

  if not os.path.isdir(options.log_dir):
    print 'Log dir does not exist or is not a directory: %s' % options.log_dir
    sys.exit(1)

  # Kill existing cluster processes based on the current configuration.
  if options.restart_impalad_only:
    if options.inprocess:
      print 'Cannot perform individual component restarts using an in-process cluster'
      sys.exit(1)
    kill_matching_processes(['impalad'], force=options.force_kill)
  else:
    kill_cluster_processes(force=options.force_kill)

  try:
    import json
    wait_for_cluster = wait_for_cluster_web
  except ImportError:
    print "json module not found, checking for cluster startup through the command-line"
    wait_for_cluster = wait_for_cluster_cmdline

  # If ImpalaCluster cannot be imported, fall back to the command-line to check
  # whether impalads/statestore are up.
  try:
    from tests.common.impala_cluster import ImpalaCluster
    if options.restart_impalad_only:
      impala_cluster = ImpalaCluster()
      if not impala_cluster.statestored or not impala_cluster.catalogd:
        print 'No running statestored or catalogd detected. Restarting entire cluster.'
        options.restart_impalad_only = False
  except ImportError:
    print 'ImpalaCluster module not found.'
    # TODO: Update this code path to work similar to the ImpalaCluster code path when
    # restarting only impalad processes. Specifically, we should do a full cluster
    # restart if either the statestored or catalogd processes are down, even if
    # restart_only_impalad=True.
    wait_for_cluster = wait_for_cluster_cmdline

  try:
    if not options.restart_impalad_only:
      start_statestore()
      start_catalogd()
    start_impalad_instances(options.cluster_size, options.num_coordinators,
                            options.use_exclusive_coordinators)
    # Sleep briefly to reduce log spam: the cluster takes some time to start up.
    sleep(3)

    # Check for the cluster to be ready.
    wait_for_cluster()
  except Exception, e:
    print 'Error starting cluster: %s' % e
    sys.exit(1)

  if options.use_exclusive_coordinators == True:
    executors = options.cluster_size - options.num_coordinators
  else:
    executors = options.cluster_size
  print 'Impala Cluster Running with %d nodes (%d coordinators, %d executors).' % (
      options.cluster_size,
      min(options.cluster_size, options.num_coordinators),
      executors)
