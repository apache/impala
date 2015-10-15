#!/usr/bin/env impala-python
# Copyright 2012 Cloudera Inc.
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

# Starts up an Impala cluster (ImpalaD + State Store) with the specified number of
# ImpalaD instances. Each ImpalaD runs on a different port allowing this to be run
# on a single machine.
import os
# psutil does not exist on hosts which build Impala packages. Furthermore, it is not
# needed as the packaging code does not start any processes.
# TODO: Remove this logic and all usage of psutil_exists once we switch to using a python
# virtualenv.
try:
  import psutil
  psutil_exists = True
except ImportError:
  psutil_exists = False
import sys
from time import sleep, time
from optparse import OptionParser
from testdata.common import cgroups
from multiprocessing.pool import ThreadPool

# Options
parser = OptionParser()
parser.add_option("-s", "--cluster_size", type="int", dest="cluster_size", default=3,
                  help="Size of the cluster (number of impalad instances to start).")
parser.add_option("--build_type", dest="build_type", default= 'debug',
                  help="Build type to use - debug / release")
parser.add_option("--impalad_args", dest="impalad_args", action="append", type="string",
                  default=[],
                  help="Additional arguments to pass to each Impalad during startup")
parser.add_option("--enable_rm", dest="enable_rm", action="store_true", default=False,
                  help="Enable resource management with Yarn and Llama.")
parser.add_option("--state_store_args", dest="state_store_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to State Store during startup")
parser.add_option("--catalogd_args", dest="catalogd_args", action="append",
                  type="string", default=[],
                  help="Additional arguments to pass to the Catalog Service at startup")
parser.add_option("--kill", "--kill_only", dest="kill_only", action="store_true",
                  default=False, help="Instead of starting the cluster, just kill all"\
                  " the running impalads and the statestored.")
parser.add_option("--force_kill", dest="force_kill", action="store_true", default=False,
                  help="Force kill impalad and statestore processes.")
parser.add_option("-r", "--restart_impalad_only", dest="restart_impalad_only",
                  action="store_true", default=False,
                  help="Restarts only the impalad processes")
parser.add_option("--in-process", dest="inprocess", action="store_true", default=False,
                  help="Start all Impala backends and state store in a single process.")
parser.add_option("--log_dir", dest="log_dir", default="/tmp",
                  help="Directory to store output logs to.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                  help="Prints all output to stderr/stdout.")
parser.add_option("--wait_for_cluster", dest="wait_for_cluster", action="store_true",
                  default=False, help="Wait until the cluster is ready to accept "\
                  "queries before returning.")
parser.add_option("--log_level", type="int", dest="log_level", default=1,
                   help="Set the impalad backend logging level")
parser.add_option("--jvm_args", dest="jvm_args", default="",
                  help="Additional arguments to pass to the JVM(s) during startup.")

options, args = parser.parse_args()

IMPALA_HOME = os.environ['IMPALA_HOME']
KNOWN_BUILD_TYPES = ['debug', 'release']
IMPALAD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(IMPALA_HOME,
    'bin/start-statestored.sh -build_type=%s' % options.build_type)
CATALOGD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-catalogd.sh -build_type=%s' % options.build_type)
MINI_IMPALA_CLUSTER_PATH = IMPALAD_PATH + " -in-process"

IMPALA_SHELL = os.path.join(IMPALA_HOME, 'bin/impala-shell.sh')
IMPALAD_PORTS = ("-beeswax_port=%d -hs2_port=%d  -be_port=%d "
                 "-state_store_subscriber_port=%d -webserver_port=%d "
                 "-llama_callback_port=%d")
JVM_ARGS = "-jvm_debug_port=%s -jvm_args=%s"
BE_LOGGING_ARGS = "-log_filename=%s -log_dir=%s -v=%s -logbufsecs=5"
RM_ARGS = ("-enable_rm=true -llama_addresses=%s -cgroup_hierarchy_path=%s "
           "-fair_scheduler_allocation_path=%s")
CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 240

def check_process_exists(binary, attempts=1):
  """Checks if a process exists given the binary name. The `attempts` count allows us to
  control the time a process needs to settle until it becomes available. After each try
  the script will sleep for one second and retry. Returns True if it exists and False
  otherwise.
  TODO: The conditional import will go away once we start using virtualenv.
  """
  if not psutil_exists:
    print "psutil not available, process invocations and kills may be unstable."
    return True
  for _ in range(attempts):
    for pid in psutil.get_pid_list():
      try:
        process = psutil.Process(pid)
        if process.name == binary: return True
      except psutil.NoSuchProcess, e:
        # Ignore the case when a process is no longer exists
        pass
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
  binaries = ['catalogd', 'impalad', 'statestored', 'mini-impala-cluster']
  arglists = [(binary, force) for binary in binaries]
  pool = ThreadPool(len(arglists))
  pool.map(lambda arglist: kill_matching_processes(*arglist), arglists, 1)
  pool.close()

def kill_matching_processes(binary_name, force=False):
  """Kills all processes with the given binary name"""
  # -w = Wait for processes to die.
  kill_cmd = "killall -w"
  if force: kill_cmd += " -9"
  os.system("%s %s" % (kill_cmd, binary_name))

  if psutil_exists and check_process_exists(binary_name):
    raise RuntimeError("Unable to kill %s. Check process permissions." % (binary_name, ))

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

def start_mini_impala_cluster(cluster_size):
  print ("Starting in-process Impala Cluster logging "
         "to %s/mini-impala-cluster.INFO" % options.log_dir)
  args = "-num_backends=%s %s" %\
         (cluster_size, build_impalad_logging_args(0, 'mini-impala-cluster'))
  stderr_log_file_path = os.path.join(options.log_dir, 'mini-impala-cluster-error.log')
  exec_impala_process(MINI_IMPALA_CLUSTER_PATH, args, stderr_log_file_path)

def build_impalad_port_args(instance_num):
  BASE_BEESWAX_PORT = 21000
  BASE_HS2_PORT = 21050
  BASE_BE_PORT = 22000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 25000
  BASE_LLAMA_CALLBACK_PORT = 28000
  return IMPALAD_PORTS % (BASE_BEESWAX_PORT + instance_num, BASE_HS2_PORT + instance_num,
                          BASE_BE_PORT + instance_num,
                          BASE_STATE_STORE_SUBSCRIBER_PORT + instance_num,
                          BASE_WEBSERVER_PORT + instance_num,
                          BASE_LLAMA_CALLBACK_PORT + instance_num)

def build_impalad_logging_args(instance_num, service_name):
  log_file_path = os.path.join(options.log_dir, "%s.INFO" % service_name)
  return BE_LOGGING_ARGS % (service_name, options.log_dir, options.log_level)

def build_jvm_args(instance_num):
  BASE_JVM_DEBUG_PORT = 30000
  return JVM_ARGS % (BASE_JVM_DEBUG_PORT + instance_num, options.jvm_args)

def build_rm_args(instance_num):
  if not options.enable_rm: return ""
  try:
    cgroup_path = cgroups.create_impala_cgroup_path(instance_num + 1)
  except Exception, ex:
    raise RuntimeError("Unable to initialize RM: %s" % str(ex))
  llama_address = "localhost:15000"

  # Don't bother checking if the path doesn't exist, the impalad won't start up
  relative_fs_cfg_path = 'cdh%s/node-%d/etc/hadoop/conf/fair-scheduler.xml' %\
      (os.environ.get('CDH_MAJOR_VERSION'), instance_num + 1)
  fs_cfg_path = os.path.join(os.environ.get('CLUSTER_DIR'), relative_fs_cfg_path)
  return RM_ARGS % (llama_address, cgroup_path, fs_cfg_path)

def start_impalad_instances(cluster_size):
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
    args = "%s %s %s %s %s" %\
          (build_impalad_logging_args(i, service_name), build_jvm_args(i),
           build_impalad_port_args(i), param_args,
           build_rm_args(i))
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
  This information is retrieved by querying the statestore debug webpage
  and each individual impalad's metrics webpage.
  """
  impala_cluster = ImpalaCluster()
  # impalad processes may take a while to come up.
  wait_for_impala_process_count(impala_cluster)
  for impalad in impala_cluster.impalads:
    impalad.service.wait_for_num_known_live_backends(options.cluster_size,
        timeout=CLUSTER_WAIT_TIMEOUT_IN_SECONDS, interval=2)
    wait_for_catalog(impalad, timeout_in_seconds=CLUSTER_WAIT_TIMEOUT_IN_SECONDS)

def wait_for_catalog(impalad, timeout_in_seconds):
  """Waits for the impalad catalog to become ready"""
  start_time = time()
  catalog_ready = False
  attempt = 0
  while (time() - start_time < timeout_in_seconds and not catalog_ready):
    try:
      num_dbs = impalad.service.get_metric_value('catalog.num-databases')
      num_tbls = impalad.service.get_metric_value('catalog.num-tables')
      catalog_ready = impalad.service.get_metric_value('catalog.ready')
      if catalog_ready or attempt % 4 == 0:
          print 'Waiting for Catalog... Status: %s DBs / %s tables (ready=%s)' %\
              (num_dbs, num_tbls, catalog_ready)
      attempt += 1
    except Exception, e:
      print e
    sleep(0.5)
  if not catalog_ready:
    raise RuntimeError('Catalog was not initialized in expected time period.')

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

  # Kill existing cluster processes based on the current configuration.
  if options.restart_impalad_only:
    if options.inprocess:
      print 'Cannot perform individual component restarts using an in-process cluster'
      sys.exit(1)
    kill_matching_processes('impalad', force=options.force_kill)
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

  if options.inprocess:
    # The statestore and the impalads start in the same process.
    start_mini_impala_cluster(options.cluster_size)
    wait_for_cluster_cmdline()
  else:
    try:
      if not options.restart_impalad_only:
        start_statestore()
        start_catalogd()
      start_impalad_instances(options.cluster_size)
      # Sleep briefly to reduce log spam: the cluster takes some time to start up.
      sleep(3)
      wait_for_cluster()
    except Exception, e:
      print 'Error starting cluster: %s' % e
      sys.exit(1)

  print 'Impala Cluster Running with %d nodes.' % options.cluster_size
