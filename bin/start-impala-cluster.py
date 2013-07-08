#!/usr/bin/env python
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
import sys
from time import sleep, time
from optparse import OptionParser

# Options
parser = OptionParser()
parser.add_option("-s", "--cluster_size", type="int", dest="cluster_size", default=3,
                  help="Size of the cluster (number of impalad instances to start).")
parser.add_option("--build_type", dest="build_type", default= 'debug',
                  help="Build type to use - debug / release")
parser.add_option("--impalad_args", dest="impalad_args", default="",
                  help="Additional arguments to pass to each Impalad during startup")
parser.add_option("--state_store_args", dest="state_store_args", default="",
                  help="Additional arguments to pass to State Store during startup")
parser.add_option("--catalogd_args", dest="catalogd_args", default="",
                  help="Additional arguments to pass to the Catalog Service at startup")
parser.add_option("--kill", "--kill_only", dest="kill_only", action="store_true",
                  default=False, help="Instead of starting the cluster, just kill all"\
                  " the running impalads and the statestored.")
parser.add_option("--force_kill", dest="force_kill", action="store_true", default=False,
                  help="Force kill impalad and statestore processes.")
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


options, args = parser.parse_args()

IMPALA_HOME = os.environ['IMPALA_HOME']
KNOWN_BUILD_TYPES = ['debug', 'release']
IMPALAD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(IMPALA_HOME, 'be/build',
    options.build_type, 'statestore/statestored')
CATALOGD_PATH = os.path.join(IMPALA_HOME,
    'bin/start-catalogd.sh -build_type=%s' % options.build_type)
MINI_IMPALA_CLUSTER_PATH = IMPALAD_PATH + " -in-process"

IMPALA_SHELL = os.path.join(IMPALA_HOME, 'bin/impala-shell.sh')
IMPALAD_PORTS = ("-beeswax_port=%d -hs2_port=%d  -be_port=%d "
                 "-state_store_subscriber_port=%d -webserver_port=%d")
BE_LOGGING_ARGS = "-log_filename=%s -log_dir=%s -v=%s -logbufsecs=5"
DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 240

def exec_impala_process(cmd, args, stderr_log_file_path):
  redirect_output = str()
  if options.verbose:
    args += ' -logtostderr=1'
  else:
    redirect_output = "1>%s" % stderr_log_file_path
  cmd = '%s %s %s 2>&1 &' % (cmd, args, redirect_output)
  os.system(cmd)

def kill_all(force=False):
  kill_cmd = "killall"
  if force:
    kill_cmd += " -9"
  os.system("%s catalogd" % kill_cmd)
  os.system("%s mini-impala-cluster" % kill_cmd)
  os.system("%s impalad" % kill_cmd)
  os.system("%s statestored" % kill_cmd)
  sleep(1)

def start_statestore():
  print "Starting State Store logging to %s/statestored.INFO" % options.log_dir
  stderr_log_file_path = os.path.join(options.log_dir, "statestore-error.log")
  args = "%s %s" % (build_impalad_logging_args(0, "statestored"),
                    options.state_store_args)
  exec_impala_process(STATE_STORE_PATH, args, stderr_log_file_path)

def start_catalogd():
  print "Starting Catalog Service logging to %s/catalogd.INFO" % options.log_dir
  stderr_log_file_path = os.path.join(options.log_dir, "catalogd-error.log")
  args = "%s %s" % (build_impalad_logging_args(0, "catalogd"),
                    options.catalogd_args)
  exec_impala_process(CATALOGD_PATH, args, stderr_log_file_path)

def start_mini_impala_cluster(cluster_size):
  print ("Starting in-process Impala Cluster logging "
         "to %s/mini-impala-cluster.INFO" % options.log_dir)
  args = "-num_backends=%s" % (cluster_size)
  args += ' ' + build_impalad_logging_args(0, 'mini-impala-cluster')
  stderr_log_file_path = os.path.join(options.log_dir, 'mini-impala-cluster-error.log')
  exec_impala_process(MINI_IMPALA_CLUSTER_PATH, args, stderr_log_file_path)

def build_impalad_port_args(instance_num):
  BASE_BEESWAX_PORT = 21000
  BASE_HS2_PORT = 21050
  BASE_BE_PORT = 22000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 25000
  return IMPALAD_PORTS % (BASE_BEESWAX_PORT + instance_num, BASE_HS2_PORT + instance_num,
                          BASE_BE_PORT + instance_num,
                          BASE_STATE_STORE_SUBSCRIBER_PORT + instance_num,
                          BASE_WEBSERVER_PORT +instance_num)

def build_impalad_logging_args(instance_num, service_name):
  log_file_path = os.path.join(options.log_dir, "%s.INFO" % service_name)
  return BE_LOGGING_ARGS % (service_name, options.log_dir, options.log_level)

def start_impalad_instances(cluster_size):
  # Start each impalad instance and optionally redirect the output to a log file.
  for i in range(options.cluster_size):
    if i == 0:
      # The first impalad always logs to impalad.INFO
      service_name = "impalad"
    else:
      service_name = "impalad_node%s" % i
    args = build_impalad_logging_args(i, service_name)
    args += ' ' + build_impalad_port_args(i) +  ' ' + options.impalad_args
    stderr_log_file_path = os.path.join(options.log_dir, '%s-error.log' % service_name)
    exec_impala_process(IMPALAD_PATH, args, stderr_log_file_path)

def wait_for_impala_process_count(impala_cluster, retries=3):
  """Checks that the desired number of impalad/statestored processes are running.

  Refresh until the number running impalad/statestored processes reaches the expected
  number based on CLUSTER_SIZE, or the retry limit is hit. Failing this, raise a
  RuntimeError.
  """
  for i in range(retries):
    if len(impala_cluster.impalads) < options.cluster_size or \
        not impala_cluster.statestored:
          sleep(2)
          impala_cluster.refresh()
  if len(impala_cluster.impalads) < options.cluster_size:
    impalads_found = len(impala_cluster.impalads)
    msg = "Expected %d impalad(s), only %d found" % (options.cluster_size, impalads_found)
    raise RuntimeError, msg
  if not impala_cluster.statestored:
    raise RuntimeError, "statestored failed to start."

def wait_for_cluster_web(timeout_in_seconds=DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
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
    impalad.service.wait_for_num_known_live_backends(options.cluster_size, interval=2)
    start_time = time()
    while (time() - start_time < 120):
      try:
        num_dbs = impalad.service.get_metric_value('catalog.num-databases')
        sleep(2)
        if num_dbs != None and int(num_dbs) > 0:
          break
        print 'Waiting for Catalog...'
      except Exception:
        pass

def wait_for_cluster_cmdline(timeout_in_seconds=DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Checks if the cluster is "ready" by executing a simple query in a loop"""
  start_time = time()
  while os.system('%s -i localhost:21000 -q "%s"' %  (IMPALA_SHELL, 'select 1')) != 0:
    if time() - timeout_in_seconds > start_time:
      raise RuntimeError, 'Cluster did not start within %d seconds' % timeout_in_seconds
    print 'Cluster not yet available. Sleeping...'
    sleep(2)

if __name__ == "__main__":
  if options.kill_only:
    kill_all(force=options.force_kill)
    sys.exit(0)

  if options.build_type not in KNOWN_BUILD_TYPES:
    print 'Invalid build type %s' % options.build_type
    print 'Valid values: %s' % ', '.join(KNOWN_BUILD_TYPES)
    sys.exit(1)

  if options.cluster_size <= 0:
    print 'Please specify a cluster size > 0'
    sys.exit(1)

  # Kill existing processes.
  kill_all(force=options.force_kill)

  try:
    import json
    wait_for_cluster = wait_for_cluster_web
  except ImportError:
    print "json module not found, checking for cluster startup through the command-line"
    wait_for_cluster = wait_for_cluster_cmdline

  # If ImpalaCluster cannot be imported, fall back to the command-line to check
  # whether impalads/statestre are up.
  try:
    from tests.common.impala_cluster import ImpalaCluster
    # Make sure the processes have been killed. We loop till we can't detect a single
    # impalad or a statestore process.
    impala_cluster = ImpalaCluster()
    while len(impala_cluster.impalads) != 0 or impala_cluster.statestored:
      impala_cluster.refresh()
  except ImportError:
    print 'ImpalaCluster module not found.'
    wait_for_cluster = wait_for_cluster_cmdline

  if options.inprocess:
    # The statestore and the impalads start in the same process. Additionally,
    # the statestore does not have a debug webpage.
    start_mini_impala_cluster(options.cluster_size)
    wait_for_cluster_cmdline()
  else:
    try:
      start_statestore()
      start_catalogd()
      start_impalad_instances(options.cluster_size)
      wait_for_cluster()
    except Exception, e:
      print 'Error starting cluster: %s' % e
      sys.exit(1)

  print 'ImpalaD Cluster Running with %d nodes.' % options.cluster_size
