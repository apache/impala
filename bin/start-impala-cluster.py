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
parser.add_option("--kill_only", dest="kill_only", action="store_true", default=False,
                  help="Instead of starting the cluster, just kill all running Impalad"\
                  " and State Store processes.")
parser.add_option("--in-process", dest="inprocess", action="store_true", default=False,
                  help="Start all Impala backends and state store in a single process.")
parser.add_option("--log_dir", dest="log_dir", default="/tmp",
                  help="Directory to store output logs to.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False,
                  help="Prints all output to stderr/stdout.")
parser.add_option("--wait_for_cluster", dest="wait_for_cluster", action="store_true",
                  default=False, help="Wait until the cluster is ready to accept "\
                  "queries before returning.")
options, args = parser.parse_args()

IMPALA_HOME = os.environ['IMPALA_HOME']
KNOWN_BUILD_TYPES = ['debug', 'release']
IMPALAD_PATH = os.path.join(IMPALA_HOME,
                            'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(IMPALA_HOME, 'be/build', options.build_type,
                                'sparrow/statestored')
MINI_IMPALA_CLUSTER_PATH = os.path.join(IMPALA_HOME, 'be/build', options.build_type,
                                        'testutil/mini-impala-cluster')
IMPALA_SHELL = os.path.join(IMPALA_HOME, 'bin/impala-shell.sh')
SET_CLASSPATH_SCRIPT_PATH = os.path.join(IMPALA_HOME, 'bin/set-classpath.sh')
IMPALAD_ARGS = "-fe_port=%d -be_port=%d -state_store_subscriber_port=%d "\
               "-webserver_port=%d " + options.impalad_args
STATE_STORE_ARGS = options.state_store_args
REDIRECT_STR = "> %(file_name)s 2>&1"
DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 120

def kill_all():
  os.system("killall mini-impala-cluster")
  os.system("killall impalad")
  os.system("killall statestored")

def start_statestore():
  output_file = os.path.join(options.log_dir, 'statestored.out')
  print "Starting State Store with logging to %s" % (output_file)
  execute_cmd_with_redirect(STATE_STORE_PATH, STATE_STORE_ARGS, output_file)

def start_mini_impala_cluster(cluster_size):
  output_file = os.path.join(options.log_dir, 'mini-impala-cluster.out')
  args = "--num_backends=%d" % cluster_size
  print "Starting Mini Impala Cluster with logging to %s" % (output_file)
  execute_cmd_with_redirect(
      '. %s;%s' % (SET_CLASSPATH_SCRIPT_PATH, MINI_IMPALA_CLUSTER_PATH), args, output_file)

def start_impalad_instances(cluster_size):
  BASE_FE_PORT = 21000
  BASE_BE_PORT = 22000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 9090

  # Start each impalad instance and optionally redirect the output to a log file.
  for i in range(options.cluster_size):
    output_file = os.path.join(options.log_dir, 'impalad.node%d.out' % i)
    print "Starting ImpalaD %d logging to %s" % (i, output_file)
    args = IMPALAD_ARGS % (BASE_FE_PORT + i, BASE_BE_PORT + i,
                           BASE_STATE_STORE_SUBSCRIBER_PORT + i, BASE_WEBSERVER_PORT + i)
    execute_cmd_with_redirect(IMPALAD_PATH, args, output_file)

def execute_cmd_with_redirect(cmd, args, output_file):
  redirect_str = '' if options.verbose else REDIRECT_STR % {'file_name': output_file}
  os.system("%s %s %s &" % (cmd, args, redirect_str))

def wait_for_cluster(timeout_in_seconds=DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Checks if the cluster is "ready" by executing a simple query in a loop"""
  start_time = time()
  while os.system('%s -i localhost:21000 -q "%s"' %  (IMPALA_SHELL, 'select 1')) != 0:
    if time() - timeout_in_seconds > start_time:
      raise RuntimeError, 'Cluster did not start within %d seconds' % timeout_in_seconds
    print 'Cluster not yet available. Sleeping...'
    sleep(2)

if __name__ == "__main__":
  if options.build_type not in KNOWN_BUILD_TYPES:
    print 'Invalid build type %s. Valid values: %s' % (options.build_type,
                                                       ', '.join(KNOWN_BUILD_TYPES))
    sys.exit(1)

  if options.cluster_size <= 0:
    print 'Please specify a cluster size > 0'
    sys.exit(1)

  kill_all()
  if not options.kill_only:
    if options.inprocess:
      start_mini_impala_cluster(options.cluster_size)
    else:
      start_statestore()
      start_impalad_instances(options.cluster_size)
    if options.wait_for_cluster:
      wait_for_cluster()

    print 'ImpalaD Cluster Running with %d nodes.' % options.cluster_size
