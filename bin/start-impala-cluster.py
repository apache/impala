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
import json
import socket
import sys
import urllib2
from time import sleep, time
from optparse import OptionParser

#log4j settings for impala
log4j_impala_properties='''log.threshold=INFO
main.logger=FA
impala.root.logger=${log.threshold},${main.logger}
log4j.rootLogger=${impala.root.logger}
log.dir=%s
log.file=%s.INFO
'''
#log4j appender settings
log4j_appender_properties='''log4j.appender.FA=org.apache.log4j.FileAppender
log4j.appender.FA.File=${log.dir}/${log.file}
log4j.appender.FA.layout=org.apache.log4j.PatternLayout
log4j.appender.FA.layout.ConversionPattern=%p%d{MMdd HH:mm:ss.SSS'000'} %t %c] %m%n
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.target=System.err
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n
'''

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
parser.add_option("--kill", "--kill_only", dest="kill_only", action="store_true",
                  default=False, help="Instead of starting the cluster, \
                  just kill all running Impalad and State Store processes.")
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
                   help="Set the impalad logging level")
options, args = parser.parse_args()

# constants
IMPALA_HOME = os.environ['IMPALA_HOME']
KNOWN_BUILD_TYPES = ['debug', 'release']
IMPALAD_PATH = os.path.join(IMPALA_HOME,
                            'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(IMPALA_HOME, 'be/build', options.build_type,
                                'statestore/statestored')
MINI_IMPALA_CLUSTER_PATH = os.path.join(IMPALA_HOME, 'be/build', options.build_type,
                                        'testutil/mini-impala-cluster')
IMPALA_SHELL = os.path.join(IMPALA_HOME, 'bin/impala-shell.sh')
SET_CLASSPATH_SCRIPT_PATH = os.path.join(IMPALA_HOME, 'bin/set-classpath.sh')
IMPALAD_ARGS = ("-beeswax_port=%d -hs2_port=%d "
                "-be_port=%d -state_store_subscriber_port=%d "
                "-webserver_port=%d -log_filename=%s -log_dir=%s -v=%d"
                + options.impalad_args)
DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS = 120
LOG4J_PROPERTIES_DIR = os.path.join(options.log_dir, 'log4j_properties_%d')
STATE_STORE_ARGS = ' -log_filename=%s -log_dir=%s ' + options.state_store_args
STATESTORE_WEBPAGE = "http://%s:25010/jsonmetrics"

# Helper functions
def get_log_file_name(service_name):
  return os.path.join(options.log_dir, '%s.INFO' % service_name)

def get_statestore_webpage():
  """Connect to the statestore webpage.

  Attempt to open a connection to the statestore webpage. Retry till a connection
  is achieved, or the timeout is hit.
  """
  start_time = time()
  while (time() - start_time) < DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS:
    try:
      statestore_webpage = urllib2.urlopen(STATESTORE_WEBPAGE % (hostname,))
      return statestore_webpage
    except Exception, e:
      print 'Retrying loading the statestore webpage'
      sleep(2)
  # the statestore did not start, terminate cluster start
  raise RuntimeError, 'The statestore did not start'

def setup_log4j(log4j_text, log4j_dir):
  """Create a custom log4j.properties file for each impalad

  The log4j.properties file uses the same template as CM. It points to the logile used
  by GLOG. This coaleses the logs into one file, making it consistent with
  CM based production enviroments.
  """
  # cleanup the old dir
  os.system('rm -rf %s' % log4j_dir)
  os.system('mkdir -p %s' % log4j_dir)
  try:
    f = open('%s/log4j.properties' % log4j_dir, 'w')
    log4j_text = log4j_text + log4j_appender_properties
    f.write(log4j_text)
  finally:
    f.close()

def execute_cmd_with_redirect(cmd, args, log_file_name, log4j=None, mini=False):
  classpath_prefix = ''
  redirect_str = ''
  if options.verbose:
    redirect_str = '-logtostderr=1 ' + redirect_str
  elif mini:
    redirect_str = ' > %s' % log_file_name

  if log4j:
    setup_log4j(*log4j)
    classpath_prefix = '-classpath_prefix=%s' % log4j[-1]
  cmd = '%s %s %s %s 2>&1 &' % (cmd, classpath_prefix, args, redirect_str)
  os.system(cmd)

# start/kill commands
def kill_all():
  os.system("killall mini-impala-cluster")
  os.system("killall impalad")
  os.system("killall statestored")

def start_statestore():
  log_file_name = get_log_file_name('statestored')
  print "Starting State Store with logging to %s" % (log_file_name)
  args = STATE_STORE_ARGS % ('statestored', options.log_dir)
  execute_cmd_with_redirect(STATE_STORE_PATH, args, log_file_name)

def start_mini_impala_cluster(cluster_size):
  log_file_name = get_log_file_name('mini-impala-cluster')
  args = "--num_backends=%d" % cluster_size
  print "Starting Mini Impala Cluster with logging to %s" % (log_file_name)
  cmd = '. %s;%s' % (SET_CLASSPATH_SCRIPT_PATH, MINI_IMPALA_CLUSTER_PATH)
  execute_cmd_with_redirect(cmd, args, log_file_name, mini=True)

def start_impalad_instances(cluster_size):
  BASE_BEESWAX_PORT = 21000
  BASE_HS2_PORT = 21050
  BASE_BE_PORT = 22000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 25000

  # Start each impalad instance and optionally redirect the output to a log file.
  for i in range(options.cluster_size):
    # The first impalad always logs to impalad.INFO
    if i == 0:
      service_name = 'impalad'
    else:
      service_name = 'impalad_node%s' % i
    log_file_name = get_log_file_name(service_name)
    log4j_impala_prop = log4j_impala_properties % (options.log_dir, service_name)
    log4j_prop_dir = LOG4J_PROPERTIES_DIR % i
    print "Starting ImpalaD %d logging to %s" % (i, log_file_name)
    args = IMPALAD_ARGS % (BASE_BEESWAX_PORT + i, BASE_HS2_PORT + i, BASE_BE_PORT + i,
                           BASE_STATE_STORE_SUBSCRIBER_PORT + i, BASE_WEBSERVER_PORT + i,
                           service_name, options.log_dir, options.log_level)
    execute_cmd_with_redirect(IMPALAD_PATH, args, log_file_name,
                              log4j=(log4j_impala_prop, log4j_prop_dir))

def wait_for_cluster(timeout_in_seconds=DEFAULT_CLUSTER_WAIT_TIMEOUT_IN_SECONDS):
  """Checks if the cluster is "ready" by polling the statestore metrics page."""
  timeout_in_seconds = float(timeout_in_seconds)
  start = time()
  while (time() - start) < timeout_in_seconds:
    statestore_webpage = get_statestore_webpage()
    info = json.loads(statestore_webpage.read())
    num_backends = info['statestore.live-backends']
    if int(num_backends) == options.cluster_size:
      print 'All impalads started'
      return
    sleep(2)
    print 'Waiting for all impalads to start...'
  raise RuntimeError, 'All impalads did not start within the specified timeout'

if __name__ == "__main__":

  if options.kill_only:
    kill_all()
    sys.exit(0)

  if options.build_type not in KNOWN_BUILD_TYPES:
    print 'Invalid build type %s' % options.build_type
    print 'Valid values: %s' % ', '.join(KNOWN_BUILD_TYPES)
    sys.exit(1)

  if options.cluster_size <= 0:
    print 'Please specify a cluster size > 0'
    sys.exit(1)

  # Kill existing processes.
  kill_all()
  # get the hostname dynamically.
  hostname = socket.gethostname()
  # urllib2 in python2.4 does not parse timeout, we can set it globally
  # for _all_ requests.
  socket.setdefaulttimeout(2)
  if options.inprocess:
    start_mini_impala_cluster(options.cluster_size)
  else:
    try:
      start_statestore()
      start_impalad_instances(options.cluster_size)
      wait_for_cluster()
    except Exception, e:
      print 'Cluster start aborted: %s' % e
      # cleanup
      kill_all()
      sys.exit(1)

  print 'ImpalaD Cluster Running with %d nodes.' % options.cluster_size
