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
parser.add_option("--kill_only", dest="kill_only", action="store_true", default = False,
                  help="Instead of starting the cluster, just kill all running Impalad"\
                  " and State Store processes.")
parser.add_option("--log_dir", dest="log_dir", default="/tmp",
                  help="Directory to store output logs to.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default = False,
                  help="Prints all output to stderr/stdout.")
options, args = parser.parse_args()

KNOWN_BUILD_TYPES = ['debug', 'release']
IMPALAD_PATH = os.path.join(os.environ['IMPALA_HOME'],
                            'bin/start-impalad.sh -build_type=%s' % options.build_type)
STATE_STORE_PATH = os.path.join(os.environ['IMPALA_BE_DIR'], 'build', options.build_type,
                                'sparrow/state-store-service')
IMPALAD_ARGS = "-fe_port=%d -be_port=%d -state_store_subscriber_port=%d "\
               "-webserver_port=%d " + options.impalad_args
STATE_STORE_ARGS = options.state_store_args
REDIRECT_STR = "> %(file_name)s 2>&1"

def kill_all():
  os.system("killall impalad")
  os.system("killall state-store-service")

def start_statestore():
  output_file = os.path.join(options.log_dir, 'state-store-service.out')
  redirect_str = '' if options.verbose else REDIRECT_STR % {'file_name': output_file}
  print "Starting State Store with logging to %s" % (output_file)
  os.system("%s %s %s&" % (STATE_STORE_PATH, STATE_STORE_ARGS, redirect_str))

def start_impalad_instances(cluster_size):
  BASE_FE_PORT = 21000
  BASE_BE_PORT = 22000
  BASE_STATE_STORE_SUBSCRIBER_PORT = 23000
  BASE_WEBSERVER_PORT = 9090

  # Start each impalad instance and optionally redirect the output to a log file.
  for i in range(options.cluster_size):
    output_file = os.path.join(options.log_dir, 'impalad.node%d.out' % i)
    print "Starting ImpalaD %d logging to %s" % (i, output_file)
    redirect_str = '' if options.verbose else REDIRECT_STR % {'file_name': output_file}
    args = IMPALAD_ARGS % (BASE_FE_PORT + i, BASE_BE_PORT + i,
                           BASE_STATE_STORE_SUBSCRIBER_PORT + i, BASE_WEBSERVER_PORT + i)
    os.system("%s %s %s &" % (IMPALAD_PATH, args, redirect_str))

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
    start_statestore()
    start_impalad_instances(options.cluster_size)
    print 'ImpalaD Cluster Running with %d nodes.' % options.cluster_size
