#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to load the proper datasets for the specified workloads. It loads
# all data via Hive except for Trevni data which needs to be loaded via Impala.
import collections
import os
import re
import subprocess
import sys
import tempfile
import time
from itertools import product
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-e", "--exploration_strategy", dest="exploration_strategy",
                  default="core",
                  help="The exploration strategy for schema gen: 'core', "\
                  "'pairwise', or 'exhaustive'")
parser.add_option("--hive_warehouse_dir", dest="hive_warehouse_dir",
                  help="The HDFS path to the base Hive test warehouse directory")
parser.add_option("-w", "--workloads", dest="workloads",
                  help="Comma-separated list of workloads to load data for. If 'all' is "\
                       "specified then data for all workloads is loaded.")
parser.add_option("-s", "--scale_factor", dest="scale_factor", default="",
                  help="An optional scale factor to generate the schema for")
parser.add_option("-f", "--force_reload", dest="force_reload", action="store_true",
                  default=False, help='Skips HDFS exists check and reloads all tables')
parser.add_option("--compute_stats", dest="compute_stats", action="store_true",
                  default= False, help="Execute COMPUTE STATISTICS statements on the "\
                  "tables that are loaded")
parser.add_option("--impalad", dest="impala_shell_args", default="localhost:21000",
                  help="Impala daemon to connect to")

(options, args) = parser.parse_args()

WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
DATASET_DIR = os.environ['IMPALA_DATASET_DIR']
TESTDATA_BIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin')

GENERATE_SCHEMA_CMD = "generate-schema-statements.py --exploration_strategy=%s "\
                      "--workload=%s --scale_factor=%s --verbose"
HIVE_CMD = os.path.join(os.environ['HIVE_HOME'], 'bin/hive')
HIVE_ARGS = "-hiveconf hive.root.logger=WARN,console -v"

IMPALA_SHELL_CMD = os.path.join(os.environ['IMPALA_HOME'], 'bin/impala-shell.sh')

def available_workloads(workload_dir):
  return [subdir for subdir in os.listdir(workload_dir)
            if os.path.isdir(os.path.join(workload_dir, subdir))]

def validate_workloads(all_workloads, workloads):
  for workload in workloads:
    if workload not in all_workloads:
      print 'Workload \'%s\' not found in workload directory' % workload
      print 'Available workloads: ' + ', '.join(all_workloads)
      sys.exit(1)

def exec_hive_query_from_file(file_name):
  hive_cmd = "%s %s -f %s" % (HIVE_CMD, HIVE_ARGS, file_name)
  print 'Executing Hive Command: ' + hive_cmd
  ret_val = subprocess.call(hive_cmd, shell = True)
  if ret_val != 0:
    print 'Error executing file from Hive: ' + file_name
    sys.exit(ret_val)

def exec_impala_query_from_file(file_name):
  impala_cmd = "%s --impalad=%s -f %s" % (IMPALA_SHELL_CMD, options.impala_shell_args, file_name)
  print 'Executing Impala Command: ' + impala_cmd
  ret_val = subprocess.call(impala_cmd, shell = True)
  if ret_val != 0:
    print 'Error executing file from Impala: ' + file_name
    sys.exit(ret_val)

def exec_bash_script(file_name):
  bash_cmd = "bash %s" % file_name
  print 'Executing Bash Command: ' + bash_cmd
  ret_val = subprocess.call(bash_cmd, shell = True)
  if ret_val != 0:
    print 'Error bash script: ' + file_name
    sys.exit(ret_val)

def generate_schema_statements(workload):
  generate_cmd = GENERATE_SCHEMA_CMD % (options.exploration_strategy, workload,
                                        options.scale_factor)
  if options.force_reload:
    generate_cmd += " --force_reload"
  if options.hive_warehouse_dir is not None:
    generate_cmd += " --hive_warehouse_dir=%s" % options.hive_warehouse_dir
  if not options.compute_stats:
    generate_cmd += " --skip_compute_stats"
  print 'Executing Generate Schema Command: ' + generate_cmd
  ret_val = subprocess.call(os.path.join(TESTDATA_BIN_DIR, generate_cmd), shell = True)
  if ret_val != 0:
    print 'Error generating schema statements for workload: ' + workload
    sys.exit(ret_val)

def get_dataset_for_workload(workload):
  dimension_file_name = os.path.join(WORKLOAD_DIR, workload,
                                     '%s_dimensions.csv' % workload)
  if not os.path.isfile(dimension_file_name):
    print 'Dimension file not found: ' + dimension_file_name
    sys.exit(1)
  with open(dimension_file_name, 'rb') as input_file:
    match = re.search('dataset:\s*([\w\-\.]+)', input_file.read())
    if match:
      return match.group(1)
    else:
      print 'Dimension file does not contain dataset for workload \'%s\'' % (workload)
      sys.exit(1)

all_workloads = available_workloads(WORKLOAD_DIR)
workloads = []
if options.workloads is None:
  print "At least one workload name must be specified."
  parser.print_help()
  sys.exit(1)
elif options.workloads == 'all':
  print 'Loading data for all workloads.'
  workloads = all_workloads
else:
  workloads = options.workloads.split(",")
  validate_workloads(all_workloads, workloads)

print 'Starting data load for the following workloads: ' + ', '.join(workloads)

loading_time_map = collections.defaultdict(float)
for workload in workloads:
  start_time = time.time()
  dataset = get_dataset_for_workload(workload)
  print "Dataset for workload '%s' is '%s'" % (workload, dataset)
  dataset_dir = os.path.join(DATASET_DIR, dataset)
  os.chdir(dataset_dir)
  generate_schema_statements(workload)
  exec_hive_query_from_file(os.path.join(dataset_dir,
     'load-%s-%s-generated.sql' % (workload, options.exploration_strategy)))

  exec_impala_query_from_file(os.path.join(dataset_dir,
     'load-trevni-%s-%s-generated.sql' % (workload, options.exploration_strategy)))
  loading_time_map[workload] = time.time() - start_time

total_time = 0.0
for workload, load_time in loading_time_map.iteritems():
  total_time += load_time
  print 'Data loading for workload \'%s\' completed in: %.2fs'\
      % (workload, load_time)
print 'Total load time: %.2fs\n' % total_time
