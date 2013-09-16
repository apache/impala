#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to load the proper datasets for the specified workloads. It loads
# all data via Hive except for parquet data which needs to be loaded via Impala.
# Most ddl commands are executed by Impala.
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
                  default="/test-warehouse",
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
parser.add_option("--table_names", dest="table_names", default=None,
                  help="Only load the specified tables - specified as a comma-seperated "\
                  "list of base table names")
parser.add_option("--table_formats", dest="table_formats", default=None,
                  help="Override the test vectors and load using the specified table "\
                  "formats. Ex. --table_formats=seq/snap/block,text/none")
parser.add_option("--hdfs_namenode", dest="hdfs_namenode", default="localhost:20500",
                  help="HDFS name node for Avro schema URLs, default localhost:20500")
parser.add_option("--workload_dir", dest="workload_dir",
                  default=os.environ['IMPALA_WORKLOAD_DIR'],
                  help="Directory that contains Impala workloads")
parser.add_option("--dataset_dir", dest="dataset_dir",
                  default=os.environ['IMPALA_DATASET_DIR'],
                  help="Directory that contains Impala datasets")
options, args = parser.parse_args()

WORKLOAD_DIR = options.workload_dir
DATASET_DIR = options.dataset_dir
TESTDATA_BIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin')
AVRO_SCHEMA_DIR = "avro_schemas"

GENERATE_SCHEMA_CMD = "generate-schema-statements.py --exploration_strategy=%s "\
                      "--workload=%s --scale_factor=%s --verbose"
HIVE_CMD = os.path.join(os.environ['HIVE_HOME'], 'bin/hive')
HIVE_ARGS = "-hiveconf hive.root.logger=WARN,console -v"
IMPALA_SHELL_CMD = os.path.join(os.environ['IMPALA_HOME'], 'bin/impala-shell.sh')
HADOOP_CMD = os.path.join(os.environ['HADOOP_HOME'], 'bin/hadoop')

def available_workloads(workload_dir):
  return [subdir for subdir in os.listdir(workload_dir)
            if os.path.isdir(os.path.join(workload_dir, subdir))]

def validate_workloads(all_workloads, workloads):
  for workload in workloads:
    if workload not in all_workloads:
      print 'Workload \'%s\' not found in workload directory' % workload
      print 'Available workloads: ' + ', '.join(all_workloads)
      sys.exit(1)

def exec_cmd(cmd, error_msg, expect_success=True):
  ret_val = subprocess.call(cmd, shell=True)
  if expect_success and ret_val != 0:
    print error_msg
    sys.exit(ret_val)
  return ret_val

def exec_hive_query_from_file(file_name):
  hive_cmd = "%s %s -f %s" % (HIVE_CMD, HIVE_ARGS, file_name)
  print 'Executing Hive Command: ' + hive_cmd
  exec_cmd(hive_cmd,  'Error executing file from Hive: ' + file_name)

def exec_hbase_query_from_file(file_name):
  hbase_cmd = "hbase shell %s" % file_name
  exec_cmd(hbase_cmd, 'Error executing hbase create commands')

def exec_impala_query_from_file(file_name):
  impala_refresh_cmd = "%s --impalad=%s -q 'invalidate metadata'" %\
      (IMPALA_SHELL_CMD, options.impala_shell_args)
  impala_cmd = "%s --impalad=%s -f %s" %\
      (IMPALA_SHELL_CMD, options.impala_shell_args, file_name)
  # Refresh catalog before and after
  exec_cmd(impala_refresh_cmd, 'Error executing refresh from Impala.')
  print 'Executing Impala Command: ' + impala_cmd
  exec_cmd(impala_cmd, 'Error executing file from Impala: ' + file_name)
  exec_cmd(impala_refresh_cmd, 'Error executing refresh from Impala.')

def exec_bash_script(file_name):
  bash_cmd = "bash %s" % file_name
  print 'Executing Bash Command: ' + bash_cmd
  exec_cmd(bash_cmd, 'Error bash script: ' + file_name)

def generate_schema_statements(workload):
  generate_cmd = GENERATE_SCHEMA_CMD % (options.exploration_strategy, workload,
                                        options.scale_factor)
  if options.table_names:
    generate_cmd += " --table_names=%s" % options.table_names
  if options.force_reload:
    generate_cmd += " --force_reload"
  if options.table_formats:
    generate_cmd += " --table_formats=%s" % options.table_formats
  if options.hive_warehouse_dir is not None:
    generate_cmd += " --hive_warehouse_dir=%s" % options.hive_warehouse_dir
  if options.hdfs_namenode is not None:
    generate_cmd += " --hdfs_namenode=%s" % options.hdfs_namenode
  print 'Executing Generate Schema Command: ' + generate_cmd
  schema_cmd = os.path.join(TESTDATA_BIN_DIR, generate_cmd)
  error_msg = 'Error generating schema statements for workload: ' + workload
  exec_cmd(schema_cmd, error_msg)

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

def copy_avro_schemas_to_hdfs(schemas_dir):
  """Recursively copies all of schemas_dir to the test warehouse."""
  if not os.path.exists(schemas_dir):
    print 'Avro schema dir (%s) does not exist. Skipping copy to HDFS.' % schemas_dir
    return

  exec_hadoop_fs_cmd("-mkdir -p " + options.hive_warehouse_dir)
  exec_hadoop_fs_cmd("-put -f %s %s/" % (schemas_dir, options.hive_warehouse_dir))

def exec_hadoop_fs_cmd(args, expect_success=True):
  cmd = "%s fs %s" % (HADOOP_CMD, args)
  print "Executing Hadoop command: " + cmd
  exec_cmd(cmd, "Error executing Hadoop command, exiting",
      expect_success=expect_success)

if __name__ == "__main__":
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
    dataset_dir = os.path.join(DATASET_DIR, dataset)
    os.chdir(dataset_dir)
    generate_schema_statements(workload)

    copy_avro_schemas_to_hdfs(AVRO_SCHEMA_DIR)

    generated_hbase_file = 'load-%s-%s-hbase.create' % (workload,
                                                        options.exploration_strategy)
    if os.path.exists(generated_hbase_file):
      exec_hbase_query_from_file(os.path.join(dataset_dir, generated_hbase_file))

    generated_impala_file = \
        'load-%s-%s-impala-generated.sql' % (workload, options.exploration_strategy)
    if os.path.exists(generated_impala_file):
      exec_impala_query_from_file(os.path.join(dataset_dir, generated_impala_file))

    generated_hive_file =\
        'load-%s-%s-hive-generated.sql' % (workload, options.exploration_strategy)
    if os.path.exists(generated_hive_file):
      exec_hive_query_from_file(os.path.join(dataset_dir, generated_hive_file))

    generated_impala_file = \
        'load-%s-%s-impala-load-generated.sql' % (workload, options.exploration_strategy)
    if os.path.exists(generated_impala_file):
      exec_impala_query_from_file(os.path.join(dataset_dir, generated_impala_file))

    loading_time_map[workload] = time.time() - start_time

  total_time = 0.0
  for workload, load_time in loading_time_map.iteritems():
    total_time += load_time
    print 'Data loading for workload \'%s\' completed in: %.2fs'\
        % (workload, load_time)
  print 'Total load time: %.2fs\n' % total_time
