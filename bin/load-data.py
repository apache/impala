#!/usr/bin/env impala-python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to load the proper datasets for the specified workloads. It loads
# all data via Hive except for parquet data which needs to be loaded via Impala.
# Most ddl commands are executed by Impala.
import collections
import os
import re
import sqlparse
import subprocess
import sys
import tempfile
import time
import getpass
from itertools import product
from optparse import OptionParser
from Queue import Queue
from tests.beeswax.impala_beeswax import *
from threading import Thread

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
parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                  help="Impala daemon to connect to")
parser.add_option("--hive_hs2_hostport", dest="hive_hs2_hostport",
                  default="localhost:11050",
                  help="HS2 host:Port to issue Hive queries against using beeline")
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
parser.add_option("--use_kerberos", action="store_true", default=False,
                  help="Load data on a kerberized cluster.")
parser.add_option("--principal", default=None, dest="principal",
                  help="Kerberos service principal, required if --use_kerberos is set")

options, args = parser.parse_args()

DATA_LOAD_DIR = '/tmp/data-load-files'
WORKLOAD_DIR = options.workload_dir
DATASET_DIR = options.dataset_dir
TESTDATA_BIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin')
AVRO_SCHEMA_DIR = "avro_schemas"

GENERATE_SCHEMA_CMD = "generate-schema-statements.py --exploration_strategy=%s "\
                      "--workload=%s --scale_factor=%s --verbose"
# Load data using Hive's beeline because the Hive shell has regressed (CDH-17222).
# The Hive shell is stateful, meaning that certain series of actions lead to problems.
# Examples of problems due to the statefullness of the Hive shell:
# - Creating an HBase table changes the replication factor to 1 for subsequent LOADs.
# - INSERTs into an HBase table fail if they are the first stmt executed in a session.
# However, beeline itself also has bugs. For example, inserting a NULL literal into
# a string-typed column leads to an NPE. We work around these problems by using LOAD from
# a datafile instead of doing INSERTs.
HIVE_CMD = os.path.join(os.environ['HIVE_HOME'], 'bin/beeline')

hive_auth = "auth=none"
if options.use_kerberos:
  if not options.principal:
    print "--principal is required when --use_kerberos is specified"
    exit(1)
  hive_auth = "principal=" + options.principal

HIVE_ARGS = '-n %s -u "jdbc:hive2://%s/default;%s" --verbose=true'\
      % (getpass.getuser(), options.hive_hs2_hostport, hive_auth)

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

def exec_cmd(cmd, error_msg, exit_on_error=True):
  ret_val = -1
  try:
    ret_val = subprocess.call(cmd, shell=True)
  except Exception as e:
    error_msg = "%s: %s" % (error_msg, str(e))
  finally:
    if ret_val != 0:
      print error_msg
      if exit_on_error: sys.exit(ret_val)
  return ret_val

def exec_hive_query_from_file(file_name):
  if not os.path.exists(file_name): return
  hive_cmd = "%s %s -f %s" % (HIVE_CMD, HIVE_ARGS, file_name)
  print 'Executing Hive Command: %s' % hive_cmd
  exec_cmd(hive_cmd,  'Error executing file from Hive: ' + file_name)

def exec_hbase_query_from_file(file_name):
  if not os.path.exists(file_name): return
  hbase_cmd = "hbase shell %s" % file_name
  print 'Executing HBase Command: %s' % hbase_cmd
  exec_cmd(hbase_cmd, 'Error executing hbase create commands')

# KERBEROS TODO: fails when kerberized and impalad principal isn't "impala"
def exec_impala_query_from_file(file_name):
  """Execute each query in an Impala query file individually"""
  is_success = True
  impala_client = ImpalaBeeswaxClient(options.impalad, use_kerberos=options.use_kerberos)
  try:
    impala_client.connect()
    with open(file_name, 'r+') as query_file:
      queries = sqlparse.split(query_file.read())
    for query in queries:
      query = sqlparse.format(query.rstrip(';'), strip_comments=True)
      print '(%s):\n%s\n' % (file_name, query.strip())
      result = impala_client.execute(query)
  except Exception as e:
    print "Data Loading from Impala failed with error: %s" % str(e)
    is_success = False
  finally:
    impala_client.close_connection()
  return is_success

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

def exec_hadoop_fs_cmd(args, exit_on_error=True):
  cmd = "%s fs %s" % (HADOOP_CMD, args)
  print "Executing Hadoop command: " + cmd
  exec_cmd(cmd, "Error executing Hadoop command, exiting",
      exit_on_error=exit_on_error)

def exec_impala_query_from_file_parallel(query_files):
  # Get the name of the query file that loads the base tables, if it exists.
  # TODO: Find a better way to detect the file that loads the base tables.
  create_base_table_file = next((q for q in query_files if 'text' in q), None)
  if create_base_table_file:
    is_success = exec_impala_query_from_file(create_base_table_file)
    query_files.remove(create_base_table_file)
    # If loading the base tables failed, exit with a non zero error code.
    if not is_success: sys.exit(1)
  if not query_files: return
  threads = []
  result_queue = Queue()
  for query_file in query_files:
    thread = Thread(target=lambda x: result_queue.put(exec_impala_query_from_file(x)),
        args=[query_file])
    thread.daemon = True
    threads.append(thread)
    thread.start()
  # Keep looping until the number of results retrieved is the same as the number of
  # threads spawned, or until a data loading query fails. result_queue.get() will
  # block until a result is available in the queue.
  num_fetched_results = 0
  while num_fetched_results < len(threads):
    success = result_queue.get()
    num_fetched_results += 1
    if not success: sys.exit(1)
  # There is a small window where a thread may still be alive even if all the threads have
  # finished putting their results in the queue.
  for thread in threads: thread.join()

def invalidate_impala_metadata():
  print "Invalidating Metadata"
  impala_client = ImpalaBeeswaxClient(options.impalad, use_kerberos=options.use_kerberos)
  impala_client.connect()
  try:
    impala_client.execute('invalidate metadata')
  finally:
    impala_client.close_connection()

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
    generate_schema_statements(workload)
    assert os.path.isdir(os.path.join(DATA_LOAD_DIR, dataset)), ("Data loading files "
        "do not exist for (%s)" % dataset)
    os.chdir(os.path.join(DATA_LOAD_DIR, dataset))
    copy_avro_schemas_to_hdfs(AVRO_SCHEMA_DIR)
    dataset_dir_contents = os.listdir(os.getcwd())
    load_file_substr = "%s-%s" % (workload, options.exploration_strategy)
    # Data loading with Impala is done in parallel, each file format has a separate query
    # file.
    create_filename = '%s-impala-generated' % load_file_substr
    load_filename = '%s-impala-load-generated' % load_file_substr
    impala_create_files = [f for f in dataset_dir_contents if create_filename in f]
    impala_load_files = [f for f in dataset_dir_contents if load_filename in f]

    # Execute the data loading scripts.
    # Creating tables in Impala has no dependencies, so we execute them first.
    # HBase table inserts are done via hive, so the hbase tables need to be created before
    # running the hive script. Some of the Impala inserts depend on hive tables,
    # so they're done at the end. Finally, the Hbase Tables that have been filled with data
    # need to be flushed.
    exec_impala_query_from_file_parallel(impala_create_files)
    exec_hbase_query_from_file('load-%s-hbase-generated.create' % load_file_substr)
    exec_hive_query_from_file('load-%s-hive-generated.sql' % load_file_substr)
    exec_hbase_query_from_file('post-load-%s-hbase-generated.sql' % load_file_substr)

    if impala_load_files: invalidate_impala_metadata()
    exec_impala_query_from_file_parallel(impala_load_files)
    loading_time_map[workload] = time.time() - start_time

  invalidate_impala_metadata()
  total_time = 0.0
  for workload, load_time in loading_time_map.iteritems():
    total_time += load_time
    print 'Data loading for workload \'%s\' completed in: %.2fs'\
        % (workload, load_time)
  print 'Total load time: %.2fs\n' % total_time
