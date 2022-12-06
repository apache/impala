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
#
# This script is used to load the proper datasets for the specified workloads. It loads
# all data via Hive except for parquet data which needs to be loaded via Impala.
# Most ddl commands are executed by Impala.
from __future__ import absolute_import, division, print_function
import collections
import getpass
import logging
import multiprocessing
import os
import re
import sqlparse
import subprocess
import sys
import time
import traceback

from optparse import OptionParser
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient
from multiprocessing.pool import ThreadPool

LOG = logging.getLogger('load-data.py')

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
parser.add_option("--impalad", dest="impalad", default="localhost",
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
parser.add_option("--num_processes", type="int", default=multiprocessing.cpu_count(),
                  dest="num_processes", help="Number of parallel processes to use.")

options, args = parser.parse_args()

SQL_OUTPUT_DIR = os.environ['IMPALA_DATA_LOADING_SQL_DIR']
WORKLOAD_DIR = options.workload_dir
DATASET_DIR = options.dataset_dir
TESTDATA_BIN_DIR = os.path.join(os.environ['IMPALA_HOME'], 'testdata/bin')
AVRO_SCHEMA_DIR = "avro_schemas"

GENERATE_SCHEMA_CMD = "generate-schema-statements.py --exploration_strategy=%s "\
                      "--workload=%s --scale_factor=%s --verbose"
# Load data using Hive's beeline because the Hive shell has regressed (HIVE-5515).
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
    print("--principal is required when --use_kerberos is specified")
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
      LOG.error('Workload \'%s\' not found in workload directory' % workload)
      LOG.error('Available workloads: ' + ', '.join(all_workloads))
      sys.exit(1)

def exec_cmd(cmd, error_msg=None, exit_on_error=True, out_file=None):
  """Run the given command in the shell returning whether the command
     succeeded. If 'error_msg' is set, log the error message on failure.
     If 'exit_on_error' is True, exit the program on failure.
     If 'out_file' is specified, log all output to that file."""
  success = True
  if out_file:
    with open(out_file, 'w') as f:
      ret_val = subprocess.call(cmd, shell=True, stderr=f, stdout=f)
  else:
    ret_val = subprocess.call(cmd, shell=True)
  if ret_val != 0:
    if error_msg: LOG.info(error_msg)
    if exit_on_error: sys.exit(ret_val)
    success = False
  return success

def exec_hive_query_from_file_beeline(file_name):
  if not os.path.exists(file_name):
    LOG.info("Error: File {0} not found".format(file_name))
    return False

  LOG.info("Beginning execution of hive SQL: {0}".format(file_name))

  output_file = file_name + ".log"
  hive_cmd = "{0} {1} -f {2}".format(HIVE_CMD, HIVE_ARGS, file_name)
  is_success = exec_cmd(hive_cmd, exit_on_error=False, out_file=output_file)

  if is_success:
    LOG.info("Finished execution of hive SQL: {0}".format(file_name))
  else:
    LOG.info("Error executing hive SQL: {0} See: {1}".format(file_name, \
             output_file))

  return is_success


def exec_hbase_query_from_file(file_name, step_name):
  if not os.path.exists(file_name): return
  LOG.info('Begin step "%s".' % step_name)
  start_time = time.time()
  hbase_cmd = "hbase shell %s" % file_name
  LOG.info('Executing HBase Command: %s' % hbase_cmd)
  exec_cmd(hbase_cmd, error_msg='Error executing hbase create commands')
  total_time = time.time() - start_time
  LOG.info('End step "%s". Total time: %.2fs\n' % (step_name, total_time))


# KERBEROS TODO: fails when kerberized and impalad principal isn't "impala"
def exec_impala_query_from_file(file_name):
  """Execute each query in an Impala query file individually"""
  if not os.path.exists(file_name):
    LOG.info("Error: File {0} not found".format(file_name))
    return False

  LOG.info("Beginning execution of impala SQL on {0}: {1}".format(
           options.impalad, file_name))
  is_success = True
  impala_client = ImpalaBeeswaxClient(options.impalad, use_kerberos=options.use_kerberos)
  output_file = file_name + ".log"
  query = None
  with open(output_file, 'w') as out_file:
    try:
      impala_client.connect()
      with open(file_name, 'r+') as query_file:
        queries = sqlparse.split(query_file.read())
        for query in queries:
          query = sqlparse.format(query.rstrip(';'), strip_comments=True)
          if query.strip() != "":
            result = impala_client.execute(query)
            out_file.write("{0}\n{1}\n".format(query, result))
    except Exception as e:
      if query:
        out_file.write("ERROR: {0}\n".format(query))
      else:
        out_file.write("Encounter errors before parsing any queries.\n")
      traceback.print_exc(file=out_file)
      is_success = False

  if is_success:
    LOG.info("Finished execution of impala SQL: {0}".format(file_name))
  else:
    LOG.info("Error executing impala SQL: {0} See: {1}".format(file_name, \
             output_file))

  return is_success

def run_dataset_preload(dataset):
  """Execute a preload script if present in dataset directory. E.g. to generate data
  before loading"""
  dataset_preload_script = os.path.join(DATASET_DIR, dataset, "preload")
  if os.path.exists(dataset_preload_script):
    LOG.info("Running preload script for " + dataset)
    if options.scale_factor > 1:
      dataset_preload_script += " " + str(options.scale_factor)
    exec_cmd(dataset_preload_script, error_msg="Error executing preload script for " + dataset,
        exit_on_error=True)

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
  generate_cmd += " --backend=%s" % options.impalad
  LOG.info('Executing Generate Schema Command: ' + generate_cmd)
  schema_cmd = os.path.join(TESTDATA_BIN_DIR, generate_cmd)
  error_msg = 'Error generating schema statements for workload: ' + workload
  exec_cmd(schema_cmd, error_msg=error_msg)

def get_dataset_for_workload(workload):
  dimension_file_name = os.path.join(WORKLOAD_DIR, workload,
                                     '%s_dimensions.csv' % workload)
  if not os.path.isfile(dimension_file_name):
    LOG.error('Dimension file not found: ' + dimension_file_name)
    sys.exit(1)
  with open(dimension_file_name, 'rb') as input_file:
    match = re.search('dataset:\s*([\w\-\.]+)', input_file.read())
    if match:
      return match.group(1)
    else:
      LOG.error('Dimension file does not contain dataset for workload \'%s\'' % (workload))
      sys.exit(1)

def copy_avro_schemas_to_hdfs(schemas_dir):
  """Recursively copies all of schemas_dir to the test warehouse."""
  if not os.path.exists(schemas_dir):
    LOG.info('Avro schema dir (%s) does not exist. Skipping copy to HDFS.' % schemas_dir)
    return

  exec_hadoop_fs_cmd("-mkdir -p " + options.hive_warehouse_dir)
  exec_hadoop_fs_cmd("-put -f %s %s/" % (schemas_dir, options.hive_warehouse_dir))

def exec_hadoop_fs_cmd(args, exit_on_error=True):
  cmd = "%s fs %s" % (HADOOP_CMD, args)
  LOG.info("Executing Hadoop command: " + cmd)
  exec_cmd(cmd, error_msg="Error executing Hadoop command, exiting",
      exit_on_error=exit_on_error)


def exec_query_files_parallel(thread_pool, query_files, execution_type, step_name):
  """Executes the query files provided using the execution engine specified
     in parallel using the given thread pool. Aborts immediately if any execution
     encounters an error."""
  assert(execution_type == 'impala' or execution_type == 'hive')
  if len(query_files) == 0: return
  if execution_type == 'impala':
    execution_function = exec_impala_query_from_file
  elif execution_type == 'hive':
    execution_function = exec_hive_query_from_file_beeline

  LOG.info('Begin step "%s".' % step_name)
  start_time = time.time()
  for result in thread_pool.imap_unordered(execution_function, query_files):
    if not result:
      thread_pool.terminate()
      sys.exit(1)
  total_time = time.time() - start_time
  LOG.info('End step "%s". Total time: %.2fs\n' % (step_name, total_time))


def impala_exec_query_files_parallel(thread_pool, query_files, step_name):
  exec_query_files_parallel(thread_pool, query_files, 'impala', step_name)


def hive_exec_query_files_parallel(thread_pool, query_files, step_name):
  exec_query_files_parallel(thread_pool, query_files, 'hive', step_name)


def main():
  logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%H:%M:%S')
  LOG.setLevel(logging.DEBUG)

  # Having the actual command line at the top of each data-load-* log can help
  # when debugging dataload issues.
  #
  LOG.debug(' '.join(sys.argv))

  all_workloads = available_workloads(WORKLOAD_DIR)
  workloads = []
  if options.workloads is None:
    LOG.error("At least one workload name must be specified.")
    parser.print_help()
    sys.exit(1)
  elif options.workloads == 'all':
    LOG.info('Loading data for all workloads.')
    workloads = all_workloads
  else:
    workloads = options.workloads.split(",")
    validate_workloads(all_workloads, workloads)

  LOG.info('Starting data load for the following workloads: ' + ', '.join(workloads))
  LOG.info('Running with {0} threads'.format(options.num_processes))

  # Note: The processes are in whatever the caller's directory is, so all paths
  #       passed to the pool need to be absolute paths. This will allow the pool
  #       to be used for different workloads (and thus different directories)
  #       simultaneously.
  thread_pool = ThreadPool(processes=options.num_processes)
  loading_time_map = collections.defaultdict(float)
  for workload in workloads:
    start_time = time.time()
    dataset = get_dataset_for_workload(workload)
    run_dataset_preload(dataset)
    # This script is tightly coupled with testdata/bin/generate-schema-statements.py
    # Specifically, this script is expecting the following:
    # 1. generate-schema-statements.py generates files and puts them in the
    #    directory ${IMPALA_DATA_LOADING_SQL_DIR}/${workload}
    #    (e.g. ${IMPALA_HOME}/logs/data_loading/sql/tpch)
    # 2. generate-schema-statements.py populates the subdirectory
    #    avro_schemas/${workload} with JSON files specifying the Avro schema for the
    #    tables being loaded.
    # 3. generate-schema-statements.py uses a particular naming scheme to distinguish
    #    between SQL files of different load phases.
    #
    #    Using the following variables:
    #    workload_exploration = ${workload}-${exploration_strategy} and
    #    file_format_suffix = ${file_format}-${codec}-${compression_type}
    #
    #    A. Impala table creation scripts run in Impala to create tables, partitions,
    #       and views. There is one for each file format. They take the form:
    #       create-${workload_exploration}-impala-generated-${file_format_suffix}.sql
    #
    #    B. Hive creation/load scripts run in Hive to load data into tables and create
    #       tables or views that Impala does not support. There is one for each
    #       file format. They take the form:
    #       load-${workload_exploration}-hive-generated-${file_format_suffix}.sql
    #
    #    C. HBase creation script runs through the hbase commandline to create
    #       HBase tables. (Only generated if loading HBase table.) It takes the form:
    #       load-${workload_exploration}-hbase-generated.create
    #
    #    D. HBase postload script runs through the hbase commandline to flush
    #       HBase tables. (Only generated if loading HBase table.) It takes the form:
    #       post-load-${workload_exploration}-hbase-generated.sql
    #
    #    E. Impala load scripts run in Impala to load data. Only Parquet and Kudu
    #       are loaded through Impala. There is one for each of those formats loaded.
    #       They take the form:
    #       load-${workload_exploration}-impala-generated-${file_format_suffix}.sql
    #
    #    F. Invalidation script runs through Impala to invalidate/refresh metadata
    #       for tables. It takes the form:
    #       invalidate-${workload_exploration}-impala-generated.sql
    generate_schema_statements(workload)

    # Determine the directory from #1
    sql_dir = os.path.join(SQL_OUTPUT_DIR, dataset)
    assert os.path.isdir(sql_dir),\
      ("Could not find the generated SQL files for loading dataset '%s'.\
        \nExpected to find the SQL files in: %s" % (dataset, sql_dir))

    # Copy the avro schemas (see #2) into HDFS
    avro_schemas_path = os.path.join(sql_dir, AVRO_SCHEMA_DIR)
    copy_avro_schemas_to_hdfs(avro_schemas_path)

    # List all of the files in the sql directory to sort out the various types of
    # files (see #3).
    dataset_dir_contents = [os.path.join(sql_dir, f) for f in os.listdir(sql_dir)]
    workload_exploration = "%s-%s" % (workload, options.exploration_strategy)

    # Remove the AVRO_SCHEMA_DIR from the list of files
    if os.path.exists(avro_schemas_path):
      dataset_dir_contents.remove(avro_schemas_path)

    # Match for Impala create files (3.A)
    impala_create_match = 'create-%s-impala-generated' % workload_exploration
    # Match for Hive create/load files (3.B)
    hive_load_match = 'load-%s-hive-generated' % workload_exploration
    # Match for HBase creation script (3.C)
    hbase_create_match = 'load-%s-hbase-generated.create' % workload_exploration
    # Match for HBase post-load script (3.D)
    hbase_postload_match = 'post-load-%s-hbase-generated.sql' % workload_exploration
    # Match for Impala load scripts (3.E)
    impala_load_match = 'load-%s-impala-generated' % workload_exploration
    # Match for Impala invalidate script (3.F)
    invalidate_match = 'invalidate-%s-impala-generated' % workload_exploration

    impala_create_files = []
    hive_load_text_files = []
    hive_load_orc_files = []
    hive_load_nontext_files = []
    hbase_create_files = []
    hbase_postload_files = []
    impala_load_files = []
    invalidate_files = []
    for filename in dataset_dir_contents:
      if impala_create_match in filename:
        impala_create_files.append(filename)
      elif hive_load_match in filename:
        if 'text-none-none' in filename:
          hive_load_text_files.append(filename)
        elif 'orc-def-block' in filename:
          hive_load_orc_files.append(filename)
        else:
          hive_load_nontext_files.append(filename)
      elif hbase_create_match in filename:
        hbase_create_files.append(filename)
      elif hbase_postload_match in filename:
        hbase_postload_files.append(filename)
      elif impala_load_match in filename:
        impala_load_files.append(filename)
      elif invalidate_match in filename:
        invalidate_files.append(filename)
      else:
        assert False, "Unexpected input file {0}".format(filename)

    # Simple helper function to dump a header followed by the filenames
    def log_file_list(header, file_list):
      if (len(file_list) == 0): return
      LOG.debug(header)
      list(map(LOG.debug, list(map(os.path.basename, file_list))))
      LOG.debug("\n")

    log_file_list("Impala Create Files:", impala_create_files)
    log_file_list("Hive Load Text Files:", hive_load_text_files)
    log_file_list("Hive Load Orc Files:", hive_load_orc_files)
    log_file_list("Hive Load Non-Text Files:", hive_load_nontext_files)
    log_file_list("HBase Create Files:", hbase_create_files)
    log_file_list("HBase Post-Load Files:", hbase_postload_files)
    log_file_list("Impala Load Files:", impala_load_files)
    log_file_list("Impala Invalidate Files:", invalidate_files)

    # Execute the data loading scripts.
    # Creating tables in Impala has no dependencies, so we execute them first.
    # HBase table inserts are done via hive, so the hbase tables need to be created before
    # running the hive scripts. Some of the Impala inserts depend on hive tables,
    # so they're done at the end. Finally, the Hbase Tables that have been filled with data
    # need to be flushed.

    impala_exec_query_files_parallel(thread_pool, impala_create_files, "Impala Create")

    # There should be at most one hbase creation script
    assert(len(hbase_create_files) <= 1)
    for hbase_create in hbase_create_files:
      exec_hbase_query_from_file(hbase_create, "HBase Create")

    # If this is loading text tables plus multiple other formats, the text tables
    # need to be loaded first
    assert(len(hive_load_text_files) <= 1)
    hive_exec_query_files_parallel(thread_pool, hive_load_text_files, "Hive Load Text")
    # IMPALA-9923: Run ORC serially separately from other non-text formats. This hacks
    # around flakiness seen when loading this in parallel (see IMPALA-12630 comments for
    # broken tests). This should be removed as soon as possible.
    assert(len(hive_load_orc_files) <= 1)
    hive_exec_query_files_parallel(thread_pool, hive_load_orc_files, "Hive Load ORC")

    # Load all non-text formats (goes parallel)
    hive_exec_query_files_parallel(thread_pool, hive_load_nontext_files,
        "Hive Load Non-Text")

    assert(len(hbase_postload_files) <= 1)
    for hbase_postload in hbase_postload_files:
      exec_hbase_query_from_file(hbase_postload, "HBase Post-Load")

    # Invalidate so that Impala sees the loads done by Hive before loading Parquet/Kudu
    # Note: This only invalidates tables for this workload.
    assert(len(invalidate_files) <= 1)
    if impala_load_files:
      impala_exec_query_files_parallel(thread_pool, invalidate_files,
          "Impala Invalidate 1")
      impala_exec_query_files_parallel(thread_pool, impala_load_files, "Impala Load")
    # Final invalidate for this workload
    impala_exec_query_files_parallel(thread_pool, invalidate_files, "Impala Invalidate 2")
    loading_time_map[workload] = time.time() - start_time

  total_time = 0.0
  thread_pool.close()
  thread_pool.join()
  for workload, load_time in loading_time_map.items():
    total_time += load_time
    LOG.info('Data loading for workload \'%s\' completed in: %.2fs'\
        % (workload, load_time))
  LOG.info('Total load time: %.2fs\n' % total_time)

if __name__ == "__main__": main()
