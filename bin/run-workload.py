#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to run benchmark queries.  It runs the set queries specified in the
# given workload(s) under <workload name>/queries. This script will first try to warm the
# buffer cache before running the query. There is a command line options to control how
# many iterations to run each query.
#
# By default, the script will have minimal output.  Verbose output can be turned on with
# the -v option which will output the normal query output.  In addition, the -p option
# can be passed which will enable gprof instrumentation and output the sampled call
# stacks.  The -v and -p option are used by the perf regression tests.
#
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
#
# The planservice or ImpalaD needs to be running before executing any workload.
# Run with the --help option to see the arguments.
import csv
import logging
import math
import os
import sys
import subprocess
from query_executor import *
from collections import defaultdict
import threading
from optparse import OptionParser
from functools import partial
from os.path import isfile, isdir
from sys import exit
from time import sleep
import pickle
from random import choice

# Options
parser = OptionParser()
parser.add_option("-p", "--profiler", dest="profiler",
                  action="store_true", default = False,
                  help="If set, also run google pprof for sample profiling.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default = False, help="If set, outputs all benchmark diagnostics.")
parser.add_option("--remote", dest="remote", action="store_true",
                  default = False, help="Set to true if running on remote cluster.")
parser.add_option("--exploration_strategy", dest="exploration_strategy", default="core",
                  help="The exploration strategy to use for running benchmark: 'core', "\
                  "'pairwise', or 'exhaustive'")
parser.add_option("-w", "--workloads", dest="workloads", default="hive-benchmark",
                  help="The workload(s) and scale factors to run in a comma-separated "\
                  " list format. Optional scale factors for each workload are specified"\
                  " using colons. For example: -w tpch:1gb. "\
                  "Some valid workloads: 'hive-benchmark', 'tpch', ...")
parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                  help="A comma-separated list of impalad instances to run the "\
                  "workload against.")
parser.add_option("--runquery_path", dest="runquery_path",
                  default=os.path.join(os.environ['IMPALA_HOME'], 'bin/run-query.sh'),
                  help="The command to use for executing queries")
parser.add_option("--runquery_args", dest="runquery_args", default='',
                  help="Additional arguments to pass to runquery.")
parser.add_option("--exec_options", dest="exec_options", default='',
                  help="Runquery exec option string.")
parser.add_option("--compare_with_hive", dest="compare_with_hive", action="store_true",
                  default= False, help="Run all queries using Hive as well as Impala")
parser.add_option("--results_csv_file", dest="results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/benchmark_results.csv",
                  help="The output file where benchmark results are saved")
parser.add_option("--hive_results_csv_file", dest="hive_results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/hive_benchmark_results.csv",
                  help="The output file where Hive benchmark results are saved")
parser.add_option("--hive_cmd", dest="hive_cmd", default="hive -e",
                  help="The command to use for executing hive queries")
parser.add_option("-i", "--iterations", type="int", dest="iterations", default=5,
                  help="Number of times to run each query.")
parser.add_option("--prime_cache", dest="prime_cache", action="store_true",
                  default= False, help="Whether or not to prime the buffer cache. ")
parser.add_option("--num_clients", type="int", dest="num_clients", default=1,
                  help="Number of clients (threads) to use when executing each query.")
parser.add_option("--file_formats", dest="file_formats", default=None,
                  help="A comma-separated list of file fomats to execute. If not "\
                  "specified all file formats in the test vector will be run.")
parser.add_option("--compression_codecs", dest="compression_codecs", default=None,
                  help="A comma-separated list of compression codecs to execute. If not "\
                  "specified all compression codecs in the test vector will be run.")
parser.add_option("--skip_impala", dest="skip_impala", action="store_true",
                  default= False, help="If set, queries will only run against Hive.")
parser.add_option("--beeswax", dest="beeswax", action="store_true", default=False,
                  help="If set, Impala queries will use the beeswax interface.")

options, args = parser.parse_args()

# globals
TARGET_IMPALADS = options.impalad.split(",")
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
IMPALA_HOME = os.environ['IMPALA_HOME']
profile_output_file = os.path.join(IMPALA_HOME, 'be/build/release/service/profile.tmp')
dev_null = open('/dev/null')

# commands
runquery_cmd = "%(runquery)s %(args)s " % {'runquery' : options.runquery_path,
                                           'args' : options.runquery_args}

gprof_cmd  = 'google-pprof --text ' + options.runquery_path + ' %s | head -n 60'
prime_cache_cmd = os.path.join(IMPALA_HOME, "testdata/bin/cache_tables.py") + " -q \"%s\""

gprof_cmd = 'google-pprof --text ' + options.runquery_path + ' %s | head -n 60'

# TODO: Consider making cache_tables a module rather than directly calling the script
prime_cache_cmd = os.path.join(os.environ['IMPALA_HOME'],
                               "testdata/bin/cache_tables.py") + " -q \"%s\""

dev_null = open('/dev/null')

run_using_hive = options.compare_with_hive or options.skip_impala

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('run-benchmark')
if options.verbose:
  LOG.setLevel(level=logging.DEBUG)

class QueryExecutionDetail(object):
  def __init__(self, executor, workload, scale_factor, file_format, compression_codec,
               compression_type, execution_result):
    self.executor = executor
    self.workload = workload
    self.scale_factor = scale_factor
    self.file_format = file_format
    self.compression_codec = compression_codec
    self.compression_type = compression_type
    self.execution_result = execution_result


# Parse for the tables used in this query
def parse_tables(query):
  """
  Parse the tables used in this query.
  """
  table_predecessor = ['from', 'join']
  tokens = query.split(' ')
  tables = []
  next_is_table = 0
  for t in tokens:
    t = t.lower()
    if next_is_table == 1:
      tables.append(t)
      next_is_table = 0
    if t in table_predecessor:
      next_is_table = 1
  return tables

def prime_remote_or_local_cache(query, remote, hive=False):
  """
  Prime either the local cache or buffer cache for a remote machine.
  """
  if hive and remote:
    prime_buffer_cache_remote_hive(query)
  elif remote:
    prime_buffer_cache_remote_impala(query)
  else:
    prime_buffer_cache_local(query)

def prime_buffer_cache_remote_impala(query):
  """
  Prime the buffer cache on remote machines for impala.

  On remote clusters, we'll prime the buffer cache by just running count(*)
  TODO: does this work well enough? Does having a real cluster and data
  locality make this more deterministic?
  """
  tables = parse_tables(query)
  subprocess_call_partial = partial()
  for table in tables:
    count_cmd = '%s -query="select count(*) from %s" --iterations=5' % \
                (runquery_cmd, table)
    subprocess.call(count_cmd, shell=True, stderr=dev_null, stdout=dev_null)

def prime_buffer_cache_remote_hive(query):
  """
  Prime the buffer cache on remote machines for hive.
  """
  tables = parse_tables(query)
  for table in tables:
    for iteration in range(5):
      count_query = 'select count(*) from %s' % table
      subprocess.call("hive -e \"%s\"" % count_query, shell=True,
                      stderr=dev_null, stdout=dev_null)

def prime_buffer_cache_local(query):
  """
  Prime the buffer cache on mini-dfs.

  We can prime the buffer cache by accessing the local file system.
  """
  command = prime_cache_cmd % query
  os.system(command)

def create_executor(executor_name):
  # Add additional query exec options here
  query_options = {
    'runquery': lambda: (execute_using_runquery,
                            RunQueryExecOptions(options.iterations,
                            impalad=choice(TARGET_IMPALADS),
                            exec_options=options.exec_options,
                            runquery_cmd=runquery_cmd,
                            enable_counters=int(options.verbose),
                            profile_output_file=profile_output_file,
                            profiler=options.profiler,
                            )),
    'hive': lambda: (execute_using_hive,
                            HiveQueryExecOptions(options.iterations,
                            hive_cmd=options.hive_cmd,
                            )),
    'impala_beeswax': lambda: (execute_using_impala_beeswax,
                               ImpalaBeeswaxExecOptions(options.iterations,
                                                        exec_options=options.exec_options,
                                                        impalad=choice(TARGET_IMPALADS)))
  } [executor_name]()

  return query_options

def run_query(executor_name, query, prime_cache, exit_on_error):
  """
  Run a query command and return the result.

  Takes in a match functional that is used to parse stderr/stdout to extract the results.
  """
  if prime_cache:
    prime_remote_or_local_cache(query, options.remote, executor_name == 'hive')

  threads = []
  results = []

  for client in xrange(options.num_clients):
    name = "Client Thread " + str(client)
    exec_tuple = create_executor(executor_name)
    threads.append(QueryExecutor(name, exec_tuple[0], exec_tuple[1], query))
  for thread in threads:
    LOG.debug(thread.name + " starting")
    thread.start()

  for thread in threads:
    thread.join()
    if not thread.success() and exit_on_error:
      LOG.error("Thread: %s returned with error. Exiting." % thread.name)
      raise RuntimeError("Error executing query. Aborting")
    results.append(thread.get_results())
    LOG.debug(thread.name + " completed")
  return results[0]

def database_name_to_use(workload, scale_factor):
  """
  Return the name of the database to use for the specified workload and scale factor.
  """
  if workload == 'tpch':
    return '%s%s.' % (workload, scale_factor)
  return ''

def build_table_suffix(file_format, codec, compression_type):
  if file_format == 'text' and codec == 'none':
    return ''
  elif codec == 'none':
    return '_%s' % (file_format)
  elif compression_type == 'record':
    return '_%s_record_%s' % (file_format, codec)
  else:
    return '_%s_%s' % (file_format, codec)

def build_query(query_format_string, file_format, codec, compression_type,
                workload, scale_factor):
  """
  Build a well formed query.

  Given the various test parameters, construct the query that will be executed.
  """
  database_name = database_name_to_use(workload, scale_factor)
  table_suffix = build_table_suffix(file_format, codec, compression_type)
  # $TABLE is used as a token for table suffix in the queries. Here we insert the proper
  # database name based on the workload and query.
  replace_from = '(\w+\.){0,1}(?P<table_name>\w+)\$TABLE'
  replace_by = '%s%s%s' % (database_name, r'\g<table_name>', table_suffix)
  return re.sub(replace_from, replace_by, query_format_string)

def read_vector_file(file_name):
  """
  Parse the test vector file.
  """
  if not isfile(file_name):
    LOG.error('Cannot find vector file: ' + file_name)
    exit(1)

  vector_values = list()
  with open(file_name, 'rb') as vector_file:
    for line in vector_file.readlines():
      if line.strip().startswith('#'):
        continue
      vector_values.append([value.split(':')[1].strip() for value in line.split(',')])
  return vector_values

def save_results(result_map, output_csv_file, is_impala_result=True):
  """
  Writes the results to an output CSV files
  """
  if result_map is None:
    LOG.error('Result map is None')
    return

  csv_writer = csv.writer(open(output_csv_file, 'wb'), delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query_tuple, execution_results in result_map.iteritems():
    for result in execution_results:
      query, query_name = query_tuple
      append_row_to_csv_file(csv_writer, query, query_name,
        result[0] if is_impala_result else result[1])

def append_row_to_csv_file(csv_writer, query, query_name, result):
  """
  Write the results to a CSV file with '|' as the delimiter.
  """
  # Replace non-existent values with N/A for reporting results.
  std_dev, avg_time = result.execution_result.std_dev, result.execution_result.avg_time
  if not std_dev: std_dev = 'N/A'
  if not avg_time: avg_time = 'N/A'
  compression_str = '%s/%s' % (result.compression_codec, result.compression_type)
  if compression_str == 'none/none':
    compression_str = 'none'
  csv_writer.writerow([result.executor, result.workload, result.scale_factor,
                       query, query_name, result.file_format, compression_str,
                       avg_time, std_dev,])

def enumerate_query_files(base_directory):
  """
  Recursively scan the given directory for all test query files.
  """
  query_files = list()
  for item in os.listdir(base_directory):
    full_path = os.path.join(base_directory, item)
    if isfile(full_path) and item.endswith('.test'):
      query_files.append(full_path)
    elif isdir(full_path):
      query_files += enumerate_query_files(full_path)
  return query_files

def is_comment_or_empty(line):
  """
  Return True of the line is a comment or and empty string.
  """
  comment = line.strip().startswith('#') or line.strip().startswith('//')
  return not line or not comment

def extract_queries_from_test_files(workload):
  """
  Enumerate all the query files for a workload and extract the query strings.
  """
  workload_base_dir = os.path.join(WORKLOAD_DIR, workload)
  if not isdir(workload_base_dir):
    LOG.error("Workload '%s' not found at path '%s'" % (workload, workload_base_dir))
    exit(1)

  query_dir = os.path.join(workload_base_dir, 'queries')
  if not isdir(query_dir):
    LOG.error("Workload query directory not found at path '%s'" % (query_dir))
    exit(1)

  query_map = dict()
  for query_file_name in enumerate_query_files(query_dir):
    LOG.debug('Parsing Query Test File: ' + query_file_name)
    with open(query_file_name, 'rb') as query_file:
      # Query files are split into sections separated by '=====', with subsections
      # separeted by '----'. The first item in each subsection is the actual query
      # to execute
      # TODO : Use string.replace
      test_name = re.sub('/', '.', query_file_name.split('.')[0])[1:]
      query_map[test_name] = []
      # Some of the read queries are just blank lines, this cleans up the noise.
      query_sections = [qs for qs in query_file.read().split('====') if qs.strip()]
      for query_section in query_sections:
        if not query_section.strip():
          continue
        query = query_section.split('----')[0]
        # TODO : Use re.match
        query_name = re.findall('QUERY_NAME : .*', query)
        query = query.split('\n')
        formatted_query = ('\n').join(filter(is_comment_or_empty, query)).strip()
        if query_name and formatted_query:
          query_name = query_name[0].split(':')[-1].strip()
          query_map[test_name].append((query_name, formatted_query))
        elif formatted_query:
          query_map[test_name].append((None, formatted_query))
  return query_map

def execute_queries(query_map, workload, scale_factor, vector_row):
  """
  Execute the queries for combinations of file format, compression, etc.

  The values needed to build the query are stored in the first 4 columns of each row.
  """
  global result_map
  global summary
  file_format, data_group, codec, compression_type = vector_row[:4]
  if (options.file_formats and file_format not in options.file_formats.split(',')) or\
     (options.compression_codecs and codec not in options.compression_codecs.split(',')):
    LOG.info("Skipping Test Vector - File Format: %s Compression: %s / %s" %\
        (file_format, codec, compression_type))
    return

  LOG.info("Running Test Vector - File Format: %s Compression: %s / %s" %\
      (file_format, codec, compression_type))
  for test_name in query_map.keys():
    for query_name, query in query_map[test_name]:
      if not query_name:
        query_name = query
      query_string = build_query(query.strip(), file_format, codec, compression_type,
                                 workload, scale_factor)

      execution_result = QueryExecutionResult()
      if not options.skip_impala:
        summary += "Results Using Impala:\n"
        summary += "Query: %s\n" % query_name
        LOG.debug('\nRunning: \n%s\n' % query)
        if query_name != query:
          LOG.info('Query Name: %s' % query_name)
        if options.beeswax:
          #import pdb; pdb.set_trace()
          execution_result = run_query('impala_beeswax', query_string,
                                       options.prime_cache, True)
        else:
          execution_result = run_query('runquery', query_string,
                                       options.prime_cache, True)
        summary += "->%s\n" % execution_result

      hive_execution_result = QueryExecutionResult()
      if run_using_hive:
        summary += "Results Using Hive:\n"
        hive_execution_result = run_query('hive', query_string,
                                          options.prime_cache, False)
        summary += "->%s\n" % hive_execution_result
      summary += '\n'
      LOG.debug("-----------------------------------------------------------------------")

      execution_detail = QueryExecutionDetail("runquery", workload, scale_factor,
          file_format, codec, compression_type, execution_result)

      hive_execution_detail = QueryExecutionDetail("hive", workload, scale_factor,
          file_format, codec, compression_type, hive_execution_result)

      result_map[(query_name, query)].append((execution_detail, hive_execution_detail))

def run_workload(workload, scale_factor):
  """
    Run queries associated with each workload specified on the commandline.

    For each workload specified in, look up the associated query files. Extract valid
    queries in each file and execute them using the specified number of execution
    iterations. Finally, write results to an output CSV file for reporting.
  """
  LOG.info('Running workload: %s / Scale factor: %s' % (workload, scale_factor))
  query_map = extract_queries_from_test_files(workload)
  vector_file_path = os.path.join(WORKLOAD_DIR, workload,
                                  "%s_%s.csv" % (workload, options.exploration_strategy))
  test_vector = read_vector_file(vector_file_path)
  args = [query_map, workload, scale_factor]
  execute_queries_partial = partial(execute_queries, *args)
  map(execute_queries_partial, test_vector)

def parse_workload_scale_factor(workload_scale_factor):
  parsed_workload_scale_factor = workload_and_scale_factor.split(':')
  if len(parsed_workload_scale_factor) == 1:
    return parsed_workload_scale_factor[0], ''
  elif len(parsed_workload_scale_factor) == 2:
    return parsed_workload_scale_factor
  else:
    LOG.error("Error parsing workload. Proper format is workload[:scale factor]")
    sys.exit(1)

def write_results(partial_results = False):
  suffix = '.partial' if partial_results else ''

  if not options.skip_impala:
    LOG.info("Results saving to: %s" % options.results_csv_file)
    save_results(result_map, options.results_csv_file + suffix, is_impala_result=True)
  if run_using_hive:
    LOG.info("Hive Results saving to: %s" % options.hive_results_csv_file)
    save_results(result_map, options.hive_results_csv_file + suffix,
                 is_impala_result=False)


if __name__ == "__main__":
  """
  Driver for the run-benchmark script.

  It runs all the workloads specified on the command line and writes them to a csv file.
  """
  result_map = defaultdict(list)
  summary = str()
  for workload_and_scale_factor in options.workloads.split(','):
    workload, scale_factor = parse_workload_scale_factor(workload_and_scale_factor)
    try:
      run_workload(workload, scale_factor)
    except Exception, e:
      write_results(partial_results=True)
      raise e
  LOG.info('Summary:\n%s' % summary)
  write_results()
