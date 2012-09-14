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
import re
import sys
import subprocess
import tempfile
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
                  default=os.path.join(os.environ['IMPALA_HOME'],
                      'be/build/release/service/runquery'),
                  help="The command to use for executing queries")
parser.add_option("--runquery_args", dest="runquery_args", default='',
                  help="Additional arguments to pass to runquery.")
parser.add_option("--compare_with_hive", dest="compare_with_hive", action="store_true",
                  default= False, help="Run all queries using Hive as well as Impala")
parser.add_option("--results_csv_file", dest="results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/benchmark_results.csv",
                  help="The output file where benchmark results are saved")
parser.add_option("--hive_cmd", dest="hive_cmd", default="hive -e",
                  help="The command to use for executing hive queries")
parser.add_option("-i", "--iterations", type="int", dest="iterations", default=5,
                  help="Number of times to run each query.")
parser.add_option("--prime_cache", dest="prime_cache", action="store_true",
                  default= False, help="Whether or not to prime the buffer cache. ")
parser.add_option("--num_clients", type="int", dest="num_clients", default=1,
                  help="Number of clients (threads) to use when executing each query.")
options, args = parser.parse_args()

# globals
TARGET_IMPALADS = options.impalad.split(",")
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
IMPALA_HOME = os.environ['IMPALA_HOME']
profile_output_file = os.path.join(IMPALA_HOME, 'be/build/release/service/profile.tmp')
dev_null = open('/dev/null')

# commands
query_cmd = "%(runquery)s %(args)s" % {'runquery' : options.runquery_path,
                                       'args' : options.runquery_args}
gprof_cmd  = 'google-pprof --text ' + options.runquery_path + ' %s | head -n 60'
prime_cache_cmd = os.path.join(IMPALA_HOME, "testdata/bin/cache_tables.py") + " -q \"%s\""

# regular expressions
gprof_cmd = 'google-pprof --text ' + options.runquery_path + ' %s | head -n 60'
prime_cache_cmd = os.path.join(os.environ['IMPALA_HOME'],
                               "testdata/bin/cache_tables.py") + " -q \"%s\""
result_single_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
result_multiple_regex = 'returned (\d*) rows? in (\d*).(\d*) s with stddev (\d*).(\d*)'
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'
dev_null = open('/dev/null')

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('run-benchmark')
if options.verbose:
  LOG.setLevel(level=logging.DEBUG)

class QueryExecutionResult(object):
  def __init__(self, avg_time = 'N/A', stddev = 'N/A'):
    self.avg_time = avg_time
    self.stddev = stddev

class QueryExecutionDetail(object):
  def __init__(self, workload, scale_factor, file_format, compression_codec,
               compression_type, impala_execution_result, hive_execution_result):
    self.workload = workload
    self.scale_factor = scale_factor
    self.file_format = file_format
    self.compression_codec = compression_codec
    self.compression_type = compression_type
    self.impala_execution_result = impala_execution_result
    self.hive_execution_result = hive_execution_result


## Functions
class QueryExecutor(threading.Thread):
  def __init__(self, logger, name, query_results_match_function, cmd, exit_on_error):
    self.LOG = logger
    self.query_results_match_function = query_results_match_function
    self.cmd = cmd
    self.exit_on_error = exit_on_error
    self.output_result = None
    self.execution_result = None
    threading.Thread.__init__(self)
    self.name = name
    self.has_execution_error = False

  # Runs the given query command and returns the execution result. Takes in a match
  # function that is used to parse stderr/stdout to extract the results.
  def _run_query_capture_results(self):
    output_stdout = tempfile.TemporaryFile("w+")
    output_stderr = tempfile.TemporaryFile("w+")
    self.LOG.info("Executing: %s" % self.cmd)
    subprocess.call(self.cmd, shell=True, stderr=output_stderr, stdout=output_stdout)
    output_stdout.seek(0)
    output_stderr.seek(0)
    run_success, execution_result =\
        self.query_results_match_function(output_stdout.readlines(),
                                          output_stderr.readlines())
    self._print_file("", output_stderr)
    self._print_file("", output_stdout)
    if not run_success:
      self.has_execution_error = True
      self.LOG.error("Query did not run successfully")
      if self.exit_on_error:
        return

    output = ''
    if run_success:
      output += "  Avg Time: %.02fs\n" % float(execution_result.avg_time)
      if execution_result.stddev != 'N/A':
        output += "  Std Dev: %.02fs\n" % float(execution_result.stddev)
    else:
      output += '  No Results - Error executing query!\n'

    output_stderr.close()
    output_stdout.close()
    self.output_result = output
    self.execution_result = execution_result

  def run(self):
    self._run_query_capture_results()

  def get_results(self):
    return self.output_result, self.execution_result

  def _print_file(self, header, output_file):
    """
    Print the contents of the file to the terminal.
    """
    output_file.seek(0)
    output = header + '\n'
    for line in output_file.readlines():
      output += line.rstrip() + '\n'
    self.LOG.debug(output)

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
                (query_cmd, table)
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

# Util functions
# TODO : Move util functions to a common module.
def calculate_avg(values):
  return sum(values) / float(len(values))

def calculate_stddev(values):
  """
  Return the stardard deviation of a numeric iterable.

  TODO: If more statistical functions are required, consider using numpy/scipy.
  """
  avg = calculate_avg(values)
  return math.sqrt(calculate_avg([(val - avg)**2 for val in values]))

def match_impala_query_results(output_stdout, output_stderr):
  """
  Parse query execution details for impala.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns these results as well as whether the query completed successfully.
  """
  avg_time = 0
  stddev = "N/A"
  run_success = False
  for line in output_stdout:
    if options.iterations == 1:
      match = re.search(result_single_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        run_success = True
    else:
      match = re.search(result_multiple_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        stddev = ('%s.%s') % (match.group(4), match.group(5))
        run_success = True
  return run_success, QueryExecutionResult(str(avg_time), str(stddev))

def match_hive_query_results(output_stdout, output_stderr):
  """
  Parse query execution details for hive.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns these results as well as whether the query completed successfully.
  """
  run_success = False
  execution_times = list()
  for line in output_stderr:
    match = re.search(hive_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))

  execution_result = QueryExecutionResult()
  if len(execution_times) == options.iterations:
    avg_time = calculate_avg(execution_times)
    stddev = 'N/A'
    if options.iterations > 1:
      stddev = calculate_stddev(execution_times)
    execution_result = QueryExecutionResult(avg_time, stddev)
    run_success = True
  return run_success, execution_result

def run_query_capture_results(query_results_match_function, cmd, exit_on_error):
  """
  Run a query command and return the result.

  Takes in a match functional that is used to parse stderr/stdout to extract the results.
  """
  threads = []
  results = []

  output = None
  execution_result = None
  for client in xrange(options.num_clients):
    name = "Client Thread " + str(client)
    target_cmd = cmd.format(impalad=choice(TARGET_IMPALADS))
    threads.append(QueryExecutor(LOG, name, match_impala_query_results,
                                 target_cmd, exit_on_error=exit_on_error))

  for thread in threads:
    LOG.debug(thread.name + " starting")
    thread.start()

  for thread in threads:
    thread.join()
    if thread.has_execution_error and exit_on_error:
      LOG.error("Thread: %s returned with error. Exiting." % thread.name)
      sys.exit(1)
    results.append((thread.get_results()))
    LOG.debug(thread.name + " completed")
  return results

def run_impala_query(query, prime_cache, iterations):
  """
  Run an Impala query and report statistics.

  Function which will run the query and report the average time and standard deviation
   - query: the query to run
   - prime_buffer_cache: if true, will try to prime buffer cache for all tables in the
     query. This is not useful for very large (e.g. > 2 GB) data sets
   - iterations: number of times to run the query
  """
  # parse the options and do some setup.
  if prime_cache:
    prime_remote_or_local_cache(query, options.remote)
  enable_counters = int(options.verbose)
  gprof_tmp_file = str()

  # Call the profiler if the option has been specified.
  if options.profiler:
    gprof_tmp_file = profile_output_file

  cmd = '%s -query="%s" -iterations=%d -enable_counters=%d -profile_output_file="%s"' %\
      (query_cmd, query, iterations, enable_counters, gprof_tmp_file)
  cmd += ' -impalad={impalad}'
  threads = []
  results = run_query_capture_results(match_impala_query_results, cmd, exit_on_error=True)
  if options.profiler:
    subprocess.call(gprof_cmd % gprof_tmp_file, shell=True)
  return results[0]

# Similar to run_impala_query except runs the given query against Hive.
def run_hive_query(query, prime_cache, iterations):
  """
  Run a given query against hive.
  """
  # prime the cache if the option has been specified.
  if prime_cache:
    prime_remote_or_local_cache(query, options.remote, hive=True)
  query_string = (query + ';') * iterations
  cmd = options.hive_cmd + "\" %s\"" % query_string
  return run_query_capture_results(match_hive_query_results, cmd, exit_on_error=False)[0]

def vector_file_name(workload, exploration_strategy):
  return "%s_%s.csv" % (workload, exploration_strategy)

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

def write_to_csv(result_map, output_csv_file):
  """
  Write the results to a CSV file with '|' as the delimiter.
  """
  csv_writer = csv.writer(open(output_csv_file, 'wb'),
                          delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query_tuple, execution_results in result_map.iteritems():
    for result in execution_results:
      compression_str = '%s/%s' % (result.compression_codec, result.compression_type)
      if compression_str == 'none/none':
        compression_str = 'none'
      csv_writer.writerow([result.workload, query_tuple[0], query_tuple[1],
                           result.file_format, compression_str,
                           result.impala_execution_result.avg_time,
                           result.impala_execution_result.stddev,
                           result.hive_execution_result.avg_time,
                           result.hive_execution_result.stddev,
                           result.scale_factor,
                           ])

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
  # TODO : Find a clean way to get rid of globals.
  global summary
  global result_map
  file_format, data_group, codec, compression_type = vector_row[:4]
  LOG.info("Running Test Vector - File Format: %s Compression: %s / %s" %\
      (file_format, codec, compression_type))
  for test_name in query_map.keys():
    for query_name, query in query_map[test_name]:
      if not query_name:
        query_name = query
      query_string = build_query(query.strip(), file_format, codec, compression_type,
                                 workload, scale_factor)
      summary += "\nQuery: %s\n" % query_name
      summary += "Results Using Impala\n"
      LOG.debug('Running: \n%s\n' % query)
      if query_name != query:
        LOG.info('Query Name: \n%s\n' % query_name)
      output, execution_result = run_impala_query(query_string,
                                                  options.prime_cache,
                                                  options.iterations)
      if output:
        summary += output
      hive_execution_result = QueryExecutionResult()

      if options.compare_with_hive:
        summary += "Results Using Hive\n"
        output, hive_execution_result = run_hive_query(query_string,
                                                       options.prime_cache,
                                                       options.iterations)
        if output:
          summary += output
      LOG.debug("-----------------------------------------------------------------------")

      execution_detail = QueryExecutionDetail(workload, scale_factor, file_format, codec,
                                              compression_type, execution_result,
                                              hive_execution_result)
      result_map[(query_name, query)].append(execution_detail)
  return

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
                                  vector_file_name(workload, options.exploration_strategy)
                                 )
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

if __name__ == "__main__":
  """
  Driver for the run-benchmark script.

  It runs all the workloads specified on the command line and writes them to a csv file.
  """
  result_map = defaultdict(list)
  summary = str()
  for workload_and_scale_factor in options.workloads.split(','):
    workload, scale_factor = parse_workload_scale_factor(workload_and_scale_factor)
    run_workload(workload, scale_factor)
  LOG.info(summary)
  LOG.info("Results saving to: " + options.results_csv_file)
  write_to_csv(result_map, options.results_csv_file)
