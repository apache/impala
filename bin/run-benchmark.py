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
import collections
import csv
import math
import os
import re
import sys
import subprocess
import tempfile
from optparse import OptionParser

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
                  help="The workload(s) to execute in a comma-separated list format."\
                  "Some valid workloads: 'hive-benchmark', 'tpch', ...")
parser.add_option("-s", "--scale_factor", dest="scale_factor", default="",
                  help="The dataset scale factor to run the workload against.")
parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                  help="The impalad coordinator to run the workload against.")
parser.add_option("--runquery_path", dest="runquery_path",
                  default=os.path.join(os.environ['IMPALA_HOME'],
                      'be/build/release/service/runquery'),
                  help="The command to use for executing queries")
parser.add_option("--runquery_args", dest="runquery_args",
                  default=' -profile_output_file=""',
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

(options, args) = parser.parse_args()

WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
profile_output_file =\
    os.path.join(os.environ['IMPALA_HOME'], 'be/build/release/service/profile.tmp')

query_cmd = "%s %s --impalad=%s" %\
    (options.runquery_path, options.runquery_args, options.impalad)

gprof_cmd = 'google-pprof --text ' + options.runquery_path + ' %s | head -n 60'
prime_cache_cmd = os.path.join(os.environ['IMPALA_HOME'],
                               "testdata/bin/cache_tables.py") + " -q \"%s\""
result_single_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
result_multiple_regex = 'returned (\d*) rows? in (\d*).(\d*) s with stddev (\d*).(\d*)'
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'

# Console color format strings
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
END = '\033[0m'

dev_null = open('/dev/null')

class QueryExecutionResult:
  def __init__(self, avg_time = 'N/A', stddev = 'N/A'):
    self.avg_time = avg_time
    self.stddev = stddev

class QueryExecutionDetail:
  def __init__(self, workload, file_format, compression_codec, compression_type,
               impala_execution_result, hive_execution_result):
    self.workload = workload
    self.file_format = file_format
    self.compression_codec = compression_codec
    self.compression_type = compression_type
    self.impala_execution_result = impala_execution_result
    self.hive_execution_result = hive_execution_result

# Parse for the tables used in this query
def parse_tables(query):
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

def prime_buffer_cache_remote_impala(query):
  # On remote clusters, we'll prime the buffer cache by just running count(*)
  # TODO: does this work well enough? Does having a real cluster and data
  # locality make this more deterministic?
  tables = parse_tables(query)
  for table in tables:
    count_cmd = '%s -query="select count(*) from %s" --iterations=5' % \
                (query_cmd, table)
    subprocess.call(count_cmd, shell=True, stderr=dev_null, stdout=dev_null)

def prime_buffer_cache_remote_hive(query):
  tables = parse_tables(query)
  for table in tables:
    for iteration in range(5):
      count_query = 'select count(*) from %s' % table
      subprocess.call("hive -e \"%s\"" % count_query, shell=True,
                      stderr=dev_null, stdout=dev_null)

def prime_buffer_cache_local(query):
  # On mini-dfs, we can prime the buffer cache by accessing the local file system
  cmd = prime_cache_cmd % query
  os.system(cmd)

def calculate_avg(values):
  return sum(values) / float(len(values))

def calculate_stddev(values):
  avg = calculate_avg(values)
  return math.sqrt(calculate_avg([(val - avg)**2 for val in values]))

def print_file(header, output_file):
  print header
  output_file.seek(0)
  for line in output_file.readlines():
    print line.rstrip()

# Parses the query execution details (avg time, stddev) from the runquery output.
# Returns these results as well as whether the query completed successfully.
def match_impala_query_results(output_stdout, output_stderr):
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

# Parses the query execution details (avg time, stddev) from running a query using Hive.
# Returns these results as well as whether the query completed successfully.
def match_hive_query_results(output_stdout, output_stderr):
  run_success = False
  execution_times = []
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
  return (run_success, execution_result)

# Runs the given query command and returns the execution result. Takes in a match
# functional that is used to parse stderr/stdout to extract the results.
def run_query_capture_results(query_results_match_function, cmd, exit_on_error):
  output_stdout = tempfile.TemporaryFile("w+")
  output_stderr = tempfile.TemporaryFile("w+")
  subprocess.call(cmd, shell=True, stderr=output_stderr, stdout=output_stdout)
  output_stdout.seek(0)
  output_stderr.seek(0)
  (run_success, execution_result) =\
      query_results_match_function(output_stdout.readlines(), output_stderr.readlines())

  if options.verbose or not run_success:
    print_file("", output_stdout)
    print_file("", output_stderr)
    if not run_success:
      print "Query did not run successfully"
      if exit_on_error:
        sys.exit(1)

  output = ''
  if run_success:
    output += "  Avg Time: %.02fs\n" % float(execution_result.avg_time)
    if execution_result.stddev != 'N/A':
      output += "  Std Dev: %.02fs\n" % float(execution_result.stddev)
  else:
    output += '  No Results - Error executing query!\n'

  output_stderr.close()
  output_stdout.close()
  return output, execution_result

# Function which will run the query and report the average time and standard deviation
#   - query: the query to run
#   - prime_buffer_cache: if true, will try to prime buffer cache for all tables in the
#     query.
#     This is not useful for very large (e.g. > 2 GB) data sets
#   - iterations: number of times to run the query
def run_impala_query(query, prime_cache, iterations):
  if prime_cache:
    if options.remote:
      prime_buffer_cache_remote_hive(query)
    else:
      prime_buffer_cache_local(query)

  enable_counters = int(options.verbose)
  gprof_tmp_file = ""
  if options.profiler:
    gprof_tmp_file = profile_output_file

  cmd = '%s -query="%s" -iterations=%d -enable_counters=%d -profile_output_file=%s' %\
      (query_cmd, query, iterations, enable_counters, gprof_tmp_file)
  output, execution_result = run_query_capture_results(match_impala_query_results, cmd,
                                                       exit_on_error=True)
  if options.profiler:
    subprocess.call(gprof_cmd % gprof_tmp_file, shell=True)
  return (output, execution_result)

# Similar to run_impala_query except runs the given query against Hive.
def run_hive_query(query, prime_cache, iterations):
  if prime_cache:
    if options.remote:
      prime_buffer_cache_remote_hive(query)
    else:
      prime_buffer_cache_local(query)
  query_string = (query + ';') * iterations
  cmd = options.hive_cmd + "\" %s\"" % query_string
  return run_query_capture_results(match_hive_query_results, cmd, exit_on_error=False)

def vector_file_name(workload, exploration_strategy):
  return "%s_%s.csv" % (workload, exploration_strategy)

# Gets the name of the database to use for the specified workload and scale factor.
def database_name_to_use(workload, scale_factor):
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
  database_name = database_name_to_use(workload, scale_factor)
  table_suffix = build_table_suffix(file_format, codec, compression_type)
  # $TABLE is used as a token for table suffix in the queries. Here we insert the proper
  # database name based on the workload and query.
  return re.sub('(\w+\.){0,1}(?P<table_name>\w+)\$TABLE', '%s%s%s' %\
                (database_name, r'\g<table_name>', table_suffix), query_format_string)

def read_vector_file(file_name):
  if not os.path.isfile(file_name):
    print 'Cannot find vector file: ' + file_name
    sys.exit(1)

  vector_values = []
  with open(file_name, 'rb') as vector_file:
    for line in vector_file.readlines():
      if line.strip().startswith('#'):
        continue
      vector_values.append([value.split(':')[1].strip() for value in line.split(',')])
  return vector_values

# Writes out results to a CSV file. Columns are delimited by '|' characters
def write_to_csv(result_map, output_csv_file):
  csv_writer = csv.writer(open(output_csv_file, 'wb'),
                          delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query, execution_results in result_map.iteritems():
    for result in execution_results:
      csv_writer.writerow([result.workload, query, result.file_format,
                           '%s/%s' % (result.compression_codec, result.compression_type),
                           result.impala_execution_result.avg_time,
                           result.impala_execution_result.stddev,
                           result.hive_execution_result.avg_time,
                           result.hive_execution_result.stddev,
                           ])

# Recursively scans the given directory for all test query files
def enumerate_query_files(base_directory):
  query_files = []
  for item in os.listdir(base_directory):
    full_path = os.path.join(base_directory, item)
    if os.path.isfile(full_path) and item.endswith('.test'):
      query_files.append(full_path)
    elif os.path.isdir(full_path):
      query_files += enumerate_query_files(full_path)
  return query_files

# Strips out comments and empty lines from the input query string
def strip_comments(query_string):
  query = []
  for line in query_string.split('\n'):
    if not line or line.strip().startswith('#') or line.strip().startswith('//'):
      continue
    query.append(line)
  return '\n'.join(query).strip()

# Enumerate all the query files for a workload and extract the actual query
# strings.
def extract_queries_from_test_files(workload):
  workload_base_dir = os.path.join(WORKLOAD_DIR, workload)
  if not os.path.isdir(workload_base_dir):
    print "Workload '%s' not found at path '%s'" % (workload, workload_base_dir)
    sys.exit(1)

  query_dir = os.path.join(workload_base_dir, 'queries')
  if not os.path.isdir(query_dir):
    print "Workload query directory not found at path '%s'" % (query_dir)

  queries = []
  for query_file_name in enumerate_query_files(query_dir):
    if options.verbose:
      print 'Parsing Query Test File: ' + query_file_name
    with open(query_file_name, 'rb') as query_file:
      # Query files are split into sections separated by '=====', with subsections
      # separeted by '----'. The first item in each subsection is the actual query
      # to execute.
      for query_section in query_file.read().split("===="):
        formatted_query = strip_comments(query_section.split("----")[0])
        if formatted_query:
          queries.append(formatted_query.strip())
  return queries

if __name__ == "__main__":
  result_map = collections.defaultdict(list)
  summary = ''
  # For each workload specified in, look up the associated query files. Extract valid
  # queries in each file and execute them using the specified number of execution
  # iterations. Finally, write results to an output CSV file for reporting.
  for workload in options.workloads.split(','):
    print 'Starting running of workload: ' + workload
    queries = extract_queries_from_test_files(workload)

    vector_file_path = os.path.join(WORKLOAD_DIR, workload,
                                    vector_file_name(workload,
                                    options.exploration_strategy))
    test_vector = read_vector_file(vector_file_path)

    # Execute the queries for combinations of file format, compression, etc.
    for row in test_vector:
      file_format, data_group, codec, compression_type = row[:4]
      print '\nTest Vector Values: ' + ', '.join(row) + '\n'
      for query in queries:
        query_string = build_query(query.strip(), file_format, codec, compression_type,
                                  workload, options.scale_factor)
        print "Query: %s" % (query_string)
        summary += "\nQuery: %s\n" % query_string
        summary += "Results Using Impala\n"
        output, execution_result =\
            run_impala_query(query_string, options.prime_cache, options.iterations)

        summary += output
        hive_execution_result = QueryExecutionResult()
        if options.compare_with_hive:
          summary += "Results Using Hive\n"
          (output, hive_execution_result) =\
              run_hive_query(query_string, options.prime_cache, options.iterations)
          summary += output
        if options.verbose != 0:
          print "------------------------------------------------------------------------"
        execution_detail = QueryExecutionDetail(workload, file_format, codec,
                                                compression_type, execution_result,
                                                hive_execution_result)
        result_map[query].append(execution_detail)

    print summary
    print "\nResults saving to: " + options.results_csv_file
    write_to_csv(result_map, options.results_csv_file)
