#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script should be used to benchmark queries.  It can either run in batch mode, in
# which case it will run the set of hive benchmark queries or to run a single query.  In
# either case, it will first try to warm the buffer cache before running the query
# multiple times.  There are command line options to control how many times to prerun the
# query for the buffer cache as well as the number of iterations.
#
# By default, the script will have minimal output.  Verbose output can be turned on with
# the -v option which will output the normal query output.  In addition, the -p option
# can be passed which will enable gprof instrumentation and output the sampled call
# stacks.  The -v and -p option are used by the perf regression tests.
#
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
#
# The planservice needs to be running before this script.
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
parser.add_option("-q", "--query", dest="query", default = "",
                  help="Query to run.  If none specified, runs all queries.")
parser.add_option("--iterations", dest="iterations", default="3",
                  help="Number of times to run the query.  Only to be used with -q")
parser.add_option("--prime_cache", dest="prime_cache", default= True, 
                  help="Whether or not to prime the buffer cache.  Only to be "\
                  "used with -q")
parser.add_option("--exploration_strategy", dest="exploration_strategy", default="core",
                  help="The exploration strategy to use for running benchmark: 'core', "\
                  "'pairwise', or 'exhaustive'")
parser.add_option("--query_cmd", dest="query_cmd",
                  default='build/release/service/runquery -profile_output_file=""',
                  help="The command to use for executing queries")
parser.add_option("--compare_with_hive", dest="compare_with_hive", action="store_true",
                  default= False, help="Run all queries using Hive as well as Impala")
parser.add_option("--results_csv_file", dest="results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/benchmark_results.csv",
                  help="The output file where benchmark results are saved")

(options, args) = parser.parse_args()

profile_output_file = 'build/release/service/profile.tmp'
gprof_cmd = 'google-pprof --text build/release/service/runquery %s | head -n 60'
prime_cache_cmd = os.environ['IMPALA_HOME'] + "/testdata/bin/cache_tables.py -q \"%s\""
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
  def __init__(self, avg_time = '', stddev = ''):
    self.avg_time = avg_time
    self.stddev = stddev

class QueryExecutionDetail:
  def __init__(self,  file_format, compression, impala_execution_result,
               hive_execution_result):
    self.impala_execution_result = impala_execution_result
    self.hive_execution_result = hive_execution_result
    self.file_format = file_format
    self.compression = compression

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
                (options.query_cmd, table)
    subprocess.call(count_cmd, shell=True, stderr=dev_null, stdout=dev_null)

def prime_buffer_cache_remote_hive(query):
  tables = parse_tables(query)
  for table in tables:
    for iteration in range(5):
      count_query = 'select count(*) from %s' % table
      subprocess.call("hive -e \"%s\"" % count_query, shell=True,
                      stderr=dev_ull, stdout=dev_null)

def prime_buffer_cache_local(query):
  # On mini-dfs, we can prime the buffer cache by accessing the local file system
  cmd = prime_cache_cmd % query
  os.system(cmd)

def calculate_avg(values):
  return sum(values) / float(len(values))

def calculate_stddev(values):
  avg = calculate_avg(values)
  return math.sqrt(calculate_avg([(val - avg)**2 for val in values]))

def run_query_using_hive(query, prime_buffer_cache, iterations):
  query = query.strip()
  if prime_buffer_cache:
    if options.remote:
      prime_buffer_cache_remote_hive(query)
    else:
      prime_buffer_cache_local(query)

  query_string = (query + ';') * iterations
  query_output = tempfile.TemporaryFile("w+")
  subprocess.call("hive -e \"%s\"" % query_string, shell=True,
                  stderr=query_output, stdout=dev_null)
  query_output.seek(0)
  execution_times = []
  for line in query_output.readlines():
    match = re.search(hive_result_regex, line)
    if match:
      if options.verbose != 0:
        print line
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))

  execution_result = QueryExecutionResult("N/A", "N/A")
  if len(execution_times) == iterations:
    avg_time = calculate_avg(execution_times)
    stddev = calculate_stddev(execution_times)
    output =  "  Avg Time: %fs\n" % avg_time
    output += "  Std Dev: %fs\n" % stddev
    execution_result = QueryExecutionResult(str(avg_time), str(stddev))
  else:
    output = "Error parsing Hive execution results. Check Hive logs."
  return [output, execution_result]

# Function which will run the query and report the average time and standard deviation
#   - reference_results: a dictionary with <query string,reference result> values
#   - query: the query to run
#   - prime_buffer_cache: if true, will try to prime buffer cache for all tables in the
#     query.
#     This is not useful for very large (e.g. > 2 GB) data sets
#   - iterations: number of times to run the query
# Returns two strings as output.  The first string is the summary of the query run.
# The second is the comparison output against reference results if there are any.
def run_query(query, prime_buffer_cache, iterations):
  query = query.strip()
  compare_output = ""
  output = ""

  if prime_buffer_cache:
    if options.remote:
      prime_buffer_cache_remote_impala(query)
    else:
      prime_buffer_cache_local(query)

  avg_time = 0
  stddev = ""
  run_success = False

  enable_counters = int(options.verbose)
  gprof_tmp_file = ""
  if options.profiler:
    gprof_tmp_file = profile_output_file

  cmd = '%s -query="%s" -iterations=%d -enable_counters=%d -profile_output_file=%s' %\
         (options.query_cmd, query, iterations, enable_counters, gprof_tmp_file)

  # Run query
  query_output = tempfile.TemporaryFile("w+")
  query_err = tempfile.TemporaryFile("w+")
  subprocess.call(cmd, shell=True, stderr=query_err, stdout=query_output)
  query_output.seek(0)
  for line in query_output.readlines():
    if options.verbose != 0:
      print line.rstrip()

    if iterations == 1:
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

  if not run_success:
    print "Query did not run succesfully"
    query_output.seek(0)
    query_err.seek(0)
    for line in query_output.readlines():
      print line.rstrip()
    for line in query_err.readlines():
      print line.rstrip()
    sys.exit(1)

  query_err.close()
  query_output.close()

  if options.profiler:
    subprocess.call(gprof_cmd % gprof_tmp_file, shell=True)

  avg_time = float(avg_time)

  output = "Query: %s\n" % (query)
  output += "  Avg Time: %fs\n" % (avg_time)
  if len(stddev) != 0:
    output += "  Std Dev:  " + stddev + "s\n"

  output.rstrip()
  execution_result = QueryExecutionResult(str(avg_time), str(stddev))
  return [output, execution_result]

def choose_input_vector_file_name(exploration_strategy):
  return "benchmark_%s.vector" % exploration_strategy

def build_query(
    query_format_string, exploration_strategy, data_set, file_format, compression):
  table_name = "%s_%s_%s" % (data_set, file_format, compression)
  return query_format_string % {'table_name': table_name}

os.chdir(os.environ['IMPALA_BE_DIR'])

# This table contains a hash of dataset -> [query, numbers of times to prime buffer cache,
# number of iterations].  Queries should be grouped by the data they touch.  This
# eliminates the need for the buffer cache priming iterations.
# TODO: it would be good if this table also contained the expected numbers and
# automatically flag regressions.  How do we reconcile the fact we are running on
# different machines?
queries = {'grep1GB': [
  ["select count(*) from %(table_name)s", 1, 5],
  ["select count(field) from %(table_name)s", 0, 5],
  ["select count(field) from %(table_name)s where field like '%%xyz%%'", 0, 5]
  ],

  'web': [
  ["select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue "\
   "from uservisits_%(table_name)s uv join rankings_%(table_name)s r on "\
   "(r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate "\
   "< '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1", 1, 5],
  ["select sourceIP, SUM(adRevenue) FROM uservisits_%(table_name)s GROUP by sourceIP "\
   "order by SUM(adRevenue) desc limit 10", 1, 5],
  ["select pageRank, pageURL from rankings_%(table_name)s where pageRank > 10 "\
   "order by pageRank limit 100", 1, 5],
  ["select count(*) from rankings_%(table_name)s where "\
   "pageRank > 10 && pageRank < 25", 1, 5],
  ["select avg(adRevenue) from uservisits_%(table_name)s", 1, 5],
  ["select avg(adRevenue) from uservisits_%(table_name)s "\
   "where visitdate > '1999-07-01' and visitdate < '1999-12-31'", 1, 5],
  ],

  'grep10GB': [
  ["select  count(field) from %(table_name)s where field like '%%xyz%%'", 0, 1]
  ]
}

# Writes out results to a CSV file. Columns are delimited by '|' characters
def write_to_csv(result_map, output_csv_file):
  csv_writer = csv.writer(open(output_csv_file, 'wb'),
                          delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query, execution_results in result_map.iteritems():
    for result in execution_results:
      csv_writer.writerow([query, result.file_format, result.compression,
                           result.impala_execution_result.avg_time,
                           result.impala_execution_result.stddev,
                           result.hive_execution_result.avg_time,
                           result.hive_execution_result.stddev])

# Run all queries
if (len(options.query) == 0):
  vector_file = open(
    '../testdata/bin/' + choose_input_vector_file_name(options.exploration_strategy))
  output = ""
  result_map = collections.defaultdict(list)

  for line in vector_file:
    file_format, data_set, compression = line.split()[:3]
    for query in queries[data_set]:
      query_string = build_query(query[0], options.exploration_strategy, data_set,
                                 file_format, compression)
      result = run_query(query_string, query[1], query[2])
      output += result[0]
      print result[0]
      execution_result = result[1]
      hive_execution_result = QueryExecutionResult("N/A", "N/A")
      if options.compare_with_hive:
        hive_result = run_query_using_hive(query_string, query[1], query[2])
        print "Hive Results:"
        print hive_result[0]
        hive_execution_result = hive_result[1]
      if options.verbose != 0:
        print "--------------------------------------------------------------------------"

      execution_detail = QueryExecutionDetail(file_format, compression, execution_result,
                                              hive_execution_result)
      result_map[query[0]].append(execution_detail)
  vector_file.close()

  print "\nResults saving to: " + options.results_csv_file
  write_to_csv(result_map, options.results_csv_file)

  print output

# Run query from command line
else:
  result = run_query(options.query, int(options.prime_cache),
                    int(options.iterations))
  print result[1] or result[0]
