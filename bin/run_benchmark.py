#!/usr/bin/env python
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
# The script can also compare results against existing results.  The file that it looks 
# for is in $IMPALA_HOME/hive_benchmark_results.txt.  If the query is in that file, it 
# will compare against the mean from that file.  The output of this script can be copy and 
# pasted into that file. TODO: make this better.
#
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
#
# The planservice needs to be running before this script.
# Run with the --help option to see the arguments.
import os
import re
import sys
from optparse import OptionParser

# Options
parser = OptionParser()
parser.add_option("-p", "--profiler", dest="profiler", default = 0,
                  help="If true, also run google pprof for sample profiling.")
parser.add_option("-v", "--verbose", dest="verbose", default = 0,
                  help="If true, outputs all benchmark diagnostics.")
parser.add_option("-q", "--query", dest="query", default = "",
                  help="Query to run.  If none specified, runs all queries.")
parser.add_option("--iterations", dest="iterations", default="3",
                  help="Number of times to run the query.  Only to be used with -q")
parser.add_option("--prime_cache", dest="prime_cache", default="3",
                  help="Number of times to prime buffer cache.  Only to be used with -q")
parser.add_option("-i", "--iteractive", dest="interactive", default = 1,
                  help="If true, outputs the results as they are generated.")

(options, args) = parser.parse_args()

profile_output_file = 'build/release/service/profile.tmp'
query_cmd = 'build/release/service/runquery -profile_output_file=""'
gprof_cmd = 'google-pprof --text build/release/service/runquery %s | head -n 30'
reference_result_file = 'hive_benchmark_results.txt'
result_single_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
result_multiple_regex = 'returned (\d*) rows? in (\d*).(\d*) s with stddev (\d*).(\d*)'

# Console color format strings
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
END = '\033[0m'

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

# Parse out the reference results so far
def parse_reference_results():
  try:
    results = {}
    current_result = {}
    full_path = os.environ['IMPALA_HOME'] + "/" + reference_result_file
    f = open(full_path)
    for line in f:
      line = line.strip()
      if line.startswith("Query:"):
        current_result = {}
        query = line[line.find(":") + 1:].strip()
        results[query] = current_result
      elif line.startswith("Avg Time:"):
        time = line[line.find(":") + 1:].strip() 
        time = time[0:len(time) - 1]
        current_result["avg"] = time
      elif line.startswith("Std Dev:"):
        time = line[line.find(":") + 1:].strip() 
        time = time[0:len(time) - 1]
        current_result["stddev"] = time
    return results
  except IOError:
    print "Could not find previous run results."
    return {}

# Function which will run the query and report the average time and standard deviation
#   - reference_results: a dictionary with <query string,reference result> values
#   - query: the query to run
#   - prime_buffer_cache: number of times to run the query to prime the buffer cache
#     This is not useful for very large (e.g. > 2 GB) data sets
#   - iterations: number of times to run the query
# Returns two strings as output.  The first string is the summary of the query run.
# The second is the comparison output against reference results if there are any.
def run_query(reference_results, query, prime_buffer_cache, iterations):
  query = query.strip()

  compare_output = ""
  output = ""

  reference_avg = 0
  reference_stddev = 0
  if query in reference_results:
    reference_result = reference_results[query]
    if "avg" in reference_result:
      reference_avg = float(reference_result["avg"])
    if "stddev" in reference_result:
      reference_stddev = float(reference_result["stddev"])
  
  if prime_buffer_cache > 0:
    # Run the query to prime the buffer cache.  It would be great to just get the files
    # and cat them to /dev/null but that is not trivial.  Instead, parse for the table
    # and run a count(*) query on the tables
    tables = parse_tables(query)
    for i in range (0, len(tables)):
      count_cmd = '%s -query="select count(*) from %s" --iterations=%d -profile_output_file=""' % (query_cmd, tables[i], prime_buffer_cache)
      os.popen3(count_cmd, "r")

  avg_time = 0
  stddev = ""
  run_success = 0

  enable_counters = int(options.verbose)
  gprof_tmp_file = ""
  if options.profiler:
    gprof_tmp_file = profile_output_file

  cmd = '%s -query="%s" -iterations=%d -enable_counters=%d -profile_output_file=%s' % (query_cmd, query, iterations, enable_counters, gprof_tmp_file)

  # Run query
  query_output = os.popen3(cmd, "r")[1]
  while 1:
    line = query_output.readline()
    if not line: break

    if options.verbose != 0:
      print line.rstrip()

    if iterations == 1:
      match = re.search(result_single_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        run_success = 1
    else:
      match = re.search(result_multiple_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        stddev = ('%s.%s') % (match.group(4), match.group(5))
        run_success = 1

  if run_success == 0:
    print "Query did not run succesfully"
    sys.exit(1)

  if options.profiler:
    os.system(gprof_cmd % gprof_tmp_file)
  
  avg_time = float(avg_time)
  
  output = "Query: %s\n" % (query)
  output += "  Avg Time: %fs\n" % (avg_time)
  if len(stddev) != 0:
    output += "  Std Dev:  " + stddev + "s\n"

  # TODO: some kind of statistics against reference_result/reference_stddev?
  if reference_avg != 0:
    compare_output = "Query: %s\n" % (query)
    if avg_time < reference_avg:
      diff = (reference_avg - avg_time) / reference_avg * 100
      compare_output += "  Avg Time: %fs (-%s%.2f%%%s)\n" % (avg_time, GREEN, diff, END)
    else:
      diff = (avg_time - reference_avg) / reference_avg * 100
      compare_output += "  Avg Time: %fs (+%s%.2f%%%s)\n" % (avg_time, RED, diff, END)
    if len(stddev) != 0:
      compare_output += "  Std Dev:  " + stddev + "s\n"

  return [output, compare_output]

os.chdir(os.environ['IMPALA_BE_DIR'])

reference_results = parse_reference_results()

# This table contains [query, numbers of times to prime buffer cache, number of iterations]
# Queries should be grouped by the data they touch.  This eliminates the need for the buffer
# cache priming iterations.
# TODO: it would be good if this table also contained the expected numbers and automatically 
# flag regressions.  How do we reconcile the fact we are running on different machines?
queries = [
  ["select count(*) from grep1gb", 5, 5],
  ["select count(field) from grep1gb", 0, 5],
  ["select count(field) from grep1gb where field like '%xyz%'", 0, 5],
  ["select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue from uservisits uv join rankings r on (r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1", 5, 5],
  ["select sourceIP, SUM(adRevenue) FROM uservisits GROUP by sourceIP order by SUM(adRevenue) desc limit 10", 5, 5],
  ["select pageRank, pageURL from rankings where pageRank > 10 order by pageRank limit 100", 0, 5],
  ["select count(field) from grep10gb where field like '%xyz%'", 0, 1]
]

if (len(options.query) == 0):
  output = ""
  compare_output = ""
  for query in queries:
    result = run_query(reference_results, query[0], query[1], query[2])
    output += result[0]
    compare_output += result[1]
    if options.interactive:
      if len(result[1]) != 0:
        print result[1]
      else:
        print result[0]
  print compare_output
  print "\nCopy and paste below to %s/%s to update the reference results:" % (os.environ['IMPALA_HOME'], reference_result_file)
  print output
else:
  result = run_query(reference_results, options.query, int(options.prime_cache), int(options.iterations))
  if len(result[1]) != 0:
    print result[1]
  else:
    print result[0]

