#!/usr/bin/env python

# This script runs the hive bencmark queries and reports the times.
# The planservice needs to be running before this script.
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
import os
import re
import sys

query_cmd = 'build/release/service/runquery -profile_output_file=""'
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
#   - output: text output with the timing information
#   - query: the query to run
#   - prime_buffer_cache: number of times to run the query to prime the buffer cache
#     This is not useful for very large (e.g. > 2 GB) data sets
#   - iterations: number of times to run the query
def run_query(reference_results, output, query, prime_buffer_cache, iterations):
  print "Query: %s" % (query)
  output += "Query: %s\n" % (query)

  cmd = '%s -query="%s" -iterations=%d -enable_counters=0' % (query_cmd, query, iterations)

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
    for i in range(0, prime_buffer_cache):
      for j in range (0, len(tables)):
        count_cmd = '%s -query="select count(*) from %s"' % (query_cmd, tables[j])
        os.popen(count_cmd, "r")

  avg_time = 0
  stddev = ""
  run_success = 0

  # Run query
  p = os.popen(cmd, "r")
  while 1:
    line = p.readline()
    if not line: break

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

  
  avg_time = float(avg_time)
  
  output += "  Avg Time: %fs\n" % (avg_time)
  if len(stddev) != 0:
    output += "  Std Dev:  " + stddev + "s\n"

  # TODO: some kind of statistics against reference_result/reference_stddev?
  if reference_avg != 0:
    if avg_time < reference_avg:
      diff = (reference_avg - avg_time) / reference_avg * 100
      print "  Avg Time: %fs (-%s%.2f%%%s)" % (avg_time, GREEN, diff, END)
    else:
      diff = (avg_time - reference_avg) / reference_avg * 100
      print "  Avg Time: %fs (+%s%.2f%%%s)" % (avg_time, RED, diff, END)
  else:
    print "  Avg Time: %fs" % (avg_time)

  if len(stddev) != 0:
    print "  Std Dev:  " + stddev + "s"
  
  return output

os.chdir(os.environ['IMPALA_BE_DIR'])

reference_results = parse_reference_results()

# This table contains [query, numbers of times to prime buffer cache, number of iterations]
# Queries should be grouped by the data they touch.  This eliminates the need for the buffer
# cache priming iterations.
# TODO: it would be good if this table also contained the expected numbers and automatically 
# flag regressions.  How do we reconcile the fact we are running on different machines?
queries = [
  ["select count(field) from grep1gb where field like '%xyz%'", 5, 5],
  ["select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue from uservisits uv join rankings r on (r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1", 5, 5],
  ["select sourceIP, SUM(adRevenue) FROM uservisits GROUP by sourceIP order by SUM(adRevenue) desc limit 10", 5, 5],
  ["select pageRank, pageURL from rankings where pageRank > 10 order by pageRank limit 100", 0, 5],
  ["select count(field) from grep10gb where field like '%xyz%'", 0, 1]
]

output = ""
for query in queries:
  output = run_query(reference_results, output, query[0], query[1], query[2])

print "\nCopy and paste below to %s/%s to update the reference results:" % (os.environ['IMPALA_HOME'], reference_result_file)
print output
