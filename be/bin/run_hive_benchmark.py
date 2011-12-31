#!/usr/bin/env python

# This script runs the hive bencmark queries and reports the times.
# The planservice needs to be running before this script.
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
import os
import re
import sys

query_cmd = 'build/service/runquery -profile_output_file=""'
result_single_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
result_multiple_regex = 'returned (\d*) rows? in (\d*).(\d*) s with stddev (\d*).(\d*)'

# Function which will run the query and report the average time and standard deviation
#   - query: the query to run
#   - prime_buffer_cache: number of times to run the query to prime the buffer cache
#     This is not useful for very large (e.g. > 2 GB) data sets
#   - iterations: number of times to run the query
def run_query(query, prime_buffer_cache, iterations):
  print "Query: %s" % (query)
  cmd = '%s -query="%s" -iterations=%d -enable_counters=0' % (query_cmd, query, iterations)
  
  # Run the query to prime the buffer cache
  # TODO: it would be great to be able to do this faster.  We need a list of files
  # from the planner and just do cat > /dev/null on those files.
  for i in range(0, prime_buffer_cache):
    os.popen(cmd, "r")

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

  print "  Avg Time: " + avg_time + "s"
  if len(stddev) != 0:
    print "  Std Dev:  " + stddev + "s"

os.chdir(os.environ['IMPALA_BE_DIR'])

# This table contains [query, numbers of times to prime buffer cache, number of iterations]
# Queries should be grouped by the data they touch.  This eliminates the need for the buffer
# cache priming iterations.
# TODO: it would be good if this table also contained the expected numbers and automatically 
# flag regressions.  How do we reconcile the fact we are running on different machines?
queries = [
  ["select count(field) from grep1gb where field like '%xyz%'", 3, 3],
  ["select pageRank, pageURL from rankings where pageRank > 10 order by pageRank limit 100", 2, 3],
  ["select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue from uservisits uv join rankings r on (r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1", 3, 3],
  ["select sourceIP, SUM(adRevenue) FROM uservisits GROUP by sourceIP order by SUM(adRevenue) desc limit 10", 0, 3],
  ["select count(field) from grep10gb where field like '%xyz%'", 0, 1]
]

for query in queries:
  run_query(query[0], query[1], query[2])
