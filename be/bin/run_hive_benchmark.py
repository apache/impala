#!/usr/bin/env python

# This script runs the hive bencmark queries and reports the times.
# The planservice needs to be running before this script.
# The script parses for output in the specific format in the regex below (result_regex).
# This is not very robust but probably okay for this script.
import os
import re
import sys

result_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
query_cmd = 'build/service/runquery -profile_output_file=""'

def run_query(query, iterations=3):
  print "Query: %s" % (query)
  cmd = '%s -query="%s" -iterations=%d' % (query_cmd, query, iterations)
  p = os.popen(cmd, "r")
  total_seconds = 0
  total_ms = 0
  run_count = 1
  while 1:
    line = p.readline()
    if not line: break
    match = re.search(result_regex, line)
    if match:
      total_seconds += int(match.group(2))
      total_ms += int(match.group(3))
      print "    Run %d: %s.%ss" % (run_count, match.group(2), match.group(3))
      run_count += 1

  if run_count == 1:
    print "Query did not run succesfully"
    sys.exit(1)

  avg_time = ('%s.%s') % (total_seconds / iterations, total_ms / iterations)
  print "  Avg Run: " + avg_time + "s."

os.chdir(os.environ['IMPALA_BE_DIR'])

# Should take < 20 secs.
run_query("select count(field) from grep1gb where field like '%xyz%'")
run_query("select pageRank, pageURL from rankings where pageRank > 10 order by pageRank limit 100")
run_query("select sourceIP, SUM(adRevenue) FROM uservisits GROUP by sourceIP order by SUM(adRevenue) desc limit 10")
run_query("select uv.sourceip, avg(r.pagerank), sum(uv.adrevenue) as totalrevenue from uservisits uv join rankings r on (r.pageurl = uv.desturl) where uv.visitdate > '1999-01-01' and uv.visitdate < '2000-01-01' group by uv.sourceip order by totalrevenue desc limit 1")

# Should take less than 5 min
run_query("select count(field) from grep10gb where field like '%xyz%'", 1)
