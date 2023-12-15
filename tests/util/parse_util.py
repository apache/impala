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

from __future__ import absolute_import, division, print_function
import re
from datetime import datetime

# IMPALA-6715: Every so often the stress test or the TPC workload directories get
# changed, and the stress test loses the ability to run the full set of queries. Set
# these constants and assert that when a workload is used, all the queries we expect to
# use are there.
EXPECTED_TPCDS_QUERIES_COUNT = 114
EXPECTED_TPCH_NESTED_QUERIES_COUNT = 22
EXPECTED_TPCH_QUERIES_COUNT = 22
# Add the number of stress test specific queries, i.e. in files like '*-stress-*.test'
EXPECTED_TPCH_STRESS_QUERIES_COUNT = EXPECTED_TPCH_QUERIES_COUNT + 3
# Regex to extract the estimated memory from an explain plan.
# The unit prefixes can be found in
# fe/src/main/java/org/apache/impala/common/PrintUtils.java
MEM_ESTIMATE_PATTERN = re.compile(
    r"Per-Host Resource Estimates: Memory=(\d+\.?\d*)(P|T|G|M|K)?B")
NEW_GLOG_ENTRY_PATTERN = re.compile(r"[IWEF](?P<Time>\d{4} \d{2}:\d{2}:\d{2}\.\d{6}).*")


def parse_glog(text, start_time=None):
  '''Parses the log 'text' and returns a list of log entries. If a 'start_time' is
     provided only log entries that are after the time will be returned.
  '''
  year = datetime.now().year
  found_start = False
  log = list()
  entry = None
  for line in text.splitlines():
    if not found_start:
      found_start = line.startswith("Log line format: [IWEF]mmdd hh:mm:ss.uuuuuu")
      continue
    match = NEW_GLOG_ENTRY_PATTERN.match(line)
    if match:
      if entry:
        log.append("\n".join(entry))
      if not start_time or start_time <= datetime.strptime(
          match.group("Time"), "%m%d %H:%M:%S.%f").replace(year):
        entry = [line]
      else:
        entry = None
    elif entry:
      entry.append(line)
  if entry:
    log.append("\n".join(entry))
  return log


def parse_mem_to_mb(mem, units):
  mem = float(mem)
  if mem <= 0:
    return
  units = units.strip().upper() if units else ""
  if units.endswith("B"):
    units = units[:-1]
  if not units:
    mem /= 2 ** 20
  elif units == "K":
    mem /= 2 ** 10
  elif units == "M":
    pass
  elif units == "G":
    mem *= 2 ** 10
  elif units == "T":
    mem *= 2 ** 20
  elif units == "P":
    mem *= 2 ** 30
  else:
    raise Exception('Unexpected memory unit "%s"' % units)
  return int(mem)


def parse_duration_string_ms(duration):
  """Parses a duration string of the form 1h2h3m4s5.6ms4.5us7.8ns into milliseconds."""
  pattern = r'(?P<value>[0-9]+\.?[0-9]*?)(?P<units>\D+)'
  matches = list(re.finditer(pattern, duration))
  assert matches, 'Failed to parse duration string %s' % duration

  times = {'h': 0, 'm': 0, 's': 0, 'ms': 0}
  for match in matches:
    parsed = match.groupdict()
    times[parsed['units']] = float(parsed['value'])

  return (times['h'] * 60 * 60 + times['m'] * 60 + times['s']) * 1000 + times['ms']


def parse_duration_string_ns(duration):
  """Parses a duration string of the form 1h2h3m4s5.6ms4.5us7.8ns into nanoseconds."""
  pattern = r'(?P<value>[0-9]+\.?[0-9]*?)(?P<units>\D+)'
  matches = list(re.finditer(pattern, duration))
  assert matches, 'Failed to parse duration string %s' % duration

  times = {'h': 0, 'm': 0, 's': 0, 'ms': 0, 'us': 0, 'ns': 0}
  for match in matches:
    parsed = match.groupdict()
    times[parsed['units']] = float(parsed['value'])

  value_ns = (times['h'] * 60 * 60 + times['m'] * 60 + times['s']) * 1000000000
  value_ns += times['ms'] * 1000000 + times['us'] * 1000 + times['ns']

  return value_ns


def get_duration_us_from_str(duration_str):
  """Parses the duration string got in profile and returns the duration in us"""
  match_res = re.search(r"\((\d+) us\)", duration_str)
  if match_res:
    return int(match_res.group(1))
  raise Exception("Illegal duration string: " + duration_str)


def match_memory_estimate(explain_lines):
  """
  Given a list of strings from EXPLAIN output, find the estimated memory needed. This is
  used as a binary search start point.

  Params:
    explain_lines: list of str

  Returns:
    2-tuple str of memory limit in decimal string and units (one of 'P', 'T', 'G', 'M',
    'K', '' bytes)

  Raises:
    Exception if no match found
  """
  # IMPALA-6441: This method is a public, first class method so it can be importable and
  # tested with actual EXPLAIN output to make sure we always find the start point.
  mem_limit, units = None, None
  for line in explain_lines:
    regex_result = MEM_ESTIMATE_PATTERN.search(line)
    if regex_result:
      mem_limit, units = regex_result.groups()
      break
  if None in (mem_limit, units):
    raise Exception('could not parse explain string:\n' + '\n'.join(explain_lines))
  return mem_limit, units


def get_bytes_summary_stats_counter(counter_name, runtime_profile):
  """Extracts a list of TSummaryStatsCounters from a given runtime profile where the units
     are in bytes. Each entry in the returned list corresponds to a single occurrence of
     the counter in the profile. If the counter is present, but it has not been updated,
     an empty TSummaryStatsCounter is returned for that entry. If the counter is not in
     the given profile, an empty list is returned. Here is an example of how this method
     should be used:

       # A single line in a runtime profile used for example purposes.
       runtime_profile = "- ExampleCounter: (Avg: 8.00 KB (8192) ; " \
                                            "Min: 8.00 KB (8192) ; " \
                                            "Max: 8.00 KB (8192) ; " \
                                            "Number of samples: 4)"
       summary_stats = get_bytes_summary_stats_counter("ExampleCounter",
                                                      runtime_profile)
       assert len(summary_stats) == 1
       assert summary_stats[0].sum == summary_stats[0].min_value == \
              summary_stats[0].max_value == 8192 and \
              summary_stats[0].total_num_values == 1
  """
  # This requires the Thrift definitions to be generated. We limit the scope of the import
  # to allow tools like the stress test to import this file without building Impala.
  from RuntimeProfile.ttypes import TSummaryStatsCounter

  regex_summary_stat = re.compile(r"""\(
    Avg:[^\(]*\((?P<avg>[0-9]+)\)\s;\s # Matches Avg: [?].[?] [?]B (?)
    Min:[^\(]*\((?P<min>[0-9]+)\)\s;\s # Matches Min: [?].[?] [?]B (?)
    Max:[^\(]*\((?P<max>[0-9]+)\)\s;\s # Matches Max: [?].[?] [?]B (?)
    Number\sof\ssamples:\s(?P<samples>[0-9]+)\) # Matches Number of samples: ?)""",
                                  re.VERBOSE)

  # First, find all lines that contain the counter name, and then extract the summary
  # stats from each line. If the summary stats cannot be extracted, return a dictionary
  # with values of 0 for all keys.
  summary_stats = []
  for counter in re.findall(counter_name + ".*", runtime_profile):
    summary_stat = re.search(regex_summary_stat, counter)
    # We need to special-case when the counter has not been updated at all because empty
    # summary counters have a different format than updated ones.
    if not summary_stat:
      assert "0 (Number of samples: 0)" in counter
      summary_stats.append(TSummaryStatsCounter(sum=0, total_num_values=0, min_value=0,
                                                max_value=0))
    else:
      summary_stat = summary_stat.groupdict()
      num_samples = int(summary_stat['samples'])
      summary_stats.append(TSummaryStatsCounter(sum=num_samples *
          int(summary_stat['avg']), total_num_values=num_samples,
          min_value=int(summary_stat['min']), max_value=int(summary_stat['max'])))

  return summary_stats


def get_time_summary_stats_counter(counter_name, runtime_profile):
  """Extracts a list of TSummaryStatsCounters from a given runtime profile where the
     units are time. Each entry in the returned list corresponds to a single occurrence
     of the counter in the profile. If the counter is present, but it has not been
     updated, an empty TSummaryStatsCounter is returned for that entry. If the counter
     is not in the given profile, an empty list is returned. All time values are returned
     as nanoseconds. Here is an example of how this method should be used:

       # A single line in a runtime profile used for example purposes.
       runtime_profile = "- ExampleTimer: (Avg: 100.000ms ; " \
                                          "Min: 100.000ms ; " \
                                          "Max: 100.000ms ; " \
                                          "Number of samples: 6)"
       summary_stats = get_bytes_summary_stats_counter("ExampleTimer",
                                                      runtime_profile)
       assert len(summary_stats) == 1
       assert summary_stats[0].sum == summary_stats[0].min_value == \
              summary_stats[0].max_value == 100000000 and \
              summary_stats[0].total_num_values == 6
  """
  # This requires the Thrift definitions to be generated. We limit the scope of the import
  # to allow tools like the stress test to import this file without building Impala.
  from RuntimeProfile.ttypes import TSummaryStatsCounter

  regex_summary_stat = re.compile(r"""\(
    Avg:\s(?P<avg>.*)\s;\s # Matches Avg: ? ;
    Min:\s(?P<min>.*)\s;\s # Matches Min: ? ;
    Max:\s(?P<max>.*)\s;\s # Matches Max: ? ;
    Number\sof\ssamples:\s(?P<samples>[0-9]+)\) # Matches Number of samples: ?)""",
                                  re.VERBOSE)

  summary_stats = []
  for counter in re.findall(counter_name + ".*", runtime_profile):
    summary_stat = re.search(regex_summary_stat, counter)
    # We need to special-case when the counter has not been updated at all because empty
    # summary counters have a different format than updated ones.
    if not summary_stat:
      assert "0ns (Number of samples: 0)" in counter
      summary_stats.append(TSummaryStatsCounter(sum=0, total_num_values=0, min_value=0,
                                                max_value=0))
    else:
      summary_stat = summary_stat.groupdict()
      num_samples = int(summary_stat['samples'])
      summary_stats.append(TSummaryStatsCounter(total_num_values=num_samples,
          sum=num_samples * parse_duration_string_ns(summary_stat['avg']),
          min_value=parse_duration_string_ns(summary_stat['min']),
          max_value=parse_duration_string_ns(summary_stat['max'])))

  return summary_stats
