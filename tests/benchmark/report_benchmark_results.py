#!/usr/bin/env impala-python3
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

# This script provides help with parsing and reporting of perf results. It currently
# provides three main capabilities:
# 1) Printing perf results to console in 'pretty' format
# 2) Comparing two perf result sets together and displaying comparison results to console
# 3) Outputting the perf results in JUnit format which is useful for plugging in to
#   Jenkins perf reporting.

# By default in Python if you divide an int by another int (5 / 2), the result will also
# be an int (2). The following line changes this behavior so that float will be returned
# if necessary (2.5).

from __future__ import absolute_import, division, print_function
from builtins import range
import difflib
import json
import logging
import math
import os
import prettytable
import re
from collections import defaultdict
from datetime import date
from optparse import OptionParser
from tests.util.calculation_util import (
    calculate_tval, calculate_avg, calculate_stddev, calculate_geomean, calculate_mwu)

LOG = logging.getLogger(__name__)

# String constants
AVG = 'avg'
AVG_TIME = 'avg_time'
BASELINE_AVG = 'baseline_avg'
BASELINE_MAX = 'baseline_max'
CLIENT_NAME = 'client_name'
COMPRESSION_CODEC = 'compression_codec'
COMPRESSION_TYPE = 'compression_type'
DELTA_AVG = 'delta_avg'
DELTA_MAX = 'delta_max'
DELTA_RSTD = 'delta_rstd'
DETAIL = 'detail'
EST_NUM_ROWS = 'est_num_rows'
EST_PEAK_MEM = 'est_peak_mem'
EXECUTOR_NAME = 'executor_name'
EXEC_SUMMARY = 'exec_summary'
FILE_FORMAT = 'file_format'
GEOMEAN = 'geomean'
ITERATIONS = 'iterations'
MAX_TIME = 'max_time'
NAME = 'name'
NUM_CLIENTS = 'num_clients'
NUM_HOSTS = 'num_hosts'
NUM_INSTANCES = 'num_instances'
NUM_ROWS = 'num_rows'
OPERATOR = 'operator'
PEAK_MEM = 'peak_mem'
PERCENT_OF_QUERY = 'percent_of_query'
PREFIX = 'prefix'
QUERY = 'query'
QUERY_STR = 'query_str'
REF_RSTD = 'ref_rstd'
RESULT_LIST = 'result_list'
RSTD = 'rstd'
RUNTIME_PROFILE = 'runtime_profile'
SCALE_FACTOR = 'scale_factor'
SORTED = 'sorted'
STDDEV = 'stddev'
STDDEV_TIME = 'stddev_time'
TEST_VECTOR = 'test_vector'
TIME_TAKEN = 'time_taken'
WORKLOAD_NAME = 'workload_name'

parser = OptionParser()
parser.add_option("--input_result_file", dest="result_file",
                 default=os.environ['IMPALA_HOME'] + '/benchmark_results.json',
                 help="The input JSON file with benchmark results")
parser.add_option("--hive_results", dest="hive_results", action="store_true",
                 help="Process results generated from queries ran against Hive")
parser.add_option("--reference_result_file", dest="reference_result_file",
                 default=os.environ['IMPALA_HOME'] + '/reference_benchmark_results.json',
                 help="The input JSON file with reference benchmark results")
parser.add_option("--junit_output_file", dest="junit_output_file", default='',
                 help='If set, outputs results in Junit format to the specified file')
parser.add_option("--no_output_table", dest="no_output_table", action="store_true",
                 default=False, help='Outputs results in table format to the console')
parser.add_option("--report_description", dest="report_description", default=None,
                 help='Optional description for the report.')
parser.add_option("--cluster_name", dest="cluster_name", default='UNKNOWN',
                 help="Name of the cluster the results are from (ex. Bolt)")
parser.add_option("--verbose", "-v", dest="verbose", action="store_true",
                 default=False, help='Outputs to console with with increased verbosity')
parser.add_option("--output_all_summary_nodes", dest="output_all_summary_nodes",
                 action="store_true", default=False,
                 help='Print all execution summary nodes')
parser.add_option("--build_version", dest="build_version", default='UNKNOWN',
                 help="Build/version info about the Impalad instance results are from.")
parser.add_option("--lab_run_info", dest="lab_run_info", default='UNKNOWN',
                 help="Information about the lab run (name/id) that published "
                 "the results.")
parser.add_option("--run_user_name", dest="run_user_name", default='anonymous',
                 help="User name that this run is associated with in the perf database")
parser.add_option("--tval_threshold", dest="tval_threshold", default=3.0,
                 type="float", help="The ttest t-value at which a performance change "
                 "will be flagged as sigificant.")
parser.add_option("--zval_threshold", dest="zval_threshold", default=3.0, type="float",
                  help="The Mann-Whitney Z-value at which a performance change will be "
                  "flagged as sigificant.")
parser.add_option("--min_percent_change_threshold",
                 dest="min_percent_change_threshold", default=5.0,
                 type="float", help="Any performance changes below this threshold"
                 " will not be classified as significant. If the user specifies an"
                 " empty value, the threshold will be set to 0")
parser.add_option("--max_percent_change_threshold",
                 dest="max_percent_change_threshold", default=float("inf"),
                 type="float", help="Any performance changes above this threshold"
                 " will be classified as significant. If the user specifies an"
                 " empty value, the threshold will be set to positive infinity")
parser.add_option("--allowed_latency_diff_secs",
                 dest="allowed_latency_diff_secs", default=0.0, type="float",
                 help="If specified, only a timing change that differs by more than"
                 " this value will be considered significant.")
options, args = parser.parse_args()


def get_dict_from_json(filename):
  """Given a JSON file, return a nested dictionary.

  Everything in this file is based on the nested dictionary data structure. The dictionary
  is structured as follows: Top level maps to workload. Each workload maps to file_format.
  Each file_format maps to queries. Each query contains a key "result_list" that maps to a
  list of ImpalaQueryResult (look at query.py) dictionaries. The compute stats method
  add additional keys such as "avg" or "stddev" here.

  Here's how the keys are structred:
    To get a workload, the key looks like this:
      (('workload_name', 'tpch'), ('scale_factor', '300gb'))
    Each workload has a key that looks like this:
      (('file_format', 'text'), ('compression_codec', 'zip'),
      ('compression_type', 'block'))
    Each Query has a key like this:
      (('name', 'TPCH_Q10'))

  This is useful for finding queries in a certain category and computing stats

  Args:
    filename (str): path to the JSON file

  returns:
    dict: a nested dictionary with grouped queries
  """

  def add_result(query_result):
    """Add query to the dictionary.

    Automatically finds the path in the nested dictionary and adds the result to the
    appropriate list.

    TODO: This method is hard to reason about, so it needs to be made more streamlined.
    """

    def get_key(level_num):
      """Build a key for a particular nesting level.

      The key is built by extracting the appropriate values from query_result.
      """

      level = list()
      # In the outer layer, we group by workload name and scale factor
      level.append([('query', 'workload_name'), ('query', 'scale_factor')])
      # In the middle layer, we group by file format and compression type
      level.append([('query', 'test_vector', 'file_format'),
                    ('query', 'test_vector', 'compression_codec'),
                    ('query', 'test_vector', 'compression_type')])
      # In the bottom layer, we group by query name
      level.append([('query', 'name')])

      key = []

      def get_nested_val(path):
        """given a path to a variable in query result, extract the value.

        For example, to extract compression_type from the query_result, we need to follow
        the this path in the nested dictionary:
        "query_result" -> "query" -> "test_vector" -> "compression_type"
        """
        cur = query_result
        for step in path:
          cur = cur[step]
        return cur

      for path in level[level_num]:
        key.append((path[-1], get_nested_val(path)))

      return tuple(key)

    # grouped is the nested dictionary defined in the outer function get_dict_from_json.
    # It stores all the results grouped by query name and other parameters.
    cur = grouped
    # range(3) because there are 3 levels of nesting, as defined in get_key
    for level_num in range(3):
      cur = cur[get_key(level_num)]
    cur[RESULT_LIST].append(query_result)

  with open(filename, "rb") as f:
    data = json.loads(f.read().decode("utf-8", "ignore"))
    grouped = defaultdict(lambda: defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))))
    for workload_name, workload in data.items():
      for query_result in workload:
        if query_result['success']:
          add_result(query_result)
    calculate_time_stats(grouped)
    return grouped


def all_query_results(grouped):
  for workload_scale, workload in grouped.items():
    for file_format, queries in workload.items():
      for query_name, results in queries.items():
        yield(results)


def get_commit_date(commit_sha):
  try:
    from urllib.request import Request, urlopen
  except ImportError:
    from urllib2 import Request, urlopen

  url = 'https://api.github.com/repos/apache/impala/commits/' + commit_sha
  try:
    request = Request(url)
    response = urlopen(request).read()
    data = json.loads(response.decode('utf8'))
    return data['commit']['committer']['date'][:10]
  except Exception:
    return ''


def get_impala_version(grouped):
  """Figure out Impala version by looking at query profile."""
  first_result = next(all_query_results(grouped))
  profile = first_result['result_list'][0]['runtime_profile']
  match = re.search(r'Impala Version:\s(.*)\s\(build\s(.*)\)', profile)
  version = match.group(1)
  commit_sha = match.group(2)
  commit_date = get_commit_date(commit_sha)
  return '{0} ({1})'.format(version, commit_date)


def calculate_time_stats(grouped):
  """
  Add statistics to the nested dictionary.

  Each query name is supplemented with the average, standard deviation, number of clients,
  iterations, and a sorted list of the time taken to complete each run.
  """

  def remove_first_run(result_list):
    """We want to remove the first result because the performance is much worse on the
    first run.
    """
    if len(result_list) > 1:
      # We want to remove the first result only if there is more that one result
      result_list.remove(min(result_list, key=lambda result: result['start_time']))

  for workload_scale, workload in grouped.items():
    for file_format, queries in workload.items():
      for query_name, results in queries.items():
        result_list = results[RESULT_LIST]
        remove_first_run(result_list)
        avg = calculate_avg(
            [query_results[TIME_TAKEN] for query_results in result_list])
        dev = calculate_stddev(
            [query_results[TIME_TAKEN] for query_results in result_list])
        num_clients = max(
            int(query_results[CLIENT_NAME]) for query_results in result_list)

        iterations = int((len(result_list) + 1) / num_clients)
        results[AVG] = avg
        results[STDDEV] = dev
        results[NUM_CLIENTS] = num_clients
        results[ITERATIONS] = iterations
        results[SORTED] = [query_results[TIME_TAKEN] for query_results in result_list]
        results[SORTED].sort()


class Report(object):

  significant_perf_change = False
  invalid_t_tests = False

  class FileFormatComparisonRow(object):
    """Represents a row in the overview table, where queries are grouped together and
    average and geomean are calculated per workload and file format (first table in the
    report).
    """
    def __init__(self, workload_scale, file_format, queries, ref_queries):

      time_list = []
      ref_time_list = []
      for query_name, results in queries.items():
        if query_name in ref_queries:
          # We want to calculate the average and geomean of the query only if it is both
          # results and reference results
          for query_results in results[RESULT_LIST]:
            time_list.append(query_results[TIME_TAKEN])
          ref_results = ref_queries[query_name]
          for ref_query_results in ref_results[RESULT_LIST]:
            ref_time_list.append(ref_query_results[TIME_TAKEN])

      self.workload_name = '{0}({1})'.format(
          workload_scale[0][1].upper(), workload_scale[1][1])

      self.file_format = '{0} / {1} / {2}'.format(
          file_format[0][1], file_format[1][1], file_format[2][1])

      self.avg = calculate_avg(time_list)
      ref_avg = calculate_avg(ref_time_list)

      self.delta_avg = calculate_change(self.avg, ref_avg)

      self.geomean = calculate_geomean(time_list)
      ref_geomean = calculate_geomean(ref_time_list)

      self.delta_geomean = calculate_change(self.geomean, ref_geomean)

  class QueryComparisonRow(object):
    """Represents a row in the table where individual queries are shown (second table in
    the report).
    """
    def __init__(self, results, ref_results):
      self.workload_name = '{0}({1})'.format(
          results[RESULT_LIST][0][QUERY][WORKLOAD_NAME].upper(),
          results[RESULT_LIST][0][QUERY][SCALE_FACTOR])
      self.query_name = results[RESULT_LIST][0][QUERY][NAME]
      self.file_format = '{0} / {1} / {2}'.format(
          results[RESULT_LIST][0][QUERY][TEST_VECTOR][FILE_FORMAT],
          results[RESULT_LIST][0][QUERY][TEST_VECTOR][COMPRESSION_CODEC],
          results[RESULT_LIST][0][QUERY][TEST_VECTOR][COMPRESSION_TYPE])
      self.avg = results[AVG]
      self.rsd = results[STDDEV] / self.avg if self.avg > 0 else 0.0
      self.significant_variability = True if self.rsd > 0.1 else False
      self.num_clients = results[NUM_CLIENTS]
      self.iters = results[ITERATIONS]

      if ref_results is None:
        self.perf_change = False
        self.zval = 0
        self.tval = 0
        # If reference results are not present, comparison columns will have inf in them
        self.base_avg = float('-inf')
        self.base_rsd = float('-inf')
        self.delta_avg = float('-inf')
        self.perf_change_str = ''
      else:
        median = results[SORTED][int(len(results[SORTED]) / 2)]
        all_diffs = [x - y for x in results[SORTED] for y in ref_results[SORTED]]
        all_diffs.sort()
        self.median_diff = all_diffs[int(len(all_diffs) / 2)] / median
        self.perf_change, self.zval, self.tval = (
          self.__check_perf_change_significance(results, ref_results))
        self.base_avg = ref_results[AVG]
        self.base_rsd = ref_results[STDDEV] / self.base_avg if self.base_avg > 0 else 0.0
        self.delta_avg = calculate_change(self.avg, self.base_avg)
        if self.perf_change:
          self.perf_change_str = self.__build_perf_change_str(
              results, ref_results, self.zval, self.tval)
          Report.significant_perf_change = True
        else:
          self.perf_change_str = ''

      if not options.hive_results:
        try:
          save_runtime_diffs(results, ref_results, self.perf_change, self.zval, self.tval)
        except Exception as e:
          LOG.error('Could not generate an html diff: {0}'.format(e))

    def __check_perf_change_significance(self, stat, ref_stat):
      zval = calculate_mwu(stat[SORTED], ref_stat[SORTED])
      try:
        tval = calculate_tval(stat[AVG], stat[STDDEV], stat[ITERATIONS],
                              ref_stat[AVG], ref_stat[STDDEV], ref_stat[ITERATIONS])
      except ZeroDivisionError:
        # t-test cannot be performed if both standard deviations are 0
        tval = float('nan')
        Report.invalid_t_tests = True
      try:
        percent_difference = abs(ref_stat[AVG] - stat[AVG]) * 100 / ref_stat[AVG]
      except ZeroDivisionError:
        percent_difference = 0.0
      absolute_difference = abs(ref_stat[AVG] - stat[AVG])
      if absolute_difference < options.allowed_latency_diff_secs:
        return False, zval, tval
      if percent_difference < options.min_percent_change_threshold:
        return False, zval, tval
      return (abs(zval) > options.zval_threshold or abs(tval) > options.tval_threshold,
              zval, tval)

    def __build_perf_change_str(self, result, ref_result, zval, tval):
      """Build a performance change string.

      For example:
      Regression: TPCDS-Q52 [parquet/none/none] (1.390s -> 1.982s [+42.59%])
      """
      perf_change_type = ("(R) Regression" if zval >= 0 and tval >= 0
                          else "(I) Improvement" if zval <= 0 and tval <= 0
                          else "(N/A) Invalid t-test" if math.isnan(tval)
                          else "(?) Anomoly")
      query = result[RESULT_LIST][0][QUERY]

      workload_name = '{0}({1})'.format(
          query[WORKLOAD_NAME].upper(),
          query[SCALE_FACTOR])
      query_name = query[NAME]
      file_format = query[TEST_VECTOR][FILE_FORMAT]
      compression_codec = query[TEST_VECTOR][COMPRESSION_CODEC]
      compression_type = query[TEST_VECTOR][COMPRESSION_TYPE]

      template = ("{perf_change_type}: "
                 "{workload_name} {query_name} "
                 "[{file_format} / {compression_codec} / {compression_type}] "
                 "({ref_avg:.2f}s -> {avg:.2f}s [{delta:+.2%}])\n")

      perf_change_str = template.format(
          perf_change_type=perf_change_type,
          workload_name=workload_name,
          query_name=query_name,
          file_format=file_format,
          compression_codec=compression_codec,
          compression_type=compression_type,
          ref_avg=ref_result[AVG],
          avg=result[AVG],
          delta=calculate_change(result[AVG], ref_result[AVG]))

      perf_change_str += build_exec_summary_str(result, ref_result)

      return perf_change_str + '\n'

  class QueryVariabilityRow(object):
    """Represents a row in the query variability table.
    """
    def __init__(self, results, ref_results):

      if ref_results is None:
        self.base_rel_stddev = float('inf')
      else:
        self.base_rel_stddev = ref_results[STDDEV] / ref_results[AVG]\
            if ref_results[AVG] > 0 else 0.0

      self.workload_name = '{0}({1})'.format(
          results[RESULT_LIST][0][QUERY][WORKLOAD_NAME].upper(),
          results[RESULT_LIST][0][QUERY][SCALE_FACTOR])

      self.query_name = results[RESULT_LIST][0][QUERY][NAME]
      self.file_format = results[RESULT_LIST][0][QUERY][TEST_VECTOR][FILE_FORMAT]
      self.compression = results[RESULT_LIST][0][QUERY][TEST_VECTOR][COMPRESSION_CODEC]\
          + ' / ' + results[RESULT_LIST][0][QUERY][TEST_VECTOR][COMPRESSION_TYPE]

      self.rel_stddev = results[STDDEV] / results[AVG] if results[AVG] > 0 else 0.0
      self.significant_variability = self.rel_stddev > 0.1

      variability_template = ("(V) Significant Variability: "
                 "{workload_name} {query_name} [{file_format} / {compression}] "
                 "({base_rel_stddev:.2%} -> {rel_stddev:.2%})\n")

      if self.significant_variability and ref_results:
        # If ref_results do not exist, variability analysis will not be conducted
        self.variability_str = variability_template.format(
            workload_name=self.workload_name,
            query_name=self.query_name,
            file_format=self.file_format,
            compression=self.compression,
            base_rel_stddev=self.base_rel_stddev,
            rel_stddev=self.rel_stddev)

        self.exec_summary_str = build_exec_summary_str(
            results, ref_results, for_variability=True)
      else:
        self.variability_str = str()
        self.exec_summary_str = str()

    def __str__(self):
      return self.variability_str + self.exec_summary_str

  def __init__(self, grouped, ref_grouped):
    self.grouped = grouped
    self.ref_grouped = ref_grouped
    self.query_comparison_rows = []
    self.file_format_comparison_rows = []
    self.query_variability_rows = []
    self.__analyze()

  def __analyze(self):
    """Generates a comparison data that can be printed later"""

    for workload_scale, workload in self.grouped.items():
      for file_format, queries in workload.items():
        if self.ref_grouped is not None and workload_scale in self.ref_grouped and\
            file_format in self.ref_grouped[workload_scale]:
          ref_queries = self.ref_grouped[workload_scale][file_format]
          self.file_format_comparison_rows.append(Report.FileFormatComparisonRow(
            workload_scale, file_format, queries, ref_queries))
        else:
          # If not present in reference results, set to None
          ref_queries = None
        for query_name, results in queries.items():
          if self.ref_grouped is not None and workload_scale in self.ref_grouped and\
              file_format in self.ref_grouped[workload_scale] and query_name in\
              self.ref_grouped[workload_scale][file_format]:
            ref_results = self.ref_grouped[workload_scale][file_format][query_name]
            query_comparison_row = Report.QueryComparisonRow(results, ref_results)
            self.query_comparison_rows.append(query_comparison_row)

            query_variability_row = Report.QueryVariabilityRow(results, ref_results)
            self.query_variability_rows.append(query_variability_row)
          else:
            # If not present in reference results, set to None
            ref_results = None

  def __str__(self):
    output = str()

    # per file format analysis overview table
    table = prettytable.PrettyTable(['Workload', 'File Format', 'Avg (s)', 'Delta(Avg)',
                                     'GeoMean(s)', 'Delta(GeoMean)'])
    table.float_format = '.2'
    table.align = 'l'
    self.file_format_comparison_rows.sort(
        key=lambda row: row.delta_geomean, reverse=True)
    for row in self.file_format_comparison_rows:
      table_row = [
          row.workload_name,
          row.file_format,
          row.avg,
          '{0:+.2%}'.format(row.delta_avg),
          row.geomean,
          '{0:+.2%}'.format(row.delta_geomean)]
      table.add_row(table_row)

    output += str(table) + '\n\n'

    # main comparison table
    detailed_performance_change_analysis_str = str()
    table = prettytable.PrettyTable(['Workload', 'Query', 'File Format', 'Avg(s)',
                                     'Base Avg(s)', 'Delta(Avg)', 'StdDev(%)',
                                     'Base StdDev(%)', 'Iters', 'Median Diff(%)',
                                     'MW Zval', 'Tval'])
    table.float_format = '.2'
    table.align = 'l'
    # Sort table from worst to best regression
    self.query_comparison_rows.sort(key=lambda row: row.delta_avg + row.median_diff,
                                    reverse=True)
    for row in self.query_comparison_rows:
      delta_avg_template = '  {0:+.2%}' if not row.perf_change else (
        'R {0:+.2%}' if row.zval >= 0 and row.tval >= 0 else 'I {0:+.2%}' if row.zval <= 0
        and row.tval <= 0 else '? {0:+.2%}')

      table_row = [
          row.workload_name,
          row.query_name,
          row.file_format,
          row.avg,
          row.base_avg if row.base_avg != float('-inf') else 'N/A',
          '   N/A' if row.delta_avg == float('-inf') else delta_avg_template.format(
            row.delta_avg),
          ('* {0:.2%} *' if row.rsd > 0.1 else '  {0:.2%}  ').format(row.rsd),
          '  N/A' if row.base_rsd == float('-inf') else (
            '* {0:.2%} *' if row.base_rsd > 0.1 else '  {0:.2%}  ').format(row.base_rsd),
          row.iters,
          '   N/A' if row.median_diff == float('-inf') else delta_avg_template.format(
            row.median_diff),
          row.zval,
          row.tval]
      table.add_row(table_row)
      detailed_performance_change_analysis_str += row.perf_change_str

    output += str(table) + '\n\n'
    output += detailed_performance_change_analysis_str

    variability_analysis_str = str()
    self.query_variability_rows.sort(key=lambda row: row.rel_stddev, reverse=True)
    for row in self.query_variability_rows:
      variability_analysis_str += str(row)

    output += variability_analysis_str

    if Report.significant_perf_change:
      output += 'Significant perf change detected.\n'

    if Report.invalid_t_tests:
      output += 'Invalid t-tests detected. It is not possible to perform t-test with ' \
                '0 standard deviation. Try increasing the number of iterations.\n'

    return output


class CombinedExecSummaries(object):
  """All execution summaries for each query are combined into this object.

  The overall average time is calculated for each node by averaging the average time
  from each execution summary. The max time time is calculated by getting the max time
  of max times.

  This object can be compared to another one and ExecSummaryComparison can be generated.

  Args:
    exec_summaries (list of list of dict): A list of exec summaries (list of dict is how
      it is received from the beeswax client.

  Attributes:
    rows (list of dict): each dict represents a row in the summary table. Each row in rows
    is a dictionary. Each dictionary has the following keys:
      prefix (str)
      operator (str)
      num_hosts (int)
      num_instances (int)
      num_rows (int)
      est_num_rows (int)
      detail (str)
      avg_time (float): averge of average times in all the execution summaries
      stddev_time: standard deviation of times in all the execution summaries
      max_time: maximum of max times in all the execution summaries
      peak_mem (int)
      est_peak_mem (int)
  """

  def __init__(self, exec_summaries):
    # We want to make sure that all execution summaries have the same structure before
    # we can combine them. If not, err_str will contain the reason why we can't combine
    # the exec summaries.
    ok, err_str = self.__check_exec_summary_schema(exec_summaries)
    self.error_str = err_str

    self.rows = []
    if ok:
      self.__build_rows(exec_summaries)

  def __build_rows(self, exec_summaries):

    first_exec_summary = exec_summaries[0]

    for row_num, row in enumerate(first_exec_summary):
      combined_row = {}
      # Copy fixed values from the first exec summary
      for key in [PREFIX, OPERATOR, NUM_HOSTS, NUM_INSTANCES, NUM_ROWS, EST_NUM_ROWS,
                  DETAIL]:
        combined_row[key] = row[key]

      avg_times = [exec_summary[row_num][AVG_TIME] for exec_summary in exec_summaries]
      max_times = [exec_summary[row_num][MAX_TIME] for exec_summary in exec_summaries]
      peak_mems = [exec_summary[row_num][PEAK_MEM] for exec_summary in exec_summaries]
      est_peak_mems = [exec_summary[row_num][EST_PEAK_MEM]
          for exec_summary in exec_summaries]

      # Set the calculated values
      combined_row[AVG_TIME] = calculate_avg(avg_times)
      combined_row[STDDEV_TIME] = calculate_stddev(avg_times)
      combined_row[MAX_TIME] = max(max_times)
      combined_row[PEAK_MEM] = max(peak_mems)
      combined_row[EST_PEAK_MEM] = max(est_peak_mems)
      self.rows.append(combined_row)

  def is_same_schema(self, reference):
    """Check if the reference CombinedExecSummaries summary has the same schema as this
    one. (For example, the operator names are the same for each node).

    The purpose of this is to check if it makes sense to combine this object with a
    reference one to produce ExecSummaryComparison.

    Args:
      reference (CombinedExecSummaries): comparison

    Returns:
      bool: True if the schama's are similar enough to be compared, False otherwise.
    """

    if len(self.rows) != len(reference.rows): return False

    for row_num, row in enumerate(self.rows):
      ref_row = reference.rows[row_num]

      if row[OPERATOR] != ref_row[OPERATOR]:
        return False

    return True

  def __str__(self):
    if self.error_str: return self.error_str

    table = prettytable.PrettyTable(
        ["Operator",
          "#Hosts",
          "#Inst",
          "Avg Time",
          "Std Dev",
          "Max Time",
          "#Rows",
          "Est #Rows"])
    table.align = 'l'
    table.float_format = '.2'

    for row in self.rows:
      table_row = [row[PREFIX] + row[OPERATOR],
          prettyprint_values(row[NUM_HOSTS]),
          prettyprint_values(row[NUM_INSTANCES]),
          prettyprint_time(row[AVG_TIME]),
          prettyprint_time(row[STDDEV_TIME]),
          prettyprint_time(row[MAX_TIME]),
          prettyprint_values(row[NUM_ROWS]),
          prettyprint_values(row[EST_NUM_ROWS])]
      table.add_row(table_row)

    return str(table)

  @property
  def total_runtime(self):
    return sum([row[AVG_TIME] for row in self.rows])

  def __check_exec_summary_schema(self, exec_summaries):
    """Check if all given exec summaries have the same structure.

    This method is called to check if it is possible a single CombinedExecSummaries from
    the list of exec_summaries. (For example all exec summaries must have the same
    number of nodes.)

    This method is somewhat similar to is_same_schema. The difference is that
    is_same_schema() checks if two CombinedExecSummaries have the same structure and this
    method checks if all exec summaries in the list have the same structure.

    Args:
      exec_summaries (list of dict): each dict represents an exec_summary

    Returns:
      (bool, str): True if all exec summaries have the same structure, otherwise False
      followed by a string containing the explanation.
    """

    err = 'Summaries cannot be combined: '

    if len(exec_summaries) < 1:
      return False, err + 'no exec summaries Found'

    first_exec_summary = exec_summaries[0]

    # This check is for Metadata queries which don't have summaries
    if len(first_exec_summary) < 1:
      return False, err + 'exec summary contains no nodes'

    for exec_summary in exec_summaries:
      if len(exec_summary) != len(first_exec_summary):
        return False, err + 'different number of nodes in exec summaries'

      for row_num, row in enumerate(exec_summary):
        comp_row = first_exec_summary[row_num]
        if row[OPERATOR] != comp_row[OPERATOR]:
          return False, err + 'different operator'

    return True, str()


class ExecSummaryComparison(object):
  """Represents a comparison between two CombinedExecSummaries.

  Args:
    combined_summary (CombinedExecSummaries): current summary.
    ref_combined_summary (CombinedExecSummaries): reference summaries.

  Attributes:
    rows (list of dict): Each dict represents a single row. Each dict has the following
    keys:
      prefix (str)
      operator (str)
      num_hosts (int)
      num_instances (int)
      avg_time (float)
      stddev_time (float)
      avg_time_change (float): % change in avg time compared to reference
      avg_time_change_total (float): % change in avg time compared to total of the query
      max_time (float)
      max_time_change (float): % change in max time compared to reference
      peak_mem (int)
      peak_mem_change (float): % change compared to reference
      num_rows (int)
      est_num_rows (int)
      est_peak_mem (int)
      detail (str)
    combined_summary (CombinedExecSummaries): original combined summary
    ref_combined_summary (CombinedExecSummaries): original reference combined summary.
      If the comparison cannot be constructed, these summaries can be printed.

  Another possible way to implement this is to generate this object when we call
  CombinedExecSummaries.compare(reference).
  """

  def __init__(self, combined_summary, ref_combined_summary, for_variability=False):

    # Store the original summaries, in case we can't build a comparison
    self.combined_summary = combined_summary
    self.ref_combined_summary = ref_combined_summary

    # If some error happened during calculations, store it here
    self.error_str = str()
    self.for_variability = for_variability

    self.rows = []

  def __build_rows(self):

    if self.combined_summary.is_same_schema(self.ref_combined_summary):
      for i, row in enumerate(self.combined_summary.rows):
        ref_row = self.ref_combined_summary.rows[i]

        comparison_row = {}
        for key in [PREFIX, OPERATOR, NUM_HOSTS, NUM_INSTANCES, AVG_TIME, STDDEV_TIME,
            MAX_TIME, PEAK_MEM, NUM_ROWS, EST_NUM_ROWS, EST_PEAK_MEM, DETAIL]:
          comparison_row[key] = row[key]

        comparison_row[PERCENT_OF_QUERY] = row[AVG_TIME] /\
            self.combined_summary.total_runtime\
            if self.combined_summary.total_runtime > 0 else 0.0

        comparison_row[RSTD] = row[STDDEV_TIME] / row[AVG_TIME]\
            if row[AVG_TIME] > 0 else 0.0

        comparison_row[BASELINE_AVG] = ref_row[AVG_TIME]

        comparison_row[DELTA_AVG] = calculate_change(
            row[AVG_TIME], ref_row[AVG_TIME])

        comparison_row[BASELINE_MAX] = ref_row[MAX_TIME]

        comparison_row[DELTA_MAX] = calculate_change(
            row[MAX_TIME], ref_row[MAX_TIME])

        self.rows.append(comparison_row)
    else:
      self.error_str = 'Execution summary structures are different'

  def __str__(self):
    """Construct a PrettyTable containing the comparison"""
    if self.for_variability:
      return str(self.__build_table_variability())
    else:
      return str(self.__build_table())

  def __build_rows_variability(self):
    if self.ref_combined_summary and self.combined_summary.is_same_schema(
        self.ref_combined_summary):
      for i, row in enumerate(self.combined_summary.rows):
        ref_row = self.ref_combined_summary.rows[i]
        comparison_row = {}
        comparison_row[OPERATOR] = row[OPERATOR]

        comparison_row[PERCENT_OF_QUERY] = row[AVG_TIME] /\
            self.combined_summary.total_runtime\
            if self.combined_summary.total_runtime > 0 else 0.0

        comparison_row[RSTD] = row[STDDEV_TIME] / row[AVG_TIME]\
            if row[AVG_TIME] > 0 else 0.0

        comparison_row[REF_RSTD] = ref_row[STDDEV_TIME] / ref_row[AVG_TIME]\
            if ref_row[AVG_TIME] > 0 else 0.0

        comparison_row[DELTA_RSTD] = calculate_change(
            comparison_row[RSTD], comparison_row[REF_RSTD])

        comparison_row[NUM_HOSTS] = row[NUM_HOSTS]
        comparison_row[NUM_INSTANCES] = row[NUM_INSTANCES]
        comparison_row[NUM_ROWS] = row[NUM_ROWS]
        comparison_row[EST_NUM_ROWS] = row[EST_NUM_ROWS]

        self.rows.append(comparison_row)
    else:
      self.error_str = 'Execution summary structures are different'

  def __build_table_variability(self):

    def is_significant(row):
      """Check if the performance change in the row was significant"""
      return options.output_all_summary_nodes or (
        row[RSTD] > 0.1 and row[PERCENT_OF_QUERY] > 0.02)

    self.__build_rows_variability()

    if self.error_str:
      # If the summary comparison could not be constructed, output both summaries
      output = self.error_str + '\n'
      output += 'Execution Summary: \n'
      output += str(self.combined_summary) + '\n'
      output += 'Reference Execution Summary: \n'
      output += str(self.ref_combined_summary)
      return output

    table = prettytable.PrettyTable(
        ['Operator',
          '% of Query',
          'StdDev(%)',
          'Base StdDev(%)',
          'Delta(StdDev(%))',
          '#Hosts',
          '#Inst',
          '#Rows',
          'Est #Rows'])
    table.align = 'l'
    table.float_format = '.2'
    table_contains_at_least_one_row = False
    for row in [r for r in self.rows if is_significant(r)]:
      table_row = [row[OPERATOR],
          '{0:.2%}'.format(row[PERCENT_OF_QUERY]),
          '{0:.2%}'.format(row[RSTD]),
          '{0:.2%}'.format(row[REF_RSTD]),
          '{0:+.2%}'.format(row[DELTA_RSTD]),
          prettyprint_values(row[NUM_HOSTS]),
          prettyprint_values(row[NUM_INSTANCES]),
          prettyprint_values(row[NUM_ROWS]),
          prettyprint_values(row[EST_NUM_ROWS])]

      table_contains_at_least_one_row = True
      table.add_row(table_row)

    if table_contains_at_least_one_row:
      return str(table) + '\n'
    else:
      return 'No Nodes with significant StdDev %\n'

  def __build_table(self):

    def is_significant(row):
      """Check if the performance change in the row was significant"""
      return options.output_all_summary_nodes or (
        row[MAX_TIME] > 100000000
        and row[PERCENT_OF_QUERY] > 0.02)

    self.__build_rows()
    if self.error_str:
      # If the summary comparison could not be constructed, output both summaries
      output = self.error_str + '\n'
      output += 'Execution Summary: \n'
      output += str(self.combined_summary) + '\n'
      output += 'Reference Execution Summary: \n'
      output += str(self.ref_combined_summary)
      return output

    table = prettytable.PrettyTable(
        ['Operator',
          '% of Query',
          'Avg',
          'Base Avg',
          'Delta(Avg)',
          'StdDev(%)',
          'Max',
          'Base Max',
          'Delta(Max)',
          '#Hosts',
          '#Inst',
          '#Rows',
          'Est #Rows'])
    table.align = 'l'
    table.float_format = '.2'

    for row in self.rows:
      if is_significant(row):

        table_row = [row[OPERATOR],
            '{0:.2%}'.format(row[PERCENT_OF_QUERY]),
            prettyprint_time(row[AVG_TIME]),
            prettyprint_time(row[BASELINE_AVG]),
            prettyprint_percent(row[DELTA_AVG]),
            ('* {0:.2%} *' if row[RSTD] > 0.1 else '  {0:.2%}  ').format(row[RSTD]),
            prettyprint_time(row[MAX_TIME]),
            prettyprint_time(row[BASELINE_MAX]),
            prettyprint_percent(row[DELTA_MAX]),
            prettyprint_values(row[NUM_HOSTS]),
            prettyprint_values(row[NUM_INSTANCES]),
            prettyprint_values(row[NUM_ROWS]),
            prettyprint_values(row[EST_NUM_ROWS])]

        table.add_row(table_row)

    return str(table)


def calculate_change(val, ref_val):
  """Calculate how big the change in val compared to ref_val is compared to total"""
  return (val - ref_val) / ref_val if ref_val != 0 else 0.0


def prettyprint(val, units, divisor):
  """ Print a value in human readable format along with it's unit.

  We start at the leftmost unit in the list and keep dividing the value by divisor until
  the value is less than divisor. The value is then printed along with the unit type.

  Args:
    val (int or float): Value to be printed.
    units (list of str): Unit names for different sizes.
    divisor (float): ratio between two consecutive units.
  """
  for unit in units:
    if abs(val) < divisor:
      if unit == units[0]:
        return "%d%s" % (val, unit)
      else:
        return "%3.2f%s" % (val, unit)
    val /= divisor


def prettyprint_bytes(byte_val):
  return prettyprint(byte_val, ['B', 'KB', 'MB', 'GB', 'TB'], 1024.0)


def prettyprint_values(unit_val):
  return prettyprint(unit_val, ["", "K", "M", "B"], 1000.0)


def prettyprint_time(time_val):
  return prettyprint(time_val, ["ns", "us", "ms", "s"], 1000.0)


def prettyprint_percent(percent_val):
  return '{0:+.2%}'.format(percent_val)


def save_runtime_diffs(results, ref_results, change_significant, zval, tval):
  """Given results and reference results, generate and output an HTML file
  containing the Runtime Profile diff.
  """

  diff = difflib.HtmlDiff(wrapcolumn=90, linejunk=difflib.IS_LINE_JUNK)

  # We are comparing last queries in each run because they should have the most
  # stable performance (unlike the first queries)
  runtime_profile = results[RESULT_LIST][-1][RUNTIME_PROFILE]
  ref_runtime_profile = ref_results[RESULT_LIST][-1][RUNTIME_PROFILE]

  template = ('{prefix}-{query_name}-{scale_factor}-{file_format}-{compression_codec}'
              '-{compression_type}-runtime_profile.html')

  query = results[RESULT_LIST][-1][QUERY]

  # Neutral - no improvement or regression
  prefix = 'neu'
  if change_significant:
    prefix = '???'
    if zval >= 0 and tval >= 0:
      prefix = 'reg'
    elif zval <= 0 and tval <= 0:
      prefix = 'imp'

  runtime_profile_file_name = template.format(
      prefix=prefix,
      query_name=query[NAME],
      scale_factor=query[SCALE_FACTOR],
      file_format=query[TEST_VECTOR][FILE_FORMAT],
      compression_codec=query[TEST_VECTOR][COMPRESSION_CODEC],
      compression_type=query[TEST_VECTOR][COMPRESSION_TYPE])

  # Go into results dir
  dir_path = os.path.join(os.environ["IMPALA_HOME"], 'results')

  if not os.path.exists(dir_path):
    os.mkdir(dir_path)
  elif not os.path.isdir(dir_path):
    raise RuntimeError("Unable to create $IMPALA_HOME/results, results file exists")

  runtime_profile_file_path = os.path.join(dir_path, runtime_profile_file_name)

  runtime_profile_diff = diff.make_file(
      ref_runtime_profile.splitlines(),
      runtime_profile.splitlines(),
      fromdesc="Baseline Runtime Profile",
      todesc="Current Runtime Profile")

  with open(runtime_profile_file_path, 'w+') as f:
    f.write(runtime_profile_diff)


def build_exec_summary_str(results, ref_results, for_variability=False):
  # There is no summary available for Hive after query execution
  # Metadata queries don't have execution summary
  exec_summaries = [result[EXEC_SUMMARY] for result in results[RESULT_LIST]]

  if options.hive_results or exec_summaries[0] is None:
    return ""

  combined_summary = CombinedExecSummaries(exec_summaries)

  if ref_results is None:
    ref_exec_summaries = None
    ref_combined_summary = None
  else:
    ref_exec_summaries = [result[EXEC_SUMMARY]
                          for result in ref_results[RESULT_LIST]]
    ref_combined_summary = CombinedExecSummaries(ref_exec_summaries)

  comparison = ExecSummaryComparison(
      combined_summary, ref_combined_summary, for_variability)

  return str(comparison) + '\n'


def build_summary_header(current_impala_version, ref_impala_version):
  summary = "Report Generated on {0}\n".format(date.today())
  if options.report_description:
    summary += 'Run Description: {0}\n'.format(options.report_description)
  if options.cluster_name:
    summary += '\nCluster Name: {0}\n'.format(options.cluster_name)
  if options.lab_run_info:
    summary += 'Lab Run Info: {0}\n'.format(options.lab_run_info)
  if not options.hive_results:
    summary += 'Impala Version:          {0}\n'.format(current_impala_version)
    summary += 'Baseline Impala Version: {0}\n'.format(ref_impala_version)
  return summary


if __name__ == "__main__":
  """Workflow:
  1. Build a nested dictionary for the current result JSON and reference result JSON.
  2. Calculate runtime statistics for each query for both results and reference results.
  5. Save performance statistics to the performance database.
  3. Construct a string with a an overview of workload runtime and detailed performance
     comparison for queries with significant performance change.
  """

  logging.basicConfig(level=logging.DEBUG if options.verbose else logging.INFO)

  # Generate a dictionary based on the JSON file
  grouped = get_dict_from_json(options.result_file)

  try:
    # Generate a dictionary based on the reference JSON file
    ref_grouped = get_dict_from_json(options.reference_result_file)
  except Exception as e:
    # If reference result file could not be read we can still continue. The result can
    # be saved to the performance database.
    LOG.error('Could not read reference result file: {0}'.format(e))
    ref_grouped = None

  report = Report(grouped, ref_grouped)

  ref_impala_version = 'N/A'
  if options.hive_results:
    current_impala_version = 'N/A'
  else:
    current_impala_version = get_impala_version(grouped)
    if ref_grouped:
      ref_impala_version = get_impala_version(ref_grouped)

  print(build_summary_header(current_impala_version, ref_impala_version))
  print(report)
