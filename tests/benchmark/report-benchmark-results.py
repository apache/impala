#!/usr/bin/env python
# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
#
# This script provides help with parsing and reporting of perf results. It currently
# provides three main capabilities:
# 1) Printing perf results to console in 'pretty' format
# 2) Comparing two perf result sets together and displaying comparison results to console
# 3) Outputting the perf results in JUnit format which is useful for plugging in to
#   Jenkins perf reporting.

# By default in Python if you divide an int by another int (5 / 2), the result will also
# be an int (2). The following line changes this behavior so that float will be returned
# if necessary (2.5).
from __future__ import division

import difflib
import json
import math
import os
import prettytable
from collections import defaultdict
from datetime import date, datetime
from optparse import OptionParser
from tests.util.calculation_util import calculate_tval, calculate_avg, calculate_stddev
from time import gmtime, strftime

# String constants
AVG = 'avg'
AVG_TIME = 'avg_time'
AVG_TIME_CHANGE = 'avg_time_change'
AVG_TIME_CHANGE_TOTAL = 'avg_time_change_total'
CLIENT_NAME = 'client_name'
COMPRESSION_CODEC = 'compression_codec'
COMPRESSION_TYPE = 'compression_type'
DETAIL = 'detail'
EST_NUM_ROWS = 'est_num_rows'
EST_PEAK_MEM = 'est_peak_mem'
EXECUTOR_NAME = 'executor_name'
EXEC_SUMMARY = 'exec_summary'
FILE_FORMAT = 'file_format'
ITERATIONS = 'iterations'
MAX_TIME = 'max_time'
MAX_TIME_CHANGE = 'max_time_change'
NAME = 'name'
NUM_CLIENTS = 'num_clients'
NUM_HOSTS = 'num_hosts'
NUM_ROWS = 'num_rows'
OPERATOR = 'operator'
PEAK_MEM = 'peak_mem'
PEAK_MEM_CHANGE = 'peak_mem_change'
PREFIX = 'prefix'
QUERY = 'query'
QUERY_STR = 'query_str'
RESULT_LIST = 'result_list'
RUNTIME_PROFILE = 'runtime_profile'
SCALE_FACTOR = 'scale_factor'
STDDEV = 'stddev'
STDDEV_TIME = 'stddev_time'
TEST_VECTOR = 'test_vector'
TIME_TAKEN = 'time_taken'
TOTAL = 'total'
WORKLOAD_NAME = 'workload_name'

parser = OptionParser()
parser.add_option("--input_result_file", dest="result_file",
                 default=os.environ['IMPALA_HOME'] + '/benchmark_results.json',
                 help="The input JSON file with benchmark results")
parser.add_option("--reference_result_file", dest="reference_result_file",
                 default=os.environ['IMPALA_HOME'] + '/reference_benchmark_results.json',
                 help="The input JSON file with reference benchmark results")
parser.add_option("--junit_output_file", dest="junit_output_file", default='',
                 help='If set, outputs results in Junit format to the specified file')
parser.add_option("--no_output_table", dest="no_output_table", action="store_true",
                 default= False, help='Outputs results in table format to the console')
parser.add_option("--report_description", dest="report_description", default=None,
                 help='Optional description for the report.')
parser.add_option("--cluster_name", dest="cluster_name", default='UNKNOWN',
                 help="Name of the cluster the results are from (ex. Bolt)")
parser.add_option("--verbose", "-v", dest="verbose", action="store_true",
                 default= False, help='Outputs to console with with increased verbosity')
parser.add_option("--build_version", dest="build_version", default='UNKNOWN',
                 help="Build/version info about the Impalad instance results are from.")
parser.add_option("--lab_run_info", dest="lab_run_info", default='UNKNOWN',
                 help="Information about the lab run (name/id) that published "\
                 "the results.")
parser.add_option("--tval_threshold", dest="tval_threshold", default=None,
                 type="float", help="The ttest t-value at which a performance change "\
                 "will be flagged as sigificant.")
parser.add_option("--min_percent_change_threshold",
                 dest="min_percent_change_threshold", default=5.0,
                 type="float", help="Any performance changes below this threshold" \
                 " will not be classified as significant. If the user specifies an" \
                 " empty value, the threshold will be set to 0")
parser.add_option("--max_percent_change_threshold",
                 dest="max_percent_change_threshold", default=20.0,
                 type="float", help="Any performance changes above this threshold"\
                 " will be classified as significant. If the user specifies an" \
                 " empty value, the threshold will be set to the system's maxint")
parser.add_option("--allowed_latency_diff_secs",
                 dest="allowed_latency_diff_secs", default=0.0, type="float",
                 help="If specified, only a timing change that differs by more than\
                 this value will be considered significant.")

# These parameters are specific to recording results in a database. This is optional
parser.add_option("--save_to_db", dest="save_to_db", action="store_true",
                 default= False, help='Saves results to the specified database.')
parser.add_option("--is_official", dest="is_official", action="store_true",
                 default= False, help='Indicates this is an official perf run result')
parser.add_option("--db_host", dest="db_host", default='localhost',
                 help="Machine hosting the database")
parser.add_option("--db_name", dest="db_name", default='perf_results',
                 help="Name of the perf database.")
parser.add_option("--db_username", dest="db_username", default='hiveuser',
                 help="Username used to connect to the database.")
parser.add_option("--db_password", dest="db_password", default='password',
                 help="Password used to connect to the the database.")
options, args = parser.parse_args()

def get_dict_from_json(filename):
  """Given a JSON file, return a nested dictionary.

  Everything in this file is based on the nested dictionary data structure. The dictionary
  is structured as follows: Top level maps to workload. Each workload maps to queries.
  Each query maps to file_format. Each file format is contains a key "result_list" that
  maps to a list of QueryResult (look at query.py) dictionaries. The compute stats method
  add additional keys such as "avg" or "stddev" here.

  Here's how the keys are structred:
    To get a workload, the key looks like this:
      (('workload_name', 'tpch'), ('scale_factor', '300gb'))
    Each workload has a key that looks like this:
      (('name', 'TPCH_Q10'))
    Each Query has a key like this:
      (('file_format', 'text'), ('compression_codec', 'zip'),
      ('compression_type', 'block'))

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
      # In the middle layer, we group by query name
      level.append([('query', 'name')])
      # In the inner layer, we group by file format and compression type
      level.append([('query', 'test_vector', 'file_format'),
      ('query', 'test_vector', 'compression_codec'),
      ('query', 'test_vector', 'compression_type')])

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

  with open(filename, "r") as f:
    data = json.load(f)
    grouped = defaultdict( lambda: defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))))
    for workload_name, workload in data.items():
      for query_result in workload:
        add_result(query_result)
    return grouped

def calculate_time_stats(grouped):
  """Adds statistics to the nested dictionary. We are calculating the average runtime
     and Standard Deviation for each query type.
  """

  for workload_scale in grouped:
    for query_name in grouped[workload_scale]:
      for file_format in grouped[workload_scale][query_name]:
        result_list = grouped[workload_scale][query_name][file_format][RESULT_LIST]
        avg = calculate_avg(
            [query_results[TIME_TAKEN] for query_results in result_list])
        dev = calculate_stddev(
            [query_results[TIME_TAKEN] for query_results in result_list])
        num_clients = max(
            int(query_results[CLIENT_NAME]) for query_results in result_list)
        iterations = len(result_list)

        grouped[workload_scale][query_name][file_format][AVG] = avg
        grouped[workload_scale][query_name][file_format][STDDEV] = dev
        grouped[workload_scale][query_name][file_format][NUM_CLIENTS] = num_clients
        grouped[workload_scale][query_name][file_format][ITERATIONS] = iterations

def calculate_workload_file_format_runtimes(grouped):
  """Calculate average time for each workload and scale factor, for each file format and
  compression.

  This returns a new dictionary with avarage times.

  Here's an example of how this dictionary is structured:
  dictionary->
  (('workload', 'tpch'), ('scale', '300gb'))->
  (('file_format','parquet'), ('compression_codec','zip'), ('compression_type','block'))->
  'avg'

  We also have access to the list of QueryResult associated with each file_format

  The difference between this dictionary and grouped_queries is that query name is missing
  after workload.
  """
  new_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

  # First populate the dictionary with query results
  for workload_scale, workload in grouped.items():
    for query_name, file_formats in workload.items():
      for file_format, results in file_formats.items():
        new_dict[workload_scale][file_format][RESULT_LIST].extend(results[RESULT_LIST])

  # Do the average calculation. Standard deviation could also be calculated here
  for workload_scale in new_dict:
    for file_format in new_dict[workload_scale]:
      avg = calculate_avg([query_results[TIME_TAKEN]
        for query_results in new_dict[workload_scale][file_format][RESULT_LIST]])
      new_dict[workload_scale][file_format][AVG] = avg
  return new_dict

def build_perf_change_str(result, ref_result, regression):
  """Build a performance change string"""

  perf_change_type = "regression" if regression else "improvement"
  query = result[RESULT_LIST][0][QUERY]

  query_name = query[NAME]
  file_format = query[TEST_VECTOR][FILE_FORMAT]
  compression_codec = query[TEST_VECTOR][COMPRESSION_CODEC]
  compression_type = query[TEST_VECTOR][COMPRESSION_TYPE]

  template = ("\nSignificant perf {perf_change_type} detected: "
             "{query_name} [{file_format}/{compression_codec}/{compression_type}] "
             "({ref_avg:.3f}s -> {avg:.3f}s)")
  return template.format(
      perf_change_type = perf_change_type,
      query_name = query_name,
      file_format = file_format,
      compression_codec = compression_codec,
      compression_type = compression_type,
      ref_avg = ref_result[AVG],
      avg = result[AVG])

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
      for key in [PREFIX, OPERATOR, NUM_HOSTS, NUM_ROWS, EST_NUM_ROWS, DETAIL]:
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
          "Avg Time",
          "Std Dev",
          "Max Time",
          "#Rows",
          "Est #Rows"])
    table.align = 'l'

    for row in self.rows:
      table_row = [ row[PREFIX] + row[OPERATOR],
          prettyprint_values(row[NUM_HOSTS]),
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

  def __init__(self, combined_summary, ref_combined_summary):

    # Store the original summaries, in case we can't build a comparison
    self.combined_summary = combined_summary
    self.ref_combined_summary = ref_combined_summary

    # If some error happened during calculations, store it here
    self.error_str = str()

    self.rows = []
    self.__build_rows()

  def __build_rows(self):

    if self.combined_summary.is_same_schema(self.ref_combined_summary):
      for i, row in enumerate(self.combined_summary.rows):
        ref_row = self.ref_combined_summary.rows[i]

        comparison_row = {}
        for key in [PREFIX, OPERATOR, NUM_HOSTS, AVG_TIME, STDDEV_TIME, MAX_TIME, PEAK_MEM,
            NUM_ROWS, EST_NUM_ROWS, EST_PEAK_MEM, DETAIL]:
          comparison_row[key] = row[key]

        comparison_row[AVG_TIME_CHANGE] = self.__calculate_change(
            row[AVG_TIME], ref_row[AVG_TIME], ref_row[AVG_TIME])

        comparison_row[AVG_TIME_CHANGE_TOTAL] = self.__calculate_change(
            row[AVG_TIME], ref_row[AVG_TIME], self.ref_combined_summary.total_runtime)

        comparison_row[MAX_TIME_CHANGE] = self.__calculate_change(
            row[MAX_TIME], ref_row[MAX_TIME], ref_row[MAX_TIME])

        comparison_row[PEAK_MEM_CHANGE] = self.__calculate_change(
            row[PEAK_MEM], ref_row[PEAK_MEM], ref_row[PEAK_MEM])

        self.rows.append(comparison_row)
    else:
      self.error_str = 'Execution summary structures are different'

  def __str__(self):
    """Construct a PrettyTable containing the comparison"""
    if self.error_str:
      # If the summary comparison could not be constructed, output both summaries
      output = self.error_str + '\n'
      output += 'Execution Summary: \n'
      output += str(self.combined_summary) + '\n'
      output += 'Reference Execution Summary: \n'
      output += str(self.ref_combined_summary)
      return output

    table = prettytable.PrettyTable(
        ["Operator",
          "#Hosts",
          "Avg Time",
          "Std Dev",
          "Avg Change",
          "Tot Change",
          "Max Time",
          "Max Change",
          "#Rows",
          "Est #Rows"])
    table.align = 'l'

    for row in self.rows:
      table_row = [ row[PREFIX] + row[OPERATOR],
          prettyprint_values(row[NUM_HOSTS]),
          prettyprint_time(row[AVG_TIME]),
          prettyprint_time(row[STDDEV_TIME]),
          prettyprint_percent(row[AVG_TIME_CHANGE]),
          prettyprint_percent(row[AVG_TIME_CHANGE_TOTAL]),
          prettyprint_time(row[MAX_TIME]),
          prettyprint_percent(row[MAX_TIME_CHANGE]),
          prettyprint_values(row[NUM_ROWS]),
          prettyprint_values(row[EST_NUM_ROWS]) ]

      table.add_row(table_row)

    return str(table)

  def __calculate_change(self, val, ref_val, compare_val):
    """Calculate how big the change in val compared to ref_val is compared to total"""
    if ref_val == 0:
      return 0
    change = abs(val - ref_val) / compare_val
    return change if val > ref_val else -change

def save_runtime_diffs(results, ref_results, change_significant, is_regression):
  """Given results and reference results, generate and output an HTML file
  containing the Runtime Profile diff.
  """

  diff = difflib.HtmlDiff(wrapcolumn=90, linejunk=difflib.IS_LINE_JUNK)

  # We are comparing last queries in each run because they should have the most
  # stable performance (unlike the first queries)
  runtime_profile = results[RESULT_LIST][-1][RUNTIME_PROFILE]
  ref_runtime_profile = ref_results[RESULT_LIST][-1][RUNTIME_PROFILE]

  template = ("{prefix}-{query_name}-{scale_factor}-{file_format}-{compression_codec}"
              "-{compression_type}")

  query = results[RESULT_LIST][-1][QUERY]

  # Neutral - no improvement or regression
  prefix = 'neu'
  if change_significant:
    prefix = 'reg' if is_regression else 'imp'

  file_name = template.format(
      prefix = prefix,
      query_name = query[NAME],
      scale_factor = query[SCALE_FACTOR],
      file_format = query[TEST_VECTOR][FILE_FORMAT],
      compression_codec = query[TEST_VECTOR][COMPRESSION_CODEC],
      compression_type = query[TEST_VECTOR][COMPRESSION_TYPE])

  # Go into results dir
  dir_path = os.path.join(os.environ["IMPALA_HOME"], 'results')

  if not os.path.exists(dir_path):
    os.mkdir(dir_path)
  elif not os.path.isdir(dir_path):
    raise RuntimeError("Unable to create $IMPALA_HOME/results, results file exists")

  runtime_profile_file_name = file_name + "-runtime_profile.html"

  runtime_profile_file_path = os.path.join(dir_path, runtime_profile_file_name)

  runtime_profile_diff = diff.make_file(
      ref_runtime_profile.splitlines(),
      runtime_profile.splitlines(),
      fromdesc = "Baseline Runtime Profile",
      todesc = "Current Runtime Profile")

  with open(runtime_profile_file_path, 'w+') as f:
    f.write(runtime_profile_diff)

def build_exec_summary_str(results, ref_results):
  exec_summaries = [result[EXEC_SUMMARY] for result in results[RESULT_LIST]]
  ref_exec_summaries = [result[EXEC_SUMMARY] for result in ref_results[RESULT_LIST]]

  if None in exec_summaries or None in ref_exec_summaries:
    return 'Unable to construct exec summary comparison\n'

  combined_summary = CombinedExecSummaries(exec_summaries)
  ref_combined_summary = CombinedExecSummaries(ref_exec_summaries)

  comparison = ExecSummaryComparison(combined_summary, ref_combined_summary)

  return str(comparison) + '\n'

def compare_time_stats(grouped, ref_grouped):
  """Given two nested dictionaries generated by get_dict_from_json, after running
  calculate_time_stats on both, compare the performance of the given run to a reference
  run.

  A string will be returned with instances where there is a significant performance
  difference
  """
  out_str = str()
  all_exec_summaries = str()
  for workload_scale_key, workload in grouped.items():
    for query_name, file_formats in workload.items():
      for file_format, results in file_formats.items():
        ref_results = ref_grouped[workload_scale_key][query_name][file_format]
        change_significant, is_regression = check_perf_change_significance(
            results, ref_results)

        if change_significant:
          out_str += build_perf_change_str(results, ref_results, is_regression) + '\n'
          out_str += build_exec_summary_str(results, ref_results)

        try:
          save_runtime_diffs(results, ref_results, change_significant, is_regression)
        except Exception as e:
          print 'Could not generate an html diff: %s' % e

  return out_str

def check_perf_change_significance(stat, ref_stat):
  absolute_difference = abs(ref_stat[AVG] - stat[AVG])
  percent_difference = abs(ref_stat[AVG] - stat[AVG]) * 100 / ref_stat[AVG]
  stddevs_are_zero = (ref_stat[STDDEV] == 0) and (stat[STDDEV] == 0)
  if absolute_difference < options.allowed_latency_diff_secs:
    return False, False
  if percent_difference < options.min_percent_change_threshold:
    return False, False
  if percent_difference > options.max_percent_change_threshold:
    return True, ref_stat[AVG] < stat[AVG]
  if options.tval_threshold and not stddevs_are_zero:
    tval = calculate_tval(stat[AVG], stat[STDDEV], stat[ITERATIONS],
        ref_stat[AVG], ref_stat[STDDEV], ref_stat[ITERATIONS])
    return abs(tval) > options.tval_threshold, tval > options.tval_threshold
  return False, False

def build_summary_header():
  summary = "Execution Summary ({0})\n".format(date.today())
  if options.report_description:
    summary += 'Run Description: {0}\n'.format(options.report_description)
  if options.cluster_name:
    summary += '\nCluster Name: {0}\n'.format(options.cluster_name)
  if options.build_version:
    summary += 'Impala Build Version: {0}\n'.format(options.build_version)
  if options.lab_run_info:
    summary += 'Lab Run Info: {0}\n'.format(options.lab_run_info)
  return summary

def get_summary_str(workload_ff):
  """This prints a table containing the average run time per file format"""
  summary_str = str()
  summary_str += build_summary_header() + '\n'

  for workload_scale in workload_ff:
    summary_str += "{0} / {1} \n".format(workload_scale[0][1], workload_scale[1][1])
    table = prettytable.PrettyTable(["File Format", "Compression", "Impala Avg"])
    table.align = 'l'
    table.float_format = '.3'
    for file_format in workload_ff[workload_scale]:
      ff = file_format[0][1]
      compression = file_format[1][1] + " / " + file_format[2][1]
      avg = str(workload_ff[workload_scale][file_format][AVG])
      table.add_row([ff, compression, avg])
    summary_str += str(table) + '\n'
  return summary_str

def get_stats_str(grouped):
  stats_str = str()
  for workload_scale_key, workload in grouped.items():
    stats_str += "Workload / Scale Factor: {0} / {1}".format(workload_scale_key[0][1],
        workload_scale_key[1][1])
    for query_name, file_formats in workload.items():
      stats_str += "\n\nQuery: {0} \n".format(query_name[0][1])
      table = prettytable.PrettyTable(
          ["File Format", "Compression", "Avg(s)", "StdDev(s)", "Num Clients", "Iters"])
      table.align = 'l'
      table.float_format = '.3'
      for file_format, results in file_formats.items():
        table_row = []
        # File Format
        table_row.append(file_format[0][1])
        # Compression
        table_row.append(file_format[1][1] + " / " + file_format[2][1])
        table_row.append(results[AVG])
        table_row.append(results[STDDEV])
        table_row.append(results[NUM_CLIENTS])
        table_row.append(results[ITERATIONS])
        table.add_row(table_row)
      stats_str += str(table)
  return stats_str

def all_query_results(grouped):
  for workload_scale_key, workload in grouped.items():
    for query_name, file_formats in workload.items():
      for file_format, results in file_formats.items():
        yield(results)


def write_results_to_datastore(grouped):
  """ Saves results to a database """
  from perf_result_datastore import PerfResultDataStore
  print 'Saving perf results to database'
  current_date = datetime.now()
  data_store = PerfResultDataStore(host=options.db_host, username=options.db_username,
      password=options.db_password, database_name=options.db_name)

  run_info_id = data_store.insert_run_info(options.lab_run_info)
  for results in all_query_results(grouped):
    first_query_result = results[RESULT_LIST][0]
    executor_name = first_query_result[EXECUTOR_NAME]
    workload = first_query_result[QUERY][WORKLOAD_NAME]
    scale_factor = first_query_result[QUERY][SCALE_FACTOR]
    query_name = first_query_result[QUERY][NAME]
    query = first_query_result[QUERY][QUERY_STR]
    file_format = first_query_result[QUERY][TEST_VECTOR][FILE_FORMAT]
    compression_codec = first_query_result[QUERY][TEST_VECTOR][COMPRESSION_CODEC]
    compression_type = first_query_result[QUERY][TEST_VECTOR][COMPRESSION_TYPE]
    avg_time = results[AVG]
    stddev = results[STDDEV]
    num_clients = results[NUM_CLIENTS]
    num_iterations = results[ITERATIONS]
    runtime_profile = first_query_result[RUNTIME_PROFILE]

    file_type_id = data_store.get_file_format_id(
        file_format, compression_codec, compression_type)
    if file_type_id is None:
      print 'Skipping unkown file type: %s / %s' % (file_format, compression)
      continue

    workload_id = data_store.get_workload_id(workload, scale_factor)
    if workload_id is None:
      workload_id = data_store.insert_workload_info(workload, scale_factor)

    query_id = data_store.get_query_id(query_name, query)
    if query_id is None:
      query_id = data_store.insert_query_info(query_name, query)

    data_store.insert_execution_result(
        query_id = query_id,
        workload_id = workload_id,
        file_type_id = file_type_id,
        num_clients = num_clients,
        cluster_name = options.cluster_name,
        executor_name = executor_name,
        avg_time = avg_time,
        stddev = stddev,
        run_date = current_date,
        version = options.build_version,
        notes = options.report_description,
        run_info_id = run_info_id,
        num_iterations = num_iterations,
        runtime_profile = runtime_profile,
        is_official = options.is_official)

if __name__ == "__main__":
  """Workflow:
  1. Build a nested dictionary for the current result JSON and reference result JSON.
  2. Calculate runtime statistics for each query for both results and reference results.
  5. Save performance statistics to the performance database.
  3. Construct a string with a an overview of workload runtime and detailed performance
     comparison for queries with significant performance change.
  """
  # Generate a dictionary based on the JSON file
  grouped = get_dict_from_json(options.result_file)

  try:
    # Generate a dictionary based on the reference JSON file
    ref_grouped = get_dict_from_json(options.reference_result_file)
  except Exception as e:
    # If reference result file could not be read we can still continue. The result can
    # be saved to the performance database.
    print 'Could not read reference result file: %s' % e
    ref_grouped = None

  # Calculate average runtime and stddev for each query type
  calculate_time_stats(grouped)
  if ref_grouped is not None:
    calculate_time_stats(ref_grouped)

  if options.save_to_db: write_results_to_datastore(grouped)

  summary_str = get_summary_str(calculate_workload_file_format_runtimes(grouped))
  stats_str = get_stats_str(grouped)
  comparison_str = 'No Comparison' if ref_grouped is None else compare_time_stats(
      grouped, ref_grouped)

  print summary_str
  print stats_str
  print comparison_str
