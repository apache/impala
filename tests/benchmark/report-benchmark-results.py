#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script provides help with parsing and reporting of perf results. It currently
# provides three main capabilities:
# 1) Printing perf results to console in 'pretty' format
# 2) Comparing two perf result sets together and displaying comparison results to console
# 3) Outputting the perf results in JUnit format which is useful for plugging in to
#    Jenkins perf reporting.
#
# The input to this script is a benchmark result CSV file which should be generated using
# the 'run-workload.py' script. The input CSV file has the format:
# <executor>|<workload>|<scale factor>|<query short name>|<full query name>|<file format>
# <compression>|<avg exec time>|<std dev>|
#
# TODO: Minimize the logic in this script so it doesn't get any more complex. Additional
# reporting will be enabled when perf results are stored in a database as well as CSV
# files.
import csv
import math
import os
import sys
from datetime import date, datetime
from itertools import groupby
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--input_result_file", dest="result_file",
                  default=os.environ['IMPALA_HOME'] + '/benchmark_results.csv',
                  help="The input CSV file with benchmark results")
parser.add_option("--reference_result_file", dest="reference_result_file",
                  default=os.environ['IMPALA_HOME'] + '/reference_benchmark_results.csv',
                  help="The input CSV file with reference benchmark results")
parser.add_option("--hive_result_file", dest="hive_result_file",
                  default=os.environ['IMPALA_HOME'] + '/hive_benchmark_results.csv',
                  help="The input CSV file with the hive reference benchmark results")
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

COL_WIDTH = 18
TOTAL_WIDTH = 132 if options.verbose else 96

# These are the indexes in the input row for each column value
EXECUTOR_IDX = 0
WORKLOAD_IDX = 1
SCALE_FACTOR_IDX = 2
QUERY_NAME_IDX = 3
QUERY_IDX = 4
FILE_FORMAT_IDX = 5
COMPRESSION_IDX = 6
AVG_IDX = 7
STDDEV_IDX = 8
NUM_CLIENTS_IDX = 9
HIVE_AVG_IDX = 10
HIVE_STDDEV_IDX = 11

# Formats a string so that is is wrapped across multiple lines with no single line
# being longer than the given width
def wrap_text(text, width):
  return '\n'.join([text[width * i : width * (i + 1)] \
      for i in xrange(int(math.ceil(1.0 * len(text) / width)))])

# Formats float values to have two decimal places. If the input string is not a float
# then the original value is returned
def format_if_float(float_str):
  try:
    return "%0.2f" % float(float_str)
  except (ValueError, TypeError):
    return str(float_str)

# Returns a string representation of the row with columns padded by the
# the given column width
def build_padded_row_string(row, column_width):
  return ''.join([format_if_float(col).ljust(column_width) for col in row])

def find_matching_row_in_reference_results(search_row, reference_results):
  for row in reference_results:
    if not row:
      continue;
    if (row[QUERY_NAME_IDX] == search_row[QUERY_NAME_IDX] and
        row[FILE_FORMAT_IDX] == search_row[FILE_FORMAT_IDX] and
        row[COMPRESSION_IDX] == search_row[COMPRESSION_IDX] and
        row[SCALE_FACTOR_IDX] == search_row[SCALE_FACTOR_IDX] and
        row[WORKLOAD_IDX] == search_row[WORKLOAD_IDX]):
      return row
  return None

def calculate_speedup(reference, actual):
  if actual != 'N/A' and reference != 'N/A' and actual != 0:
    return float(reference) / float(actual);
  else:
    return 'N/A'

def calculate_impala_hive_speedup(row):
  return calculate_speedup(row[HIVE_AVG_IDX], row[AVG_IDX])

def calculate_geomean(times):
  """ Calculates the geometric mean of the given collection of numerics """
  if len(times) > 0:
    return (reduce(lambda x, y: float(x) * float(y), times)) ** (1.0 / len(times))
  return 'N/A'

def build_table_header(verbose):
  table_header = ['File Format', 'Compression', 'Avg(s)', 'StdDev(s)']
  if verbose:
    table_header += ['Hive Avg(s)', 'Hive StdDev(s)']
  return table_header + ['Impala Speedup (vs Hive)']

# Prints out the given result set in table format, grouped by query
def build_table(results, verbose, reference_results = None):
  output = str()
  sort_key = lambda x: (x[QUERY_NAME_IDX])
  results.sort(key = sort_key)
  for query_group, group in groupby(results, key = sort_key):
    output += 'Query: ' + wrap_text(query_group, TOTAL_WIDTH) + '\n'
    output += build_padded_row_string(build_table_header(verbose), COL_WIDTH) + '\n'
    output += "-" * TOTAL_WIDTH + '\n'

    for row in group:
      full_row = list(row)
      if not options.verbose:
        del full_row[HIVE_STDDEV_IDX]
        del full_row[HIVE_AVG_IDX]

      full_row += [format_if_float(calculate_impala_hive_speedup(row)) + 'X']
      if reference_results is not None:
        comparison_row = find_matching_row_in_reference_results(row, reference_results)
        # There wasn't a matching row so don't display speedup information
        if comparison_row is None:
          output += build_padded_row_string(full_row[FILE_FORMAT_IDX:], COL_WIDTH) + '\n'
          continue

        speedup = calculate_speedup(float(comparison_row[AVG_IDX]), float(row[AVG_IDX]))
        full_row[AVG_IDX] = format_if_float(full_row[AVG_IDX])
        full_row[AVG_IDX] = full_row[AVG_IDX] + ' (%sX)' % format_if_float(speedup)
        output += build_padded_row_string(full_row[FILE_FORMAT_IDX:], COL_WIDTH) + '\n'
      else:
        output += build_padded_row_string(full_row[FILE_FORMAT_IDX:], COL_WIDTH) + '\n'
    output +=  "-" * TOTAL_WIDTH + '\n\n'
  return output

def geometric_mean_execution_time(results):
  """
  Returns the geometric mean of the average execution times

  Returns three sets of numbers - the mean of all the Impala times, the mean of the
  Impala times that have matching hive results, and the mean of the hive results.
  """
  impala_avgs = []
  impala_avgs_with_hive_match = []
  hive_avgs = []
  for row in results:
    impala_avg, hive_avg = (row[AVG_IDX], row[HIVE_AVG_IDX])
    if impala_avg != 'N/A':
      impala_avgs.append(float(impala_avg))
      if hive_avg != 'N/A':
        impala_avgs_with_hive_match.append(float(impala_avg))
        hive_avgs.append(float(hive_avg))

  return calculate_geomean(impala_avgs),\
         calculate_geomean(impala_avgs_with_hive_match),\
         calculate_geomean(hive_avgs)

# Returns the sum of the average execution times for the given result
# collection
def sum_avg_execution_time(results):
  impala_time = 0
  hive_time = 0
  for row in results:
    impala_time += float(row[AVG_IDX]) if str(row[AVG_IDX]) != 'N/A' else 0
    hive_time += float(row[HIVE_AVG_IDX]) if str(row[HIVE_AVG_IDX]) != 'N/A' else 0
  return impala_time, hive_time

# Returns dictionary of column_value to sum of the average times grouped by the specified
# key function
def sum_execution_time_by_key(results, key):
  results.sort(key = key)
  execution_results = dict()
  for key, group in groupby(results, key=key):
    execution_results[key] = (sum_avg_execution_time(group))
  return execution_results

def geometric_mean_execution_time_by_key(results, key):
  results.sort(key = key)
  execution_results = dict()
  for key, group in groupby(results, key=key):
    execution_results[key] = (geometric_mean_execution_time(group))
  return execution_results

# Returns dictionary of column_value to sum of the average times grouped by the specified
# column index
def sum_execution_time_by_col_idx(results, column_index):
  return sum_execution_time_by_key(results, key=lambda x: x[column_index])

def sum_execution_by_file_format(results):
  return sum_execution_time_by_col_idx(results, FILE_FORMAT_IDX)

def sum_execution_by_query(results):
  return sum_execution_time_by_col_idx(results, QUERY_IDX)

def sum_execution_by_compression(results):
  return sum_execution_time_by_col_idx(results, COMPRESSION_IDX)

def geometric_mean_by_file_format_compression(results):
  key = lambda x: (x[FILE_FORMAT_IDX], x[COMPRESSION_IDX])
  return geometric_mean_execution_time_by_key(results, key)

# Writes perf tests results in a "fake" JUnit output format. The main use case for this
# is so the Jenkins Perf plugin can be leveraged to report results. We create a few
# "fake" tests that are actually just aggregating the execution times in different ways.
# For example, create tests that have the aggregate execution time for each file format
# so we can see if a perf regression happens in this area.
def write_junit_output_file(results, output_file):
  test_case_format = '<testcase time="%s" classname="impala.perf.tests" name="%s"/>'

  lines = ['<testsuite failures="0" time="%s" errors="0" skipped="0" tests="%s"\
            name="impala.perf.tests">']
  for file_format, time in sum_execution_by_file_format(results).iteritems():
    lines.append(test_case_format % (format_if_float(time), 'sum_avg_' + file_format))

  for compression, time in sum_execution_by_compression(results).iteritems():
    lines.append(test_case_format % (format_if_float(time), 'sum_avg_' + compression))

  for query, time in sum_execution_by_query(results).iteritems():
    lines.append(test_case_format % (format_if_float(time), 'sum_avg_' + query))

  total_tests = len(lines)
  sum_avg = format_if_float(sum_avg_execution_time(results))
  lines[0] = lines[0] % (sum_avg, total_tests)
  lines.append('</testsuite>')
  output_file.write('\n'.join(lines))

# read results file in CSV format, then copies to a list and returns the value
def read_csv_result_file(file_name):
  results = []
  for row in csv.reader(open(file_name, 'rb'), delimiter='|'):
    # Older results may not have num_clients, so default to 1
    if len(row) == STDDEV_IDX + 1:
      row.append('1')
    results.append(row)
  return results

def filter_sort_results(results, workload, scale_factor, key):
  filtered_res = [result for result in results if (
      result[WORKLOAD_IDX] == workload and result[SCALE_FACTOR_IDX] == scale_factor)]
  return sorted(filtered_res, key=sort_key)

def scale_factor_name(scale_factor):
  return scale_factor if scale_factor else 'default'

def merge_hive_results(results, hive_results):
  new_results = []
  for row in results:
    matching_row = find_matching_row_in_reference_results(row, hive_results)
    if matching_row is not None:
      new_results.append(row + [matching_row[AVG_IDX], matching_row[STDDEV_IDX]])
    else:
      new_results.append(row + ['N/A', 'N/A'])
  return new_results

def write_results_to_datastore(results):
  """ Saves results to a database """
  current_date = datetime.now()
  data_store = PerfResultDataStore(host=options.db_host, username=options.db_username,
      password=options.db_password, database_name=options.db_name)

  run_info_id = data_store.insert_run_info(options.lab_run_info)
  for row in results:
    # We ignore everything aver the stddev column
    executor, workload, scale_factor, query_name, query, file_format,\
        compression, avg_time, stddev = row[0:STDDEV_IDX + 1]

    # Instead of storing 'N/A' in the database we want to store NULL
    avg_time = avg_time if avg_time and avg_time != 'N/A' else 'NULL'
    stddev = stddev if stddev and stddev != 'N/A' else 'NULL'

    file_type_id = data_store.get_file_format_id(file_format, compression)
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
        query_id=query_id, workload_id=workload_id, file_type_id=file_type_id,
        num_clients=int(row[NUM_CLIENTS_IDX]), cluster_name=options.cluster_name,
        executor_name=executor, avg_time=avg_time, stddev=stddev,
        run_date=current_date, version=options.build_version,
        notes=options.report_description, run_info_id=run_info_id,
        is_official=options.is_official)

def build_summary_header():
  summary = "Execution Summary (%s)\n" % date.today()
  if options.report_description:
    summary += 'Run Description: %s\n' % options.report_description
  if options.cluster_name:
    summary += '\nCluster Name: %s\n' % options.cluster_name
  if options.build_version:
    summary += 'Impala Build Version: %s\n' % options.build_version
  if options.lab_run_info:
    summary += 'Lab Run Info: %s\n' % options.lab_run_info
  return summary

reference_results = []
hive_reference_results = []
results = []
if os.path.isfile(options.result_file):
  results = read_csv_result_file(options.result_file)
else:
  print 'Results file: ' + options.result_file + ' not found.'
  sys.exit(1)

if os.path.isfile(options.hive_result_file):
  hive_reference_results = read_csv_result_file(options.hive_result_file)
else:
  print 'Hive result file: ' + options.hive_result_file + ' not found'

# We want to marge hive results, even if they are empty, so row indexes stay the same.
results = merge_hive_results(results, hive_reference_results)

if os.path.isfile(options.reference_result_file):
  reference_results = read_csv_result_file(options.reference_result_file)
else:
  print 'No Impala reference result file found.'

if not options.no_output_table:
  summary, table_output = str(), str()

  sort_key = lambda k: (k[WORKLOAD_IDX], k[SCALE_FACTOR_IDX])
  results_sorted = sorted(results, key=sort_key)

  summary += build_summary_header()
  if results:
    summary += 'Num Clients: %s' % results[0][NUM_CLIENTS_IDX]
  summary += "\nWorkload / Scale Factor\n\n"

  # First step is to break the result down into groups or workload/scale factor
  for workload_scale_factor, group in groupby(results_sorted, key=sort_key):
    workload, scale_factor = workload_scale_factor
    summary += '%s / %s\n' % (workload, scale_factor_name(scale_factor))

    # Based on the current workload/scale factor grouping, filter and sort results
    filtered_results = filter_sort_results(results, workload, scale_factor, sort_key)
    header = ['File Format', 'Compression', 'Impala Avg(s)', 'Impala Speedup (vs Hive)']
    summary += '  ' + build_padded_row_string(header, COL_WIDTH) + '\n'

    # Calculate execution details for each workload/scale factor
    for file_format_compression, times in geometric_mean_by_file_format_compression(
        filtered_results).iteritems():
      file_format, compression = file_format_compression
      impala_avg, impala_with_hive_match_avg, hive_avg = times
      impala_speedup = format_if_float(
          calculate_speedup(hive_avg, impala_with_hive_match_avg)) +\
          'X' if hive_avg != 'N/A' else 'N/A'

      summary += '  ' + build_padded_row_string(
          [file_format, compression, impala_avg, impala_speedup], COL_WIDTH) + '\n'
    summary += '\n'

    table_output += "-" * TOTAL_WIDTH + '\n'
    table_output += "-- Workload / Scale Factor: %s / %s\n" %\
        (workload, scale_factor_name(scale_factor))
    table_output += "-" * TOTAL_WIDTH + '\n'

    # Build a table with detailed execution results for the workload/scale factor
    table_output += build_table(filtered_results, options.verbose,
                                reference_results) + '\n'
  print summary, table_output
  print 'Total Avg Execution Time: ' + str(sum_avg_execution_time(results)[0])

if options.junit_output_file:
  write_junit_output_file(results, open(options.junit_output_file, 'w'))

if options.save_to_db:
  print 'Saving perf results to database'
  from perf_result_datastore import PerfResultDataStore
  write_results_to_datastore(results)
