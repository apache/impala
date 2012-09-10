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
# the 'run_benchmark.py' script. The input CSV file has the format:
# <query>|<file_format>|<compression>|<avg exec time>|<std dev>|<hive avg exec time>|
# <hive stddev>|<scale factor>
#
# TODO: This script should accept two sets of reference results - one for Hive and one
# for Impala. The reference results for each should be in the same format. Part of that
# chould would be outputting two result files when running run-workload.py. As part of
# the change this file should be cleaned up and simplified.
import csv
import math
import os
import sys

from datetime import date
from itertools import groupby
from optparse import OptionParser

parser = OptionParser()
parser.add_option("--input_result_file", dest="result_file",
                  default=os.environ['IMPALA_HOME'] + '/benchmark_results.csv',
                  help="The input CSV file with benchmark results")
parser.add_option("--reference_result_file", dest="reference_result_file",
                  default=os.environ['IMPALA_HOME'] + '/reference_benchmark_results.csv',
                  help="The input CSV file with reference benchmark results")
parser.add_option("--junit_output_file", dest="junit_output_file", default='',
                  help='If set, outputs results in Junit format to the specified file')
parser.add_option("--no_output_table", dest="no_output_table", action="store_true",
                  default= False, help='Outputs results in table format to the console')
parser.add_option("--verbose", "-v", dest="verbose", action="store_true",
                  default= False, help='Outputs to console with with increased verbosity')
parser.add_option("--nocolor", dest="nocolor", action="store_true",
                  default= False, help='Prints diff results without coloring the console')
(options, args) = parser.parse_args()

# Console color format strings
GREEN = '' if options.nocolor else '\033[92m'
YELLOW = '' if options.nocolor else '\033[93m'
RED = '' if options.nocolor else '\033[91m'
END =  '' if options.nocolor else '\033[0m'

COLUMN_WIDTH = 18
TOTAL_WIDTH = 132 if options.verbose else 96

# These are the indexes in the input row for each column value
WORKLOAD_IDX = 0
QUERY_NAME_IDX = 1
QUERY_IDX = 2
FILE_FORMAT_IDX = 3
COMPRESSION_IDX = 4
IMPALA_AVG_IDX = 5
IMPALA_STDDEV_IDX = 6
HIVE_AVG_IDX = 7
HIVE_STDDEV_IDX = 8
SCALE_FACTOR_IDX = 9

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

def build_padded_row_string_comparison(row, speedup, column_width):
  row_string = ''
  for i in range(len(row)):
    col = format_if_float(row[i])
    # Since we have sliced the array, we need to substract 1 from the index
    if i == IMPALA_AVG_IDX - 1 and speedup != 'N/A':
      color = GREEN if float(speedup) >= 1.00 else RED
      col = '{0:s} ({1:s}{2:.2f}X{3:s})'.format(col, color, float(speedup), END)
      row_string += col.ljust(column_width + 9)
    else:
      row_string += col.ljust(column_width)
  return row_string

def find_matching_row_in_reference_results(search_row, reference_results):
  for row in reference_results:
    if (row[QUERY_IDX] == search_row[QUERY_IDX] and
        row[FILE_FORMAT_IDX] == search_row[FILE_FORMAT_IDX] and
        row[COMPRESSION_IDX] == search_row[COMPRESSION_IDX] and
        row[WORKLOAD_IDX] == search_row[WORKLOAD_IDX]):
      return row
  return None

def calculate_speedup(reference, actual):
  if actual != 'N/A' and reference != 'N/A':
    return float(reference) / float(actual);
  else:
    return 'N/A'

def calculate_impala_hive_speedup(row):
  return calculate_speedup(row[HIVE_AVG_IDX], row[IMPALA_AVG_IDX])

# Prints out the given result set in table format, grouped by query
def build_table(results, verbose, reference_results = None):
  output = str()
  sort_key = lambda x: (x[QUERY_NAME_IDX])
  results.sort(key = sort_key)
  for query_group, group in groupby(results, key = sort_key):
    output += 'Query: ' + wrap_text(query_group, TOTAL_WIDTH) + '\n'
    table_header = ['File Format', 'Compression', 'Avg(s)', 'StdDev(s)']
    if verbose:
      table_header += ['Hive Avg(s)', 'Hive StdDev(s)']
    table_header += ['Impala Speedup (vs Hive)']

    output += build_padded_row_string(table_header, COLUMN_WIDTH) + '\n'
    output += "-" * TOTAL_WIDTH + '\n'
    for row in group:
      # Strip out columns we don't want to display
      full_row = row[3:len(row) - 1]
      full_row += [format_if_float(calculate_impala_hive_speedup(row)) + 'X']
      if not verbose:
        del full_row[HIVE_AVG_IDX - 3]
        del full_row[HIVE_STDDEV_IDX - 4]
      if reference_results is not None:
        comparison_row = find_matching_row_in_reference_results(row, reference_results)
        # There wasn't a matching row
        if comparison_row is None:
          output += build_padded_row_string(full_row, COLUMN_WIDTH) + '\n'
          continue

        reference_avg = float(comparison_row[IMPALA_AVG_IDX])
        avg = float(row[IMPALA_AVG_IDX])
        speedup = calculate_speedup(reference_avg, avg)
        output += build_padded_row_string_comparison(full_row, speedup, COLUMN_WIDTH)
        output += '\n'
      else:
        output += build_padded_row_string(full_row, COLUMN_WIDTH) + '\n'
    output +=  "-" * TOTAL_WIDTH + '\n'
    output += " " + '\n'
  return output

# Returns the sum of the average execution times for the given result
# collection
def sum_avg_execution_time(results):
  impala_time = 0
  hive_time = 0
  for row in results:
    impala_time += float(row[IMPALA_AVG_IDX])
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

def sum_execution_by_file_format_compression(results):
  key = lambda x: (x[FILE_FORMAT_IDX], x[COMPRESSION_IDX])
  return sum_execution_time_by_key(results, key)

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
    results.append(row)
  return results

def filter_sort_results(results, workload, scale_factor, key):
  filtered_res = [result for result in results if (
      result[WORKLOAD_IDX] == workload and result[SCALE_FACTOR_IDX] == scale_factor)]
  return sorted(filtered_res, key=sort_key)

def scale_factor_name(scale_factor):
  return scale_factor if scale_factor else 'default'

reference_results = []
results = []
if os.path.isfile(options.result_file):
  results = read_csv_result_file(options.result_file)
else:
  print 'Results file: ' + options.result_file + ' not found.'
  sys.exit(1)

if os.path.isfile(options.reference_result_file):
  reference_results = read_csv_result_file(options.reference_result_file)
else:
  print 'No reference result file found.'


if not options.no_output_table:
  summary, table_output = str(), str()

  sort_key = lambda k: (k[WORKLOAD_IDX], k[SCALE_FACTOR_IDX])
  results_sorted = sorted(results, key=sort_key)
  summary += "Execution Summary (%s)\n" % date.today()
  summary += "Workload / Scale Factor\n\n"

  # First step is to break the result down into groups or workload/scale factor
  for workload_scale_factor, group in groupby(results_sorted, key=sort_key):
    workload, scale_factor = workload_scale_factor
    summary += '%s / %s\n' % (workload, scale_factor_name(scale_factor))

    # Based on the current workload/scale factor grouping, filter and sort results
    filtered_results = filter_sort_results(results, workload, scale_factor, sort_key)
    header = ['File Format', 'Compression', 'Impala Avg(s)', 'Impala Speedup (vs Hive)']
    summary += '  ' + build_padded_row_string(header, COLUMN_WIDTH) + '\n'

    # Calculate execution details for each workload/scale factor
    for file_format_compression, times in sum_execution_by_file_format_compression(
        filtered_results).iteritems():
      file_format, compression = file_format_compression
      impala_avg, hive_avg = times
      impala_speedup = format_if_float(calculate_speedup(hive_avg, impala_avg)) +\
          'X' if hive_avg != 0 else 'N/A'
      summary += '  ' + build_padded_row_string(
          [file_format, compression, impala_avg, impala_speedup], COLUMN_WIDTH) + '\n'
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
