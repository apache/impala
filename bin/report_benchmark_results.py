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
# <hive stddev>
#
import csv
import math
import os

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
TOTAL_WIDTH = 122 if options.verbose else 90

# These are the indexes in the input row for each column value
QUERY_IDX = 0
FILE_FORMAT_IDX = 1
COMPRESSION_IDX = 2
IMPALA_AVG_IDX = 3
IMPALA_STDDEV_IDX = 4
HIVE_AVG_IDX = 5
HIVE_STDDEV_IDX = 6

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

def build_padded_row_string_comparison(row, diff_percent, column_width):
  row_string = ''
  for i in range(len(row)):
    col = format_if_float(row[i])
    # Since we have sliced the array, we need to substract 1 from the index
    if i == IMPALA_AVG_IDX - 1:
      color = GREEN if diff_percent >= 0.00 else RED
      col = '{0:s} ({1:s}{2:+.2f}%{3:s})'.format(col, color, float(diff_percent), END)
      row_string += col.ljust(column_width + 9)
    else:
      row_string += col.ljust(column_width)
  return row_string

def find_matching_row_in_reference_results(search_row, reference_results):
  for row in reference_results:
    if (row[QUERY_IDX] == search_row[QUERY_IDX] and
        row[FILE_FORMAT_IDX] == search_row[FILE_FORMAT_IDX] and
        row[COMPRESSION_IDX] == search_row[COMPRESSION_IDX]):
      return row
  return None

def calculate_impala_hive_speedup(row):
  impala_speedup = "N/A"
  if row[HIVE_AVG_IDX] != 'N/A':
    impala_speedup = str(float(row[HIVE_AVG_IDX]) / float(row[IMPALA_AVG_IDX]));
  return impala_speedup

def calculate_percentage_change(reference_val, new_val):
  return (float(reference_val) - float(new_val)) / float(reference_val) * 100

# Prints out the given result set in table format, grouped by query
def print_table(results, verbose, reference_results = None):
  results.sort(key = lambda x: x[QUERY_IDX])
  for query, group in groupby(results, lambda x: x[QUERY_IDX]):
    print 'Query:\n' + wrap_text(query, TOTAL_WIDTH)
    table_header = ['File Format', 'Compression', 'Impala Avg(s)', 'Impala StdDev(s)']
    if verbose:
      table_header += ['Hive Avg(s)', 'Hive StdDev(s)']
    table_header += ['Impala Speedup']

    print build_padded_row_string(table_header, COLUMN_WIDTH)
    print "-" * TOTAL_WIDTH
    for row in group:
      full_row = row[1:] + [format_if_float(calculate_impala_hive_speedup(row)) + 'X']
      if not verbose:
        del full_row[HIVE_AVG_IDX - 1]
        del full_row[HIVE_STDDEV_IDX - 2]
      if reference_results is not None:
        comparison_row = find_matching_row_in_reference_results(row, reference_results)
        # There wasn't a matching row
        if comparison_row is None:
          print build_padded_row_string(full_row, COLUMN_WIDTH)
          continue

        reference_avg = float(comparison_row[IMPALA_AVG_IDX])
        avg = float(row[IMPALA_AVG_IDX])
        percent_diff = calculate_percentage_change(reference_avg, avg)
        print build_padded_row_string_comparison(full_row, percent_diff, COLUMN_WIDTH)
      else:
        print build_padded_row_string(full_row, COLUMN_WIDTH)
    print "-" * TOTAL_WIDTH
    print " "

# Returns the sum of the average execution times for the given result
# collection
def sum_avg_execution_time(results):
  return sum(float(row[IMPALA_AVG_IDX]) for row in results)

# Returns dictionary of column_value to sum of the average times grouped by the specified
# column index
def sum_execution_time_by_col_idx(results, column_index):
  column_key = lambda x: x[column_index]
  results.sort(key = column_key)
  execution_results = dict()
  for file_format, group in groupby(results, column_key):
    execution_results[file_format] = sum_avg_execution_time(group)
  return execution_results

def sum_execution_by_file_format(results):
  return sum_execution_time_by_col_idx(results, FILE_FORMAT_IDX)

def sum_execution_by_query(results):
  return sum_execution_time_by_col_idx(results, QUERY_IDX)

def sum_execution_by_compression(results):
  return sum_execution_time_by_col_idx(results, COMPRESSION_IDX)

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
  print_table(results, options.verbose, reference_results)
  # Print some summary information
  print 'Sum Average Execution Time: ' + str(sum_avg_execution_time(results))
  print '\nSum Average Execution Time By File Format:'
  for file_format, time in sum_execution_by_file_format(results).iteritems():
    print build_padded_row_string([file_format, time], COLUMN_WIDTH)

  print '\nSum Average Execution Time By Compression:'
  for compression, time in sum_execution_by_compression(results).iteritems():
    print build_padded_row_string([compression, time], COLUMN_WIDTH)

if options.junit_output_file:
  write_junit_output_file(results, open(options.junit_output_file, 'w'))
