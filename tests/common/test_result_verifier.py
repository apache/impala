#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This modules contians utility functions used to help verify query test results.
#
import logging
import os
import pytest
import sys
import re
from tests.util.test_file_parser import remove_comments

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('test_result_verfier')

# Represents a single test result (row set)
class QueryTestResult(object):
  def __init__(self, result_list, column_types, order_matters):
    self.column_types = column_types
    # The order of the result set might be different if running with multiple nodes.
    # Unless there is an ORDER BY clause, the results should be sorted for comparison.
    test_results = result_list
    if not order_matters:
      test_results = sorted(result_list)
    self.rows = [ResultRow(row, column_types) for row in test_results]

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return self.column_types == other.column_types and self.rows == other.rows

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return '\n'.join(['%s' % row for row in self.rows])


# Represents a row in a result set
class ResultRow(object):
  def __init__(self, row_string, column_types):
    self.skip_comparison = False
    # TODO: Still need to add support for verifying regex results. For now just skip
    # this verification
    if row_string and row_string.strip().startswith('regex:'):
      self.skip_comparison = True
      self.columns = list()
    else:
      self.columns = self.__parse_row(row_string, column_types)

  def __parse_row(self, row_string, column_types):
    """Parses a row string and build a list of ResultColumn objects"""
    column_values = list()
    string_val = None
    current_column = 0
    for col_val in row_string.split(','):
      # This is a bit tricky because we need to handle the case where a comma may be in
      # the middle of a string. We detect this by finding a split that starts with an
      # opening string character but that doesn't end in a string character. It is
      # possible for the first character to be a single-quote, so handle that case
      if (col_val.startswith("'") and not col_val.endswith("'")) or (col_val == "'"):
        string_val = col_val
        continue

      if string_val is not None:
        string_val += ',' + col_val
        if col_val.endswith("'"):
          col_val = string_val
          string_val = None
        else:
          continue
      assert current_column < len(column_types),\
          'Number of columns returned > the number of column types: %s' % column_types
      column_values.append(ResultColumn(col_val, column_types[current_column]))
      current_column = current_column + 1
    return column_values

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    return (self.skip_comparison or other.skip_comparison) or\
        (self.columns == other.columns)

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return ','.join(['%s' % col for col in self.columns])


# Represents a column in a row
class ResultColumn(object):
  def __init__(self, value, column_type):
    """Value of the column and the type (double, float, string, etc...)"""
    self.value = value
    self.column_type = column_type.lower()

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    # Make sure the column types are the same
    if self.column_type != other.column_type:
      return False

    if (self.value == 'NULL' or other.value == 'NULL') or \
       ('inf' in self.value or 'inf' in other.value):
      return self.value == other.value
    # If comparing against a float or double, don't do a strict comparison
    # See: http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm
    elif self.column_type == 'float':
      return abs(float(self.value) - float(other.value)) <= 10e-5
    elif self.column_type == 'double':
      return abs(float(self.value) - float(other.value)) <= 10e-10
    else:
      return self.value == other.value

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return self.value

  def __repr__(self):
    return 'Type: %s Value: %s' % (self.column_type, self.value)

def verify_query_results(expected_results, actual_results):
  """Verifies the actual versus expected results of a query"""
  assert actual_results is not None
  assert expected_results is not None
  assert expected_results == actual_results

def verify_results(expected_results, actual_results, order_matters):
  """Verifies the actual versus expected result strings"""
  assert actual_results is not None
  assert expected_results is not None

  # The order of the result set might be different if running with multiple nodes. Unless
  # there is an order by clause, sort the expected and actual results before comparison.
  if not order_matters:
    expected_results = sorted(expected_results)
    actual_results = sorted(actual_results)

  if len(expected_results) > 0 and 'regex:' in expected_results[0]:
    return

  failure_str = '\nExpected:\n%s\n\nActual:\n%s' %\
      ('\n'.join(expected_results), '\n'.join(actual_results))

  assert expected_results == actual_results, failure_str

def verify_column_types(actual_col_types, exec_result_schema):
  actual_col_types = [c.strip().upper() for c in actual_col_types.split(',')]
  expected_col_types = parse_column_types(exec_result_schema)
  verify_results(actual_col_types, expected_col_types, order_matters=True)

def verify_raw_results(test_section, exec_result):
  """
  Accepts a raw exec_result object and verifies it matches the expected results

  This process includes the parsing/transformation of the raw data results into the
  result format used in the tests.
  """
  expected_results = None

  if 'RESULTS' in test_section:
    expected_results = remove_comments(test_section['RESULTS'])
  else:
    LOG.info("No results found. Skipping verification");
    return

  if 'TYPES' in test_section:
    verify_column_types(test_section['TYPES'], exec_result.schema)
    expected_types = [c.strip().upper() for c in test_section['TYPES'].split(',')]
    actual_types = parse_column_types(exec_result.schema)
  else:
    # This is an insert, so we are comparing the number of rows inserted
    expected_types = ['BIGINT']
    actual_types = ['BIGINT']

  order_matters = contains_order_by(exec_result.query)
  expected = QueryTestResult(expected_results.split('\n'), expected_types, order_matters)
  actual = QueryTestResult(parse_result_rows(exec_result), actual_types, order_matters)
  verify_query_results(expected, actual)

def contains_order_by(query):
  """Returns true of the query contains an 'order by' clause"""
  return re.search( r'order\s+by\b', query, re.M|re.I) is not None

def parse_column_types(schema):
  """Enumerates all field schemas and returns a list of column type strings"""
  return [fs.type.upper() for fs in schema.fieldSchemas]

def parse_result_rows(exec_result):
  """
  Parses a query result set and transforms it to the format used by the query test files
  """
  raw_result = exec_result.data
  if not raw_result:
    return ['']

  # If the schema is 'None' assume this is an insert statement
  if exec_result.schema is None:
    return raw_result

  result = list()
  col_types = parse_column_types(exec_result.schema)

  for row in exec_result.data:
    cols = row.split('\t')
    assert len(cols) == len(col_types)
    new_cols = list()
    for i in xrange(len(cols)):
      if col_types[i] == 'STRING':
        new_cols.append("'%s'" % cols[i])
      else:
        new_cols.append(cols[i])
    result.append(','.join(new_cols))
  return result
