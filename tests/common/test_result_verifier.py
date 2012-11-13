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

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('test_result_verfier')

def verify_results(expected_results, actual_results, order_matters):
  """Verifies the actual versus expected results of a query"""
  assert actual_results is not None
  assert expected_results is not None

  # The order of the result set might be different if running with multiple nodes. Unless
  # there is an order by clause, sort the expected and actual results before comparison.
  if not order_matters:
    expected_results = sorted(expected_results)
    actual_results = sorted(actual_results)

  expected = "Expected: \n%s\n\n" % '\n'.join(expected_results)
  actual = "Actual: \n%s\n\n" % '\n'.join(actual_results)

  # TODO: Still need to add support for verifying regex results. For now just skip
  # this verification
  if len(expected_results) > 0 and 'regex:' in expected_results[0]:
    return

  assert expected_results == actual_results, '%s%s' % (expected, actual)

def verify_column_types(actual_col_types, exec_result_schema):
  actual_col_types = [c.strip().upper() for c in actual_col_types.split(',')]
  expected_col_types = parse_column_types(exec_result_schema)
  verify_results(actual_col_types, expected_col_types, order_matters=True)

def verify_raw_results(test_section, exec_result):
  """
  Accepts a raw exec_result object and verifies it matches the expected results

  This process includes the parsing/transformation of the raw data results into the
  result format used in the tests. It also chooses the appropriate result section to
  verify from the test case because insert tests need to verify different things
  than select tests.
  """
  expected_results = None

  # If there is a 'PARTITIONS' section then assume this is an insert test and get the
  # expected results from there
  if 'PARTITIONS' in test_section:
    partition_results = list()
    for row in test_section['PARTITIONS'].split('\n'):
      # TODO: Currently we don't get the partition names back so strip these out of the
      # expected results
      if ':' in row:
        partition_results.append(row.split(':')[1].strip())
      elif row.strip():
        partition_results.append(row.strip())

    # TODO: Currently we just return the total number of rows inserted rather than
    # rows per partition. This should be updated to validated on a per-partition
    # basis.
    expected_results = str(sum([int(p_count) for p_count in partition_results]))
  else:
    expected_results = test_section['RESULTS']

  if 'TYPES' in test_section:
    verify_column_types(test_section['TYPES'], exec_result.schema)
  verify_results(expected_results.split('\n'), parse_result_rows(exec_result),
                 contains_order_by(exec_result.query))

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
