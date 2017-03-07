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

# This modules contians utility functions used to help verify query test results.

import logging
import math
import re

from functools import wraps
from tests.util.test_file_parser import (join_section_lines, remove_comments,
    split_section_lines)
from tests.util.hdfs_util import NAMENODE

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('test_result_verfier')

# Special prefix for column values that indicates the actual column value
# is equal to the expected one if the actual value matches the given regex.
# Accepted syntax in test files is 'regex: pattern' without the quotes.
COLUMN_REGEX_PREFIX_PATTERN = "regex:"
COLUMN_REGEX_PREFIX = re.compile(COLUMN_REGEX_PREFIX_PATTERN, re.I)

# Special prefix for row values that indicates the actual row value
# is equal to the expected one if the actual value matches the given regex.
ROW_REGEX_PREFIX_PATTERN = 'row_regex:'
ROW_REGEX_PREFIX = re.compile(ROW_REGEX_PREFIX_PATTERN, re.I)

# Represents a single test result (row set)
class QueryTestResult(object):
  def __init__(self, result_list, column_types, column_labels, order_matters):
    self.column_types = column_types
    self.result_list = result_list
    # The order of the result set might be different if running with multiple nodes.
    # Unless there is an ORDER BY clause, the results should be sorted for comparison.
    test_results = result_list
    if not order_matters:
      test_results = sorted(result_list)
    self.rows = [ResultRow(row, column_types, column_labels) for row in test_results]

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
  def __init__(self, row_string, column_types, column_labels):
    self.columns = self.__parse_row(row_string, column_types, column_labels)
    self.row_string = row_string
    # If applicable, pre-compile the regex that actual row values (row_string)
    # should be matched against instead of self.columns.
    self.regex = try_compile_regex(row_string)

  def __parse_row(self, row_string, column_types, column_labels):
    """Parses a row string and build a list of ResultColumn objects"""
    column_values = list()
    if not row_string:
      return column_values
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
      column_values.append(ResultColumn(col_val, column_types[current_column],
          column_labels[current_column]))
      current_column = current_column + 1
    return column_values

  def __getitem__(self, key):
    """Allows accessing a column value using the column alias or the position of the
    column in the result set. All values are returned as strings and an exception is
    thrown if the column label or column position does not exist."""
    if isinstance(key, basestring):
      for col in self.columns:
        if col.column_label == key.lower(): return col.value
      raise IndexError, 'No column with label: ' + key
    elif isinstance(key, int):
      # If the key (column position) does not exist this will throw an IndexError when
      # indexing into the self.columns
      return str(self.columns[key])
    raise TypeError, 'Unsupported indexing key type: ' + type(key)

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    # Check equality based on a supplied regex if one was given.
    if self.regex is not None:
      return self.regex.match(other.row_string)
    if other.regex is not None:
      return other.regex.match(self.row_string)
    return self.columns == other.columns

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return ','.join(['%s' % col for col in self.columns])

# Check if the string is a row regex, and if so compile it.
# Return None if the row does not have a regex prefix.
def try_compile_regex(row_string):
  if row_string and ROW_REGEX_PREFIX.match(row_string):
    pattern = row_string[len(ROW_REGEX_PREFIX_PATTERN):].strip()
    regex = re.compile(pattern)
    if regex is None:
      assert False, "Invalid row regex specification: %s" % row_string
    return regex
  return None

# If comparing against a float or double, don't do a strict comparison
# See: http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm
def compare_float(x, y, epsilon):
  # For the purposes of test validation, we want to treat nans as equal.  The
  # floating point spec defines nan != nan.
  if math.isnan(x) and math.isnan(y):
    return True
  if math.isinf(x) or math.isinf(y):
    return x == y
  return abs(x - y) <= epsilon

# Represents a column in a row
class ResultColumn(object):
  def __init__(self, value, column_type, column_label):
    """Value of the column and the type (double, float, string, etc...)"""
    self.value = value
    self.column_type = column_type.lower()
    self.column_label = column_label.lower()
    # If applicable, pre-compile the regex that actual column values
    # should be matched against instead of self.value.
    self.regex = None
    if COLUMN_REGEX_PREFIX.match(value):
      pattern = self.value[len(COLUMN_REGEX_PREFIX_PATTERN):].strip()
      self.regex = re.compile(pattern)
      if self.regex is None:
        assert False, "Invalid column regex specification: %s" % self.value

  def __eq__(self, other):
    if not isinstance(other, self.__class__):
      return False
    # Make sure the column types are the same
    if self.column_type != other.column_type:
      return False
    # Check equality based on a supplied regex if one was given.
    if self.regex is not None:
      return self.regex.match(other.value)
    if other.regex is not None:
      return other.regex.match(self.value)

    if (self.value == 'NULL' or other.value == 'NULL'):
      return self.value == other.value
    elif self.column_type == 'float':
      return compare_float(float(self.value), float(other.value), 10e-5)
    elif self.column_type == 'double':
      return compare_float(float(self.value), float(other.value), 10e-10)
    elif self.column_type == 'boolean':
      return str(self.value).lower() == str(other.value).lower()
    else:
      return self.value == other.value

  def __ne__(self, other):
    return not self.__eq__(other)

  def __str__(self):
    return self.value

  def __repr__(self):
    return 'Type: %s Value: %s' % (self.column_type, self.value)

def assert_args_not_none(*args):
  for arg in args:
    assert arg is not None

def convert_results_to_sets(expected_results, actual_results):
  assert_args_not_none(expected_results, actual_results)
  expected_set = set(map(str, expected_results.rows))
  actual_set = set(map(str, actual_results.rows))
  return expected_set, actual_set

def verify_query_result_is_subset(expected_results, actual_results):
  """Check whether the results in expected_results are a subset of the results in
  actual_results. This uses set semantics, i.e. any duplicates are ignored."""
  expected_set, actual_set = convert_results_to_sets(expected_results, actual_results)
  assert expected_set <= actual_set

def verify_query_result_is_superset(expected_results, actual_results):
  """Check whether the results in expected_results are a superset of the results in
  actual_results. This uses set semantics, i.e. any duplicates are ignored."""
  expected_set, actual_set = convert_results_to_sets(expected_results, actual_results)
  assert expected_set >= actual_set


def verify_query_result_is_equal(expected_results, actual_results):
  assert_args_not_none(expected_results, actual_results)
  assert expected_results == actual_results

def verify_query_result_is_not_in(expected_results, actual_results):
  assert_args_not_none(expected_results, actual_results)
  expected_set = set(map(str, expected_results.rows))
  actual_set = set(map(str, actual_results.rows))
  assert expected_set.isdisjoint(actual_set)

# Global dictionary that maps the verification type to appropriate verifier.
# The RESULTS section of a .test file is tagged with the verifier type. We may
# add more verifiers in the future. If a tag is not found, it defaults to verifying
# equality.
VERIFIER_MAP = {'VERIFY_IS_SUBSET' : verify_query_result_is_subset,
                'VERIFY_IS_SUPERSET' : verify_query_result_is_superset,
                'VERIFY_IS_EQUAL_SORTED'  : verify_query_result_is_equal,
                'VERIFY_IS_EQUAL'  : verify_query_result_is_equal,
                'VERIFY_IS_NOT_IN' : verify_query_result_is_not_in,
                 None              : verify_query_result_is_equal}

def verify_results(expected_results, actual_results, order_matters):
  """Verifies the actual versus expected result strings"""
  assert_args_not_none(expected_results, actual_results)
  # The order of the result set might be different if running with multiple nodes. Unless
  # there is an order by clause, sort the expected and actual results before comparison.
  if not order_matters:
    expected_results = sorted(expected_results)
    actual_results = sorted(actual_results)
  assert expected_results == actual_results

def verify_errors(expected_errors, actual_errors):
  """Convert the errors to our test format, treating them as a single string column row
  set if not a row_regex. This requires enclosing the data in single quotes."""
  converted_expected_errors = []
  for expected_error in expected_errors:
    if not expected_error: continue
    if ROW_REGEX_PREFIX.match(expected_error):
      converted_expected_errors.append(expected_error)
    else:
      converted_expected_errors.append("'%s'" % expected_error)
  expected = QueryTestResult(converted_expected_errors, ['STRING'], ['DUMMY_LABEL'],
      order_matters=False)
  actual = QueryTestResult(["'%s'" % l for l in actual_errors if l], ['STRING'],
      ['DUMMY_LABEL'], order_matters=False)
  VERIFIER_MAP['VERIFY_IS_EQUAL'](expected, actual)

def apply_error_match_filter(error_list, replace_filenames=True):
  """Applies a filter to each entry in the given list of errors to ensure result matching
  is stable."""
  file_regex = r'%s.*/[\w\.\-]+' % NAMENODE
  def replace_fn(row):
    # The actual file path isn't very interesting and can vary. Change it to a canonical
    # string that allows result rows to sort in the same order as expected rows.
    if replace_filenames: row = re.sub(file_regex, '__HDFS_FILENAME__', row)
    # The "Backend <id>" can also vary, so filter it out as well.
    return re.sub(r'Backend \d+:', '', row)
  return [replace_fn(row) for row in error_list]

def verify_raw_results(test_section, exec_result, file_format, update_section=False,
                       replace_filenames=True, result_section='RESULTS'):
  """
  Accepts a raw exec_result object and verifies it matches the expected results,
  including checking the ERRORS, TYPES, and LABELS test sections.
  If update_section is true, updates test_section with the actual results
  if they don't match the expected results. If update_section is false, failed
  verifications result in assertion failures, otherwise they are ignored.

  This process includes the parsing/transformation of the raw data results into the
  result format used in the tests.

  The result_section parameter can be used to make this function check the results in
  a DML_RESULTS section instead of the regular RESULTS section.
  TODO: separate out the handling of sections like ERRORS from checking of query results
  to allow regular RESULTS/ERRORS sections in tests with DML_RESULTS (IMPALA-4471).
  """
  expected_results = None
  if result_section in test_section:
    expected_results = remove_comments(test_section[result_section])
  else:
    assert 'ERRORS' not in test_section, "'ERRORS' section must have accompanying 'RESULTS' section"
    LOG.info("No results found. Skipping verification");
    return
  if 'ERRORS' in test_section:
    expected_errors = split_section_lines(remove_comments(test_section['ERRORS']))
    actual_errors = apply_error_match_filter(exec_result.log.split('\n'),
                                             replace_filenames)
    try:
      verify_errors(expected_errors, actual_errors)
    except AssertionError:
      if update_section:
        test_section['ERRORS'] = join_section_lines(actual_errors)
      else:
        raise

  if 'TYPES' in test_section:
    # Distinguish between an empty list and a list with an empty string.
    expected_types = list()
    if test_section.get('TYPES'):
      expected_types = [c.strip().upper() for c in test_section['TYPES'].rstrip('\n').split(',')]

    # Avro and Kudu represent TIMESTAMP columns as strings, so tests using TIMESTAMP are
    # skipped because results will be wrong.
    if file_format in ('avro', 'kudu') and 'TIMESTAMP' in expected_types:
        LOG.info("TIMESTAMP columns unsupported in %s, skipping verification." %\
            file_format)
        return

    # Avro does not support as many types as Hive, so the Avro test tables may
    # have different column types than we expect (e.g., INT instead of
    # TINYINT). Bypass the type checking by ignoring the actual types of the Avro
    # table.
    if file_format == 'avro':
      LOG.info("Skipping type verification of Avro-format table.")
      actual_types = expected_types
    else:
      actual_types = parse_column_types(exec_result.schema)

    try:
      verify_results(expected_types, actual_types, order_matters=True)
    except AssertionError:
      if update_section:
        test_section['TYPES'] = join_section_lines([', '.join(actual_types)])
      else:
        raise
  else:
    # This is an insert, so we are comparing the number of rows inserted
    expected_types = ['BIGINT']
    actual_types = ['BIGINT']

  actual_labels = ['DUMMY_LABEL']
  if exec_result and exec_result.schema:
    actual_labels = parse_column_labels(exec_result.schema)

  if 'LABELS' in test_section:
    assert actual_labels is not None
    # Distinguish between an empty list and a list with an empty string.
    expected_labels = list()
    if test_section.get('LABELS'):
      expected_labels = [c.strip().upper() for c in test_section['LABELS'].split(',')]
    try:
      verify_results(expected_labels, actual_labels, order_matters=True)
    except AssertionError:
      if update_section:
        test_section['LABELS'] = join_section_lines([', '.join(actual_labels)])
      else:
        raise

  # Get the verifier if specified. In the absence of an explicit
  # verifier, defaults to verifying equality.
  verifier = test_section.get('VERIFIER')

  order_matters = contains_order_by(exec_result.query)

  # If the test section is explicitly annotated to specify the order matters,
  # then do not sort the actual and expected results.
  if verifier and verifier.upper() == 'VERIFY_IS_EQUAL':
    order_matters = True

  # If the test result section is explicitly annotated to specify order does not matter,
  # then sort the actual and expected results before verification.
  if verifier and verifier.upper() == 'VERIFY_IS_EQUAL_SORTED':
    order_matters = False
  expected_results_list = []
  if 'MULTI_LINE' in test_section:
    expected_results_list = map(lambda s: s.replace('\n', '\\n'),
        re.findall(r'\[(.*?)\]', expected_results, flags=re.DOTALL))
  else:
    expected_results_list = split_section_lines(expected_results)
  expected = QueryTestResult(expected_results_list, expected_types,
      actual_labels, order_matters)
  actual = QueryTestResult(parse_result_rows(exec_result), actual_types,
      actual_labels, order_matters)
  assert verifier in VERIFIER_MAP.keys(), "Unknown verifier: " + verifier
  try:
    VERIFIER_MAP[verifier](expected, actual)
  except AssertionError:
    if update_section:
      test_section[results_section] = join_section_lines(actual.result_list)
    else:
      raise

def contains_order_by(query):
  """Returns true of the query contains an 'order by' clause"""
  return re.search( r'order\s+by\b', query, re.M|re.I) is not None

def parse_column_types(schema):
  """Enumerates all field schemas and returns a list of column type strings"""
  return [fs.type.upper() for fs in schema.fieldSchemas]

def parse_column_labels(schema):
  """Enumerates all field schemas and returns a list of column label strings"""
  # This is an statement doesn't return a field schema (insert statement).
  if schema is None or schema.fieldSchemas is None: return list()
  return [fs.name.upper() for fs in schema.fieldSchemas]

def create_query_result(exec_result, order_matters=False):
  """Creates query result in the test format from the result returned from a query"""
  col_labels = parse_column_labels(exec_result.schema)
  col_types = parse_column_types(exec_result.schema)
  data = parse_result_rows(exec_result)
  return QueryTestResult(data, col_types, col_labels, order_matters)

def parse_result_rows(exec_result):
  """
  Parses a query result set and transforms it to the format used by the query test files
  """
  raw_result = exec_result.data
  if not raw_result:
    return []

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
      if col_types[i] == 'STRING' or col_types[i] == 'CHAR':
        col = cols[i].encode('unicode_escape')
        new_cols.append("'%s'" % col)
      else:
        new_cols.append(cols[i])
    result.append(','.join(new_cols))
  return result

# Special syntax for basic aggregation over fields in the runtime profile.
# The syntax is:
# aggregation(function, field_name): expected_value
# Currently, the only implemented function is SUM and only integers are supported.
AGGREGATION_PREFIX_PATTERN = 'aggregation\('
AGGREGATION_PREFIX = re.compile(AGGREGATION_PREFIX_PATTERN)
AGGREGATION_SYNTAX_MATCH_PATTERN = 'aggregation\((\w+)[ ]*,[ ]*(\w+)\):[ ]*(\d+)'

def try_compile_aggregation(row_string):
  """
  Check to see if this row string specifies an aggregation. If the row string contains
  an aggregation, it returns a tuple with all the information for evaluating the
  aggregation. Otherwise, it returns None.
  """
  if row_string and AGGREGATION_PREFIX.match(row_string):
    function, field, value = re.findall(AGGREGATION_SYNTAX_MATCH_PATTERN, row_string)[0]
    # Validate function
    assert(function == 'SUM')
    # Validate value is integer
    expected_value = int(value)
    return (function, field, expected_value)
  return None

def compute_aggregation(function, field, runtime_profile):
  """
  Evaluate an aggregation function over a field on the runtime_profile. This skips
  the averaged fragment and returns the aggregate value. It currently supports only
  integer values and the SUM function.
  """
  start_avg_fragment_re = re.compile('[ ]*Averaged Fragment')
  field_regex = "{0}: (\d+)".format(field)
  field_regex_re = re.compile(field_regex)
  inside_avg_fragment = False
  avg_fragment_indent = None
  past_avg_fragment = False
  match_list = []
  for line in runtime_profile.splitlines():
    # Detect the boundaries of the averaged fragment by looking at indentation.
    # The averaged fragment starts with a particular indentation level. All of
    # its children are at a greater indent. When the indentation gets back to
    # the level of the the averaged fragment start, then the averaged fragment
    # is done.
    if inside_avg_fragment:
      indentation = len(line) - len(line.lstrip())
      if indentation > avg_fragment_indent:
        continue
      else:
        inside_avg_fragment = False
        past_avg_fragment = True

    if not past_avg_fragment and start_avg_fragment_re.match(line):
      inside_avg_fragment = True
      avg_fragment_indent = len(line) - len(line.lstrip())
      continue

    if (field_regex_re.search(line)):
      match_list.extend(re.findall(field_regex, line))

  int_match_list = map(int, match_list)
  result = None
  if function == 'SUM':
    result = sum(int_match_list)

  return result

def verify_runtime_profile(expected, actual):
  """
  Check that lines matching all of the expected runtime profile entries are present
  in the actual text runtime profile. The check passes if, for each of the expected
  rows, at least one matching row is present in the actual runtime profile. Rows
  with the "row_regex:" prefix are treated as regular expressions. Rows with
  the "aggregation(function,field): value" syntax specifies an aggregation over
  the runtime profile.
  """
  expected_lines = remove_comments(expected).splitlines()
  matched = [False] * len(expected_lines)
  expected_regexes = []
  expected_aggregations = []
  for expected_line in expected_lines:
    expected_regexes.append(try_compile_regex(expected_line))
    expected_aggregations.append(try_compile_aggregation(expected_line))

  # Check the expected and actual rows pairwise.
  for line in actual.splitlines():
    for i in xrange(len(expected_lines)):
      if matched[i]: continue
      if expected_regexes[i] is not None:
        match = expected_regexes[i].match(line)
      elif expected_aggregations[i] is not None:
        # Aggregations are enforced separately
        match = True
      else:
        match = expected_lines[i].strip() == line.strip()
      if match:
        matched[i] = True
        break

  unmatched_lines = []
  for i in xrange(len(expected_lines)):
    if not matched[i]:
      unmatched_lines.append(expected_lines[i])
  assert len(unmatched_lines) == 0, ("Did not find matches for lines in runtime profile:"
      "\nEXPECTED LINES:\n%s\n\nACTUAL PROFILE:\n%s" % ('\n'.join(unmatched_lines),
        actual))

  # Compute the aggregations and check against values
  for i in xrange(len(expected_aggregations)):
    if (expected_aggregations[i] is None): continue
    function, field, expected_value = expected_aggregations[i]
    actual_value = compute_aggregation(function, field, actual)
    assert actual_value == expected_value, ("Aggregation of %s over %s did not match "
        "expected results.\nEXPECTED VALUE:\n%d\n\nACTUAL VALUE:\n%d"
        "\n\nPROFILE:\n%s\n" % (function, field, expected_value, actual_value, actual))

def get_node_exec_options(profile_string, exec_node_id):
  """ Return a list with all of the ExecOption strings for the given exec node id. """
  results = []
  matched_node = False
  id_string = "(id={0})".format(exec_node_id)
  for line in profile_string.splitlines():
    if matched_node and line.strip().startswith("ExecOption:"):
      results.append(line.strip())
    matched_node = False
    if id_string in line:
      # Check for the ExecOption string on the next line.
      matched_node = True
  return results

def assert_codegen_enabled(profile_string, exec_node_ids):
  """ Check that codegen is enabled for the given exec node ids by parsing the text
  runtime profile in 'profile_string'"""
  for exec_node_id in exec_node_ids:
    for exec_options in get_node_exec_options(profile_string, exec_node_id):
      assert 'Codegen Enabled' in exec_options
      assert not 'Codegen Disabled' in exec_options
