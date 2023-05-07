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

# This module is used for common utilities related to parsing test files

from __future__ import absolute_import, division, print_function
from builtins import map
import codecs
import collections
import logging
import os
import os.path
import re

from collections import defaultdict
from textwrap import dedent

LOG = logging.getLogger('impala_test_suite')

# constants
SECTION_DELIMITER = "===="
SUBSECTION_DELIMITER = "----"
ALLOWED_TABLE_FORMATS = ['kudu', 'parquet', 'orc']


# The QueryTestSectionReader provides utility functions that help to parse content
# from a query test file
class QueryTestSectionReader(object):
  @staticmethod
  def build_query(query_section_text):
    """Build a query by stripping comments and trailing newlines and semi-colons."""
    query_section_text = remove_comments(query_section_text)
    return query_section_text.rstrip("\n;")

  @staticmethod
  def get_table_name_components(table_format, table_name, scale_factor=''):
    """
    Returns a pair (db_name, tbl_name). If the table_name argument is
    fully qualified, return the database name mentioned there,
    otherwise get the default db name from the table format and scale
    factor.
    """
    # If table name is fully qualified return the db prefix
    split = table_name.split('.')
    assert len(split) <= 2, 'Unexpected table format: %s' % table_name
    db_name = split[0] if len(split) == 2 else \
        QueryTestSectionReader.get_db_name(table_format, scale_factor)
    return (db_name, split[-1])

  @staticmethod
  def get_db_name(table_format, scale_factor=''):
    """
    Get the database name to use.

    Database names are dependent on the scale factor, file format, compression type
    and compression codec. This method returns the appropriate database name to the
    caller based on the table format information provided.
    """
    if table_format.file_format == 'text' and table_format.compression_codec == 'none':
      suffix = ''
    elif table_format.compression_codec == 'none':
      suffix = '_%s' % (table_format.file_format)
    elif table_format.compression_type == 'record':
      suffix = '_%s_record_%s' % (table_format.file_format,
                                   table_format.compression_codec)
    else:
      suffix = '_%s_%s' % (table_format.file_format, table_format.compression_codec)
    dataset = table_format.dataset.replace('-', '')
    return dataset + scale_factor + suffix


def remove_comments(section_text):
  lines = [line for line in section_text.split('\n') if not line.strip().startswith('#')]
  return '\n'.join(lines)


def parse_query_test_file(file_name, valid_section_names=None, encoding=None):
  """
  Reads the specified query test file accepting the given list of valid section names
  Uses a default list of valid section names if valid_section_names is None

  Returns the result as a list of dictionaries. Each dictionary in the list corresponds
  to a test case and each key in the dictionary maps to a section in that test case.
  """
  # Update the valid section names as we support other test types
  # (ex. planner, data error)
  section_names = valid_section_names
  if section_names is None:
    section_names = ['QUERY', 'HIVE_QUERY', 'RESULTS', 'TYPES', 'LABELS', 'SETUP',
        'CATCH', 'ERRORS', 'USER', 'RUNTIME_PROFILE', 'SHELL', 'DML_RESULTS',
        'HS2_TYPES', 'HIVE_MAJOR_VERSION', 'LINEAGE', 'IS_HDFS_ONLY']
  return parse_test_file(file_name, section_names, encoding=encoding,
      skip_unknown_sections=False)


def parse_table_constraints(constraints_file):
  """Reads a table constraints file, if one exists"""
  schema_include = defaultdict(list)
  schema_exclude = defaultdict(list)
  schema_only = defaultdict(list)
  if not os.path.isfile(constraints_file):
    LOG.info('No schema constraints file file found')
  else:
    with open(constraints_file, 'rb') as constraints_file:
      for line in constraints_file.readlines():
        line = line.strip()
        if not line or line.startswith('#'):
          continue
        # Format: table_name:<name>, constraint_type:<type>, table_format:<t1>,<t2>,...
        table_name, constraint_type, table_formats =\
            [value.split(':')[1].strip() for value in line.split(',', 2)]

        # 'only' constraint -- If a format defines an only constraint, only those tables
        # collected for the same table_format will be created.
        if constraint_type == 'only':
          for f in map(parse_table_format_constraint, table_formats.split(',')):
            schema_only[f].append(table_name.lower())
        elif constraint_type == 'restrict_to':
          schema_include[table_name.lower()] +=\
              list(map(parse_table_format_constraint, table_formats.split(',')))
        elif constraint_type == 'exclude':
          schema_exclude[table_name.lower()] +=\
              list(map(parse_table_format_constraint, table_formats.split(',')))
        else:
          raise ValueError('Unknown constraint type: %s' % constraint_type)
  return schema_include, schema_exclude, schema_only


def parse_table_format_constraint(table_format_constraint):
  # TODO: Expand how we parse table format constraints to support syntax such as
  # a table format string with a wildcard character. Right now we don't do anything.
  return table_format_constraint


def parse_test_file(test_file_name, valid_section_names, skip_unknown_sections=True,
    encoding=None):
  """
  Parses an Impala test file

  Test files have the format:
  ==== <- Section
  ---- [Name] <- Named subsection
  // some text
  ---- [Name2] <- Named subsection
  ...
  ====

  The valid section names are passed in to this function. The encoding to use
  when reading the data can be specified with the 'encoding' flag.
  """
  with open(test_file_name, 'rb') as test_file:
    file_data = test_file.read()
    if encoding: file_data = file_data.decode(encoding)
    if os.environ["USE_APACHE_HIVE"] == "true":
      # Remove Hive 4.0 feature for tpcds_schema_template.sql
      if "tpcds_schema_template" in test_file_name:
        # HIVE-20703
        file_data = file_data.replace(
          'set hive.optimize.sort.dynamic.partition.threshold=1;', '')
        # HIVE-18284
        file_data = file_data.replace('distribute by ss_sold_date_sk', '')
    return parse_test_file_text(file_data, valid_section_names,
                                skip_unknown_sections)


def parse_runtime_profile_table_formats(subsection_comment):
  prefix = "table_format="
  if not subsection_comment.startswith(prefix):
    raise RuntimeError('RUNTIME_PROFILE comment (%s) must be of the form '
                       '"table_format=FORMAT[,FORMAT2,...]"' % subsection_comment)

  parsed_formats = subsection_comment[len(prefix):].split(',')
  table_formats = list()
  for table_format in parsed_formats:
    if table_format not in ALLOWED_TABLE_FORMATS:
      raise RuntimeError('RUNTIME_PROFILE table format (%s) must be in: %s' %
                         (table_format, ALLOWED_TABLE_FORMATS))
    else:
      table_formats.append(table_format)
  return table_formats


def parse_test_file_text(text, valid_section_names, skip_unknown_sections=True):
  sections = list()
  section_start_regex = re.compile(r'(?m)^%s' % SECTION_DELIMITER)
  match = section_start_regex.search(text)
  if match is not None:
    # Assume anything before the first section (==== tag) is a header and ignore it. To
    # ensure that test will not be skipped unintentionally we reject headers that start
    # with what looks like a subsection.
    header = text[:match.start()]
    if re.match(r'^%s' % SUBSECTION_DELIMITER, header):
      raise RuntimeError(dedent("""
          Header must not start with '%s'. Everything before the first line matching '%s'
          is considered header information and will be ignored. However a header must not
          start with '%s' to prevent test cases from accidentally being ignored.""" %
          (SUBSECTION_DELIMITER, SECTION_DELIMITER, SUBSECTION_DELIMITER)))
    text = text[match.start():]

  # Split the test file up into sections. For each section, parse all subsections.
  for section in section_start_regex.split(text):
    parsed_sections = collections.defaultdict(str)
    for sub_section in re.split(r'(?m)^%s' % SUBSECTION_DELIMITER, section[1:]):
      # Skip empty subsections
      if not sub_section.strip():
        continue

      lines = sub_section.split('\n')
      subsection_name = lines[0].strip()
      subsection_comment = None

      subsection_info = [s.strip() for s in subsection_name.split(':')]
      if len(subsection_info) == 2:
        subsection_name, subsection_comment = subsection_info
        if subsection_comment == "":
          # ignore empty subsection_comment
          subsection_comment = None

      lines_content = lines[1:-1]

      subsection_str = '\n'.join([line for line in lines_content])
      if len(lines_content) != 0:
        # Add trailing newline to last line if present. This disambiguates between the
        # case of no lines versus a single line with no text.
        # This extra newline is needed only for the verification of the subsections of
        # RESULTS, DML_RESULTS and ERRORS and will be removed in write_test_file()
        # when '--update_results' is added to output the updated golden file.
        subsection_str += "\n"

      if subsection_name not in valid_section_names:
        if skip_unknown_sections or not subsection_name:
          print(sub_section)
          print('Unknown section \'%s\'' % subsection_name)
          continue
        else:
          raise RuntimeError('Unknown subsection: %s' % subsection_name)

      if subsection_name == 'QUERY' and subsection_comment:
        parsed_sections['QUERY_NAME'] = subsection_comment

      if subsection_name == 'RESULTS' and subsection_comment:
        for comment in subsection_comment.split(','):
          if comment == 'MULTI_LINE':
            parsed_sections['MULTI_LINE'] = comment
          elif comment == 'RAW_STRING':
            parsed_sections['RAW_STRING'] = comment
          elif comment.startswith('VERIFY'):
            parsed_sections['VERIFIER'] = comment
          else:
            raise RuntimeError('Unknown subsection comment: %s' % comment)

      if subsection_name == 'CATCH':
        parsed_sections['CATCH'] = list()
        if subsection_comment is None:
          parsed_sections['CATCH'].append(subsection_str)
        elif subsection_comment == 'ANY_OF':
          parsed_sections['CATCH'].extend(lines_content)
        else:
          raise RuntimeError('Unknown subsection comment: %s' % subsection_comment)
        for exception_str in parsed_sections['CATCH']:
          assert exception_str.strip(), "Empty exception string."
        continue

      # The DML_RESULTS section is used to specify what the state of the table should be
      # after executing a DML query (in the QUERY section). The target table name must
      # be specified in a table comment, and then the expected rows in the table are the
      # contents of the section. If the TYPES and LABELS sections are provided, they
      # will be verified against the DML_RESULTS. Using both DML_RESULTS and RESULTS is
      # not supported.
      if subsection_name == 'DML_RESULTS':
        if subsection_comment is None:
          raise RuntimeError('DML_RESULTS requires that the table is specified '
              'in the comment.')
        parsed_sections['DML_RESULTS_TABLE'] = subsection_comment
        parsed_sections['VERIFIER'] = 'VERIFY_IS_EQUAL_SORTED'

      # The RUNTIME_PROFILE section is used to specify lines of text that should be
      # present in the query runtime profile. It takes an option comment containing a
      # table format. RUNTIME_PROFILE sections with a comment are only evaluated for the
      # specified format. If there is a RUNTIME_PROFILE section without a comment, it is
      # evaluated for all formats that don't have a commented section for this query.
      if subsection_name == 'RUNTIME_PROFILE':
        if subsection_comment:
          table_formats = parse_runtime_profile_table_formats(subsection_comment)
          for table_format in table_formats:
            subsection_name_for_format = 'RUNTIME_PROFILE_%s' % table_format
            parsed_sections[subsection_name_for_format] = subsection_str
          continue

      parsed_sections[subsection_name] = subsection_str

    if parsed_sections:
      sections.append(parsed_sections)
  return sections


def split_section_lines(section_str):
  """
  Given a section string as produced by parse_test_file_text(), split it into separate
  lines. The section string must have a trailing newline.
  """
  if section_str == '':
    return []
  assert section_str[-1] == '\n'
  # Trim off the trailing newline and split into lines.
  return section_str[:-1].split('\n')


def join_section_lines(lines):
  """
  The inverse of split_section_lines().
  The extra newline at the end will be removed in write_test_file() so that when the
  actual contents of a subsection do not match the expected contents, we won't see
  extra newlines in those subsections (ERRORS, TYPES, LABELS, RESULTS and DML_RESULTS).
  """
  return '\n'.join(lines) + '\n'


def write_test_file(test_file_name, test_file_sections, encoding=None):
  """
  Given a list of test file sections, write out the corresponding test file

  This is useful when updating the results of a test.
  The file encoding can be specified in the 'encoding' parameter. If not specified
  the default system encoding will be used.
  """
  with codecs.open(test_file_name, 'w', encoding=encoding) as test_file:
    test_file_text = list()
    for test_case in test_file_sections:
      test_file_text.append(SECTION_DELIMITER)
      for section_name, section_value in test_case.items():
        # Have to special case query name and verifier because they have annotations
        # in the headers
        if section_name in ['QUERY_NAME', 'VERIFIER']:
          continue

        # TODO: We need a more generic way of persisting the old test file.
        # Special casing will blow up.
        full_section_name = section_name
        if section_name == 'QUERY' and test_case.get('QUERY_NAME'):
          full_section_name = '%s: %s' % (section_name, test_case['QUERY_NAME'])
        if section_name == 'RESULTS' and test_case.get('VERIFIER'):
          full_section_name = '%s: %s' % (section_name, test_case['VERIFIER'])
        test_file_text.append("%s %s" % (SUBSECTION_DELIMITER, full_section_name))
        # We remove the extra newlines added in parse_test_file_text() so that in the
        # updated golden file we will not see an extra newline at the end of each
        # subsection.
        section_value = ''.join(test_case[section_name]).strip()
        if section_value:
          test_file_text.append(section_value)
    test_file_text.append(SECTION_DELIMITER)
    test_file.write(('\n').join(test_file_text))


def load_tpc_queries(workload, include_stress_queries=False, query_name_filters=[]):
  """
  Returns a list of queries for the given workload. 'workload' should either be 'tpch',
  'tpcds', 'tpch_nested', or 'targeted-perf'. The types of queries that are returned:
  - 'standard' queries, i.e. from the spec for that workload. These queries will have
    filenames like '{workload}-q*.test' and one query per file.
  - 'stress' queries, if 'include_stress_queries' is true, which run against data from
    the workkload but were designed to stress specific aspects of Impala. These queries
    have filenames like '{workload}-stress-*.test' and may have multiple queries per file.
  - 'targeted' queries, if the workload is 'targeted-perf', which have no restrictions.
  The returned queries are filtered according to 'query_name_filters', a list of query
  name regexes, if specified.
  All queries are required to have a name specified, i.e. each test case should have:
    ---- QUERY: WORKLOAD-<QUERY_NAME>
  """
  LOG.info("Loading %s queries", workload)
  queries = dict()
  query_dir = os.path.join(
      os.environ['IMPALA_HOME'], "testdata", "workloads", workload, "queries")
  # Check whether the workload name corresponds to an existing directory.
  if not os.path.isdir(query_dir):
    raise ValueError("Workload %s not found in %s" % (workload, query_dir))

  # IMPALA-6715 and others from the past: This pattern enforces the queries we actually
  # find. Both workload directories contain other queries that are not part of the TPC
  # spec.
  file_workload = workload
  if workload == "tpcds":
    # TPCDS is assumed to always use decimal_v2, which is the default since 3.0
    file_workload = "tpcds-decimal_v2"
  if include_stress_queries:
    file_name_pattern = re.compile(r"^{0}-(q.*|stress-.*).test$".format(file_workload))
  else:
    file_name_pattern = re.compile(r"^{0}-(q.*).test$".format(file_workload))

  query_name_pattern = re.compile(r"^{0}-(.*)$".format(workload.upper()))
  if workload == "tpch_nested":
    query_name_pattern = re.compile(r"^TPCH-(.*)$")

  if workload == "targeted-perf":
    # We don't enforce any restrictions on targeted-perf queries.
    file_name_pattern = re.compile(r"(.*)")
    query_name_pattern = re.compile(r"(.*)")

  if query_name_filters:
    query_name_filters = list(map(str.strip, query_name_filters))
  else:
    query_name_filters = []
  filter_regex = re.compile(r'|'.join(['^%s$' % n for n in query_name_filters]), re.I)

  for query_file in os.listdir(query_dir):
    is_standard = "stress" not in query_file and workload != "targeted-perf"
    file_match = file_name_pattern.search(query_file)
    if not file_match:
      continue
    file_path = os.path.join(query_dir, query_file)
    test_cases = parse_query_test_file(file_path)
    for test_case in test_cases:
      query_sql = remove_comments(test_case["QUERY"])

      if re.match(filter_regex, test_case["QUERY_NAME"]):
        query_name_match = query_name_pattern.search(test_case["QUERY_NAME"])
        # For standard queries, we require that the query name matches the file name.
        if is_standard and file_match.group(1).upper() != query_name_match.group(1):
          raise Exception("Query name '%s' does not match file name '%s'"
              % (query_name_match.group(1), file_match.group(1).upper()))
        queries[query_name_match.group(0)] = query_sql

    # The standard tpc queries must have one query per file.
    if is_standard and len(test_cases) != 1:
      raise Exception("Expected exactly 1 query to be in file %s but got %s"
          % (file_path, len(test_cases)))
  return queries
