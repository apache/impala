# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This module is used for common utilities related to parsing test files
import collections
import logging
import re
from collections import defaultdict
from os.path import isfile, isdir
from tests.common.test_dimensions import TableFormatInfo

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('impala_test_suite')

# The QueryTestSectionReader provides utility functions that help to parse content
# from a query test file
class QueryTestSectionReader(object):
  @staticmethod
  def build_query(query_section_text, table_format_info, scale_factor):
    """
    Build a well formed query.

    Given the various test parameters, construct the query that will be executed. This
    does more work than replace_table_suffix because it needs to properly replace the
    database name based on the given scale factor
    """
    query_section_text = remove_comments(query_section_text)
    return query_section_text.rstrip(';')

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
      suffix =  '_%s_record_%s' % (table_format.file_format,
                                   table_format.compression_codec)
    else:
      suffix =  '_%s_%s' % (table_format.file_format, table_format.compression_codec)
    dataset = table_format.dataset.replace('-', '')
    return dataset + scale_factor + suffix


def remove_comments(section_text):
  return '\n'.join([l for l in section_text.split('\n') if not l.strip().startswith('#')])

def parse_query_test_file(file_name):
  """
  Reads the specified query test file

  Returns the result as a list of dictionaries. Each dictionary in the list corresponds
  to a test case and each key in the dictionary maps to a section in that test case.
  """
  # Update the valid section names as we support other test types
  # (ex. planner, data error)
  VALID_SECTION_NAMES = ['QUERY', 'RESULTS', 'TYPES', 'PARTITIONS', 'SETUP']
  return parse_test_file(file_name, VALID_SECTION_NAMES)

def parse_table_constraints(constraints_file):
  """
  Reads a table contraints file, if one exists

  TODO: once the python test frame changes are committed this can be moved to a common
  utility function so the tests themselves can make use of this code.
  """
  schema_include = defaultdict(list)
  schema_exclude = defaultdict(list)
  if not isfile(constraints_file):
    LOG.info('No schema constraints file file found')
  else:
    with open(constraints_file, 'rb') as constraints_file:
      for line in constraints_file.readlines():
        line = line.strip()
        if not line or line.startswith('#'):
          continue
        # Format: table_name:<name>, contraint_type:<type>, file_format:<t1>,<t2>,...
        table_name, constraint_type, file_types =\
            [value.split(':')[1].strip() for value in line.split(',', 2)]
        if constraint_type == 'restrict_to':
          schema_include[table_name.lower()] += file_types.split(',')
        elif constraint_type == 'exclude':
          schema_exclude[table_name.lower()] += file_types.split(',')
        else:
          raise ValueError, 'Unknown constraint type: %s' % constraint_type
  return schema_include, schema_exclude

def parse_test_file(test_file_name, valid_section_names, skip_unknown_sections=True):
  """
  Parses an Impala test file

  Test files have the format:
  ==== <- Section
  ---- [Name] <- Named subsection
  // some text
  ---- [Name2] <- Named subsection
  ...
  ====

  The valid section names are passed in to this function.
  """
  test_file = open(test_file_name, 'rb')
  sections = list()

  # Read test file, stripping out all comments
  file_lines = [l for l in test_file.read().split('\n') if not l.strip().startswith('#')]

  # Split the test file up into sections. For each section parse all subsections.
  for section in '\n'.join(file_lines).split('===='):
    parsed_sections = collections.defaultdict(str)
    for sub_section in section.split('----')[1:]:
      lines = sub_section.split('\n')
      subsection_name = lines[0].strip()
      subsection_comment = None

      subsection_info = [s.strip() for s in subsection_name.split(':')]
      if(len(subsection_info) == 2):
        subsection_name, subsection_comment = subsection_info

      if subsection_name not in valid_section_names:
        if skip_unknown_sections or not subsection_name:
          print 'Unknown section %s' % subsection_name
          continue
        else:
          raise RuntimeError, 'Unknown subsection: %s' % subsection_name

      if subsection_name == 'QUERY' and subsection_comment:
        parsed_sections['QUERY_NAME'] = subsection_comment

      parsed_sections[subsection_name] = '\n'.join([line for line in lines[1:-1]])

    if parsed_sections:
      sections.append(parsed_sections)
  test_file.close()
  return sections


def write_test_file(test_file_name, test_file_sections):
  """
  Given a list of test file sections, write out the corresponding test file

  This is useful when updating the results of a test.
  """
  with open(test_file_name, 'w') as test_file:
    for test_case in test_file_sections:
      test_file.write("====\n")
      for section_name, section_value in test_case.items():
        # Have to special case query name because it is a section name annotation
        if section_name == 'QUERY_NAME':
          continue

        full_section_name = section_name
        if section_name == 'QUERY' and 'QUERY_NAME' in test_case:
          full_section_name = '%s : %s' % (section_name, test_case['QUERY_NAME'])
        test_file.write('---- %s\n' % full_section_name)
        test_file.write(test_case[section_name] + '\n')
    test_file.write('====')
