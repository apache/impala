# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This module is used for common utilities related to parsing test files
import collections
import codecs
import logging
import re
from collections import defaultdict
from os.path import isfile, isdir
from tests.common.test_dimensions import TableFormatInfo

LOG = logging.getLogger('impala_test_suite')

# constants
SECTION_DELIMITER = "===="
SUBSECTION_DELIMITER = "----"

# The QueryTestSectionReader provides utility functions that help to parse content
# from a query test file
class QueryTestSectionReader(object):
  @staticmethod
  def build_query(query_section_text):
    """Build a query by stripping comments and trailing semi-colons."""
    query_section_text = remove_comments(query_section_text)
    return query_section_text.rstrip(';')

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
      suffix =  '_%s_record_%s' % (table_format.file_format,
                                   table_format.compression_codec)
    else:
      suffix =  '_%s_%s' % (table_format.file_format, table_format.compression_codec)
    dataset = table_format.dataset.replace('-', '')
    return dataset + scale_factor + suffix


def remove_comments(section_text):
  return '\n'.join([l for l in section_text.split('\n') if not l.strip().startswith('#')])

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
    section_names = ['QUERY', 'RESULTS', 'TYPES', 'LABELS', 'SETUP', 'CATCH', 'ERRORS',
        'USER', 'RUNTIME_PROFILE']
  return parse_test_file(file_name, section_names, encoding=encoding,
      skip_unknown_sections=False)

def parse_table_constraints(constraints_file):
  """Reads a table contraints file, if one exists"""
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
        # Format: table_name:<name>, constraint_type:<type>, table_format:<t1>,<t2>,...
        table_name, constraint_type, table_formats =\
            [value.split(':')[1].strip() for value in line.split(',', 2)]
        if constraint_type == 'restrict_to':
          schema_include[table_name.lower()] +=\
              map(parse_table_format_constraint, table_formats.split(','))
        elif constraint_type == 'exclude':
          schema_exclude[table_name.lower()] +=\
              map(parse_table_format_constraint, table_formats.split(','))
        else:
          raise ValueError, 'Unknown constraint type: %s' % constraint_type
  return schema_include, schema_exclude

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
    return parse_test_file_text(file_data, valid_section_names,
                                skip_unknown_sections)

def parse_test_file_text(text, valid_section_names, skip_unknown_sections=True):
  sections = list()
  section_start_regex = re.compile(r'(?m)^%s' % SECTION_DELIMITER)
  match = section_start_regex.search(text)
  if match is not None:
    # Assume anything before the first section (==== tag) is a header and ignore it
    text = text[match.start():]

  # Split the test file up into sections. For each section, parse all subsections.
  for section in section_start_regex.split(text):
    parsed_sections = collections.defaultdict(str)
    for sub_section in re.split(r'(?m)^%s' % SUBSECTION_DELIMITER, section[1:]):
      # Skip empty subsections
      if not sub_section:
        continue

      lines = sub_section.split('\n')
      subsection_name = lines[0].strip()
      subsection_comment = None

      subsection_info = [s.strip() for s in subsection_name.split(':')]
      if(len(subsection_info) == 2):
        subsection_name, subsection_comment = subsection_info

      if subsection_name not in valid_section_names:
        if skip_unknown_sections or not subsection_name:
          print sub_section
          print 'Unknown section %s' % subsection_name
          continue
        else:
          raise RuntimeError, 'Unknown subsection: %s' % subsection_name

      if subsection_name == 'QUERY' and subsection_comment:
        parsed_sections['QUERY_NAME'] = subsection_comment

      if subsection_name == 'RESULTS' and subsection_comment:
        for comment in subsection_comment.split(','):
          if comment == 'MULTI_LINE':
            parsed_sections['MULTI_LINE'] = comment
          elif comment.startswith('VERIFY'):
            parsed_sections['VERIFIER'] = comment
          else:
            raise RuntimeError, 'Unknown subsection comment: %s' % comment

      parsed_sections[subsection_name] = '\n'.join([line for line in lines[1:-1]])

    if parsed_sections:
      sections.append(parsed_sections)
  return sections


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
        if test_case[section_name].strip():
          test_file_text.append(test_case[section_name])
    test_file_text.append(SECTION_DELIMITER)
    test_file.write(('\n').join(test_file_text))
