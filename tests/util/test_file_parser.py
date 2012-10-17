#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This module is used for common utilities related to parsing test files
import collections
import logging
import re
from collections import defaultdict
from os.path import isfile, isdir

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

  # Strip out all comments
  stripped_file_text = '\n'.join(\
      [line for line in test_file.read().split('\n') if not line.strip().startswith('#')])

  # Split the test file up into sections. For each section parse all subsections.
  for section in stripped_file_text.split('===='):
    parsed_sections = collections.defaultdict(str)
    for sub_section in section.split('----')[1:]:
      lines = sub_section.split('\n')
      subsection_name = lines[0].strip()
      if subsection_name not in valid_section_names:
        if skip_unknown_sections or not subsection_name:
          print 'Unknown section %s' % subsection_name
          continue
        else:
          raise RuntimeError, 'Unknown subsection: %s' % subsection_name

      # TODO: This is currently broken due to stripping out all the comments before
      # parsing test sections. The best fix would be to change how QUERY_NAME is defined
      query_name = re.findall('QUERY_NAME : .*', sub_section)
      if query_name:
        parsed_sections['QUERY_NAME'] = query_name[0].split(':')[-1].strip()

      parsed_sections[subsection_name] = '\n'.join([line.strip() for line in lines[1:-1]])
    if parsed_sections:
      sections.append(parsed_sections)
  return sections
