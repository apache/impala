# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import fnmatch
import re

from collections import defaultdict
from tests.performance.query import Query
from tests.util.test_file_parser import parse_query_test_file

class Workload(object):
  """Represents a workload.

  A workload is the internal representation for the set of queries on a dataset. It
  consists of the dataset name, and a mapping of query names to query strings.

  Args:
    name (str): workload name. (Eg. tpch)
    query_name_filters (list of str): List of regular expressions used for matching query
      names

  Attributes:
    name (str): workload name (Eg. tpch)
    _query_map (dict): contains a query name -> string mapping; mapping of query name to
      section (ex. "TPCH-Q10" -> "select * from...")
  """

  WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']

  def __init__(self, name, query_name_filters=None):
    self._name = name
    self._query_map = dict()
    # Build the query name -> string mapping in the c'tor. We want to fail fast and early
    # if the user input is bad.
    self._validate_and_load(query_name_filters)

  @property
  def name(self):
    return self._name

  @property
  def query_map(self):
    return self._query_map

  def _validate_and_load(self, query_name_filters):
    """Validates that the Workload is legal."""
    query_name_filters = map(str.strip, query_name_filters) if query_name_filters else []
    self._base_dir = os.path.join(Workload.WORKLOAD_DIR, self._name, 'queries')
    # Check whether the workload name corresponds to an existing directory.
    if not os.path.isdir(self._base_dir):
      raise ValueError("Workload %s not found in %s" % (self._name, self._base_dir))
    sections = list()
    # Parse all queries files for the given workload.
    for file_name in self._list_query_files():
      sections.extend(parse_query_test_file(file_name))
    # If the user has specified query names, check whether all the user specified queries
    # exist in the query files.
    all_query_names = [s['QUERY_NAME'] for s in sections if s['QUERY_NAME'].strip()]
    regex = re.compile(r'|'.join(['^%s$' % n for n in query_name_filters]), re.I)
    matched_query_names = filter(lambda x: re.match(regex, x), all_query_names)
    assert len(matched_query_names) > 0, "No matching queries found for %s" % self._name
    # Filter the sections based on the queries the user wants.
    sections = filter(lambda x: x['QUERY_NAME'] in matched_query_names, sections)
    # Add the filtered queries to the query map
    for section in sections:
      self._query_map[section['QUERY_NAME']] = section['QUERY']

  def _list_query_files(self):
    """Return a list of all the .test files that contain queries"""
    query_files = list()
    for root, dirs, file_names in os.walk(self._base_dir):
      for file_name in fnmatch.filter(file_names, '*.test'):
        query_files.append(os.path.join(root, file_name))
    assert len(query_files) > 0, "No Query Files found in %s" % self._base_dir
    return query_files

  def construct_queries(self, test_vector, scale_factor):
    """Transform a query map into a list of query objects.

    Transform all the queries in the workload's query map to query objects based on the
    input test vector and scale factor.

    Args:
      test_vector (?): query vector
      scale_factor (str): eg. "300gb"

    Returns:
      (list of Query): these will be consumed by ?
    """

    queries = list()
    for query_name, query_str in self._query_map.iteritems():
      queries.append(Query(name=query_name,
                           query_str=query_str,
                           workload=self._name,
                           scale_factor=scale_factor,
                           test_vector=test_vector))
    return queries

