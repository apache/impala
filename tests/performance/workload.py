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
#

from __future__ import absolute_import, division, print_function
import os
import fnmatch
import re

from tests.performance.query import Query
from tests.util.test_file_parser import load_tpc_queries

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
    self._query_map = load_tpc_queries(self._name, query_name_filters=query_name_filters)
    assert len(self._query_map) > 0, "No matching queries found for %s" % self._name

  @property
  def name(self):
    return self._name

  @property
  def query_map(self):
    return self._query_map

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
    for query_name, query_str in self._query_map.items():
      queries.append(Query(name=query_name,
                           query_str=query_str,
                           workload=self._name,
                           scale_factor=scale_factor,
                           test_vector=test_vector))
    return queries

