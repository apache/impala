#!/usr/bin/env python
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

from tests.util.test_file_parser import QueryTestSectionReader

# TODO: This interface needs to be more robust; At the moment, it has two users with
# completely different uses (the benchmark suite and the impala test suite)
class Query(object):
  """Represents a query and all the information neede to execute it"""
  def __init__(self, **kwargs):
    self.query_str = kwargs.get('query_str')
    self.name = kwargs.get('name')
    self.scale_factor = kwargs.get('scale_factor')
    self.test_vector = kwargs.get('test_vector')
    self.results = kwargs.get('results')
    self.workload_name = kwargs.get('workload')
    self.table_format_str = kwargs.get('table_format_str', str())
    self.db = None
    # Only attempt to build the query if a query_str has been passed to the c'tor.
    # If it's None, assume the user wants to set a qualified query_str
    if self.query_str: self.__build_query()

  def __eq__(self, other):
    return (self.query_str == other.query_str and
            self.name == other.name and
            self.scale_factor == other.scale_factor and
            self.test_vector == other.test_vector and
            self.workload_name == other.workload_name and
            self.db == other.db)

  def __build_query(self):
    self.db = QueryTestSectionReader.get_db_name(self.test_vector, self.scale_factor)
    self.query_str = QueryTestSectionReader.build_query(self.query_str.strip())
    self.table_format_str = '%s/%s/%s' % (self.test_vector.file_format,
                                          self.test_vector.compression_codec,
                                          self.test_vector.compression_type)

  def __str__(self):
    msg = "Name: %s, Workload: %s, Scale Factor: %s, Table Format: %s" % (self.name,
        self.workload_name, self.scale_factor, self.table_format_str)
    return msg


class QueryResult(object):
  """Contains the results of a query execution.

  A query execution results contains the following fields:
  query - The query object
  time_taken - Time taken to execute the query
  start_time - The time at which the client submits the query.
  data - Query results
  client_name - The thread id
  runtime_profile - Saved runtime profile of the query's execution.
  query_error - Empty string if the query succeeded. Error returned by the client if
                it failed.
  """
  def __init__(self, query, **kwargs):
    self.query = query
    self.time_taken = kwargs.get('time_taken', 0.0)
    self.__summary = kwargs.get('summary', str())
    self.data = kwargs.get('data', str())
    self.start_time = kwargs.get('start_time')
    self.query_config = kwargs.get('query_config')
    self.client_name = kwargs.get('client_name')
    self.runtime_profile = kwargs.get('runtime_profile', str())
    self.success = kwargs.get('success', False)
    self.query_error = str()
    self.executor_name = str()

  @property
  def summary(self):
    return self.__summary

  @summary.setter
  def summary(self, value):
    self.__summary = value

  def __str__(self):
    """Print human readable query execution details"""
    msg = "Query: %s, Start Time: %s, Time Taken: %s, Client Name: %s" % (self.query,
        self.start_time, self.time_taken, self.client_name)
    if not self.success: msg += " Error: %s" % self.query_error
    return msg
