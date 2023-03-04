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

from __future__ import absolute_import, division, print_function
from tests.util.test_file_parser import QueryTestSectionReader

# TODO: This interface needs to be more robust; At the moment, it has two users with
# completely different uses (the benchmark suite and the impala test suite)
class Query(object):
  """Represents a query and all the information neede to execute it

  Attributes:
    query_str (str): SQL query string; contains 1 or more ;-delimited SQL statements.
    name (str): query name?
    scale_factor (str): for example 300gb, used to determine the database.
    test_vector (?): Specifies some parameters
    results (list of ?): ?
    workload_name (str): for example tpch, tpcds, visa (used to determine directory)
    db (str): ? represents the database
    table_format_str (str): ?
  """
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
    if self.query_str: self._build_query()

  def __eq__(self, other):
    return (self.query_str == other.query_str and
            self.name == other.name and
            self.scale_factor == other.scale_factor and
            self.test_vector == other.test_vector and
            self.workload_name == other.workload_name and
            self.db == other.db)

  def __hash__(self):
    return hash((self.query_str, self.name, self.scale_factor, self.test_vector, self.db))

  def _build_query(self):
    """Populates db, query_str, table_format_str"""
    self.db = QueryTestSectionReader.get_db_name(self.test_vector, self.scale_factor)
    self.query_str = QueryTestSectionReader.build_query(self.query_str.strip())
    self.table_format_str = '%s/%s/%s' % (self.test_vector.file_format,
                                          self.test_vector.compression_codec,
                                          self.test_vector.compression_type)

  def __str__(self):
    msg = "Name: %s, Workload: %s, Scale Factor: %s, Table Format: %s" % (self.name,
        self.workload_name, self.scale_factor, self.table_format_str)
    return msg


class HiveQueryResult(object):
  """Contains the results of a query execution.

  Parameters:
    Required:
      query (Query): The query object associated with this result.
      start_time (datetime): Timestamp at the start of execution.
      query_config (HiveHS2QueryExecConfig)
      client_name (int): The thread id

    Optional:
      time_taken (float): Time taken to execute the query.
      summary (str): query exection summary (ex. returned 10 rows)
      data (list of str): Query results returned by Impala.
      success (bool): True if the execution was successful.

  Attributes - these are modified by another class:
    query_error (str): Empty string if the query succeeded. Error returned by the client
        if it failed.
    executor_name (str)
  """

  def __init__(self, query, **kwargs):
    self.query = query
    self.time_taken = kwargs.get('time_taken', 0.0)
    self._summary = kwargs.get('summary', str())
    self.data = kwargs.get('data', str())
    self.start_time = kwargs.get('start_time')
    self.query_config = kwargs.get('query_config')
    self.client_name = kwargs.get('client_name')
    self.success = kwargs.get('success', False)
    self.query_error = str()
    self.executor_name = str()

  @property
  def summary(self):
    return self._summary

  @summary.setter
  def summary(self, value):
    self._summary = value

  def __str__(self):
    """Print human readable query execution details"""
    msg = "Query: %s, Start Time: %s, Time Taken: %s, Client Name: %s" % (self.query,
        self.start_time, self.time_taken, self.client_name)
    if not self.success: msg += " Error: %s" % self.query_error
    return msg


class ImpalaQueryResult(HiveQueryResult):
  """Contains the results of an Impala query execution.

  Parameters:
    Required:
      query (Query): The query object associated with this result.
      start_time (datetime): Timestamp at the start of execution.
      query_config (BeeswaxQueryExecConfig, HS2QueryExecConfig)
      client_name (int): The thread id

    Optional:
      time_taken (float): Time taken to execute the query.
      summary (str): query exection summary (ex. returned 10 rows)
      data (list of str): Query results returned by Impala.
      runtime_profile (str): Saved runtime profile of the query's execution.
      exec_summary (TExecSummary)
      success (bool): True if the execution was successful.

  Attributes - these are modified by another class:
    query_error (str): Empty string if the query succeeded. Error returned by the client
        if it failed.
    executor_name (str)
  """

  def __init__(self, query, **kwargs):
    super(ImpalaQueryResult, self).__init__(query, **kwargs)
    self.runtime_profile = kwargs.get('runtime_profile', str())
    self.exec_summary = kwargs.get('exec_summary', str())
