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

# Talk to an impalad through beeswax.
# Usage:
#   * impalad is a string with the host and port of the impalad
#     with which the connection should be established.
#     The format is "<hostname>:<port>"
#   * query_string is the query to be executed, as a string.
#   client = ImpalaBeeswaxClient(impalad)
#   client.connect()
#   result = client.execute(query_string)
#   where result is an object of the class ImpalaBeeswaxResult.
from __future__ import absolute_import, division, print_function
from builtins import filter, map
import logging
import time
import shlex
import getpass
import re

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from datetime import datetime
from ImpalaService import ImpalaService
from tests.util.thrift_util import create_transport
from thrift.transport.TTransport import TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException

LOG = logging.getLogger('impala_beeswax')


# Custom exception wrapper.
# All exceptions coming from thrift/beeswax etc. go through this wrapper.
# TODO: Add the ability to print some of the stack.
class ImpalaBeeswaxException(Exception):
  __name__ = "ImpalaBeeswaxException"

  def __init__(self, message, inner_exception):
    self.__message = message
    self.inner_exception = inner_exception

  def __str__(self):
    return self.__message


class ImpalaBeeswaxResult(object):
  def __init__(self, **kwargs):
    self.query = kwargs.get('query', None)
    self.query_id = kwargs['query_id']
    self.success = kwargs.get('success', False)
    # Insert returns an int, convert into list to have a uniform data type.
    # TODO: We should revisit this if we have more datatypes to deal with.
    self.data = kwargs.get('data', None)
    if not isinstance(self.data, list):
      self.data = str(self.data)
      self.data = [self.data]
    self.log = None
    self.time_taken = kwargs.get('time_taken', 0)
    self.summary = kwargs.get('summary', str())
    self.schema = kwargs.get('schema', None)
    self.column_types = None
    self.column_labels = None
    if self.schema is not None:
      # Extract labels and types so there is a shared interface with HS2ResultSet.
      self.column_types = [fs.type.upper() for fs in self.schema.fieldSchemas]
      self.column_labels = [fs.name.upper() for fs in self.schema.fieldSchemas]
    self.runtime_profile = kwargs.get('runtime_profile', str())
    self.exec_summary = kwargs.get('exec_summary', None)

  def get_data(self):
    return self.__format_data()

  def __format_data(self):
    if self.data:
      return '\n'.join(self.data)
    return ''

  def __str__(self):
    message = ('Summary: %s\n'
               'Success: %s\n'
               'Took: %s(s)\n'
               'Data:\n%s\n'
               % (self.summary, self.success, self.time_taken,
                  self.__format_data()))
    return message


# Interface to beeswax. Responsible for executing queries, fetching results.
class ImpalaBeeswaxClient(object):
  # Regex applied to all tokens of a query to detect the query type.
  INSERT_REGEX = re.compile("^insert$", re.I)

  def __init__(self, impalad, use_kerberos=False, user=None, password=None,
               use_ssl=False):
    self.connected = False
    split_impalad = impalad.split(":")
    assert len(split_impalad) in [1, 2]
    self.impalad_host = split_impalad[0]
    self.impalad_port = 21000  # Default beeswax port
    if len(split_impalad) == 2:
      self.impalad_port = int(split_impalad[1])
    self.imp_service = None
    self.transport = None
    self.use_kerberos = use_kerberos
    self.use_ssl = use_ssl
    self.user, self.password = user, password
    self.use_ldap = (self.user is not None)
    self.__query_options = {}
    self.query_states = QueryState._NAMES_TO_VALUES

  def __options_to_string_list(self):
    return ["%s=%s" % (k, v) for (k, v) in self.__query_options.items()]

  def get_query_options(self):
    return self.__query_options

  def set_query_option(self, name, value):
    self.__query_options[name.upper()] = value

  def set_query_options(self, query_option_dict):
    if query_option_dict is None:
      raise ValueError('Cannot pass None value for query options')
    self.clear_query_options()
    for name, value in query_option_dict.items():
      self.set_query_option(name, value)

  def get_query_option(self, name):
    return self.__query_options.get(name.upper())

  def clear_query_options(self):
    self.__query_options.clear()

  def connect(self):
    """Connect to impalad specified in intializing this object

    Raises an exception if the connection is unsuccesful.
    """
    try:
      self.transport = self.__get_transport()
      self.transport.open()
      # TODO: TBinaryProtocol led to negative size error, check if this is a known
      #       issue in Thrift
      protocol = TBinaryProtocol.TBinaryProtocolAccelerated(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      self.connected = True
    except Exception as e:
      raise ImpalaBeeswaxException(self.__build_error_message(e), e)

  def close_connection(self):
    """Close the transport if it's still open"""
    if self.transport:
      self.transport.close()
    self.connected = False

  def __get_transport(self):
    """Creates the proper transport type based environment (secure vs unsecure)"""
    trans_type = 'buffered'
    if self.use_kerberos:
      trans_type = 'kerberos'
    elif self.use_ldap:
      trans_type = 'plain_sasl'
    return create_transport(host=self.impalad_host, port=self.impalad_port,
                            service='impala', transport_type=trans_type, user=self.user,
                            password=self.password, use_ssl=self.use_ssl)

  def execute(self, query_string, user=None, fetch_profile_after_close=False):
    """Re-directs the query to its appropriate handler, returns ImpalaBeeswaxResult"""
    # Take care of leading/trailing whitespaces.
    query_string = query_string.strip()
    start = time.time()
    start_time = datetime.now()
    handle = self.__execute_query(query_string.strip(), user=user)
    if self.__get_query_type(query_string) == 'insert':
      # DML queries are finished by this point.
      time_taken = time.time() - start

      if not fetch_profile_after_close:
        # fetch_results() will close the query after which there is no guarantee that
        # profile and log will be available so fetch them first.
        runtime_profile = self.get_runtime_profile(handle)

      exec_summary = self.get_exec_summary_and_parse(handle)
      log = self.get_log(handle.log_context)

      result = self.fetch_results(query_string, handle)

      if fetch_profile_after_close:
        # Fetch the profile again after the query has closed and the profile is complete.
        runtime_profile = self.get_runtime_profile(handle)

      result.time_taken, result.start_time, result.runtime_profile, result.log = \
          time_taken, start_time, runtime_profile, log
      result.exec_summary = exec_summary
    else:
      # For SELECT queries, execution might still be ongoing. fetch_results() will block
      # until the query is completed.
      result = self.fetch_results(query_string, handle)
      result.time_taken = time.time() - start
      result.start_time = start_time
      result.exec_summary = self.get_exec_summary_and_parse(handle)
      result.log = self.get_log(handle.log_context)

      if not fetch_profile_after_close:
        result.runtime_profile = self.get_runtime_profile(handle)

      self.close_query(handle)

      if fetch_profile_after_close:
        # Fetch the profile again after the query has closed and the profile is complete.
        result.runtime_profile = self.get_runtime_profile(handle)

    return result

  def get_exec_summary(self, handle):
    return self.__do_rpc(lambda: self.imp_service.GetExecSummary(handle))

  def get_exec_summary_and_parse(self, handle):
    """Calls GetExecSummary() for the last query handle, parses it and returns a summary
    table. Returns None in case of an error or an empty result"""
    try:
      summary = self.get_exec_summary(handle)
    except ImpalaBeeswaxException:
      summary = None

    if summary is None or summary.nodes is None:
      return None
      # If exec summary is not implemented in Impala, this function returns, so we do not
      # get the function build_exec_summary_table which requires TExecStats to be
      # imported.

    output = list()
    self.__build_summary_table(summary, output)
    return output

  def __build_summary_table(self, summary, output):
    from shell.impala_client import build_exec_summary_table
    result = list()
    build_exec_summary_table(summary, 0, 0, False, result, is_prettyprint=False,
                             separate_prefix_column=True)
    keys = ['prefix', 'operator', 'num_hosts', 'num_instances', 'avg_time', 'max_time',
            'num_rows', 'est_num_rows', 'peak_mem', 'est_peak_mem', 'detail']
    for row in result:
      assert len(keys) == len(row)
      summ_map = dict(zip(keys, row))
      output.append(summ_map)

  def get_runtime_profile(self, handle):
    return self.__do_rpc(lambda: self.imp_service.GetRuntimeProfile(handle))

  def execute_query_async(self, query_string, user=None):
    """
    Executes a query asynchronously

    Issues a query and returns the query handle to the caller for processing.
    """
    query = BeeswaxService.Query()
    query.query = query_string
    query.hadoop_user = user if user is not None else getpass.getuser()
    query.configuration = self.__options_to_string_list()
    handle = self.__do_rpc(lambda: self.imp_service.query(query,))
    LOG.info("Started query {0}".format(handle.id))
    return handle

  def __execute_query(self, query_string, user=None):
    """Executes a query and waits for completion"""
    handle = self.execute_query_async(query_string, user=user)
    # Wait for the query to finish execution.
    self.wait_for_finished(handle)
    return handle

  def cancel_query(self, query_id):
    return self.__do_rpc(lambda: self.imp_service.Cancel(query_id))

  def close_query(self, handle):
    self.__do_rpc(lambda: self.imp_service.close(handle))

  def _get_sleep_interval(self, start_time):
    """Returns the time to sleep in seconds before polling again. This uses a fixed
       50 millisecond sleep that doesn't vary by elapsed time. This is only used for
       testing, and there is no reason to sleep longer for test environments."""
    return 0.05

  def wait_for_finished(self, query_handle):
    """Given a query handle, polls the coordinator waiting for the query to transition to
       'FINISHED' state"""
    loop_start = time.time()
    while True:
      start_rpc_time = time.time()
      query_state = self.get_state(query_handle)
      rpc_time = time.time() - start_rpc_time
      # if the rpc succeeded, the output is the query state
      if query_state == self.query_states["FINISHED"]:
        break
      elif query_state == self.query_states["EXCEPTION"]:
        try:
          error_log = self.__do_rpc(
            lambda: self.imp_service.get_log(query_handle.log_context))
          raise ImpalaBeeswaxException(error_log, None)
        finally:
          self.close_query(query_handle)
      sleep_time = self._get_sleep_interval(loop_start)
      if rpc_time < sleep_time:
        time.sleep(sleep_time - rpc_time)

  def wait_for_finished_timeout(self, query_handle, timeout=10):
    """Given a query handle and a timeout, polls the coordinator waiting for the query to
       transition to 'FINISHED' state till 'timeout' seconds"""
    start_time = time.time()
    while (time.time() - start_time < timeout):
      start_rpc_time = time.time()
      query_state = self.get_state(query_handle)
      rpc_time = time.time() - start_rpc_time
      # if the rpc succeeded, the output is the query state
      if query_state == self.query_states["FINISHED"]:
        return True
      elif query_state == self.query_states["EXCEPTION"]:
        try:
          error_log = self.__do_rpc(
            lambda: self.imp_service.get_log(query_handle.log_context))
          raise ImpalaBeeswaxException(error_log, None)
        finally:
          self.close_query(query_handle)
      sleep_time = self._get_sleep_interval(start_time)
      if rpc_time < sleep_time:
        time.sleep(sleep_time - rpc_time)
    return False

  def wait_for_admission_control(self, query_handle):
    """Given a query handle, polls the coordinator waiting for it to complete
      admission control processing of the query"""
    while True:
      query_state = self.get_state(query_handle)
      if query_state > self.query_states["COMPILED"]:
        break
      time.sleep(0.05)

  def get_admission_result(self, query_handle):
    """Given a query handle, returns the admission result from the query profile"""
    query_state = self.get_state(query_handle)
    if query_state > self.query_states["COMPILED"]:
      query_profile = self.get_runtime_profile(query_handle)
      admit_result = re.search(r"Admission result: (.*)", query_profile)
      if admit_result:
        return admit_result.group(1)
    return ""

  def get_default_configuration(self):
    return self.__do_rpc(lambda: self.imp_service.get_default_configuration(False))

  def get_state(self, query_handle):
    return self.__do_rpc(lambda: self.imp_service.get_state(query_handle))

  def get_log(self, query_handle):
    return self.__do_rpc(lambda: self.imp_service.get_log(query_handle))

  def fetch_results(self, query_string, query_handle, max_rows=-1):
    """Fetches query results given a handle and query type (insert, use, other)"""
    query_type = self.__get_query_type(query_string)
    if query_type == 'use':
      # TODO: "use <database>" does not currently throw an error. Need to update this
      # to handle the error case once that behavior has been changed.
      return ImpalaBeeswaxResult(query=query_string, query_id=query_handle.id,
                                 success=True, data=[])

    # Result fetching for insert is different from other queries.
    exec_result = None
    if query_type == 'insert':
      exec_result = self.__fetch_insert_results(query_handle)
    else:
      exec_result = self.__fetch_results(query_handle, max_rows)
    exec_result.query = query_string
    return exec_result

  def __fetch_results(self, handle, max_rows=-1):
    """Handles query results, returns a ImpalaBeeswaxResult object"""
    schema = self.__do_rpc(lambda: self.imp_service.get_results_metadata(handle)).schema
    # The query has finished, we can fetch the results
    result_rows = []
    while len(result_rows) < max_rows or max_rows < 0:
      fetch_rows = -1 if max_rows < 0 else max_rows - len(result_rows)
      results = self.__do_rpc(lambda: self.imp_service.fetch(handle, False, fetch_rows))
      result_rows.extend(results.data)
      if not results.has_more:
        break

    # The query executed successfully and all the data was fetched.
    exec_result = ImpalaBeeswaxResult(query_id=handle.id, success=True, data=result_rows,
                                      schema=schema)
    exec_result.summary = 'Returned %d rows' % (len(result_rows))
    return exec_result

  def close_dml(self, handle):
    return self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))

  def __fetch_insert_results(self, handle):
    """Executes an insert query"""
    result = self.close_dml(handle)
    # The insert was successful
    num_rows = sum(map(int, result.rows_modified.values()))
    data = ["%s: %s" % row for row in result.rows_modified.items()]
    exec_result = ImpalaBeeswaxResult(query_id=handle.id, success=True, data=data)
    exec_result.summary = "Inserted %d rows" % (num_rows,)
    return exec_result

  def __get_query_type(self, query_string):
    # Set posix=True and add "'" to escaped quotes
    # to deal with escaped quotes in string literals
    lexer = shlex.shlex(query_string.lstrip(), posix=True)
    lexer.escapedquotes += "'"
    tokens = list(lexer)
    # Do not classify explain queries as 'insert'
    if (tokens[0].lower() == "explain"):
      return tokens[0].lower()
    # Because the WITH clause may precede INSERT or SELECT queries,
    # just checking the first token is insufficient.
    if list(filter(self.INSERT_REGEX.match, tokens)):
      return "insert"
    return tokens[0].lower()

  def __build_error_message(self, exception):
    """Construct a meaningful exception string"""
    message = str(exception)
    if isinstance(exception, BeeswaxService.BeeswaxException):
      message = exception.message
    return message

  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking.

    Catches all the relevant exceptions and re throws them wrapped
    in a custom exception [ImpalaBeeswaxException].
    """
    if not self.connected:
      raise ImpalaBeeswaxException("Not connected", None)
    try:
      return rpc()
    except BeeswaxService.BeeswaxException as b:
      raise ImpalaBeeswaxException(self.__build_error_message(b), b)
    except TTransportException as e:
      self.connected = False
      raise ImpalaBeeswaxException(self.__build_error_message(e), e)
    except TApplicationException as t:
      raise ImpalaBeeswaxException(self.__build_error_message(t), t)
    except Exception as u:
      raise ImpalaBeeswaxException(self.__build_error_message(u), u)
