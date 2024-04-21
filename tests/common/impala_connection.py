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
# Common for connections to Impala. Currently supports Beeswax connections and
# in the future will support HS2 connections. Provides tracing around all
# operations.

from __future__ import absolute_import, division, print_function
import abc
import codecs
from future.utils import with_metaclass
import logging
import re

import impala.dbapi as impyla
import tests.common
from RuntimeProfile.ttypes import TRuntimeProfileFormat
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient


LOG = logging.getLogger('impala_connection')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# All logging needs to be either executable SQL or a SQL comment (prefix with --).
console_handler.setFormatter(logging.Formatter('%(message)s'))
LOG.addHandler(console_handler)
LOG.propagate = False

# Regular expression that matches the "progress" entry in the HS2 log.
PROGRESS_LOG_RE = re.compile(
    r'^Query [a-z0-9:]+ [0-9]+% Complete \([0-9]+ out of [0-9]+\)$')

MAX_SQL_LOGGING_LENGTH = 128 * 1024


# test_exprs.py's TestExprLimits executes extremely large SQLs (multiple MBs). It is the
# only test that runs SQL larger than 128KB. Logging these SQLs in execute() increases
# the size of the JUnitXML files, causing problems for users of JUnitXML like Jenkins.
# This function limits the size of the SQL logged if it is larger than 128KB.
def log_sql_stmt(sql_stmt):
  """If the 'sql_stmt' is shorter than MAX_SQL_LOGGING_LENGTH, log it unchanged. If
     it is larger than MAX_SQL_LOGGING_LENGTH, truncate it and comment it out."""
  if (len(sql_stmt) <= MAX_SQL_LOGGING_LENGTH):
    LOG.info("{0};\n".format(sql_stmt))
  else:
    # The logging output should be valid SQL, so the truncated SQL is commented out.
    LOG.info("-- Skip logging full SQL statement of length {0}".format(len(sql_stmt)))
    LOG.info("-- Logging a truncated version, commented out:")
    for line in sql_stmt[0:MAX_SQL_LOGGING_LENGTH].split("\n"):
      LOG.info("-- {0}".format(line))
    LOG.info("-- [...]")


# Common wrapper around the internal types of HS2/Beeswax operation/query handles.
class OperationHandle(object):
  def __init__(self, handle, sql_stmt):
    self.__handle = handle
    self.__sql_stmt = sql_stmt

  def get_handle(self):
    return self.__handle

  def sql_stmt(self):
    return self.__sql_stmt


# Represents an Impala connection.
class ImpalaConnection(with_metaclass(abc.ABCMeta, object)):

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  @abc.abstractmethod
  def set_configuration_option(self, name, value):
    """Sets a configuration option name to the given value"""
    pass

  def set_configuration(self, config_option_dict):
    """Replaces existing configuration with the given dictionary"""
    assert config_option_dict is not None, "config_option_dict cannot be None"
    self.clear_configuration()
    for name, value in config_option_dict.items():
      self.set_configuration_option(name, value)

  @abc.abstractmethod
  def clear_configuration(self):
    """Clears all existing configuration."""
    pass

  @abc.abstractmethod
  def get_default_configuration(self):
    """Return the default configuration for the connection, before any modifications are
    made to the session state. Returns a map with the config variable as the key and a
    string representation of the default value as the value."""
    pass

  @abc.abstractmethod
  def connect(self):
    """Opens the connection"""
    pass

  @abc.abstractmethod
  def close(self):
    """Closes the connection. Can be called multiple times"""
    pass

  @abc.abstractmethod
  def close_query(self, handle):
    """Closes the query."""
    pass

  @abc.abstractmethod
  def get_state(self, operation_handle):
    """Returns the state of a query"""
    pass

  @abc.abstractmethod
  def state_is_finished(self, operation_handle):
    """Returns whether the state of a query is finished"""
    pass

  @abc.abstractmethod
  def get_log(self, operation_handle):
    """Returns the log of an operation as a string, with entries separated by newlines."""
    pass

  @abc.abstractmethod
  def cancel(self, operation_handle):
    """Cancels an in-flight operation"""
    pass

  def execute(self, sql_stmt):
    """Executes a query and fetches the results"""
    pass

  @abc.abstractmethod
  def execute_async(self, sql_stmt):
    """Issues a query and returns the handle to the caller for processing. Only one
    async operation per connection at a time is supported, due to limitations of the
    Beeswax protocol and the Impyla client."""
    pass

  @abc.abstractmethod
  def fetch(self, sql_stmt, operation_handle, max_rows=-1):
    """Fetches query results up to max_rows given a handle and sql statement.
    If max_rows < 0, all rows are fetched. If max_rows > 0 but the number of
    rows returned is less than max_rows, all the rows have been fetched."""
    pass


# Represents a connection to Impala using the Beeswax API.
class BeeswaxConnection(ImpalaConnection):
  def __init__(self, host_port, use_kerberos=False, user=None, password=None,
               use_ssl=False):
    self.__beeswax_client = ImpalaBeeswaxClient(host_port, use_kerberos, user=user,
                                                password=password, use_ssl=use_ssl)
    self.__host_port = host_port
    self.QUERY_STATES = self.__beeswax_client.query_states

  def set_configuration_option(self, name, value):
    # Only set the option if it's not already set to the same value.
    if self.__beeswax_client.get_query_option(name) != value:
      LOG.info('SET %s=%s;' % (name, value))
      self.__beeswax_client.set_query_option(name, value)

  def get_default_configuration(self):
    result = {}
    for item in self.__beeswax_client.get_default_configuration():
      result[item.key] = item.value
    return result

  def clear_configuration(self):
    self.__beeswax_client.clear_query_options()
    # A hook in conftest sets tests.common.current_node.
    if hasattr(tests.common, "current_node"):
      self.set_configuration_option("client_identifier", tests.common.current_node)

  def connect(self):
    LOG.info("-- connecting to: %s" % self.__host_port)
    self.__beeswax_client.connect()

  # TODO: rename to close_connection
  def close(self):
    LOG.info("-- closing connection to: %s" % self.__host_port)
    self.__beeswax_client.close_connection()

  def close_query(self, operation_handle):
    LOG.info("-- closing query for operation handle: %s" % operation_handle)
    self.__beeswax_client.close_query(operation_handle.get_handle())

  def close_dml(self, operation_handle):
    LOG.info("-- closing DML query for operation handle: %s" % operation_handle)
    self.__beeswax_client.close_dml(operation_handle.get_handle())

  def execute(self, sql_stmt, user=None, fetch_profile_after_close=False):
    LOG.info("-- executing against %s\n" % (self.__host_port))
    log_sql_stmt(sql_stmt)
    return self.__beeswax_client.execute(sql_stmt, user=user,
        fetch_profile_after_close=fetch_profile_after_close)

  def execute_async(self, sql_stmt, user=None):
    LOG.info("-- executing async: %s\n" % (self.__host_port))
    log_sql_stmt(sql_stmt)
    beeswax_handle = self.__beeswax_client.execute_query_async(sql_stmt, user=user)
    return OperationHandle(beeswax_handle, sql_stmt)

  def cancel(self, operation_handle):
    LOG.info("-- canceling operation: %s" % operation_handle)
    return self.__beeswax_client.cancel_query(operation_handle.get_handle())

  def get_state(self, operation_handle):
    LOG.info("-- getting state for operation: %s" % operation_handle)
    return self.__beeswax_client.get_state(operation_handle.get_handle())

  def state_is_finished(self, operation_handle):
    LOG.info("-- checking finished state for operation: {0}".format(operation_handle))
    return self.get_state(operation_handle) == self.QUERY_STATES["FINISHED"]

  def get_exec_summary(self, operation_handle):
    LOG.info("-- getting exec summary operation: %s" % operation_handle)
    return self.__beeswax_client.get_exec_summary(operation_handle.get_handle())

  def get_runtime_profile(self, operation_handle):
    LOG.info("-- getting runtime profile operation: %s" % operation_handle)
    return self.__beeswax_client.get_runtime_profile(operation_handle.get_handle())

  def wait_for_finished_timeout(self, operation_handle, timeout):
    LOG.info("-- waiting for query to reach FINISHED state: %s" % operation_handle)
    return self.__beeswax_client.wait_for_finished_timeout(
      operation_handle.get_handle(), timeout)

  def wait_for_admission_control(self, operation_handle):
    LOG.info("-- waiting for completion of the admission control processing of the "
        "query: %s" % operation_handle)
    return self.__beeswax_client.wait_for_admission_control(operation_handle.get_handle())

  def get_admission_result(self, operation_handle):
    LOG.info("-- getting the admission result: %s" % operation_handle)
    return self.__beeswax_client.get_admission_result(operation_handle.get_handle())

  def get_log(self, operation_handle):
    LOG.info("-- getting log for operation: %s" % operation_handle)
    return self.__beeswax_client.get_log(operation_handle.get_handle().log_context)

  def fetch(self, sql_stmt, operation_handle, max_rows=-1):
    LOG.info("-- fetching results from: %s" % operation_handle)
    return self.__beeswax_client.fetch_results(
        sql_stmt, operation_handle.get_handle(), max_rows)


class ImpylaHS2Connection(ImpalaConnection):
  """Connection to Impala using the impyla client connecting to HS2 endpoint.
  impyla implements the standard Python dbabi: https://www.python.org/dev/peps/pep-0249/
  plus Impala-specific extensions, e.g. for fetching runtime profiles.
  TODO: implement support for kerberos, SSL, etc.
  """
  def __init__(self, host_port, use_kerberos=False, is_hive=False,
               use_http_transport=False, http_path=""):
    self.__host_port = host_port
    self.__use_http_transport = use_http_transport
    self.__http_path = http_path
    if use_kerberos:
      raise NotImplementedError("Kerberos support not yet implemented")
    # Impyla connection and cursor is initialised in connect(). We need to reuse the same
    # cursor for different operations (as opposed to creating a new cursor per operation)
    # so that the session is preserved. This means that we can only execute one operation
    # at a time per connection, which is a limitation also imposed by the Beeswax API.
    self.__impyla_conn = None
    self.__cursor = None
    # Query options to send along with each query.
    self.__query_options = {}
    self._is_hive = is_hive

  def set_configuration_option(self, name, value):
    self.__query_options[name] = str(value)

  def get_default_configuration(self):
    return self.__default_query_options.copy()

  def clear_configuration(self):
    self.__query_options.clear()
    if hasattr(tests.common, "current_node") and not self._is_hive:
      self.set_configuration_option("client_identifier", tests.common.current_node)

  def connect(self):
    LOG.info("-- connecting to {0} with impyla".format(self.__host_port))
    host, port = self.__host_port.split(":")
    conn_kwargs = {}
    if self._is_hive:
      conn_kwargs['auth_mechanism'] = 'PLAIN'
    self.__impyla_conn = impyla.connect(host=host, port=int(port),
                                        use_http_transport=self.__use_http_transport,
                                        http_path=self.__http_path, **conn_kwargs)
    # Get the default query options for the session before any modifications are made.
    self.__cursor = self.__impyla_conn.cursor(convert_types=False)
    self.__default_query_options = {}
    if not self._is_hive:
      self.__cursor.execute("set all")
      for name, val, _ in self.__cursor:
        self.__default_query_options[name] = val
      self.__cursor.close_operation()
      LOG.debug("Default query options: {0}".format(self.__default_query_options))

  def close(self):
    LOG.info("-- closing connection to: {0}".format(self.__host_port))
    try:
      # Explicitly close the cursor so that it will close the session.
      self.__cursor.close()
    except Exception as e:
      # The session may no longer be valid if the impalad was restarted during the test.
      pass
    try:
      self.__impyla_conn.close()
    except AttributeError as e:
      # When the HTTP endpoint restarts, Thrift HTTP will close the endpoint and calling
      # close() will result in an exception.
      if not (self.__use_http_transport and 'NoneType' in str(e)):
        raise

  def get_tables(self, database=None):
    """Trigger the GetTables() HS2 request on the given database (None means all dbs).
    Returns a list of (catalogName, dbName, tableName, tableType, tableComment).
    """
    LOG.info("-- getting tables for database: {0}".format(database))
    self.__cursor.get_tables(database_name=database)
    return self.__cursor.fetchall()

  def close_query(self, operation_handle):
    LOG.info("-- closing query for operation handle: {0}".format(operation_handle))
    operation_handle.get_handle().close_operation()

  def execute(self, sql_stmt, user=None, profile_format=TRuntimeProfileFormat.STRING,
      fetch_profile_after_close=False):
    self.__cursor.execute(sql_stmt, configuration=self.__query_options)
    handle = OperationHandle(self.__cursor, sql_stmt)

    r = None
    try:
      r = self.__fetch_results(handle, profile_format=profile_format)
    finally:
      if r is None:
        # Try to close the query handle but ignore any exceptions not to replace the
        # original exception raised by '__fetch_results'.
        try:
          self.close_query(handle)
        except Exception:
          pass
      elif fetch_profile_after_close:
        op_handle = handle.get_handle()._last_operation
        self.close_query(handle)

        # Match ImpalaBeeswaxResult by placing the full profile including end time and
        # duration into the return object.
        r.runtime_profile = op_handle.get_profile(profile_format)
        return r
      else:
        self.close_query(handle)
        return r

  def execute_async(self, sql_stmt, user=None):
    LOG.info("-- executing against {0} at {1}\n".format(
        self._is_hive and 'Hive' or 'Impala', self.__host_port))
    log_sql_stmt(sql_stmt)
    if user is not None:
      raise NotImplementedError("Not yet implemented for HS2 - authentication")
    try:
      self.__cursor.execute_async(sql_stmt, configuration=self.__query_options)
      handle = OperationHandle(self.__cursor, sql_stmt)
      LOG.info("Started query {0}".format(self.get_query_id(handle)))
      return handle
    except Exception:
      self.__cursor.close_operation()
      raise

  def cancel(self, operation_handle):
    LOG.info("-- canceling operation: {0}".format(operation_handle))
    cursor = operation_handle.get_handle()
    return cursor.cancel_operation(reset_state=False)

  def get_query_id(self, operation_handle):
    """Return the string representation of the query id."""
    guid_bytes = \
        operation_handle.get_handle()._last_operation.handle.operationId.guid
    return "{0}:{1}".format(codecs.encode(guid_bytes[7::-1], 'hex_codec'),
                            codecs.encode(guid_bytes[16:7:-1], 'hex_codec'))

  def get_state(self, operation_handle):
    LOG.info("-- getting state for operation: {0}".format(operation_handle))
    cursor = operation_handle.get_handle()
    return cursor.status()

  def state_is_finished(self, operation_handle):
    LOG.info("-- checking finished state for operation: {0}".format(operation_handle))
    cursor = operation_handle.get_handle()
    # cursor.status contains a string representation of one of
    # TCLIService.TOperationState.
    return cursor.status() == "FINISHED_STATE"

  def get_exec_summary(self, operation_handle):
    LOG.info("-- getting exec summary operation: {0}".format(operation_handle))
    cursor = operation_handle.get_handle()
    # summary returned is thrift, not string.
    return cursor.get_summary()

  def get_runtime_profile(self, operation_handle, profile_format):
    LOG.info("-- getting runtime profile operation: {0}".format(operation_handle))
    cursor = operation_handle.get_handle()
    return cursor.get_profile(profile_format=profile_format)

  def wait_for_finished_timeout(self, operation_handle, timeout):
    LOG.info("-- waiting for query to reach FINISHED state: {0}".format(operation_handle))
    raise NotImplementedError("Not yet implemented for HS2 - states differ from beeswax")

  def wait_for_admission_control(self, operation_handle):
    LOG.info("-- waiting for completion of the admission control processing of the "
        "query: {0}".format(operation_handle))
    raise NotImplementedError("Not yet implemented for HS2 - states differ from beeswax")

  def get_admission_result(self, operation_handle):
    LOG.info("-- getting the admission result: {0}".format(operation_handle))
    raise NotImplementedError("Not yet implemented for HS2 - states differ from beeswax")

  def get_log(self, operation_handle):
    LOG.info("-- getting log for operation: {0}".format(operation_handle))
    # HS2 includes non-error log messages that we need to filter out.
    cursor = operation_handle.get_handle()
    lines = [line for line in cursor.get_log().split('\n')
             if not PROGRESS_LOG_RE.match(line)]
    return '\n'.join(lines)

  def fetch(self, sql_stmt, handle, max_rows=-1):
    LOG.info("-- fetching results from: {0}".format(handle))
    return self.__fetch_results(handle, max_rows)

  def __fetch_results(self, handle, max_rows=-1,
                      profile_format=TRuntimeProfileFormat.STRING):
    """Implementation of result fetching from handle."""
    cursor = handle.get_handle()
    assert cursor is not None
    # Don't fetch data for queries with no results.
    result_tuples = None
    column_labels = None
    column_types = None
    if cursor.has_result_set:
      desc = cursor.description
      column_labels = [col_desc[0].upper() for col_desc in desc]
      column_types = [col_desc[1].upper() for col_desc in desc]
      if max_rows < 0:
        result_tuples = cursor.fetchall()
      else:
        result_tuples = cursor.fetchmany(max_rows)

    if not self._is_hive:
      log = self.get_log(handle)
      profile = self.get_runtime_profile(handle, profile_format=profile_format)
    else:
      log = None
      profile = None
    return ImpylaHS2ResultSet(success=True, result_tuples=result_tuples,
                              column_labels=column_labels, column_types=column_types,
                              query=handle.sql_stmt(), log=log, profile=profile,
                              query_id=self.get_query_id(handle))


class ImpylaHS2ResultSet(object):
  """This emulates the interface of ImpalaBeeswaxResult so that it can be used in
  place of it. TODO: when we deprecate/remove Beeswax, clean this up."""
  def __init__(self, success, result_tuples, column_labels, column_types, query, log,
      profile, query_id):
    self.success = success
    self.column_labels = column_labels
    self.column_types = column_types
    self.query = query
    self.log = log
    self.profile = profile
    self.query_id = query_id
    self.__result_tuples = result_tuples
    # self.data is the data in the ImpalaBeeswaxResult format: a list of rows with each
    # row represented as a tab-separated string.
    self.data = None
    if result_tuples is not None:
      self.data = [self.__convert_result_row(tuple) for tuple in result_tuples]

  def __convert_result_row(self, result_tuple):
    """Take primitive values from a result tuple and construct the tab-separated string
    that would have been returned via beeswax."""
    return '\t'.join([self.__convert_result_value(val) for val in result_tuple])

  def __convert_result_value(self, val):
    """Take a primitive value from a result tuple and its type and construct the string
    that would have been returned via beeswax."""
    if val is None:
      return 'NULL'
    if type(val) == float:
      # Same format as what Beeswax uses in the backend.
      return "{:.16g}".format(val)
    else:
      return str(val)


def create_connection(host_port, use_kerberos=False, protocol='beeswax',
    is_hive=False):
  if protocol == 'beeswax':
    c = BeeswaxConnection(host_port=host_port, use_kerberos=use_kerberos)
  elif protocol == 'hs2':
    c = ImpylaHS2Connection(host_port=host_port, use_kerberos=use_kerberos,
        is_hive=is_hive)
  else:
    assert protocol == 'hs2-http'
    c = ImpylaHS2Connection(host_port=host_port, use_kerberos=use_kerberos,
        is_hive=is_hive, use_http_transport=True, http_path='cliservice')

  # A hook in conftest sets tests.common.current_node. Skip for Hive connections since
  # Hive cannot modify client_identifier at runtime.
  if hasattr(tests.common, "current_node") and not is_hive:
    c.set_configuration_option("client_identifier", tests.common.current_node)
  return c


def create_ldap_connection(host_port, user, password, use_ssl=False):
  return BeeswaxConnection(host_port=host_port, user=user, password=password,
                           use_ssl=use_ssl)
