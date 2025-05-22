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
import getpass
import logging
import math
import re
import time

from future.utils import with_metaclass
import impala.dbapi as impyla
import impala.error as impyla_error
import impala.hiveserver2 as hs2

from impala_thrift_gen.beeswax.BeeswaxService import QueryState
from impala_thrift_gen.RuntimeProfile.ttypes import TRuntimeProfileFormat
from tests.beeswax.impala_beeswax import (
    DEFAULT_SLEEP_INTERVAL,
    ImpalaBeeswaxClient,
    ImpalaBeeswaxException,
)
import tests.common
from tests.common.network import split_host_port
from tests.common.patterns import LOG_FORMAT
from tests.common.test_vector import BEESWAX, HS2, HS2_HTTP
from tests.util.thrift_util import op_handle_to_query_id, session_handle_to_session_id

LOG = logging.getLogger(__name__)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# All logging needs to be either executable SQL or a SQL comment (prefix with --).
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
LOG.addHandler(console_handler)
LOG.propagate = False

# Regular expression that matches the "progress" entry in the HS2 log.
PROGRESS_LOG_RE = re.compile(
    r'^Query [a-z0-9:]+ [0-9]+% Complete \([0-9]+ out of [0-9]+\)$')

MAX_SQL_LOGGING_LENGTH = 128 * 1024

# Tuple of root exception types from different client protocol.
IMPALA_CONNECTION_EXCEPTION = (ImpalaBeeswaxException, impyla_error.Error)

# String representation of ClientRequestState::ExecState
INITIALIZED = 'INITIALIZED'
PENDING = 'PENDING'
RUNNING = 'RUNNING'
FINISHED = 'FINISHED'
ERROR = 'ERROR'
# ExecState that is final.
EXEC_STATES_FINAL = set([FINISHED, ERROR])
# Possible ExecState after query passed admission controller.
EXEC_STATES_ADMITTED = set([RUNNING, FINISHED, ERROR])
# Mapping of a ExecState to a set of possible future ExecState.
LEGAL_FUTURE_STATES = {
  INITIALIZED: set([PENDING, RUNNING, FINISHED, ERROR]),
  PENDING: set([RUNNING, FINISHED, ERROR]),
  RUNNING: set([FINISHED, ERROR]),
  FINISHED: set([ERROR]),
  ERROR: set()
}


def has_legal_future_state(impala_state, future_states):
  """Return True if 'impala_state' can transition to one of state listed in
  'future_states'."""
  assert impala_state in LEGAL_FUTURE_STATES
  expected_impala_states = set(future_states)
  return len(LEGAL_FUTURE_STATES[impala_state] & expected_impala_states) > 0


# test_exprs.py's TestExprLimits executes extremely large SQLs (multiple MBs). It is the
# only test that runs SQL larger than 128KB. Logging these SQLs in execute() increases
# the size of the JUnitXML files, causing problems for users of JUnitXML like Jenkins.
# This function limits the size of the returned string if it is larger than 128KB.
def format_sql_for_logging(sql_stmt):
  """If the 'sql_stmt' is shorter than MAX_SQL_LOGGING_LENGTH, only wrap sql_stmt with
  new lines and semicolon. If it is larger than MAX_SQL_LOGGING_LENGTH, truncate it
  and comment it out. This function returns a unicode string."""
  # sql_stmt could contain Unicode characters, so explicitly use unicode literals
  # so that Python 2 works.
  if (len(sql_stmt) <= MAX_SQL_LOGGING_LENGTH):
    return u"\n{0};\n".format(sql_stmt)
  else:
    # The logging output should be valid SQL, so the truncated SQL is commented out.
    truncated_sql = u'\n--'.join(
      [line for line in sql_stmt[0:MAX_SQL_LOGGING_LENGTH].split("\n")])
    return (u"\n-- Skip logging full SQL statement of length {0}"
            u"\n-- Logging a truncated version, commented out:"
            u"\n-- {1}"
            u"\n-- [...]\n").format(len(sql_stmt), truncated_sql)


def build_summary_table_from_thrift(thrift_exec_summary):
  from impala_shell.exec_summary import build_exec_summary_table
  result = list()
  build_exec_summary_table(thrift_exec_summary, 0, 0, False, result,
                           is_prettyprint=False, separate_prefix_column=True)
  keys = ['prefix', 'operator', 'num_hosts', 'num_instances', 'avg_time', 'max_time',
          'num_rows', 'est_num_rows', 'peak_mem', 'est_peak_mem', 'detail']
  output = list()
  for row in result:
    assert len(keys) == len(row)
    summ_map = dict(zip(keys, row))
    output.append(summ_map)
  return output


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
  def get_test_protocol(self):
    """Return client protocol name that is specific to Impala test framework.
    Possible return value are either of 'beeswax', 'hs2', or 'hs2-http'."""
    pass

  @abc.abstractmethod
  def get_host_port(self):
    """Return the 'host:port' string of impala server that this object connecting to."""
    pass

  @abc.abstractmethod
  def set_configuration_option(self, name, value, is_log_sql=True):
    """Sets a configuration option name to the given value.
    Return True if option is changing. Otherwise, return False (option already has the
    same value). If is_log_sql True, log the equivalent SET query to INFO. Do note though
    that the option change does not actually happen by issuing SET query."""
    pass

  def set_configuration(self, config_option_dict):
    """Replaces existing configuration with the given dictionary.
    If config_option_dict is an empty dictionary, simply clear current client
    configuration."""
    assert isinstance(config_option_dict, dict), \
        "config_option_dict must be a dictionary"
    self.clear_configuration()
    if not config_option_dict:
      return
    log_lines = list()
    for name, value in config_option_dict.items():
      if self.set_configuration_option(name, value, False):
        log_lines.append("set {0}={1};".format(name, value))
    if log_lines:
      self.log_client("set_configuration:\n\n{}\n".format('\n'.join(log_lines)))

  @abc.abstractmethod
  def clear_configuration(self):
    """Clears all existing configuration."""
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
  def close_query(self, handle, fetch_profile_after_close=False):
    """Closes the query."""
    pass

  @abc.abstractmethod
  def get_state(self, operation_handle):
    """Returns the state of a query.
    May raise en error, depending on connection type."""
    pass

  @abc.abstractmethod
  def get_impala_exec_state(self, operation_handle):
    """Returns a string translation from client specific state of operation_handle
    to Impala's ClientRequestState::ExecState."""
    pass

  def __is_at_exec_state(self, operation_handle, impala_state):
    self.log_handle(
      operation_handle, 'checking ' + impala_state + ' state for operation')
    return self.get_impala_exec_state(operation_handle) == impala_state

  def state_is_finished(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is FINISHED.
    DEPRECATED: use is_finished() instead."""
    return self.is_finished(operation_handle)

  def is_initialized(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is INITIALIZED"""
    return self.__is_at_exec_state(operation_handle, INITIALIZED)

  def is_pending(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is PENDING"""
    return self.__is_at_exec_state(operation_handle, PENDING)

  def is_running(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is RUNNING"""
    return self.__is_at_exec_state(operation_handle, RUNNING)

  def is_finished(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is FINISHED"""
    return self.__is_at_exec_state(operation_handle, FINISHED)

  def is_error(self, operation_handle):
    """Returns whether the Impala exec state of a operation_handle is ERROR.
    Internally, it will call get_state(), and any exception thrown by get_state() will
    cause this method to return True."""
    return self.__is_at_exec_state(operation_handle, ERROR)

  def is_executing(self, operation_handle):
    """Returns whether the state of a operation_handle is executing or will be
    executing. Return False if operation_handle has ended, either successful or
    with error."""
    return self.get_impala_exec_state(operation_handle) not in EXEC_STATES_FINAL

  def is_admitted(self, operation_handle):
    """Returns whether the state of a operation_handle has passed Impala
    admission control. Return True if handle state is error."""
    return self.get_impala_exec_state(operation_handle) in EXEC_STATES_ADMITTED

  @abc.abstractmethod
  def get_log(self, operation_handle):
    """Returns the log of an operation as a string, with entries separated by newlines."""
    pass

  @abc.abstractmethod
  def cancel(self, operation_handle):
    """Cancels an in-flight operation"""
    pass

  def execute(self, sql_stmt, user=None, fetch_profile_after_close=False,  # noqa: U100
              fetch_exec_summary=False,  # noqa: U100
              profile_format=TRuntimeProfileFormat.STRING):  # noqa: U100
    """Executes a query and fetches the results"""
    pass

  @abc.abstractmethod
  def execute_async(self, sql_stmt):
    """Issues a query and returns the handle to the caller for processing. Only one
    async operation per connection at a time is supported, due to limitations of the
    Beeswax protocol and the Impyla client."""
    pass

  @abc.abstractmethod
  def fetch(self, sql_stmt, operation_handle, max_rows=-1, discard_results=False):
    """Fetches query results up to max_rows given a handle and sql statement.
    Caller must ensure that query has passed PENDING state before calling fetch.
    If max_rows < 0, all rows are fetched. If max_rows > 0 but the number of
    rows returned is less than max_rows, all the rows have been fetched.
    Return None if discard_results is True.
    TODO: 'sql_stmt' can be obtained from 'operation_handle'."""
    pass

  @abc.abstractmethod
  def get_runtime_profile(self, operation_handle,
                          profile_format=TRuntimeProfileFormat.STRING):
    """Get runtime profile of given 'operation_handle'.
    Handle must stay open."""
    pass

  @abc.abstractmethod
  def handle_id(self, operation_handle):
    """Return a string id for given operation_handle.
    Most implementations will return an Impala query id for given handle.
    Otherwise, return str(operation_handle)."""
    pass

  def log_handle(self, operation_handle, message):
    """Log 'message' at INFO level, along with id of 'operation_handle'."""
    handle_id = self.handle_id(operation_handle)
    LOG.info(u"{0}: {1}".format(handle_id, message))

  def log_client(self, message):
    """Log 'message' at INFO level, prefixed wih the protocol name of this connection."""
    LOG.info(u"{0}: {1}".format(self.get_test_protocol(), message))

  def wait_for_impala_state(self, operation_handle, expected_impala_state, timeout):
    """Waits for the given 'operation_handle' to reach the 'expected_impala_state'.
    'expected_impala_state' must be a string of either 'INITIALIZED', 'PENDING',
    'RUNNING', 'FINISHED', or 'ERROR'. If it does not reach the given state within
    'timeout' seconds, the method throws an AssertionError.
    """
    self.wait_for_any_impala_state(operation_handle, [expected_impala_state], timeout)

  def wait_for_any_impala_state(self, operation_handle, expected_impala_states,
                                timeout_s):
    """Waits for the given 'operation_handle' to reach one of 'expected_impala_states'.
    Each string in 'expected_impala_states' must either be 'INITIALIZED', 'PENDING',
    'RUNNING', 'FINISHED', or 'ERROR'. If it does not reach one of the given states
    within 'timeout' seconds, the method throws an AssertionError.
    Returns the final state.
    """
    start_time = time.time()
    timeout_msg = None
    while True:
      impala_state = self.get_impala_exec_state(operation_handle)
      interval = time.time() - start_time
      if impala_state in expected_impala_states:
        # Reached one of expected_impala_states.
        break
      elif not has_legal_future_state(impala_state, expected_impala_states):
        timeout_msg = ("query '{0}' can not transition from last known state '{1}' to "
                       "any of the expected states {2}. Stop waiting after {3} "
                       "seconds.").format(
          self.handle_id(operation_handle), impala_state, expected_impala_states,
          interval)
        break
      elif interval >= timeout_s:
        timeout_msg = ("query '{0}' did not reach one of the expected states {1}, last "
                       "known state {2}").format(
          self.handle_id(operation_handle), expected_impala_states, impala_state)
        break
      time.sleep(DEFAULT_SLEEP_INTERVAL)

    if timeout_msg is not None:
      raise tests.common.errors.Timeout(timeout_msg)
    return impala_state

  @abc.abstractmethod
  def wait_for_admission_control(self, operation_handle, timeout_s=60):
    """Given an 'operation_handle', polls the coordinator waiting for it to complete
    admission control processing of the query.
    Return True if query pass admission control after given 'timeout_s'."""
    pass

  @abc.abstractmethod
  def get_admission_result(self, operation_handle):
    """Given an 'operation_handle', returns the admission result from the query
    profile"""
    pass

  @abc.abstractmethod
  def get_exec_summary(self, operation_handle):  # noqa: U100
    pass

  def get_exec_summary_table(self, operation_handle):
    summary_table = list()
    summary = self.get_exec_summary(operation_handle)
    if summary:
        summary_table = build_summary_table_from_thrift(summary)
    return summary_table


# Represents a connection to Impala using the Beeswax API.
class BeeswaxConnection(ImpalaConnection):

  # This is based on ClientRequestState::BeeswaxQueryState().
  __QUERY_STATE_TO_EXEC_STATE = {
    QueryState.CREATED: INITIALIZED,
    QueryState.COMPILED: PENDING,
    QueryState.RUNNING: RUNNING,
    QueryState.FINISHED: FINISHED,
    QueryState.EXCEPTION: ERROR,
    # These are not official ExecState, but added to complete mapping.
    QueryState.INITIALIZED: 'UNIMPLEMENTED_INITIALIZED',
  }

  def __init__(self, host_port, use_kerberos=False, user=None, password=None,
               use_ssl=False):
    self.__beeswax_client = ImpalaBeeswaxClient(host_port, use_kerberos, user=user,
                                                password=password, use_ssl=use_ssl)
    self.__host_port = host_port
    self.QUERY_STATES = self.__beeswax_client.query_states

  def get_test_protocol(self):
    return BEESWAX

  def get_host_port(self):
    return self.__host_port

  def set_configuration_option(self, name, value, is_log_sql=True):
    # Only set the option if it's not already set to the same value.
    name = name.lower()
    value = str(value)
    if self.__beeswax_client.get_query_option(name) != value:
      self.__beeswax_client.set_query_option(name, value)
      if is_log_sql:
        self.log_client("\n\nset {0}={1};\n".format(name, value))
      return True
    return False

  def clear_configuration(self):
    self.__beeswax_client.clear_query_options()
    # A hook in conftest sets tests.common.current_node.
    if hasattr(tests.common, "current_node"):
      self.set_configuration_option("client_identifier", tests.common.current_node)

  def connect(self):
    try:
      self.__beeswax_client.connect()
      self.log_client("connected to %s with beeswax" % self.__host_port)
    except Exception as e:
      self.log_client("failed connecting to %s with beeswax" % self.__host_port)
      raise e

  # TODO: rename to close_connection
  def close(self):
    self.log_client("closing beeswax connection to: %s" % self.__host_port)
    self.__beeswax_client.close_connection()

  def close_query(self, operation_handle, fetch_profile_after_close=False):
    self.log_handle(operation_handle, 'closing query for operation')
    return self.__beeswax_client.close_query(operation_handle.get_handle(),
                                             fetch_profile_after_close)

  def close_dml(self, operation_handle):
    self.log_handle(operation_handle, 'closing DML query')
    self.__beeswax_client.close_dml(operation_handle.get_handle())

  def execute(self, sql_stmt, user=None, fetch_profile_after_close=False,
              fetch_exec_summary=False, profile_format=TRuntimeProfileFormat.STRING):
    assert profile_format == TRuntimeProfileFormat.STRING, (
      "Beeswax client only supports getting runtime profile in STRING format.")
    self.log_client(u"executing against {0}\n{1}".format(
      self.__host_port, format_sql_for_logging(sql_stmt)))
    return self.__beeswax_client.execute(sql_stmt, user=user,
        fetch_profile_after_close=fetch_profile_after_close,
        fetch_exec_summary=fetch_exec_summary)

  def execute_async(self, sql_stmt, user=None):
    self.log_client(u"executing async {0}\n{1}".format(
      self.__host_port, format_sql_for_logging(sql_stmt)))
    beeswax_handle = self.__beeswax_client.execute_query_async(sql_stmt, user=user)
    return OperationHandle(beeswax_handle, sql_stmt)

  def cancel(self, operation_handle):
    self.log_handle(operation_handle, 'canceling operation')
    return self.__beeswax_client.cancel_query(operation_handle.get_handle())

  def get_state(self, operation_handle):
    self.log_handle(operation_handle, 'getting state')
    return self.__beeswax_client.get_state(operation_handle.get_handle())

  def get_impala_exec_state(self, operation_handle):
    return self.__QUERY_STATE_TO_EXEC_STATE[self.get_state(operation_handle)]

  def get_exec_summary(self, operation_handle):
    self.log_handle(operation_handle, 'getting exec summary operation')
    return self.__beeswax_client.get_exec_summary(operation_handle.get_handle())

  def get_runtime_profile(self, operation_handle,
                          profile_format=TRuntimeProfileFormat.STRING):
    assert profile_format == TRuntimeProfileFormat.STRING, (
      "Beeswax client only supports getting runtime profile in STRING format.")
    self.log_handle(operation_handle, 'getting runtime profile operation')
    return self.__beeswax_client.get_runtime_profile(operation_handle.get_handle())

  def wait_for_finished_timeout(self, operation_handle, timeout):
    self.log_handle(operation_handle, 'waiting for query to reach FINISHED state')
    return self.__beeswax_client.wait_for_finished_timeout(
      operation_handle.get_handle(), timeout)

  def wait_for_admission_control(self, operation_handle, timeout_s=60):
    self.log_handle(operation_handle, 'waiting for completion of the admission control')
    return self.__beeswax_client.wait_for_admission_control(
      operation_handle.get_handle(), timeout_s=timeout_s)

  def get_admission_result(self, operation_handle):
    self.log_handle(operation_handle, 'getting the admission result')
    return self.__beeswax_client.get_admission_result(operation_handle.get_handle())

  def get_log(self, operation_handle):
    self.log_handle(operation_handle, 'getting log for operation')
    return self.__beeswax_client.get_log(operation_handle.get_handle().log_context)

  def fetch(self, sql_stmt, operation_handle, max_rows=-1, discard_results=False):
    self.log_handle(operation_handle, 'fetching {} rows'.format(
      'all' if max_rows < 0 else max_rows))
    return self.__beeswax_client.fetch_results(
        sql_stmt, operation_handle.get_handle(), max_rows, discard_results)

  def handle_id(self, operation_handle):
    query_id = operation_handle.get_handle().id
    return query_id if query_id else str(operation_handle)

  def get_query_id(self, operation_handle):
    return operation_handle.get_handle().id


class ImpylaHS2Connection(ImpalaConnection):
  """Connection to Impala using the impyla client connecting to HS2 endpoint.
  impyla implements the standard Python dbabi: https://www.python.org/dev/peps/pep-0249/
  plus Impala-specific extensions, e.g. for fetching runtime profiles.
  TODO: implement support for kerberos, SSL, etc.
  """

  # ClientRequestState::TOperationState()
  OPERATION_STATE_TO_EXEC_STATE = {
    'INITIALIZED_STATE': INITIALIZED,
    'PENDING_STATE': PENDING,
    'RUNNING_STATE': RUNNING,
    'FINISHED_STATE': FINISHED,
    'ERROR_STATE': ERROR,
    # These are not official ExecState, but added to complete mapping.
    'CANCELED_STATE': 'UNIMPLEMENTED_CANCELLED',
    'CLOSED_STATE': 'UNIMPLEMENTED_CLOSED',
    'UKNOWN_STATE': 'UNIMPLEMENTED_UNKNOWN'
  }

  def __init__(self, host_port, use_kerberos=False, is_hive=False,
               use_http_transport=False, http_path="", use_ssl=False,
               collect_profile_and_log=True, user=None):
    self.__host_port = host_port
    self.__use_http_transport = use_http_transport
    self.__http_path = http_path
    self.__use_ssl = use_ssl
    if use_kerberos:
      raise NotImplementedError("Kerberos support not yet implemented")
    # Impyla connection and cursor is initialised in connect(). We need to reuse the same
    # cursor for different operations (as opposed to creating a new cursor per operation)
    # so that the session is preserved. This means that we can only execute one operation
    # at a time per connection, which is a limitation also imposed by the Beeswax API.
    # However, for ease of async query testing, opening multiple cursors through single
    # ImpylaHS2Connection is allowed if executing query through execute_async() or
    # execute() with user parameter that is different than self.__user. Do note though
    # that they will not share the same session with self.__cursor.
    self.__impyla_conn = None
    self.__cursor = None
    # List of all cursors that created through execute_async.
    self.__async_cursors = list()
    # Query options to send along with each query.
    self.__query_options = {}
    self._is_hive = is_hive
    # Some Hive HS2 protocol, such as custom Calcite planner, may be able to collect
    # profile and log from Impala.
    self._collect_profile_and_log = collect_profile_and_log
    self.__user = user

  def get_test_protocol(self):
    if self.__http_path:
      return HS2_HTTP
    else:
      return HS2

  def get_host_port(self):
    return self.__host_port

  def set_configuration_option(self, name, value, is_log_sql=True):
    # Only set the option if it's not already set to the same value.
    # value must be parsed to string.
    name = name.lower()
    value = str(value)
    if self.__query_options.get(name, "") != value:
      self.__query_options[name] = value
      if is_log_sql:
        self.log_client("\n\nset {0}={1};\n".format(name, value))
      return True
    return False

  def clear_configuration(self):
    self.__query_options.clear()
    if hasattr(tests.common, "current_node") and not self._is_hive:
      self.set_configuration_option("client_identifier", tests.common.current_node)

  def __open_single_cursor(self, user=None):
    return self.__impyla_conn.cursor(user=user, convert_types=False,
                                     close_finished_queries=False)

  def __close_single_cursor(self, cursor):
    try:
      # Explicitly close the cursor so that it will close the session.
      cursor.close()
    except Exception:
      # The session may no longer be valid if the impalad was restarted during the test.
      pass

  def default_cursor(self):
    if self.__cursor is None:
      self.__cursor = self.__open_single_cursor(user=self.__user)
    return self.__cursor

  def connect(self):
    host, port = split_host_port(self.__host_port)
    conn_kwargs = {}
    if self._is_hive:
      conn_kwargs['auth_mechanism'] = 'PLAIN'
    try:
      self.__impyla_conn = impyla.connect(
        host=host, port=port, use_http_transport=self.__use_http_transport,
        http_path=self.__http_path, use_ssl=self.__use_ssl, **conn_kwargs)
      self.log_client("connected to {0} with impyla {1}".format(
        self.__host_port, self.get_test_protocol()))
    except Exception as e:
      self.log_client("failed connecting to {0} with impyla {1}".format(
        self.__host_port, self.get_test_protocol()
      ))
      raise e

  def close(self):
    self.log_client("closing 1 sync and {0} async {1} connections to: {2}".format(
      len(self.__async_cursors), self.get_test_protocol(), self.__host_port))
    if self.__cursor is not None:
      self.__close_single_cursor(self.__cursor)
    for async_cursor in self.__async_cursors:
      self.__close_single_cursor(async_cursor)
    # Remove all async cursors.
    self.__async_cursors = list()
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
    self.log_client("getting tables for database: {0}".format(database))
    self.default_cursor().get_tables(database_name=database)
    return self.default_cursor().fetchall()

  def close_query(self, operation_handle, fetch_profile_after_close=False):
    self.log_handle(operation_handle, 'closing query for operation')
    # close_operation() will wipe out _last_operation.
    # Assign it to op_handle so that we can pull the profile after close_operation().
    op_handle = operation_handle.get_handle()._last_operation
    operation_handle.get_handle().close_operation()
    if fetch_profile_after_close:
      assert self._collect_profile_and_log, (
        "This connection is not configured to collect profile.")
      return op_handle.get_profile(TRuntimeProfileFormat.STRING)
    return None

  def __log_execute(self, cursor, user, sql_stmt):
    self.log_client(
      (u"executing against {0} at {1}. session: {2} main_cursor: {3} "
       u"user: {4}\n{5}").format(
         (self._is_hive and 'Hive' or 'Impala'), self.__host_port,
         self.__get_session_id(cursor), (cursor == self.default_cursor()), user,
         format_sql_for_logging(sql_stmt))
    )

  def execute(self, sql_stmt, user=None, fetch_profile_after_close=False,
              fetch_exec_summary=False, profile_format=TRuntimeProfileFormat.STRING):
    if user is None:
      user = self.__user
    same_user = (user == self.__user)
    cursor = (self.default_cursor() if same_user
              # Must create a new cursor to supply 'user'.
              else self.__open_single_cursor(user=user))
    result = None
    try:
      self.__log_execute(cursor, user, sql_stmt)
      cursor.execute(sql_stmt, configuration=self.__query_options)
      handle = OperationHandle(cursor, sql_stmt)
      self.log_handle(handle, "query started")
      result = self.__fetch_results_and_profile(
        handle, fetch_profile_after_close=fetch_profile_after_close,
        fetch_exec_summary=fetch_exec_summary, profile_format=profile_format)
    finally:
      cursor.close_operation()
      if not same_user:
        self.__close_single_cursor(cursor)
    return result

  def __fetch_results_and_profile(
      self, operation_handle, fetch_profile_after_close=False,
      fetch_exec_summary=False, profile_format=TRuntimeProfileFormat.STRING):
    r = None
    try:
      r = self.__fetch_results(operation_handle, fetch_exec_summary=fetch_exec_summary,
                               profile_format=profile_format)
    finally:
      if r is None:
        # Try to close the query handle but ignore any exceptions not to replace the
        # original exception raised by '__fetch_results'.
        try:
          self.close_query(operation_handle)
        except Exception:
          pass
      elif fetch_profile_after_close:
        # Match ImpalaBeeswaxResult by placing the full profile including end time and
        # duration into the return object.
        r.runtime_profile = self.close_query(operation_handle, fetch_profile_after_close)
        return r
      else:
        self.close_query(operation_handle)
        return r

  def execute_async(self, sql_stmt, user=None):
    async_cursor = None
    if user is None:
      user = self.__user
    try:
      async_cursor = self.__open_single_cursor(user=user)
      handle = OperationHandle(async_cursor, sql_stmt)
      self.__log_execute(async_cursor, user, sql_stmt)
      async_cursor.execute_async(sql_stmt, configuration=self.__query_options)
      self.__async_cursors.append(async_cursor)
      return handle
    except Exception as e:
      if async_cursor:
        async_cursor.close_operation()
        self.__close_single_cursor(async_cursor)
      raise e

  def cancel(self, operation_handle):
    self.log_handle(operation_handle, 'canceling operation')
    cursor = operation_handle.get_handle()
    return cursor.cancel_operation(reset_state=False)

  def get_query_id(self, operation_handle):
    """Return the string representation of the query id.
    Return empty string if handle is already canceled or closed."""
    id = None
    last_op = operation_handle.get_handle()._last_operation
    if last_op is not None:
      id = op_handle_to_query_id(last_op.handle)
    return "" if id is None else id

  def __get_session_id(self, cursor):
    """Return the string representation of the session id.
    Return empty string if handle is already canceled or closed."""
    id = None
    if cursor.session is not None:
      id = session_handle_to_session_id(cursor.session.handle)
    return "" if id is None else id

  def session_id(self, operation_handle):
    cursor = operation_handle.get_handle()
    session_id = self.__get_session_id(cursor)
    return session_id if session_id else str(cursor.session)

  def handle_id(self, operation_handle):
    query_id = self.get_query_id(operation_handle)
    return query_id if query_id else str(operation_handle)

  def get_state(self, operation_handle):
    self.log_handle(operation_handle, 'getting state')
    cursor = operation_handle.get_handle()
    # cursor.status contains a string representation of one of
    # TCLIService.TOperationState.
    return cursor.status()

  def get_impala_exec_state(self, operation_handle):
    try:
      return self.OPERATION_STATE_TO_EXEC_STATE[self.get_state(operation_handle)]
    except impyla_error.Error:
      return ERROR
    except Exception as e:
      raise e

  def get_exec_summary(self, operation_handle):
    self.log_handle(operation_handle, 'getting exec summary operation')
    cursor = operation_handle.get_handle()
    # summary returned is thrift, not string.
    return cursor.get_summary()

  def get_runtime_profile(self, operation_handle,
                          profile_format=TRuntimeProfileFormat.STRING):
    self.log_handle(operation_handle, 'getting runtime profile operation')
    cursor = operation_handle.get_handle()
    return cursor.get_profile(profile_format=profile_format)

  def wait_for_finished_timeout(self, operation_handle, timeout):
    self.log_handle(operation_handle, 'waiting for query to reach FINISHED state')
    start_time = time.time()
    while time.time() - start_time < timeout:
      start_rpc_time = time.time()
      impala_state = self.get_impala_exec_state(operation_handle)
      rpc_time = time.time() - start_rpc_time
      # if the rpc succeeded, the output is the query state
      if impala_state == FINISHED:
        return True
      elif impala_state == ERROR:
        try:
          error_log = operation_handle.get_handle().get_log()
          raise impyla_error.OperationalError(error_log, None)
        finally:
          self.close_query(operation_handle)
      if rpc_time < DEFAULT_SLEEP_INTERVAL:
        time.sleep(DEFAULT_SLEEP_INTERVAL - rpc_time)
    return False

  def wait_for_admission_control(self, operation_handle, timeout_s=60):
    self.log_handle(operation_handle, 'waiting for completion of the admission control')
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      start_rpc_time = time.time()
      if self.is_admitted(operation_handle):
        return True
      rpc_time = time.time() - start_rpc_time
      if rpc_time < DEFAULT_SLEEP_INTERVAL:
        time.sleep(DEFAULT_SLEEP_INTERVAL - rpc_time)
    return False

  def get_admission_result(self, operation_handle):
    self.log_handle(operation_handle, 'getting the admission result')
    if self.is_admitted(operation_handle):
      query_profile = self.get_runtime_profile(operation_handle)
      admit_result = re.search(r"Admission result: (.*)", query_profile)
      if admit_result:
        return admit_result.group(1)
    return ""

  def get_log(self, operation_handle):
    self.log_handle(operation_handle, 'getting log for operation')
    # HS2 includes non-error log messages that we need to filter out.
    cursor = operation_handle.get_handle()
    lines = [line for line in cursor.get_log().split('\n')
             if not PROGRESS_LOG_RE.match(line)]
    return '\n'.join(lines)

  def fetch(self, sql_stmt, operation_handle, max_rows=-1, discard_results=False):
    self.log_handle(operation_handle, 'fetching {} rows'.format(
      'all' if max_rows < 0 else max_rows))
    return self.__fetch_results(operation_handle, max_rows, discard_results)

  def __fetch_results(self, handle, max_rows=-1,
                      discard_results=False,
                      fetch_exec_summary=False,
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

    result = None
    if discard_results:
      return result

    log = None
    profile = None
    exec_summary = None
    if not self._is_hive:
      if fetch_exec_summary:
        exec_summary = self.get_exec_summary_table(handle)
      if self._collect_profile_and_log:
        log = self.get_log(handle)
        profile = self.get_runtime_profile(handle, profile_format=profile_format)

    result = ImpylaHS2ResultSet(success=True, result_tuples=result_tuples,
                                column_labels=column_labels, column_types=column_types,
                                query=handle.sql_stmt(), log=log, profile=profile,
                                query_id=self.get_query_id(handle),
                                exec_summary=exec_summary)
    return result


class ImpylaHS2ResultSet(object):
  """This emulates the interface of ImpalaBeeswaxResult so that it can be used in
  place of it. TODO: when we deprecate/remove Beeswax, clean this up."""
  def __init__(self, success, result_tuples, column_labels, column_types, query, log,
      profile, query_id, exec_summary):
    self.success = success
    self.column_labels = column_labels
    self.column_types = column_types
    self.query = query
    self.log = log
    # ImpalaBeeswaxResult store profile at runtime_profile field
    self.runtime_profile = profile
    self.query_id = query_id
    self.__result_tuples = result_tuples
    # self.data is the data in the ImpalaBeeswaxResult format: a list of rows with each
    # row represented as a tab-separated string.
    self.data = None
    if result_tuples is not None:
      self.data = [self.__convert_result_row(tuple) for tuple in result_tuples]
    self.exec_summary = exec_summary

  def tuples(self):
    """Return the raw HS2 result set, which is a list of tuples."""
    return self.__result_tuples

  def get_data(self):
    if self.data:
      return '\n'.join(self.data)
    return ''

  def __convert_result_row(self, result_tuple):
    """Take primitive values from a result tuple and construct the tab-separated string
    that would have been returned via beeswax."""
    row = list()
    for idx, val in enumerate(result_tuple):
      row.append(self.__convert_result_value(val, self.column_types[idx]))
    return '\t'.join(row)

  def __convert_result_value(self, val, col_type):
    """Take a primitive value from a result tuple and its type and construct the string
    that would have been returned via beeswax."""
    if val is None:
      return 'NULL'
    if type(val) == float:
      # Same format as what Beeswax uses in the backend.
      if math.isnan(val):
        return 'NaN'
      elif math.isinf(val):
        if val < 0:
          return '-Infinity'
        else:
          return 'Infinity'
      else:
        return "{:.16g}".format(val)
    elif col_type == 'BOOLEAN':
      # Beeswax return 'false' or 'true' for boolean column.
      # HS2 return 'False' or 'True'.
      return str(val).lower()
    elif col_type == 'BINARY':
      # With Python 3, binary columns are represented as bytes. The default string
      # representation of bytes has an extra b'...' surrounding the actual value.
      # To avoid that, this decodes the bytes to a regular string. Since binary values
      # could be invalid Unicode, this uses 'backslashreplace' to avoid throwing an
      # error.
      return val.decode(errors='backslashreplace')
    else:
      return str(val)


def create_connection(host_port, use_kerberos=False, protocol=BEESWAX,
    is_hive=False, use_ssl=False, collect_profile_and_log=True, user=None):
  if protocol == BEESWAX:
    c = BeeswaxConnection(host_port=host_port, use_kerberos=use_kerberos,
                          user=user, use_ssl=use_ssl)
  elif protocol == HS2:
    c = ImpylaHS2Connection(host_port=host_port, use_kerberos=use_kerberos,
        is_hive=is_hive, use_ssl=use_ssl,
        collect_profile_and_log=collect_profile_and_log, user=user)
  else:
    assert protocol == HS2_HTTP
    c = ImpylaHS2Connection(host_port=host_port, use_kerberos=use_kerberos,
        is_hive=is_hive, use_http_transport=True, http_path='cliservice',
        use_ssl=use_ssl, collect_profile_and_log=collect_profile_and_log,
        user=user)

  # A hook in conftest sets tests.common.current_node. Skip for Hive connections since
  # Hive cannot modify client_identifier at runtime.
  if hasattr(tests.common, "current_node") and not is_hive:
    c.set_configuration_option("client_identifier", tests.common.current_node)
  return c


def create_ldap_connection(host_port, user, password, use_ssl=False):
  return BeeswaxConnection(host_port=host_port, user=user, password=password,
                           use_ssl=use_ssl)


class MinimalHS2OperationHandle(OperationHandle):
  def __str__(self):
    return op_handle_to_query_id(self.get_handle())


class MinimalHS2Connection(ImpalaConnection):
  """
  Connection to Impala using the HiveServer2 (HS2) protocol.

  This class does not use Impyla's DB-API cursors. Instead, it is built directly on the
  HS2 RPC layer to support manipulating one operation from multiple connections
  concurrently.

  This class is designed to be minimalistic to facilitate testing. Each method is mapped
  to only one Thrift RPC.
  """
  def __init__(self, host_port, user=None):
    self.__host_port = host_port
    host, port = host_port.split(":")
    self.__conn = hs2.connect(host, port, auth_mechanism='NOSASL')
    self.__user = user if user is not None else getpass.getuser()
    self.__session = self.__conn.open_session(self.__user)
    self.__query_options = dict()

  def connect(self):
    pass  # Do nothing

  def close(self):
    self.log_client("closing connection to: %s" % self.__host_port)
    try:
      self.__session.close()
    finally:
      self.__conn.close()

  def __log_execute(self, sql_stmt):
    session_id = session_handle_to_session_id(self.__session.handle)
    self.log_client(
      u"executing at {0}. session: {1} user: {2}\n{3}".format(
        self.__host_port, session_id, self.__user, format_sql_for_logging(sql_stmt))
    )

  def log_client(self, message):
    """Log 'message' at INFO level, prefixed wih the protocol name of this connection."""
    LOG.info(u"minimal_{0}: {1}".format(self.get_test_protocol(), message))

  def execute(self, sql_stmt, user=None, fetch_profile_after_close=False,  # noqa: U100
              fetch_exec_summary=False,  # noqa: U100
              profile_format=TRuntimeProfileFormat.STRING):  # noqa: U100
    raise NotImplementedError()

  def execute_async(self, sql_stmt):
    self.__log_execute(sql_stmt)
    hs2_operation = self.__session.execute(sql_stmt, configuration=self.__query_options)
    operation_handle = MinimalHS2OperationHandle(hs2_operation.handle, sql_stmt)
    self.log_handle(operation_handle, "query started")
    return operation_handle

  def __get_operation(self, operation_handle):
    return hs2.Operation(self.__session, operation_handle.get_handle())

  def fetch(self, sql_stmt, operation_handle, max_rows=-1):  # noqa: U100
    """
    Fetch the results of the query. It will block the current connection if the results
    are not available yet.
    """
    self.log_handle(operation_handle, "fetching results")
    return self.__get_operation(operation_handle).fetch(max_rows=max_rows)

  def fetch_error(self, operation_handle):
    """
    Fetch the error of the query.
    """
    try:
      self.fetch(None, operation_handle)
      assert False, "Failed to catch the error of the query."
    except Exception as exc:
      return exc

  def get_state(self, operation_handle):
    return self.__get_operation(operation_handle).get_status()

  def wait_for(self, operation_handle, timeout_s=60):
    """
    Wait until the query is in a terminal state.
    """
    start_time = time.time()
    while True:
      operation_state = self.get_state(operation_handle)
      if operation_state not in ("PENDING_STATE", "INITIALIZED_STATE", "RUNNING_STATE"):
        return operation_state
      if time.time() - start_time > timeout_s:
        raise Exception("Timed out waiting for the query")
      time.sleep(0.1)

  def cancel(self, operation_handle):
    self.log_handle(operation_handle, "canceling operation")
    return self.__get_operation(operation_handle).cancel()

  def close_query(self, operation_handle):
    self.log_handle(operation_handle, "closing query for operation")
    return self.__get_operation(operation_handle).close()

  def state_is_finished(self, operation_handle):  # noqa: U100
    raise NotImplementedError()

  def get_log(self, operation_handle):
    self.log_handle(operation_handle, 'getting log for operation')
    # HS2 includes non-error log messages that we need to filter out.
    cursor = operation_handle.get_handle()
    lines = [line for line in cursor.get_log().split('\n')
             if not PROGRESS_LOG_RE.match(line)]
    return '\n'.join(lines)

  def set_configuration_option(self, name, value, is_log_sql=True):
    # Only set the option if it's not already set to the same value.
    # value must be parsed to string.
    name = name.lower()
    value = str(value)
    if self.__query_options.get(name, "") != value:
      self.__query_options[name] = value
      if is_log_sql:
        self.log_client("\n\nset {0}={1};\n".format(name, value))
      return True
    return False

  def clear_configuration(self):
    self.__query_options.clear()
    if hasattr(tests.common, "current_node"):
      self.set_configuration_option("client_identifier", tests.common.current_node)

  def get_host_port(self):
    return self.__host_port

  def get_test_protocol(self):
    return HS2

  def handle_id(self, operation_handle):
    return str(operation_handle)

  def get_admission_result(self, operation_handle):  # noqa: U100
    raise NotImplementedError()

  def get_impala_exec_state(self, operation_handle):
    try:
      return ImpylaHS2Connection.OPERATION_STATE_TO_EXEC_STATE[
        self.get_state(operation_handle)]
    except impyla_error.Error:
      return ERROR
    except Exception as e:
      raise e

  def get_runtime_profile(self, operation_handle,
                          profile_format=TRuntimeProfileFormat.STRING):
    return self.__get_operation(operation_handle).get_profile(profile_format)

  def wait_for_admission_control(self, operation_handle, timeout_s=60):
    self.log_handle(operation_handle, 'waiting for completion of the admission control')
    start_time = time.time()
    while time.time() - start_time < timeout_s:
      start_rpc_time = time.time()
      if self.is_admitted(operation_handle):
        return True
      rpc_time = time.time() - start_rpc_time
      if rpc_time < DEFAULT_SLEEP_INTERVAL:
        time.sleep(DEFAULT_SLEEP_INTERVAL - rpc_time)
    return False

  def get_exec_summary(self, operation_handle):  # noqa: U100
    raise NotImplementedError()

  def wait_for_finished_timeout(self, operation_handle, timeout):
    start_time = time.time()
    while time.time() - start_time < timeout:
      start_rpc_time = time.time()
      hs2_state = self.get_state(operation_handle)
      rpc_time = time.time() - start_rpc_time
      # if the rpc succeeded, the output is the query state
      if hs2_state == "FINISHED_STATE":
        return True
      elif hs2_state == "ERROR_STATE":
        break
      if rpc_time < DEFAULT_SLEEP_INTERVAL:
        time.sleep(DEFAULT_SLEEP_INTERVAL - rpc_time)
    return False
