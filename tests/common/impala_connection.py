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
# Common for connections to Impala. Currently supports Beeswax connections and
# in the future will support HS2 connections. Provides tracing around all
# operations.

from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient, ImpalaBeeswaxResult
from thrift.transport.TSocket import TSocket
from thrift.protocol import TBinaryProtocol
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from getpass import getuser

import abc
import logging
import os

LOG = logging.getLogger('impala_connection')
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
# All logging needs to be either executable SQL or a SQL comment (prefix with --).
console_handler.setFormatter(logging.Formatter('%(message)s'))
LOG.addHandler(console_handler)
LOG.propagate = False

# Common wrapper around the internal types of HS2/Beeswax operation/query handles.
class OperationHandle(object):
  def __init__(self, handle):
    self.__handle = handle

  def get_handle(self): return self.__handle


# Represents an Impala connection.
class ImpalaConnection(object):
  __metaclass__ = abc.ABCMeta

  @abc.abstractmethod
  def set_configuration_option(self, name, value):
    """Sets a configuraiton option name to the given value"""
    pass

  @abc.abstractmethod
  def get_configuration(self):
    """Returns the configuration (a dictionary of key-value pairs) for this connection"""
    pass

  @abc.abstractmethod
  def set_configuration(self, configuration_option_dict):
    """Replaces existing configuration with the given dictionary"""
    pass

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
  def close_query(self, handle):
    """Closes the query."""
    pass

  @abc.abstractmethod
  def get_state(self, operation_handle):
    """Returns the state of a query"""
    pass

  @abc.abstractmethod
  def get_log(self, operation_handle):
    """Returns the log of an operation"""
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
    """Issues a query and returns the handle to the caller for processing"""
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

  def get_configuration(self):
    return self.__beeswax_client.get_query_options()

  def get_default_configuration(self):
    return self.__beeswax_client.get_default_configuration()

  def set_configuration(self, config_option_dict):
    assert config_option_dict is not None, "config_option_dict cannot be None"
    self.clear_configuration()
    for name, value in config_option_dict.iteritems():
      self.set_configuration_option(name, value)

  def clear_configuration(self):
    self.__beeswax_client.clear_query_options()

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

  def execute(self, sql_stmt, user=None):
    LOG.info("-- executing against %s\n%s;\n" % (self.__host_port, sql_stmt))
    return self.__beeswax_client.execute(sql_stmt, user=user)

  def execute_async(self, sql_stmt, user=None):
    LOG.info("-- executing async: %s\n%s;\n" % (self.__host_port, sql_stmt))
    return OperationHandle(self.__beeswax_client.execute_query_async(sql_stmt, user=user))

  def cancel(self, operation_handle):
    LOG.info("-- canceling operation: %s" % operation_handle)
    return self.__beeswax_client.cancel_query(operation_handle.get_handle())

  def get_state(self, operation_handle):
    LOG.info("-- getting state for operation: %s" % operation_handle)
    return self.__beeswax_client.get_state(operation_handle.get_handle())

  def get_runtime_profile(self, operation_handle):
    LOG.info("-- getting runtime profile operation: %s" % operation_handle)
    return self.__beeswax_client.get_runtime_profile(operation_handle.get_handle())

  def get_log(self, operation_handle):
    LOG.info("-- getting log for operation: %s" % operation_handle)
    return self.__beeswax_client.get_log(operation_handle.get_handle())

  def refresh(self):
    """Invalidate the Impalad catalog"""
    return self.execute("invalidate metadata")

  def invalidate_table(self, table_name):
    """Invalidate a specific table from the catalog"""
    return self.execute("invalidate metadata %s" % (table_name))

  def refresh_table(self, db_name, table_name):
    """Refresh a specific table from the catalog"""
    return self.execute("refresh %s.%s" % (db_name, table_name))

  def fetch(self, sql_stmt, operation_handle, max_rows = -1):
    LOG.info("-- fetching results from: %s" % operation_handle)
    return self.__beeswax_client.fetch_results(
        sql_stmt, operation_handle.get_handle(), max_rows)

def create_connection(host_port, use_kerberos=False):
  # TODO: Support HS2 connections.
  return BeeswaxConnection(host_port=host_port, use_kerberos=use_kerberos)

def create_ldap_connection(host_port, user, password, use_ssl=False):
  return BeeswaxConnection(host_port=host_port, user=user, password=password,
                           use_ssl=use_ssl)
