#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Talk to an impalad through beeswax.
# Usage:
#   * impalad is a string with the host and port of the impalad
#     with which the connection should be established.
#     The format is "<hostname>:<port>"
#   * query_string is the query to be executed, as a string.
#   client = ImpalaBeeswaxClient(impalad)
#   client.connect()
#   result = client.execute(query_string)
#   where result is an object of the class QueryResult.
import time
import sys
import shlex
import traceback

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from JavaConstants.constants import DEFAULT_QUERY_OPTIONS
from ImpalaService import ImpalaService
from ImpalaService.ImpalaService import TImpalaQueryOptions
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException

# Custom exception wrapper.
# All exceptions coming from thrift/beeswax etc. go through this wrapper.
# __str__ preserves the exception type.
# TODO: Add the ability to print some of the stack.
class ImpalaBeeswaxException(Exception):
  def __init__(self, message, inner_exception):
    self.__message = message
    if inner_exception is not None:
      self.inner_exception = inner_exception

  def __str__(self):
    return "%s: %s" % (self.__class__, self.__message)

# Encapsulates a typical query result.
class QueryResult(object):
  def __init__(self, success=False, data=None, time_taken=0, summary=''):
    self.success = success
    # Insert returns an int, convert into list to have a uniform data type.
    # TODO: We shold revisit this if we have more datatypes to deal with.
    self.data = data
    if not isinstance(self.data, list):
      self.data = str(self.data)
      self.data = [self.data]
    self.time_taken = time_taken
    self.summary = summary

  def get_data(self):
    return self.__format_data()

  def __format_data(self):
    if self.data:
      return '\n'.join(self.data)
    return ''

  def __str__(self):
    message = ('Success: %s'
               'Took: %s s\n'
               'Summary: %s\n'
               'Data:\n%s'
               % (self.success,self.time_taken,
                  self.summary, self.__format_data())
              )
    return message

# Interface to beeswax. Responsible for executing queries, fetching results.
class ImpalaBeeswaxClient(object):
  def __init__(self, impalad):
    self.connected = False
    self.impalad = impalad
    self.imp_service = None
    self.transport = None
    self.query_options = {}
    self.query_state = QueryState._NAMES_TO_VALUES
    self.__make_default_options()

  def __make_default_options(self):
    def get_name(option): return TImpalaQueryOptions._VALUES_TO_NAMES[option]
    for option, default in DEFAULT_QUERY_OPTIONS.iteritems():
      self.query_options[get_name(option)] = default

  def __options_to_string_list(self):
    return ["%s:%s" % (k,v) for (k,v) in self.query_options.iteritems()]

  def get_query_options(self):
    return '\n'.join(["\t%s: %s" % (k,v) for (k,v) in self.query_options.iteritems()])

  def connect(self):
    """Connect to impalad specified in intializing this object

    Raises an exception if the connection is unsuccesful.
    """
    try:
      host, port = self.impalad.split(":")
      self.transport = TBufferedTransport(TSocket(host, int(port)))
      self.transport.open()
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      self.connected = True
    except Exception, e:
      raise ImpalaBeeswaxException(self.__build_error_message(e), e)

  def execute(self, query_string):
    """Re-directs the query to its appropriate handler, returns QueryResult"""
    # Take care of leading/trailing whitespaces.
    query_string = query_string.strip()
    query_type = shlex.split(query_string)[0]
    handle, time_taken = self.__execute_query(query_string)
    # Result fetching for insert is different from other queries.
    if query_type.lower() == 'insert':
      return self.__fetch_insert_results(handle, time_taken)
    return self.__fetch_results(handle, time_taken)

  def __execute_query(self, query_string):
    """Executes a query, returns the query handle and time taken for further processing."""
    query = BeeswaxService.Query()
    query.query = query_string
    query.configuration = self.__options_to_string_list()
    handle = self.__do_rpc(lambda: self.imp_service.query(query,))
    start, end = time.time(), 0
    # Wait for the query to finish execution.
    while True:
      query_state = self.__do_rpc(lambda: self.imp_service.get_state(handle))
      # if the rpc succeeded, the output is the query state
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        raise ImpalaBeeswaxException("Query aborted", None)
      time.sleep(0.05)
    end = time.time()
    return (handle, end - start)

  def __fetch_results(self, handle, time_taken):
    """Handles query results, returns a QueryResult object"""
    # The query has finished, we can fetch the results
    result_rows = []
    while True:
      results = self.__do_rpc(lambda: self.imp_service.fetch(handle, False, -1))
      result_rows.extend(results.data)
      if not results.has_more:
        break
    self.__do_rpc(lambda: self.imp_service.close(handle))
    # The query executed successfully and all the data was fetched.
    exec_result = QueryResult(success=True,
                              time_taken=time_taken,
                              data = result_rows)
    exec_result.summary = 'Returned %d rows' % (len(result_rows))
    return exec_result

  def __fetch_insert_results(self, handle, time_taken):
    """Executes an insert query"""
    result = self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))
    # The insert was successfull
    num_rows = sum(map(int, result.rows_appended.values()))
    exec_result = QueryResult(success=True,
                              time_taken=time_taken,
                              data = num_rows)
    exec_result.summary = "Inserted %d rows" % (num_rows,)
    return exec_result

  def __build_error_message(self, exception):
    """Construct a meaningful exception string"""
    return 'ERROR: %s, %s' % (type(exception), exception)

  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking.

    Catches all the relevant exceptions and re throws them wrapped
    in a custom exception [ImpalaBeeswaxException].
    """
    if not self.connected:
      raise ImpalaBeeswaxException("Not connected", None)
    try:
      return rpc()
    except BeeswaxService.BeeswaxException, b:
      raise ImpalaBeeswaxException(self.__build_error_message(b), b)
    except TTransportException, e:
      self.connected = False
      raise ImpalaBeeswaxException(self.__build_error_message(e), e)
    except TApplicationException, t:
      raise ImpalaBeeswaxException(self.__build_error_message(t), t)
    except Exception, u:
      raise ImpalaBeeswaxException(self.__build_error_message(u), t)
