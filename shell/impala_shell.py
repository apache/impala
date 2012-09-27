#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Impala's shell
import cmd
import time
import sys
from optparse import OptionParser

from ImpalaService import ImpalaService
from JavaConstants.constants import DEFAULT_QUERY_OPTIONS
from ImpalaService.ImpalaService import TImpalaQueryOptions
from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException

VERSION_STRING = "Impala v0.1 "

# Tarball / packaging build makes impala_build_version available
try:
  from impala_build_version import get_version_string
  from impala_build_version import get_build_date  
  VERSION_STRING += "(" + get_version_string()[:7] + ") built on " + get_build_date()
except Exception:
  VERSION_STRING += "(build version not available)"

# Simple Impala shell. Can issue queries (with configurable options)
# Basic usage: type connect <host:port> to connect to an impalad
# Then issue queries or other commands. Tab-completion should show the set of
# available commands.
# TODO: (amongst others)
#   - Column headers / metadata support
#   - Report profiles
#   - A lot of rpcs return a verbose TStatus from thrift/Status.thrift
#     This will be useful for better error handling. The next iteration
#     of the shell should handle this return paramter.
#   - Make commands case agnostic.
class ImpalaShell(cmd.Cmd):
  def __init__(self, options):
    cmd.Cmd.__init__(self)
    self.is_alive = True
    self.disconnected_prompt = "[Not connected] > "
    self.prompt = self.disconnected_prompt
    self.connected = False
    self.handle = None
    self.impalad = None
    self.imp_service = None
    self.transport = None
    self.query_options = {}
    self.__make_default_options()
    self.query_state = QueryState._NAMES_TO_VALUES
    if options.impalad != None:
      if self.do_connect(options.impalad) == False:
        sys.exit(1)

  def __make_default_options(self):
    self.query_options = {}
    def get_name(option): return TImpalaQueryOptions._VALUES_TO_NAMES[option]
    for option, default in DEFAULT_QUERY_OPTIONS.iteritems():
      self.query_options[get_name(option)] = default

  def __print_options(self):
    print '\n'.join(["\t%s: %s" % (k,v) for (k,v) in self.query_options.iteritems()])

  def __options_to_string_list(self):
    return ["%s:%s" % (k,v) for (k,v) in self.query_options.iteritems()]

  def do_options(self, args):
    self.__print_options()

  def do_set(self, args):
    tokens = args.split(" ")
    if len(tokens) != 2:
      print "Error: SET <option> <value>"
      return False
    option_upper = tokens[0].upper()
    if option_upper not in ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys():
      print "Unknown query option: %s" % (tokens[0],)
      available_options = \
          '\n\t'.join(ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys())
      print "Available query options are: \n\t%s" % available_options
      return False
    self.query_options[option_upper] = tokens[1]
    return False

  def do_quit(self, args):
    print "Goodbye"
    self.is_alive = False
    return True

  def do_connect(self, args):
    tokens = args.split(" ")
    if len(tokens) != 1:
      print "CONNECT takes exactly one argument: <hostname:port> of impalad to connect to"
      return False
    try:
      host, port = tokens[0].split(":")
      self.impalad = (host, port)
    except ValueError:
      print "Connect string must be of form <hostname:port>"
      return False

    if self.__connect():
      self.prompt = "[%s:%s] > " % self.impalad

  def __connect(self):
    if self.transport is not None:
      self.transport.close()
      self.transport = None

    self.transport = TBufferedTransport(TSocket(self.impalad[0], int(self.impalad[1])))
    try:
      self.transport.open()
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      self.connected = True
    except TTransportException, e:
      print "Error connecting: %s" % (e,)
      self.connected = False
    return self.connected

  def __query_with_results(self, query):
    print "Query: %s" % (query.query,)
    start, end = time.time(), 0
    (self.handle, ok) = self.__do_rpc(lambda: self.imp_service.query(query))
    fetching = False
    if not ok: return False

    try:
      while self.__get_query_state() not in [self.query_state["FINISHED"],
                                             self.query_state["EXCEPTION"]]:
        time.sleep(0.05)

      # The query finished with an exception, skip fetching results and close the handle.
      if self.__get_query_state() == self.query_state["EXCEPTION"]:
        print 'Query aborted, unable to fetch data'
        return self.__close_query_handle()
      # Results are ready, fetch them till they're done.
      print 'Query finished, fetching results ...'
      result_rows = []
      fetching = True
      while True:
        # TODO: Fetch more than one row at a time.
        (results, ok) = self.__do_rpc(lambda: self.imp_service.fetch(self.handle,
                                                                     False, -1))
        if not ok: return False
        result_rows.extend(results.data)
        if not results.has_more:
          fetching = False
          break
      end = time.time()
    except KeyboardInterrupt:
      # If Ctrl^C was caught during query execution, cancel the query.
      # If it was caught during fetching results, return to the prompt.
      if not fetching:
        return self.__cancel_query()
      else:
        print 'Fetching results aborted.'
        return self.__close_query_handle()

    print '\n'.join(result_rows)
    print "Returned %d row(s) in %2.2fs" % (len(result_rows), end - start)
    return self.__close_query_handle()

  def __close_query_handle(self):
    """Close the query handle and get back to the prompt"""
    self.__do_rpc(lambda: self.imp_service.close(self.handle))
    self.handle = None
    return False

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    query = BeeswaxService.Query()
    query.query = "select %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__query_with_results(query)

  def do_use(self, args):
    """Executes a USE... query"""
    query = BeeswaxService.Query()
    query.query = "use %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__query_with_results(query)

  def do_show(self, args):
    """Executes a SHOW... query, fetching all rows"""
    query = BeeswaxService.Query()
    query.query = "show %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__query_with_results(query)

  def do_describe(self, args):
    """Executes a DESCRIBE... query, fetching all rows"""
    query = BeeswaxService.Query()
    query.query = "describe %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__query_with_results(query)

  def do_insert(self, args):
    """Executes an INSERT query"""
    query = BeeswaxService.Query()
    query.query = "insert %s" % (args,)
    query.configuration = self.__options_to_string_list()
    print "Query: %s" % (query.query,)
    start, end = time.time(), 0
    (handle, ok) = \
        self.__do_rpc(lambda: self.imp_service.executeAndWait(query, "ImpalaCLI") )
    if not ok: return False
    (insert_result, ok) = self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))
    end = time.time()
    if not ok: return False
    num_rows = sum([int(k) for k in insert_result.rows_appended.values()])
    print "Inserted %d rows in %2.2fs" % (num_rows, end - start)

  def __cancel_query(self):
    """Cancel a query on keyboard interrupt from the shell."""
    print 'Cancelling query ...'
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    self.__do_rpc(lambda: self.imp_service.Cancel(self.handle))
    return False

  def __get_query_state(self):
    state, ok = self.__do_rpc(lambda : self.imp_service.get_state(self.handle))
    if ok:
      return state
    else:
      return self.query_state["EXCEPTION"]

  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking. Returns
       (rpc_result, True) if request was successful, (None, False) otherwise"""
    if not self.connected:
      print "Not connected (use CONNECT to establish a connection)"
      return (None, False)
    try:
      return (rpc(), True)
    except BeeswaxService.QueryNotFoundException, q:
      print 'Error: Stale query handle'
    # beeswaxException prints out the enture object, printing
    # just the message a far more readable/helpful.
    except BeeswaxService.BeeswaxException, b:
      print "ERROR: %s" % (b.message,)
    except TTransportException, e:
      print "Error communicating with impalad: %s" % (e,)
      self.connected = False
      self.prompt = self.disconnected_prompt
    except TApplicationException, t:
      print "Application Exception : %s" % (t,)
    except Exception, u:
      print 'Unknown Exception : %s' % (u,)
    return (None, False)

  def do_explain(self, args):
    query = BeeswaxService.Query()
    # Args is all text except for 'explain', so no need to strip it out
    query.query = args
    query.configuration = self.__options_to_string_list()
    print "Explain query: %s" % (query.query,)
    (explanation, ok) = self.__do_rpc(lambda: self.imp_service.explain(query))
    if ok:
      print explanation.textual
    return False

  def do_refresh(self, args):
    (_, ok) = self.__do_rpc(lambda: self.imp_service.ResetCatalog())
    if ok:
      print "Successfully refreshed catalog"
    return False

  def default(self, args):
    print "Unrecognized command"
    return False

  def emptyline(self):
    """If an empty line is entered, do nothing"""
    return False

  def do_version(self, args):
    """Prints the Impala build version"""
    print "Build version: %s" % VERSION_STRING
    return False

  def do_EOF(self, args):
    """Exit the shell on Ctrl^d"""
    print 'Goodbye'
    self.is_alive = False
    return True

WELCOME_STRING = """Welcome to the Impala shell. Press TAB twice to see a list of \
available commands.

Copyright (c) 2012 Cloudera, Inc. All rights reserved.
(Build version: %s)""" % VERSION_STRING

if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("-i", "--impalad", dest="impalad", default=None,
                    help="<host:port> of impalad to connect to")
  (options, args) = parser.parse_args()

  intro = WELCOME_STRING
  shell = ImpalaShell(options)
  while shell.is_alive:
    try:
      shell.cmdloop(intro)
    except KeyboardInterrupt:
      intro = '\n'
      pass
