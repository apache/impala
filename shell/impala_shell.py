#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Impala's shell
import cmd
import time
import sys
import os
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
# Methods that implement shell commands return a boolean tuple (stop, status)
# stop is a flag the command loop uses to continue/discontinue the prompt.
# Status tells the caller that the command completed successfully.
# TODO: (amongst others)
#   - Column headers / metadata support
#   - Report profiles
#   - A lot of rpcs return a verbose TStatus from thrift/Status.thrift
#     This will be useful for better error handling. The next iteration
#     of the shell should handle this return paramter.
class ImpalaShell(cmd.Cmd):
  def __init__(self, options):
    cmd.Cmd.__init__(self)
    self.is_alive = True
    self.verbose = options.verbose
    self.impalad = options.impalad
    self.disconnected_prompt = "[Not connected] > "
    self.prompt = self.disconnected_prompt
    self.connected = False
    self.handle = None
    self.imp_service = None
    self.transport = None
    self.query_options = {}
    self.__make_default_options()
    self.query_state = QueryState._NAMES_TO_VALUES
    if self.impalad != None:
      if self.do_connect(self.impalad) == False:
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
    """Print query options"""
    self.__print_if_verbose("Impala query options:")
    self.__print_options()
    return (False, True)

  def do_shell(self, args):
    """Run a command on the shell
    Usage: shell <cmd>
           ! <cmd>

    """
    try:
      os.system(args)
    except Exception, e:
      print 'Error running command : %s' % e
    return (False, True)

  def precmd(self, args):
    """Convert the command to lower case, so it's recognized"""
    tokens = args.strip().split(' ')
    # The first token should be the command
    # If it's EOF, call do_quit()
    if tokens[0] == 'EOF':
      return 'quit'
    else:
      tokens[0] = tokens[0].lower()
    return ' '.join(tokens)

  def postcmd(self, return_tuple, args):
    """Hack to make non interactive mode work"""
    # TODO : Remove in the future, this has to be cleaner.
    (stop, status) = return_tuple
    return stop

  def do_set(self, args):
    """Set query options:
    Usage: SET <option> <value>

    """
    tokens = args.split(" ")
    if len(tokens) != 2:
      print "Error: SET <option> <value>"
      return (False, False)
    option_upper = tokens[0].upper()
    if option_upper not in ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys():
      print "Unknown query option: %s" % (tokens[0],)
      available_options = \
          '\n\t'.join(ImpalaService.TImpalaQueryOptions._NAMES_TO_VALUES.keys())
      print "Available query options are: \n\t%s" % available_options
      return (False, False)
    self.query_options[option_upper] = tokens[1]
    self.__print_if_verbose('%s set to %s' % (option_upper, tokens[1]))
    return (False, True)

  def do_quit(self, args):
    """Quit the Impala shell"""
    self.__print_if_verbose("Goodbye")
    self.is_alive = False
    return (True, True)

  def do_connect(self, args):
    """Connect to an Impalad instance:
    Usage: connect <hostname:port>

    """
    tokens = args.split(" ")
    if len(tokens) != 1:
      print "CONNECT takes exactly one argument: <hostname:port> of impalad to connect to"
      return (False, False)
    try:
      host, port = tokens[0].split(":")
      self.impalad = (host, port)
    except ValueError:
      print "Connect string must be of form <hostname:port>"
      return (False, False)

    if self.__connect():
      self.__print_if_verbose('Connected to %s:%s' % self.impalad)
      self.prompt = "[%s:%s] > " % self.impalad
    return (False, True)

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
    self.__print_if_verbose("Query: %s" % (query.query,))
    start, end = time.time(), 0
    (self.handle, ok) = self.__do_rpc(lambda: self.imp_service.query(query))
    fetching = False
    if not ok: return (False, False)

    try:
      while self.__get_query_state() not in [self.query_state["FINISHED"],
                                             self.query_state["EXCEPTION"]]:
        time.sleep(0.05)

      # The query finished with an exception, skip fetching results and close the handle.
      if self.__get_query_state() == self.query_state["EXCEPTION"]:
        print 'Query aborted, unable to fetch data'
        return self.__close_query_handle()
      # Results are ready, fetch them till they're done.
      self.__print_if_verbose('Query finished, fetching results ...')
      result_rows = []
      fetching = True
      while True:
        # TODO: Fetch more than one row at a time.
        (results, ok) = self.__do_rpc(lambda: self.imp_service.fetch(self.handle,
                                                                     False, -1))
        if not ok: return (False, False)
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
    return (False, True)

  def __print_if_verbose(self, message):
    if self.verbose:
      print message

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
    if not ok: return (False, False)
    (insert_result, ok) = self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))
    end = time.time()
    if not ok: return (False, False)
    num_rows = sum([int(k) for k in insert_result.rows_appended.values()])
    print "Inserted %d rows in %2.2fs" % (num_rows, end - start)

  def __cancel_query(self):
    """Cancel a query on a keyboard interrupt from the shell."""
    print 'Cancelling query ...'
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    self.__do_rpc(lambda: self.imp_service.Cancel(self.handle))
    return (False, False)

  def __get_query_state(self):
    state, ok = self.__do_rpc(lambda : self.imp_service.get_state(self.handle))
    if ok:
      return state
    else:
      return self.query_state["EXCEPTION"]

  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking. Returns
       (rpc_result, True) if request was successful, (None, False) otherwise

    """
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
    """Explain the query execution plan"""
    query = BeeswaxService.Query()
    # Args is all text except for 'explain', so no need to strip it out
    query.query = args
    query.configuration = self.__options_to_string_list()
    print "Explain query: %s" % (query.query,)
    (explanation, ok) = self.__do_rpc(lambda: self.imp_service.explain(query))
    if ok:
      print explanation.textual
    return (False, True)

  def do_refresh(self, args):
    """Reload the Impalad catalog"""
    (_, ok) = self.__do_rpc(lambda: self.imp_service.ResetCatalog())
    if ok:
      print "Successfully refreshed catalog"
    return (False, True)

  def default(self, args):
    print "Unrecognized command"
    return (False, True)

  def emptyline(self):
    """If an empty line is entered, do nothing"""
    return (False, True)

  def do_version(self, args):
    """Prints the Impala build version"""
    print "Build version: %s" % VERSION_STRING
    return False


WELCOME_STRING = """Welcome to the Impala shell. Press TAB twice to see a list of \
available commands.

Copyright (c) 2012 Cloudera, Inc. All rights reserved.
(Build version: %s)""" % VERSION_STRING

def execute_queries_non_interactive_mode(options):
  """Run queries in non-interactive mode."""
  queries = []
  if options.query_file:
    try:
      query_file_handle = open(options.query_file, 'r')
      queries = query_file_handle.read().split(';')
      query_file_handle.close()
    except Exception, e:
      print 'Error: %s' % e
      return
  elif options.query:
    queries = [options.query]
  shell = ImpalaShell(options)
  # Deal with case.
  queries = map(shell.precmd, queries)
  for query in queries:
    (_, ok) = shell.onecmd(query)
    if not ok:
      print 'Could not execute command: %s' % query
      return

if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("-i", "--impalad", dest="impalad", default=None,
                    help="<host:port> of impalad to connect to")
  parser.add_option("-q", "--query", dest="query", default=None,
                    help="Execute a query without the shell")
  parser.add_option("-f", "--query_file", dest="query_file", default=None,
                    help="Execute the queries in the query file, delimited by ;")
  parser.add_option("-V", dest="verbose", default=False, action="store_true",
                    help="Enable verbose output")

  (options, args) = parser.parse_args()

  if options.query or options.query_file:
    execute_queries_non_interactive_mode(options)
    sys.exit(0)
  intro = WELCOME_STRING
  shell = ImpalaShell(options)
  while shell.is_alive:
    try:
      shell.cmdloop(intro)
    except KeyboardInterrupt:
      intro = '\n'
