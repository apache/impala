#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Impala's shell
import cmd
import time
import sys
import os
import signal
import threading
from optparse import OptionParser

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from ImpalaService import ImpalaService
from ImpalaService.ImpalaService import TImpalaQueryOptions
from JavaConstants.constants import DEFAULT_QUERY_OPTIONS
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

class RpcStatus:
  """Convenience enum to describe Rpc return statuses"""
  OK = 0
  ERROR = 1

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
  DISCONNECTED_PROMPT = "[Not connected] > "

  def __init__(self, options):
    cmd.Cmd.__init__(self)
    self.is_alive = True
    self.use_kerberos = options.use_kerberos
    self.verbose = options.verbose
    self.impalad = None
    self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    self.connected = False
    self.imp_service = None
    self.transport = None
    self.query_options = {}
    self.__make_default_options()
    self.query_state = QueryState._NAMES_TO_VALUES
    if options.impalad != None:
      self.do_connect(options.impalad)

    # We handle Ctrl-C ourselves, using an Event object to signal cancellation
    # requests between the handler and the main shell thread
    self.is_interrupted = threading.Event()
    signal.signal(signal.SIGINT, self.__signal_handler)

  def __get_option_name(self, option):
    return TImpalaQueryOptions._VALUES_TO_NAMES[option]

  def __make_default_options(self):
    self.query_options = {}
    for option, default in DEFAULT_QUERY_OPTIONS.iteritems():
      self.query_options[self.__get_option_name(option)] = default

  def __print_options(self):
    print '\n'.join(["\t%s: %s" % (k,v) for (k,v) in self.query_options.iteritems()])

  def __options_to_string_list(self):
    return ["%s:%s" % (k,v) for (k,v) in self.query_options.iteritems()]

  def do_options(self, args):
    """Print query options"""
    self.__print_if_verbose("Impala query options:")
    self.__print_options()
    return True

  def do_shell(self, args):
    """Run a command on the shell
    Usage: shell <cmd>
           ! <cmd>

    """
    try:
      os.system(args)
    except Exception, e:
      print 'Error running command : %s' % e
    return True

  def sanitise_input(self, args):
    """Convert the command to lower case, so it's recognized"""
    # A command terminated by a semi-colon is legal. Check for the trailing
    # semi-colons and strip them from the end of the command.
    args = args.strip().rstrip(';')
    tokens = args.split(' ')
    # The first token should be the command
    # If it's EOF, call do_quit()
    if tokens[0] == 'EOF':
      return 'quit'
    else:
      tokens[0] = tokens[0].lower()
    return ' '.join(tokens)

  def __signal_handler(self, signal, frame):
    self.is_interrupted.set()

  def precmd(self, args):
    self.is_interrupted.clear()
    return self.sanitise_input(args)

  def postcmd(self, status, args):
    """Hack to make non interactive mode work"""
    self.is_interrupted.clear()
    # cmd expects return of False to keep going, and True to quit.
    # Shell commands return True on success, False on error, and None to quit, so
    # translate between them.
    # TODO : Remove in the future once shell and Impala query processing can be separated.
    if status == None:
      return True
    else:
      return False

  def do_set(self, args):
    """Set query options:
    Usage: SET <option> <value>

    """
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
    self.__print_if_verbose('%s set to %s' % (option_upper, tokens[1]))
    return True

  def do_quit(self, args):
    """Quit the Impala shell"""
    self.__print_if_verbose("Goodbye")
    self.is_alive = False
    # None is crutch to tell shell loop to quit
    return None

  def do_connect(self, args):
    """Connect to an Impalad instance:
    Usage: connect <hostname:port>

    """
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
      self.__print_if_verbose('Connected to %s:%s' % self.impalad)
      self.prompt = "[%s:%s] > " % self.impalad
    return True

  def __connect(self):
    if self.transport is not None:
      self.transport.close()
      self.transport = None

    try:
      self.transport = self.__get_transport()
      self.transport.open()
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      self.connected = True
    except Exception, e:
      print "Error connecting: %s, %s" % (type(e),e)
      self.connected = False
    return self.connected

  def __get_transport(self):
    """Create a Transport.

       A non-kerberized impalad just needs a simple buffered transport. For
       the kerberized version, a sasl transport is created.
    """
    sock = TSocket(self.impalad[0], int(self.impalad[1]))
    if not self.use_kerberos:
      return TBufferedTransport(sock)
    # Initializes a sasl client
    def sasl_factory():
      sasl_client = sasl.Client()
      sasl_client.setAttr("host", self.impalad[0])
      sasl_client.setAttr("service", "impala")
      sasl_client.init()
      return sasl_client
    # GSSASPI is the underlying mechanism used by kerberos to authenticate.
    return TSaslClientTransport(sasl_factory, "GSSAPI", sock)

  def __get_sleep_interval(self, start_time):
    """Returns a step function of time to sleep in seconds before polling
    again. Maximum sleep is 1s, minimum is 0.1s"""
    elapsed = time.time() - start_time
    if elapsed < 10.0:
      return 0.1
    elif elapsed < 60.0:
      return 0.5

    return 1.0

  def __query_with_results(self, query):
    self.__print_if_verbose("Query: %s" % (query.query,))
    start, end = time.time(), 0
    (handle, status) = self.__do_rpc(lambda: self.imp_service.query(query))

    if self.is_interrupted.isSet():
      if status == RpcStatus.OK:
        self.__close_query_handle(handle)
      return False
    if status != RpcStatus.OK:
      return False

    loop_start = time.time()
    while True:
      query_state = self.__get_query_state(handle)
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        print 'Query aborted, unable to fetch data'
        if self.connected:
          return self.__close_query_handle(handle)
        else:
          return False
      elif self.is_interrupted.isSet():
        return self.__cancel_query(handle)
      time.sleep(self.__get_sleep_interval(loop_start))

    # Results are ready, fetch them till they're done.
    self.__print_if_verbose('Query finished, fetching results ...')
    result_rows = []
    while True:
      # TODO: Fetch more than one row at a time.
      # Also fetch rows asynchronously, so we can print them to screen without
      # pausing the fetch process
      (results, status) = self.__do_rpc(lambda: self.imp_service.fetch(handle,
                                                                       False, -1))

      if self.is_interrupted.isSet() or status != RpcStatus.OK:
        # Worth trying to cleanup the query even if fetch failed
        if self.connected:
          self.__close_query_handle(handle)
        return False

      result_rows.extend(results.data)
      if not results.has_more:
        break
    end = time.time()

    print '\n'.join(result_rows)
    self.__print_if_verbose(
      "Returned %d row(s) in %2.2fs" % (len(result_rows), end - start))
    return self.__close_query_handle(handle)

  def __close_query_handle(self, handle):
    """Close the query handle"""
    self.__do_rpc(lambda: self.imp_service.close(handle))
    return True

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
    (handle, status) = self.__do_rpc(lambda: self.imp_service.query(query))

    if status != RpcStatus.OK:
      return False

    while True:
      query_state = self.__get_query_state(handle)
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        print 'Remote error'
        if self.connected:
          # It's ok to close an INSERT that's failed rather than do the full
          # CloseInsert. The latter builds an InsertResult which is meaningless
          # here.
          return self.__close_query_handle(handle)
        else:
          return False
      elif self.is_interrupted.isSet():
        return self.__cancel_query(handle)
      time.sleep(0.05)

    (insert_result, status) = self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))
    end = time.time()
    if status != RpcStatus.OK or self.is_interrupted.isSet():
      return False

    num_rows = sum([int(k) for k in insert_result.rows_appended.values()])
    self.__print_if_verbose("Inserted %d rows in %2.2fs" % (num_rows, end - start))
    return True

  def __cancel_query(self, handle):
    """Cancel a query on a keyboard interrupt from the shell."""
    print 'Cancelling query ...'
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    (_, status) = self.__do_rpc(lambda: self.imp_service.Cancel(handle))
    if status != RpcStatus.OK:
      return False

    return True

  def __get_query_state(self, handle):
    state, status = self.__do_rpc(lambda : self.imp_service.get_state(handle))
    if status != RpcStatus.OK:
      return self.query_state["EXCEPTION"]
    return state

  def __do_rpc(self, rpc):
    """Executes the RPC lambda provided with some error checking. Returns
       (rpc_result, RpcStatus.OK) if request was successful,
       (None, RpcStatus.ERROR) otherwise.

       If an exception occurs that cannot be recovered from, the connection will
       be closed and self.connected will be set to False.

    """
    if not self.connected:
      print "Not connected (use CONNECT to establish a connection)"
      return (None, RpcStatus.ERROR)
    try:
      ret = rpc()
      return (ret, RpcStatus.OK)
    except BeeswaxService.QueryNotFoundException, q:
      print 'Error: Stale query handle'
    # beeswaxException prints out the entire object, printing
    # just the message a far more readable/helpful.
    except BeeswaxService.BeeswaxException, b:
      print "ERROR: %s" % (b.message,)
    except TTransportException, e:
      print "Error communicating with impalad: %s" % (e,)
      self.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    except TApplicationException, t:
      print "Application Exception : %s" % (t,)
    except Exception, u:
      print 'Unknown Exception : %s' % (u,)
      self.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    return (None, RpcStatus.ERROR)

  def do_explain(self, args):
    """Explain the query execution plan"""
    query = BeeswaxService.Query()
    # Args is all text except for 'explain', so no need to strip it out
    query.query = args
    query.configuration = self.__options_to_string_list()
    print "Explain query: %s" % (query.query,)
    (explanation, status) = self.__do_rpc(lambda: self.imp_service.explain(query))
    if status != RpcStatus.OK:
      return False

    print explanation.textual
    return True

  def do_refresh(self, args):
    """Reload the Impalad catalog"""
    (_, status) = self.__do_rpc(lambda: self.imp_service.ResetCatalog())
    if status != RpcStatus.OK:
      return False

    print "Successfully refreshed catalog"
    return True

  def default(self, args):
    print "Unrecognized command"
    return True

  def emptyline(self):
    """If an empty line is entered, do nothing"""
    return True

  def do_version(self, args):
    """Prints the Impala build version"""
    print "Build version: %s" % VERSION_STRING
    return True


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
  queries = map(shell.sanitise_input, queries)
  for query in queries:
    if not shell.onecmd(query):
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
  parser.add_option("-k", "--kerberos", dest="use_kerberos", default=False,
                    action="store_true",help="Connect to a kerberized impalad.")
  parser.add_option("-V", "--verbose", dest="verbose", default=False, action="store_true",
                    help="Enable verbose output")

  (options, args) = parser.parse_args()

  if options.use_kerberos:
    from thrift_sasl import TSaslClientTransport
    import sasl
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
