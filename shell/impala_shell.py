#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
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
# Impala's shell
import cmd
import getpass
import prettytable
import os
import shlex
import signal
import socket
import sqlparse
import sys
import threading
import time
import re
from optparse import OptionParser
from Queue import Queue, Empty
from shell_output import OutputStream, DelimitedOutputFormatter, PrettyOutputFormatter

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from ImpalaService import ImpalaService
from ImpalaService.ImpalaService import TImpalaQueryOptions, TResetTableReq
from ImpalaService.ImpalaService import TPingImpalaServiceResp
from Status.ttypes import TStatus, TStatusCode
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport, TTransportException
from thrift.protocol import TBinaryProtocol
from thrift.Thrift import TApplicationException

VERSION_FORMAT = "Impala Shell v%(version)s (%(git_hash)s) built on %(build_date)s"
VERSION_STRING = "build version not available"
HISTORY_LENGTH = 100

# Tarball / packaging build makes impala_build_version available
try:
  from impala_build_version import get_git_hash, get_build_date, get_version
  VERSION_STRING = VERSION_FORMAT % {'version': get_version(),
                                     'git_hash': get_git_hash()[:7],
                                     'build_date': get_build_date()}
except Exception:
  pass

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
#   - A lot of rpcs return a verbose TStatus from thrift/Status.thrift
#     This will be useful for better error handling. The next iteration
#     of the shell should handle this return paramter.
class ImpalaShell(cmd.Cmd):
  DISCONNECTED_PROMPT = "[Not connected] > "
  # If not connected to an impalad, the server version is unknown.
  UNKNOWN_SERVER_VERSION = "Not Connected"
  # Commands are terminated with the following delimiter.
  CMD_DELIM = ';'
  DEFAULT_DB = 'default'
  # Regex applied to all tokens of a query to detect the query type.
  INSERT_REGEX = re.compile("^insert$", re.I)

  def __init__(self, options):
    cmd.Cmd.__init__(self)
    self.user = getpass.getuser()
    self.is_alive = True
    self.use_kerberos = options.use_kerberos
    self.verbose = options.verbose
    self.kerberos_service_name = options.kerberos_service_name
    self.impalad = None
    self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    self.connected = False
    self.imp_service = None
    self.transport = None
    self.fetch_batch_size = 1024
    self.default_query_options = {}
    self.set_query_options = {}
    self.query_state = QueryState._NAMES_TO_VALUES
    self.refresh_after_connect = options.refresh_after_connect
    self.current_db = options.default_db
    self.history_file = os.path.expanduser("~/.impalahistory")
    self.server_version = ImpalaShell.UNKNOWN_SERVER_VERSION
    self.show_profiles = options.show_profiles
    # Stores the state of user input until a delimiter is seen.
    self.partial_cmd = str()
    # Stores the old prompt while the user input is incomplete.
    self.cached_prompt = str()
    # Tracks query handle of the last query executed. Used by the 'profile' command.
    self.last_query_handle = None
    self.output_file = options.output_file
    # Output formatting flags/options
    self.output_delimiter = options.output_delimiter
    self.write_delimited = options.write_delimited
    self.print_header = options.print_header
    try:
      self.readline = __import__('readline')
      self.readline.set_history_length(HISTORY_LENGTH)
    except ImportError:
      self.readline = None
    if options.impalad != None:
      self.do_connect(options.impalad)

    # We handle Ctrl-C ourselves, using an Event object to signal cancellation
    # requests between the handler and the main shell thread. Ctrl-C is explicitly
    # not intercepted during an rpc, as it may interrupt system calls leaving
    # the underlying socket unusable.
    self.is_interrupted = threading.Event()
    signal.signal(signal.SIGINT, self.__signal_handler)

  def __print_options(self, options):
    if not options:
      print '\tNo options available.'
    else:
      print '\n'.join(["\t%s: %s" % (k,v) for (k,v) in options.iteritems()])

  def __options_to_string_list(self):
    return ["%s=%s" % (k,v) for (k,v) in self.set_query_options.iteritems()]

  def __build_default_query_options_dict(self):
    # The default query options are retrieved from a rpc call, and are dependent
    # on the impalad to which a connection has been established. They need to be
    # refreshed each time a connection is made. This is particularly helpful when
    # there is a version mismatch between the shell and the impalad.
    get_default_query_options = self.imp_service.get_default_configuration(False)
    options, status = self.__do_rpc(lambda: get_default_query_options)
    if status != RpcStatus.OK:
      print_to_stderr('Unable to retrive default query options')
    for option in options:
      self.default_query_options[option.key.upper()] = option.value

  def do_shell(self, args):
    """Run a command on the shell
    Usage: shell <cmd>
           ! <cmd>

    """
    try:
      os.system(args)
    except Exception, e:
      print_to_stderr('Error running command : %s' % e)
    return True

  def sanitise_input(self, args, interactive=True):
    """Convert the command to lower case, so it's recognized"""
    # A command terminated by a semi-colon is legal. Check for the trailing
    # semi-colons and strip them from the end of the command.
    args = args.strip()
    tokens = args.split(' ')
    if not interactive:
      tokens[0] = tokens[0].lower()
      # Strip all the non-interactive commands of the delimiter.
      return ' '.join(tokens).rstrip(ImpalaShell.CMD_DELIM)
    # The first token is converted into lower case to route it to the
    # appropriate command handler. This only applies to the first line of user input.
    # Modifying tokens in subsequent lines may change the semantics of the command,
    # so do not modify the text.
    if not self.partial_cmd:
      # The first token is the command.
      # If it's EOF, call do_quit()
      if tokens[0] == 'EOF':
        return 'quit'
      else:
        tokens[0] = tokens[0].lower()
    elif tokens[0] == "EOF":
      # If a command is in progress and the user hits a Ctrl-D, clear its state
      # and reset the prompt.
      self.prompt = self.cached_prompt
      self.partial_cmd = str()
      # The print statement makes the new prompt appear in a new line.
      # Also print an extra newline to indicate that the current command has
      # been cancelled.
      print '\n'
      return str()
    args = self.__check_for_command_completion(' '.join(tokens).strip())
    return args.rstrip(ImpalaShell.CMD_DELIM)

  def __check_for_command_completion(self, cmd):
    """Check for a delimiter at the end of user input.

    The end of the user input is scanned for a legal delimiter.
    If a delimiter is not found:
      - Input is not send to onecmd()
        - onecmd() is a method in Cmd which routes the user input to the
          appropriate method. An empty string results in a no-op.
      - Input is removed from history.
      - Input is appended to partial_cmd
    If a delimiter is found:
      - The contents of partial_cmd are put in history, as they represent
        a completed command.
      - The contents are passed to the appropriate method for execution.
      - partial_cmd is reset to an empty string.
    """
    if self.readline:
      current_history_len = self.readline.get_current_history_length()
    # Input is incomplete, store the contents and do nothing.
    if not cmd.endswith(ImpalaShell.CMD_DELIM):
      # The user input is incomplete, change the prompt to reflect this.
      if not self.partial_cmd and cmd:
        self.cached_prompt = self.prompt
        self.prompt = '> '.rjust(len(self.cached_prompt))
      # partial_cmd is already populated, add the current input after a newline.
      if self.partial_cmd and cmd:
        self.partial_cmd = "%s\n%s" % (self.partial_cmd, cmd)
      else:
        # If the input string is empty or partial_cmd is empty.
        self.partial_cmd = "%s%s" % (self.partial_cmd, cmd)
      # Remove the most recent item from history if:
      #   -- The current state of user input in incomplete.
      #   -- The most recent user input is not an empty string
      if self.readline and current_history_len > 0 and cmd:
        self.readline.remove_history_item(current_history_len - 1)
      # An empty string results in a no-op. Look at emptyline()
      return str()
    elif self.partial_cmd: # input ends with a delimiter and partial_cmd is not empty
      if cmd != ImpalaShell.CMD_DELIM:
        completed_cmd = "%s\n%s" % (self.partial_cmd, cmd)
      else:
        completed_cmd = "%s%s" % (self.partial_cmd, cmd)
      # Reset partial_cmd to an empty string
      self.partial_cmd = str()
      # Replace the most recent history item with the completed command.
      completed_cmd = sqlparse.format(completed_cmd, strip_comments=True)
      if self.readline and current_history_len > 0:
        # Update the history item to replace newlines with spaces. This is needed so
        # readline can properly restore the history (otherwise it interprets each newline
        # as a separate history item).
        self.readline.replace_history_item(current_history_len - 1,\
          completed_cmd.replace('\n', ' '))
      # Revert the prompt to its earlier state
      self.prompt = self.cached_prompt
    else: # Input has a delimiter and partial_cmd is empty
      completed_cmd = sqlparse.format(cmd, strip_comments=True)
    return completed_cmd

  def __signal_handler(self, signal, frame):
    self.is_interrupted.set()

  def precmd(self, args):
    self.is_interrupted.clear()
    args = self.sanitise_input(args)
    if not args: return args
    # Split args using sqlparse. If there are multiple queries present in user input,
    # the length of the returned query list will be greater than one.
    parsed_cmds = sqlparse.split(args)
    if len(parsed_cmds) > 1:
      # The last command needs a delimiter to be successfully executed.
      parsed_cmds[-1] += ImpalaShell.CMD_DELIM
      self.cmdqueue.extend(parsed_cmds)
      # If cmdqueue is populated, then commands are executed from the cmdqueue, and user
      # input is ignored. Send an empty string as the user input just to be safe.
      return str()
    return args

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
    """Set or display query options.

    Display query options:
    Usage: SET
    Set query options:
    Usage: SET <option>=<value>

    """
    # TODO: Expand set to allow for setting more than just query options.
    if not self.connected:
      print "Query options currently set:"
      self.__print_options(self.set_query_options)
      print "Connect to an impalad to see the default query options"
      return True
    if len(args) == 0:
      print "Default query options:"
      self.__print_options(self.default_query_options)
      print "Query options currently set:"
      self.__print_options(self.set_query_options)
      return True

    tokens = args.split("=")
    if len(tokens) != 2:
      print_to_stderr("Error: SET <option>=<value>")
      return False
    option_upper = tokens[0].upper()
    if option_upper not in self.default_query_options.keys():
      print "Unknown query option: %s" % (tokens[0],)
      print "Available query options, with their default values are:"
      self.__print_options(self.default_query_options)
      return False
    self.set_query_options[option_upper] = tokens[1]
    self.__print_if_verbose('%s set to %s' % (option_upper, tokens[1]))
    return True

  def do_unset(self, args):
    """Unset a query option"""
    if len(args.split()) != 1:
      print 'Usage: unset <option>'
      return False
    option = args.upper()
    if self.set_query_options.get(option):
      print 'Unsetting %s' % option
      del self.set_query_options[option]
    else:
      print "No option called %s is set" % args
    return True

  def do_quit(self, args):
    """Quit the Impala shell"""
    self.__print_if_verbose("Goodbye")
    self.is_alive = False
    # None is crutch to tell shell loop to quit
    return None

  def do_exit(self, args):
    """Exit the impala shell"""
    return self.do_quit(args)

  def do_connect(self, args):
    """Connect to an Impalad instance:
    Usage: connect <hostname:port>
           connect <hostname>, defaults to port 21000

    """
    tokens = args.split(" ")
    if len(tokens) != 1:
      print ("CONNECT takes exactly one argument: <hostname:port> or"
             " <hostname> of the impalad to connect to")
      return False

    # validate the connection string.
    host_port = [val for val in tokens[0].split(':') if val.strip()]
    if (':' in tokens[0] and len(host_port) != 2) or (not host_port):
      print "Connect string must be of form <hostname:port> or <hostname>"
      return False
    elif len(host_port) == 1:
      host_port.append(21000)
    self.impalad = tuple(host_port)

    if self.__connect():
      self.__print_if_verbose('Connected to %s:%s' % self.impalad)
      self.__print_if_verbose('Server version: %s' % self.server_version)
      self.prompt = "[%s:%s] > " % self.impalad
      if self.refresh_after_connect:
        self.cmdqueue.append('invalidate metadata' + ImpalaShell.CMD_DELIM)
        print_to_stderr("Invalidating Metadata")
      if self.current_db:
        self.cmdqueue.append('use %s' % self.current_db + ImpalaShell.CMD_DELIM)
      self.__build_default_query_options_dict()
    self.last_query_handle = None
    # In the case that we lost connection while a command was being entered,
    # we may have a dangling command, clear partial_cmd
    self.partial_cmd = str()
    # Check if any of query options set by the user are inconsistent
    # with the impalad being connected to
    for set_option in self.set_query_options.keys():
      if set_option not in set(self.default_query_options.keys()):
        print ('%s is not supported for the impalad being '
               'connected to, ignoring.' % set_option)
        del self.set_query_options[set_option]
    return True

  def __connect(self):
    if self.transport is not None:
      self.transport.close()
      self.transport = None

    self.connected = False
    self.server_version = ImpalaShell.UNKNOWN_SERVER_VERSION
    try:
      self.transport = self.__get_transport()
      self.transport.open()
      protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
      self.imp_service = ImpalaService.Client(protocol)
      try:
        result = self.imp_service.PingImpalaService()
        self.server_version = result.version
        self.connected = True
      except Exception, e:
        print_to_stderr("Error: Unable to communicate with impalad service. This "
               "service may not be an impalad instance. Check host:port and try again.")
        self.transport.close()
        raise
    except Exception, e:
      print_to_stderr("Error connecting: %s, %s" % (type(e),e))
      # If a connection to another impalad failed while already connected
      # reset the prompt to disconnected.
      self.prompt = self.DISCONNECTED_PROMPT

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
      sasl_client.setAttr("service", self.kerberos_service_name)
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

  def __create_beeswax_query_handle(self):
    handle = BeeswaxService.Query()
    handle.hadoop_user = self.user
    return handle

  def __get_column_names(self, handle):
    metadata = self.__do_rpc(lambda: self.imp_service.get_results_metadata(handle))
    return [fs.name for fs in metadata[0].schema.fieldSchemas]

  def __construct_table_header(self, handle, column_names):
    """ Constructs the table header for a given query handle.

    Should be called after the query has finished and before data is fetched. All data
    is left aligned.
    """
    table = prettytable.PrettyTable()
    for column in column_names:
      table.add_column(column, [])
    table.align = "l"
    return table

  def __expect_result_metadata(self, query_str):
    """ Given a query string, return True if impalad expects result metadata"""
    excluded_query_types = ['use', 'alter', 'create', 'drop']
    if True in set(map(query_str.startswith, excluded_query_types)):
      return False
    return True

  def __execute_query(self, query):
    self.__print_if_verbose("Query: %s" % (query.query,))
    start, end = time.time(), 0
    (handle, status) = self.__do_rpc(lambda: self.imp_service.query(query))

    if self.is_interrupted.isSet():
      if status == RpcStatus.OK:
        self.__cancel_and_close_query(handle)
      return False
    if status != RpcStatus.OK:
      return False

    loop_start = time.time()
    while True:
      query_state = self.__get_query_state(handle)
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        print_to_stderr('Query aborted, unable to fetch data')
        if self.connected:
          log, status = self.__do_rpc(
              lambda: self.imp_service.get_log(handle.log_context))
          print log
          return self.__close_query_handle(handle)
        else:
          return False
      elif self.is_interrupted.isSet():
        return self.__cancel_and_close_query(handle)
      time.sleep(self.__get_sleep_interval(loop_start))

    # impalad does not support the fetching of metadata for certain types of queries.
    if not self.__expect_result_metadata(query.query):
      self.__close_query_handle(handle)
      return True

    # Results are ready, fetch them till they're done.
    self.__print_if_verbose('Query finished, fetching results ...')
    column_names = self.__get_column_names(handle)
    if self.write_delimited:
      formatter = DelimitedOutputFormatter(field_delim=self.output_delimiter)
      self.output_stream = OutputStream(formatter, filename=self.output_file)
      # print the column names
      if self.print_header:
        self.output_stream.write([column_names])
    else:
      prettytable = self.__construct_table_header(handle, column_names)
      formatter = PrettyOutputFormatter(prettytable)
      self.output_stream = OutputStream(formatter, filename=self.output_file)

    result_rows = []
    num_rows_fetched = 0
    while True:
      # Fetch rows in batches of at most fetch_batch_size
      (results, status) = self.__do_rpc(\
        lambda: self.imp_service.fetch(handle, False, self.fetch_batch_size))

      if self.is_interrupted.isSet() or status != RpcStatus.OK:
        # Worth trying to cleanup the query even if fetch failed
        self.__cancel_and_close_query(handle)
        return False
      num_rows_fetched += len(results.data)
      result_rows.extend(results.data)
      if len(result_rows) >= self.fetch_batch_size or not results.has_more:
        rows = [row.split('\t') for row in result_rows]
        self.output_stream.write(rows)
        result_rows = []
      if not results.has_more:
        break

    # Don't include the time to get the runtime profile or runtime state log in the query
    # execution time
    end = time.time()
    self.__print_runtime_profile_if_enabled(handle)
    # Even though the query completed successfully, there may have been errors
    # encountered during execution
    log, status = self.__do_rpc(lambda: self.imp_service.get_log(handle.log_context))
    if log and log.strip():
      print_to_stderr('\nERRORS ENCOUNTERED DURING EXECUTION: %s' % log)

    self.__print_if_verbose(
      "Returned %d row(s) in %2.2fs" % (num_rows_fetched, end - start))
    self.last_query_handle = handle
    return self.__close_query_handle(handle)

  def __cancel_and_close_query(self, handle):
    """Cancel a query and immediately close it"""
    if self.__cancel_query(handle):
      return self.__close_query_handle(handle)
    else:
      return False

  def __close_query_handle(self, handle):
    """Close the query handle"""
    (_, status) = self.__do_rpc(lambda: self.imp_service.close(handle))
    return status == RpcStatus.OK

  def __print_runtime_profile_if_enabled(self, handle):
    if self.show_profiles:
      self.__print_runtime_profile(handle)

  def __print_runtime_profile(self, handle):
    profile = self.__get_runtime_profile(handle)
    if profile is not None:
      print "Query Runtime Profile:"
      print profile

  def __print_if_verbose(self, message):
    if self.verbose:
      print_to_stderr(message)

  def __parse_table_name_arg(self, arg):
    """ Parses an argument string and returns the result as a db name, table name combo.

    If the table name was not fully qualified, the current database is returned as the db.
    Otherwise, the table is split into db/table name parts and returned.
    If an invalid format is provided, None is returned.
    """
    if not arg:
      return None

    # If a multi-line argument, the name might be split across lines
    arg = arg.replace('\n','')
    # Get the database and table name, using the current database if the table name
    # wasn't fully qualified.
    db_name, tbl_name = self.current_db, arg
    if db_name is None:
      db_name = ImpalaShell.DEFAULT_DB
    db_table_name = arg.split('.')
    if len(db_table_name) == 1:
      return db_name, db_table_name[0]
    if len(db_table_name) == 2:
      return db_table_name

  def __handle_insert_query(self, query):
    print_to_stderr("Query: %s" % (query.query,))
    start, end = time.time(), 0
    (handle, status) = self.__do_rpc(lambda: self.imp_service.query(query))
    if status != RpcStatus.OK:
      return False

    query_successful = True
    while True:
      query_state = self.__get_query_state(handle)
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        print_to_stderr('Query failed')
        if self.connected:
          # Retrieve error message (if any) from log.
          log, status = self._ImpalaShell__do_rpc(
            lambda: self.imp_service.get_log(handle.log_context))
          print_to_stderr(log)
          query_successful = False
          break
        else:
          return False
      elif self.is_interrupted.isSet():
        return self.__cancel_and_close_query(handle)
      time.sleep(0.05)

    (insert_result, status) = self.__do_rpc(lambda: self.imp_service.CloseInsert(handle))
    end = time.time()
    if status != RpcStatus.OK or self.is_interrupted.isSet():
      return False

    if query_successful:
      self.__print_runtime_profile_if_enabled(handle)
      num_rows = sum([int(k) for k in insert_result.rows_appended.values()])
      self.__print_if_verbose("Inserted %d rows in %2.2fs" % (num_rows, end - start))
      self.last_query_handle = handle
    return query_successful

  def do_alter(self, args):
    query = BeeswaxService.Query()
    query.query = "alter %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_create(self, args):
    query = self.__create_beeswax_query_handle()
    query.query = "create %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_drop(self, args):
    query = self.__create_beeswax_query_handle()
    query.query = "drop %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_load(self, args):
    query = self.__create_beeswax_query_handle()
    query.query = "load %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_profile(self, args):
    """Prints the runtime profile of the last INSERT or SELECT query executed."""
    if len(args) > 0:
      print_to_stderr("'profile' does not accept any arguments")
      return False
    elif self.last_query_handle is None:
      print_to_stderr('No previous query available to profile')
      return False
    self.__print_runtime_profile(self.last_query_handle)
    return True

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    query = self.__create_beeswax_query_handle()
    query.query = "select %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_values(self, args):
    """Executes a VALUES(...) query, fetching all rows"""
    query = self.__create_beeswax_query_handle()
    query.query = "values %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_with(self, args):
    """Executes a query with a WITH clause, fetching all rows"""
    query = self.__create_beeswax_query_handle()
    query.query = "with %s" % (args,)
    query.configuration = self.__options_to_string_list()
    # Set posix=True and add "'" to escaped quotes
    # to deal with escaped quotes in string literals
    lexer = shlex.shlex(query.query.lstrip(), posix=True)
    lexer.escapedquotes += "'"
    # Because the WITH clause may precede INSERT or SELECT queries,
    # just checking the first token is insufficient.
    tokens = list(lexer)
    if filter(self.INSERT_REGEX.match, tokens):
      return self.__handle_insert_query(query)
    return self.__execute_query(query)

  def do_use(self, args):
    """Executes a USE... query"""
    query = self.__create_beeswax_query_handle()
    query.query = "use %s" % (args,)
    query.configuration = self.__options_to_string_list()
    result = self.__execute_query(query)
    if result:
      self.current_db = args
    return result

  def do_show(self, args):
    """Executes a SHOW... query, fetching all rows"""
    query = self.__create_beeswax_query_handle()
    query.query = "show %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_describe(self, args):
    """Executes a DESCRIBE... query, fetching all rows"""
    query = self.__create_beeswax_query_handle()
    query.query = "describe %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def do_desc(self, args):
    return self.do_describe(args)

  def do_insert(self, args):
    """Executes an INSERT query"""
    query = self.__create_beeswax_query_handle()
    query.query = "insert %s" % (args,)
    query.configuration = self.__options_to_string_list()
    return self.__handle_insert_query(query)

  def __cancel_query(self, handle):
    """Cancel a query on a keyboard interrupt from the shell."""
    print_to_stderr('Cancelling query ...')
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    (_, status) = self.__do_rpc(lambda: self.imp_service.Cancel(handle))
    return status == RpcStatus.OK

  def __get_query_state(self, handle):
    state, status = self.__do_rpc(lambda : self.imp_service.get_state(handle))
    if status != RpcStatus.OK:
      return self.query_state["EXCEPTION"]
    return state

  def __get_runtime_profile(self, handle):
    profile, status = self.__do_rpc(lambda: self.imp_service.GetRuntimeProfile(handle))
    if status == RpcStatus.OK and profile:
      return profile

  def __do_rpc(self, rpc):
    """Creates a child thread which executes the provided callable.

    Blocks until the child thread terminates. Reads its results, if any,
    from a Queue object. The child thread puts its results in the Queue object
    upon completion.

    """
    # The queue  is responsible for passing the rpc results from __do_rpc_thread
    # to __do_rpc.
    # TODO: Investigate whether this can be done without using a Queue object.
    rpc_results = Queue()
    rpc_thread = threading.Thread(target=self.__do_rpc_thread, args=[rpc, rpc_results])
    rpc_thread.start()
    rpc_thread.join()
    # The results should be in the queue. If they're not, return (None, RpcStatus.ERROR)
    try:
      results = rpc_results.get_nowait()
    except Empty:
      # Unexpected exception in __do_rpc_thread.
      print_to_stderr('Unexpected exception, no results returned.')
      results = (None, RpcStatus.ERROR)
    return results

  def __do_rpc_thread(self, rpc, rpc_results):
    """Executes the RPC lambda provided with some error checking.

       Puts the result tuple in the result queue. The result tuple is
       (rpc_result, RpcStatus.OK) if the rpc succeeded, (None, RpcStatus.ERROR)
       if it failed. If an exception occurs that cannot be recovered from,
       the connection will be closed and self.connected will be set to False.
       (None, RpcStatus.ERROR) will be put in the queue.

    """
    if not self.connected:
      print_to_stderr("Not connected (use CONNECT to establish a connection)")
      rpc_results.put((None, RpcStatus.ERROR))
      return
    try:
      ret = rpc()
      status = RpcStatus.OK
      # TODO: In the future more advanced error detection/handling can be done based on
      # the TStatus return value. For now, just print any error(s) that were encountered
      # and validate the result of the operation was a succes.
      if ret is not None and isinstance(ret, TStatus):
        if ret.status_code != TStatusCode.OK:
          if ret.error_msgs:
            print 'RPC Error: %s' % '\n'.join(ret.error_msgs)
          status = RpcStatus.ERROR
      rpc_results.put((ret, status))
    except BeeswaxService.QueryNotFoundException, q:
      print_to_stderr('Error: Stale query handle')
    # beeswaxException prints out the entire object, printing
    # just the message is far more readable/helpful.
    except BeeswaxService.BeeswaxException, b:
      print_to_stderr("ERROR: %s" % (b.message,))
    except TTransportException, e:
      print_to_stderr("Error communicating with impalad: %s" % (e,))
      self.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    except TApplicationException, t:
      print_to_stderr("Application Exception : %s" % (t,))
    except Exception, u:
      print_to_stderr('Unknown Exception : %s' % (u,))
      self.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    rpc_results.put((None, RpcStatus.ERROR))

  def do_explain(self, args):
    """Explain the query execution plan"""
    query = self.__create_beeswax_query_handle()
    # Args is all text except for 'explain', so no need to strip it out
    query.query = args
    query.configuration = self.__options_to_string_list()
    print_to_stderr("Explain query: %s" % (query.query,))
    (explanation, status) = self.__do_rpc(lambda: self.imp_service.explain(query))
    if status != RpcStatus.OK:
      return False

    print_to_stderr(explanation.textual)
    return True

  def do_history(self, args):
    """Display command history"""
    # Deal with readline peculiarity. When history does not exists,
    # readline returns 1 as the history length and stores 'None' at index 0.
    if self.readline and self.readline.get_current_history_length() > 0:
      for index in xrange(1, self.readline.get_current_history_length() + 1):
        cmd = self.readline.get_history_item(index)
        print_to_stderr('[%d]: %s' % (index, cmd))
    else:
      print_to_stderr('readline module not found, history is not supported.')
    return True

  def preloop(self):
    """Load the history file if it exists"""
    if self.readline:
      try:
        self.readline.read_history_file(self.history_file)
      except IOError, i:
        print_to_stderr('Unable to load history: %s' % i)

  def postloop(self):
    """Save session commands in history."""
    if self.readline:
      try:
        self.readline.write_history_file(self.history_file)
      except IOError, i:
        print_to_stderr('Unable to save history: %s' % i)

  def default(self, args):
    query = self.__create_beeswax_query_handle()
    query.query = args
    query.configuration = self.__options_to_string_list()
    return self.__execute_query(query)

  def emptyline(self):
    """If an empty line is entered, do nothing"""
    return True

  def do_version(self, args):
    """Prints the Impala build version"""
    print_to_stderr("Shell version: %s" % VERSION_STRING)
    print_to_stderr("Server version: %s" % self.server_version)
    return True

WELCOME_STRING = """Welcome to the Impala shell. Press TAB twice to see a list of \
available commands.

Copyright (c) 2012 Cloudera, Inc. All rights reserved.

(Shell build version: %s)""" % VERSION_STRING

def print_to_stderr(message):
  print >>sys.stderr, message

def parse_query_text(query_text):
  """Parse query file text and filter comments """
  queries = sqlparse.split(query_text)
  return map(strip_comments_from_query, queries)

def strip_comments_from_query(query):
  """Strip comments from an individual query """
  #TODO: Make query format configurable by the user.
  return sqlparse.format(query, strip_comments=True, reindent=True)

def execute_queries_non_interactive_mode(options):
  """Run queries in non-interactive mode."""
  queries = []
  if options.query_file:
    try:
      query_file_handle = open(options.query_file, 'r')
      queries = parse_query_text(query_file_handle.read())
      query_file_handle.close()
    except Exception, e:
      print_to_stderr('Error: %s' % e)
      sys.exit(1)
  elif options.query:
    queries = parse_query_text(options.query)
  shell = ImpalaShell(options)
  # The impalad was specified on the command line and the connection failed.
  # Return with an error, no need to process the query.
  if options.impalad and shell.connected == False:
    sys.exit(1)
  queries = shell.cmdqueue + queries
  # Deal with case.
  sanitized_queries = []
  for query in queries:
    sanitized_queries.append(shell.sanitise_input(query, interactive=False))
  for query in sanitized_queries:
    if not shell.onecmd(query):
      print_to_stderr('Could not execute command: %s' % query)
      if not options.ignore_query_failure:
        sys.exit(1)

if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("-i", "--impalad", dest="impalad", default=socket.getfqdn(),
                    help="<host:port> of impalad to connect to")
  parser.add_option("-q", "--query", dest="query", default=None,
                    help="Execute a query without the shell")
  parser.add_option("-f", "--query_file", dest="query_file", default=None,
                    help="Execute the queries in the query file, delimited by ;")
  parser.add_option("-k", "--kerberos", dest="use_kerberos", default=False,
                    action="store_true", help="Connect to a kerberized impalad")
  parser.add_option("-o", "--output_file", dest="output_file", default=None,
                    help=("If set, query results will written to the "
                          "given file. Results from multiple semicolon-terminated "
                          "queries will be appended to the same file"))
  parser.add_option("-B", "--delimited", dest="write_delimited", action="store_true",
                    help="Output rows in delimited mode")
  parser.add_option("--print_header", dest="print_header", action="store_true",
                    help="Print column names in delimited mode, true by default"
                         " when pretty-printed.")
  parser.add_option("--output_delimiter", dest="output_delimiter", default='\t',
                    help="Field delimiter to use for output in delimited mode")
  parser.add_option("-s", "--kerberos_service_name",
                    dest="kerberos_service_name", default=None,
                    help="Service name of a kerberized impalad, default is 'impala'")
  parser.add_option("-V", "--verbose", dest="verbose", default=True, action="store_true",
                    help="Enable verbose output")
  parser.add_option("-p", "--show_profiles", dest="show_profiles", default=False,
                    action="store_true",
                    help="Always display query profiles after execution")
  parser.add_option("--quiet", dest="verbose", default=True, action="store_false",
                    help="Disable verbose output")
  parser.add_option("-v", "--version", dest="version", default=False, action="store_true",
                    help="Print version information")
  parser.add_option("-c", "--ignore_query_failure", dest="ignore_query_failure",
                    default=False, action="store_true", help="Continue on query failure")
  parser.add_option("-r", "--refresh_after_connect", dest="refresh_after_connect",
                    default=False, action="store_true",
                    help="Refresh Impala catalog after connecting")
  parser.add_option("-d", "--database", dest="default_db", default=None,
                    help="Issue a use database command on startup.")
  options, args = parser.parse_args()

  # Arguments that could not be parsed are stored in args. Print an error and exit.
  if len(args) > 0:
    print_to_stderr('Error, could not parse arguments "%s"' % (' ').join(args))
    parser.print_help()
    sys.exit(1)

  if options.version:
    print VERSION_STRING
    sys.exit(0)

  if options.use_kerberos:
    # The sasl module is bundled with the shell.
    print_to_stderr("Starting Impala Shell in secure mode (using Kerberos)")
    try:
      import sasl
    except ImportError:
      print_to_stderr('sasl not found.')
      sys.exit(1)
    from thrift_sasl import TSaslClientTransport

    # The service name defaults to 'impala' if not specified by the user.
    if not options.kerberos_service_name:
      options.kerberos_service_name = 'impala'
    print_to_stderr("Using service name '%s'" % options.kerberos_service_name)
  else:
     print_to_stderr("Starting Impala Shell in unsecure mode")

  if options.output_file:
    try:
      # Make sure the given file can be opened for writing. This will also clear the file
      # if successful.
      open(options.output_file, 'wb')
    except IOError, e:
      print_to_stderr('Error opening output file for writing: %s' % e)
      sys.exit(1)

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
