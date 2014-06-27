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
import sasl
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
from subprocess import call

from beeswaxd import BeeswaxService
from beeswaxd.BeeswaxService import QueryState
from ImpalaService import ImpalaService
from ImpalaService.ImpalaService import TImpalaQueryOptions, TResetTableReq
from ExecStats.ttypes import TExecStats
from ImpalaService.ImpalaService import TPingImpalaServiceResp
from Status.ttypes import TStatus, TStatusCode
from thrift_sasl import TSaslClientTransport
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

class ImpalaPrettyTable(prettytable.PrettyTable):
  """Patched version of PrettyTable that TODO"""
  def _unicode(self, value):
    if not isinstance(value, basestring):
      value = str(value)
    if not isinstance(value, unicode):
      # If a value cannot be encoded, replace it with a placeholder.
      value = unicode(value, self.encoding, "replace")
    return value

class RpcStatus:
  """Convenience enum to describe Rpc return statuses"""
  OK = 0
  ERROR = 1


class RpcResult(object):
  """Wrapper for Rpc results.

  An Rpc results consists of the status and the result of the rpc.
  If a queue object is passed to the ctor, get_results blocks until there's a result in
  the queue. If the rpc result and status are passed to the ctor, it acts as a wrapper and
  returns them.
  """
  def __init__(self, queue=None, result=None, status=None):
    self.result_queue = queue
    self.result = result
    self.status = status

  def get_results(self):
    if self.result_queue:
      # Block until the results are available.
      # queue.get() without a timeout is not interruptable with KeyboardInterrupt.
      # Set the timeout to a day (a reasonable limit for a single rpc call)
      self.result, self.status = self.result_queue.get(True, 60 * 24 * 24)
    return self.result, self.status


class ImpalaShell(cmd.Cmd):
  """ Simple Impala Shell.

  Basic usage: type connect <host:port> to connect to an impalad
  Then issue queries or other commands. Tab-completion should show the set of
  available commands.
  Methods that implement shell commands return a boolean tuple (stop, status)
  stop is a flag the command loop uses to continue/discontinue the prompt.
  Status tells the caller that the command completed successfully.
  """

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
    self.user = options.user
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
    self.ssl_enabled = options.ssl
    self.ca_cert = options.ca_cert
    self.use_ldap = options.use_ldap
    # Stores the state of user input until a delimiter is seen.
    self.partial_cmd = str()
    # Stores the old prompt while the user input is incomplete.
    self.cached_prompt = str()
    # Tracks query handle of the last query executed. Used by the 'profile' command.
    self.last_query_handle = None
    self.query_handle_closed = False
    # Indicates whether the rpc being run is interruptable. If True, the signal handler
    # reconnects and closes the query handle.
    self.rpc_is_interruptable = False
    self.output_file = options.output_file
    # Output formatting flags/options
    self.output_delimiter = options.output_delimiter
    self.write_delimited = options.write_delimited
    self.print_header = options.print_header
    if options.strict_unicode:
      self.utf8_encode_policy = 'strict'
    else:
      self.utf8_encode_policy = 'ignore'
    self.__populate_command_list()
    try:
      self.readline = __import__('readline')
      self.readline.set_history_length(HISTORY_LENGTH)
    except ImportError:
      self.__disable_readline()

    if self.use_ldap:
      self.ldap_password = getpass.getpass("LDAP password for %s:" % self.user)

    if options.impalad != None:
      self.do_connect(options.impalad)

    # We handle Ctrl-C ourselves, using an Event object to signal cancellation
    # requests between the handler and the main shell thread. Ctrl-C is explicitly
    # not intercepted during an rpc, as it may interrupt system calls leaving
    # the underlying socket unusable.
    self.is_interrupted = threading.Event()
    signal.signal(signal.SIGINT, self.__signal_handler)

  def __populate_command_list(self):
    """Populate a list of commands in the shell.

    Each command has its own method of the form do_<command>, and can be extracted by
    introspecting the class directory.
    """
    # Slice the command method name to get the name of the command.
    self.commands = [cmd[3:] for cmd in dir(self.__class__) if cmd.startswith('do_')]

  def __disable_readline(self):
    """Disables the readline module.

    The readline module is responsible for keeping track of command history.
    """
    self.readline = None

  def __print_options(self, default_options, set_options):
    # Prints the current query options
    # with default values distinguished from set values by brackets []
    if not default_options and not set_options:
      print '\tNo options available.'
    else:
      for k in sorted(default_options.keys()):
        if k in set_options.keys() and set_options[k] != default_options[k]:
          print '\n'.join(["\t%s: %s" % (k, set_options[k])])
        else:
          print '\n'.join(["\t%s: [%s]" % (k, default_options[k])])

  def __options_to_string_list(self):
    return ["%s=%s" % (k, v) for (k, v) in self.set_query_options.iteritems()]

  def __build_default_query_options_dict(self):
    # The default query options are retrieved from a rpc call, and are dependent
    # on the impalad to which a connection has been established. They need to be
    # refreshed each time a connection is made. This is particularly helpful when
    # there is a version mismatch between the shell and the impalad.
    get_default_query_options = self.imp_service.get_default_configuration(False)
    rpc_result = self.__do_rpc(lambda: get_default_query_options)
    options, status = rpc_result.get_results()
    if status != RpcStatus.OK:
      print_to_stderr('Unable to retrieve default query options')
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

  def __cmd_ends_with_delim(self, line):
    """Check if the input command ends with a command delimiter.

    A command ending with the delimiter and containing an open quotation character is
    not considered terminated. If no open quotation is found, it's considered
    terminated.
    """
    if line.endswith(ImpalaShell.CMD_DELIM):
      try:
        # Look for an open quotation in the entire command, and not just the
        # current line.
        if self.partial_cmd: line = '%s %s' % (self.partial_cmd, line)
        shlex.split(line)
        return True
      # If the command ends with a delimiter, check if it has an open quotation.
      # shlex.split() throws a ValueError iff an open quoation is found.
      # A quotation can either be a single quote or a double quote.
      except ValueError:
        pass
    return False

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
    if not self.__cmd_ends_with_delim(cmd):
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
    elif self.partial_cmd:  # input ends with a delimiter and partial_cmd is not empty
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
        self.readline.replace_history_item(current_history_len - 1,
          completed_cmd.replace('\n', ' '))
      # Revert the prompt to its earlier state
      self.prompt = self.cached_prompt
    else:  # Input has a delimiter and partial_cmd is empty
      completed_cmd = sqlparse.format(cmd, strip_comments=True)
    # The comments have been parsed out, there is no need to retain the newlines.
    # They can cause parse errors in sqlparse when unescaped quotes and delimiters
    # come into play.
    return completed_cmd.replace('\n', ' ')

  def __signal_handler(self, signal, frame):
    self.is_interrupted.set()
    if not self.rpc_is_interruptable: return
    # If the is_interruptable event object is set, the rpc may be still in progress.
    # Create a new connection to the impalad and cancel the query.
    try:
      self.__connect()
      print_to_stderr('Closing Query handle.')
      self.__close_query()
      if self.current_db:
        self.cmdqueue.append('use %s' % self.current_db + ImpalaShell.CMD_DELIM)
    except Exception, e:
      print_to_stderr("Failed to reconnect and close: %s" % str(e))

  def precmd(self, args):
    self.is_interrupted.clear()
    self.rpc_is_interruptable = False
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
    return args.encode('utf-8', self.utf8_encode_policy)

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

  def __build_summary_table(self, summary, idx, is_fragment_root, indent_level, output):
    """Direct translation of Coordinator::PrintExecSummary() to recursively build a list
    of rows of summary statistics, one per exec node

    summary: the TExecSummary object that contains all the summary data

    idx: the index of the node to print

    is_fragment_root: true if the node to print is the root of a fragment (and therefore
    feeds into an exchange)

    indent_level: the number of spaces to print before writing the node's label, to give
    the appearance of a tree. The 0th child of a node has the same indent_level as its
    parent. All other children have an indent_level of one greater than their parent.

    output: the list of rows into which to append the rows produced for this node and its
    children.

    Returns the index of the next exec node in summary.exec_nodes that should be
    processed, used internally to this method only.
    """
    attrs = ["latency_ns", "cpu_time_ns", "cardinality", "memory_used"]

    # Initialise aggregate and maximum stats
    agg_stats, max_stats = TExecStats(), TExecStats()
    for attr in attrs:
      setattr(agg_stats, attr, 0)
      setattr(max_stats, attr, 0)

    node = summary.nodes[idx]
    for stats in node.exec_stats:
      for attr in attrs:
        val = getattr(stats, attr)
        if val is not None:
          setattr(agg_stats, attr, getattr(agg_stats, attr) + val)
          setattr(max_stats, attr, max(getattr(max_stats, attr), val))

    if len(node.exec_stats) > 0:
      avg_time = agg_stats.latency_ns / len(node.exec_stats)
    else:
      avg_time = 0

    # If the node is a broadcast-receiving exchange node, the cardinality of rows produced
    # is the max over all instances (which should all have received the same number of
    # rows). Otherwise, the cardinality is the sum over all instances which process
    # disjoint partitions.
    if node.is_broadcast and is_fragment_root:
      cardinality = max_stats.cardinality
    else:
      cardinality = agg_stats.cardinality

    est_stats = node.estimated_stats
    label_prefix = ""
    if indent_level > 0:
      label_prefix = "|"
      if is_fragment_root:
        label_prefix += "  " * indent_level
      else:
        label_prefix += "--" * indent_level

    def prettyprint(val, units, divisor):
      for unit in units:
        if val < divisor:
          if unit == units[0]:
            return "%d%s" % (val, unit)
          else:
            return "%3.2f%s" % (val, unit)
        val /= divisor

    def prettyprint_bytes(byte_val):
      return prettyprint(byte_val, [' B', ' KB', ' MB', ' GB', ' TB'], 1024.0)

    def prettyprint_units(unit_val):
      return prettyprint(unit_val, ["", "K", "M", "B"], 1000.0)

    def prettyprint_time(time_val):
      return prettyprint(time_val, ["ns", "us", "ms", "s"], 1000.0)

    row = [ label_prefix + node.label,
            len(node.exec_stats),
            prettyprint_time(avg_time),
            prettyprint_time(max_stats.latency_ns),
            prettyprint_units(cardinality),
            prettyprint_units(est_stats.cardinality),
            prettyprint_bytes(max_stats.memory_used),
            prettyprint_bytes(est_stats.memory_used),
            node.label_detail ]

    output.append(row)
    try:
      sender_idx = summary.exch_to_sender_map[idx]
      # This is an exchange node, so the sender is a fragment root, and should be printed
      # next.
      self.__build_summary_table(summary, sender_idx, True, indent_level, output)
    except (KeyError, TypeError):
      # Fall through if idx not in map, or if exch_to_sender_map itself is not set
      pass

    idx += 1
    if node.num_children > 0:
      first_child_output = []
      idx = \
        self.__build_summary_table(summary, idx, False, indent_level, first_child_output)
      for child_idx in xrange(1, node.num_children):
        # All other children are indented (we only have 0, 1 or 2 children for every exec
        # node at the moment)
        idx = self.__build_summary_table(summary, idx, False, indent_level + 1, output)
      output += first_child_output
    return idx

  def do_summary(self, args):
    if not self.connected:
      print_to_stderr("Must be connected to an Impala demon to retrieve query summaries")
      return True
    summary = self.__get_summary()
    if summary is None:
      print_to_stderr("Could not retrieve summary for query.")
      return True
    if summary.nodes is None:
      print_to_stderr("Summary not available")
      return True
    output = []
    table = self.__construct_table_header(["Operator", "#Hosts", "Avg Time", "Max Time",
                                           "#Rows", "Est. #Rows", "Peak Mem",
                                           "Est. Peak Mem", "Detail"])
    self.__build_summary_table(summary, 0, False, 0, output)
    formatter = PrettyOutputFormatter(table)
    self.output_stream = OutputStream(formatter, filename=self.output_file)
    self.output_stream.write(output)
    return True

  def do_set(self, args):
    """Set or display query options.

    Display query options:
    Usage: SET
    Set query options:
    Usage: SET <option>=<value>

    """
    # TODO: Expand set to allow for setting more than just query options.
    if not self.connected:
      print ("Connect to an impalad to view and set query options.")
      return True
    if len(args) == 0:
      print "Query options (defaults shown in []):"
      self.__print_options(self.default_query_options, self.set_query_options);
      return True

    # Remove any extra spaces surrounding the tokens.
    # Allows queries that have spaces around the = sign.
    tokens = [arg.strip() for arg in args.split("=")]
    if len(tokens) != 2:
      print_to_stderr("Error: SET <option>=<value>")
      return False
    option_upper = tokens[0].upper()
    if option_upper not in self.default_query_options.keys():
      print "Unknown query option: %s" % (tokens[0])
      print "Available query options, with their values are (defaults shown in []):"
      self.__print_options(self.default_query_options, self.set_query_options)
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
    Usage: connect, defaults to the fqdn of the localhost and port 21000
           connect <hostname:port>
           connect <hostname>, defaults to port 21000

    """
    # Assume the user wants to connect to the local impalad if no connection string is
    # specified. Conneting to a kerberized impalad requires an fqdn as the host name.
    if not args: args = socket.getfqdn()
    tokens = args.split(" ")
    # validate the connection string.
    host_port = [val for val in tokens[0].split(':') if val.strip()]
    if (':' in tokens[0] and len(host_port) != 2):
      print_to_stderr("Connection string must either be empty, or of the form "
                      "<hostname[:port]>")
      return False
    elif len(host_port) == 1:
      host_port.append(21000)
    self.impalad = tuple(host_port)
    self.__connect()
    # If the connection fails and the Kerberos has not been enabled,
    # check for a valid kerberos ticket and retry the connection
    # with kerberos enabled.
    if not self.connected and not self.use_kerberos:
      try:
        if call(["klist", "-s"]) == 0:
          print_to_stderr(("Kerberos ticket found in the credentials cache, retrying "
                           "the connection with a secure transport."))
          self.use_kerberos = True
          self.__connect()
      except OSError, e:
        pass

    if self.connected:
      self.__print_if_verbose('Connected to %s:%s' % self.impalad)
      self.__print_if_verbose('Server version: %s' % self.server_version)
      self.prompt = "[%s:%s] > " % self.impalad
      if self.refresh_after_connect:
        self.cmdqueue.append('invalidate metadata' + ImpalaShell.CMD_DELIM)
        print_to_stderr("Invalidating Metadata")
      if self.current_db:
        self.cmdqueue.append(('use `%s`' % self.current_db) + ImpalaShell.CMD_DELIM)
      self.__build_default_query_options_dict()
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
      # We get a TApplicationException if the transport is valid, but the RPC does not
      # exist.
      except TApplicationException:
        print_to_stderr("Error: Unable to communicate with impalad service. This "
               "service may not be an impalad instance. Check host:port and try again.")
        self.transport.close()
        raise
    except Exception, e:
      print_to_stderr("Error connecting: %s, %s" % (type(e).__name__, e))
      # If a connection to another impalad failed while already connected
      # reset the prompt to disconnected.
      self.prompt = self.DISCONNECTED_PROMPT

  def __get_transport(self):
    """Create a Transport.

       A non-kerberized impalad just needs a simple buffered transport. For
       the kerberized version, a sasl transport is created.

       If SSL is enabled, a TSSLSocket underlies the transport stack; otherwise a TSocket
       is used.
    """
    # sasl does not accept unicode strings, explicitly encode the string into ascii.
    host, port = self.impalad[0].encode('ascii', 'ignore'), int(self.impalad[1])
    if self.ssl_enabled:
      if self.ca_cert is None:
        # No CA cert means don't try to verify the certificate
        sock = TSSLSocket.TSSLSocket(host, port, validate=False)
      else:
        sock = TSSLSocket.TSSLSocket(host, port, validate=True, ca_certs=self.ca_cert)
    else:
      sock = TSocket(host, port)
    if not (self.use_ldap or self.use_kerberos):
      return TBufferedTransport(sock)
    # Initializes a sasl client
    def sasl_factory():
      sasl_client = sasl.Client()
      sasl_client.setAttr("host", host)
      if self.use_ldap:
        sasl_client.setAttr("username", self.user)
        sasl_client.setAttr("password", self.ldap_password)
      else:
        sasl_client.setAttr("service", self.kerberos_service_name)
      sasl_client.init()
      return sasl_client
    # GSSASPI is the underlying mechanism used by kerberos to authenticate.
    if self.use_kerberos:
      return TSaslClientTransport(sasl_factory, "GSSAPI", sock)
    else:
      return TSaslClientTransport(sasl_factory, "PLAIN", sock)


  def __get_sleep_interval(self, start_time):
    """Returns a step function of time to sleep in seconds before polling
    again. Maximum sleep is 1s, minimum is 0.1s"""
    elapsed = time.time() - start_time
    if elapsed < 10.0:
      return 0.1
    elif elapsed < 60.0:
      return 0.5

    return 1.0

  def __create_beeswax_query(self, query_str):
    """Create a beeswax query object from a query string"""
    query = BeeswaxService.Query()
    query.hadoop_user = self.user
    query.query = query_str
    query.configuration = self.__options_to_string_list()
    return query

  def __get_column_names(self):
    rpc_result = self.__do_rpc(
        lambda: self.imp_service.get_results_metadata(self.last_query_handle))
    metadata, _ = rpc_result.get_results()
    return [fs.name for fs in metadata.schema.fieldSchemas]

  def __construct_table_header(self, column_names):
    """ Constructs the table header for a given query handle.

    Should be called after the query has finished and before data is fetched. All data
    is left aligned.
    """
    table = ImpalaPrettyTable()
    for column in column_names:
      # Column names may be encoded as utf-8
      table.add_column(column.decode('utf-8', 'ignore'), [])
    table.align = "l"
    return table

  def __expect_result_metadata(self, query_str):
    """ Given a query string, return True if impalad expects result metadata"""
    excluded_query_types = ['use', 'alter', 'drop']
    if True in set(map(query_str.startswith, excluded_query_types)):
      return False
    return True

  def __execute_query(self, query, is_insert=False):
    self.__print_if_verbose("Query: %s" % (query.query,))
    start_time = time.time()
    rpc_result = self.__do_rpc(lambda: self.imp_service.query(query))
    self.last_query_handle, status = rpc_result.get_results()
    self.query_handle_closed = False

    if self.is_interrupted.isSet():
      if status == RpcStatus.OK:
        self.__close_query()
      return False
    if status != RpcStatus.OK:
      return False

    loop_start = time.time()
    while True:
      query_state = self.__get_query_state()
      if query_state == self.query_state["FINISHED"]:
        break
      elif query_state == self.query_state["EXCEPTION"]:
        if self.connected:
          self.__print_warning_log()
          # Close the query handle even if it's an insert.
          return self.__close_query()
        else:
          return False
      elif self.is_interrupted.isSet():
        return self.__close_query()
      time.sleep(self.__get_sleep_interval(loop_start))

    if is_insert:
      self.__print_warning_log()
      return self.__close_insert(query, start_time)
    return self.__fetch(query, start_time)

  def __fetch(self, query, start_time):
    """Wrapper around __fetch_internal that ensures that __print_warning_log() and
    __close_query() are called."""
    result = self.__fetch_internal(query, start_time)
    self.__print_warning_log()
    close_result = self.__close_query()
    return result and close_result

  def __fetch_internal(self, query, start_time):
    """Fetch all the results."""
    # impalad does not support the fetching of metadata for certain types of queries.
    if not self.__expect_result_metadata(query.query):
      return True

    # Results are ready, fetch them till they're done.
    column_names = self.__get_column_names()
    if self.write_delimited:
      formatter = DelimitedOutputFormatter(field_delim=self.output_delimiter)
      self.output_stream = OutputStream(formatter, filename=self.output_file)
      # print the column names
      if self.print_header:
        self.output_stream.write([column_names])
    else:
      prettytable = self.__construct_table_header(column_names)
      formatter = PrettyOutputFormatter(prettytable)
      self.output_stream = OutputStream(formatter, filename=self.output_file)

    result_rows = []
    num_rows_fetched = 0
    self.rpc_is_interruptable = True
    # If the user hit a Ctrl-C before rpc_is_interruptable is set, close the query.
    if self.is_interrupted.isSet():
      return False
    while True:
      # Fetch rows in batches of at most fetch_batch_size
      # Since a single call to fetch() can potentially take a long time, we execute this
      # rpc asynchronously to enable cancellation while it's in progress.
      rpc_result = self.__do_rpc(
        lambda: self.imp_service.fetch(self.last_query_handle, False,
          self.fetch_batch_size))
      result, status = rpc_result.get_results()
      # is_interrupted is set when the user has explicitly cancelled the fetch.
      # The query's already been closed, so there's no need to explicitly close it.
      if self.is_interrupted.isSet(): return False
      if status != RpcStatus.OK:
        return False

      num_rows_fetched += len(result.data)
      result_rows.extend(result.data)
      if len(result_rows) >= self.fetch_batch_size or not result.has_more:
        rows = [row.split('\t') for row in result_rows]
        self.output_stream.write(rows)
        result_rows = []
      if not result.has_more:
        break
    self.rpc_is_interruptable = False
    # Don't include the time to get the runtime profile or runtime state log in the query
    # execution time
    end_time = time.time()
    self.__print_runtime_profile_if_enabled()

    self.__print_if_verbose(
      "Returned %d row(s) in %2.2fs" % (num_rows_fetched, end_time - start_time))
    return True

  def __close_insert(self, query, start_time):
    """Fetches the results of an INSERT query"""
    rpc_result = self.__do_rpc(
        lambda: self.imp_service.CloseInsert(self.last_query_handle))
    insert_result, status = rpc_result.get_results()
    end_time = time.time()
    if status != RpcStatus.OK or self.is_interrupted.isSet():
      return False

    self.__print_runtime_profile_if_enabled()
    num_rows = sum([int(k) for k in insert_result.rows_appended.values()])
    self.__print_if_verbose("Inserted %d rows in %2.2fs" % (num_rows,
      end_time - start_time))
    return True

  def __close_query(self):
    """Close the query handle"""
    # Make closing a query handle idempotent
    if self.query_handle_closed:
      return True
    self.query_handle_closed = True
    rpc_result = self.__do_rpc(lambda: self.imp_service.close(self.last_query_handle))
    _, status = rpc_result.get_results()
    return status == RpcStatus.OK

  def __print_runtime_profile_if_enabled(self):
    if self.show_profiles:
      self.__print_runtime_profile()

  def __print_runtime_profile(self):
    profile = self.__get_runtime_profile()
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
    arg = arg.replace('\n', '')
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


  def __print_warning_log(self):
    rpc_result = self.__do_rpc(
        lambda: self.imp_service.get_log(self.last_query_handle.log_context))
    log, status = rpc_result.get_results()
    if status != RpcStatus.OK:
      print_to_stderr("Failed to get error log: %s" % status)
    if log and log.strip():
      print_to_stderr("WARNINGS: %s" % log)

  def do_alter(self, args):
    return self.__execute_query(self.__create_beeswax_query("alter %s" % args))

  def do_create(self, args):
    return self.__execute_query(self.__create_beeswax_query("create %s" % args))

  def do_drop(self, args):
    return self.__execute_query(self.__create_beeswax_query("drop %s" % args))

  def do_load(self, args):
    return self.__execute_query(self.__create_beeswax_query("load %s" % args))

  def do_profile(self, args):
    """Prints the runtime profile of the last INSERT or SELECT query executed."""
    if len(args) > 0:
      print_to_stderr("'profile' does not accept any arguments")
      return False
    elif self.last_query_handle is None:
      print_to_stderr('No previous query available to profile')
      return False
    self.__print_runtime_profile()
    return True

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    return self.__execute_query(self.__create_beeswax_query("select %s" % args))

  def do_values(self, args):
    """Executes a VALUES(...) query, fetching all rows"""
    return self.__execute_query(self.__create_beeswax_query("values %s" % args))

  def do_with(self, args):
    """Executes a query with a WITH clause, fetching all rows"""
    query = self.__create_beeswax_query("with %s" % args)
    # Set posix=True and add "'" to escaped quotes
    # to deal with escaped quotes in string literals
    lexer = shlex.shlex(query.query.lstrip(), posix=True)
    lexer.escapedquotes += "'"
    # Because the WITH clause may precede INSERT or SELECT queries,
    # just checking the first token is insufficient.
    is_insert = False
    tokens = list(lexer)
    if filter(self.INSERT_REGEX.match, tokens): is_insert = True
    return self.__execute_query(query, is_insert=is_insert)

  def do_use(self, args):
    """Executes a USE... query"""
    result = self.__execute_query(self.__create_beeswax_query("use %s" % args))
    if result: self.current_db = args
    return result

  def do_show(self, args):
    """Executes a SHOW... query, fetching all rows"""
    return self.__execute_query(self.__create_beeswax_query("show %s" % args))

  def do_describe(self, args):
    """Executes a DESCRIBE... query, fetching all rows"""
    return self.__execute_query(self.__create_beeswax_query("describe %s" % args))

  def do_desc(self, args):
    return self.do_describe(args)

  def do_insert(self, args):
    """Executes an INSERT query"""
    return self.__execute_query(self.__create_beeswax_query("insert %s" % args),
        is_insert=True)

  def __cancel_query(self):
    """Cancel a query on a keyboard interrupt from the shell."""
    print_to_stderr('Cancelling query ...')
    # Cancel sets query_state to EXCEPTION before calling cancel() in the
    # co-ordinator, so we don't need to wait.
    rpc_result = self.__do_rpc(lambda: self.imp_service.Cancel(self.last_query_handle))
    _, status = rpc_result.get_results()
    return status == RpcStatus.OK

  def __get_query_state(self):
    rpc_result = self.__do_rpc(
        lambda: self.imp_service.get_state(self.last_query_handle))
    state, status = rpc_result.get_results()
    if status != RpcStatus.OK:
      return self.query_state["EXCEPTION"]
    return state

  def __get_runtime_profile(self):
    rpc_result = self.__do_rpc(
        lambda: self.imp_service.GetRuntimeProfile(self.last_query_handle))
    profile, status = rpc_result.get_results()
    if status == RpcStatus.OK and profile:
      return profile

  def __get_summary(self):
    """Calls GetExecSummary() for the last query handle"""
    rpc_result = self.__do_rpc(
      lambda: self.imp_service.GetExecSummary(self.last_query_handle))
    summary, status = rpc_result.get_results()
    if status == RpcStatus.OK and summary:
      return summary
    return None

  def __do_rpc(self, rpc):
    """Creates a child thread which executes the provided callable.

    Blocks until the child thread terminates. Reads its results, if any,
    from a Queue object. The child thread puts its results in the Queue object
    upon completion. If the rpc being run is interruptable, then it returns the result
    queue without waiting for a result.

    """
    # The queue  is responsible for passing the rpc results from __do_rpc_thread
    # to __do_rpc.
    # TODO: Investigate whether this can be done without using a Queue object.
    rpc_results = Queue()
    rpc_thread = threading.Thread(target=self.__do_rpc_thread, args=[rpc, rpc_results])
    rpc_thread.start()
    if self.rpc_is_interruptable:
      return RpcResult(queue=rpc_results)
    else:
      rpc_thread.join()
      # The results should be in the queue. If they're not, return (None, RpcStatus.ERROR)
      try:
        result, status = rpc_results.get_nowait()
        rpc_result = RpcResult(result=result, status=status)
      except Empty:
        # Unexpected exception in __do_rpc_thread.
        print_to_stderr('Unexpected exception, no results returned.')
        rpc_result = RpcResult(result=None, status=RpcStatus.ERROR)
    return rpc_result

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
      return
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
    return self.__execute_query(self.__create_beeswax_query("explain %s" % args))

  def do_history(self, args):
    """Display command history"""
    # Deal with readline peculiarity. When history does not exists,
    # readline returns 1 as the history length and stores 'None' at index 0.
    if self.readline and self.readline.get_current_history_length() > 0:
      for index in xrange(1, self.readline.get_current_history_length() + 1):
        cmd = self.readline.get_history_item(index)
        print_to_stderr('[%d]: %s' % (index, cmd))
    else:
      print_to_stderr("The readline module was either not found or disabled. Command "
                      "history will not be collected.")
    return True

  def preloop(self):
    """Load the history file if it exists"""
    if self.readline:
      # The history file is created when the Impala shell is invoked and commands are
      # issued. In the first invocation of the shell, the history file will not exist.
      # Clearly, this is not an error, return.
      if not os.path.exists(self.history_file): return
      try:
        self.readline.read_history_file(self.history_file)
      except IOError, i:
        msg = "Unable to load command history (disabling history collection): %s" % i
        print_to_stderr(msg)
        # This history file exists but is not readable, disable readline.
        self.__disable_readline()

  def postloop(self):
    """Save session commands in history."""
    if self.readline:
      try:
        self.readline.write_history_file(self.history_file)
      except IOError, i:
        msg = "Unable to save command history (disabling history collection): %s" % i
        print_to_stderr(msg)
        # The history file is not writable, disable readline.
        self.__disable_readline()

  def default(self, args):
    return self.__execute_query(self.__create_beeswax_query(args))

  def emptyline(self):
    """If an empty line is entered, do nothing"""
    return True

  def do_version(self, args):
    """Prints the Impala build version"""
    print_to_stderr("Shell version: %s" % VERSION_STRING)
    print_to_stderr("Server version: %s" % self.server_version)
    return True

  def completenames(self, text, *ignored):
    """Make tab completion of commands case agnostic

    Override the superclass's completenames() method to support tab completion for
    upper case and mixed case commands.
    """
    cmd_names = [cmd for cmd in self.commands if cmd.startswith(text.lower())]
    # If the user input is upper case, return commands in upper case.
    if text.isupper(): return [cmd_names.upper() for cmd_names in cmd_names]
    # If the user input is lower case or mixed case, return lower case commands.
    return cmd_names


WELCOME_STRING = """Welcome to the Impala shell. Press TAB twice to see a list of \
available commands.

Copyright (c) 2012 Cloudera, Inc. All rights reserved.

(Shell build version: %s)""" % VERSION_STRING

def print_to_stderr(message):
  print >> sys.stderr, message

def parse_query_text(query_text, utf8_encode_policy='strict'):
  """Parse query file text, by stripping comments and encoding into utf-8"""
  return [ strip_comments_from_query(q).encode('utf-8', utf8_encode_policy)
           for q in sqlparse.split(query_text) ]

def strip_comments_from_query(query):
  """Strip comments from an individual query """
  # We only use the strip_comments filter, using other filters can lead to a significant
  # performance hit if the query is very large.
  return sqlparse.format(query, strip_comments=True)

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
                    dest="kerberos_service_name", default='impala',
                    help="Service name of a kerberized impalad, default is 'impala'")
  parser.add_option("-V", "--verbose", dest="verbose", default=True, action="store_true",
                    help="Verbose output, enabled by default")
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
  parser.add_option("-l", "--ldap", dest="use_ldap", default=False, action="store_true",
                    help="Use LDAP to authenticate with Impala. Impala must be configured"
                    " to allow LDAP authentication.")
  parser.add_option("-u", "--user", dest="user", default=getpass.getuser(),
                    help="User to authenticate with.")
  parser.add_option("--ssl", dest="ssl", default=False, action="store_true",
                    help="Connect to Impala via SSL-secured connection")
  parser.add_option("--ca_cert", dest="ca_cert", default=None, help=("Full path to "
                    "certificate file used to authenticate Impala's SSL certificate."
                    " May either be a copy of Impala's certificate (for self-signed "
                    "certs) or the certificate of a trusted third-party CA. If not set, "
                    "but SSL is enabled, the shell will NOT verify Impala's server "
                    "certificate"))
  parser.add_option("--strict_unicode", dest="strict_unicode", default=True,
                    action="store_true", help=("If true, non UTF-8 compatible input "
                    "characters are rejected by the shell. If false, such characters are"
                    " silently ignored."))
  options, args = parser.parse_args()

  # Arguments that could not be parsed are stored in args. Print an error and exit.
  if len(args) > 0:
    print_to_stderr('Error, could not parse arguments "%s"' % (' ').join(args))
    parser.print_help()
    sys.exit(1)

  if options.version:
    print VERSION_STRING
    sys.exit(0)

  if options.use_kerberos and options.use_ldap:
    print_to_stderr("Please specify at most one authentication mechanism (-k or -l)")
    sys.exit(1)

  if options.use_kerberos:
    print_to_stderr("Starting Impala Shell using Kerberos authentication")
    print_to_stderr("Using service name '%s'" % options.kerberos_service_name)
    # Check if the user has a ticket in the credentials cache
    try:
      if call(['klist', '-s']) != 0:
        print_to_stderr(("-k requires a valid kerberos ticket but no valid kerberos "
                         "ticket found."))
        sys.exit(1)
    except OSError, e:
      print_to_stderr('klist not found on the system, install kerberos clients')
      sys.exit(1)
  elif options.use_ldap:
    print_to_stderr("Starting Impala Shell using LDAP-based authentication")
  else:
    print_to_stderr("Starting Impala Shell without Kerberos authentication")

  if options.ssl:
    try:
      # TSSLSocket needs the ssl module, which may not be standard on all Operating
      # Systems. Only attempt to import TSSLSocket if the user wants an SSL connection.
      from thrift.transport import TSSLSocket
    except ImportError:
      print_to_stderr(("Unable to import the python 'ssl' module. It is required for an "
                       "SSL-secured connection."))
      sys.exit(1)
    if options.ca_cert is None:
      print_to_stderr("SSL is enabled. Impala server certificates will NOT be verified"\
                      " (set --ca_cert to change)")
    else:
      print_to_stderr("SSL is enabled")

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
