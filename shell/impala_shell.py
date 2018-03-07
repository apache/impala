#!/usr/bin/env python
#
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
# Impala's shell
import cmd
import errno
import getpass
import os
import prettytable
import random
import re
import shlex
import signal
import socket
import sqlparse
import subprocess
import sys
import textwrap
import time

from impala_client import (ImpalaClient, DisconnectedException, QueryStateException,
                           RPCException, TApplicationException,
                           QueryCancelledByShellException)
from impala_shell_config_defaults import impala_shell_defaults
from option_parser import get_option_parser, get_config_from_file
from shell_output import DelimitedOutputFormatter, OutputStream, PrettyOutputFormatter
from shell_output import OverwritingStdErrOutputStream
from subprocess import call
from thrift.Thrift import TException

VERSION_FORMAT = "Impala Shell v%(version)s (%(git_hash)s) built on %(build_date)s"
VERSION_STRING = "build version not available"
READLINE_UNAVAILABLE_ERROR = "The readline module was either not found or disabled. " \
                             "Command history will not be collected."

# Tarball / packaging build makes impala_build_version available
try:
  from impala_build_version import get_git_hash, get_build_date, get_version
  VERSION_STRING = VERSION_FORMAT % {'version': get_version(),
                                     'git_hash': get_git_hash()[:7],
                                     'build_date': get_build_date()}
except Exception:
  pass

class CmdStatus:
  """Values indicate the execution status of a command to the cmd shell driver module
  SUCCESS and ERROR continue running the shell and ABORT exits the shell
  Since SUCCESS == None, successful commands do not need to explicitly return
  anything on completion
  """
  SUCCESS = None
  ABORT = True
  ERROR = False

class ImpalaPrettyTable(prettytable.PrettyTable):
  """Patched version of PrettyTable that TODO"""
  def _unicode(self, value):
    if not isinstance(value, basestring):
      value = str(value)
    if not isinstance(value, unicode):
      # If a value cannot be encoded, replace it with a placeholder.
      value = unicode(value, self.encoding, "replace")
    return value

class QueryOptionLevels:
  """These are the levels used when displaying query options.
  The values correspond to the ones in TQueryOptionLevel"""
  REGULAR = 0
  ADVANCED = 1
  DEVELOPMENT = 2
  DEPRECATED = 3
  REMOVED = 4

class QueryOptionDisplayModes:
  REGULAR_OPTIONS_ONLY = 1
  ALL_OPTIONS = 2

class ImpalaShell(object, cmd.Cmd):
  """ Simple Impala Shell.

  Basic usage: type connect <host:port> to connect to an impalad
  Then issue queries or other commands. Tab-completion should show the set of
  available commands.
  Methods that implement shell commands return a boolean tuple (stop, status)
  stop is a flag the command loop uses to continue/discontinue the prompt.
  Status tells the caller that the command completed successfully.
  """

  # If not connected to an impalad, the server version is unknown.
  UNKNOWN_SERVER_VERSION = "Not Connected"
  DISCONNECTED_PROMPT = "[Not connected] > "
  UNKNOWN_WEBSERVER = "0.0.0.0"
  # Message to display in shell when cancelling a query
  CANCELLATION_MESSAGE = ' Cancelling Query'
  # Number of times to attempt cancellation before giving up.
  CANCELLATION_TRIES = 3
  # Commands are terminated with the following delimiter.
  CMD_DELIM = ';'
  # Valid variable name pattern
  VALID_VAR_NAME_PATTERN = r'[A-Za-z][A-Za-z0-9_]*'
  # Pattern for removal of comments preceding SET commands
  COMMENTS_BEFORE_SET_PATTERN = r'^(\s*/\*(.|\n)*?\*/|\s*--.*\n)*\s*((un)?set)'
  COMMENTS_BEFORE_SET_REPLACEMENT = r'\3'
  # Variable names are prefixed with the following string
  VAR_PREFIXES = [ 'VAR', 'HIVEVAR' ]
  DEFAULT_DB = 'default'
  # Regex applied to all tokens of a query to detect DML statements.
  DML_REGEX = re.compile("^(insert|upsert|update|delete)$", re.I)
  # Seperator for queries in the history file.
  HISTORY_FILE_QUERY_DELIM = '_IMP_DELIM_'

  VALID_SHELL_OPTIONS = {
    'LIVE_PROGRESS' : (lambda x: x in ("true", "TRUE", "True", "1"), "print_progress"),
    'LIVE_SUMMARY' : (lambda x: x in ("true", "TRUE", "True", "1"), "print_summary")
  }

  # Minimum time in seconds between two calls to get the exec summary.
  PROGRESS_UPDATE_INTERVAL = 1.0

  def __init__(self, options, query_options):
    cmd.Cmd.__init__(self)
    self.is_alive = True

    self.impalad = None
    self.use_kerberos = options.use_kerberos
    self.kerberos_service_name = options.kerberos_service_name
    self.use_ssl = options.ssl
    self.ca_cert = options.ca_cert
    self.user = options.user
    self.ldap_password = options.ldap_password
    self.use_ldap = options.use_ldap

    self.verbose = options.verbose
    self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    self.server_version = ImpalaShell.UNKNOWN_SERVER_VERSION
    self.webserver_address = ImpalaShell.UNKNOWN_WEBSERVER

    self.current_db = options.default_db
    self.history_file = os.path.expanduser("~/.impalahistory")
    # Stores the state of user input until a delimiter is seen.
    self.partial_cmd = str()
    # Stores the old prompt while the user input is incomplete.
    self.cached_prompt = str()

    self.show_profiles = options.show_profiles

    # Output formatting flags/options
    self.output_file = options.output_file
    self.output_delimiter = options.output_delimiter
    self.write_delimited = options.write_delimited
    self.print_header = options.print_header

    self.progress_stream = OverwritingStdErrOutputStream()

    self.set_query_options = query_options
    self.set_variables = options.variables

    self._populate_command_list()

    self.imp_client = None;
    self.orig_cmd = None

    # Tracks query handle of the last query executed. Used by the 'profile' command.
    self.last_query_handle = None;
    self.query_handle_closed = None

    self.print_summary = options.print_summary
    self.print_progress = options.print_progress

    self.ignore_query_failure = options.ignore_query_failure

    # Due to a readline bug in centos/rhel7, importing it causes control characters to be
    # printed. This breaks any scripting against the shell in non-interactive mode. Since
    # the non-interactive mode does not need readline - do not import it.
    if options.query or options.query_file:
      self.interactive = False
      self._disable_readline()
    else:
      self.interactive = True
      try:
        self.readline = __import__('readline')
        try:
          self.readline.set_history_length(int(options.history_max))
        except ValueError:
          print_to_stderr("WARNING: history_max option malformed %s\n"
            % options.history_max)
          self.readline.set_history_length(1000)
      except ImportError:
        self._disable_readline()

    if options.impalad is not None:
      self.do_connect(options.impalad)

    # We handle Ctrl-C ourselves, using an Event object to signal cancellation
    # requests between the handler and the main shell thread.
    signal.signal(signal.SIGINT, self._signal_handler)

  def _populate_command_list(self):
    """Populate a list of commands in the shell.

    Each command has its own method of the form do_<command>, and can be extracted by
    introspecting the class directory.
    """
    # Slice the command method name to get the name of the command.
    self.commands = [cmd[3:] for cmd in dir(self.__class__) if cmd.startswith('do_')]

  def _disable_readline(self):
    """Disables the readline module.

    The readline module is responsible for keeping track of command history.
    """
    self.readline = None

  def _print_options(self, print_mode):
    """Prints the current query options with default values distinguished from set values
    by brackets [], followed by shell-local options.
    The options are displayed in groups based on option levels received in parameter.
    Input parameter decides whether all groups or just the 'Regular' and 'Advanced'
    options are displayed."""
    print "Query options (defaults shown in []):"
    if not self.imp_client.default_query_options and not self.set_query_options:
      print '\tNo options available.'
    else:
      (regular_options, advanced_options, development_options, deprecated_options,
          removed_options) = self._get_query_option_grouping()
      self._print_option_group(regular_options)
      # If the shell is connected to an Impala that predates IMPALA-2181 then
      # the advanced_options would be empty and only the regular options would
      # be displayed.
      if advanced_options:
        print '\nAdvanced Query Options:'
        self._print_option_group(advanced_options)
      if print_mode == QueryOptionDisplayModes.ALL_OPTIONS:
        if development_options:
          print '\nDevelopment Query Options:'
          self._print_option_group(development_options)
        if deprecated_options:
          print '\nDeprecated Query Options:'
          self._print_option_group(deprecated_options)
    self._print_shell_options()

  def _get_query_option_grouping(self):
    """For all the query options received through rpc this function determines the
    query option level for display purposes using the received query_option_levels
    parameters.
    If the option level can't be determined then it defaults to 'REGULAR'"""
    (regular_options, advanced_options, development_options, deprecated_options,
        removed_options) = {}, {}, {}, {}, {}
    for option_name, option_value in self.imp_client.default_query_options.iteritems():
      level = self.imp_client.query_option_levels.get(option_name,
                                                      QueryOptionLevels.REGULAR)
      if level == QueryOptionLevels.REGULAR:
        regular_options[option_name] = option_value
      elif level == QueryOptionLevels.DEVELOPMENT:
        development_options[option_name] = option_value
      elif level == QueryOptionLevels.DEPRECATED:
        deprecated_options[option_name] = option_value
      elif level == QueryOptionLevels.REMOVED:
        removed_options[option_name] = option_value
      else:
        advanced_options[option_name] = option_value
    return (regular_options, advanced_options, development_options, deprecated_options,
        removed_options)

  def _print_option_group(self, query_options):
    """Gets query options and prints them. Value is inside [] for the ones having
    default values.
    query_options parameter is a subset of the default_query_options map"""
    for option_name in sorted(query_options):
      if (option_name in self.set_query_options and
          self.set_query_options[option_name] != query_options[option_name]):
        print '\n'.join(["\t%s: %s" % (option_name, self.set_query_options[option_name])])
      else:
        print '\n'.join(["\t%s: [%s]" % (option_name, query_options[option_name])])

  def _print_variables(self):
    # Prints the currently defined variables.
    if not self.set_variables:
      print '\tNo variables defined.'
    else:
      for k in sorted(self.set_variables):
        print '\n'.join(["\t%s: %s" % (k, self.set_variables[k])])

  def _print_shell_options(self):
    """Prints shell options, which are local and independent of query options."""
    print "\nShell Options"
    for x in self.VALID_SHELL_OPTIONS:
      print "\t%s: %s" % (x, self.__dict__[self.VALID_SHELL_OPTIONS[x][1]])

  def _create_beeswax_query(self, args):
    """Original command should be stored before running the method. The method is usually
    used in do_* methods and the command is kept at precmd()."""
    command = self.orig_cmd
    self.orig_cmd = None
    if not command:
      print_to_stderr("Unexpected error: Failed to execute query due to command "\
                      "is missing")
      sys.exit(1)
    return self.imp_client.create_beeswax_query("%s %s" % (command, args),
                                                 self.set_query_options)

  def do_shell(self, args):
    """Run a command on the shell
    Usage: shell <cmd>
           ! <cmd>

    """
    try:
      start_time = time.time()
      os.system(args)
      self._print_if_verbose("--------\nExecuted in %2.2fs" % (time.time() - start_time))
    except Exception, e:
      print_to_stderr('Error running command : %s' % e)
      return CmdStatus.ERROR

  def _remove_comments_before_set(self, line):
    """SET commands preceded by a comment become a SET query, which are not processed
       locally. SET VAR:* commands must be processed locally, since they are not known
       to the frontend. Thus, we remove comments that precede SET commands to enforce the
       local processing."""
    regexp = re.compile(ImpalaShell.COMMENTS_BEFORE_SET_PATTERN, re.IGNORECASE)
    return regexp.sub(ImpalaShell.COMMENTS_BEFORE_SET_REPLACEMENT, line, 1)

  def sanitise_input(self, args):
    # A command terminated by a semi-colon is legal. Check for the trailing
    # semi-colons and strip them from the end of the command.
    if not self.interactive:
      # Strip all the non-interactive commands of the delimiter.
      args = self._remove_comments_before_set(args)
      tokens = args.strip().split(' ')
      return ' '.join(tokens).rstrip(ImpalaShell.CMD_DELIM)
    # Handle EOF if input is interactive
    tokens = args.strip().split(' ')
    if tokens[0].lower() == 'eof':
      if not self.partial_cmd:
        # The first token is the command.
        # If it's EOF, call do_quit()
        return 'quit'
      else:
        # If a command is in progress and the user hits a Ctrl-D, clear its state
        # and reset the prompt.
        self.prompt = self.cached_prompt
        self.partial_cmd = str()
        # The print statement makes the new prompt appear in a new line.
        # Also print an extra newline to indicate that the current command has
        # been cancelled.
        print '\n'
        return str()
    args = self._check_for_command_completion(args)
    args = self._remove_comments_before_set(args)
    tokens = args.strip().split(' ')
    args = ' '.join(tokens).strip()
    return args.rstrip(ImpalaShell.CMD_DELIM)

  def _shlex_split(self, line):
    """Reimplement shlex.split() so that escaped single quotes
    are actually escaped. shlex.split() only escapes double quotes
    by default. This method will throw a ValueError if an open
    quotation (either single or double) is found.
    """
    my_split = shlex.shlex(line, posix=True)
    my_split.escapedquotes = '"\''
    my_split.whitespace_split = True
    my_split.commenters = ''
    return list(my_split)

  def _cmd_ends_with_delim(self, line):
    """Check if the input command ends with a command delimiter.

    A command ending with the delimiter and containing an open quotation character is
    not considered terminated. If no open quotation is found, it's considered
    terminated.
    """
    # Strip any comments to make a statement such as the following be considered as
    # ending with a delimiter:
    # select 1 + 1; -- this is a comment
    line = sqlparse.format(line, strip_comments=True).rstrip()
    if line.endswith(ImpalaShell.CMD_DELIM):
      try:
        # Look for an open quotation in the entire command, and not just the
        # current line.
        if self.partial_cmd: line = '%s %s' % (self.partial_cmd, line)
        self._shlex_split(line)
        return True
      # If the command ends with a delimiter, check if it has an open quotation.
      # shlex in self._split() throws a ValueError iff an open quotation is found.
      # A quotation can either be a single quote or a double quote.
      except ValueError:
        pass

    # This checks to see if there are any backslashed quotes
    # outside of quotes, since backslashed quotes
    # outside of single or double quotes should not be escaped.
    # Ex. 'abc\'xyz' -> closed because \' is escaped
    #     \'abcxyz   -> open because \' is not escaped
    #     \'abcxyz'  -> closed
    # Iterate through the line and switch the state if a single or double quote is found
    # and ignore escaped single and double quotes if the line is considered open (meaning
    # a previous single or double quote has not been closed yet)
      state_closed = True;
      opener = None;
      for i, char in enumerate(line):
        if state_closed and (char in ['\'', '\"']):
          state_closed = False
          opener = char
        elif not state_closed and opener == char:
          if line[i - 1] != '\\':
            state_closed = True
            opener = None;

      return state_closed

    return False

  def _check_for_command_completion(self, cmd):
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
    if self.readline: current_history_len = self.readline.get_current_history_length()
    # Input is incomplete, store the contents and do nothing.
    if not self._cmd_ends_with_delim(cmd):
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
      completed_cmd = sqlparse.format(completed_cmd)
      if self.readline and current_history_len > 0:
        self.readline.replace_history_item(current_history_len - 1,
            completed_cmd.encode('utf-8'))
      # Revert the prompt to its earlier state
      self.prompt = self.cached_prompt
    else:  # Input has a delimiter and partial_cmd is empty
      completed_cmd = sqlparse.format(cmd)
    return completed_cmd

  def _new_impala_client(self):
    return ImpalaClient(self.impalad, self.use_kerberos,
                        self.kerberos_service_name, self.use_ssl,
                        self.ca_cert, self.user, self.ldap_password,
                        self.use_ldap)

  def _signal_handler(self, signal, frame):
    """Handles query cancellation on a Ctrl+C event"""
    if self.last_query_handle is None or self.query_handle_closed:
      return
    # Create a new connection to the impalad and cancel the query.
    for cancel_try in xrange(ImpalaShell.CANCELLATION_TRIES):
      try:
        self.imp_client.is_query_cancelled = True
        self.query_handle_closed = True
        print_to_stderr(ImpalaShell.CANCELLATION_MESSAGE)
        new_imp_client = self._new_impala_client()
        new_imp_client.connect()
        new_imp_client.cancel_query(self.last_query_handle, False)
        self.imp_client.close_query(self.last_query_handle)
        break
      except Exception, e:
        # Suppress harmless errors.
        err_msg = str(e).strip()
        if err_msg in ['ERROR: Cancelled', 'ERROR: Invalid or unknown query handle']:
          break
        print_to_stderr("Failed to reconnect and close (try %i/%i): %s" % (
            cancel_try + 1, ImpalaShell.CANCELLATION_TRIES, err_msg))

  def _replace_variables(self, query):
    """Replaces variable within the query text with their corresponding values"""
    errors = False
    matches = set(map(lambda v: v.upper(), re.findall(r'(?<!\\)\${([^}]+)}', query)))
    for name in matches:
      value = None
      # Check if syntax is correct
      var_name = self._get_var_name(name)
      if var_name is None:
        print_to_stderr('Error: Unknown substitution syntax (%s). ' % (name,) + \
                        'Use ${VAR:var_name}.')
        errors = True
      else:
        # Replaces variable value
        if self.set_variables and var_name in self.set_variables:
          value = self.set_variables[var_name]
          regexp = re.compile(r'(?<!\\)\${%s}' % (name,), re.IGNORECASE)
          query = regexp.sub(value, query)
        else:
          print_to_stderr('Error: Unknown variable %s' % (var_name))
          errors = True
    if errors:
      return None
    else:
      return query

  def precmd(self, args):
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
    try:
      self.imp_client.test_connection()
    except TException:
      print_to_stderr("Connection lost, reconnecting...")
      self._connect()
      self._validate_database(immediately=True)
    return args.encode('utf-8')

  def onecmd(self, line):
    """Overridden to ensure the variable replacement is processed in interactive
       as well as non-interactive mode, since the precmd method would only work for
       the interactive case, when cmdloop is called.
    """
    # Replace variables in the statement before it's executed
    line = self._replace_variables(line)
    # Cmd is an old-style class, hence we need to call the method directly
    # instead of using super()
    # TODO: This may have to be changed to a super() call once we move to Python 3
    if line == None:
      return CmdStatus.ERROR
    else:
      # This code is based on the code from the standard Python library package cmd.py:
      # https://github.com/python/cpython/blob/master/Lib/cmd.py#L192
      # One change is lowering command before getting a function. The lowering
      # is necessary to find a proper function and here is a right place
      # because the lowering command in front of the finding can avoid a
      # side effect.
      command, arg, line = self.parseline(line)
      if not line:
        return self.emptyline()
      if command is None:
        return self.default(line)
      self.lastcmd = line
      if line == 'EOF' :
        self.lastcmd = ''
      if command == '':
        return self.default(line)
      else:
        try:
          func = getattr(self, 'do_' + command.lower())
          self.orig_cmd = command
        except AttributeError:
          return self.default(line)
        return func(arg)

  def postcmd(self, status, args):
    # status conveys to shell how the shell should continue execution
    # should always be a CmdStatus
    return status

  def do_summary(self, args):
    summary = None
    try:
      summary = self.imp_client.get_summary(self.last_query_handle)
    except RPCException, e:
      import re
      error_pattern = re.compile("ERROR: Query id \d+:\d+ not found.")
      if error_pattern.match(e.value):
        print_to_stderr("Could not retrieve summary for query.")
      else:
        print_to_stderr(e)
      return CmdStatus.ERROR
    if summary.nodes is None:
      print_to_stderr("Summary not available")
      return CmdStatus.SUCCESS
    output = []
    table = self._default_summary_table()
    self.imp_client.build_summary_table(summary, 0, False, 0, False, output)
    formatter = PrettyOutputFormatter(table)
    self.output_stream = OutputStream(formatter, filename=self.output_file)
    self.output_stream.write(output)

  def _handle_shell_options(self, token, value):
    try:
      handle = self.VALID_SHELL_OPTIONS[token]
      self.__dict__[handle[1]] = handle[0](value)
      return True
    except KeyError:
      return False

  def _get_var_name(self, name):
    """Look for a namespace:var_name pattern in an option name.
       Return the variable name if it's a match or None otherwise.
    """
    ns_match = re.match(r'^([^:]*):(.*)', name)
    if ns_match is not None:
      ns = ns_match.group(1)
      var_name = ns_match.group(2)
      if ns in ImpalaShell.VAR_PREFIXES:
        return var_name
    return None

  def _print_with_set(self, print_level):
    self._print_options(print_level)
    print "\nVariables:"
    self._print_variables()

  def do_set(self, args):
    """Set or display query options.

    Display query options:
    Usage: SET (to display the Regular options) or
           SET ALL (to display all the options)
    Set query options:
    Usage: SET <option>=<value>
           OR
           SET VAR:<variable>=<value>

    """
    # TODO: Expand set to allow for setting more than just query options.
    if len(args) == 0:
      self._print_with_set(QueryOptionDisplayModes.REGULAR_OPTIONS_ONLY)
      return CmdStatus.SUCCESS

    # Remove any extra spaces surrounding the tokens.
    # Allows queries that have spaces around the = sign.
    tokens = [arg.strip() for arg in args.split("=")]
    if len(tokens) != 2:
      if len(tokens) == 1 and tokens[0].upper() == "ALL":
        self._print_with_set(QueryOptionDisplayModes.ALL_OPTIONS)
        return CmdStatus.SUCCESS
      else:
        print_to_stderr("Error: SET <option>=<value>")
        print_to_stderr("       OR")
        print_to_stderr("       SET VAR:<variable>=<value>")
        return CmdStatus.ERROR
    option_upper = tokens[0].upper()
    # Check if it's a variable
    var_name = self._get_var_name(option_upper)
    if var_name is not None:
      # Set the variable
      self.set_variables[var_name] = tokens[1]
      self._print_if_verbose('Variable %s set to %s' % (var_name, tokens[1]))
    elif not self._handle_shell_options(option_upper, tokens[1]):
      if option_upper not in self.imp_client.default_query_options.keys():
        print "Unknown query option: %s" % (tokens[0])
        print "Available query options, with their values (defaults shown in []):"
        self._print_options(QueryOptionDisplayModes.REGULAR_OPTIONS_ONLY)
        return CmdStatus.ERROR
      if self.imp_client.query_option_levels[option_upper] == QueryOptionLevels.REMOVED:
        self._print_if_verbose("Ignoring removed query option: '{0}'".format(tokens[0]))
        return CmdStatus.SUCCESS
      self.set_query_options[option_upper] = tokens[1]
      self._print_if_verbose('%s set to %s' % (option_upper, tokens[1]))

  def do_unset(self, args):
    """Unset a query option"""
    if len(args.split()) != 1:
      print 'Usage: unset <option>'
      return CmdStatus.ERROR
    option = args.upper()
    # Check if it's a variable
    var_name = self._get_var_name(option)
    if var_name is not None:
      if self.set_variables.get(var_name):
        print 'Unsetting variable %s' % var_name
        del self.set_variables[var_name]
      else:
        print "No variable called %s is set" % var_name
    elif self.set_query_options.get(option):
      print 'Unsetting option %s' % option
      del self.set_query_options[option]
    else:
      print "No option called %s is set" % option

  def do_quit(self, args):
    """Quit the Impala shell"""
    self._print_if_verbose("Goodbye " + self.user)
    self.is_alive = False
    return CmdStatus.ABORT

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
    # specified. Connecting to a kerberized impalad requires an fqdn as the host name.
    if self.use_ldap and self.ldap_password is None:
      self.ldap_password = getpass.getpass("LDAP password for %s: " % self.user)

    if not args: args = socket.getfqdn()
    tokens = args.split(" ")
    # validate the connection string.
    host_port = [val for val in tokens[0].split(':') if val.strip()]
    if (':' in tokens[0] and len(host_port) != 2):
      print_to_stderr("Connection string must either be empty, or of the form "
                      "<hostname[:port]>")
      return CmdStatus.ERROR
    elif len(host_port) == 1:
      host_port.append('21000')
    self.impalad = tuple(host_port)
    if self.imp_client: self.imp_client.close_connection()
    self.imp_client = self._new_impala_client()
    self._connect()
    # If the connection fails and the Kerberos has not been enabled,
    # check for a valid kerberos ticket and retry the connection
    # with kerberos enabled.
    if not self.imp_client.connected and not self.use_kerberos:
      try:
        if call(["klist", "-s"]) == 0:
          print_to_stderr("Kerberos ticket found in the credentials cache, retrying "
                          "the connection with a secure transport.")
          self.use_kerberos = True
          self.use_ldap = False
          self.ldap_password = None
          self.imp_client = self._new_impala_client()
          self._connect()
      except OSError, e:
        pass

    if self.imp_client.connected:
      self._print_if_verbose('Connected to %s:%s' % self.impalad)
      self._print_if_verbose('Server version: %s' % self.server_version)
      self.prompt = "[%s:%s] > " % self.impalad
      self._validate_database()
    try:
      self.imp_client.build_default_query_options_dict()
    except RPCException, e:
      print_to_stderr(e)
    # In the case that we lost connection while a command was being entered,
    # we may have a dangling command, clear partial_cmd
    self.partial_cmd = str()
    # Check if any of query options set by the user are inconsistent
    # with the impalad being connected to

    # Use a temporary to avoid changing set_query_options during iteration.
    new_query_options = {}
    for set_option, value in self.set_query_options.iteritems():
      if set_option not in set(self.imp_client.default_query_options):
        print ('%s is not supported for the impalad being '
               'connected to, ignoring.' % set_option)
      else:
        new_query_options[set_option] = value
    self.set_query_options = new_query_options

  def _connect(self):
    try:
      result = self.imp_client.connect()
      self.server_version = result.version
      self.webserver_address = result.webserver_address
    except TApplicationException:
      # We get a TApplicationException if the transport is valid,
      # but the RPC does not exist.
      print_to_stderr("Error: Unable to communicate with impalad service. This "
               "service may not be an impalad instance. Check host:port and try again.")
      self.imp_client.close_connection()
      raise
    except ImportError:
      print_to_stderr("Unable to import the python 'ssl' module. It is"
      " required for an SSL-secured connection.")
      sys.exit(1)
    except socket.error, (code, e):
      # if the socket was interrupted, reconnect the connection with the client
      if code == errno.EINTR:
        self._reconnect_cancellation()
      else:
        print_to_stderr("Socket error %s: %s" % (code, e))
        self.prompt = self.DISCONNECTED_PROMPT
    except Exception, e:
      print_to_stderr("Error connecting: %s, %s" % (type(e).__name__, e))
      # A secure connection may still be open. So we explicitly close it.
      self.imp_client.close_connection()
      # If a connection to another impalad failed while already connected
      # reset the prompt to disconnected.
      self.server_version = self.UNKNOWN_SERVER_VERSION
      self.prompt = self.DISCONNECTED_PROMPT

  def _reconnect_cancellation(self):
    self._connect()
    self._validate_database()

  def _validate_database(self, immediately=False):
    """ Issues a "USE <db>" command where <db> is the current database.
    It is typically needed after the connection is (re-)established to the Impala daemon.

    If immediately is False, it appends the USE command to self.cmdqueue.
    If immediately is True, it executes the USE command right away.
    """
    if self.current_db:
      self.current_db = self.current_db.strip('`')
      use_current_db = ('use `%s`' % self.current_db)

      if immediately:
        self.onecmd(use_current_db)
      else:
        self.cmdqueue.append(use_current_db + ImpalaShell.CMD_DELIM)

  def _print_if_verbose(self, message):
    if self.verbose:
      print_to_stderr(message)

  def print_runtime_profile(self, profile, status=False):
    if self.show_profiles or status:
      if profile is not None:
        print "Query Runtime Profile:\n" + profile

  def _parse_table_name_arg(self, arg):
    """ Parses an argument string and returns the result as a db name, table name combo.

    If the table name was not fully qualified, the current database is returned as the db.
    Otherwise, the table is split into db/table name parts and returned.
    If an invalid format is provided, None is returned.
    """
    if not arg: return
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

  def do_alter(self, args):
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_create(self, args):
    # We want to print the webserver link only for CTAS queries.
    print_web_link = "select" in args
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query, print_web_link=print_web_link)

  def do_drop(self, args):
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_load(self, args):
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_profile(self, args):
    """Prints the runtime profile of the last DML statement or SELECT query executed."""
    if len(args) > 0:
      print_to_stderr("'profile' does not accept any arguments")
      return CmdStatus.ERROR
    elif self.last_query_handle is None:
      print_to_stderr('No previous query available to profile')
      return CmdStatus.ERROR
    profile = self.imp_client.get_runtime_profile(self.last_query_handle)
    return self.print_runtime_profile(profile, True)

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query, print_web_link=True)

  def do_compute(self, args):
    """Executes a COMPUTE STATS query.
    Impala shell cannot get child query handle so it cannot
    query live progress for COMPUTE STATS query. Disable live
    progress/summary callback for COMPUTE STATS query."""
    query = self._create_beeswax_query(args)
    (prev_print_progress, prev_print_summary) = self.print_progress, self.print_summary
    (self.print_progress, self.print_summary) = False, False;
    try:
      ret = self._execute_stmt(query)
    finally:
      (self.print_progress, self.print_summary) = prev_print_progress, prev_print_summary
    return ret

  def _format_outputstream(self):
    column_names = self.imp_client.get_column_names(self.last_query_handle)
    if self.write_delimited:
      formatter = DelimitedOutputFormatter(field_delim=self.output_delimiter)
      self.output_stream = OutputStream(formatter, filename=self.output_file)
      # print the column names
      if self.print_header:
        self.output_stream.write([column_names])
    else:
      prettytable = self.construct_table_with_header(column_names)
      formatter = PrettyOutputFormatter(prettytable)
      self.output_stream = OutputStream(formatter, filename=self.output_file)

  def _periodic_wait_callback(self):
    """If enough time elapsed since the last call to the periodic callback,
    execute the RPC to get the query exec summary and depending on the set options
    print either the progress or the summary or both to stderr.
    """
    if not self.print_progress and not self.print_summary: return

    checkpoint = time.time()
    if checkpoint - self.last_summary > self.PROGRESS_UPDATE_INTERVAL:
      summary = self.imp_client.get_summary(self.last_query_handle)
      if summary and summary.progress:
        progress = summary.progress

        # If the data is not complete return and wait for a good result.
        if not progress.total_scan_ranges and not progress.num_completed_scan_ranges:
          self.last_summary = time.time()
          return

        data = ""
        if self.print_progress and progress.total_scan_ranges > 0:
          val = ((summary.progress.num_completed_scan_ranges * 100) /
                 summary.progress.total_scan_ranges)
          fragment_text = "[%s%s] %s%%\n" % ("#" * val, " " * (100 - val), val)
          data += fragment_text

        if self.print_summary:
          table = self._default_summary_table()
          output = []
          self.imp_client.build_summary_table(summary, 0, False, 0, False, output)
          formatter = PrettyOutputFormatter(table)
          data += formatter.format(output) + "\n"

        self.progress_stream.write(data)
      self.last_summary = time.time()

  def _default_summary_table(self):
    return self.construct_table_with_header(["Operator", "#Hosts", "Avg Time", "Max Time",
                                             "#Rows", "Est. #Rows", "Peak Mem",
                                             "Est. Peak Mem", "Detail"])

  def _execute_stmt(self, query, is_dml=False, print_web_link=False):
    """ The logic of executing any query statement

    The client executes the query and the query_handle is returned immediately,
    even as the client waits for the query to finish executing.

    If the query was not dml, the results are fetched from the client
    as they are streamed in, through the use of a generator.

    The execution time is printed and the query is closed if it hasn't been already
    """

    self._print_if_verbose("Query: %s" % query.query)
    # TODO: Clean up this try block and refactor it (IMPALA-3814)
    try:
      if self.webserver_address == ImpalaShell.UNKNOWN_WEBSERVER:
        print_web_link = False
      if print_web_link:
        self._print_if_verbose("Query submitted at: %s (Coordinator: %s)" %
            (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            self.webserver_address))

      start_time = time.time()
      self.last_query_handle = self.imp_client.execute_query(query)
      self.query_handle_closed = False
      self.last_summary = time.time()
      if print_web_link:
        self._print_if_verbose(
            "Query progress can be monitored at: %s/query_plan?query_id=%s" %
            (self.webserver_address, self.last_query_handle.id))

      wait_to_finish = self.imp_client.wait_to_finish(self.last_query_handle,
          self._periodic_wait_callback)
      # Reset the progress stream.
      self.progress_stream.clear()

      if is_dml:
        # retrieve the error log
        warning_log = self.imp_client.get_warning_log(self.last_query_handle)
        (num_rows, num_row_errors) = self.imp_client.close_dml(self.last_query_handle)
      else:
        # impalad does not support the fetching of metadata for certain types of queries.
        if not self.imp_client.expect_result_metadata(query.query):
          # Close the query
          self.imp_client.close_query(self.last_query_handle)
          self.query_handle_closed = True
          return CmdStatus.SUCCESS

        self._format_outputstream()
        # fetch returns a generator
        rows_fetched = self.imp_client.fetch(self.last_query_handle)
        num_rows = 0

        for rows in rows_fetched:
          # IMPALA-4418: Break out of the loop to prevent printing an unnecessary empty line.
          if len(rows) == 0:
            break
          self.output_stream.write(rows)
          num_rows += len(rows)

        # retrieve the error log
        warning_log = self.imp_client.get_warning_log(self.last_query_handle)

      end_time = time.time()

      if warning_log:
        self._print_if_verbose(warning_log)
      # print 'Modified' when is_dml is true (i.e. 1), or 'Fetched' otherwise.
      verb = ["Fetched", "Modified"][is_dml]
      time_elapsed = end_time - start_time

      # Add the number of row errors if this DML and the operation supports it.
      # num_row_errors is None if the DML operation doesn't return it.
      if is_dml and num_row_errors is not None:
        error_report = ", %d row error(s)" % (num_row_errors)
      else:
        error_report = ""

      self._print_if_verbose("%s %d row(s)%s in %2.2fs" %\
          (verb, num_rows, error_report, time_elapsed))

      if not is_dml:
        self.imp_client.close_query(self.last_query_handle, self.query_handle_closed)
      self.query_handle_closed = True
      try:
        profile = self.imp_client.get_runtime_profile(self.last_query_handle)
        self.print_runtime_profile(profile)
      except RPCException, e:
        if self.show_profiles: raise e
      return CmdStatus.SUCCESS
    except QueryCancelledByShellException, e:
      return CmdStatus.SUCCESS
    except RPCException, e:
      # could not complete the rpc successfully
      print_to_stderr(e)
    except QueryStateException, e:
      # an exception occurred while executing the query
      self.imp_client.close_query(self.last_query_handle, self.query_handle_closed)
      print_to_stderr(e)
    except DisconnectedException, e:
      # the client has lost the connection
      print_to_stderr(e)
      self.imp_client.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    except socket.error, (code, e):
      # if the socket was interrupted, reconnect the connection with the client
      if code == errno.EINTR:
        print ImpalaShell.CANCELLATION_MESSAGE
        self._reconnect_cancellation()
      else:
        print_to_stderr("Socket error %s: %s" % (code, e))
        self.prompt = self.DISCONNECTED_PROMPT
        self.imp_client.connected = False
    except Exception, u:
      # if the exception is unknown, there was possibly an issue with the connection
      # set the shell as disconnected
      print_to_stderr('Unknown Exception : %s' % (u,))
      self.imp_client.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    return CmdStatus.ERROR

  def construct_table_with_header(self, column_names):
    """ Constructs the table header for a given query handle.

    Should be called after the query has finished and before data is fetched.
    All data is left aligned.
    """
    table = ImpalaPrettyTable()
    for column in column_names:
      # Column names may be encoded as utf-8
      table.add_column(column.decode('utf-8', 'ignore'), [])
    table.align = "l"
    return table

  def do_values(self, args):
    """Executes a VALUES(...) query, fetching all rows"""
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_with(self, args):
    """Executes a query with a WITH clause, fetching all rows"""
    query = self._create_beeswax_query(args)
    # Set posix=True and add "'" to escaped quotes
    # to deal with escaped quotes in string literals
    lexer = shlex.shlex(query.query.lstrip(), posix=True)
    lexer.escapedquotes += "'"
    # Because the WITH clause may precede DML or SELECT queries,
    # just checking the first token is insufficient.
    is_dml = False
    tokens = list(lexer)
    if filter(self.DML_REGEX.match, tokens): is_dml = True
    return self._execute_stmt(query, is_dml=is_dml, print_web_link=True)

  def do_use(self, args):
    """Executes a USE... query"""
    query = self._create_beeswax_query(args)
    if self._execute_stmt(query) is CmdStatus.SUCCESS:
      self.current_db = args
    else:
      return CmdStatus.ERROR

  def do_show(self, args):
    """Executes a SHOW... query, fetching all rows"""
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_describe(self, args):
    """Executes a DESCRIBE... query, fetching all rows"""
    # original command should be overridden because the server cannot
    # recognize "desc" as a keyword. Thus, given command should be
    # replaced with "describe" here.
    self.orig_cmd = "describe"
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_desc(self, args):
    return self.do_describe(args)

  def __do_dml(self, args):
    """Executes a DML query"""
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query, is_dml=True, print_web_link=True)

  def do_upsert(self, args):
    return self.__do_dml(args)

  def do_update(self, args):
    return self.__do_dml(args)

  def do_delete(self, args):
    return self.__do_dml(args)

  def do_insert(self, args):
    return self.__do_dml(args)

  def do_explain(self, args):
    """Explain the query execution plan"""
    query = self._create_beeswax_query(args)
    return self._execute_stmt(query)

  def do_history(self, args):
    """Display command history"""
    # Deal with readline peculiarity. When history does not exist,
    # readline returns 1 as the history length and stores 'None' at index 0.
    if self.readline and self.readline.get_current_history_length() > 0:
      for index in xrange(1, self.readline.get_current_history_length() + 1):
        cmd = self.readline.get_history_item(index)
        print_to_stderr('[%d]: %s' % (index, cmd))
    else:
      print_to_stderr(READLINE_UNAVAILABLE_ERROR)

  def do_rerun(self, args):
    """Rerun a command with an command index in history
    Example: @1;
    """
    history_len = self.readline.get_current_history_length()
    # Rerun command shouldn't appear in history
    self.readline.remove_history_item(history_len - 1)
    history_len -= 1
    if not self.readline:
      print_to_stderr(READLINE_UNAVAILABLE_ERROR)
      return CmdStatus.ERROR
    try:
      cmd_idx = int(args)
    except ValueError:
      print_to_stderr("Command index to be rerun must be an integer.")
      return CmdStatus.ERROR
    if not (0 < cmd_idx <= history_len or -history_len <= cmd_idx < 0):
      print_to_stderr("Command index out of range. Valid range: [1, {0}] and [-{0}, -1]"
                      .format(history_len))
      return CmdStatus.ERROR
    if cmd_idx < 0:
      cmd_idx += history_len + 1
    cmd = self.readline.get_history_item(cmd_idx)
    print_to_stderr("Rerunning " + cmd)
    return self.onecmd(cmd.rstrip(";"))

  def do_tip(self, args):
    """Print a random tip"""
    print_to_stderr(random.choice(TIPS))

  def do_src(self, args):
    return self.do_source(args)

  def do_source(self, args):
    try:
      cmd_file = open(args, "r")
    except Exception, e:
      print_to_stderr("Error opening file '%s': %s" % (args, e))
      return CmdStatus.ERROR
    if self.execute_query_list(parse_query_text(cmd_file.read())):
      return CmdStatus.SUCCESS
    else:
      return CmdStatus.ERROR

  def preloop(self):
    """Load the history file if it exists"""
    if self.readline:
      # The history file is created when the Impala shell is invoked and commands are
      # issued. In the first invocation of the shell, the history file will not exist.
      # Clearly, this is not an error, return.
      if not os.path.exists(self.history_file): return
      try:
        self.readline.read_history_file(self.history_file)
        self._replace_history_delimiters(ImpalaShell.HISTORY_FILE_QUERY_DELIM, '\n')
      except IOError, i:
        msg = "Unable to load command history (disabling history collection): %s" % i
        print_to_stderr(msg)
        # This history file exists but is not readable, disable readline.
        self._disable_readline()

  def postloop(self):
    """Save session commands in history."""
    if self.readline:
      try:
        self._replace_history_delimiters('\n', ImpalaShell.HISTORY_FILE_QUERY_DELIM)
        self.readline.write_history_file(self.history_file)
      except IOError, i:
        msg = "Unable to save command history (disabling history collection): %s" % i
        print_to_stderr(msg)
        # The history file is not writable, disable readline.
        self._disable_readline()

  def parseline(self, line):
    """Parse the line into a command name and a string containing
    the arguments.  Returns a tuple containing (command, args, line).
    'command' and 'args' may be None if the line couldn't be parsed.
    'line' in return tuple is the rewritten original line, with leading
    and trailing space removed and special characters transformed into
    their aliases.
    """
    line = line.strip()
    if line and line[0] == '@':
      line = 'rerun ' + line[1:]
    return super(ImpalaShell, self).parseline(line)


  def _replace_history_delimiters(self, src_delim, tgt_delim):
    """Replaces source_delim with target_delim for all items in history.

    Read all the items from history into a local list. Clear the history and copy them
    back after doing the transformation.
    """
    history_len = self.readline.get_current_history_length()
    # load the history and replace the shell's delimiter with EOL
    history_items = map(self.readline.get_history_item, xrange(1, history_len + 1))
    history_items = [item.replace(src_delim, tgt_delim) for item in history_items]
    # Clear the original history and replace it with the mutated history.
    self.readline.clear_history()
    for history_item in history_items:
      self.readline.add_history(history_item)

  def default(self, args):
    query = self.imp_client.create_beeswax_query(args, self.set_query_options)
    return self._execute_stmt(query, print_web_link=True)

  def emptyline(self):
    """If an empty line is entered, do nothing"""

  def do_version(self, args):
    """Prints the Impala build version"""
    print_to_stderr("Shell version: %s" % VERSION_STRING)
    print_to_stderr("Server version: %s" % self.server_version)

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

  def execute_query_list(self, queries):
    if not self.imp_client.connected:
      print_to_stderr('Not connected to Impala, could not execute queries.')
      return False
    queries = [self.sanitise_input(q) for q in queries]
    for q in queries:
      if self.onecmd(q) is CmdStatus.ERROR:
        print_to_stderr('Could not execute command: %s' % q)
        if not self.ignore_query_failure: return False
    return True


TIPS=[
  "Press TAB twice to see a list of available commands.",
  "After running a query, type SUMMARY to see a summary of where time was spent.",
  "The SET command shows the current value of all shell and query options.",
  "To see live updates on a query's progress, run 'set LIVE_SUMMARY=1;'.",
  "To see a summary of a query's progress that updates in real-time, run 'set \
LIVE_PROGRESS=1;'.",
  "The HISTORY command lists all shell commands in chronological order.",
  "The '-B' command line flag turns off pretty-printing for query results. Use this flag \
to remove formatting from results you want to save for later, or to benchmark Impala.",
  "You can run a single query from the command line using the '-q' option.",
  "When pretty-printing is disabled, you can use the '--output_delimiter' flag to set \
the delimiter for fields in the same row. The default is ','.",
  "Run the PROFILE command after a query has finished to see a comprehensive summary of \
all the performance and diagnostic information that Impala gathered for that query. Be \
warned, it can be very long!",
  "To see more tips, run the TIP command.",
  "Every command must be terminated by a ';'.",
  "Want to know what version of Impala you're connected to? Run the VERSION command to \
find out!",
  "You can change the Impala daemon that you're connected to by using the CONNECT \
command."
  "To see how Impala will plan to run your query without actually executing it, use the \
EXPLAIN command. You can change the level of detail in the EXPLAIN output by setting the \
EXPLAIN_LEVEL query option.",
  "When you set a query option it lasts for the duration of the Impala shell session."
  ]

HEADER_DIVIDER =\
  "***********************************************************************************"

def _format_tip(tip):
  """Takes a tip string and splits it on word boundaries so that it fits neatly inside the
  shell header."""
  return '\n'.join([l for l in textwrap.wrap(tip, len(HEADER_DIVIDER))])

WELCOME_STRING = """\
***********************************************************************************
Welcome to the Impala shell.
(%s)

%s
***********************************************************************************\
""" \
  % (VERSION_STRING, _format_tip(random.choice(TIPS)))

def print_to_stderr(message):
  print >> sys.stderr, message

def parse_query_text(query_text, utf8_encode_policy='strict'):
  """Parse query file text to extract queries and encode into utf-8"""
  query_list = [q.encode('utf-8', utf8_encode_policy) for q in sqlparse.split(query_text)]
  # Remove trailing comments in the input, if any. We do this because sqlparse splits the
  # input at query boundaries and associates the query only with preceding comments
  # (following comments are associated with the next query). This is a problem with
  # trailing comments. For example, consider the following input:
  # -------------
  # -- comment1
  # select 1;
  # -- comment2
  # -------------
  # When sqlparse splits the query, "comment1" is associated with the query "select 1" and
  # "--comment2" is sent as is. Impala's parser however doesn't consider it a valid SQL
  # and throws an exception. We identify such trailing comments and ignore them (do not
  # send them to Impala).
  if query_list and not sqlparse.format(query_list[-1], strip_comments=True).strip("\n"):
    query_list.pop()
  return query_list

def parse_variables(keyvals):
  """Parse variable assignments passed as arguments in the command line"""
  kv_pattern = r'(%s)=(.*)$' % (ImpalaShell.VALID_VAR_NAME_PATTERN,)
  vars = {}
  if keyvals:
    for keyval in keyvals:
      match = re.match(kv_pattern, keyval)
      if not match:
        print_to_stderr('Error: Could not parse key-value "%s". ' % (keyval,) +
                        'It must follow the pattern "KEY=VALUE".')
        parser.print_help()
        sys.exit(1)
      else:
        vars[match.groups()[0].upper()] = match.groups()[1]
  return vars

def execute_queries_non_interactive_mode(options, query_options):
  """Run queries in non-interactive mode."""
  if options.query_file:
    try:
      # "-" here signifies input from STDIN
      if options.query_file == "-":
        query_file_handle = sys.stdin
      else:
        query_file_handle = open(options.query_file, 'r')
    except Exception, e:
      print_to_stderr("Could not open file '%s': %s" % (options.query_file, e))
      sys.exit(1)

    query_text = query_file_handle.read()
  elif options.query:
    query_text = options.query
  else:
    return

  queries = parse_query_text(query_text)
  shell = ImpalaShell(options, query_options)
  if not (shell.execute_query_list(shell.cmdqueue) and
          shell.execute_query_list(queries)):
    sys.exit(1)

def get_intro(options):
  """Get introduction message for start-up. The last character should not be a return."""
  if not options.verbose:
    return ""

  intro = WELCOME_STRING

  if not options.ssl and options.creds_ok_in_clear and options.use_ldap:
    intro += ("\n\nLDAP authentication is enabled, but the connection to Impala is "
              "not secured by TLS.\nALL PASSWORDS WILL BE SENT IN THE CLEAR TO IMPALA.")
  return intro

if __name__ == "__main__":
  """
  There are two types of options: shell options and query_options. Both can be set in the
  command line, which override the options set in config file (.impalarc). The default
  shell options come from impala_shell_config_defaults.py. Query options have no defaults
  within the impala-shell, but they do have defaults on the server. Query options can be
  also changed in impala-shell with the 'set' command.
  """
  # pass defaults into option parser
  parser = get_option_parser(impala_shell_defaults)
  options, args = parser.parse_args()
  # use path to file specified by user in config_file option
  user_config = os.path.expanduser(options.config_file);
  # by default, use the .impalarc in the home directory
  config_to_load = impala_shell_defaults.get("config_file")
  # verify user_config, if found
  if os.path.isfile(user_config) and user_config != config_to_load:
    if options.verbose:
      print_to_stderr("Loading in options from config file: %s \n" % user_config)
    # Command line overrides loading ~/.impalarc
    config_to_load = user_config
  elif user_config != config_to_load:
    print_to_stderr('%s not found.\n' % user_config)
    sys.exit(1)

  query_options = {}

  # default shell options loaded in from impala_shell_config_defaults.py
  # options defaults overwritten by those in config file
  try:
    loaded_shell_options, query_options = get_config_from_file(config_to_load)
    impala_shell_defaults.update(loaded_shell_options)
  except Exception, e:
    print_to_stderr(e)
    sys.exit(1)

  parser = get_option_parser(impala_shell_defaults)
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

  if not options.ssl and not options.creds_ok_in_clear and options.use_ldap:
    print_to_stderr("LDAP credentials may not be sent over insecure " +
                    "connections. Enable SSL or set --auth_creds_ok_in_clear")
    sys.exit(1)

  if not options.use_ldap and options.ldap_password_cmd:
    print_to_stderr("Option --ldap_password_cmd requires using LDAP authentication " +
                    "mechanism (-l)")
    sys.exit(1)

  if options.use_kerberos:
    if options.verbose:
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
    if options.verbose:
      print_to_stderr("Starting Impala Shell using LDAP-based authentication")
  else:
    if options.verbose:
      print_to_stderr("Starting Impala Shell without Kerberos authentication")

  options.ldap_password = None
  if options.use_ldap and options.ldap_password_cmd:
    try:
      p = subprocess.Popen(shlex.split(options.ldap_password_cmd), stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
      options.ldap_password, stderr = p.communicate()
      if p.returncode != 0:
        print_to_stderr("Error retrieving LDAP password (command was '%s', error was: "
                        "'%s')" % (options.ldap_password_cmd, stderr.strip()))
        sys.exit(1)
    except Exception, e:
      print_to_stderr("Error retrieving LDAP password (command was: '%s', exception "
                      "was: '%s')" % (options.ldap_password_cmd, e))
      sys.exit(1)

  if options.ssl:
    if options.ca_cert is None:
      if options.verbose:
        print_to_stderr("SSL is enabled. Impala server certificates will NOT be verified"\
                        " (set --ca_cert to change)")
    else:
      if options.verbose:
        print_to_stderr("SSL is enabled")

  if options.output_file:
    try:
      # Make sure the given file can be opened for writing. This will also clear the file
      # if successful.
      open(options.output_file, 'wb')
    except IOError, e:
      print_to_stderr('Error opening output file for writing: %s' % e)
      sys.exit(1)

  options.variables = parse_variables(options.keyval)

  # Override query_options from config file with those specified on the command line.
  query_options.update(
     [(k.upper(), v) for k, v in parse_variables(options.query_options).items()])

  if options.query or options.query_file:
    if options.print_progress or options.print_summary:
      print_to_stderr("Error: Live reporting is available for interactive mode only.")
      sys.exit(1)

    execute_queries_non_interactive_mode(options, query_options)
    sys.exit(0)

  intro = get_intro(options)

  shell = ImpalaShell(options, query_options)
  while shell.is_alive:
    try:
      try:
        shell.cmdloop(intro)
      except KeyboardInterrupt:
        intro = '\n'
      # A last measure against any exceptions thrown by an rpc
      # not caught in the shell
      except socket.error, (code, e):
        # if the socket was interrupted, reconnect the connection with the client
        if code == errno.EINTR:
          print shell.CANCELLATION_MESSAGE
          shell._reconnect_cancellation()
        else:
          print_to_stderr("Socket error %s: %s" % (code, e))
          shell.imp_client.connected = False
          shell.prompt = shell.DISCONNECTED_PROMPT
      except DisconnectedException, e:
        # the client has lost the connection
        print_to_stderr(e)
        shell.imp_client.connected = False
        shell.prompt = shell.DISCONNECTED_PROMPT
      except QueryStateException, e:
        # an exception occurred while executing the query
        shell.imp_client.close_query(shell.last_query_handle,
                                     shell.query_handle_closed)
        print_to_stderr(e)
      except RPCException, e:
        # could not complete the rpc successfully
        print_to_stderr(e)
      except IOError, e:
        # Interrupted system calls (e.g. because of cancellation) should be ignored.
        if e.errno != errno.EINTR: raise
    finally:
      intro = ''
