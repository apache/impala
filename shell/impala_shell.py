#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
from __future__ import print_function, unicode_literals
from compatibility import _xrange as xrange

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
import traceback

from impala_client import ImpalaHS2Client, StrictHS2Client, \
    ImpalaBeeswaxClient, QueryOptionLevels
from impala_shell_config_defaults import impala_shell_defaults
from option_parser import get_option_parser, get_config_from_file
from shell_output import (DelimitedOutputFormatter, OutputStream, PrettyOutputFormatter,
                          OverwritingStdErrOutputStream)
from subprocess import call
from shell_exceptions import (RPCException, DisconnectedException, QueryStateException,
    QueryCancelledByShellException, MissingThriftMethodException)


VERSION_FORMAT = "Impala Shell v%(version)s (%(git_hash)s) built on %(build_date)s"
VERSION_STRING = "impala shell build version not available"
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

DEFAULT_BEESWAX_PORT = 21000
DEFAULT_HS2_PORT = 21050
DEFAULT_HS2_HTTP_PORT = 28000
DEFAULT_STRICT_HS2_PORT = 11050
DEFAULT_STRICT_HS2_HTTP_PORT = 10001

def strip_comments(sql):
  """sqlparse default implementation of strip comments has a bad performance when parsing
  very large SQL due to the grouping. This is because the default implementation tries to
  format the SQL for pretty-printing. Impala shell use of strip comments is mostly for
  checking and not for altering the actual SQL, so having a pretty-formatted SQL is
  irrelevant in Impala shell. Removing the grouping gives a significant performance boost.
  """
  stack = sqlparse.engine.FilterStack()
  stack.stmtprocess.append(sqlparse.filters.StripCommentsFilter())
  stack.postprocess.append(sqlparse.filters.SerializerUnicode())
  return ''.join(stack.run(sql, 'utf-8')).strip()


class CmdStatus:
  """Values indicate the execution status of a command to the cmd shell driver module
  SUCCESS and ERROR continue running the shell and ABORT exits the shell
  Since SUCCESS == None, successful commands do not need to explicitly return
  anything on completion
  """
  SUCCESS = None
  ABORT = True
  ERROR = False


class FatalShellException(Exception):
  """Thrown if a fatal error occurs that requires terminating the shell. The cause of the
  error should be logged to stderr before raising this exception."""
  pass


class QueryOptionDisplayModes:
  REGULAR_OPTIONS_ONLY = 1
  ALL_OPTIONS = 2


class QueryAttemptDisplayModes:
  """The display mode when runtime profiles or summaries are printed to the console.
  If the query has not been retried, then the display mode does not change anything.
  The format is always the same. If the query has been retried, then the ALL option will
  print both the original and retried profiles/summaries. If the LATEST option is
  specified, then only the retried profile/summary will be printed. If the ORIGINAL option
  is specified, then only the original profile/summary will be printed."""
  ALL = "all"
  LATEST = "latest"
  ORIGINAL = "original"


# Py3 method resolution order requires 'object' to be last w/ multiple inheritance
class ImpalaShell(cmd.Cmd, object):
  """ Simple Impala Shell.

  Implements the context manager interface to ensure client connections and sessions
  are cleanly torn down. ImpalaShell instances should be used within a "with" statement
  to ensure correct teardown.

  Basic usage: type connect <host:port> to connect to an impalad
  Then issue queries or other commands. Tab-completion should show the set of
  available commands.
  Methods that implement shell commands return a boolean tuple (stop, status)
  stop is a flag the command loop uses to continue/discontinue the prompt.
  Status tells the caller that the command completed successfully.
  """

  # If not connected to an impalad, the server version is unknown.
  UNKNOWN_SERVER_VERSION = "Not Connected"
  PROMPT_FORMAT = "[{host}:{port}] {db}> "
  DISCONNECTED_PROMPT = "[Not connected] > "
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
  # Strings that are interpreted as True for some shell options.
  TRUE_STRINGS = ("true", "TRUE", "True", "1")
  # List of quit commands
  QUIT_COMMANDS = ("quit", "exit")

  VALID_SHELL_OPTIONS = {
    'LIVE_PROGRESS': (lambda x: x in ImpalaShell.TRUE_STRINGS, "live_progress"),
    'LIVE_SUMMARY': (lambda x: x in ImpalaShell.TRUE_STRINGS, "live_summary"),
    'WRITE_DELIMITED' : (lambda x: x in ImpalaShell.TRUE_STRINGS, "write_delimited"),
    'VERBOSE' : (lambda x: x in ImpalaShell.TRUE_STRINGS, "verbose"),
    'DELIMITER' : (lambda x: " " if x == '\\s' else x, "output_delimiter"),
    'OUTPUT_FILE' : (lambda x: None if x == '' else x, "output_file"),
  }

  # Minimum time in seconds between two calls to get the exec summary.
  PROGRESS_UPDATE_INTERVAL = 1.0
  # Environment variable used to source a global config file
  GLOBAL_CONFIG_FILE = "IMPALA_SHELL_GLOBAL_CONFIG_FILE"

  def __init__(self, options, query_options):
    cmd.Cmd.__init__(self)
    self.is_alive = True

    self.impalad = None
    self.kerberos_host_fqdn = options.kerberos_host_fqdn
    self.use_kerberos = options.use_kerberos
    self.kerberos_service_name = options.kerberos_service_name
    self.use_ssl = options.ssl
    self.ca_cert = options.ca_cert
    self.user = options.user
    self.ldap_password_cmd = options.ldap_password_cmd
    self.strict_hs2_protocol = options.strict_hs2_protocol
    self.ldap_password = options.ldap_password
    # When running tests in strict mode, the server uses the ldap
    # protocol but can allow any password.
    if options.use_ldap_test_password:
      self.ldap_password = 'password'
    self.use_ldap = options.use_ldap or \
        (self.strict_hs2_protocol and not self.use_kerberos)
    self.client_connect_timeout_ms = options.client_connect_timeout_ms
    self.http_socket_timeout_s = None
    if (options.http_socket_timeout_s != 'None' and
          options.http_socket_timeout_s is not None):
        self.http_socket_timeout_s = float(options.http_socket_timeout_s)
    self.verbose = options.verbose
    self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    self.server_version = ImpalaShell.UNKNOWN_SERVER_VERSION
    self.webserver_address = None

    self.current_db = options.default_db
    self.history_file = os.path.expanduser(options.history_file)
    # Stores the state of user input until a delimiter is seen.
    self.partial_cmd = str()
    # Stores the old prompt while the user input is incomplete.
    self.cached_prompt = str()

    self.show_profiles = options.show_profiles

    # Output formatting flags/options
    self.output_file = options.output_file
    self.output_delimiter = " " if options.output_delimiter == "\\s" \
        else options.output_delimiter
    self.write_delimited = options.write_delimited
    self.print_header = options.print_header

    self.progress_stream = OverwritingStdErrOutputStream()

    self.set_query_options = query_options
    self.set_variables = options.variables

    self._populate_command_list()

    self.imp_client = None

    # Used to pass the original unmodified command into do_*() methods.
    self.orig_cmd = None

    # Tracks query handle of the last query executed. Used by the 'profile' command.
    self.last_query_handle = None;

    # live_summary and live_progress are turned off in strict_hs2_protocol mode
    if options.strict_hs2_protocol:
      if options.live_summary:
        warning = "WARNING: Unable to track live summary with strict_hs2_protocol"
        print(warning, file=sys.stderr)
      if options.live_progress:
        warning = "WARNING: Unable to track live progress with strict_hs2_protocol"
        print(warning, file=sys.stderr)

      # do not allow live_progress or live_summary to be changed.
      self.VALID_SHELL_OPTIONS['LIVE_PROGRESS'] = (lambda x: x in (), "live_progress")
      self.VALID_SHELL_OPTIONS['LIVE_SUMMARY'] = (lambda x: x in (), "live_summary")

    self.live_summary = options.live_summary and not options.strict_hs2_protocol
    self.live_progress = options.live_progress and not options.strict_hs2_protocol

    self.ignore_query_failure = options.ignore_query_failure

    self.http_path = options.http_path
    self.fetch_size = options.fetch_size
    self.http_cookie_names = options.http_cookie_names

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
          # The history file is created when the Impala shell is invoked and commands are
          # issued. In case it does not exist do not read the history file.
          if os.path.exists(self.history_file):
            self.readline.read_history_file(self.history_file)
            self._replace_history_delimiters(ImpalaShell.HISTORY_FILE_QUERY_DELIM, '\n')
        except ValueError:
          warning = "WARNING: history_max option malformed %s\n" % options.history_max
          print(warning, file=sys.stderr)
          self.readline.set_history_length(1000)
        except IOError as i:
          warning = "WARNING: Unable to load command history (disabling impala-shell " \
              "command history): %s" % i
          print(warning, file=sys.stderr)
          # This history file exists but is not readable, disable readline.
          self._disable_readline()
      except ImportError as i:
        warning = "WARNING: Unable to import readline module (disabling impala-shell " \
            "command history): %s" % i
        print(warning, file=sys.stderr)
        self._disable_readline()

    if options.impalad is not None:
      self.do_connect(options.impalad)
      # Check if the database in shell option exists
      self._validate_database(immediately=True)

    # We handle Ctrl-C ourselves, using an Event object to signal cancellation
    # requests between the handler and the main shell thread.
    signal.signal(signal.SIGINT, self._signal_handler)

  def __enter__(self):
    return self

  def __exit__(self, type, value, traceback):
    self.close_connection()

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
    print("Query options (defaults shown in []):")
    if not self.imp_client.default_query_options and not self.set_query_options:
      print('\tNo options available.')
    else:
      (regular_options, advanced_options, development_options, deprecated_options,
          removed_options) = self._get_query_option_grouping()
      self._print_option_group(regular_options)
      # If the shell is connected to an Impala that predates IMPALA-2181 then
      # the advanced_options would be empty and only the regular options would
      # be displayed.
      if advanced_options:
        print('\nAdvanced Query Options:')
        self._print_option_group(advanced_options)
      if print_mode == QueryOptionDisplayModes.ALL_OPTIONS:
        if development_options:
          print('\nDevelopment Query Options:')
          self._print_option_group(development_options)
        if deprecated_options:
          print('\nDeprecated Query Options:')
          self._print_option_group(deprecated_options)
    self._print_shell_options()

  def _get_query_option_grouping(self):
    """For all the query options received through rpc this function determines the
    query option level for display purposes using the received query_option_levels
    parameters.
    If the option level can't be determined then it defaults to 'REGULAR'"""
    (regular_options, advanced_options, development_options, deprecated_options,
        removed_options) = {}, {}, {}, {}, {}
    if sys.version_info.major < 3:
      client_default_query_opts = self.imp_client.default_query_options.iteritems()
    else:
      client_default_query_opts = self.imp_client.default_query_options.items()
    for option_name, option_value in client_default_query_opts:
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
    for option in sorted(query_options):
      if (option in self.set_query_options and
          self.set_query_options[option] != query_options[option]):  # noqa
        print('\n'.join(["\t%s: %s" % (option, self.set_query_options[option])]))
      else:
        print('\n'.join(["\t%s: [%s]" % (option, query_options[option])]))

  def _print_variables(self):
    # Prints the currently defined variables.
    if not self.set_variables:
      print('\tNo variables defined.')
    else:
      for k in sorted(self.set_variables):
        print('\n'.join(["\t%s: %s" % (k, self.set_variables[k])]))

  def _print_shell_options(self):
    """Prints shell options, which are local and independent of query options."""
    print("\nShell Options")
    for x in self.VALID_SHELL_OPTIONS:
      print("\t%s: %s" % (x, self.__dict__[self.VALID_SHELL_OPTIONS[x][1]]))

  def _build_query_string(self, leading_comment, cmd, args):
    """Called to build a query string based on the parts output by parseline():
    the leading comment, the command name and the arguments to the command."""
    # In order to deduce the correct cmd, parseline stripped the leading comment.
    # To preserve the original query, the leading comment (if exists) will be
    # prepended when constructing the query sent to the Impala front-end.
    return "{0}{1} {2}".format(leading_comment or '', cmd or '', args)

  def do_shell(self, args):
    """Run a command on the shell
    Usage: shell <cmd>
           ! <cmd>

    """
    try:
      start_time = time.time()
      os.system(args)
      self._print_if_verbose("--------\nExecuted in %2.2fs" % (time.time() - start_time))
    except Exception as e:
      print('Error running command : %s' % e, file=sys.stderr)
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
        print('\n')
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
    line = strip_comments(line).rstrip()
    if line.endswith(ImpalaShell.CMD_DELIM):
      try:
        # Look for an open quotation in the entire command, and not just the
        # current line.
        if self.partial_cmd:
          line = strip_comments('%s %s' % (self.partial_cmd, line))
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
      state_closed = True
      opener = None
      for i, char in enumerate(line):
        if state_closed and (char in ['\'', '\"']):
          state_closed = False
          opener = char
        elif not state_closed and opener == char:
          if line[i - 1] != '\\':
            state_closed = True
            opener = None

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

    Returns text type result (unicode in Python2, str in Python3)
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
        self.partial_cmd = "{0}\n{1}".format(self.partial_cmd, cmd)
      else:
        # If the input string is empty or partial_cmd is empty.
        self.partial_cmd = "{0}{1}".format(self.partial_cmd, cmd)
      # Remove the most recent item from history if:
      #   -- The current state of user input in incomplete.
      #   -- The most recent user input is not an empty string
      if self.readline and current_history_len > 0 and cmd:
        self.readline.remove_history_item(current_history_len - 1)
      # An empty string results in a no-op. Look at emptyline()
      return str()
    elif self.partial_cmd:  # input ends with a delimiter and partial_cmd is not empty
      if cmd != ImpalaShell.CMD_DELIM:
        completed_cmd = "{0}\n{1}".format(self.partial_cmd, cmd)
      else:
        completed_cmd = "{0}{1}".format(self.partial_cmd, cmd)
      # Reset partial_cmd to an empty string
      self.partial_cmd = str()
      # Replace the most recent history item with the completed command.
      completed_cmd = sqlparse.format(completed_cmd)
      if self.readline and current_history_len > 0:
        # readline.replace_history_item(pos, line) requires 'line' in bytes in Python2,
        # and str in Python3.
        if sys.version_info.major == 2:
          history_cmd = completed_cmd.encode('utf-8')
        else:
          history_cmd = completed_cmd
        self.readline.replace_history_item(current_history_len - 1, history_cmd)
      # Revert the prompt to its earlier state
      self.prompt = self.cached_prompt
    else:  # Input has a delimiter and partial_cmd is empty
      completed_cmd = sqlparse.format(cmd)
    return completed_cmd

  def _new_impala_client(self):
    protocol = options.protocol.lower()
    if options.strict_hs2_protocol:
      assert protocol == 'hs2' or protocol == 'hs2-http'
      if protocol == 'hs2':
        return StrictHS2Client(self.impalad, self.fetch_size, self.kerberos_host_fqdn,
                          self.use_kerberos, self.kerberos_service_name, self.use_ssl,
                          self.ca_cert, self.user, self.ldap_password, True,
                          self.client_connect_timeout_ms, self.verbose,
                          use_http_base_transport=False, http_path=self.http_path,
                          http_cookie_names=None)
      elif protocol == 'hs2-http':
        return StrictHS2Client(self.impalad, self.fetch_size, self.kerberos_host_fqdn,
                          self.use_kerberos, self.kerberos_service_name, self.use_ssl,
                          self.ca_cert, self.user, self.ldap_password, self.use_ldap,
                          self.client_connect_timeout_ms, self.verbose,
                          use_http_base_transport=True, http_path=self.http_path,
                          http_cookie_names=self.http_cookie_names)
    if protocol == 'hs2':
      return ImpalaHS2Client(self.impalad, self.fetch_size, self.kerberos_host_fqdn,
                          self.use_kerberos, self.kerberos_service_name, self.use_ssl,
                          self.ca_cert, self.user, self.ldap_password, self.use_ldap,
                          self.client_connect_timeout_ms, self.verbose,
                          use_http_base_transport=False, http_path=self.http_path,
                          http_cookie_names=None)
    elif protocol == 'hs2-http':
      return ImpalaHS2Client(self.impalad, self.fetch_size, self.kerberos_host_fqdn,
                          self.use_kerberos, self.kerberos_service_name, self.use_ssl,
                          self.ca_cert, self.user, self.ldap_password, self.use_ldap,
                          self.client_connect_timeout_ms, self.verbose,
                          use_http_base_transport=True, http_path=self.http_path,
                          http_cookie_names=self.http_cookie_names,
                          http_socket_timeout_s=self.http_socket_timeout_s)
    elif protocol == 'beeswax':
      return ImpalaBeeswaxClient(self.impalad, self.fetch_size, self.kerberos_host_fqdn,
                          self.use_kerberos, self.kerberos_service_name, self.use_ssl,
                          self.ca_cert, self.user, self.ldap_password, self.use_ldap,
                          self.client_connect_timeout_ms, self.verbose)
    else:
      err_msg = "Invalid --protocol value {0}, must be beeswax, hs2 or hs2-http."
      print(err_msg.format(protocol), file=sys.stderr)
      raise FatalShellException()

  def close_connection(self):
    """Closes the current Impala connection."""
    if self.imp_client:
      self.imp_client.close_connection()

  def _signal_handler(self, signal, frame):
    """Handles query cancellation on a Ctrl+C event"""
    if self.last_query_handle is None or self.last_query_handle.is_closed:
      if self.partial_cmd:
        # Revert the prompt to its earlier state
        self.prompt = self.cached_prompt
        # Reset the already given commands
        self.partial_cmd = str()
      raise KeyboardInterrupt()
    # Create a new connection to the impalad and cancel the query.
    # TODO: this isn't thread-safe with respect to the main thread executing the
    # query. This probably contributes to glitchiness when cancelling query in
    # the shell.
    for cancel_try in xrange(ImpalaShell.CANCELLATION_TRIES):
      try:
        self.imp_client.is_query_cancelled = True
        print(ImpalaShell.CANCELLATION_MESSAGE, file=sys.stderr)
        new_imp_client = self._new_impala_client()
        new_imp_client.connect()
        try:
          new_imp_client.cancel_query(self.last_query_handle)
          new_imp_client.close_query(self.last_query_handle)
        finally:
          new_imp_client.close_connection()
        break
      except Exception as e:
        # Suppress harmless errors.
        err_msg = str(e).strip()
        if err_msg in ['ERROR: Cancelled', 'ERROR: Invalid or unknown query handle']:
          break
        err_details = "Failed to reconnect and close (try %i/%i): %s"
        print(err_details % (cancel_try + 1, ImpalaShell.CANCELLATION_TRIES, err_msg),
              file=sys.stderr)

  def _is_quit_command(self, command):
    # Do a case insensitive check
    return command.lower() in ImpalaShell.QUIT_COMMANDS

  def set_prompt(self, db):
    self.prompt = ImpalaShell.PROMPT_FORMAT.format(
        host=self.impalad[0], port=self.impalad[1], db=db)

  def precmd(self, args):
    # In Python2, 'args' could in str type if it's the original input line, or in unicode
    # type if it's a split query appended by us. See how we deal with 'parsed_cmds' below.
    if sys.version_info.major == 2 and isinstance(args, str):
      args = self.sanitise_input(args.decode('utf-8'))  # python2
    else:
      args = self.sanitise_input(args)  # python3
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
    # There is no need to reconnect if we are quitting.
    if not self.imp_client.is_connected() and not self._is_quit_command(args):
      print("Connection lost, reconnecting...", file=sys.stderr)
      self._connect()
      self._validate_database(immediately=True)
    return args

  def onecmd(self, line):
    """Overridden to ensure the variable replacement is processed in interactive
       as well as non-interactive mode, since the precmd method would only work for
       the interactive case, when cmdloop is called.
    """
    # Replace variables in the statement before it's executed
    line = replace_variables(self.set_variables, line)
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
      command, arg, line, leading_comment = self.parseline(line)
      if not line:
        return self.emptyline()
      # orig_cmd and last_leading_comment are passed into do_*() functions
      # via this side mechanism because the cmd module limits us to passing
      # in the argument list only.
      self.orig_cmd = command
      self.last_leading_comment = leading_comment
      self.lastcmd = line
      if not command:
        return self.default(line)
      elif line == 'EOF':
        self.lastcmd = ''
      else:
        try:
          func = getattr(self, 'do_' + command.lower())
        except AttributeError:
          return self.default(line)
        return func(arg)

  def postcmd(self, status, args):
    # status conveys to shell how the shell should continue execution
    # should always be a CmdStatus
    return status

  def do_summary(self, args):
    split_args = args.split()
    if len(split_args) > 1:
      print("'summary' only accepts 0 or 1 arguments", file=sys.stderr)
      return CmdStatus.ERROR
    if not self.last_query_handle:
      print("Could not retrieve summary: no previous query.", file=sys.stderr)
      return CmdStatus.ERROR

    display_mode = QueryAttemptDisplayModes.LATEST
    if len(split_args) == 1:
      display_mode = self.get_query_attempt_display_mode(split_args[0])
      if display_mode is None:
        return CmdStatus.ERROR

    try:
      summary, failed_summary = self.imp_client.get_summary(self.last_query_handle)
    except RPCException as e:
      import re
      error_pattern = re.compile("ERROR: Query id \d+:\d+ not found.")
      if error_pattern.match(e.value):
        print("Could not retrieve summary for query.", file=sys.stderr)
      else:
        print(e, file=sys.stderr)
      return CmdStatus.ERROR
    if summary.nodes is None:
      print("Summary not available", file=sys.stderr)
      return CmdStatus.SUCCESS

    if display_mode == QueryAttemptDisplayModes.ALL:
      print("Query Summary:")
      self.print_exec_summary(summary)
      if failed_summary:
        print("Failed Query Summary:")
        self.print_exec_summary(failed_summary)
    elif display_mode == QueryAttemptDisplayModes.LATEST:
      self.print_exec_summary(summary)
    elif display_mode == QueryAttemptDisplayModes.ORIGINAL:
      self.print_exec_summary(failed_summary)
    else:
      raise FatalShellException("Invalid value for query summary display mode")

  @staticmethod
  def get_query_attempt_display_mode(arg_mode):
    arg_mode = str(arg_mode).lower()
    if arg_mode not in [QueryAttemptDisplayModes.ALL,
        QueryAttemptDisplayModes.LATEST, QueryAttemptDisplayModes.ORIGINAL]:
      print("Invalid value for query attempt display mode: \'" +
          arg_mode + "\'. Valid values are [ALL | LATEST | ORIGINAL]")
    return arg_mode

  def print_exec_summary(self, summary):
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

  def _handle_unset_shell_options(self, token):
    try:
      handle = self.VALID_SHELL_OPTIONS[token]
      self.__dict__[handle[1]] = impala_shell_defaults[handle[1]]
      return True
    except KeyError:
      return False

  def _print_with_set(self, print_level):
    self._print_options(print_level)
    print("\nVariables:")
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
        set_err_msg = ("Error: SET <option>=<value>\n"
                       "       OR\n"
                       "       SET VAR:<variable>=<value>")
        print(set_err_msg, file=sys.stderr)
        return CmdStatus.ERROR
    option_upper = tokens[0].upper()
    # Check if it's a variable
    var_name = get_var_name(option_upper)
    if var_name is not None:
      # Set the variable
      self.set_variables[var_name] = tokens[1]
      self._print_if_verbose('Variable %s set to %s' % (var_name, tokens[1]))
    elif not self._handle_shell_options(option_upper, tokens[1]):
      if option_upper not in self.imp_client.default_query_options.keys():
        print("Unknown query option: %s" % (tokens[0]))
        print("Available query options, with their values (defaults shown in []):")
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
      print('Usage: unset <option>')
      return CmdStatus.ERROR
    option = args.upper()
    # Check if it's a variable
    var_name = get_var_name(option)
    if var_name is not None:
      if self.set_variables.get(var_name):
        print('Unsetting variable %s' % var_name)
        del self.set_variables[var_name]
      else:
        print("No variable called %s is set" % var_name)
    elif self.set_query_options.get(option):
      print('Unsetting option %s' % option)
      del self.set_query_options[option]
    elif self._handle_unset_shell_options(option):
      print('Unsetting shell option %s' % option)
    else:
      print("No option called %s is set" % option)

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
    Usage: connect, defaults to the fqdn of the localhost and the protocol's default port
           connect <hostname:port>
           connect <hostname>, defaults to the protocol's default port

    """
    # Assume the user wants to connect to the local impalad if no connection string is
    # specified. Connecting to a kerberized impalad requires an fqdn as the host name.
    if self.use_ldap and self.ldap_password is None:
      self.ldap_password = getpass.getpass("LDAP password for %s: " % self.user)

    if not args: args = socket.getfqdn()
    tokens = args.split(" ")
    # validate the connection string.
    host_port = [val for val in tokens[0].split(':') if val.strip()]
    protocol = options.protocol.lower()
    if (':' in tokens[0] and len(host_port) != 2):
      print("Connection string must either be empty, or of the form "
            "<hostname[:port]>", file=sys.stderr)
      return CmdStatus.ERROR
    elif len(host_port) == 1:
      if options.strict_hs2_protocol:
        if protocol == 'hs2':
          port = str(DEFAULT_STRICT_HS2_PORT)
        elif protocol == 'hs2-http':
          port = str(DEFAULT_STRICT_HS2_HTTP_PORT)
        else:
          print("Invalid protocol specified for 'strict_hs2_protocol' option: %s"
              % protocol, file=sys.stderr)
          raise FatalShellException()
      elif protocol == 'hs2':
        port = str(DEFAULT_HS2_PORT)
      elif protocol == 'hs2-http':
        port = str(DEFAULT_HS2_HTTP_PORT)
      elif protocol == 'beeswax':
        port = str(DEFAULT_BEESWAX_PORT)
      else:
        print("Invalid protocol specified: %s" % protocol, file=sys.stderr)
        raise FatalShellException()
      host_port.append(port)
    self.impalad = tuple(host_port)
    self.close_connection()
    self.imp_client = self._new_impala_client()
    self._connect()
    # If the connection fails and the Kerberos has not been enabled,
    # check for a valid kerberos ticket and retry the connection
    # with kerberos enabled.
    # IMPALA-8932: Kerberos is not yet supported for hs2-http, so don't retry.
    if not self.imp_client.connected and not self.use_kerberos and protocol != 'hs2-http':
      try:
        if call(["klist", "-s"]) == 0:
          print("Kerberos ticket found in the credentials cache, retrying "
                "the connection with a secure transport.", file=sys.stderr)
          self.use_kerberos = True
          self.use_ldap = False
          self.ldap_password = None
          self.imp_client = self._new_impala_client()
          self._connect()
      except OSError:
        pass

    if self.imp_client.connected:
      self._print_if_verbose('Connected to %s:%s' % self.impalad)
      self._print_if_verbose('Server version: %s' % self.server_version)
      self.set_prompt(ImpalaShell.DEFAULT_DB)
      self._validate_database()
    # In the case that we lost connection while a command was being entered,
    # we may have a dangling command, clear partial_cmd
    self.partial_cmd = str()
    # Check if any of query options set by the user are inconsistent
    # with the impalad being connected to

    # Use a temporary to avoid changing set_query_options during iteration.
    new_query_options = {}
    default_query_option_keys = set(self.imp_client.default_query_options)
    if sys.version_info.major < 3:
      query_options_to_set = self.set_query_options.iteritems()
    else:
      query_options_to_set = self.set_query_options.items()

    for set_option, value in query_options_to_set:
      if set_option not in default_query_option_keys:
        print('%s is not supported for the impalad being '
              'connected to, ignoring.' % set_option)
      else:
        new_query_options[set_option] = value

    if "CLIENT_IDENTIFIER" not in new_query_options \
        and "CLIENT_IDENTIFIER" in default_query_option_keys:
      # Programmatically set default CLIENT_IDENTIFIER to our version string,
      # if the Impala version supports this option.
      new_query_options["CLIENT_IDENTIFIER"] = VERSION_STRING

    self.set_query_options = new_query_options

  def _connect(self):
    try:
      self.server_version, self.webserver_address = self.imp_client.connect()
    except MissingThriftMethodException as e:
      if options.protocol.lower() == 'beeswax':
        port_flag = "-beeswax_port"
        addtl_suggestion = ""
      else:
        assert options.protocol.lower() == 'hs2'
        port_flag = "-hs2_port"
        addtl_suggestion = (" Also check that the Impala daemon connected to has version "
                            "3.3 or greater. impala-shell only supports connecting to "
                            "the HS2 interface of Impala version 3.3 or greater. For "
                            "older Impala versions you must use the (deprecated) beeswax "
                            "protocol with the --protocol=beeswax.")
      # We get a TApplicationException if the transport is valid,
      # but the RPC does not exist.
      print("\n".join(textwrap.wrap(
        "Error: Unable to communicate with impalad service because of the error "
        "reported below. The service does not implement a required Thrift method. "
        "The service may not be an Impala Daemon or you may have specified the wrong "
        "port to connect to. Check host:port to ensure that the port matches the "
        "{port_flag} flag on the Impala Daemon and try again.{addtl_suggestion}".format(
          port_flag=port_flag, addtl_suggestion=addtl_suggestion))), file=sys.stderr)
      print(str(e), file=sys.stderr)
      self.close_connection()
      raise
    except ImportError:
      print("Unable to import the python 'ssl' module. It is"
            " required for an SSL-secured connection.", file=sys.stderr)
      raise FatalShellException()
    except socket.error as e:
      # if the socket was interrupted, reconnect the connection with the client
      if e.errno == errno.EINTR:
        self._reconnect_cancellation()
      else:
        print("Socket error %s: %s" % (e.errno, e), file=sys.stderr)
        self.close_connection()
        self.prompt = self.DISCONNECTED_PROMPT
    except Exception as e:
      if self.ldap_password_cmd and \
          self.ldap_password and \
          self.ldap_password.endswith('\n'):
        print("Warning: LDAP password contains a trailing newline. "
              "Did you use 'echo' instead of 'echo -n'?", file=sys.stderr)
      if self.use_ssl and sys.version_info < (2,7,9) \
          and "EOF occurred in violation of protocol" in str(e):
        print("Warning: TLSv1.2 is not supported for Python < 2.7.9", file=sys.stderr)
      print("Error connecting: %s, %s" % (type(e).__name__, e), file=sys.stderr)
      # A secure connection may still be open. So we explicitly close it.
      self.close_connection()
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
    if not self.imp_client.connected:
      return
    # Should only check if successfully connected.
    if self.current_db:
      self.current_db = self.current_db.strip('`')
      use_current_db = ('use `%s`' % self.current_db)
      if immediately:
        self.onecmd(use_current_db)
      else:
        self.cmdqueue.append(use_current_db + ImpalaShell.CMD_DELIM)

  def _print_if_verbose(self, message, file_descriptor=sys.stderr, flush=True):
    if self.verbose:
      print(message, file=file_descriptor)

    # print() takes a flush keyword argument in python3, but not in python2,
    # so we do it as a second step
    if flush:
      file_descriptor.flush()

  def print_runtime_profile(self, profile, failed_profile,
        profile_display_mode=QueryAttemptDisplayModes.LATEST, status=False):
    """Prints the given runtime profiles to the console. Optionally prints the failed
    profile if the query was retried. The format the profiles are printed is controlled
    by the option profile_display_mode, see QueryProfileDisplayModes docs above.
    """
    if self.show_profiles or status:
      if profile:
        query_profile_prefix = "Query Runtime Profile:\n"
        if profile_display_mode == QueryAttemptDisplayModes.ALL:
          print(query_profile_prefix + profile)
          if failed_profile:
            print("Failed Query Runtime Profile(s):\n" + failed_profile)
        elif profile_display_mode == QueryAttemptDisplayModes.LATEST:
          print(query_profile_prefix + profile)
        elif profile_display_mode == QueryAttemptDisplayModes.ORIGINAL:
          print(query_profile_prefix + failed_profile if failed_profile else profile)
        else:
          raise FatalShellException("Invalid value for query profile display mode")

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
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_create(self, args):
    # We want to print the webserver link only for CTAS queries.
    print_web_link = "select" in args
    query = self._build_query_string(self.last_leading_comment, self.orig_cmd, args)
    return self._execute_stmt(query, print_web_link=print_web_link)

  def do_drop(self, args):
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_load(self, args):
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_profile(self, args):
    """Prints the runtime profile of the last DML statement or SELECT query executed."""
    split_args = args.split()
    if len(split_args) > 1:
      print("'profile' only accepts 0 or 1 arguments", file=sys.stderr)
      return CmdStatus.ERROR
    elif self.last_query_handle is None:
      print('No previous query available to profile', file=sys.stderr)
      return CmdStatus.ERROR

    # Parse and validate the QueryProfileDisplayModes option.
    profile_display_mode = QueryAttemptDisplayModes.LATEST
    if len(split_args) == 1:
      profile_display_mode = self.get_query_attempt_display_mode(split_args[0])
      if profile_display_mode is None:
        return CmdStatus.ERROR

    profile, failed_profile = self.imp_client.get_runtime_profile(
        self.last_query_handle)
    return self.print_runtime_profile(profile, failed_profile, profile_display_mode,
            True)

  def do_select(self, args):
    """Executes a SELECT... query, fetching all rows"""
    query_str = self._build_query_string(self.last_leading_comment, self.orig_cmd, args)
    return self._execute_stmt(query_str, print_web_link=True)

  def do_compute(self, args):
    """Executes a COMPUTE STATS query.
    Impala shell cannot get child query handle so it cannot
    query live progress for COMPUTE STATS query. Disable live
    progress/summary callback for COMPUTE STATS query."""

    query = self._build_query_string(self.last_leading_comment, self.orig_cmd, args)
    (prev_live_progress, prev_live_summary) = self.live_progress, self.live_summary
    (self.live_progress, self.live_summary) = False, False
    try:
      ret = self._execute_stmt(query)
    finally:
      (self.live_progress, self.live_summary) = prev_live_progress, prev_live_summary
    return ret

  def _format_outputstream(self):
    column_names = self.imp_client.get_column_names(self.last_query_handle)
    if sys.version_info.major == 2:
      column_names = [
          col.decode("utf8", errors="replace") if isinstance(col, str) else col
          for col in column_names]
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
    if not self.live_progress and not self.live_summary: return

    checkpoint = time.time()
    if checkpoint - self.last_summary > self.PROGRESS_UPDATE_INTERVAL:
      summary, failed_summary = self.imp_client.get_summary(self.last_query_handle)
      if not summary:
        return

      if summary.is_queued:
        queued_msg = "Query queued. Latest queuing reason: %s\n" % summary.queued_reason
        self.progress_stream.write(queued_msg)
        self.last_summary = time.time()
        return

      data = ""
      if summary.error_logs:
        for error_line in summary.error_logs:
          data += error_line + "\n"
          if self.webserver_address:
            query_id_search = re.search("Retrying query using query id: (.*)",
                                        error_line)
            if query_id_search and len(query_id_search.groups()) == 1:
              retried_query_id = query_id_search.group(1)
              data += "Retried query link: %s\n"\
                      % self.imp_client.get_query_link(retried_query_id)

      if summary.progress:
        progress = summary.progress

        # If the data is not complete return and wait for a good result.
        if not progress.total_scan_ranges and not progress.num_completed_scan_ranges:
          self.last_summary = time.time()
          return

        if self.live_progress and progress.total_scan_ranges > 0:
          val = ((summary.progress.num_completed_scan_ranges * 100) /
                 summary.progress.total_scan_ranges)
          fragment_text = "[%s%s] %s%%\n" % ("#" * val, " " * (100 - val), val)
          data += fragment_text

        if self.live_summary:
          table = self._default_summary_table()
          output = []
          self.imp_client.build_summary_table(summary, 0, False, 0, False, output)
          formatter = PrettyOutputFormatter(table)
          data += formatter.format(output) + "\n"

      self.progress_stream.write(data)
      self.last_summary = time.time()

  def _default_summary_table(self):
    return self.construct_table_with_header(["Operator", "#Hosts", "#Inst",
                                             "Avg Time", "Max Time", "#Rows",
                                             "Est. #Rows", "Peak Mem",
                                             "Est. Peak Mem", "Detail"])

  def _execute_stmt(self, query_str, is_dml=False, print_web_link=False):
    """Executes 'query_str' with options self.set_query_options on the Impala server.
    The query is run to completion and close with any results, warnings, errors or
    other output that we want to display to users printed to the output.
    Results for queries are streamed from the server and displayed as they become
    available.
    is_dml: True iff the caller detects that 'query_str' is a DML statement, false
            otherwise.
    print_web_link: if True, a link to the query on the Impala debug webserver is printed.
    """
    self._print_if_verbose("Query: %s" % query_str)
    # TODO: Clean up this try block and refactor it (IMPALA-3814)
    try:
      if not self.webserver_address:
        print_web_link = False
      if print_web_link:
        self._print_if_verbose("Query submitted at: %s (Coordinator: %s)" %
            (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
            self.webserver_address))

      start_time = time.time()
      self.last_query_handle = None
      self.last_query_handle = self.imp_client.execute_query(
          query_str, self.set_query_options)
      self.last_summary = time.time()
      if print_web_link:
        self._print_if_verbose(
            "Query progress can be monitored at: %s" % self.imp_client.get_query_link(
             self.imp_client.get_query_id_str(self.last_query_handle)))

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
        if not self.imp_client.expect_result_metadata(query_str, self.last_query_handle):
          # Close the query
          self.imp_client.close_query(self.last_query_handle)
          return CmdStatus.SUCCESS

        self._format_outputstream()
        # fetch returns a generator
        rows_fetched = self.imp_client.fetch(self.last_query_handle)
        num_rows = 0

        for rows in rows_fetched:
          # IMPALA-4418: Break out of the loop to prevent printing an unnecessary empty line.
          if len(rows) == 0:
            continue
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

      if num_rows is not None:
        self._print_if_verbose("%s %d row(s)%s in %2.2fs" %
            (verb, num_rows, error_report, time_elapsed))
      else:
        self._print_if_verbose("Time elapsed: %2.2fs" %
            (time_elapsed))

      if not is_dml:
        self.imp_client.close_query(self.last_query_handle)
      try:
        profile, retried_profile = self.imp_client.get_runtime_profile(
            self.last_query_handle)
        self.print_runtime_profile(profile, retried_profile)
      except RPCException as e:
        if self.show_profiles: raise e
      return CmdStatus.SUCCESS
    except QueryCancelledByShellException as e:
      return CmdStatus.SUCCESS
    except RPCException as e:
      # could not complete the rpc successfully
      print(e, file=sys.stderr)
    except UnicodeDecodeError as e:
      # An error occoured possibly during the fetching.
      # Depending of which protocol is at use it can come from different places.
      # Possibly occours because we try to display binary data which contains
      # undecodable elements.
      if self.last_query_handle is not None:
        self.imp_client.close_query(self.last_query_handle)
      print('UnicodeDecodeError : %s \nPlease check for columns containing binary data '
          'to find the possible source of the error.' % (e,), file=sys.stderr)
    except QueryStateException as e:
      # an exception occurred while executing the query
      if self.last_query_handle is not None:
        self.imp_client.close_query(self.last_query_handle)
      msg = e.value
      # Python2 will implicitly convert unicode to str when printing to stderr. It's done
      # using the default 'ascii' encoding, which will fail for UTF-8 error messages.
      # Here we use 'utf-8' to explicitly convert 'msg' to str if it's in unicode type.
      if sys.version_info.major == 2 and isinstance(msg, unicode):
        msg = msg.encode('utf-8')
      print(msg, file=sys.stderr)
    except DisconnectedException as e:
      # the client has lost the connection
      print(e, file=sys.stderr)
      self.imp_client.connected = False
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    except socket.error as e:
      # if the socket was interrupted, reconnect the connection with the client
      if e.errno == errno.EINTR:
        print(ImpalaShell.CANCELLATION_MESSAGE)
        self._reconnect_cancellation()
      else:
        print("Socket error %s: %s" % (e.errno, e), file=sys.stderr)
        self.prompt = self.DISCONNECTED_PROMPT
        self.imp_client.connected = False
    except Exception as e:
      # if the exception is unknown, there was possibly an issue with the connection
      # set the shell as disconnected
      print('Unknown Exception : %s' % (e,), file=sys.stderr)
      # Print the stack trace for the exception.
      traceback.print_exc()
      self.close_connection()
      self.prompt = ImpalaShell.DISCONNECTED_PROMPT
    return CmdStatus.ERROR


  def construct_table_with_header(self, column_names):
    """ Constructs the table header for a given query handle.

    Should be called after the query has finished and before data is fetched.
    All data is left aligned.
    """
    table = prettytable.PrettyTable()
    for column in column_names:
      table.add_column(column, [])
    table.align = "l"
    return table

  def do_values(self, args):
    """Executes a VALUES(...) query, fetching all rows"""
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_with(self, args):
    """Executes a query with a WITH clause, fetching all rows"""
    query = self._build_query_string(self.last_leading_comment, self.orig_cmd, args)
    # Parse the query with sqlparse to identify if it is a DML query or not.
    # Because the WITH clause may precede DML or SELECT queries, checking the
    # first string token is insufficient.
    parsed = sqlparse.parse(query)[0]
    query_type = sqlparse.sql.Statement(parsed.tokens).get_type()
    try:
      is_dml = self.DML_REGEX.match(query_type.lower())
      return self._execute_stmt(query, bool(is_dml), print_web_link=True)
    except ValueError:
      return self._execute_stmt(query, print_web_link=True)

  def do_use(self, args):
    """Executes a USE... query"""
    cmd_status = self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))
    if cmd_status is CmdStatus.SUCCESS:
      self.current_db = args.strip('`').strip()
      self.set_prompt(self.current_db)
    elif args.strip('`') == self.current_db:
      # args == current_db means -d option was passed but the "use [db]" operation failed.
      # We need to set the current_db to None so that it does not show a database, which
      # may not exist.
      self.current_db = None
      return CmdStatus.ERROR
    else:
      return CmdStatus.ERROR

  def do_show(self, args):
    """Executes a SHOW... query, fetching all rows"""
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_describe(self, args):
    return self.__do_describe(self.orig_cmd, args)

  def do_desc(self, args):
    return self.__do_describe("describe", args)

  def __do_describe(self, cmd, args):
    """Executes a DESCRIBE... query, fetching all rows"""
    # original command should be overridden because the server cannot
    # recognize "desc" as a keyword. Thus, given command should be
    # replaced with "describe" here.
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, cmd, args))

  def __do_dml(self, orig_cmd, args):
    """Executes a DML query"""
    query = self._build_query_string(self.last_leading_comment, orig_cmd, args)
    return self._execute_stmt(query, is_dml=True, print_web_link=True)

  def do_upsert(self, args):
    return self.__do_dml(self.orig_cmd, args)

  def do_update(self, args):
    return self.__do_dml(self.orig_cmd, args)

  def do_delete(self, args):
    return self.__do_dml(self.orig_cmd, args)

  def do_insert(self, args):
    return self.__do_dml(self.orig_cmd, args)

  def do_explain(self, args):
    """Explain the query execution plan"""
    return self._execute_stmt(
        self._build_query_string(self.last_leading_comment, self.orig_cmd, args))

  def do_history(self, args):
    """Display command history"""
    # Deal with readline peculiarity. When history does not exist,
    # readline returns 1 as the history length and stores 'None' at index 0.
    if self.readline and self.readline.get_current_history_length() > 0:
      for index in xrange(1, self.readline.get_current_history_length() + 1):
        cmd = self.readline.get_history_item(index)
        print('[%d]: %s' % (index, cmd.decode('utf-8', 'replace')), file=sys.stderr)
    else:
      print(READLINE_UNAVAILABLE_ERROR, file=sys.stderr)

  def do_rerun(self, args):
    """Rerun a command with an command index in history
    Example: @1;
    """
    history_len = self.readline.get_current_history_length()
    # Rerun command shouldn't appear in history
    self.readline.remove_history_item(history_len - 1)
    history_len -= 1
    if not self.readline:
      print(READLINE_UNAVAILABLE_ERROR, file=sys.stderr)
      return CmdStatus.ERROR
    try:
      cmd_idx = int(args)
    except ValueError:
      print("Command index to be rerun must be an integer.", file=sys.stderr)
      return CmdStatus.ERROR
    if not (0 < cmd_idx <= history_len or -history_len <= cmd_idx < 0):
      print("Command index out of range. Valid range: [1, {0}] and [-{0}, -1]"
                      .format(history_len), file=sys.stderr)
      return CmdStatus.ERROR
    if cmd_idx < 0:
      cmd_idx += history_len + 1
    cmd = self.readline.get_history_item(cmd_idx)
    print("Rerunning " + cmd, file=sys.stderr)
    return self.onecmd(cmd.rstrip(";"))

  def do_tip(self, args):
    """Print a random tip"""
    print(random.choice(TIPS), file=sys.stderr)

  def do_src(self, args):
    return self.do_source(args)

  def do_source(self, args):
    try:
      cmd_file = open(args, "r")
    except Exception as e:
      print("Error opening file '%s': %s" % (args, e), file=sys.stderr)
      return CmdStatus.ERROR
    if self.execute_query_list(parse_query_text(cmd_file.read())):
      return CmdStatus.SUCCESS
    else:
      return CmdStatus.ERROR

  def postloop(self):
    """Save session commands in history."""
    if self.readline:
      try:
        self._replace_history_delimiters('\n', ImpalaShell.HISTORY_FILE_QUERY_DELIM)
        self.readline.write_history_file(self.history_file)
      except IOError as i:
        msg = "Unable to save command history (disabling history collection): %s" % i
        print(msg, file=sys.stderr)
        # The history file is not writable, disable readline.
        self._disable_readline()

  def parseline(self, line):
    """Parse the line into a command name and a string containing
    the arguments.  Returns a tuple containing (command, args, line, leading comment).
    'command' and 'args' may be None if the line couldn't be parsed.
    'line' in return tuple is the rewritten original line, with leading
    and trailing space removed and special characters transformed into
    their aliases. If the line contains a leading comment, the leading
    comment will be removed in order to deduce a 'command' correctly.
    The 'command' is used to determine which 'do_<command>' function to invoke.
    The 'do_<command>' implementation can decide whether to retain or ignore the
    leading comment.

    Examples:

    > /*comment*/ help connect;
    line: help connect
    args: connect
    command: help
    leading comment: /*comment*/

    > /*comment*/ ? connect;
    line: help connect
    args: connect
    command: help
    leading comment: /*comment*/

    > /*first comment*/ select /*second comment*/ 1;
    line: select /*second comment*/ 1
    args: /*second comment*/ 1
    command: select
    leading comment: /*first comment*/
    """
    if ImpalaShell._has_leading_comment(line):
      leading_comment, line = ImpalaShell.strip_leading_comment(line.strip())
    else:
      leading_comment, line = None, line.strip()

    if line and line[0] == '@':
      line = 'rerun ' + line[1:]
    return super(ImpalaShell, self).parseline(line) + (leading_comment,)

  @staticmethod
  def _has_leading_comment(raw_line):
    """
    Helper function that returns Boolean true if a query starts with a comment.
    This saves us from relying on sqlparse filtering, which can be slow.
    """
    line = raw_line.lstrip()
    if line and (line.startswith('--') or line.startswith('/*')):
      return True
    else:
      return False

  @staticmethod
  def strip_leading_comment(sql):
    """
    Filter a leading comment in the SQL statement. This function returns a tuple
    containing (leading comment, line without the leading comment).
    """
    class StripLeadingCommentFilter(object):
      def __init__(self):
        self.comment = None

      def _process(self, tlist):
        """
        Iterate through the list of tokens, appending each leading commment
        to self.comment, and then popping that element off the list. When we
        hit the first non-comment and non-whitespace token, then we're done --
        the remainder after that point is the SQL statement.
        """
        token = tlist.token_first()
        while token:
          if self._is_comment(token) or self._is_whitespace(token):
            if self.comment is None:
              self.comment = token.value
            else:
              self.comment += token.value
            tlist.tokens.pop(0)

            # skip_ws=False treats white space characters as tokens also
            token = tlist.token_first(skip_ws=False)
          else:
            break

      def _is_comment(self, token):
        return isinstance(token, sqlparse.sql.Comment) or \
               token.ttype == sqlparse.tokens.Comment.Single or \
               token.ttype == sqlparse.tokens.Comment.Multiline

      def _is_whitespace(self, token):
        return token.ttype == sqlparse.tokens.Whitespace or \
               token.ttype == sqlparse.tokens.Newline

      def process(self, stmt):
        [self.process(sgroup) for sgroup in stmt.get_sublists()]
        self._process(stmt)

    stack = sqlparse.engine.FilterStack()
    strip_leading_comment_filter = StripLeadingCommentFilter()
    stack.stmtprocess.append(strip_leading_comment_filter)
    stack.postprocess.append(sqlparse.filters.SerializerUnicode())
    stripped_line = ''.join(stack.run(sql, 'utf-8'))
    return strip_leading_comment_filter.comment, stripped_line

  def _replace_history_delimiters(self, src_delim, tgt_delim):
    """Replaces source_delim with target_delim for all items in history.

    Read all the items from history into a local list. Clear the history and copy them
    back after doing the transformation.
    """
    history_len = self.readline.get_current_history_length()
    # load the history and replace the shell's delimiter with EOL
    history_items = map(self.readline.get_history_item, xrange(1, history_len + 1))
    if sys.version_info.major == 2:
      src_delim = src_delim.encode('utf-8')
      tgt_delim = tgt_delim.encode('utf-8')
    history_items = [item.replace(src_delim, tgt_delim) for item in history_items]
    # Clear the original history and replace it with the mutated history.
    self.readline.clear_history()
    for history_item in history_items:
      self.readline.add_history(history_item)

  def default(self, line):
    """Called for any command that doesn't have a do_*() method. Sends the command
    with any arguments and trailing comment to the server."""
    return self._execute_stmt(line, print_web_link=True)

  def emptyline(self):
    """If an empty line is entered, do nothing"""

  def do_version(self, args):
    """Prints the Impala build version"""
    print("Shell version: %s" % VERSION_STRING, file=sys.stderr)
    print("Server version: %s" % self.server_version, file=sys.stderr)

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
      print('Not connected to Impala, could not execute queries.', file=sys.stderr)
      return False
    queries = [self.sanitise_input(q) for q in queries]
    for q in queries:
      if self.onecmd(q) is CmdStatus.ERROR:
        print('Could not execute command: %s' % q, file=sys.stderr)
        if not self.ignore_query_failure: return False
    return True


TIPS = [
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
the delimiter for fields in the same row. The default is '\\t'.",
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


def parse_query_text(query_text):
  """Parse query file text to extract queries"""
  query_list = sqlparse.split(query_text)
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
  if query_list and not strip_comments(query_list[-1]).strip("\n"):
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
        print('Error: Could not parse key-value "%s". ' % (keyval,) +
              'It must follow the pattern "KEY=VALUE".', file=sys.stderr)
        parser.print_help()
        raise FatalShellException()
      else:
        vars[match.groups()[0].upper()] = replace_variables(vars, match.groups()[1])
  return vars


def replace_variables(set_variables, input_string):
  """
  Replaces variable within the input_string with their corresponding values
  from the given set_variables.
  """
  errors = False

  # In the case of a byte sequence, re.findall() will choke under python 3
  if sys.version_info.major > 2:
    if not isinstance(input_string, str):
      input_string = input_string.decode('utf-8')

  matches = set([v.upper() for v in re.findall(r'(?<!\\)\${([^}]+)}', input_string)])
  for name in matches:
    value = None
    # Check if syntax is correct
    var_name = get_var_name(name)
    if var_name is None:
      print('Error: Unknown substitution syntax (%s). ' % (name,) +
            'Use ${VAR:var_name}.', file=sys.stderr)
      errors = True
    else:
      # Replaces variable value
      if set_variables and var_name in set_variables:
        value = set_variables[var_name]
        if value is None:
          errors = True
        else:
          regexp = re.compile(r'(?<!\\)\${%s}' % (name,), re.IGNORECASE)
          input_string = regexp.sub(value, input_string)
      else:
        print('Error: Unknown variable %s' % (var_name), file=sys.stderr)
        errors = True
  if errors:
    return None
  else:
    return input_string


def get_var_name(name):
  """Looks for a namespace:var_name pattern in an option name.
     Returns the variable name if it's a match or None otherwise.
  """
  ns_match = re.match(r'^([^:]*):(.*)', name)
  if ns_match is not None:
    ns = ns_match.group(1)
    var_name = ns_match.group(2)
    if ns in ImpalaShell.VAR_PREFIXES:
      return var_name
  return None


def execute_queries_non_interactive_mode(options, query_options):
  """Run queries in non-interactive mode. Return True on success. Logs the
  error and returns False otherwise."""
  if options.query_file:
    # "-" here signifies input from STDIN
    if options.query_file == "-":
      query_text = sys.stdin.read()
    else:
      try:
        with open(options.query_file, 'r') as query_file_handle:
          query_text = query_file_handle.read()
      except Exception as e:
        print("Could not open file '%s': %s" % (options.query_file, e), file=sys.stderr)
        return False
  elif options.query:
    query_text = options.query
  else:
    return True

  queries = parse_query_text(query_text)
  with ImpalaShell(options, query_options) as shell:
    return (shell.execute_query_list(shell.cmdqueue) and
            shell.execute_query_list(queries))


def get_intro(options):
  """Get introduction message for start-up. The last character should not be a return."""
  if not options.verbose:
    return ""

  intro = WELCOME_STRING

  if not options.ssl and options.creds_ok_in_clear and options.use_ldap:
    intro += ("\n\nLDAP authentication is enabled, but the connection to Impala is "
              "not secured by TLS.\nALL PASSWORDS WILL BE SENT IN THE CLEAR TO IMPALA.")

  if options.protocol == 'beeswax':
    intro += ("\n\nWARNING: The beeswax protocol is deprecated and will be removed in a "
              "future version of Impala.")

  return intro


def impala_shell_main():
  """
  There are two types of options: shell options and query_options. Both can be set on the
  command line, which override default options. Specifically, if there exists a global
  config file (default path: /etc/impalarc) then options are loaded from that file. If
  there exists a user config file (~/.impalarc), then options are loaded in from that
  file and override any options already loaded from the global impalarc. The default shell
  options come from impala_shell_config_defaults.py. Query options have no defaults within
  the impala-shell, but they do have defaults on the server. Query options can be also
  changed in impala-shell with the 'set' command.
  """
  # pass defaults into option parser
  global options, parser
  parser = get_option_parser(impala_shell_defaults)
  options, args = parser.parse_args()

  # by default, use the impalarc in the user's home directory
  # and superimpose it on the global impalarc config
  global_config = os.path.expanduser(
    os.environ.get(ImpalaShell.GLOBAL_CONFIG_FILE,
                   impala_shell_defaults['global_config_default_path']))
  if os.path.isfile(global_config):
    # Always output the source of the global config if verbose
    if options.verbose:
      print(
        "Loading in options from global config file: %s \n" % global_config,
        file=sys.stderr)
  elif global_config != impala_shell_defaults['global_config_default_path']:
    print('%s not found.\n' % global_config, file=sys.stderr)
    raise FatalShellException()
  # Override the default user config by a custom config if necessary
  user_config = impala_shell_defaults.get("config_file")
  input_config = os.path.expanduser(options.config_file)
  # verify input_config, if found
  if input_config != user_config:
    if os.path.isfile(input_config):
      if options.verbose:
        print("Loading in options from config file: %s \n" % input_config,
              file=sys.stderr)
      # command line overrides loading ~/.impalarc
      user_config = input_config
    else:
      print('%s not found.\n' % input_config, file=sys.stderr)
      raise FatalShellException()
  configs_to_load = [global_config, user_config]

  # load shell and query options from the list of config files
  # in ascending order of precedence
  try:
    loaded_shell_options = {}
    query_options = {}
    for config_file in configs_to_load:
      s_options, q_options = get_config_from_file(config_file,
                                                  parser.option_list)
      loaded_shell_options.update(s_options)
      query_options.update(q_options)

    impala_shell_defaults.update(loaded_shell_options)
  except Exception as e:
    print(e, file=sys.stderr)
    raise FatalShellException()

  parser = get_option_parser(impala_shell_defaults)
  options, args = parser.parse_args()

  # Arguments that could not be parsed are stored in args. Print an error and exit.
  if len(args) > 0:
    print('Error, could not parse arguments "%s"' % (' ').join(args), file=sys.stderr)
    parser.print_help()
    raise FatalShellException()

  if options.version:
    print(VERSION_STRING)
    return

  if options.write_delimited:
    if isinstance(options.output_delimiter, str):
      delim_sequence = bytearray(options.output_delimiter, 'utf-8')
    else:
      delim_sequence = options.output_delimiter
    delim = delim_sequence.decode('unicode_escape')
    if len(delim) != 1:
      print("Illegal delimiter %s, the delimiter "
            "must be a 1-character string." % delim, file=sys.stderr)
      raise FatalShellException()

  if options.use_kerberos and options.use_ldap:
    print("Please specify at most one authentication mechanism (-k or -l)",
          file=sys.stderr)
    raise FatalShellException()

  if not options.ssl and not options.creds_ok_in_clear and options.use_ldap:
    print("LDAP credentials may not be sent over insecure " +
          "connections. Enable SSL or set --auth_creds_ok_in_clear",
          file=sys.stderr)
    raise FatalShellException()

  if not options.use_ldap and options.ldap_password_cmd:
    print("Option --ldap_password_cmd requires using LDAP authentication " +
          "mechanism (-l)", file=sys.stderr)
    raise FatalShellException()

  start_msg = "Starting Impala Shell"

  py_version_msg = "using Python {0}.{1}.{2}".format(
    sys.version_info.major, sys.version_info.minor, sys.version_info.micro)

  if options.use_kerberos:
    if options.verbose:
      kerb_msg = "with Kerberos authentication"
      print("{0} {1} {2}".format(start_msg, kerb_msg, py_version_msg), file=sys.stderr)
      print("Using service name '%s'" % options.kerberos_service_name, file=sys.stderr)
    # Check if the user has a ticket in the credentials cache
    try:
      if call(['klist', '-s']) != 0:
        print(("-k requires a valid kerberos ticket but no valid kerberos "
               "ticket found."), file=sys.stderr)
        raise FatalShellException()
    except OSError as e:
      print('klist not found on the system, install kerberos clients', file=sys.stderr)
      raise FatalShellException()
  elif options.use_ldap:
    if options.verbose:
      ldap_msg = "with LDAP-based authentication"
      print("{0} {1} {2}".format(start_msg, ldap_msg, py_version_msg), file=sys.stderr)
  else:
    if options.verbose:
      no_auth_msg = "with no authentication"
      print("{0} {1} {2}".format(start_msg, no_auth_msg, py_version_msg), file=sys.stderr)

  options.ldap_password = None
  if options.use_ldap and options.ldap_password_cmd:
    try:
      p = subprocess.Popen(shlex.split(options.ldap_password_cmd), stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
      options.ldap_password, stderr = p.communicate()
      if p.returncode != 0:
        print("Error retrieving LDAP password (command was '%s', error was: "
              "'%s')" % (options.ldap_password_cmd, stderr.strip()), file=sys.stderr)
        raise FatalShellException()
    except Exception as e:
      print("Error retrieving LDAP password (command was: '%s', exception "
            "was: '%s')" % (options.ldap_password_cmd, e), file=sys.stderr)
      raise FatalShellException()

  if options.ssl:
    if options.ca_cert is None:
      if options.verbose:
        print("SSL is enabled. Impala server certificates will NOT be verified "
              "(set --ca_cert to change)", file=sys.stderr)
    else:
      if options.verbose:
        print("SSL is enabled", file=sys.stderr)

  if options.output_file:
    try:
      # Make sure the given file can be opened for writing. This will also clear the file
      # if successful.
      open(options.output_file, 'wb')
    except IOError as e:
      print('Error opening output file for writing: %s' % e, file=sys.stderr)
      raise FatalShellException()

  if options.http_socket_timeout_s is not None:
    if (options.http_socket_timeout_s != 'None' and
          float(options.http_socket_timeout_s) < 0):
        print("http_socket_timeout_s must be a nonnegative floating point number"
              " expressing seconds, or None", file=sys.stderr)
        raise FatalShellException()

  options.variables = parse_variables(options.keyval)

  # Override query_options from config file with those specified on the command line.
  query_options.update(
     [(k.upper(), v) for k, v in parse_variables(options.query_options).items()])

  # Non-interactive mode
  if options.query or options.query_file:
    # Impala shell will disable live_progress if non-interactive mode is detected.
    if options.live_progress:
      if options.verbose:
        print("Warning: live_progress only applies to interactive shell sessions, "
              "and is being skipped for now.", file=sys.stderr)
      options.live_progress = False
    if options.live_summary:
      print("Error: live_summary is available for interactive mode only.",
            file=sys.stderr)
      raise FatalShellException()

    if execute_queries_non_interactive_mode(options, query_options):
      return
    else:
      raise FatalShellException()

  intro = get_intro(options)

  with ImpalaShell(options, query_options) as shell:
    while shell.is_alive:
      try:
        try:
          shell.cmdloop(intro)
        except KeyboardInterrupt:
          print('^C', file=sys.stderr)
        # A last measure against any exceptions thrown by an rpc
        # not caught in the shell
        except socket.error as e:
          # if the socket was interrupted, reconnect the connection with the client
          if e.errno == errno.EINTR:
            print(shell.CANCELLATION_MESSAGE)
            shell._reconnect_cancellation()
          else:
            print("Socket error %s: %s" % (e.errno, e), file=sys.stderr)
            shell.imp_client.connected = False
            shell.prompt = shell.DISCONNECTED_PROMPT
        except DisconnectedException as e:
          # the client has lost the connection
          print(e, file=sys.stderr)
          shell.imp_client.connected = False
          shell.prompt = shell.DISCONNECTED_PROMPT
        except QueryStateException as e:
          # an exception occurred while executing the query
          shell.imp_client.close_query(shell.last_query_handle)
          print(e, file=sys.stderr)
        except RPCException as e:
          # could not complete the rpc successfully
          print(e, file=sys.stderr)
        except IOError as e:
          # Interrupted system calls (e.g. because of cancellation) should be ignored.
          if e.errno != errno.EINTR: raise
      finally:
        intro = ''


if __name__ == "__main__":
  try:
    impala_shell_main()
  except FatalShellException:
    # Ensure that fatal errors cause a clean exit with error.
    sys.exit(1)
