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

# Example .impalarc file:
#
# [impala]
# impalad=localhost:21002
# verbose=false
#
# [impala.query_options]
# EXPLAIN_LEVEL=2
# MT_DOP=2
from __future__ import print_function, unicode_literals

import sys

try:
  from configparser import ConfigParser  # python3
except ImportError:
  from ConfigParser import ConfigParser  # python2

from impala_shell_config_defaults import impala_shell_defaults
from optparse import OptionParser, SUPPRESS_HELP


class ConfigFileFormatError(Exception):
  """Raised when the config file cannot be read by ConfigParser."""
  pass


class InvalidOptionValueError(Exception):
  """Raised when an option contains an invalid value."""
  pass


def parse_bool_option(value):
  """Returns True for '1' and 'True', and False for '0' and 'False'.
     Throws ValueError for other values.
  """
  if value.lower() in ["true", "1"]:
    return True
  elif value.lower() in ["false", "0"]:
    return False
  else:
    raise InvalidOptionValueError("Unexpected value in configuration file. '" + value
      + "' is not a valid value for a boolean option.")


def parse_shell_options(options, defaults, option_list):
  """Filters unknown options and converts some values from string to their corresponding
     python types (booleans and None).  'option_list' contains the list of valid options,
     and 'defaults' is used to deduce the type of some options (only bool at the moment).

     Returns a dictionary with option names as keys and option values as values.
  """
  # Build a dictionary that maps short and long option name to option for a quick lookup.
  option_dests = dict()
  for option in option_list:
    if len(option._short_opts) > 0:
      option_dests[option._short_opts[0][1:]] = option
    if len(option._long_opts) > 0:
      option_dests[option._long_opts[0][2:]] = option
    if option.dest not in option_dests:
      # Allowing dest name for backward compatibility.
      option_dests[option.dest] = option

  result = {}
  for option, value in options:
    opt = option_dests.get(option)
    if opt is None:
      warn_msg = (
        "WARNING: Unable to read configuration file correctly. "
        "Ignoring unrecognized config option: '%s'" % option
      )
      print('\n{0}'.format(warn_msg), file=sys.stderr)
    elif isinstance(defaults.get(option), bool) or \
        opt.action == "store_true" or opt.action == "store_false":
      result[option] = parse_bool_option(value)
    elif opt.action == "append":
      result[option] = value.split(",%s=" % option)
    elif value.lower() == "none":
      result[option] = None
    else:
      result[option] = value
  return result


def get_config_from_file(config_filename, option_list):
  """Reads contents of configuration file

  Two config sections are supported:
  "[impala]":
  Overrides the defaults of the shell arguments. Unknown options are filtered
  and some values are converted from string to their corresponding python types
  (booleans and None).

  Multiple flags are appended with ",option_name=" as its delimiter, e.g.
  The delimiter is for multiple options is ,<option>=. For example:
  var=msg1=hello,var=msg2=world.

  Setting 'config_filename' in the config file would have no effect,
  so its original value is kept.

  "[impala.query_options]"
  Overrides the defaults of the query options. Not validated here,
  because validation will take place after connecting to impalad.

  Returns a pair of dictionaries (shell_options, query_options), with option names
  as keys and option values as values.
  """
  try:
    config = ConfigParser(strict=False)  # python3
  except TypeError:
    config = ConfigParser()  # python2

  # Preserve case-sensitivity since flag names are case sensitive.
  config.optionxform = str
  try:
    config.read(config_filename)
  except Exception as e:
    raise ConfigFileFormatError(
      "Unable to read configuration file correctly. Check formatting: %s" % e)

  shell_options = {}
  if config.has_section("impala"):
    shell_options = parse_shell_options(config.items("impala"), impala_shell_defaults,
                                        option_list)
    if "config_file" in shell_options:
      warn_msg = "WARNING: Option 'config_file' can be only set from shell."
      print('\n{0}'.format(warn_msg), file=sys.stderr)
      shell_options["config_file"] = config_filename

  query_options = {}
  if config.has_section("impala.query_options"):
    # Query option keys must be "normalized" to upper case before updating with
    # options coming from command line.
    query_options = dict(
      [(k.upper(), v) for k, v in config.items("impala.query_options")])
  return shell_options, query_options


def get_option_parser(defaults):
  """Creates OptionParser and adds shell options (flags)

  Default values are loaded in initially
  """

  parser = OptionParser()
  parser.add_option("-i", "--impalad", dest="impalad",
                    help="<host:port> of impalad to connect to \t\t")
  parser.add_option("-b", "--kerberos_host_fqdn", dest="kerberos_host_fqdn",
                    help="If set, overrides the expected hostname of the Impalad's "
                         "kerberos service principal. impala-shell will check that "
                         "the server's principal matches this hostname. This may be "
                         "used when impalad is configured to be accessed via a "
                         "load-balancer, but it is desired for impala-shell to talk "
                         "to a specific impalad directly.")
  parser.add_option("-q", "--query", dest="query",
                    help="Execute a query without the shell")
  parser.add_option("-f", "--query_file", dest="query_file",
                    help="Execute the queries in the query file, delimited by ;."
                         " If the argument to -f is \"-\", then queries are read from"
                         " stdin and terminated with ctrl-d.")
  parser.add_option("-k", "--kerberos", dest="use_kerberos",
                    action="store_true", help="Connect to a kerberized impalad")
  parser.add_option("-o", "--output_file", dest="output_file",
                    help=("If set, query results are written to the "
                          "given file. Results from multiple semicolon-terminated "
                          "queries will be appended to the same file"))
  parser.add_option("-B", "--delimited", dest="write_delimited",
                    action="store_true",
                    help="Output rows in delimited mode")
  parser.add_option("--print_header", dest="print_header",
                    action="store_true",
                    help="Print column names in delimited mode"
                         " when pretty-printed.")
  parser.add_option("--output_delimiter", dest="output_delimiter",
                    help="Field delimiter to use for output in delimited mode")
  parser.add_option("-s", "--kerberos_service_name",
                    dest="kerberos_service_name",
                    help="Service name of a kerberized impalad")
  parser.add_option("-V", "--verbose", dest="verbose",
                    action="store_true",
                    help="Verbose output")
  parser.add_option("-p", "--show_profiles", dest="show_profiles",
                    action="store_true",
                    help="Always display query profiles after execution")
  parser.add_option("--quiet", dest="verbose",
                    action="store_false",
                    help="Disable verbose output")
  parser.add_option("-v", "--version", dest="version",
                    action="store_true",
                    help="Print version information")
  parser.add_option("-c", "--ignore_query_failure", dest="ignore_query_failure",
                    action="store_true", help="Continue on query failure")
  parser.add_option("-d", "--database", dest="default_db",
                    help="Issues a use database command on startup \t")
  parser.add_option("-l", "--ldap", dest="use_ldap",
                    action="store_true",
                    help="Use LDAP to authenticate with Impala. Impala must be configured"
                    " to allow LDAP authentication. \t\t")
  parser.add_option("-u", "--user", dest="user",
                    help="User to authenticate with.")
  parser.add_option("--ssl", dest="ssl",
                    action="store_true",
                    help="Connect to Impala via SSL-secured connection \t")
  parser.add_option("--ca_cert", dest="ca_cert",
                    help=("Full path to "
                    "certificate file used to authenticate Impala's SSL certificate."
                    " May either be a copy of Impala's certificate (for self-signed "
                    "certs) or the certificate of a trusted third-party CA. If not set, "
                    "but SSL is enabled, the shell will NOT verify Impala's server "
                    "certificate"))
  parser.add_option("--config_file", dest="config_file",
                    help=("Specify the configuration file to load options. "
                          "The following sections are used: [impala], "
                          "[impala.query_options]. Section names are case sensitive. "
                          "Specifying this option within a config file will have "
                          "no effect. Only specify this as an option in the commandline."
                          ))
  parser.add_option("--history_file", dest="history_file",
                    help=("The file in which to store shell history. This may also be "
                          "configured using the IMPALA_HISTFILE environment variable."))
  parser.add_option("--live_summary", dest="live_summary", action="store_true",
                    help="Print a query summary every 1s while the query is running.")
  parser.add_option("--live_progress", dest="live_progress", action="store_true",
                    help="Print a query progress every 1s while the query is running."
                         " The default value of the flag is True in the interactive mode."
                         " If live_progress is set to False in a config file, this flag"
                         " will override it")
  parser.add_option("--disable_live_progress", dest="live_progress", action="store_false",
                    help="A command line flag allows users to disable live_progress in"
                         " the interactive mode.")
  parser.add_option("--auth_creds_ok_in_clear", dest="creds_ok_in_clear",
                    action="store_true", help="If set, LDAP authentication " +
                    "may be used with an insecure connection to Impala. " +
                    "WARNING: Authentication credentials will therefore be sent " +
                    "unencrypted, and may be vulnerable to attack.")
  parser.add_option("--ldap_password_cmd", dest="ldap_password_cmd",
                    help="Shell command to run to retrieve the LDAP password")
  parser.add_option("--var", dest="keyval", action="append",
                    help="Defines a variable to be used within the Impala session."
                         " Can be used multiple times to set different variables."
                         " It must follow the pattern \"KEY=VALUE\","
                         " KEY starts with an alphabetic character and"
                         " contains alphanumeric characters or underscores.")
  parser.add_option("-Q", "--query_option", dest="query_options", action="append",
                    help="Sets the default for a query option."
                         " Can be used multiple times to set different query options."
                         " It must follow the pattern \"KEY=VALUE\","
                         " KEY must be a valid query option. Valid query options "
                         " can be listed by command 'set'.")
  parser.add_option("-t", "--client_connect_timeout_ms",
                    help="Timeout in milliseconds after which impala-shell will time out"
                    " if it fails to connect to Impala server. Set to 0 to disable any"
                    " timeout.")
  parser.add_option("--http_socket_timeout_s",
                    help="Timeout in seconds after which the socket will time out"
                    " if the associated operation cannot be completed. Set to None to"
                    " disable any timeout. Only supported for hs2-http mode.")
  parser.add_option("--protocol", dest="protocol", default="hs2",
                    help="Protocol to use for client/server connection. Valid inputs are "
                         "['hs2', 'hs2-http', 'beeswax']. 'hs2-http' uses HTTP transport "
                         "to speak to the coordinator while 'hs2' and 'beeswax' use the "
                         "binary TCP based transport. Beeswax support is deprecated "
                         "and will be removed in the future.")
  parser.add_option("--strict_hs2_protocol", dest="strict_hs2_protocol",
                    action="store_true",
                    help="True if the hs2 connection is using the strict hs2 protocol."
                         "Only useful if connecting straight to hs2 instead of Impala."
                         "The default hs2 port is 11050 and the default hs2 http port "
                         "is 10001.")
  parser.add_option("--use_ldap_test_password", dest="use_ldap_test_password",
                    action="store_true",
                    help="True if need to use the default LDAP password. This is needed "
                         "when running tests in strict mode.")
  parser.add_option("--http_path", dest="http_path", default="cliservice",
                    help="Default http path on the coordinator to connect to. The final "
                    "connection URL looks like <http(s)>://<coordinator-host>:<port>/"
                    "<http_path>. While the coordinator server implementation does not "
                    "enforce any http path for the incoming requests, deployments could "
                    "still put it behind a loadbalancer that can expect the traffic at a "
                    "certain path.")
  parser.add_option("--fetch_size", type="int", dest="fetch_size", default=10240,
                    help="The fetch size when fetching rows from the Impala coordinator. "
                    "The fetch size controls how many rows a single fetch RPC request "
                    "(RPC from the Impala shell to the Impala coordinator) reads at a "
                    "time. This option is most effective when result spooling is enabled "
                    "('spool_query_results'=true). When result spooling is enabled "
                    "values over the batch_size are honored. When result spooling is "
                    "disabled, values over the batch_size have no affect. By default, "
                    "the fetch_size is set to 10240 which is equivalent to 10 row "
                    "batches (assuming the default batch size). Note that if result "
                    "spooling is disabled only a single row batch can be fetched at a "
                    "time regardless of the specified fetch_size.")
  parser.add_option("--http_cookie_names", dest="http_cookie_names",
                    default="impala.auth,impala.session.id",
                    help="A comma-separated list of HTTP cookie names that are supported "
                    "by the impala-shell. If a cookie with one of these names is "
                    "returned in an http response by the server or an intermediate proxy "
                    "then it will be included in each subsequent request for the same "
                    "connection.")


  # add default values to the help text
  for option in parser.option_list:
    if option.dest is not None:
      # option._short_opts returns a list of short options, e.g. ["-Q"].
      # option._long_opts returns a list of long options, e.g. ["--query_option"].
      # The code below removes the - from the short option and -- from the long option.
      short_opt = option._short_opts[0][1:] if len(option._short_opts) > 0 else None
      long_opt = option._long_opts[0][2:] if len(option._long_opts) > 0 else None
      # In order to set the default flag values, optparse requires the keys to be the
      # dest names. The default flag values are set in impala_shell_config_defaults.py and
      # the default flag values may contain default values that are not for flags.
      if short_opt in defaults:
        if option.dest not in defaults:
          defaults[option.dest] = defaults[short_opt]
        elif type(defaults[option.dest]) == list:
          defaults[option.dest].extend(defaults[short_opt])
      elif long_opt in defaults:
        if option.dest not in defaults:
          defaults[option.dest] = defaults[long_opt]
        elif type(defaults[option.dest]) == list:
          defaults[option.dest].extend(defaults[long_opt])

    # since the quiet flag is the same as the verbose flag
    # we need to make sure to print the opposite value for it
    # (print quiet is false since verbose is true)
    if option == parser.get_option('--quiet'):
      option.help += " [default: %s]" % (not defaults['verbose'])
    # print default value of disable_live_progress in the help messages as opposite
    # value for default value of live_progress
    # (print disable_live_progress is false since live_progress is true)
    elif option == parser.get_option('--disable_live_progress'):
      option.help += " [default: %s]" % (not defaults['live_progress'])
    elif option != parser.get_option('--help') and option.help is not SUPPRESS_HELP:
      # don't want to print default value for help or options without help text
      option.help += " [default: %default]"

  # mutually exclusive flags should not be used in the same time
  if '--live_progress' in sys.argv and '--disable_live_progress' in sys.argv:
    parser.error("options --live_progress and --disable_live_progress are mutually "
                 "exclusive")

  if '--strict_hs2_protocol' in sys.argv:
    if '--live_progress' in sys.argv:
      parser.error("options --strict_hs2_protocol does not support --live_progress")
    if '--live_summary' in sys.argv:
      parser.error("options --strict_hs2_protocol does not support --live_summary")

  if '--verbose' in sys.argv and '--quiet' in sys.argv:
    parser.error("options --verbose and --quiet are mutually exclusive")

  parser.set_defaults(**defaults)

  return parser
