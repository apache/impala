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

# Example .impalarc file:
#
# [impala]
# impalad=localhost:21002
# verbose=false
# refresh_after_connect=true
#
# [impala.query_options]
# EXPLAIN_LEVEL=2
# MT_DOP=2

import ConfigParser
import sys
from impala_shell_config_defaults import impala_shell_defaults
from optparse import OptionParser

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
    raise InvalidOptionValueError("Unexpected value in configuration file. '" + value \
      + "' is not a valid value for a boolean option.")

def parse_shell_options(options, defaults):
  """Filters unknown options and converts some values from string to their corresponding
     python types (booleans and None).

     Returns a dictionary with option names as keys and option values as values.
  """
  result = {}
  for option, value in options:
    if option not in defaults:
      print >> sys.stderr, "WARNING: Unable to read configuration file correctly. " \
        "Ignoring unrecognized config option: '%s'\n" % option
    elif isinstance(defaults[option], bool):
      result[option] = parse_bool_option(value)
    elif value.lower() == "none":
      result[option] = None
    else:
      result[option] = value
  return result

def get_config_from_file(config_filename):
  """Reads contents of configuration file

  Two config sections are supported:
  "[impala]":
  Overrides the defaults of the shell arguments. Unknown options are filtered
  and some values are converted from string to their corresponding python types
  (booleans and None).

  Setting 'config_filename' in the config file would have no effect,
  so its original value is kept.

  "[impala.query_options]"
  Overrides the defaults of the query options. Not validated here,
  because validation will take place after connecting to impalad.

  Returns a pair of dictionaries (shell_options, query_options), with option names
  as keys and option values as values.
  """

  config = ConfigParser.ConfigParser()
  try:
    config.read(config_filename)
  except Exception, e:
    raise ConfigFileFormatError( \
      "Unable to read configuration file correctly. Check formatting: %s" % e)

  shell_options = {}
  if config.has_section("impala"):
    shell_options = parse_shell_options(config.items("impala"), impala_shell_defaults)
    if "config_file" in shell_options:
      print >> sys.stderr, "WARNING: Option 'config_file' can be only set from shell."
      shell_options["config_file"] = config_filename

  query_options = {}
  if config.has_section("impala.query_options"):
    # Query option keys must be "normalized" to upper case before updating with
    # options coming from command line.
    query_options = dict( \
      [ (k.upper(), v) for k, v in config.items("impala.query_options") ])

  return shell_options, query_options

def get_option_parser(defaults):
  """Creates OptionParser and adds shell options (flags)

  Default values are loaded in initially
  """

  parser = OptionParser()
  parser.set_defaults(**defaults)

  parser.add_option("-i", "--impalad", dest="impalad",
                    help="<host:port> of impalad to connect to \t\t")
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
  parser.add_option("-r", "--refresh_after_connect", dest="refresh_after_connect",
                    action="store_true",
                    help="Refresh Impala catalog after connecting \t")
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
  parser.add_option("--live_summary", dest="print_summary", action="store_true",
                    help="Print a query summary every 1s while the query is running.")
  parser.add_option("--live_progress", dest="print_progress", action="store_true",
                    help="Print a query progress every 1s while the query is running.")
  parser.add_option("--auth_creds_ok_in_clear", dest="creds_ok_in_clear",
                    action="store_true", help="If set, LDAP authentication " +
                    "may be used with an insecure connection to Impala. " +
                    "WARNING: Authentication credentials will therefore be sent " +
                    "unencrypted, and may be vulnerable to attack.")
  parser.add_option("--ldap_password_cmd",
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

  # add default values to the help text
  for option in parser.option_list:
    # since the quiet flag is the same as the verbose flag
    # we need to make sure to print the opposite value for it
    # (print quiet is false since verbose is true)
    if option == parser.get_option('--quiet'):
      option.help += " [default: %s]" % (not defaults['verbose'])
    elif option != parser.get_option('--help'):
      # don't want to print default value for help
      option.help += " [default: %default]"

  return parser
