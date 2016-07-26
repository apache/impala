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
# [Options]
# impalad=localhost:21002
# verbose=false
# refresh_after_connect=true
#

import ConfigParser
from impala_shell_config_defaults import impala_shell_defaults
from optparse import OptionParser

def get_config_from_file(config_filename):
  """Reads contents of configuration file

  Validates some values (False, True, None)
  because ConfigParser reads values as strings

  """

  config = ConfigParser.ConfigParser()
  config.read(config_filename)
  section_title = "impala"
  if config.has_section(section_title):
    loaded_options = config.items(section_title);

    for i, (option, value) in enumerate(loaded_options):
      if impala_shell_defaults[option] in [True, False]:
        # validate the option if it can only be a boolean value
        # the only choice for these options is true or false
        if value.lower() == "true":
          loaded_options[i] = (option, True)
        elif value.lower() == 'false':
          loaded_options[i] = (option, False)
        else:
          # if the option is not set to either true or false, use the default
          loaded_options[i] = (option, impala_shell_defaults[option])
      elif value.lower() == "none":
        loaded_options[i] = (option, None)
      elif option.lower() == "config_file":
        loaded_options[i] = (option, config_filename)
      else:
        loaded_options[i] = (option, value)
  else:
    loaded_options = [];
  return loaded_options

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
                    help="Execute the queries in the query file, delimited by ;")
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
                          "File must have case-sensitive '[impala]' header. "
                          "Specifying this option within a config file will have "
                          "no effect. Only specify this as a option in the commandline."
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
                    help="Define variable(s) to be used within the Impala session.")

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
