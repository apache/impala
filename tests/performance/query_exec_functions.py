# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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

import logging
import re
from datetime import datetime
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient, ImpalaBeeswaxResult
from tests.performance.query import Query, QueryResult
from tests.performance.query_executor import (
    QueryExecConfig,
    ImpalaQueryExecConfig,
    JdbcQueryExecConfig,
    BeeswaxQueryExecConfig
    )
from tests.util.shell_util import exec_process

DEFAULT_BEESWAX_PORT = 21000
DEFAULT_HS2_PORT = 21050

logging.basicConfig(level=logging.INFO, format='[%(name)s] %(threadName)s: %(message)s')
LOG = logging.getLogger('query_exec_functions')

def establish_beeswax_connection(query, query_config):
  """Establish a connection to the user specified impalad.

  Args:
    query_config (QueryExecConfig)

  Returns:
    (boolean, ImpalaBeeswaxClient): True if successful
  """
  use_kerberos = query_config.use_kerberos
  # If the impalad is for the form host, convert it to host:port that the Impala beeswax
  # client accepts.
  if len(query_config.impalad.split(":")) == 1:
    query_config.impalad = "{0}:{1}".format(query_config.impalad, DEFAULT_BEESWAX_PORT)
  client = ImpalaBeeswaxClient(query_config.impalad, use_kerberos=use_kerberos)
  # Try connect
  client.connect()
  # Set the exec options.
  client.set_query_options(query_config.exec_options)
  LOG.info("Connected to %s" % query_config.impalad)
  return (True, client)

def execute_using_impala_beeswax(query, query_config):
  """Executes a query using beeswax.

  A new client is created per query, then destroyed.

  Args:
    query (str): string containing the query to be executed.
    query_config (QueryExecConfig)

  Returns:
    QueryResult
  """

  # Create a client object to talk to impalad
  exec_result = QueryResult(query, query_config=query_config)
  plugin_runner = query_config.plugin_runner
  (success, client) = establish_beeswax_connection(query.query_str, query_config)
  if not success: return exec_result
  # We need to issue a use database here.
  if query.db:
    use_query = 'use %s' % query.db
    client.execute(use_query)
  # create a map for query options and the query names to send to the plugin
  context = build_context(query, query_config)
  if plugin_runner: plugin_runner.run_plugins_pre(context=context, scope="Query")
  result = ImpalaBeeswaxResult()
  try:
    result = client.execute(query.query_str)
  except Exception, e:
    LOG.error(e)
    exec_result.query_error = str(e)
  finally:
    client.close_connection()
    if plugin_runner: plugin_runner.run_plugins_post(context=context, scope="Query")
    return construct_exec_result(result, exec_result)

def build_context(query, query_config):
  """Build context based on query config for plugin_runner.

  Why not pass QueryExecConfig to plugins directly?

  Args:
    query (str)
    query_config (QueryExecConfig)

  Returns:
    dict str -> str
  """

  context = vars(query_config)
  context['query'] = query
  return context

def construct_exec_result(result, exec_result):
  """ Transform an ImpalaBeeswaxResult object to a QueryResult object.

  Args:
    result (ImpalaBeeswasResult): Tranfers data from here.
    exec_result (QueryResult): Transfers data to here.

  Returns:
    QueryResult
  """

  # Return immedietely if the query failed.
  if not result.success: return exec_result
  exec_result.success = True
  attrs = ['data', 'runtime_profile', 'start_time',
      'time_taken', 'summary', 'exec_summary']
  for attr in attrs:
    setattr(exec_result, attr, getattr(result, attr))
  return exec_result

def execute_using_jdbc(query, query_config):
  """Executes a query using JDBC"""
  query_string = query.query_str + ';'
  if query.db:
    query_string = 'use %s; %s' % (query.db, query_string)
  cmd = query_config.jdbc_client_cmd + " -q \"%s\"" % query_string
  return run_query_capture_results(cmd, query, exit_on_error=False)

def parse_jdbc_query_results(stdout, stderr):
  """
  Parse query execution results for the Impala JDBC client

  Parses the query execution details (avg time, stddev) from the output of the Impala
  JDBC test client.
  """
  jdbc_result_regex = 'row\(s\) in (\d*).(\d*)s'
  time_taken = 0.0
  for line in stdout.split('\n'):
    match = re.search(jdbc_result_regex, line)
    if match:
      time_taken = float(('%s.%s') % (match.group(1), match.group(2)))
      break
  result_data = re.findall(r'\[START\]----\n(.*?)\n----\[END\]', stdout, re.DOTALL)[0]
  return create_exec_result(time_taken, result_data)

def create_exec_result(time_taken, result_data):
  exec_result = QueryResult()
  if result_data:
    LOG.debug('Data:\n%s\n' % result_data)
    exec_result.data = result_data
  exec_result.time_taken = time_taken
  exec_result.success = True
  return exec_result

def run_query_capture_results(cmd, query, exit_on_error):
  """
  Runs the given query command and returns the execution result.

  Takes in a match function that is used to parse stderr/stdout to extract the results.
  """
  exec_result = QueryResult(query)
  start_time = datetime.now()
  try:
    rc, stdout, stderr = exec_process(cmd)
  except Exception, e:
    LOG.error('Error while executing query command: %s' % e)
    exec_result.query_error = str(e)
    # TODO: Should probably save the start time and query string for failed queries.
    return exec_result
  if rc != 0:
    msg = ('Command returned with an error:\n'
           'rc: %d\n'
           'STDERR:\n%s'
           'STDOUT:\n%s'
           % (rc, stderr, stdout))
    LOG.error(msg)
    exec_result.query_error = msg
    return exec_result
  # The command completed
  exec_result = parse_jdbc_query_results(stdout, stderr)
  exec_result.query = query
  exec_result.start_time = start_time
  return exec_result
