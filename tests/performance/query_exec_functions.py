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

from __future__ import absolute_import, division, print_function
import logging
import re
from datetime import datetime
from impala.dbapi import connect
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient, ImpalaBeeswaxResult
from sys import maxsize
from tests.performance.query import HiveQueryResult, ImpalaQueryResult
from tests.util.shell_util import exec_process
from time import time
import threading

DEFAULT_BEESWAX_PORT = 21000
DEFAULT_HS2_PORT = 21050
DEFAULT_HIVE_HS2_PORT = 10000

LOG = logging.getLogger('query_exec_functions')

def get_hs2_hive_cursor(hiveserver, user=None, use_kerberos=False,
                        database=None, execOptions=None):
  host, port = hiveserver, DEFAULT_HIVE_HS2_PORT
  cursor = None
  try:
    conn = connect(host=host,
        port=DEFAULT_HIVE_HS2_PORT,
        user=user,
        database=database,
        auth_mechanism="GSSAPI" if use_kerberos else "PLAIN",
        timeout=maxsize)

    cursor = conn.cursor(configuration=execOptions)
    LOG.info("Connected to {0}:{1}".format(host, port))
  except Exception as e:
    LOG.error("Error Connecting: {0}".format(str(e)))
  return cursor

def execute_using_hive_hs2(query, query_config):
  exec_result = HiveQueryResult(query, query_config=query_config)
  plugin_runner = query_config.plugin_runner
  cursor = getattr(threading.current_thread(), 'cursor', None)
  if cursor is None:
    cursor = get_hs2_hive_cursor(query_config.hiveserver,
      user=query_config.user,
      database=query.db,
      use_kerberos=query_config.use_kerberos,
      execOptions=query_config.exec_options)
    threading.current_thread().cursor = cursor

  if cursor is None: return exec_result

  if plugin_runner: plugin_runner.run_plugins_pre(scope="Query")
  try:
    exec_result.start_time, start = datetime.now(), time()
    cursor.execute(query.query_str)
    exec_result.data = cursor.fetchall()
    exec_result.time_taken = time() - start
    exec_result.success = True
  except Exception as e:
    LOG.error(str(e))
    exec_result.query_error = str(e)
  finally:
    if plugin_runner: plugin_runner.run_plugins_post(scope="Query")
  return exec_result

def get_hs2_impala_cursor(impalad, use_kerberos=False, database=None):
  """Get a cursor to an impalad

  Args:
    impalad: A string in form 'hostname:port' or 'hostname'
    use_kerberos: boolean indication whether to get a secure connection.
    database: default db to use in the connection.

  Returns:
    HiveServer2Cursor if the connection suceeds, None otherwise.
  """
  try:
    host, port = impalad.split(":")
  except ValueError:
    host, port = impalad, DEFAULT_HS2_PORT
  cursor = None
  try:
    conn = connect(host=host,
        port=port,
        database=database,
        auth_mechanism="GSSAPI" if use_kerberos else "NOSASL")
    cursor = conn.cursor()
    LOG.info("Connected to {0}:{1}".format(host, port))
  except Exception as e:
    LOG.error("Error connecting: {0}".format(str(e)))
  return cursor

def execute_using_impala_hs2(query, query_config):
  """Executes a sql query against Impala using the hs2 interface.

  Args:
    query: Query
    query_config: ImpalaHS2Config

  Returns:
    ImpalaQueryResult
  """
  exec_result = ImpalaQueryResult(query, query_config=query_config)
  plugin_runner = query_config.plugin_runner
  cursor = get_hs2_impala_cursor(query_config.impalad,
      use_kerberos=query_config.use_kerberos,
      database=query.db)
  if cursor is None: return exec_result
  if plugin_runner: plugin_runner.run_plugins_pre(scope="Query")
  try:
    exec_result.start_time, start = datetime.now(), time()
    cursor.execute(query.query_str)
    exec_result.data = cursor.fetchall()
    exec_result.time_taken = time() - start
    exec_result.runtime_profile = cursor.get_profile()
    exec_result.exec_summary = str(cursor.get_summary())
    exec_result.success = True
  except Exception as e:
    LOG.error(str(e))
    exec_result.query_error = str(e)
  finally:
    cursor.close()
    if plugin_runner: plugin_runner.run_plugins_post(scope="Query")
    return exec_result

def establish_beeswax_connection(query_config):
  """Establish a connection to the user specified impalad.

  Args:
    query_config (QueryExecConfig)

  Returns:
    ImpalaBeeswaxClient is the connection suceeds, None otherwise.
  """
  use_kerberos = query_config.use_kerberos
  user = query_config.user
  password = query_config.password
  use_ssl = query_config.use_ssl
  # If the impalad is for the form host, convert it to host:port that the Impala beeswax
  # client accepts.
  if len(query_config.impalad.split(":")) == 1:
    query_config.impalad = "{0}:{1}".format(query_config.impalad, DEFAULT_BEESWAX_PORT)
  client = None
  try:
    client = ImpalaBeeswaxClient(query_config.impalad, use_kerberos=use_kerberos,
                                 user=user, password=password, use_ssl=use_ssl)
    # Try connect
    client.connect()
    # Set the exec options.
    client.set_query_options(query_config.exec_options)
    LOG.info("Connected to %s" % query_config.impalad)
  except Exception as e:
    LOG.error("Error connecting: {0}".format(str(e)))
  return client

def execute_using_impala_beeswax(query, query_config):
  """Executes a query using beeswax.

  A new client is created per query, then destroyed.

  Args:
    query (str): string containing the query to be executed.
    query_config (QueryExecConfig)

  Returns:
    ImpalaQueryResult
  """

  # Create a client object to talk to impalad
  exec_result = ImpalaQueryResult(query, query_config=query_config)
  plugin_runner = query_config.plugin_runner
  client = establish_beeswax_connection(query_config)
  if client is None: return exec_result
  # We need to issue a use database here.
  if query.db: client.execute("use {0}".format(query.db))
  # create a map for query options and the query names to send to the plugin
  context = build_context(query, query_config)
  if plugin_runner: plugin_runner.run_plugins_pre(context=context, scope="Query")
  result = None
  try:
    result = client.execute(query.query_str)
  except Exception as e:
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
  """ Transform an ImpalaBeeswaxResult object to a ImpalaQueryResult object.

  Args:
    result (ImpalaBeeswasResult): Tranfers data from here.
    exec_result (ImpalaQueryResult): Transfers data to here.

  Returns:
    ImpalaQueryResult
  """

  # Return immedietely if the query failed.
  if result is None or not result.success: return exec_result
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

def parse_jdbc_query_results(stdout, stderr, query):
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
  return create_exec_result(time_taken, result_data, query)

def create_exec_result(time_taken, result_data, query):
  exec_result = HiveQueryResult(query)
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
  exec_result = HiveQueryResult(query)
  start_time = datetime.now()
  try:
    rc, stdout, stderr = exec_process(cmd)
  except Exception as e:
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
  exec_result = parse_jdbc_query_results(stdout, stderr, query)
  exec_result.query = query
  exec_result.start_time = start_time
  return exec_result
