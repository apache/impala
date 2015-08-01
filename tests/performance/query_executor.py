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
# Module used for executing queries and gathering results.
# The QueryExecutor is meant to be generic and doesn't
# have the knowledge of how to actually execute a query. It takes a query and its config
# and executes is against a executor function.
# For example (in pseudo-code):
#
# def exec_func(query, config):
# ...
#
# config = ImpalaBeeswaxQueryExecConfig()
# executor = QueryExecutor('beeswax', query, config, exec_func)
# executor.run()
# result = executor.result

import logging
import os
import re
import shlex

from collections import defaultdict, deque
from datetime import datetime
from random import randint
from subprocess import Popen, PIPE
from tests.performance.query import Query, QueryResult
from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient, ImpalaBeeswaxResult
from threading import Thread, Lock

# Setup logging for this module.
logging.basicConfig(level=logging.INFO, format='[%(name)s] %(threadName)s: %(message)s')
LOG = logging.getLogger('query_executor')
LOG.setLevel(level=logging.INFO)

# globals.
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'

## TODO: Split executors into their own modules.
class QueryExecConfig(object):
  """Base Class for Execution Configs

  Attributes:
    plugin_runner (PluginRunner?)
  """
  def __init__(self, plugin_runner=None):
    self.plugin_runner = plugin_runner


class ImpalaQueryExecConfig(QueryExecConfig):
  """Base class for Impala query execution config

  Attributes:
    impalad (str): address of impalad <host>:<port>
  """

  def __init__(self, plugin_runner=None, impalad='localhost:21000'):
    super(ImpalaQueryExecConfig, self).__init__(plugin_runner=plugin_runner)
    self._impalad = impalad

  @property
  def impalad(self):
    return self._impalad

  @impalad.setter
  def impalad(self, value):
    self._impalad = value


class JdbcQueryExecConfig(ImpalaQueryExecConfig):
  """Impala query execution config for jdbc

  Attributes:
    tranport (?): ?
  """

  JDBC_CLIENT_PATH = os.path.join(os.environ['IMPALA_HOME'], 'bin/run-jdbc-client.sh')

  def __init__(self, plugin_runner=None, impalad='localhost:21050', transport=None):
    super(JdbcQueryExecConfig, self).__init__(plugin_runner=plugin_runner,
        impalad=impalad)
    self.transport = transport

  @property
  def jdbc_client_cmd(self):
    """The args to run the jdbc client.

    Constructed on the fly, since the impalad it points to can change.
    """
    return JdbcQueryExecConfig.JDBC_CLIENT_PATH + ' -i "%s" -t %s' % (self._impalad,
                                                                      self.transport)

class BeeswaxQueryExecConfig(ImpalaQueryExecConfig):
  """Impala query execution config for beeswax

  Args:
    use_kerberos (boolean)
    exec_options (str): String formatted as "opt1:val1;opt2:val2"
    impalad (str): address of impalad <host>:<port>
    plugin_runner (?): ?

  Attributes:
    use_kerberos (boolean)
    exec_options (dict str -> str): execution options
  """

  def __init__(self, use_kerberos=False, exec_options=None, impalad='localhost:21000',
      plugin_runner=None):
    super(BeeswaxQueryExecConfig, self).__init__(plugin_runner=plugin_runner,
        impalad=impalad)
    self.use_kerberos = use_kerberos
    self.exec_options = dict()
    self._build_options(exec_options)

  def _build_options(self, exec_options):
    """Read the exec_options into self.exec_options

    Args:
      exec_options (str): String formatted as "opt1:val1;opt2:val2"
    """

    if exec_options:
      # exec_options are seperated by ; on the command line
      options = exec_options.split(';')
      for option in options:
        key, value = option.split(':')
        # The keys in ImpalaService QueryOptions are upper case.
        self.exec_options[key.upper()] = value


class HiveQueryExecConfig(QueryExecConfig):
  """Hive query execution config"""
  def __init__(self, plugin_runner=None, hive_cmd='hive -e'):
    super(HiveQueryExecConfig, self).__init__(plugin_runner=plugin_runner)
    self.hive_cmd = hive_cmd

  def build_argument_string(self):
    """ Builds the actual argument string that is passed to hive """
    return str()


class QueryExecutor(object):
  """Executes a query.

  Args:
    name (str): eg. "hive"
    query (str): string containing SQL query to be executed
    func (function): Function that accepts a QueryExecOption parameter and returns a
      QueryResult. Eg. execute_using_impala_beeswax
    config (QueryExecOption)
    exit_on_error (boolean): Exit right after an error encountered.

  Attributes:
    exec_func (function): Function that accepts a QueryExecOption parameter and returns a
      QueryResult.
    exec_config (QueryExecOption)
    query (str): string containing SQL query to be executed
    exit_on_error (boolean): Exit right after an error encountered.
    executor_name (str): eg. "hive"
    result (QueryResult): Contains the result after execute method is called.
  """

  def __init__(self, name, query, func, config, exit_on_error):
    self.exec_func = func
    self.exec_config = config
    self.query = query
    self.exit_on_error = exit_on_error
    self.executor_name = name
    self._result = QueryResult(query, query_config=self.exec_config)

  def prepare(self, impalad):
    """Prepare the query to be run.

    For now, this sets the impalad that the query connects to. If the executor is hive,
    it's a no op.
    """
    if self.executor_name != 'hive':
      self.exec_config.impalad = impalad

  def execute(self):
    """Execute the query using the given execution function"""
    self._result = self.exec_func(self.query, self.exec_config)
    if not self._result.success:
      if self.exit_on_error:
        raise RuntimeError(self._result.query_error)
      else:
        LOG.info("Continuing execution")

  @property
  def result(self):
    """Getter for the result of the query execution.

    A result is a QueryResult object that contains the details of a single run of the
    query.
    """
    return self._result

def establish_beeswax_connection(query, query_config):
  """Establish a connection to the user specified impalad.

  Args:
    query_config (QueryExecConfig)

  Returns:
    (boolean, ImpalaBeeswaxClient): True if successful
  """

  # TODO: Make this generic, for hive etc.
  use_kerberos = query_config.use_kerberos
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

def execute_shell_cmd(cmd):
  """Executes a command in the shell, pipes the output to local variables

  Args:
    cmd (str): Command to be executed.

  Returns:
    (str, str, str): return code, stdout, stderr
  """

  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.
  # The first element is the command, with the rest being arguments.
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)
  stdout, stderr = p.communicate()
  rc = p.returncode
  return rc, stdout, stderr

def execute_using_hive(query, query_config):
  """Executes a query via hive"""
  query_string = query.query_str + ';'
  if query.db:
    query_string = 'use %s;%s' % (query.db, query_string)
  cmd = query_config.hive_cmd + " \"%s\"" % query_string
  return run_query_capture_results(cmd, query, parse_hive_query_results)

def parse_hive_query_results(stdout, stderr, iterations):
  """
  Parse query execution details for hive.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns a QueryResult object.
  """
  run_success = False
  execution_times = list()
  std_dev = None
  for line in stderr.split('\n'):
    match = re.search(hive_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))
      break
  # TODO: Get hive results
  return create_exec_result(execution_times, iterations, None)

def execute_using_jdbc(query, query_config):
  """Executes a query using JDBC"""
  query_string = query.query_str + ';'
  if query.db:
    query_string = 'use %s; %s' % (query.db, query_string)
  cmd = query_config.jdbc_client_cmd + " -q \"%s\"" % query_string
  return run_query_capture_results(cmd, query, parse_jdbc_query_results,
    exit_on_error=False)

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

def run_query_capture_results(cmd, query, query_result_parse_function, exit_on_error):
  """
  Runs the given query command and returns the execution result.

  Takes in a match function that is used to parse stderr/stdout to extract the results.
  """
  exec_result = QueryResult(query)
  start_time = datetime.now()
  try:
    rc, stdout, stderr = execute_shell_cmd(cmd)
  except Exception, e:
    LOG.error('Error while executing query command: %s' % e)
    exec_result.query_error = str(e)
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
  exec_result = query_result_parse_function(stdout, stderr)
  exec_result.query = query
  exec_result.start_time = start_time
  return exec_result
