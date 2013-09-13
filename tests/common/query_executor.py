#!/usr/bin/env python
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
# Module used for executing queries and gathering results and allowing for executing
# multiple queries concurrently. The QueryExecutor is meant to be very generic and doesn't
# have the knowledge of how to actually execute a query. It takes an executor
# function and a query option object and returns the QueryExecResult. It's
# responsible for parallelizing execution. If a query fails, it either raises an
# exception to the caller, or swallows the exception and prints out an error message if
# the caller's exit_on_error is set to False.
# For example (in pseudo-code):
#
# def execute_using_impala_beeswax(query, query_option):
# ...
#
# exec_option = ImpalaBeeswaxQueryExecOptions()
# qe = QueryExecutor(execute_using_impala_beeswax, exec_options query, num_clients,
#                   exit_on_error)
# qe.run()
# exec_result = qe.get_results()
#
import logging
import os
import re
import shlex

from collections import defaultdict
from random import randint
from subprocess import Popen, PIPE
from tests.beeswax.impala_beeswax import *
from tests.util.calculation_util import calculate_avg, calculate_stddev
from threading import Thread, Lock

# Setup logging for this module.
logging.basicConfig(level=logging.INFO, format='[%(name)s] %(threadName)s: %(message)s')
LOG = logging.getLogger('query_executor')
LOG.setLevel(level=logging.INFO)

# globals.
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'

class QueryExecResult(object):
  """Container for saving the results of a query execution.

  A query contains the following fields:
  avg_time - Average query execution time.
  std_dev - The standard deviation of execution times (None if only run once)
  success - False when a query failed, True otherwise.
  data - Tab delimited rows returned by the query.
  runtime_profile - The query's runtime profile.
  query_error - Empty string if the query succeeded. Error returned by the client if
                it failed.
  """
  def __init__(self, **kwargs):
    self.query = kwargs.get('query')
    self.avg_time = kwargs.get('avg_time')
    self.std_dev = kwargs.get('avg_time')
    self.__note = kwargs.get('note')
    self.data = kwargs.get('data')
    self.runtime_profile = kwargs.get('runtime_profile', str())
    self.success = False
    self.query_error = str()
    self.executor_name = str()

  def set_result_note(self, note):
    self.__note = note

  def __str__(self):
    """Print human readable query execution details"""
    message = str()
    if self.__note: message = "%s, " % self.__note
    message += 'Avg Time: %s, Std Dev: %s' % (self.avg_time, self.std_dev)
    return message


class QueryExecOptions(object):
  """Base class for Query execution options"""
  def __init__(self, iterations, **kwargs):
    self.iterations = iterations
    self.db_name = kwargs.get('db_name', None)


class ImpalaQueryExecOptions(QueryExecOptions):
  """Base class for Impala query options"""
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.impalad = kwargs.get('impalad', 'localhost:21000')
    self.plugin_runner = kwargs.get('plugin_runner', None)


class JdbcQueryExecOptions(QueryExecOptions):
  """Class for Impala query options when running a jdbc client"""
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.impalad = kwargs.get('impalad', 'localhost:21050')
    self.transport = self.options.get('transport', None)
    self.jdbc_client_cmd = \
        os.path.join(os.environ['IMPALA_HOME'], 'bin/run-jdbc-client.sh')
    self.jdbc_client_cmd += ' -i "%s" -t %s' % (self.impalad, self.transport)


class ImpalaBeeswaxExecOptions(ImpalaQueryExecOptions):
  """Class for Impala query options when running a beeswax client"""
  def __init__(self, iterations, **kwargs):
    ImpalaQueryExecOptions.__init__(self, iterations, **kwargs)
    self.use_kerberos = kwargs.get('use_kerberos', False)
    self.exec_options = dict()
    self.__build_options(kwargs)

  def __build_options(self, kwargs):
    """Read the exec_options into a dictionary"""
    exec_options = kwargs.get('exec_options', None)
    if exec_options:
      # exec_options are seperated by ; on the command line
      options = exec_options.split(';')
      for option in options:
        key, value = option.split(':')
        # The keys in ImpalaService QueryOptions are upper case.
        self.exec_options[key.upper()] = value


# Hive query exec options
class HiveQueryExecOptions(QueryExecOptions):
  """Class for hive query options"""
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.hive_cmd = kwargs.get('hive_cmd', 'hive -e ')

  def build_argument_string(self):
    """ Builds the actual argument string that is passed to hive """
    return str()


class QueryExecutor(object):
  def __init__(self, query_exec_func, executor_name, exec_options, query, num_clients,
      exit_on_error):
    """
    Execute a query in parallel.

    The query_exec_func needs to be a function that accepts a QueryExecOption parameter
    and returns a QueryExecResult.
    """
    self.query_exec_func = query_exec_func
    self.query_exec_options = exec_options
    self.query = query
    self.num_clients = num_clients
    self.exit_on_error = exit_on_error
    self.executor_name = executor_name
    self.__results = list()
    self.__result_list_lock = Lock()
    self.__create_thread_name_template()

  def __create_thread_name_template(self):
    """Create the thread prefix

    The thread prefix is a concatanation of the query name and the scale factor
    (if present)
    """
    if self.query.scale_factor:
      self.thread_name = '[%s:%s]'% (self.query.name, self.query.scale_factor)
    else:
      self.thread_name = '[%s]'% self.query.name
    self.thread_name += ' Thread %d'

  def __get_thread_name(self, thread_num):
    """Generate a thread name."""
    return "%s Thread %d" % (self.__thread_prefix, thread_num)

  def __create_query_threads(self):
    """Create a thread for each client."""
    self.query_threads = []
    for i in xrange(self.num_clients):
      self.query_threads.append(Thread(target=self.__run_query,
        name=self.thread_name % i))

  def __update_results(self, result):
    """Thread safe update for results.

    Also responsible for handling query failures.
    """
    self.__result_list_lock.acquire()
    try:
      if not result.success:
        error_msg = "Error executing query %s, Error: %s." % (self.query.name,
            result.query_error)
        if self.exit_on_error:
          raise RuntimeError, error_msg + ' Aborting.'
        else:
          LOG.error(error_msg + ' Ignoring')
      else:
        self.__results.append(result)
    finally:
      self.__result_list_lock.release()

  def __run_query(self):
    """Run method for a query thread.

    Responsible for running the query and updating the results.
    """
    result = self.query_exec_func(self.query, self.query_exec_options)
    result.executor_name = self.executor_name
    self.__update_results(result)

  def run(self):
    """Create query threads based on the number clients and execute them"""
    # Create threads on the fly; This makes the run() method resusable.
    self.__create_query_threads()
    for thread_num, t in enumerate(self.query_threads):
      LOG.info("Starting %s" % self.thread_name % thread_num)
      t.start()
    for thread_num, t in enumerate(self.query_threads):
      # Wait for threads to complete.
      t.join()
      LOG.info("Finished %s" % self.thread_name % thread_num)

  def get_results(self):
    """Returns the result(s) of the query execution.

    Results are returned as a list of QueryExecResult objects. get_results() should be
    called after run(), otherwise the result list will be empty.
    """
    return self.__results

def establish_beeswax_connection(query, query_options):
  """Establish a connection to the user specified impalad"""
  # TODO: Make this generic, for hive etc.
  use_kerberos = query_options.use_kerberos
  client = ImpalaBeeswaxClient(query_options.impalad, use_kerberos=use_kerberos)
  # Try connect
  client.connect()
  LOG.info('Connected to %s' % query_options.impalad)
  # Set the exec options.
  client.set_query_options(query_options.exec_options)
  return (True, client)

def execute_using_impala_beeswax(query, query_options):
  """Executes a query using beeswax.

  A new client is created per query, then destroyed. Returns QueryExecResult()
  """
  # Create a client object to talk to impalad
  exec_result = QueryExecResult()
  plugin_runner = query_options.plugin_runner
  (success, client) = establish_beeswax_connection(query.query_str, query_options)
  if not success:
    return exec_result
  # we need to issue a use database here.
  if query.db:
    use_query = 'use %s' % query.db
    client.execute(use_query)
  # execute the query
  results = []
  # create a map for query options and the query names to send to the plugin
  context = build_context(query, query_options)
  for i in xrange(query_options.iterations):
    LOG.info("Running iteration %d" % (i+1))
    context['iteration'] = i
    result = QueryResult()
    if plugin_runner: plugin_runner.run_plugins_pre(context=context, scope="Query")
    try:
      result = client.execute(query.query_str)
      LOG.info("Iteration %d finished in %f(s)" % (i+1, result.time_taken))
    except Exception, e:
      LOG.error(e)
      client.close_connection()
      exec_result.query_error = str(e)
      return exec_result
    if plugin_runner: plugin_runner.run_plugins_post(context=context, scope="Query")
    results.append(result)
  # We only need to print the results for a successfull run, not all.
  LOG.debug('Result:\n%s\n' % results[0])
  client.close_connection()
  # get rid of the client object
  del client
  # construct the execution result.
  return construct_exec_result(query_options.iterations, query, results)

def build_context(query, query_options):
  context = vars(query_options)
  context['query_name'] = query.query_str
  context['table_format'] = query.table_format_str
  context['short_query_name'] = query.name
  return context

def construct_exec_result(iterations, query, results):
  """
  Calculate average running time and standard deviation.

  The summary of the first result is used as the summary for the entire execution.
  """
  # Use the output from the first result.
  exec_result = QueryExecResult()
  exec_result.query = query
  exec_result.data = results[0].data
  exec_result.beeswax_result = results[0]
  exec_result.set_result_note(results[0].summary)
  exec_result.runtime_profile = results[0].runtime_profile
  # If running more than 2 iterations, throw the first result out. Don't throw away
  # the first result if iterations = 2 to preserve the stddev calculation.
  if iterations > 2:
    results = results[1:]

  runtimes = [r.time_taken for r in results]
  exec_result.success = True
  exec_result.avg_time = calculate_avg(runtimes)
  if iterations > 1:
    exec_result.std_dev = calculate_stddev(runtimes)
  return exec_result

def execute_shell_cmd(cmd):
  """Executes a command in the shell, pipes the output to local variables"""
  LOG.debug('Executing: %s' % (cmd,))
  # Popen needs a list as its first parameter.
  # The first element is the command, with the rest being arguments.
  p = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE)
  stdout, stderr = p.communicate()
  rc = p.returncode
  return rc, stdout, stderr

def execute_using_hive(query, query_options):
  """Executes a query via hive"""
  query_string = (query.query_str + ';') * query_options.iterations
  if query.db:
    query_string = 'use %s;' % query.db + query_string
  cmd = query_options.hive_cmd + " \"%s\"" % query_string
  return run_query_capture_results(cmd, query, parse_hive_query_results,
                                   query_options.iterations, exit_on_error=False)

def parse_hive_query_results(stdout, stderr, iterations):
  """
  Parse query execution details for hive.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns a QueryExecResult object.
  """
  run_success = False
  execution_times = list()
  std_dev = None
  for line in stderr.split('\n'):
    match = re.search(hive_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))
  # TODO: Get hive results
  return create_exec_result(execution_times, iterations, None)

def execute_using_jdbc(query, query_options):
  """Executes a query using JDBC"""
  query_string = (query.query_str + ';') * query_options.iterations
  if query.db:
    query_string = 'use %s; %s' % (query.db, query_string)
  cmd = query_options.jdbc_client_cmd + " -q \"%s\"" % query_string
  return run_query_capture_results(cmd, query, parse_jdbc_query_results,
      query_options.iterations, exit_on_error=False)

def parse_jdbc_query_results(stdout, stderr, iterations):
  """
  Parse query execution results for the Impala JDBC client

  Parses the query execution details (avg time, stddev) from the output of the Impala
  JDBC test client.
  """
  run_success = False
  execution_times = list()
  std_dev = None
  jdbc_result_regex = 'row\(s\) in (\d*).(\d*)s'
  for line in stdout.split('\n'):
    match = re.search(jdbc_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))

  result_data = re.findall(r'\[START\]----\n(.*?)\n----\[END\]', stdout, re.DOTALL)
  return create_exec_result(execution_times, iterations, result_data)

def create_exec_result(execution_times, iterations, result_data):
  exec_result = QueryExecResult()
  exec_result.success = False

  if result_data:
    # Just print the first result returned. There may be additional results if
    # there were multiple iterations executed.
    LOG.debug('Data:\n%s\n' % result_data[0])
    exec_result.data = result_data[0].split('\n')

  if len(execution_times) == iterations:
    exec_result.avg_time = calculate_avg(execution_times)
    if iterations > 1:
      exec_result.std_dev = calculate_stddev(execution_times)
    exec_result.success = True
  return exec_result

def run_query_capture_results(cmd, query, query_result_parse_function, iterations,
                              exit_on_error):
  """
  Runs the given query command and returns the execution result.

  Takes in a match function that is used to parse stderr/stdout to extract the results.
  """
  exec_result = QueryExecResult()
  try:
    rc, stdout, stderr = execute_shell_cmd(cmd)
  except Exception, e:
    LOG.error('Error while executing query command: %s' % e)
    exec_result.query_error = str(e)
    return exec_result
  if rc != 0:
    LOG.error(('Command returned with an error:\n'
               'rc: %d\n'
               'STDERR:\n%s'
               'STDOUT:\n%s'
                % (rc, stderr, stdout)))
    return exec_result
  # The command completed
  exec_result = query_result_parse_function(stdout, stderr, iterations)
  exec_result.query = query
  if not exec_result.success:
    LOG.error("Query did not run successfully")
    LOG.error("STDERR:\n%s\nSTDOUT:\n%s" % (stderr, stdout))
  return exec_result
