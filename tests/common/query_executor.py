#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Module used for executing queries and gathering results and allowing for executing
# multiple queries concurrently. The QueryExecutor is meant to be very generic and doesn't
# have the knowledge of how to actually execute a query. It just takes an executor
# function and a query option object and returns the QueryExecutionResult.
# For example (in pseudo-code):
#
# def execute_using_impala_beeswax(query, query_options):
# ...
#
# exec_option = ImpalaBeeswaxQueryExecOptions()
# qe = QueryExecutor(execute_using_impala_beeswax, exec_options)
# qe.run()
# execution_result qe.get_results()
#
import logging
import os
import re
import threading
import shlex

from collections import defaultdict
from random import randint
from subprocess import Popen, PIPE
from tests.beeswax.impala_beeswax import *
from tests.util.calculation_util import calculate_avg, calculate_stddev

# Setup logging for this module.
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('query_executor')
LOG.setLevel(level=logging.DEBUG)

# globals.
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'

# Contains details about the execution result of a query
class QueryExecutionResult(object):
  def __init__(self, avg_time=None, std_dev=None, data=None, note=None):
    self.avg_time = avg_time
    self.std_dev = std_dev
    self.__note = note
    self.success = False
    self.data = data

  def set_result_note(self, note):
    self.__note = note

  def __str__(self):
    """Print human readable query execution details"""
    message = str()
    if self.__note: message = "%s, " % self.__note
    message += 'Avg Time: %s, Std Dev: %s' % (self.avg_time, self.std_dev)
    return message


# Base class for query exec options
class QueryExecOptions(object):
  def __init__(self, iterations, **kwargs):
    self.options = kwargs
    self.iterations = iterations
    self.db_name = self.options.get('db_name', None)


# Base class for Impala query exec options
class ImpalaQueryExecOptions(QueryExecOptions):
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.impalad = self.options.get('impalad', 'localhost:21000')


class JdbcQueryExecOptions(QueryExecOptions):
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.impalad = self.options.get('impalad', 'localhost:21050')
    self.jdbc_client_cmd = \
        os.path.join(os.environ['IMPALA_HOME'], 'bin/run-jdbc-client.sh')
    self.jdbc_client_cmd += ' -i "%s"' % self.impalad


# constructs exec_options for query execution through beeswax
# TODO: Make argument handling better.
class ImpalaBeeswaxExecOptions(ImpalaQueryExecOptions):
  def __init__(self, iterations, **kwargs):
    ImpalaQueryExecOptions.__init__(self, iterations, **kwargs)
    self.use_kerberos = kwargs.get('use_kerberos', False)
    self._build_exec_options(kwargs.get('exec_options', None))

  def _build_exec_options(self, exec_options):
    """Read the exec_options into a dictionary"""
    self.exec_options = dict()
    if exec_options:
      # exec_options are seperated by ; on the command line
      options = exec_options.split(';')
      for option in options:
        key, value = option.split(':')
        # The keys in ImpalaService QueryOptions are upper case.
        self.exec_options[key.upper()] = value


# Hive query exec options
class HiveQueryExecOptions(QueryExecOptions):
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.hive_cmd = self.options.get('hive_cmd', 'hive -e ')

  def build_argument_string(self):
    """ Builds the actual argument string that is passed to hive """
    return str()


# The QueryExecutor is used to run the given query using the target executor (Hive,
# Impala, Impala Beeswax)
class QueryExecutor(threading.Thread):
  def __init__(self, name, query_exec_func, exec_options, query):
    """
    Initialize the QueryExecutor

    The query_exec_func needs to be a function that accepts a QueryExecOption parameter
    and returns a QueryExecutionResult and output string. The output string is used so
    callers can devide whether or not to display the output or do other manipulation
    on it.
    """
    self.query_exec_func = query_exec_func
    self.query_exec_options = exec_options
    self.query = query
    self.output_result = None
    self.execution_result = QueryExecutionResult()
    threading.Thread.__init__(self)
    self.name = name

  def _execute_query(self):
    self.execution_result = self.query_exec_func(self.query, self.query_exec_options)
    LOG.debug('Result:\n  -> %s\n' % self.execution_result)

  def success(self):
    return self.execution_result is not None and self.execution_result.success

  def run(self):
    """ Runs the actual query """
    self._execute_query()

  def get_results(self):
    """ Returns the result of the query execution """
    return self.execution_result

# Standalone Functions
def establish_beeswax_connection(query, query_options):
  # TODO: Make this generic, for hive etc.
  use_kerberos = query_options.use_kerberos
  client = ImpalaBeeswaxClient(query_options.impalad, use_kerberos=use_kerberos)
  # Try connect
  client.connect()
  LOG.debug('Connected to %s' % query_options.impalad)
  # Set the exec options.
  exec_options = query_options.exec_options
  for exec_option in exec_options.keys():
    # TODO: Move the validation to the ImpalaBeeswaxExecOptions.
    if not client.get_query_option(exec_option):
      LOG.error('Illegal exec_option: %s' % exec_option)
      return (False, None)
    # change the default value to the user specified value.
    client.set_query_option(exec_option, exec_options[exec_option])
  return (True, client)

def execute_using_impala_beeswax(query, query_options):
  """Executes a query using beeswax.

  A new client is created per query, then destroyed. Returns QueryExecutionResult()
  """
  # Create a client object to talk to impalad
  exec_result = QueryExecutionResult()
  (success, client) = establish_beeswax_connection(query, query_options)
  if not success:
    return exec_result
  # we need to issue a use database here.
  if query_options.db_name:
    use_query = 'use %s' % query_options.db_name
    client.execute(use_query)
  # execute the query
  results = []
  for i in xrange(query_options.iterations):
    LOG.debug("Running iteration %d" % (i+1))
    result = QueryResult()
    try:
      result = client.execute(query)
    except Exception, e:
      LOG.error(e)
      client.close_connection()
      return exec_result
    results.append(result)
  # We only need to print the results for a successfull run, not all.
  LOG.debug('Data:\n%s\n' % results[0].get_data())
  client.close_connection()
  # get rid of the client object
  del client
  # construct the execution result.
  return construct_execution_result(query_options.iterations, results)

def construct_execution_result(iterations, results):
  """Calculate average running time and standard deviation.

  The summary of the first result is used as the summary for the entire execution.
  """
  # Use the output from the first result.
  exec_result = QueryExecutionResult()
  exec_result.data = results[0].data
  exec_result.beeswax_result = results[0]
  exec_result.set_result_note(results[0].summary)

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
  query_string = (query + ';') * query_options.iterations
  if query_options.db_name:
    query_string = 'use %s;' % query_options.db_name + query_string
  cmd = query_options.hive_cmd + " \"%s\"" % query_string
  return run_query_capture_results(cmd, parse_hive_query_results,
                                   query_options.iterations, exit_on_error=False)

def parse_hive_query_results(stdout, stderr, iterations):
  """
  Parse query execution details for hive.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns a QueryExecutionResult object.
  """
  run_success = False
  execution_times = list()
  std_dev = None
  for line in stderr.split('\n'):
    match = re.search(hive_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))
  # TODO: Get hive results
  return create_execution_result(execution_times, iterations, None)

def execute_using_jdbc(query, query_options):
  """Executes a query using JDBC"""
  query_string = (query + ';') * query_options.iterations
  if query_options.db_name:
    query_string = 'use %s; %s' % (query_options.db_name, query_string)
  cmd = query_options.jdbc_client_cmd + " -q \"%s\"" % query_string
  return run_query_capture_results(cmd, parse_jdbc_query_results,
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
  return create_execution_result(execution_times, iterations, result_data)

def create_execution_result(execution_times, iterations, result_data):
  execution_result = QueryExecutionResult()
  execution_result.success = False

  if result_data:
    # Just print the first result returned. There may be additional results if
    # there were multiple iterations executed.
    LOG.debug('Data:\n%s\n' % result_data[0])
    execution_result.data = result_data[0].split('\n')

  if len(execution_times) == iterations:
    execution_result.avg_time = calculate_avg(execution_times)
    if iterations > 1:
      execution_result.std_dev = calculate_stddev(execution_times)
    execution_result.success = True
  return execution_result

def run_query_capture_results(cmd, query_result_parse_function, iterations,
                              exit_on_error):
  """
  Runs the given query command and returns the execution result.

  Takes in a match function that is used to parse stderr/stdout to extract the results.
  """
  execution_result = QueryExecutionResult()
  try:
    rc, stdout, stderr = execute_shell_cmd(cmd)
  except Exception, e:
    LOG.error('Error while executing query command: %s' % e)
    return execution_result
  if rc != 0:
    LOG.error(('Command returned with an error:\n'
               'rc: %d\n'
               'STDERR:\n%s'
               'STDOUT:\n%s'
                % (rc, stderr, stdout)))
    return execution_result
  # The command completed
  execution_result = query_result_parse_function(stdout, stderr, iterations)
  if not execution_result.success:
    LOG.error("Query did not run successfully")
    LOG.error("STDERR:\n%s\nSTDOUT:\n%s" % (stderr, stdout))
  return execution_result

# Util functions
# TODO : Move util functions to a common module.

