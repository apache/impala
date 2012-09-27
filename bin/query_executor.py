#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Module used for executing queries and gathering results and allowing for executing
# multiple queries concurrently. The QueryExecutor is meant to be very generic and doesn't
# have the knowledge of how to actually execute a query. It just takes an executor
# function and a query option object and returns the QueryExecutionResult.
# For example (in pseudo-code):
#
# def execute_beeswax_query(query, query_options):
# ...
#
# exec_option = ImpalaBeeswaxQueryExecOption()
# qe = QueryExecutor(execute_beeswax_query, exec_options)
# qe.run()
# execution_result, output = qe.get_results()
#
import logging
import math
import re
import subprocess
import tempfile
import threading
from collections import defaultdict

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('query_executor')
LOG.setLevel(level=logging.DEBUG)

result_single_regex = 'returned (\d*) rows? in (\d*).(\d*) s'
result_multiple_regex = 'returned (\d*) rows? in (\d*).(\d*) s with stddev (\d*).(\d*)'
hive_result_regex = 'Time taken: (\d*).(\d*) seconds'

# Contains details about the execution result of a query
class QueryExecutionResult(object):
  def __init__(self, avg_time='N/A', stddev='N/A'):
    self.avg_time = avg_time
    self.stddev = stddev
    self.success = False


# Base class for query exec options
class QueryExecOptions(object):
  def __init__(self, iterations, **kwargs):
    self.options = kwargs
    self.iterations = iterations


# Base class for Impala query exec options
class ImpalaQueryExecOptions(QueryExecOptions):
  def __init__(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs)
    self.impalad = self.options.get('impalad', 'localhost:21000')
    self.disable_codegen = self.options.get('disable_codegen', False)
    self.num_scanner_threads = self.options.get('num_scanner_threads', 0)


# Execution options specific to runquery
class RunQueryExecOptions(ImpalaQueryExecOptions):
  def __init__(self, iterations, **kwargs):
    ImpalaQueryExecOptions.__init__(self, iterations, **kwargs)
    self.runquery_cmd = self.options.get('runquery_cmd', 'runquery ')
    self.profiler = self.options.get('profiler', False)
    self.enable_counters = self.options.get('enable_counters', False)
    self.profile_output_file = self.options.get('profile_output_file', str())

  def _build_exec_options(self):
    additional_exec_options = self.options.get('exec_options', '')
    exec_options = additional_exec_options.split(';')
    if additional_exec_options:
      additional_exec_options = ';' + additional_exec_options
    # Make sure to add additional exec options to the end in case they are duplicates
    # of some of the default (last defined value is what is applied)
    return 'num_scanner_threads:%s;disable_codegen:%s%s' %\
        (self.num_scanner_threads, self.disable_codegen, additional_exec_options)

  def build_argument_string(self):
    """ Builds the actual argument string that is passed to runquery """
    arg_str = ' --impalad=%(impalad)s --iterations=%(iterations)d '\
              '--exec_options="%(exec_options)s" '

    return arg_str % {'impalad': self.impalad,
                      'iterations': self.iterations,
                      'exec_options': self._build_exec_options(), }


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
    self.execution_result = None
    threading.Thread.__init__(self)
    self.name = name

  def _execute_query(self):
    self.execution_result, self.output_result =\
        self.query_exec_func(self.query, self.query_exec_options)

  def success(self):
    return self.execution_result is not None and self.execution_result.success

  def run(self):
    """ Runs the actual query """
    self._execute_query()

  def get_results(self):
    """ Returns the result of the query execution """
    return self.output_result, self.execution_result


# Standalone Functions
def execute_using_runquery(query, query_options):
  """Executes a query via runquery"""
  print query_options.options
  enable_counters = query_options.enable_counters
  gprof_tmp_file = str()

  # Call the profiler if the option has been specified.
  if query_options.profiler:
    gprof_tmp_file = query_options.profile_output_file

  cmd = '%(runquery_cmd)s %(args)s --enable_counters=%(enable_counters)d '\
        '--profile_output_file="%(prof_output_file)s" --query="%(query)s"' % {\
            'runquery_cmd': query_options.runquery_cmd,
            'args': query_options.build_argument_string(),
            'prof_output_file': gprof_tmp_file,
            'enable_counters': enable_counters,
            'query': query
        }

  results = run_query_capture_results(cmd, match_impala_query_results,
                                      query_options.iterations, exit_on_error=True)
  if query_options.profiler:
    subprocess.call(gprof_cmd % gprof_tmp_file, shell=True)
  return results

def execute_using_hive(query, query_options):
  """Executes a query via hive"""
  query_string = (query + ';') * query_options.iterations
  cmd = query_options.hive_cmd + "\" %s\"" % query_string
  return run_query_capture_results(cmd, match_hive_query_results,
                                   query_options.iterations, exit_on_error=False)

def run_query_capture_results(cmd, query_result_match_function, iterations,
                              exit_on_error):
  """
  Runs the given query command and returns the execution result.

  Takes in a match function that is used to parse stderr/stdout to extract the results.
  """
  # TODO: Find a good way to stream the output to console and log file this tmp
  # file stuff isn't ideal.
  output_stdout = tempfile.TemporaryFile("w+")
  output_stderr = tempfile.TemporaryFile("w+")
  LOG.info("Executing: %s" % cmd)
  subprocess.call(cmd, shell=True, stderr=output_stderr, stdout=output_stdout)
  output_stdout.seek(0)
  output_stderr.seek(0)
  execution_result = query_result_match_function(output_stdout.readlines(),
                                                 output_stderr.readlines(),
                                                 iterations)
  print_file("", output_stderr)
  print_file("", output_stdout)
  if not execution_result.success:
    has_execution_error = True
    LOG.error("Query did not run successfully")
    if exit_on_error:
      return execution_result, None

  output = ''
  if execution_result.success:
    output += "  Avg Time: %.02fs\n" % float(execution_result.avg_time)
    if execution_result.stddev != 'N/A':
      output += "  Std Dev: %.02fs\n" % float(execution_result.stddev)
  else:
    output += '  No Results - Error executing query!\n'

  output_stderr.close()
  output_stdout.close()
  return execution_result, output

def match_impala_query_results(output_stdout, output_stderr, iterations):
  """
  Parse query execution details for impala.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns these results as well as whether the query completed successfully.
  """
  avg_time = 0
  stddev = "N/A"
  run_success = False
  for line in output_stdout:
    if iterations == 1:
      match = re.search(result_single_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        run_success = True
    else:
      match = re.search(result_multiple_regex, line)
      if match:
        avg_time = ('%s.%s') % (match.group(2), match.group(3))
        stddev = ('%s.%s') % (match.group(4), match.group(5))
        run_success = True
  execution_result = QueryExecutionResult(str(avg_time), str(stddev))
  execution_result.success = run_success
  return execution_result

def match_hive_query_results(output_stdout, output_stderr, iterations):
  """
  Parse query execution details for hive.

  Parses the query execution details (avg time, stddev) from the runquery output.
  Returns these results as well as whether the query completed successfully.
  """
  run_success = False
  execution_times = list()
  for line in output_stderr:
    match = re.search(hive_result_regex, line)
    if match:
      execution_times.append(float(('%s.%s') % (match.group(1), match.group(2))))

  execution_result = QueryExecutionResult()
  if len(execution_times) == iterations:
    avg_time = calculate_avg(execution_times)
    stddev = 'N/A'
    if iterations > 1:
      stddev = calculate_stddev(execution_times)
    execution_result = QueryExecutionResult(avg_time, stddev)
    run_success = True
  execution_result.success = run_success
  return execution_result

# Util functions
# TODO : Move util functions to a common module.
def calculate_avg(values):
  return sum(values) / float(len(values))

def calculate_stddev(values):
  """
  Return the stardard deviation of a numeric iterable.

  TODO: If more statistical functions are required, consider using numpy/scipy.
  """
  avg = calculate_avg(values)
  return math.sqrt(calculate_avg([(val - avg)**2 for val in values]))

def print_file(header, output_file):
  """
  Print the contents of the file to the terminal.
  """
  output_file.seek(0)
  output = header + '\n'
  for line in output_file.readlines():
    output += line.rstrip() + '\n'
  LOG.debug(output)
