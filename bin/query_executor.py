#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Module used for executing queries and gathering results and allowing for executing
# multiple queries concurrently. The QueryExecutor is meant to be very generic and doesn't
# have the knowledge of how to actually execute a query. It just takes an executor function and a
# query option object and returns the QueryExecutionResult.
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
# TODO: In the future we might want to push more of the query execution logic into
# this class.
import logging
import subprocess
from collections import defaultdict
import threading

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
    QueryExecOptions.__init__(self, iterations, **kwargs);
    self.impalad = self.options.get('impalad', 'localhost:21000')
    self.disable_codegen = self.options.get('disable_codegen', False)
    self.num_scanner_threads = self.options.get('num_scanner_threads', 0)


# Execution options specific to runquery
class RunQueryExecOptions(ImpalaQueryExecOptions):
  def __init(self, iterations, **kwargs):
    ImpalaQueryExecOptions.__init__(self, iterations, **kwargs);

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
    ARG_STRING = ' --impalad=%(impalad)s --iterations=%(iterations)d '\
                 '--exec_options="%(exec_options)s" '
    return ARG_STRING % {'impalad': self.impalad, 'iterations': self.iterations,
                         'exec_options': self._build_exec_options(), }


# Hive query exec options
class HiveQueryExecOptions(QueryExecOptions):
  def __init(self, iterations, **kwargs):
    QueryExecOptions.__init__(self, iterations, **kwargs);

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
