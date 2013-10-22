#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This module is used to run benchmark queries.  It runs the set queries specified in the
# given workload(s) under <workload name>/queries. This script will first try to warm the
# buffer cache before running the query. There is also a parameter to to control how
# many iterations to run each query.
import csv
import logging
import math
import os
import sys
import subprocess
import threading
from collections import defaultdict, deque
from functools import partial
from math import ceil
from optparse import OptionParser
from os.path import isfile, isdir
from tests.common.query_executor import *
from tests.common.test_dimensions import *
from tests.common.test_result_verifier import *
from tests.common.workload_executor import *
from tests.util.calculation_util import calculate_median
from tests.util.test_file_parser import *
from time import sleep
from random import choice

# Globals
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
IMPALA_HOME = os.environ['IMPALA_HOME']

# Setup Logging
logging.basicConfig(level=logging.INFO, format='[%(name)s] %(threadName)s: %(message)s')
LOG = logging.getLogger('workload_runner')


class Query(object):
  """Represents the notion of a query in the Impala test infrastructure"""
  def __init__(self, *args, **kwargs):
    self.query_str = kwargs.get('query_str')
    self.name = kwargs.get('name')
    self.scale_factor = kwargs.get('scale_factor')
    self.test_vector = kwargs.get('test_vector')
    self.results = kwargs.get('results')
    self.workload = kwargs.get('workload')
    self.db = kwargs.get('db', str())
    self.table_format_str = kwargs.get('table_format_str', str())
    # Only attempt to build the query if a query_str has been passed to the c'tor.
    # If it's None, assume the user wants to set a qualified query_str
    if self.query_str: self.__build_query()

  def __build_query(self):
    self.db = QueryTestSectionReader.get_db_name(self.test_vector, self.scale_factor)
    self.query_str = QueryTestSectionReader.build_query(self.query_str.strip())
    self.table_format_str = '%s/%s/%s' % (self.test_vector.file_format,
                                          self.test_vector.compression_codec,
                                          self.test_vector.compression_type)


class WorkloadRunner(object):
  """Runs query files and captures results from the specified workload(s)

  The usage is:
   1) Initialize WorkloadRunner with desired execution parameters.
   2) Call workload_runner.run_workload() passing in a workload name(s) and scale
      factor(s).

   Internally, for each workload, this module looks up and parses that workload's
   query files and reads the workload's test vector to determine what combination(s)
   of file format / compression to run with. The queries are then executed
   and the results are displayed as well as saved to a CSV file.
  """
  def __init__(self, **kwargs):
    self.verbose = kwargs.get('verbose', False)
    if self.verbose:
      LOG.setLevel(level=logging.DEBUG)

    self.client_type = kwargs.get('client_type', 'beeswax')
    self.skip_impala = kwargs.get('skip_impala', False)
    self.compare_with_hive = kwargs.get('compare_with_hive', False)
    self.hive_cmd = kwargs.get('hive_cmd', 'hive -e ')
    self.target_impalads = deque(kwargs.get('impalad', 'localhost:21000').split(","))
    self.iterations = kwargs.get('iterations', 2)
    self.num_clients = kwargs.get('num_clients', 1)
    self.exec_options = kwargs.get('exec_options', str())
    self.remote = not self.target_impalads[0].startswith('localhost')
    self.profiler = kwargs.get('profiler', False)
    self.use_kerberos = kwargs.get('use_kerberos', False)
    self.run_using_hive = kwargs.get('compare_with_hive', False) or self.skip_impala
    self.verify_results = kwargs.get('verify_results', False)
    self.plugin_runner = kwargs.get('plugin_runner', None)
    self.execution_scope = kwargs.get('execution_scope')
    self.shuffle = kwargs.get('shuffle_queries')
    # TODO: Need to find a way to get this working without runquery
    #self.gprof_cmd = 'google-pprof --text ' + self.runquery_path + ' %s | head -n 60'
    self.__summary = str()
    self.__result_map = defaultdict(list)

  def get_next_impalad(self):
    """Maintains a rotating list of impalads"""
    self.target_impalads.rotate(-1)
    return self.target_impalads[-1]

  # Parse for the tables used in this query
  @staticmethod
  def __parse_tables(query):
    """
    Parse the tables used in this query.
    """
    table_predecessor = ['from', 'join']
    tokens = query.split(' ')
    tables = []
    next_is_table = 0
    for t in tokens:
      t = t.lower()
      if next_is_table == 1:
        tables.append(t)
        next_is_table = 0
      if t in table_predecessor:
        next_is_table = 1
    return tables

  def __get_executor_name(self):
    executor_name = self.client_type
    # We want to indicate this is IMPALA beeswax.
    # We currently don't support hive beeswax.
    return 'impala_beeswax' if executor_name == 'beeswax' else executor_name

  def create_executor(self, executor_name, query, iterations):
    # Add additional query exec options here
    query_options = {
        'hive': lambda: (execute_using_hive,
          HiveQueryExecOptions(iterations,
          hive_cmd=self.hive_cmd,
          )),
        'impala_beeswax': lambda: (execute_using_impala_beeswax,
          ImpalaBeeswaxExecOptions(iterations,
          plugin_runner=self.plugin_runner,
          exec_options=self.exec_options,
          use_kerberos=self.use_kerberos,
          impalad=self.get_next_impalad(),
          query=query
          )),
        'jdbc': lambda: (execute_using_jdbc,
          JdbcQueryExecOptions(iterations,
          impalad=self.get_next_impalad())),
    } [executor_name]()
    return query_options


  def run_query(self, executor_name, query, exit_on_error):
    """
    Run a query command and return the result.

    Creates a query executor object and runs the query. The results are processed
    and coalesced into a single QueryExecResult object before being returned.
    """
    query_exec_func, exec_options = self.create_executor(executor_name, query,
        self.iterations)
    query_executor = QueryExecutor(query_exec_func, executor_name, exec_options, query,
        self.num_clients, exit_on_error)
    query_executor.run()
    results = query_executor.get_results()
    # If all the threads failed, do not call __get_median_exec_result
    # and return an empty execution result.
    if not results: return QueryExecResult()
    return self.__get_median_exec_result(results)

  def __get_median_exec_result(self, results):
    """
    Returns an ExecutionResult object whose avg/stddev is the median of all results.

    This is used when running with multiple clients to select a good representative value
    for the overall execution time.
    """
    # Choose a result to update with the mean avg/stddev values. It doesn't matter which
    # one, so just pick the first one.
    final_result = results[0]
    # Pick a runtime profile from the middle of the result set, for queries that have run
    # for multiple iterations.
    final_result.runtime_profile = results[int(ceil(len(results) / 2))].runtime_profile
    if len(results) == 1:
      return final_result
    final_result.avg_time = calculate_median([result.avg_time for result in results])
    if self.iterations > 1:
      final_result.std_dev = calculate_median([result.std_dev for result in results])
    return final_result

  @staticmethod
  def __enumerate_query_files(base_directory):
    """
    Recursively scan the given directory for all test query files.
    """
    query_files = list()
    for item in os.listdir(base_directory):
      full_path = os.path.join(base_directory, item)
      if isfile(full_path) and item.endswith('.test'):
        query_files.append(full_path)
      elif isdir(full_path):
        query_files += WorkloadRunner.__enumerate_query_files(full_path)
    return query_files

  @staticmethod
  def __extract_queries_from_test_files(workload, query_names):
    """
    Enumerate all the query files for a workload and extract the query strings.
    If the user has specified a subset of queries to execute, only extract those query
    strings.
    """
    query_regex = None
    if query_names:
      # Build a single regex from all query name regex strings.
      query_regex = r'(?:' + ')|('.join([name for name in query_names.split(',')]) + ')'
    workload_base_dir = os.path.join(WORKLOAD_DIR, workload)
    if not isdir(workload_base_dir):
      raise ValueError,\
             "Workload '%s' not found at path '%s'" % (workload, workload_base_dir)

    query_dir = os.path.join(workload_base_dir, 'queries')
    if not isdir(query_dir):
      raise ValueError, "Workload query directory not found at path '%s'" % (query_dir)

    query_map = defaultdict(list)
    for query_file_name in WorkloadRunner.__enumerate_query_files(query_dir):
      LOG.debug('Parsing Query Test File: ' + query_file_name)
      sections = parse_query_test_file(query_file_name)
      test_name = re.sub('/', '.', query_file_name.split('.')[0])[1:]
      # If query_names is not none, only extract user specified queries to
      # the query map.
      if query_names:
        sections = [s for s in sections if re.match(query_regex, s['QUERY_NAME'], re.I)]
      for section in sections:
        query_map[test_name].append((section['QUERY_NAME'],
                                     (section['QUERY'], section['RESULTS'])))
    return query_map

  def execute_queries(self, queries, stop_on_query_error):
    """
    Execute the queries for combinations of file format, compression, etc.

    The values needed to build the query are stored in the first 4 columns of each row.
    """
    executor_name = self.__get_executor_name()
    # each list of queries has the same test vector. pick the first one.
    LOG.info(("Running Test Vector - "
              "File Format: %s "
              "Compression: %s / %s" % (queries[0].test_vector.file_format,
                queries[0].test_vector.compression_codec,
                queries[0].test_vector.compression_type)))

    for query in queries:
      self.__summary += "\nQuery (%s): %s\n" % (query.table_format_str, query.name)
      exec_result = QueryExecResult()
      if not self.skip_impala:
        self.__summary += " Impala Results: "
        LOG.debug('Running: \n%s\n' % query.query_str)
        LOG.info('Query Name: \n%s\n' % query.name)
        exec_result = self.run_query(executor_name, query, stop_on_query_error)
        if exec_result:
          self.__summary += "%s\n" % exec_result

      hive_exec_result = QueryExecResult()
      if self.compare_with_hive or self.skip_impala:
        self.__summary += " Hive Results: "
        hive_exec_result = self.run_query('hive', query, False)
        if hive_exec_result:
          self.__summary += "%s\n" % hive_exec_result
      LOG.debug("---------------------------------------------------------------------")
      self.__result_map[query].append((exec_result, hive_exec_result))

  def execute_workload(self, queries, exit_on_error):
    """Execute a set of queries in a workload.

    A workload is a unique combination of the dataset and the test vector.
    """
    executor_name = self.__get_executor_name()
    query_pipelines =[]
    # Since parallelism and iterations are at the level of a workload, each
    # QueryExecutor runs a single thread once.
    num_query_iter = num_query_clients = 1
    for i in xrange(self.num_clients):
      query_pipeline = dict()
      # Create a mapping from the query name to its executor.
      for query in queries:
        # The number of iterations for an individual query should be 1
        query_exec_func, exec_options = self.create_executor(executor_name, query,
            num_query_iter)
        query_executor = QueryExecutor(query_exec_func, executor_name, exec_options,
            query, num_query_clients, exit_on_error)
        query_pipeline[query] = query_executor
      query_pipelines.append(query_pipeline)

    # Create a workload executor and run the workload.
    workload_executor = WorkloadExecutor(query_pipelines=query_pipelines,
        shuffle=self.shuffle, iterations=self.iterations)
    workload_executor.run()
    query_results = workload_executor.get_results()
    self.__summary = "\nWorkload [%s]:\n" % (queries[0].db.upper())
    # Save the results
    for query, results in query_results.iteritems():
      if not results:
        exec_result = QueryExecResult()
      else:
        exec_result = self.__get_median_exec_result(results)
      self.__result_map[query].append((exec_result, QueryExecResult()))
      self.__summary += " Impala Results: %s\n" % exec_result

  def construct_queries(self, query_map, workload, scale_factor, query_names,
                        test_vector):
    """Constructs a list of query objects based on the test vector and workload"""
    queries = []
    for test_name in query_map.keys():
      for query_name, query_and_expected_result in query_map[test_name]:
        query_str, results = query_and_expected_result
        if not query_name:
          query_name = query_str
        query = Query(name=query_name,
                      query_str=query_str,
                      results=results,
                      workload=workload,
                      scale_factor=scale_factor,
                      test_vector=test_vector)
        queries.append(query)
    return queries

  def get_summary_str(self):
    return self.__summary

  def get_results(self):
    return self.__result_map

  def run_workload(self, workload, scale_factor=str(), table_formats=None,
                   query_names=None, exploration_strategy='core',
                   stop_on_query_error=True):
    """
      Run queries associated with each workload specified on the commandline.

      For each workload specified, look up the associated query files and extract them.
      Execute the queries in a workload as an execution unit if the scope us 'workload'.
      If the scope of execution is a query, run each query individually. Finally,
      aggregate the results.
    """
    LOG.info('Running workload: %s / Scale factor: %s' % (workload, scale_factor))
    query_map = WorkloadRunner.__extract_queries_from_test_files(workload, query_names)
    if not query_map:
      LOG.error('No queries selected to run.')
      return

    test_vectors = None
    if table_formats:
      table_formats = table_formats.split(',')
      dataset = get_dataset_from_workload(workload)
      test_vectors =\
          [TableFormatInfo.create_from_string(dataset, tf) for tf in table_formats]
    else:
      test_vectors = [vector.value for vector in\
          load_table_info_dimension(workload, exploration_strategy)]
    args = [query_map, workload, scale_factor, query_names]
    construct_queries_partial = partial(self.construct_queries, *args)
    query_lists = map(construct_queries_partial, test_vectors)
    exec_func = self.execute_queries
    # Scope is case insensitive.
    if self.execution_scope.lower() == 'workload':
      exec_func = self.execute_workload
    for query_list in query_lists:
      exec_func(query_list, stop_on_query_error)
