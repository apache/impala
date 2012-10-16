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
from query_executor import *
from collections import defaultdict
from optparse import OptionParser
from functools import partial
from os.path import isfile, isdir
from time import sleep
from random import choice

# globals
WORKLOAD_DIR = os.environ['IMPALA_WORKLOAD_DIR']
IMPALA_HOME = os.environ['IMPALA_HOME']
PROFILE_OUTPUT_FILE = os.path.join(IMPALA_HOME, 'be/build/release/service/profile.tmp')
RUNQUERY_CMD = "%(runquery)s %(args)s "
PRIME_CACHE_CMD = os.path.join(IMPALA_HOME, "testdata/bin/cache_tables.py") + " -q \"%s\""

dev_null = open(os.devnull)

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('workload_runner')

class QueryExecutionDetail(object):
  def __init__(self, executor, workload, scale_factor, file_format, compression_codec,
               compression_type, execution_result):
    self.executor = executor
    self.workload = workload
    self.scale_factor = scale_factor
    self.file_format = file_format
    self.compression_codec = compression_codec
    self.compression_type = compression_type
    self.execution_result = execution_result


# Runs query files and captures results from the specified workload(s)
# The usage is:
# 1) Initialize WorkloadRunner with desired execution parameters.
# 2) Call workload_runner.run_workload() passing in a workload name(s) and scale
# factor(s).
# Internally, for each workload, this module looks up and parses that workload's
# query files and reads the workload's test vector to determine what combination(s)
# of file format / compression to run with. The queries are then executed
# and the results are displayed as well as saved to a CSV file.
class WorkloadRunner(object):
  def __init__(self, **kwargs):
    self.verbose = kwargs.get('verbose', False)
    if self.verbose:
      LOG.setLevel(level=logging.DEBUG)

    self.beeswax = kwargs.get('beeswax', True)
    self.skip_impala = kwargs.get('skip_impala', False)
    self.compare_with_hive = kwargs.get('compare_with_hive', False)
    self.runquery_path = kwargs.get('runquery_path',\
                                    os.path.join(IMPALA_HOME, 'bin/run-query.sh'))
    self.hive_cmd = kwargs.get('hive_cmd', 'hive -e ')
    self.TARGET_IMPALADS = kwargs.get('impalad', 'localhost:21000').split(",")
    self.iterations = kwargs.get('iterations', 2)
    self.num_clients = kwargs.get('num_clients', 1)
    self.exec_options = kwargs.get('exec_options', str())
    self.prime_cache = kwargs.get('prime_cache', False)
    self.remote = not self.TARGET_IMPALADS[0].startswith('localhost')
    self.profiler = kwargs.get('profiler', False)
    self.use_kerberos = kwargs.get('use_kerberos', False)
    self.runquery_cmd = RUNQUERY_CMD % {'runquery' : self.runquery_path,
                                        'args' : kwargs.get('runquery_args', str())}
    self.run_using_hive = kwargs.get('compare_with_hive', False) or self.skip_impala
    self.gprof_cmd = 'google-pprof --text ' + self.runquery_path + ' %s | head -n 60'
    self.__summary = str()
    self.__result_map = defaultdict(list)

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

  def prime_remote_or_local_cache(self, query, remote, hive=False):
    """
    Prime either the local cache or buffer cache for a remote machine.
    """
    if hive and remote:
      self.prime_buffer_cache_remote_hive(query)
    elif remote:
      self.prime_buffer_cache_remote_impala(query)
    else:
      self.prime_buffer_cache_local(query)

  def prime_buffer_cache_remote_impala(self, query):
    """
    Prime the buffer cache on remote machines for impala.

    On remote clusters, we'll prime the buffer cache by just running count(*)
    TODO: does this work well enough? Does having a real cluster and data
    locality make this more deterministic?
    """
    tables = WorkloadRunner.__parse_tables(query)
    subprocess_call_partial = partial()
    for table in tables:
      count_cmd = '%s -query="select count(*) from %s" --iterations=5' % \
                  (self.runquery_cmd, table)
      subprocess.call(count_cmd, shell=True, stderr=dev_null, stdout=dev_null)

  def prime_buffer_cache_remote_hive(self, query):
    """
    Prime the buffer cache on remote machines for hive.
    """
    tables = WorkloadRunner.__parse_tables(query)
    for table in tables:
      for iteration in range(5):
        count_query = 'select count(*) from %s' % table
        subprocess.call(self.hive_cmd + "\"%s\"" % count_query, shell=True,
                        stderr=dev_null, stdout=dev_null)

  def prime_buffer_cache_local(self, query):
    """
    Prime the buffer cache on mini-dfs.

    We can prime the buffer cache by accessing the local file system.
    """
    # TODO: Consider making cache_tables a module rather than directly calling the script
    command = PRIME_CACHE_CMD % query
    os.system(command)

  def create_executor(self, executor_name):
    # Add additional query exec options here
    query_options = {
      'runquery': lambda: (execute_using_runquery,
          RunQueryExecOptions(self.iterations,
          impalad=choice(self.TARGET_IMPALADS),
          exec_options=self.exec_options,
          runquery_cmd=self.runquery_cmd,
          enable_counters=int(self.verbose),
          profile_output_file=PROFILE_OUTPUT_FILE,
          profiler=self.profiler,
          )),
      'hive': lambda: (execute_using_hive,
          HiveQueryExecOptions(self.iterations,
          hive_cmd=self.hive_cmd,
          )),
      'impala_beeswax': lambda: (execute_using_impala_beeswax,
          ImpalaBeeswaxExecOptions(self.iterations,
          exec_options=self.exec_options,
          use_kerberos=self.use_kerberos,
          impalad=choice(self.TARGET_IMPALADS)))
    } [executor_name]()
    return query_options

  def run_query(self, executor_name, query, prime_cache, exit_on_error):
    """
    Run a query command and return the result.

    Takes in a match functional that is used to parse stderr/out to extract the results.
    """
    if prime_cache:
      self.prime_remote_or_local_cache(query, self.remote, executor_name == 'hive')

    threads = []
    results = []

    output = None
    execution_result = None
    for client in xrange(self.num_clients):
      name = "Client Thread " + str(client)
      exec_tuple = self.create_executor(executor_name)
      threads.append(QueryExecutor(name, exec_tuple[0], exec_tuple[1], query))
    for thread in threads:
      LOG.debug(thread.name + " starting")
      thread.start()

    for thread in threads:
      thread.join()
      if not thread.success() and exit_on_error:
        LOG.error("Thread: %s returned with error. Exiting." % thread.name)
        raise RuntimeError, "Error executing query. Aborting"

      results.append((thread.get_results()))
      LOG.debug(thread.name + " completed")
    return results[0]

  @staticmethod
  def __database_name_to_use(workload, scale_factor):
    """
    Return the name of the database to use for the specified workload and scale factor.
    """
    if workload == 'tpch':
      return '%s%s.' % (workload, scale_factor)
    return ''

  @staticmethod
  def __build_table_suffix(file_format, codec, compression_type):
    if file_format == 'text' and codec == 'none':
      return ''
    elif codec == 'none':
      return '_%s' % (file_format)
    elif compression_type == 'record':
      return '_%s_record_%s' % (file_format, codec)
    else:
      return '_%s_%s' % (file_format, codec)

  @staticmethod
  def __build_query(query_format_string, file_format, codec, compression_type,
                  workload, scale_factor):
    """
    Build a well formed query.

    Given the various test parameters, construct the query that will be executed.
    """
    database_name = WorkloadRunner.__database_name_to_use(workload, scale_factor)
    table_suffix = WorkloadRunner.__build_table_suffix(file_format, codec,
                                                       compression_type)

    # $TABLE is used as a token for table suffix in the queries. Here we replace the token
    # the proper database name based on the workload and scale factor.
    # There also may be cases where there is dbname.table_name without a
    # $TABLE (in the case of insert).
    replace_from =\
        '(%(workload)s\.)(?P<table_name>\w+)' % {'workload': workload}
    replace_by = '%s%s' % (database_name, r'\g<table_name>')
    query_str = re.sub(replace_from, replace_by, query_format_string)

    replace_from =\
        '(%(workload)s){0,1}(?P<table_name>\w+)\$TABLE' % {'workload': database_name}
    replace_by = '%s%s%s' % (database_name, r'\g<table_name>', table_suffix)
    return re.sub(replace_from, replace_by, query_str)

  @staticmethod
  def __read_vector_file(file_name):
    """
    Parse the test vector file.

    TODO: Move this parsing functionality into its own module.
    """
    if not isfile(file_name):
      raise ValueError, 'Cannot find vector file: ' + file_name

    vector_values = list()
    with open(file_name, 'rb') as vector_file:
      for line in vector_file.readlines():
        if line.strip().startswith('#'):
          continue
        vector_values.append([value.split(':')[1].strip() for value in line.split(',')])
    return vector_values

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
  def __is_comment_or_empty(line):
    """
    Return True of the line is a comment or and empty string.
    """
    comment = line.strip().startswith('#') or line.strip().startswith('//')
    return not line or not comment

  @staticmethod
  def __extract_queries_from_test_files(workload):
    """
    Enumerate all the query files for a workload and extract the query strings.
    """
    workload_base_dir = os.path.join(WORKLOAD_DIR, workload)
    if not isdir(workload_base_dir):
      raise ValueError,\
             "Workload '%s' not found at path '%s'" % (workload, workload_base_dir)

    query_dir = os.path.join(workload_base_dir, 'queries')
    if not isdir(query_dir):
      raise ValueError, "Workload query directory not found at path '%s'" % (query_dir)

    query_map = dict()
    for query_file_name in WorkloadRunner.__enumerate_query_files(query_dir):
      LOG.debug('Parsing Query Test File: ' + query_file_name)
      with open(query_file_name, 'rb') as query_file:
        # Query files are split into sections separated by '=====', with subsections
        # separeted by '----'. The first item in each subsection is the actual query
        # to execute
        # TODO : Use string.replace
        test_name = re.sub('/', '.', query_file_name.split('.')[0])[1:]
        query_map[test_name] = []
        # Some of the read queries are just blank lines, this cleans up the noise.
        query_sections = [qs for qs in query_file.read().split('====') if qs.strip()]
        for query_section in query_sections:
          if not query_section.strip():
            continue
          query = query_section.split('----')[0]
          # TODO : Use re.match
          query_name = re.findall('QUERY_NAME : .*', query)
          query = query.split('\n')
          formatted_query = ('\n').join(\
              filter(WorkloadRunner.__is_comment_or_empty, query)).strip()
          if query_name and formatted_query:
            query_name = query_name[0].split(':')[-1].strip()
            query_map[test_name].append((query_name, formatted_query))
          elif formatted_query:
            query_map[test_name].append((None, formatted_query))
    return query_map


  def execute_queries(self, query_map, workload, scale_factor, exploration_strategy,
                      file_formats, compression_codecs, query_names, stop_on_query_error,
                      vector_row):
    """
    Execute the queries for combinations of file format, compression, etc.

    The values needed to build the query are stored in the first 4 columns of each row.
    """
    # TODO : Find a clean way to get rid of globals.
    file_format, data_group, codec, compression_type = vector_row[:4]
    if (file_formats and (file_format not in file_formats.split(','))) or\
       (compression_codecs and (codec not in compression_codecs.split(','))):
      LOG.info("Skipping Test Vector - File Format: %s Compression: %s / %s" %\
          (file_format, codec, compression_type))
      return
    executor_name = 'impala_beeswax' if self.beeswax else 'runquery'

    query_name_filter = None
    if query_names:
      query_name_filter = [name.lower() for name in query_names.split(',')]

    LOG.info("Running Test Vector - File Format: %s Compression: %s / %s" %\
        (file_format, codec, compression_type))
    for test_name in query_map.keys():
      for query_name, query in query_map[test_name]:
        if not query_name:
          query_name = query
        if query_name_filter and (query_name.lower() not in query_name_filter):
          LOG.info("Skipping query '%s'" % query_name)
          continue

        query_string = WorkloadRunner.__build_query(query.strip(), file_format, codec,
                                                    compression_type, workload,
                                                    scale_factor)
        self.__summary += "\nQuery: %s\n" % query_name
        execution_result = QueryExecutionResult()
        if not self.skip_impala:
          self.__summary += "Results Using Impala\n"
          LOG.debug('Running: \n%s\n' % query_string)
          if query_name != query:
            LOG.info('Query Name: \n%s\n' % query_name)

          execution_result = self.run_query(executor_name, query_string,
                                            self.prime_cache, stop_on_query_error)
          self.__summary += " ->%s\n" % execution_result

        hive_execution_result = QueryExecutionResult()
        if self.compare_with_hive or self.skip_impala:
          self.__summary += "Results Using Hive\n"
          hive_execution_result = self.run_query('hive', query_string,
                                                         self.prime_cache,
                                                         False)
          self.__summary += " ->%s\n" % hive_execution_result
        LOG.debug("---------------------------------------------------------------------")

        execution_detail = QueryExecutionDetail(executor_name, workload, scale_factor,
            file_format, codec, compression_type, execution_result)

        hive_execution_detail = QueryExecutionDetail('hive', workload, scale_factor,
            file_format, codec, compression_type, hive_execution_result)

        self.__result_map[(query_name, query)].append((execution_detail,
                                                       hive_execution_detail))

  def get_summary_str(self):
    return self.__summary

  def get_results(self):
    return self.__result_map

  def run_workload(self, workload, scale_factor=str(), file_formats=None,
                   compression_codecs=None,query_names=None, exploration_strategy='core',
                   stop_on_query_error=True):
    """
      Run queries associated with each workload specified on the commandline.

      For each workload specified in, look up the associated query files. Extract valid
      queries in each file and execute them using the specified number of execution
      iterations. Finally, write results to an output CSV file for reporting.
    """
    LOG.info('Running workload: %s / Scale factor: %s' % (workload, scale_factor))
    query_map = WorkloadRunner.__extract_queries_from_test_files(workload)
    vector_file_path = os.path.join(WORKLOAD_DIR, workload,
                                    "%s_%s.csv" % (workload, exploration_strategy))
    test_vector = WorkloadRunner.__read_vector_file(vector_file_path)
    args = [query_map, workload, scale_factor, exploration_strategy,
            file_formats, compression_codecs, query_names, stop_on_query_error]
    execute_queries_partial = partial(self.execute_queries, *args)
    map(execute_queries_partial, test_vector)
