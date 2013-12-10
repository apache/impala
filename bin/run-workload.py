#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# This script is used to run benchmark queries.  It runs the set queries specified in the
# given workload(s) under <workload name>/queries. This script will first try to warm the
# buffer cache before running the query. There is a command line options to control how
# many iterations to run each query.
#
# By default, the script will have minimal output.  Verbose output can be turned on with
# the -v option which will output the normal query output.  In addition, the -p option
# can be passed which will enable gprof instrumentation and output the sampled call
# stacks.  The -v and -p option are used by the perf regression tests.
#
import csv
import logging
import math
import os
import pickle
import sys
import subprocess
import threading
from collections import defaultdict
from functools import partial
from optparse import OptionParser
from os.path import isfile, isdir
from random import choice
from sys import exit
from time import sleep
from tests.common.workload_runner import WorkloadRunner
from tests.util.plugin_runner import PluginRunner

# Options
# TODO: Group the options for user friendliness and readability.
# TODO: Find a way to reduce the number of options.
parser = OptionParser()
parser.add_option("-p", "--profiler", dest="profiler",
                  action="store_true", default = False,
                  help="If set, also run google pprof for sample profiling.")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default = False, help="If set, outputs all benchmark diagnostics.")
parser.add_option("--exploration_strategy", dest="exploration_strategy", default="core",
                  help="The exploration strategy to use for running benchmark: 'core', "\
                  "'pairwise', or 'exhaustive'")
parser.add_option("-w", "--workloads", dest="workloads", default="hive-benchmark",
                  help="The workload(s) and scale factors to run in a comma-separated "\
                  " list format. Optional scale factors for each workload are specified"\
                  " using colons. For example: -w tpcds,tpch:400gb,tpch:1gb. "\
                  "Some valid workloads: 'hive-benchmark', 'tpch', 'tpcds', ...")
parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                  help="A comma-separated list of impalad instances to run the "\
                  "workload against.")
parser.add_option("--exec_options", dest="exec_options", default='',
                  help="Runquery exec option string.")
parser.add_option("--compare_with_hive", dest="compare_with_hive", action="store_true",
                  default= False, help="Run all queries using Hive as well as Impala")
parser.add_option("--results_csv_file", dest="results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/benchmark_results.csv",
                  help="The output file where benchmark results are saved")
parser.add_option("--hive_results_csv_file", dest="hive_results_csv_file",
                  default=os.environ['IMPALA_HOME'] + "/hive_benchmark_results.csv",
                  help="The output file where Hive benchmark results are saved")
parser.add_option("--hive_cmd", dest="hive_cmd", default="hive -e",
                  help="The command to use for executing hive queries")
parser.add_option("-i", "--iterations", type="int", dest="iterations", default=1,
                  help="Number of times to run each query.")
parser.add_option("--num_clients", type="int", dest="num_clients", default=1,
                  help="Number of clients (threads) to use when executing each query.")
parser.add_option("--query_names", dest="query_names", default=None,
                  help="A comma-separated list of query names to execute.")
parser.add_option("--table_formats", dest="table_formats", default=None, help=\
                  "Override the default test vectors and run using only the specified "\
                  "table formats. Ex. --table_formats=seq/snap/block,text/none")
parser.add_option("--skip_impala", dest="skip_impala", action="store_true",
                  default= False, help="If set, queries will only run against Hive.")
parser.add_option("--execution_scope", dest="execution_scope", default="query",
                  help=("The unit of execution for a workload and its test vectors. The "
                        "default scope is 'query'; In this scope, user provided "
                        "execution parameters like number of iterations and number of "
                        "clents will operate on on a single query, which is a "
                        "combination of the workload, scale factor and test vector. "
                        "When 'workload' is specified as the scope, the execution "
                        "paramaters will operate over a set of queries under a "
                        "workload, scale factor and a test vector"))
parser.add_option("--shuffle_query_exec_order", dest="shuffle_queries",
                  action="store_true", default=False, help=("Randomizes the order "
                    "of query execution. Useful when the execution scope is a workload"))

parser.add_option("--use_kerberos", dest="use_kerberos", action="store_true",
                  default=False, help="If set, enables talking to a kerberized impalad")
parser.add_option("--continue_on_query_error", dest="continue_on_query_error",
                  action="store_true", default=False, help="If set, continue execution "\
                  "on each query error.")
parser.add_option("-V", "--verify_results", dest="verify_results", action="store_true",
                  default=False, help="If set, verifies query results")
parser.add_option("-c", "--client_type", dest="client_type", default='beeswax',
                  help="Client type. Valid options are 'beeswax' or 'jdbc'")
parser.add_option("--plugin_names", dest="plugin_names", default=None,
                  help=("Set of comma-separated plugin names with scope; Plugins are"
                    " specified as <plugin_name>[:<scope>]. If no scope if specified,"
                    " it defaults to Query. Plugin names are case sensitive"))

# These options are used for configuring failure testing
parser.add_option("--failure_frequency", type="int", dest="failure_frequency", default=0,
                  help="Interval (in seconds) to inject each failure or 0 to disable"\
                  " failure injection.")
parser.add_option("--cm_cluster_name", dest="cm_cluster_name",
                  help="The CM name of the cluster under test")
parser.add_option("--cm_server_host", dest="cm_server_host",
                  help="The host of the CM server.")
parser.add_option("--cm_username", dest="cm_username", default='admin',
                  help="The username to the CM server")
parser.add_option("--cm_password", dest="cm_password", default='admin',
                  help="The password to the CM server")
options, args = parser.parse_args()

logging.basicConfig(level=logging.INFO, format='[%(name)s]: %(message)s')
LOG = logging.getLogger('run-workload')

def save_results(result_map, output_csv_file, is_impala_result=True):
  """
  Writes the results to an output CSV files
  TODO: This should be changed to use JSON as the container format. This is especially
  important as we add more fields to the results.
  """
  if result_map is None:
    LOG.error('Result map is None')
    return

  # The default field size limit is too small to write big runtime profiles. Set
  # the limit to an artibrarily large value.
  csv.field_size_limit(sys.maxint)
  csv_writer = csv.writer(open(output_csv_file, 'wb'), delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query, exec_results in result_map.iteritems():
    for result in exec_results:
      append_row_to_csv_file(csv_writer, query,
        result[0] if is_impala_result else result[1])

def append_row_to_csv_file(csv_writer, query, result):
  """
  Write the results to a CSV file with '|' as the delimiter.
  """
  # If the query failed, and returned an empty result, print its details to console.
  if not result.success:
    LOG.info("All threads failed for Query:%s, Scale Factor:%s, Table Format:%s" %
        (query.name, query.scale_factor, query.table_format_str))
    # Don't store a blank result.
    return
  # Replace non-existent values with N/A for reporting results.
  if not result.std_dev: result.std_dev = 'N/A'
  if not result.avg_time: result.avg_time = 'N/A'
  compression_str = '%s/%s' % (query.test_vector.compression_codec,
                               query.test_vector.compression_type)
  if compression_str == 'none/none': compression_str = 'none'
  # The number of iterations in the performance database is a query execution parameter.
  # Since the query is run only once when executing at a 'workload' scope, reset it to 1
  num_iters = options.iterations if options.execution_scope.lower() == 'query' else 1
  csv_writer.writerow([result.executor_name, query.workload, query.scale_factor,
                       query.name, query.query_str, query.test_vector.file_format,
                       compression_str, result.avg_time, result.std_dev,
                       options.num_clients, num_iters, result.runtime_profile])

def enumerate_query_files(base_directory):
  """
  Recursively scan the given directory for all test query files.
  """
  query_files = list()
  for item in os.listdir(base_directory):
    full_path = os.path.join(base_directory, item)
    if isfile(full_path) and item.endswith('.test'):
      query_files.append(full_path)
    elif isdir(full_path):
      query_files += enumerate_query_files(full_path)
  return query_files

def parse_workload_scale_factor(workload_scale_factor):
  parsed_workload_scale_factor = workload_scale_factor.split(':')
  if len(parsed_workload_scale_factor) == 1:
    return parsed_workload_scale_factor[0], ''
  elif len(parsed_workload_scale_factor) == 2:
    return parsed_workload_scale_factor
  else:
    LOG.error("Error parsing workload. Proper format is workload[:scale factor]")
    sys.exit(1)

def write_results(result_map, partial_results = False):
  suffix = '.partial' if partial_results else ''

  if not options.skip_impala:
    LOG.info("Saving results to: %s" % options.results_csv_file)
    save_results(result_map, options.results_csv_file + suffix, is_impala_result=True)
  if options.skip_impala or options.compare_with_hive:
    LOG.info("Saving hive results to: %s" % options.hive_results_csv_file)
    save_results(result_map, options.hive_results_csv_file + suffix,
                 is_impala_result=False)

def run_workloads(workload_runner, failure_injector=None):
  stop_on_error = (failure_injector is None) and (not options.continue_on_query_error)
  if failure_injector is not None:
    failure_injector.start()

  for workload_and_scale_factor in options.workloads.split(','):
    workload, scale_factor = parse_workload_scale_factor(workload_and_scale_factor)
    if plugin_runner: plugin_runner.run_plugins_pre(scope="Workload")
    workload_runner.run_workload(workload, scale_factor,
        table_formats=options.table_formats,
        query_names=options.query_names,
        exploration_strategy=options.exploration_strategy,
        stop_on_query_error=stop_on_error)
    if plugin_runner: plugin_runner.run_plugins_post(scope="Workload")

  if failure_injector is not None:
    failure_injector.cancel()

def process_results(workload_runner, is_partial_result=False):
  write_results(workload_runner.get_results(), is_partial_result)
  print '\nSUMMARY OF RESULTS'
  print '------------------'
  print workload_runner.get_summary_str()

if __name__ == "__main__":
  """
  Driver for the run-benchmark script.

  It runs all the workloads specified on the command line and writes them to a csv file.
  """
  if options.use_kerberos:
    try:
      import sasl
    except ImportError:
      print 'The sasl module is needed to query a kerberized impalad'
      sys.exit(1)

  VALID_CLIENT_TYPES = ['beeswax', 'jdbc']
  if options.client_type not in VALID_CLIENT_TYPES:
    LOG.error("Invalid client type %s" % options.client_type)
    sys.exit(1)

  plugin_runner = None
  if options.plugin_names:
    plugin_runner = PluginRunner(options.plugin_names.split(','))

  # create a dictionary for the configuration.
  workload_config = vars(options)
  workload_config['plugin_runner'] = plugin_runner
  workload_runner = WorkloadRunner(**workload_config)

  failure_injector = None
  if options.failure_frequency > 0:
    # If not doing failure testing there is no reason to import these modules which
    # have additional dependencies.
    from common.failure_injector import FailureInjector
    from common.impala_cluster import ImpalaCluster

    cluster = ImpalaCluster(options.cm_server_host, options.cm_cluster_name,
       username=options.cm_username, password=options.cm_password)
    failure_injector = FailureInjector(cluster,
        failure_frequency=options.failure_frequency,
        impalad_exclude_list=options.impalad.split(','))

  try:
    run_workloads(workload_runner, failure_injector)
  except Exception, e:
    if failure_injector is not None:
      failure_injector.cancel()
    process_results(workload_runner, is_partial_result=True)
    raise
  process_results(workload_runner, is_partial_result=False)
