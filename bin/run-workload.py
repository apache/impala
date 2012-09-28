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
from tests.common.workload_runner import WorkloadRunner, QueryExecutionDetail

# Options
# TODO: Find ways to reduce the number of options.
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
                  " using colons. For example: -w tpch:1gb. "\
                  "Some valid workloads: 'hive-benchmark', 'tpch', ...")
parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                  help="A comma-separated list of impalad instances to run the "\
                  "workload against.")
parser.add_option("--runquery_path", dest="runquery_path",
                  default=os.path.join(os.environ['IMPALA_HOME'], 'bin/run-query.sh'),
                  help="The command to use for executing queries")
parser.add_option("--runquery_args", dest="runquery_args", default='',
                  help="Additional arguments to pass to runquery.")
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
parser.add_option("-i", "--iterations", type="int", dest="iterations", default=5,
                  help="Number of times to run each query.")
parser.add_option("--prime_cache", dest="prime_cache", action="store_true",
                  default= False, help="Whether or not to prime the buffer cache. ")
parser.add_option("--num_clients", type="int", dest="num_clients", default=1,
                  help="Number of clients (threads) to use when executing each query.")
parser.add_option("--file_formats", dest="file_formats", default=None,
                  help="A comma-separated list of file fomats to execute. If not "\
                  "specified all file formats in the test vector will be run.")
parser.add_option("--query_names", dest="query_names", default=None,
                  help="A comma-separated list of query names to execute.")
parser.add_option("--compression_codecs", dest="compression_codecs", default=None,
                  help="A comma-separated list of compression codecs to execute. If not "\
                  "specified all compression codecs in the test vector will be run.")
parser.add_option("--skip_impala", dest="skip_impala", action="store_true",
                  default= False, help="If set, queries will only run against Hive.")
parser.add_option("--beeswax", dest="beeswax", action="store_true", default=False,
                  help="If set, Impala queries will use the beeswax interface.")

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

# globals
runquery_cmd = "%(runquery)s %(args)s " % {'runquery' : options.runquery_path,
                                           'args' : options.runquery_args}

logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')

LOG = logging.getLogger('run-workload')

def save_results(result_map, output_csv_file, is_impala_result=True):
  """
  Writes the results to an output CSV files
  """
  if result_map is None:
    LOG.error('Result map is None')
    return

  csv_writer = csv.writer(open(output_csv_file, 'wb'), delimiter='|',
                          quoting=csv.QUOTE_MINIMAL)

  for query_tuple, execution_results in result_map.iteritems():
    for result in execution_results:
      query, query_name = query_tuple
      append_row_to_csv_file(csv_writer, query, query_name,
        result[0] if is_impala_result else result[1])

def append_row_to_csv_file(csv_writer, query, query_name, result):
  """
  Write the results to a CSV file with '|' as the delimiter.
  """
  # Replace non-existent values with N/A for reporting results.
  std_dev, avg_time = result.execution_result.std_dev, result.execution_result.avg_time
  if not std_dev: std_dev = 'N/A'
  if not avg_time: avg_time = 'N/A'
  compression_str = '%s/%s' % (result.compression_codec, result.compression_type)
  if compression_str == 'none/none':
    compression_str = 'none'
  csv_writer.writerow([result.executor, result.workload, result.scale_factor,
                       query, query_name, result.file_format, compression_str,
                       avg_time, std_dev,])

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
    LOG.info("Results saving to: %s" % options.results_csv_file)
    save_results(result_map, options.results_csv_file + suffix, is_impala_result=True)
  if options.skip_impala or options.compare_with_hive:
    LOG.info("Hive Results saving to: %s" % options.hive_results_csv_file)
    save_results(result_map, options.hive_results_csv_file + suffix,
                 is_impala_result=False)

def run_workloads(workload_runner, failure_injector=None):
  if failure_injector is not None:
    failure_injector.start()

  for workload_and_scale_factor in options.workloads.split(','):
    workload, scale_factor = parse_workload_scale_factor(workload_and_scale_factor)
    workload_runner.run_workload(workload, scale_factor,
        file_formats=options.file_formats,
        compression_codecs=options.compression_codecs,
        exploration_strategy=options.exploration_strategy,
        stop_on_query_error=failure_injector is None)

  if failure_injector is not None:
    failure_injector.cancel()

def process_results(workload_runner, is_partial_result=False):
  write_results(workload_runner.get_results(), is_partial_result)
  LOG.info(workload_runner.get_summary_str())

if __name__ == "__main__":
  """
  Driver for the run-benchmark script.

  It runs all the workloads specified on the command line and writes them to a csv file.
  """
  workload_runner = WorkloadRunner(
    beeswax=options.beeswax,
    runquery_path=options.runquery_path,
    hive_cmd=options.hive_cmd,
    impalad=options.impalad,
    iterations=options.iterations,
    num_clients=options.num_clients,
    compare_with_hive=options.compare_with_hive,
    skip_impala=options.skip_impala,
    exec_options=options.exec_options,
    profiler=options.profiler,
    verbose=options.verbose,
    prime_cache=options.prime_cache)

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
    raise e
  process_results(workload_runner, is_partial_result=False)
