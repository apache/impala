#!/usr/bin/env impala-python
#
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
# This script is used as the driver to run performance benchmarks.
# It does the following:
#   - parses the user defined options and validates them.
#   - Matches each workload to its set of queries and constructs the required objects.
#   - Runs each workload in serial order (a workload is a combination of dataset and scale
#     factor)
#   - Pretty prints the results of each query's execution.
#   - Stores the execution details in JSON format.
#

from __future__ import absolute_import, division, print_function
import getpass
import json
import logging
import os
import prettytable

from collections import deque
from copy import deepcopy
from datetime import datetime
from decimal import Decimal
from itertools import groupby
from optparse import OptionParser
from random import shuffle
from sys import exit

from tests.common.test_dimensions import TableFormatInfo
from tests.performance.query import Query, HiveQueryResult
from tests.performance.query_executor import QueryExecConfig
from tests.performance.workload_runner import WorkloadRunner
from tests.performance.workload import Workload
from tests.util.plugin_runner import PluginRunner

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store_true",
                  default=False, help="If set, outputs all benchmark diagnostics.")
parser.add_option("--exploration_strategy", dest="exploration_strategy", default="core",
                  help=("The exploration strategy to use for running benchmark: 'core', "
                        "'pairwise', or 'exhaustive'"))
parser.add_option("-w", "--workloads", dest="workloads", default="tpcds",
                  help=("The workload(s) and scale factors to run in a comma-separated "
                  " list format. Optional scale factors for each workload are specified"
                  " using colons. For example: -w tpcds,tpch:400gb,tpch:1gb. "
                  "Some valid workloads:'tpch', 'tpcds', ..."))
parser.add_option("--impalads", dest="impalads", default="localhost",
                  help=("A comma-separated list of impalad instances to run the "
                  "workload against."))
parser.add_option("--exec_options", dest="exec_options", default=str(),
                  help=("Run query exec option string "
                    "(formatted as 'opt1:val1;opt2:val2')."))
parser.add_option("--results_json_file", dest="results_json_file",
                  default=os.environ['IMPALA_HOME'] + "/benchmark_results.json",
                  help="The output file where benchmark results are saved")
parser.add_option("-i", "--query_iterations", type="int", dest="query_iterations",
                  default=1, help="Number of times to run each query within a workload")
parser.add_option("-x", "--workload_iterations", type="int", dest="workload_iterations",
                  default=1, help="Number of times to run each workload.")
parser.add_option("--num_clients", type="int", dest="num_clients", default=1,
                  help="Number of clients (threads) to use when executing each query.")
parser.add_option("--query_names", dest="query_names", default=str(),
                  help="A comma-separated list of regular expressions. A query is"
                    " executed if it matches any of the expressions.")
parser.add_option("--table_formats", dest="table_formats", default=str(),
                  help=("Override the default test vectors and run using only the"
                        " specified table formats. Ex. --table_formats=seq/snap/block"
                        ",text/none"))
parser.add_option("--shuffle_query_exec_order", dest="shuffle_queries",
                  action="store_true", default=False, help=("Randomizes the order "
                    "of query execution. Useful when the execution scope is a workload"))
parser.add_option("--plan_first", dest="plan_first", action="store_true", default=False,
                  help=("Runs EXPLAIN before running the query so that metadata loading"
                        " is excluded from the timing"))

parser.add_option("--use_kerberos", dest="use_kerberos", action="store_true",
                  default=False, help="If set, enables talking to a kerberized impalad")
parser.add_option("--continue_on_query_error", dest="continue_on_query_error",
                  action="store_true", default=False,
                  help="If set, continue execution on each query error.")
parser.add_option("-c", "--client_type", dest="client_type", default='beeswax',
                  choices=['beeswax', 'jdbc', 'hs2'],
                  help="Client type. Valid options are 'beeswax' or 'jdbc' or 'hs2'")
parser.add_option("--plugin_names", dest="plugin_names", default=None,
                  help=("Set of comma-separated plugin names with scope; Plugins are"
                    " specified as <plugin_name>[:<scope>]. If no scope if specified,"
                    " it defaults to Query. Plugin names are case sensitive"))
parser.add_option("--exec_engine", dest="exec_engine", default="impala",
                  choices=['impala', 'hive'],
                  help=("Which SQL engine to use - impala, hive are valid options"))
parser.add_option("--hiveserver", dest="hiveserver", default="localhost",
                  help=("Host that has HiveServers2 service running"))
parser.add_option("--user", dest="user", default=getpass.getuser(),
                  help=("User account under which workload/query will run"))
parser.add_option("--get_password", dest="get_password", default=False,
                  action="store_true", help=("Prompt for password for user account"))
parser.add_option("--use_ssl", dest="use_ssl", action="store_true", default=False,
                  help=("Whether to use SSL or not"))

options, args = parser.parse_args()

options.password = None
if options.get_password:
  options.password = getpass.getpass()
  options.get_password = None

LOG = logging.getLogger('run-workload')


class WorkloadConfig(object):
  """Converts the options dict into a class"""
  def __init__(self, **config):
    self.__dict__.update(config)


class CustomJSONEncoder(json.JSONEncoder):
  """Override the JSONEncoder's default method.

  This class is needed for two reasons:
    - JSON does have a datetime field. We intercept a datetime object and convert it into
      a standard iso string.
    - JSON does not know how to serialize object. We intercept the objects and
      provide their __dict__ representations
  """
  def default(self, obj,):
    if isinstance(obj, Decimal):
      return str(obj)
    if isinstance(obj, datetime):
      # Convert datetime into an standard iso string
      return obj.isoformat()
    elif isinstance(obj, (Query, HiveQueryResult, QueryExecConfig, TableFormatInfo)):
      # Serialize these objects manually by returning their __dict__ methods.
      return obj.__dict__
    else:
      super(CustomJSONEncoder, self).default(obj)


def prettytable_print(results, failed=False):
  """Print a list of query results in prettytable"""
  column_names = ['Query', 'Start Time', 'Time Taken (s)', 'Client ID']
  if failed: column_names.append('Error')
  table = prettytable.PrettyTable(column_names)
  table.align = 'l'
  table.float_format = '.2'
  # Group the results by table format.
  for table_format_str, gr in groupby(results, lambda x: x.query.table_format_str):
    print("Table Format: %s" % table_format_str)
    for result in gr:
      start_time = result.start_time.strftime("%Y-%m-%d %H:%M:%S") if result.start_time \
          is not None else '-'
      row = [result.query.name, start_time, result.time_taken, result.client_name]
      if failed: row.append(result.query_error)
      table.add_row(row)
    print(table.get_string(sortby='Client ID'))
    table.clear_rows()
    print(str())


def print_result_summary(results):
  """Print failed and successfull queries for a given result list"""
  failed_results = [x for x in results if not x.success]
  successful_results = [x for x in results if x.success]
  prettytable_print(successful_results)
  if failed_results: prettytable_print(failed_results, failed=True)


def get_workload_scale_factor():
  """Extract the workload -> scale factor mapping from the command line

  The expected string is "workload_1[:scale_factor_1],...,workload_n[:scale_factor_n]"
  """
  workload_str = options.workloads
  workload_tuples = split_and_strip(workload_str)
  assert len(workload_tuples) > 0, "At least one workload must be specified"
  for workload_tuple in workload_tuples:
    # Each member should conform to workload[:scale_factor]
    workload_tuple = split_and_strip(workload_tuple, delim=":")
    assert len(workload_tuple) in [1, 2], "Error parsing workload:scale_factor"
    if len(workload_tuple) == 1: workload_tuple.append(str())
    yield workload_tuple


def split_and_strip(input_string, delim=","):
  """Convert a string into a list using the given delimiter"""
  if not input_string: return list()
  return list(map(str.strip, input_string.split(delim)))


def create_workload_config():
  """Parse command line inputs.

  Some user inputs needs to be transformed from delimited strings to lists in order to be
  consumed by the performacne framework. Additionally, plugin_names are converted into
  objects, and need to be added to the config.
  """
  config = deepcopy(vars(options))
  # We don't need workloads and query_names in the config map as they're already specified
  # in the workload object.
  del config['workloads']
  del config['query_names']
  config['plugin_runner'] = plugin_runner
  # transform a few options from strings to lists
  config['table_formats'] = split_and_strip(config['table_formats'])
  impalads = split_and_strip(config['impalads'])
  # Randomize the order of impalads.
  shuffle(impalads)
  config['impalads'] = deque(impalads)
  return WorkloadConfig(**config)


def _validate_options():
  """Basic validation for some commandline options"""
  # the sasl module must be importable on a secure setup.
  if options.use_kerberos: import sasl

  # If Hive is the exec engine, hs2 is the only suported interface.
  if options.exec_engine.lower() == "hive" and options.client_type != "hs2":
    raise RuntimeError("The only supported client type for Hive engine is hs2")

  # Check for duplicate workload/scale_factor combinations
  workloads = split_and_strip(options.workloads)
  if not len(set(workloads)) == len(workloads):
    raise RuntimeError("Duplicate workload/scale factor combinations are not allowed")

  # The list of Impalads must be provided as a comma separated list of either host:port
  # combination or just host.
  for impalad in split_and_strip(options.impalads):
    if len(impalad.split(":")) not in [1, 2]:
      raise RuntimeError("Impalads must be of the form host:port or host.")


if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO, format='[%(name)s]: %(message)s')
  # Check for badly formed user options.
  _validate_options()

  # Intialize the PluginRunner.
  plugin_runner = None
  if options.plugin_names:
    plugin_runner = PluginRunner(split_and_strip(options.plugin_names))

  # Intialize workloads.
  workload_runners = list()
  query_name_filters = split_and_strip(options.query_names)
  # Create a workload config object.
  for workload_name, scale_factor in get_workload_scale_factor():
    config = create_workload_config()
    workload = Workload(workload_name, query_name_filters=query_name_filters)
    workload_runners.append(WorkloadRunner(workload, scale_factor, config))

  # Run all the workloads serially
  result_map = dict()
  exit_code = 0
  for workload_runner in workload_runners:
    try:
      if plugin_runner: plugin_runner.run_plugins_pre(scope="Workload")
      workload_runner.run()
      if plugin_runner: plugin_runner.run_plugins_post(scope="Workload")
    finally:
      key = "%s_%s" % (workload_runner.workload.name, workload_runner.scale_factor)
      result_map[key] = workload_runner.results

      if not all(result.success for result in workload_runner.results): exit_code = 1

      # Print the results
      print("\nWorkload: {0}, Scale Factor: {1}\n".format(
          workload_runner.workload.name.upper(), workload_runner.scale_factor))
      print_result_summary(workload_runner.results)

  # Store the results
  with open(options.results_json_file, 'w') as f:
    json.dump(result_map, f, cls=CustomJSONEncoder, ensure_ascii=False)

  exit(exit_code)
