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

# Script to load TPC-[H|DS] data in a Kudu cluster.
#
# Kudu tables are created in the specified 'target-db' using the existing HDFS tables
# from 'source-db'.

from __future__ import absolute_import, division, print_function
import logging
import os
import sqlparse
import sys

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

source_db = None
target_db = None
kudu_master = None
verbose = False
buckets = None
workload = None

tpch_tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region',
    'revenue', 'supplier']

tpcds_tables = ['call_center', 'catalog_page', 'catalog_returns', 'catalog_sales',
    'customer', 'customer_address', 'customer_demographics', 'date_dim',
    'household_demographics', 'income_band', 'inventory', 'item', 'promotion',
    'reason', 'ship_mode', 'store', 'store_returns', 'store_sales', 'time_dim',
    'warehouse', 'web_page', 'web_returns', 'web_sales', 'web_site']

def clean_data():
  """Drop the specified 'target_db' and all its tables"""
  with cluster.impala.cursor() as impala:
    tbls_to_clean = tpch_tables if workload.lower() == 'tpch' else tpcds_tables
    # TODO: Replace with DROP DATABASE CASCADE when it is supported for Kudu tables
    for table_name in tbls_to_clean:
      impala.execute("drop table if exists {}.{}".format(target_db, table_name))
    impala.drop_db_if_exists(target_db)

def load_data():
  sql_params = {
      "source_db_name": source_db,
      "target_db_name": target_db,
      "kudu_master": kudu_master,
      "buckets": buckets}

  sql_file_path = get_test_file_path(workload)
  with open(sql_file_path, "r") as test:
    queries = sqlparse.split(test.read())

  with cluster.impala.cursor() as impala:
    impala.create_db_if_not_exists(target_db)
    impala.execute("USE %s" % target_db)
    for query in queries:
      query = sqlparse.format(query.rstrip(';'), strip_comments=True)
      query_str = query.format(**sql_params)
      if (len(query_str)) == 0: continue
      if verbose: print(query_str)
      impala.execute(query_str)

def get_test_file_path(workload):
  if "IMPALA_HOME" not in os.environ:
    raise Exception("IMPALA_HOME must be set")
  sql_file_path = os.path.join(os.environ["IMPALA_HOME"], "testdata", "datasets",
      workload, "%s_kudu_template.sql" % (workload))
  return sql_file_path

if __name__ == "__main__":
  from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
  import tests.comparison.cli_options as cli_options

  parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_cluster_options(parser)
  parser.add_argument("-s", "--source-db", required=True,
      help="Source DB to load data from.")
  parser.add_argument("-t", "--target-db", required=True,
      help="Target DB to load data to.")
  parser.add_argument("-w", "--workload", choices=['tpch', 'tpcds'],
      required=True)
  parser.add_argument("--kudu_master", required=True,
      help="Address or host name of Kudu master")
  # TODO: Automatically set #buckets as a function of cluster nodes and/or
  # scale
  parser.add_argument("-b", "--buckets", default="9",
      help="Number of buckets to partition Kudu tables (only for hash-based).")
  parser.add_argument("-v", "--verbose", action='store_true',
      help="Print the executed statements.")
  parser.add_argument("--clean", action='store_true',
      help="Drop all tables in the speficied target database.")
  args = parser.parse_args()

  cli_options.configure_logging(args.log_level, debug_log_file=args.debug_log_file)
  cluster = cli_options.create_cluster(args)
  source_db = args.source_db
  target_db = args.target_db
  buckets = args.buckets
  kudu_master = args.kudu_master
  workload = args.workload
  verbose = args.verbose
  if args.clean: clean_data()
  load_data()
