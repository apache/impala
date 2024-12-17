#!/usr/bin/env impala-python3
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
# Utility for computing table statistics of tables in the Hive Metastore

from __future__ import absolute_import, division, print_function
from contextlib import contextmanager
from argparse import ArgumentParser
import logging
import multiprocessing
import multiprocessing.pool
import os

from tests.common.impala_connection import create_connection

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(threadName)s: %(message)s')
LOG = logging.getLogger(__name__)
DEFAULT_PARALLELISM = int(
    os.environ.get('IMPALA_BUILD_THREADS', multiprocessing.cpu_count()))


def compute_stats_table(client_factory, db, table):
  """
  Runs 'compute stats' on a given table. If continue_on_error is
  True, exceptions computing statistics are swallowed.
  """
  with client_factory() as impala_client:
    db_table = "%s.%s" % (db, table)
    statement = "compute stats %s" % (db_table,)
    LOG.info('Executing: %s', statement)
    try:
      result = impala_client.execute(statement)
      LOG.info(" %s -> %s", db_table, ' '.join(result.data).strip())
      return db_table
    except Exception as e:
      LOG.exception(' Failed on table %s', db_table)
      raise e


def log_completion(completed, total_tables, error=None):
    if error:
      LOG.error("Completed COMPUTE STATS for %d/%d tables with error.",
                completed, total_tables, exc_info=error)
    else:
      LOG.info("Completed COMPUTE STATS for %d/%d tables.", completed, total_tables)


def compute_stats(client_factory, db_names=None, table_names=None,
                  exclude_table_names=None, continue_on_error=False,
                  parallelism=DEFAULT_PARALLELISM):
  """
  Runs COMPUTE STATS over the selected tables. The target tables can be filtered by
  specifying a list of databases and/or table names. If no filters are specified this will
  run COMPUTE STATS on all tables in all databases.

  parallelism controls the size of the thread pool to which compute_stats
  is sent.
  """
  LOG.info("Enumerating databases and tables for compute stats. "
           "db_names={} table_names={} exclude_table_names={} parallelism={}.".format(
             str(db_names), str(table_names), str(exclude_table_names), parallelism
           ))

  pool = multiprocessing.pool.ThreadPool(processes=parallelism)
  futures = []

  with client_factory() as impala_client:
    db_table_map = {}
    total_tables = 0
    all_dbs = set(name.split('\t')[0].lower() for name
        in impala_client.execute("show databases").data)
    selected_dbs = all_dbs if db_names is None else set(db_names)
    for db in all_dbs.intersection(selected_dbs):
      all_tables = set(
        [t.lower() for t in impala_client.execute("show tables in %s" % db).data])
      selected_tables = all_tables if table_names is None else set(table_names)
      excluded_tables = (set() if exclude_table_names is None
                         else set(exclude_table_names))
      tables_to_compute = (all_tables.intersection(selected_tables)
                           - excluded_tables)
      db_table_map[db] = tables_to_compute
      total_tables += len(tables_to_compute)

    for db, tables in db_table_map.items():
      for table in tables:
        # Submit command to threadpool
        futures.append(
          pool.apply_async(compute_stats_table, (client_factory, db, table,)))

    # Wait for all stats commands to finish
    completed = 0
    for f in futures:
      try:
        f.get()
        completed += 1
      except Exception as e:
        if not continue_on_error:
          log_completion(completed, total_tables, e)
          raise e
    log_completion(completed, total_tables)


if __name__ == "__main__":
  parser = ArgumentParser()
  group_continuation_opt = parser.add_mutually_exclusive_group()
  group_continuation_opt.add_argument(
    "--continue_on_error", dest="continue_on_error", action="store_true",
    help="If True, continue if there is an error executing the compute stats statement.")
  group_continuation_opt.add_argument(
    "--stop_on_error", dest="continue_on_error", action="store_false",
    help="If True, stop if there is an error executing the compute stats statement.")
  parser.add_argument(
    "--impalad", dest="impalad", default="localhost:21050",
    help="Impala daemon <host:hs2_port> to connect to.")
  parser.add_argument(
    "--use_kerberos", action="store_true", default=False,
    help="Compute stats on a kerberized cluster.")
  parser.add_argument(
    "--use_ssl", action="store_true", default=False,
    help="Compute stats on a cluster with SSL enabled.")
  parser.add_argument(
    "--parallelism", type=int, default=DEFAULT_PARALLELISM,
    help="Number of parallel compute stats commands.")
  parser.add_argument(
    "--db_names", dest="db_names", default=None, help=(
      "Comma-separated list of database names for which to compute stats. "
      "Can be used in conjunction with the --table_names or --exclude_table_names flag. "
      "If not specified, compute stats will run on tables from all databases."))
  group_selection_opt = parser.add_mutually_exclusive_group()
  group_selection_opt.add_argument(
    "--table_names", dest="table_names", default=None, help=(
      "Comma-separated list of table names to compute stats over. "
      "A substring comparison is done. If no tables are specified stats are computed "
      "across all tables. Can not be used in conjunction with --exclude_table_names."))
  group_selection_opt.add_argument(
    "--exclude_table_names", dest="exclude_table_names", default=None, help=(
      "Comma-separated list of table names to exclude compute stats. "
      "A substring comparison is done. If no tables are specified stats are computed "
      "across all tables. Can not be used in conjunction with --table_names."))
  args = parser.parse_args()

  table_names = None
  if args.table_names is not None:
    table_names = [name.lower().strip() for name in args.table_names.split(',')]

  exclude_table_names = None
  if args.exclude_table_names is not None:
    exclude_table_names = [name.lower().strip()
                           for name in args.exclude_table_names.split(',')]

  db_names = None
  if args.db_names is not None:
    db_names = [name.lower().strip() for name in args.db_names.split(',')]

  @contextmanager
  def client_factory():
    impala_client = create_connection(args.impalad,
        use_kerberos=args.use_kerberos, use_ssl=args.use_ssl, protocol='hs2',
        collect_profile_and_log=False)
    impala_client.connect()
    yield impala_client
    impala_client.close()

  compute_stats(client_factory, db_names=db_names, table_names=table_names,
                exclude_table_names=exclude_table_names,
                continue_on_error=args.continue_on_error, parallelism=args.parallelism)
