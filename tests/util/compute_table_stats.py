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
# Utility for computing table statistics of tables in the Hive Metastore

from __future__ import absolute_import, division, print_function
from contextlib import contextmanager
from optparse import OptionParser
import logging
import multiprocessing
import multiprocessing.pool

from tests.beeswax.impala_beeswax import ImpalaBeeswaxClient

def compute_stats_table(client_factory, db, table, continue_on_error):
  """
  Runs 'compute stats' on a given table. If continue_on_error is
  True, exceptions computing statistics are swallowed.
  """
  with client_factory() as impala_client:
    db_table = "%s.%s" % (db, table)
    statement = "compute stats %s" % (db_table,)
    logging.info('Executing: %s', statement)
    try:
      result = impala_client.execute(statement)
      logging.info(" %s -> %s", db_table, ' '.join(result.data).strip())
    except:
      logging.exception(' Failed on table %s', db_table)
      if not continue_on_error:
        raise

def compute_stats(client_factory, db_names=None, table_names=None,
    continue_on_error=False, parallelism=multiprocessing.cpu_count()):
  """
  Runs COMPUTE STATS over the selected tables. The target tables can be filtered by
  specifying a list of databases and/or table names. If no filters are specified this will
  run COMPUTE STATS on all tables in all databases.

  parallelism controls the size of the thread pool to which compute_stats
  is sent.
  """
  logging.info("Enumerating databases and tables for compute stats.")

  pool = multiprocessing.pool.ThreadPool(processes=parallelism)
  futures = []
  with client_factory() as impala_client:
    all_dbs = set(name.split('\t')[0].lower() for name
        in impala_client.execute("show databases").data)
    selected_dbs = all_dbs if db_names is None else set(db_names)
    for db in all_dbs.intersection(selected_dbs):
      all_tables =\
          set([t.lower() for t in impala_client.execute("show tables in %s" % db).data])
      selected_tables = all_tables if table_names is None else set(table_names)
      for table in all_tables.intersection(selected_tables):
        # Submit command to threadpool
        futures.append(pool.apply_async(compute_stats_table,
            (client_factory, db, table, continue_on_error,)))
    # Wait for all stats commands to finish
    for f in futures:
      f.get()

if __name__ == "__main__":
  logging.basicConfig(level=logging.INFO, format='%(asctime)s %(threadName)s: %(message)s')
  parser = OptionParser()
  parser.add_option("--continue_on_error", dest="continue_on_error",
                    action="store_true", default=True, help="If True, continue "\
                    "if there is an error executing the compute stats statement.")
  parser.add_option("--stop_on_error", dest="continue_on_error",
                    action="store_false", default=True, help="If True, stop "\
                    "if there is an error executing the compute stats statement.")
  parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                    help="Impala daemon <host:port> to connect to.")
  parser.add_option("--use_kerberos", action="store_true", default=False,
                    help="Compute stats on a kerberized cluster.")
  parser.add_option("--use_ssl", action="store_true", default=False,
                    help="Compute stats on a cluster with SSL enabled.")
  parser.add_option("--parallelism", type=int, default=multiprocessing.cpu_count(),
                    help="Number of parallel compute stats commands.")
  parser.add_option("--db_names", dest="db_names", default=None,
                    help="Comma-separated list of database names for which to compute "\
                    "stats. Can be used in conjunction with the --table_names flag. "\
                    "If not specified, compute stats will run on tables from all "\
                    "databases.")
  parser.add_option("--table_names", dest="table_names", default=None,
                    help="Comma-separated list of table names to compute stats over. A"\
                    " substring comparison is done. If no tables are specified stats "\
                    "are computed across all tables.")
  options, args = parser.parse_args()
  table_names = None
  if options.table_names is not None:
    table_names = [name.lower().strip() for name in options.table_names.split(',')]

  db_names = None
  if options.db_names is not None:
    db_names = [name.lower().strip() for name in options.db_names.split(',')]

  @contextmanager
  def client_factory():
    impala_client = ImpalaBeeswaxClient(options.impalad,
        use_kerberos=options.use_kerberos, use_ssl=options.use_ssl)
    impala_client.connect()
    yield impala_client
    impala_client.close_connection()

  compute_stats(client_factory, db_names=db_names, table_names=table_names,
      continue_on_error=options.continue_on_error, parallelism=options.parallelism)
