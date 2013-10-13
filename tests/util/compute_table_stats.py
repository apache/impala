#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Utility for computing table statistics of tables in the Hive Metastore
import sys
from hive_metastore import ThriftHiveMetastore
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from optparse import OptionParser
from subprocess import call
from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol

COMPUTE_STATS_STATEMENT =\
    'ANALYZE TABLE %(db_name)s.%(table_name)s %(partitions)s COMPUTE STATISTICS'

# Compute column stats using fully qualified table names doesn't work (HIVE-4118).
# Column stats is also broken for empty tables and tables that contain data with errors,
# so computing column stats across all functional tables will not work. For more
# information see: HIVE-4119 and HIVE-4122.
COMPUTE_COLUMN_STATS_STATEMENT =\
    'USE %(db_name)s; ANALYZE TABLE %(table_name)s COMPUTE STATISTICS FOR COLUMNS %(col)s'

HIVE_ARGS = "-hiveconf hive.root.logger=ERROR,console"

def compute_stats(hive_metastore_host, hive_metastore_port, db_names=None,
                  table_names=None, continue_on_error=False, hive_cmd='hive'):
  """
  Queries the Hive Metastore and runs compute stats over all tables

  Optionally, a list of table names can be passed and compute stats will only
  run matching table names
  """
  # Create a Hive Metastore Client
  # TODO: Consider creating a utility module for this because it is also duplicated in
  # impala_test_suite.py.
  hive_transport = TTransport.TBufferedTransport(
      TSocket.TSocket(hive_metastore_host, int(hive_metastore_port)))
  protocol = TBinaryProtocol.TBinaryProtocol(hive_transport)
  hive_metastore_client = ThriftHiveMetastore.Client(protocol)
  hive_client = ThriftHive.Client(protocol)
  hive_transport.open()

  print "Enumerating databases and tables for compute stats."

  all_dbs = set(name.lower() for name in hive_metastore_client.get_all_databases())
  selected_dbs = all_dbs if db_names is None else set(db_names)
  if db_names is not None:
    print 'Skipping compute stats on databases:\n%s' % '\n'.join(all_dbs - selected_dbs)

  for db in all_dbs.intersection(selected_dbs):
    all_tables = set(name.lower() for name in hive_metastore_client.get_all_tables(db))
    selected_tables = all_tables if table_names is None else set(table_names)
    if table_names:
      print 'Skipping compute stats on tables:\n%s' %\
          '\n'.join(['%s.%s' % (db, tbl)  for tbl in all_tables - selected_tables])

    for table in all_tables.intersection(selected_tables):
      statement = __build_compute_stat_statement(hive_metastore_client, db, table)
      print 'Executing: %s' % statement
      # For some still unknown reason, computing table stats using the hive meta store
      # client no longer works (it did work at one point in Hive v0.9). No errors are
      # thrown, but tables are set to have 0 rows. It seems to work fine when run via
      # the Hive CLI, so just use that for now.
      exit_code =\
          call([hive_cmd + " " + HIVE_ARGS + " -e \"" + statement + ";\""], shell=True)
      if exit_code != 0 and not continue_on_error:
        sys.exit(exit_code)
  hive_transport.close()

def __build_compute_stat_statement(metastore_client, db_name, table_name):
  """Builds the HQL statements to compute table and column stats for the given table"""
  partitions = metastore_client.get_partition_names(db_name, table_name, 1)
  partition_str = str()
  if len(partitions) > 0:
    partition_names = [p.split('=')[0] for p in partitions[0].split('/')]
    partition_str = 'PARTITION(%s)' % ', '.join(partition_names)
  stmt = COMPUTE_STATS_STATEMENT %\
      {'db_name': db_name, 'table_name': table_name, 'partitions': partition_str}
  stmt += ';\n'
  col_names = [f.name for f in metastore_client.get_fields(db_name, table_name)]
  stmt += COMPUTE_COLUMN_STATS_STATEMENT %\
      {'db_name': db_name, 'table_name': table_name, 'col': ', '.join(col_names)}
  return stmt

if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("--continue_on_error", dest="continue_on_error",
                    action="store_true", default=False, help="If True, continue "\
                    "if there is an error computing the table or column stats.")
  parser.add_option("--hive_cmd", dest="hive_cmd", default='hive',
                    help="The command to use for executing hive.")
  parser.add_option("--hive_metastore", dest="hive_metastore", default='localhost:9083',
                    help="<host:port> of hive metastore server to connect to")
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

  compute_stats(*options.hive_metastore.split(':'), db_names=db_names,
      table_names=table_names, continue_on_error=options.continue_on_error,
      hive_cmd=options.hive_cmd)
