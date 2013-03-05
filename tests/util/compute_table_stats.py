#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Utility for computing table statistics of tables in the Hive Metastore
from hive_metastore import ThriftHiveMetastore
from hive_service import ThriftHive
from hive_service.ttypes import HiveServerException
from optparse import OptionParser
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

def compute_stats(hive_server_host, hive_server_port, db_names=None, table_names=None):
  """
  Queries the Hive Metastore and runs compute stats over all tables

  Optionally, a list of table names can be passed and compute stats will only
  run matching table names
  """
  # Create a Hive Metastore Client
  # TODO: Consider creating a utility module for this because it is also duplicated in
  # impala_test_suite.py.
  hive_transport = TTransport.TBufferedTransport(
      TSocket.TSocket(hive_server_host, int(hive_server_port)))
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
    for table in hive_metastore_client.get_all_tables(db):
      if table_names is not None and not\
          any(name.lower() in table.lower() for name in table_names):
        print 'Skipping table: %s' % table
        continue

      statement = __build_compute_stat_statement(hive_metastore_client, db, table)
      try:
        print 'Executing: %s' % statement
        map(hive_client.execute, statement.split(';'))
      except HiveServerException as e:
        print 'Error executing statement:\n%s' % e
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
  parser.add_option("--hive_server", dest="hive_server", default='localhost:10000',
                    help="<host:port> of hive server to connect to")
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

  compute_stats(*options.hive_server.split(':'),
      db_names=db_names, table_names=table_names)
