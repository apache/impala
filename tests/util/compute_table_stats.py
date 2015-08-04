#!/usr/bin/env impala-python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Utility for computing table statistics of tables in the Hive Metastore
import sys
from optparse import OptionParser
from tests.beeswax.impala_beeswax import *

def compute_stats(impala_client, db_names=None, table_names=None,
    continue_on_error=False):
  """
  Runs COMPUTE STATS over the selected tables. The target tables can be filtered by
  specifying a list of databases and/or table names. If no filters are specified this will
  run COMPUTE STATS on all tables in all databases.
  """
  print "Enumerating databases and tables for compute stats."

  all_dbs = set(name.lower() for name in impala_client.execute("show databases").data)
  selected_dbs = all_dbs if db_names is None else set(db_names)
  for db in all_dbs.intersection(selected_dbs):
    all_tables =\
        set([t.lower() for t in impala_client.execute("show tables in %s" % db).data])
    selected_tables = all_tables if table_names is None else set(table_names)
    for table in all_tables.intersection(selected_tables):
      statement = "compute stats %s.%s" % (db, table)
      print 'Executing: %s' % statement
      try:
        result = impala_client.execute(statement)
        print "  -> %s\n" % '\n'.join(result.data)
      except Exception, e:
        print "  -> Error: %s\n" % str(e)
        if not continue_on_error: raise e

if __name__ == "__main__":
  parser = OptionParser()
  parser.add_option("--continue_on_error", dest="continue_on_error",
                    action="store_true", default=True, help="If True, continue "\
                    "if there is an error executing the compute stats statement.")
  parser.add_option("--impalad", dest="impalad", default="localhost:21000",
                    help="Impala daemon <host:port> to connect to.")
  parser.add_option("--use_kerberos", action="store_true", default=False,
                    help="Compute stats on a kerberized cluster.")
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

  impala_client = ImpalaBeeswaxClient(options.impalad, use_kerberos=options.use_kerberos)
  impala_client.connect()
  try:
    compute_stats(impala_client, db_names=db_names,
        table_names=table_names, continue_on_error=options.continue_on_error)
  finally:
    impala_client.close_connection()
