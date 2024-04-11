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

# Script to create TPC-[H|DS] external JDBC tables.
#
# External JDBC tables are created in the specified 'jdbc_db_name' database.

from __future__ import absolute_import, division, print_function
import logging
import os
import sqlparse

LOG = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])

jdbc_db_name = None
verbose = False
workload = None
database_type = None
jdbc_url = None
jdbc_driver = None
jdbc_auth = None
jdbc_properties = None
dbcp_username = None
dbcp_password = None


def drop_tables():
  """Drop the specified 'jdbc_db_name' database and all its tables"""
  with cluster.impala.cursor() as impala:
    # DROP DATABASE CASCADE
    impala.execute("drop database if exists {0} cascade".format(jdbc_db_name))


def create_tables():
  """Create the 'jdbc_db_name' database and tables for the given workload"""
  # SQL parameters for JDBC schema template.
  sql_params = {
      "jdbc_db_name": jdbc_db_name,
      "database_type": database_type,
      "jdbc_url": jdbc_url,
      "jdbc_driver": jdbc_driver,
      "driver_url": driver_url,
      "jdbc_auth": jdbc_auth,
      "jdbc_properties": jdbc_properties,
      "dbcp_username": dbcp_username,
      "dbcp_password": dbcp_password}

  sql_file_path = get_test_file_path(workload)
  with open(sql_file_path, "r") as test:
    queries = sqlparse.split(test.read())

  with cluster.impala.cursor() as impala:
    impala.create_db_if_not_exists(jdbc_db_name)
    impala.execute("USE %s" % jdbc_db_name)
    for query in queries:
      query = sqlparse.format(query.rstrip(';'), strip_comments=True)
      query_str = query.format(**sql_params)
      if (len(query_str)) == 0: continue
      if verbose: print(query_str)
      impala.execute(query_str)


def get_test_file_path(workload):
  """Get filename of schema template file:
  tpch_jdbc_schema_template.sql or tpcds_jdbc_schema_template.sql """
  if "IMPALA_HOME" not in os.environ:
    raise Exception("IMPALA_HOME must be set")
  sql_file_path = os.path.join(os.environ["IMPALA_HOME"], "testdata", "datasets",
      workload, "%s_jdbc_schema_template.sql" % (workload))
  return sql_file_path


if __name__ == "__main__":
  from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
  import tests.comparison.cli_options as cli_options

  parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_cluster_options(parser)
  parser.add_argument("-t", "--jdbc_db_name", required=True,
      help="Target JDBC database name.")
  parser.add_argument("-w", "--workload", choices=['tpch', 'tpcds'],
      required=True)
  parser.add_argument("--database_type", required=True,
      help="Database type")
  parser.add_argument("--database_host", required=False,
      help="Hostname or IP address of RDBMS server")
  parser.add_argument("--database_port", required=False,
      help="TCP port of RDBMS server")
  parser.add_argument("--database_name", required=False,
      help="The name of database on RDBMS server")
  parser.add_argument("--jdbc_auth", required=False,
      help="The authentication method for RDBMS server")
  parser.add_argument("--jdbc_properties", required=False,
      help="Additional JDBC properties as comma seperated name/value pairs")
  parser.add_argument("-v", "--verbose", action='store_true',
      help="Print the executed statements.")
  parser.add_argument("--clean", action='store_true',
      help="Drop all tables in the specified target JDBC database.")
  args = parser.parse_args()

  FILESYSTEM_PREFIX = os.getenv("FILESYSTEM_PREFIX")
  INTERNAL_LISTEN_HOST = os.getenv("INTERNAL_LISTEN_HOST")

  cli_options.configure_logging(args.log_level, debug_log_file=args.debug_log_file)
  cluster = cli_options.create_cluster(args)
  db_port = args.database_port
  jdbc_db_name = args.jdbc_db_name
  jdbc_auth = args.jdbc_auth
  jdbc_properties = args.jdbc_properties
  workload = args.workload
  database_type = args.database_type.upper()
  if args.database_host:
    rdbms_db_host = args.database_host
  elif INTERNAL_LISTEN_HOST:
    rdbms_db_host = INTERNAL_LISTEN_HOST
  else:
    rdbms_db_host = 'localhost'
  if args.database_name:
    rdbms_db_name = args.database_name
  else:
    rdbms_db_name = workload
  verbose = args.verbose

  if database_type == 'IMPALA':
    if not db_port:
      db_port = 21050
    jdbc_url = 'jdbc:impala://{0}:{1}/{2}'.format(rdbms_db_host, db_port, rdbms_db_name)
    jdbc_driver = 'com.cloudera.impala.jdbc.Driver'
    driver_url = '{0}/test-warehouse/data-sources/jdbc-drivers/{1}'.format(
        FILESYSTEM_PREFIX, 'ImpalaJDBC42.jar')
    if not jdbc_auth:
      jdbc_auth = 'AuthMech=0'
    if not jdbc_properties:
      jdbc_properties = ''
    dbcp_username = 'impala'
    dbcp_password = 'cloudera'
  elif database_type == 'POSTGRES':
    if not db_port:
      db_port = 5432
    jdbc_url = 'jdbc:postgresql://{0}:{1}/{2}'.format(
        rdbms_db_host, db_port, rdbms_db_name)
    jdbc_driver = 'org.postgresql.Driver'
    driver_url = '{0}/test-warehouse/data-sources/jdbc-drivers/{1}'.format(
        FILESYSTEM_PREFIX, 'postgresql-jdbc.jar')
    jdbc_auth = ''
    jdbc_properties = ''
    dbcp_username = 'hiveuser'
    dbcp_password = 'password'
  elif database_type == 'MYSQL':
    if not db_port:
      db_port = 3306
    jdbc_url = 'jdbc:mysql://{0}:{1}/{2}'.format(rdbms_db_host, db_port, rdbms_db_name)
    jdbc_driver = 'com.mysql.cj.jdbc.Driver"'
    driver_url = '{0}/test-warehouse/data-sources/jdbc-drivers/{1}'.format(
        FILESYSTEM_PREFIX, 'mysql-jdbc.jar')
    jdbc_auth = ''
    jdbc_properties = ''
    dbcp_username = 'hiveuser'
    dbcp_password = 'password'
  else:
    # TODO support other database servers.
    raise Exception('Unsupported database type: {0}'.format(database_type))

  if args.clean: drop_tables()
  create_tables()
