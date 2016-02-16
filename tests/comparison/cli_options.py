# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

'''Helpers for parsing command line options'''

import logging
import os
import sys
from getpass import getuser
from tempfile import gettempdir

import db_connection
from cluster import CmCluster, MiniCluster
from db_types import TYPES

def add_logging_options(section, default_debug_log_file=None):
  if not default_debug_log_file:
    default_debug_log_file = os.path.join(
        gettempdir(), os.path.basename(sys.modules["__main__"].__file__) + ".log")
  section.add_argument('--log-level', default='INFO',
      help='The log level to use.', choices=('DEBUG', 'INFO', 'WARN', 'ERROR'))
  section.add_argument('--debug-log-file', default=default_debug_log_file,
      help='Path to debug log file.')


def configure_logging(log_level, debug_log_file=None, log_thread_id=False,
    log_process_id=False):
  root_logger = logging.getLogger()
  root_logger.setLevel(logging.DEBUG)

  console_logger = logging.StreamHandler(sys.stdout)
  console_logger.name = "console"
  console_logger.setLevel(getattr(logging, log_level))
  format = "%(asctime)s"
  if log_process_id:
    format += " %(process)d"
  if log_thread_id:
    format += " %(thread)d"
  format += " %(levelname)s:%(module)s[%(lineno)s]:%(message)s"
  console_logger.setFormatter(logging.Formatter(format, "%H:%M:%S"))
  root_logger.addHandler(console_logger)

  if debug_log_file:
    file_logger = logging.FileHandler(debug_log_file, mode="w")
    file_logger.name = "file"
    file_logger.setFormatter(logging.Formatter(format, "%H:%M:%S"))
    file_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_logger)

  def create_third_party_filter(level):
    def filter_record(record):
      name = record.name
      if name.startswith("impala.") or name.startswith("paramiko.") \
          or name.startswith("hdfs") or name.startswith("requests"):
        return record.levelno >= level
      return True
    log_filter = logging.Filter()
    log_filter.filter = filter_record
    return log_filter
  console_logger.addFilter(create_third_party_filter(logging.WARN))
  if debug_log_file:
    file_logger.addFilter(create_third_party_filter(logging.INFO))


def add_ssh_options(section):
  section.add_argument('--ssh-user', metavar='user name', default=getuser(),
      help='The user name to use for SSH connections to cluster nodes.')
  section.add_argument('--ssh-key-file', metavar='path to file',
      help='Specify an additional SSH key other than the defaults in ~/.ssh.')
  section.add_argument('--ssh-port', metavar='number', type=int, default=22,
      help='The port number to use when connecting through SSH.')


def add_db_name_option(section):
  section.add_argument('--db-name', default='randomness',
      help='The name of the database to use. Ex: functional.')


def add_cluster_options(section):
  add_minicluster_options(section)
  add_cm_options(section)
  add_ssh_options(section)
  section.add_argument("--hadoop-user-name", default=getuser(),
      help="The user name to use when interacting with hadoop.")

def add_minicluster_options(section):
  section.add_argument('--minicluster-num-impalads', default=3, type=int,
      metavar='num impalads', help='The number of impalads in the mini cluster.')

def add_cm_options(section):
  section.add_argument('--cm-host', metavar='host name',
      help='The host name of the CM server.')
  section.add_argument('--cm-port', default=7180, type=int, metavar='port number',
      help='The port of the CM server.')
  section.add_argument('--cm-user', default="admin", metavar='user name',
      help='The name of the CM user.')
  section.add_argument('--cm-password', default="admin", metavar='password',
      help='The password for the CM user.')
  section.add_argument('--cm-cluster-name', metavar='name',
      help='If CM manages multiple clusters, use this to specify which cluster to use.')


def create_cluster(args):
  if args.cm_host:
    cluster = CmCluster(args.cm_host, user=args.cm_user, password=args.cm_password,
        cluster_name=args.cm_cluster_name, ssh_user=args.ssh_user, ssh_port=args.ssh_port,
        ssh_key_file=args.ssh_key_file)
  else:
    cluster = MiniCluster(args.minicluster_num_impalads)
  cluster.hadoop_user_name = args.hadoop_user_name
  return cluster


def add_storage_format_options(section):
  storage_formats = ['avro', 'parquet', 'rcfile', 'sequencefile', 'textfile']
  section.add_argument('--storage-file-formats', default=','.join(storage_formats),
      help='A comma separated list of storage formats to use.')


def add_data_types_options(section):
  section.add_argument('--data-types',
      default=','.join(type_.__name__ for type_ in TYPES),
      help='A comma separated list of data types to use.')


def add_timeout_option(section):
  section.add_argument('--timeout', default=(3 * 60), type=int,
      help='Query timeout in seconds')


def add_connection_option_groups(parser):
  group = parser.add_argument_group('MySQL Options')
  group.add_argument('--use-mysql', action='store_true',
      help='Use MySQL')
  group.add_argument('--mysql-host', default='localhost',
      help='The name of the host running the MySQL database.')
  group.add_argument('--mysql-port', default=3306, type=int,
      help='The port of the host running the MySQL database.')
  group.add_argument('--mysql-user', default='root',
      help='The user name to use when connecting to the MySQL database.')
  group.add_argument('--mysql-password',
      help='The password to use when connecting to the MySQL database.')
  parser.add_argument_group(group)

  group = parser.add_argument_group('Oracle Options')
  group.add_argument('--use-oracle', action='store_true',
      help='Use Oracle')
  group.add_argument('--oracle-host', default='localhost',
      help='The name of the host running the Oracle database.')
  group.add_argument('--oracle-port', default=1521, type=int,
      help='The port of the host running the Oracle database.')
  group.add_argument('--oracle-user', default='system',
      help='The user name to use when connecting to the Oracle database.')
  group.add_argument('--oracle-password',
      help='The password to use when connecting to the Oracle database.')
  parser.add_argument_group(group)

  group = parser.add_argument_group('Postgresql Options')
  group.add_argument('--use-postgresql', action='store_true',
      help='Use Postgresql')
  group.add_argument('--postgresql-host', default='localhost',
      help='The name of the host running the Postgresql database.')
  group.add_argument('--postgresql-port', default=5432, type=int,
      help='The port of the host running the Postgresql database.')
  group.add_argument('--postgresql-user', default='postgres',
      help='The user name to use when connecting to the Postgresql database.')
  group.add_argument('--postgresql-password',
      help='The password to use when connecting to the Postgresql database.')
  parser.add_argument_group(group)


def get_db_type(args):
  db_types = list()
  if args.use_mysql:
    db_types.append(db_connection.MYSQL)
  if args.use_oracle:
    db_types.append(db_connection.ORACLE)
  if args.use_postgresql:
    db_types.append(db_connection.POSTGRESQL)
  if not db_types:
    raise Exception("At least one of --use-mysql, --use-oracle, or --use-postgresql"
        "must be used")
  elif len(db_types) > 1:
    raise Exception("Too many databases requested: %s" % db_types)
  return db_types[0]


def create_connection(args, db_type=None, db_name=None):
  if not db_type:
    db_type = get_db_type(args)
  if db_type == db_connection.POSTGRESQL:
    conn_class = db_connection.PostgresqlConnection
  elif db_type == db_connection.MYSQL:
    conn_class = db_connection.MySQLConnection
  elif db_type == db_connection.ORACLE:
    conn_class = db_connection.OracleConnection
  else:
    raise Exception('Unexpected db_type: %s; expected one of %s.'
        % (db_type, ', '.join([db_connection.POSTGRESQL, db_connection.MYSQL,
              db_connection.ORACLE])))
  prefix = db_type.lower()
  return conn_class(
      user_name=getattr(args, prefix + '_user'),
      password=getattr(args, prefix + '_password'),
      host_name=getattr(args, prefix + '_host'),
      port=getattr(args, prefix + '_port'),
      db_name=db_name)


def add_kerberos_options(section):
  section.add_argument("--use-kerberos", action="store_true",
      help="Use kerberos when communicating with Impala. This requires that kinit has"
      " already been done before running this script.")
  section.add_argument("--kerberos-principal", default=getuser(),
      help="The principal name to use.")
