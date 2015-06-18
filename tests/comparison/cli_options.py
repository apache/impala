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
from optparse import NO_DEFAULT, OptionGroup
from tempfile import gettempdir

from tests.comparison.types import TYPES

def add_logging_options(section, default_debug_log_file=None):
  if not default_debug_log_file:
    default_debug_log_file = os.path.join(
        gettempdir(), os.path.basename(sys.modules["__main__"].__file__) + ".log")
  section.add_option('--log-level', default='INFO',
      help='The log level to use.', choices=('DEBUG', 'INFO', 'WARN', 'ERROR'))
  section.add_option('--debug-log-file', default=default_debug_log_file,
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


def add_cm_options(section):
  section.add_option('--cm-host', metavar='host name',
      help='The host name of the CM server.')
  section.add_option('--cm-port', default=7180, type=int, metavar='port number',
      help='The port of the CM server.')
  section.add_option('--cm-user', default="admin", metavar='user name',
      help='The name of the CM user.')
  section.add_option('--cm-password', default="admin", metavar='password',
      help='The password for the CM user.')
  section.add_option('--cm-cluster-name', metavar='name',
      help='If CM manages multiple clusters, use this to specify which cluster to use.')


def add_db_name_option(section):
  section.add_option('--db-name', default='randomness',
      help='The name of the database to use. Ex: functional.')


def add_storage_format_options(section):
  storage_formats = ['avro', 'parquet', 'rcfile', 'sequencefile', 'textfile']
  section.add_option('--storage-file-formats', default=','.join(storage_formats),
      help='A comma separated list of storage formats to use.')


def add_data_types_options(section):
  section.add_option('--data-types', default=','.join(type_.__name__ for type_ in TYPES),
      help='A comma separated list of data types to use.')


def add_connection_option_groups(parser):
  group = OptionGroup(parser, "Impala Options")
  group.add_option('--impalad-host', default='localhost',
      help="The name of the host running the Impala daemon")
  group.add_option("--impalad-hs2-port", default=21050, type=int,
      help="The hs2 port of the host running the Impala daemon")
  parser.add_option_group(group)

  group = OptionGroup(parser, "Hive Options")
  group.add_option('--use-hive', action='store_true', default=False,
      help='Use Hive (Impala will be skipped)')
  group.add_option('--hive-host', default='localhost',
      help="The name of the host running the HS2")
  group.add_option("--hive-port", default=10000, type=int,
      help="The port of HiveServer2")
  group.add_option('--hive-user', default='hive',
      help="The user name to use when connecting to HiveServer2")
  group.add_option('--hive-password', default='hive',
      help="The password to use when connecting to HiveServer2")
  group.add_option('--hdfs-host',
      help='The host for HDFS backing Hive tables, necessary for external HiveServer2')
  group.add_option('--hdfs-port',
      help='The port for HDFS backing Hive tables, necessary for external HiveServer2')
  parser.add_option_group(group)

  group = OptionGroup(parser, 'MySQL Options')
  group.add_option('--use-mysql', action='store_true', default=False,
      help='Use MySQL')
  group.add_option('--mysql-host', default='localhost',
      help='The name of the host running the MySQL database.')
  group.add_option('--mysql-port', default=3306, type=int,
      help='The port of the host running the MySQL database.')
  group.add_option('--mysql-user', default='root',
      help='The user name to use when connecting to the MySQL database.')
  group.add_option('--mysql-password',
      help='The password to use when connecting to the MySQL database.')
  parser.add_option_group(group)

  group = OptionGroup(parser, 'Oracle Options')
  group.add_option('--use-oracle', action='store_true', default=False,
      help='Use Oracle')
  group.add_option('--oracle-host', default='localhost',
      help='The name of the host running the Oracle database.')
  group.add_option('--oracle-port', default=1521, type=int,
      help='The port of the host running the Oracle database.')
  group.add_option('--oracle-user', default='system',
      help='The user name to use when connecting to the Oracle database.')
  group.add_option('--oracle-password',
      help='The password to use when connecting to the Oracle database.')
  parser.add_option_group(group)

  group = OptionGroup(parser, 'Postgresql Options')
  group.add_option('--use-postgresql', action='store_true', default=False,
      help='Use Postgresql')
  group.add_option('--postgresql-host', default='localhost',
      help='The name of the host running the Postgresql database.')
  group.add_option('--postgresql-port', default=5432, type=int,
      help='The port of the host running the Postgresql database.')
  group.add_option('--postgresql-user', default='postgres',
      help='The user name to use when connecting to the Postgresql database.')
  group.add_option('--postgresql-password',
      help='The password to use when connecting to the Postgresql database.')
  parser.add_option_group(group)


def add_default_values_to_help(parser):
  for group in parser.option_groups + [parser]:
    for option in group.option_list:
      if option.default != NO_DEFAULT and option.help:
        option.help += ' [default: %default]'
