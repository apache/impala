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

'''This module provides random data generation and database population.

   When this module is run directly for purposes of database population, the default is
   to use a fixed seed for randomization. The result should be that the generated random
   data is the same regardless of when or where the execution is done.

'''

from __future__ import absolute_import, division, print_function
from builtins import filter, range
import os
from copy import deepcopy
from logging import getLogger
from random import choice, randint, seed
from time import time

from tests.comparison.data_generator_mapred_common import (
    estimate_rows_per_reducer,
    MB_PER_REDUCER,
    serialize,
    TextTableDataGenerator)
from tests.comparison.common import Column, Table
from tests.comparison.db_types import (
    Char,
    Decimal,
    EXACT_TYPES,
    get_char_class,
    get_decimal_class,
    get_varchar_class,
    String,
    Timestamp,
    TYPES,
    VarChar)
from tests.comparison import db_connection

LOG = getLogger(__name__)

def index_tables_in_db_if_possible(cursor):
  if not cursor.conn.supports_index_creation:
    return
  for table_name in cursor.list_table_names():
    LOG.info('Indexing %s on %s' % (table_name, cursor.db_type))
    cursor.index_table(table_name)


def migrate_db(src_cursor, dst_cursor, include_table_names=None):
  '''Read table metadata and data from the source database and create a replica in
     the destination database. For example, the Impala functional test database could
     be copied into Postgresql.
  '''
  for table_name in src_cursor.list_table_names():
    if include_table_names and table_name not in include_table_names:
      continue
    table = src_cursor.describe_table(table_name)
    dst_cursor.create_table(table)
    src_cursor.execute('SELECT * FROM ' + table_name)
    while True:
      rows = src_cursor.fetchmany(size=100)
      if not rows:
        break
      sql = dst_cursor.make_insert_sql_from_data(table, rows)
      dst_cursor.execute(sql)
  index_tables_in_db_if_possible(dst_cursor)


class DbPopulator(object):
  '''This class will populate a database with randomly generated data. The population
     includes table creation and data generation. Table names are hard coded as
     table_<table number>.

  '''

  def __init__(self, db_engine=db_connection.IMPALA):
    self.cluster = None
    self.db_name = None
    self.db_engine = db_engine

    self.min_col_count = None
    self.max_col_count = None
    self.min_row_count = None
    self.max_row_count = None
    self.allowed_storage_formats = None
    self.randomization_seed = None

  def populate_db(self, table_count, postgresql_conn=None):
    '''Create tables with a random number of cols.

       The given db_name must have already been created.
    '''
    self.cluster.hdfs.ensure_home_dir()
    hdfs = self.cluster.hdfs.create_client()

    table_and_generators = list()
    for table_idx in range(table_count):
      table = self._create_random_table(
          'table_%s' % (table_idx + 1),
          self.min_col_count,
          self.max_col_count,
          self.allowed_storage_formats)
      self._prepare_table_storage(table, self.db_name)
      if table.storage_format == 'TEXTFILE':
        text_table = table
      else:
        text_table = deepcopy(table)
        text_table.name += '_text'
        text_table.storage_format = 'TEXTFILE'
        text_table.storage_location = None
        text_table.schema_location = None
        self._prepare_table_storage(text_table, self.db_name)
      table_data_generator = TextTableDataGenerator()
      table_data_generator.randomization_seed = self.randomization_seed
      table_data_generator.table = text_table
      table_data_generator.row_count = randint(self.min_row_count, self.max_row_count)
      table_and_generators.append((table, table_data_generator))

    self._run_data_generator_mr_job([g for _, g in table_and_generators], self.db_name)

    with self.cluster.hive.cursor(db_name=self.db_name) as cursor:
      for table, table_data_generator in table_and_generators:
        cursor.create_table(table)
        text_table = table_data_generator.table
        if postgresql_conn:
          with postgresql_conn.cursor() as postgresql_cursor:
            postgresql_cursor.create_table(table)
            for data_file in hdfs.list(text_table.storage_location):
              with hdfs.read(text_table.storage_location + '/' + data_file) as reader:
                postgresql_cursor.copy_expert(
                    r"COPY %s FROM STDIN WITH DELIMITER E'\x01'" % table.name, reader)
        if table.storage_format != 'TEXTFILE':
          cursor.create_table(text_table)
          cursor.execute('INSERT INTO %s SELECT * FROM %s'
              % (table.name, text_table.name))
          cursor.drop_table(text_table.name)
    if self.db_engine is db_connection.IMPALA:
      with self.cluster.impala.cursor(db_name=self.db_name) as cursor:
        cursor.invalidate_metadata()
        cursor.compute_stats()
    elif self.db_engine is db_connection.HIVE:
      with self.cluster.hive.cursor(db_name=self.db_name) as cursor:
        cursor.invalidate_metadata()
        cursor.compute_stats()
    else:
      raise ValueError("db_engine must be of type %s or %s", db_connection.IMPALA,
                       db_connection.HIVE)
    if postgresql_conn:
      with postgresql_conn.cursor() as postgresql_cursor:
        index_tables_in_db_if_possible(postgresql_cursor)

  def _create_random_table(self,
      table_name,
      min_col_count,
      max_col_count,
      allowed_storage_formats):
    '''Create and return a Table with a random number of cols.'''
    col_count = randint(min_col_count, max_col_count)
    storage_format = choice(allowed_storage_formats)
    table = Table(table_name)
    table.storage_format = storage_format
    allowed_types = list(TYPES)
    # Avro doesn't support timestamps yet.
    if table.storage_format == 'AVRO':
      allowed_types.remove(Timestamp)
    # TODO: 'table.cols' returns a copy of all scalar cols, so 'table.cols.append()'
    #       doesn't actually modify the table's columns. 'table.cols' should be changed
    #       to allow access to the real columns.
    cols = table.cols
    for col_idx in range(col_count):
      col_type = choice(allowed_types)
      col_type = \
        choice(list(filter(lambda type_: issubclass(type_, col_type), EXACT_TYPES)))
      if issubclass(col_type, VarChar) and not issubclass(col_type, String):
        col_type = get_varchar_class(randint(1, VarChar.MAX))
      elif issubclass(col_type, Char) and not issubclass(col_type, String):
        col_type = get_char_class(randint(1, Char.MAX))
      elif issubclass(col_type, Decimal):
        max_digits = randint(1, Decimal.MAX_DIGITS)
        col_type = get_decimal_class(max_digits, randint(1, max_digits))
      col = Column(
          table,
          '%s_col_%s' % (col_type.__name__.lower(), col_idx + 1),
          col_type)
      cols.append(col)
    table.cols = cols
    return table

  def _prepare_table_storage(self, table, db_name):
    with self.cluster.hive.cursor(db_name=self.db_name) as cursor:
      cursor.ensure_storage_location(table)
    hdfs = self.cluster.hdfs.create_client()
    if hdfs.exists(table.storage_location):
      hdfs.delete(table.storage_location, recursive=True)
    hdfs.makedirs(table.storage_location, permission='777')

  def _run_data_generator_mr_job(self, table_data_generators, db_name):
    timestamp = int(time())
    mapper_input_file = '/tmp/data_gen_%s_mr_input_%s' % (db_name, timestamp)
    hdfs = self.cluster.hdfs.create_client()
    if hdfs.exists(mapper_input_file):
      hdfs.delete(mapper_input_file)
    reducer_count = 0
    mapper_input_data = list()
    for table_data_generator in table_data_generators:
      reducer_count += (table_data_generator.row_count
          // estimate_rows_per_reducer(table_data_generator, MB_PER_REDUCER)) + 1
      mapper_input_data.append(serialize(table_data_generator))
    hdfs.write(mapper_input_file, data='\n'.join(mapper_input_data))

    files = ['common.py', 'db_types.py', 'data_generator_mapred_common.py',
        'data_generator_mapper.py', 'data_generator_reducer.py',
        'random_val_generator.py']
    dir_path = os.path.dirname(__file__)
    files = [os.path.join(dir_path, f) for f in files]

    hdfs_output_dir = '/tmp/data_gen_%s_mr_output_%s' % (db_name, timestamp)
    if hdfs.exists(hdfs_output_dir):
      hdfs.delete(hdfs_output_dir, recursive=True)

    LOG.info('Starting MR job to generate data for %s', db_name)
    self.cluster.yarn.run_mr_job(self.cluster.yarn.find_mr_streaming_jar(), job_args=r'''
        -D mapred.reduce.tasks=%s \
        -D stream.num.map.output.key.fields=2 \
        -libjars '%s/share/hadoop/hdfs/lib/*' \
        -files %s \
        -input %s \
        -output %s \
        -mapper data_generator_mapper.py \
        -reducer data_generator_reducer.py'''.strip()
        % (reducer_count, os.environ["HADOOP_HOME"], ','.join(files), mapper_input_file,
           hdfs_output_dir))


if __name__ == '__main__':
  from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser

  from tests.comparison import cli_options

  parser = ArgumentParser(
      usage='usage: \n'
          '  %(prog)s [options] [populate]\n\n'
          '     Create and populate database(s). The Impala database will always be \n'
          '     included. Postgres is optional. The other databases are not supported.\n\n'
          '  %(prog)s [options] migrate\n\n'
          '     Migrate an Impala database to another database type. The destination \n'
          '     database will be dropped and recreated.',
      formatter_class=ArgumentDefaultsHelpFormatter)
  cli_options.add_logging_options(parser)
  cli_options.add_cluster_options(parser)
  cli_options.add_db_name_option(parser)
  cli_options.add_connection_option_groups(parser)

  group = parser.add_argument_group('Database Population Options')
  group.add_argument('--randomization-seed', default=1, type=int,
      help='The randomization will be initialized with this seed. Using the same seed '
          'will produce the same results across runs.')
  cli_options.add_storage_format_options(group)
  group.add_argument('--create-data-files', default=False, action='store_true',
      help='Create files that can be used to repopulate the databases elsewhere.')
  group.add_argument('--table-count', default=10, type=int,
      help='The number of tables to generate.')
  group.add_argument('--min-column-count', default=1, type=int,
      help='The minimum number of columns to generate per table.')
  group.add_argument('--max-column-count', default=100, type=int,
      help='The maximum number of columns to generate per table.')
  group.add_argument('--min-row-count', default=(10 ** 3), type=int,
      help='The minimum number of rows to generate per table.')
  group.add_argument('--max-row-count', default=(10 ** 6), type=int,
      help='The maximum number of rows to generate per table.')
  parser.add_argument_group(group)

  group = parser.add_argument_group('Database Migration Options')
  group.add_argument('--migrate-table-names',
      help='Table names should be separated with commas. The default is to migrate all '
          'tables.')
  parser.add_argument_group(group)
  parser.add_argument('command', nargs='*', help='The command to run either "populate"'
      ' or "migrate".')
  args = parser.parse_args()
  if len(args.command) > 1:
    raise Exception('Only one command can be chosen. Requested commands were: %s'
        % args.command)
  command = args.command[0] if args.command else 'populate'
  if command not in ('populate', 'migrate'):
    raise Exception('Command must either be "populate" or "migrate" but was "%s"'
        % command)
  if command == 'migrate' and \
      not any((args.use_mysql, args.use_postgresql, args.use_oracle)):
    raise Exception('At least one destination database must be chosen with '
          '--use-<database type>')

  cli_options.configure_logging(args.log_level, debug_log_file=args.debug_log_file)

  seed(args.randomization_seed)

  cluster = cli_options.create_cluster(args)

  populator = DbPopulator(db_connection.HIVE if args.use_hive else db_connection.IMPALA)
  if command == 'populate':
    populator.randomization_seed = args.randomization_seed
    populator.cluster = cluster
    populator.db_name = args.db_name
    populator.min_col_count = args.min_column_count
    populator.max_col_count = args.max_column_count
    populator.min_row_count = args.min_row_count
    populator.max_row_count = args.max_row_count
    populator.allowed_storage_formats = args.storage_file_formats.split(',')

    if args.use_hive:
      with cluster.hive.connect() as conn:
        with conn.cursor() as cursor:
          cursor.ensure_empty_db(args.db_name)
    else:
      with cluster.impala.connect() as conn:
        with conn.cursor() as cursor:
          cursor.invalidate_metadata()
          cursor.ensure_empty_db(args.db_name)

    if args.use_postgresql:
      with cli_options.create_connection(args) as postgresql_conn:
        with postgresql_conn.cursor() as cursor:
          cursor.ensure_empty_db(args.db_name)
      postgresql_conn = cli_options.create_connection(args, db_name=args.db_name)
    else:
      postgresql_conn = None
    populator.populate_db(args.table_count, postgresql_conn=postgresql_conn)
  else:
    if args.migrate_table_names:
      table_names = args.migrate_table_names.split(',')
    else:
      table_names = None
    with cli_options.create_connection(args) as conn:
      with conn.cursor() as cursor:
        cursor.ensure_empty_db(args.db_name)
    with cli_options.create_connection(args, db_name=args.db_name) as conn:
      with conn.cursor() as dst:
        with cluster.impala.cursor(db_name=args.db_name) as src:
          migrate_db(src, dst, include_table_names=table_names)
