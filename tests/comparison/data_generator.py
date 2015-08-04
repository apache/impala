#!/usr/bin/env impala-python

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

'''This module provides random data generation and database population.

   When this module is run directly for purposes of database population, the default is
   to use a fixed seed for randomization. The result should be that the generated random
   data is the same regardless of when or where the execution is done.

'''

from datetime import datetime, timedelta
from decimal import Decimal as PyDecimal
from logging import basicConfig, getLogger
from random import choice, randint, random, seed, uniform

from tests.comparison.db_connector import (
    DbConnector,
    HIVE,
    IMPALA,
    MYSQL,
    ORACLE,
    POSTGRESQL,
    HIVE_FOR_IMPALA)
from tests.comparison.common import Column, Table
from tests.comparison.types import (
    Boolean,
    Char,
    Decimal,
    EXACT_TYPES,
    Float,
    get_char_class,
    get_decimal_class,
    get_varchar_class,
    Int,
    String,
    Timestamp,
    TYPES,
    VarChar)

LOG = getLogger(__name__)

class RandomValGenerator(object):
  '''This class will generate random data of various data types.'''

  def __init__(self,
      min_number=-1000,
      max_number=1000,
      min_date=datetime(1990, 1, 1),
      max_date=datetime(2030, 1, 1),
      null_val_percentage=0.1,
      max_decimal_fractional_digits=2):
    if type(min_number) != int or type(max_number) != int:
      raise Exception("min_number and max_number must be integers but were %s and %s"
          % (type(min_number), type(max_number)))
    self.min_number = min_number
    self.max_number = max_number
    self.min_date = min_date
    self.max_date = max_date
    self.null_val_percentage = null_val_percentage
    self.max_decimal_fractional_digits = max_decimal_fractional_digits

  def generate_val(self, val_type):
    '''Generate and return a single random val. Use the val_type parameter to
       specify the type of val to generate. See types.py for valid val_type
       options.

       Ex:
         generator = RandomValGenerator(min_number=1, max_number=5)
         val = generator.generate_val(model.Int)
         assert 1 <= val and val <= 5
    '''
    if issubclass(val_type, Char):
      val = self.generate_val(Int)
      return None if val is None else str(val)[:val_type.MAX]
    if random() < self.null_val_percentage:
      return None
    if issubclass(val_type, Int):
      return randint(
          max(self.min_number, val_type.MIN), min(val_type.MAX, self.max_number))
    if issubclass(val_type, Decimal):
      # Create an int within the maximum length of the Decimal, then shift the decimal
      # point as needed.
      if val_type.MAX_FRACTIONAL_DIGITS > self.max_decimal_fractional_digits:
        max_digits = val_type.MAX_DIGITS \
            - (val_type.MAX_FRACTIONAL_DIGITS - self.max_decimal_fractional_digits)
        fractal_digits = self.max_decimal_fractional_digits
      else:
        max_digits = val_type.MAX_DIGITS
        fractal_digits = val_type.MAX_FRACTIONAL_DIGITS
      max_type_val = 10 ** max_digits
      decimal_point_shift = 10 ** fractal_digits
      max_val = min(self.max_number * decimal_point_shift, max_type_val)
      min_val = max(self.min_number * decimal_point_shift, -1 * max_type_val)
      return PyDecimal(randint(min_val + 1, max_val - 1)) / decimal_point_shift
    if issubclass(val_type, Float):
      return uniform(self.min_number, self.max_number)
    if issubclass(val_type, Timestamp):
      delta = self.max_date - self.min_date
      delta_in_seconds = delta.days * 24 * 60 * 60 + delta.seconds
      offset_in_seconds = randint(0, delta_in_seconds)
      val = self.min_date + timedelta(0, offset_in_seconds)
      return datetime(val.year, val.month, val.day)
    if issubclass(val_type, Boolean):
      return randint(0, 1) == 1
    raise Exception('Unsupported type %s' % val_type.__name__)


class DatabasePopulator(object):
  '''This class will populate a database with randomly generated data. The population
     includes table creation and data generation. Table names are hard coded as
     table_<table number>.

  '''

  def __init__(self):
    self.val_generator = RandomValGenerator()

  def populate_db_with_random_data(self,
      db_name,
      db_connectors,
      min_number_of_tables,
      max_number_of_tables,
      min_number_of_cols,
      max_number_of_cols,
      min_number_of_rows,
      max_number_of_rows,
      allowed_storage_formats,
      create_files):
    '''Create tables with a random number of cols.

       The given db_name must have already been created.
    '''
    connections = list()
    hive_connection = None
    for connector in db_connectors:
      connection = connector.create_connection(db_name=db_name)
      connections.append(connection)
      if connector.db_type == IMPALA:
        # The Impala table creator needs help from Hive for some storage formats.
        # Eventually Impala should be able to write in all formats and this can be
        # removed.
        hive_connection = DbConnector(HIVE_FOR_IMPALA).create_connection(db_name=db_name)
        connection.hive_connection = hive_connection
    for table_idx in xrange(randint(min_number_of_tables, max_number_of_tables)):
      table = self.create_random_table(
          'table_%s' % (table_idx + 1),
          min_number_of_cols,
          max_number_of_cols,
          allowed_storage_formats)

      for connection in connections:
        connection.bulk_load_data_file = open(
            "/tmp/%s_%s.data" % (table.name, connection.db_type.lower()), "w")
        connection.begin_bulk_load_table(table)

      row_count = randint(min_number_of_rows, max_number_of_rows)
      LOG.info('Inserting %s rows into %s', row_count, table.name)
      while row_count:
        batch_size = min(1000, row_count)
        rows = self.generate_table_data(table, number_of_rows=batch_size)
        row_count -= batch_size
        for connection in connections:
          connection.handle_bulk_load_table_data(rows)

      for connection in connections:
        connection.end_bulk_load_table()

    self.index_tables_in_database(connections)

    for connection in connections:
      connection.close()
    if hive_connection:
      hive_connection.close()

  def migrate_database(self,
      db_name,
      source_db_connector,
      destination_db_connectors,
      include_table_names=None):
    '''Read table metadata and data from the source database and create a replica in
       the destination databases. For example, the Impala functional test database could
       be copied into Postgresql.

       source_db_connector and items in destination_db_connectors should be
       of type db_connector.DbConnector. destination_db_connectors and
       include_table_names should be iterables.
    '''
    source_connection = source_db_connector.create_connection(db_name)

    cursors = [connector.create_connection(db_name=db_name).create_cursor()
               for connector in destination_db_connectors]

    for table_name in source_connection.list_table_names():
      if include_table_names and table_name not in include_table_names:
        continue
      try:
        table = source_connection.describe_table(table_name)
      except Exception as e:
        LOG.warn('Error fetching metadata for %s: %s', table_name, e)
        continue
      for destination_cursor in cursors:
        sql = destination_cursor.connection.make_create_table_sql(table)
        destination_cursor.execute(sql)
      with source_connection.open_cursor() as source_cursor:
        try:
          source_cursor.execute('SELECT * FROM ' + table_name)
          while True:
            rows = source_cursor.fetchmany(size=100)
            if not rows:
              break
            for destination_cursor in cursors:
              sql = destination_cursor.connection.make_insert_sql_from_data(table, rows)
              destination_cursor.execute(sql)
        except Exception as e:
          LOG.error('Error fetching data for %s: %s', table_name, e)
          continue

    self.index_tables_in_database([cursor.connection for cursor in cursors])

    for cursor in cursors:
      cursor.close()
      cursor.connection.close()

  def create_random_table(self,
      table_name,
      min_number_of_cols,
      max_number_of_cols,
      allowed_storage_formats):
    '''Create and return a Table with a random number of cols.'''
    col_count = randint(min_number_of_cols, max_number_of_cols)
    storage_format = choice(allowed_storage_formats)
    table = Table(table_name)
    table.storage_format = storage_format
    for col_idx in xrange(col_count):
      col_type = choice(TYPES)
      col_type = choice(filter(lambda type_: issubclass(type_, col_type), EXACT_TYPES))
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
      table.cols.append(col)
    return table

  def generate_table_data(self, table, number_of_rows=100):
      rows = list()
      for row_idx in xrange(number_of_rows):
        row = list()
        for col in table.cols:
          row.append(self.val_generator.generate_val(col.exact_type))
        rows.append(row)
      return rows

  def drop_and_create_database(self, db_name, db_connectors):
    for connector in db_connectors:
      with connector.open_connection() as connection:
        connection.drop_db_if_exists(db_name)
        connection.create_database(db_name)

  def index_tables_in_database(self, connections):
    for connection in connections:
      if connection.supports_index_creation:
        for table_name in connection.list_table_names():
          LOG.info('Indexing %s on %s' % (table_name, connection.db_type))
          connection.index_table(table_name)


if __name__ == '__main__':
  import logging
  from optparse import NO_DEFAULT, OptionGroup, OptionParser

  import tests.comparison.cli_options as cli_options

  parser = OptionParser(
      usage='usage: \n'
            '  %prog [options] [populate]\n\n'
            '     Create and populate database(s). The Impala database will always be \n'
            '     included, the other database types are optional.\n\n'
            '  %prog [options] migrate\n\n'
            '     Migrate an Impala database to another database type. The destination \n'
            '     database will be dropped and recreated.')
  cli_options.add_logging_options(parser)
  cli_options.add_db_name_option(parser)
  cli_options.add_connection_option_groups(parser)

  group = OptionGroup(parser, 'Database Population Options')
  group.add_option('--randomization-seed', default=1, type='int',
      help='The randomization will be initialized with this seed. Using the same seed '
          'will produce the same results across runs.')
  storage_formats = ['avro', 'parquet', 'rcfile', 'sequencefile', 'textfile']
  group.add_option('--storage-file-formats', default=','.join(storage_formats),
      help='A comma separated list of storage formats to choose from.')
  group.add_option('--create-data-files', default=False, action='store_true',
      help='Create files that can be used to repopulate the databases elsewhere.')
  group.add_option('--min-table-count', default=10, type='int',
      help='The minimum number of tables to generate.')
  group.add_option('--max-table-count', default=100, type='int',
      help='The maximum number of tables to generate.')
  group.add_option('--min-column-count', default=10, type='int',
      help='The minimum number of columns to generate per table.')
  group.add_option('--max-column-count', default=100, type='int',
      help='The maximum number of columns to generate per table.')
  group.add_option('--min-row-count', default=(10 ** 3), type='int',
      help='The minimum number of rows to generate per table.')
  group.add_option('--max-row-count', default=(10 ** 6), type='int',
      help='The maximum number of rows to generate per table.')
  parser.add_option_group(group)

  group = OptionGroup(parser, 'Database Migration Options')
  group.add_option('--migrate-table-names',
      help='Table names should be separated with commas. The default is to migrate all '
          'tables.')
  parser.add_option_group(group)

  for group in parser.option_groups + [parser]:
    for option in group.option_list:
      if option.default != NO_DEFAULT:
        option.help += ' [default: %default]'

  options, args = parser.parse_args()
  command = args[0] if args else 'populate'
  if len(args) > 1 or command not in ['populate', 'migrate']:
    raise Exception('Command must either be "populate" or "migrate" but was "%s"' %
        ' '.join(args))
  if command == 'migrate' and \
      not any((options.use_mysql, options.use_postgresql, options.use_oracle)):
    raise Exception('At least one destination database must be chosen with '
          '--use-<database type>')

  basicConfig(level=getattr(logging, options.log_level))

  seed(options.randomization_seed)

  impala_connector = DbConnector(IMPALA)
  db_connectors = []
  if options.use_postgresql:
    db_connectors.append(DbConnector(POSTGRESQL,
      user_name=options.postgresql_user,
      password=options.postgresql_password,
      host_name=options.postgresql_host,
      port=options.postgresql_port))
  if options.use_oracle:
    db_connectors.append(DbConnector(ORACLE,
      user_name=options.oracle_user,
      password=options.oracle_password,
      host_name=options.oracle_host,
      port=options.oracle_port))
  if options.use_mysql:
    db_connectors.append(DbConnector(MYSQL,
      user_name=options.mysql_user,
      password=options.mysql_password,
      host_name=options.mysql_host,
      port=options.mysql_port))
  if options.use_hive:
    db_connectors.append(DbConnector(HIVE,
      user_name=options.hive_user,
      password=options.hive_password,
      host_name=options.hive_host,
      port=options.hive_port,
      hdfs_host=options.hdfs_host,
      hdfs_port=options.hdfs_port))

  populator = DatabasePopulator()
  if command == 'populate':
    if not options.use_hive:
      db_connectors.append(impala_connector)
    populator.drop_and_create_database(options.db_name, db_connectors)
    populator.populate_db_with_random_data(
        options.db_name,
        db_connectors,
        options.min_table_count,
        options.max_table_count,
        options.min_column_count,
        options.max_column_count,
        options.min_row_count,
        options.max_row_count,
        options.storage_file_formats.split(','),
        options.create_data_files)
  else:
    populator.drop_and_create_database(options.db_name, db_connectors)
    if options.migrate_table_names:
      table_names = options.migrate_table_names.split(',')
    else:
      table_names = None
    populator.migrate_database(
        options.db_name,
        impala_connector,
        db_connectors,
        include_table_names=table_names)
