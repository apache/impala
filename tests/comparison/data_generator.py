#!/usr/bin/env python

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
from logging import basicConfig, getLogger
from random import choice, randint, random, seed, uniform

from tests.comparison.db_connector import (
    DbConnector,
    IMPALA,
    MYSQL,
    POSTGRESQL)
from tests.comparison.model import (
    Boolean,
    Column,
    Float,
    Int,
    Number,
    String,
    Table,
    Timestamp,
    TYPES)

LOG = getLogger(__name__)

class RandomValGenerator(object):
  '''This class will generate random data of various data types. Currently only numeric
     and string data types are supported.

  '''

  def __init__(self,
      min_number=-1000,
      max_number=1000,
      min_date=datetime(1990, 1, 1),
      max_date=datetime(2030, 1, 1),
      null_val_percentage=0.1):
    self.min_number = min_number
    self.max_number = max_number
    self.min_date = min_date
    self.max_date = max_date
    self.null_val_percentage = null_val_percentage

  def generate_val(self, val_type):
    '''Generate and return a single random val. Use the val_type parameter to
       specify the type of val to generate. See model.DataType for valid val_type
       options.

       Ex:
         generator = RandomValGenerator(min_number=1, max_number=5)
         val = generator.generate_val(model.Int)
         assert 1 <= val and val <= 5
    '''
    if issubclass(val_type, String):
      val = self.generate_val(Int)
      return None if val is None else str(val)
    if random() < self.null_val_percentage:
      return None
    if issubclass(val_type, Int):
      return randint(
          max(self.min_number, val_type.MIN), min(val_type.MAX, self.max_number))
    if issubclass(val_type, Number):
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
      number_of_tables=10,
      allowed_data_types=TYPES,
      create_files=False):
    '''Create tables with a random number of cols with data types chosen from
       allowed_data_types, then fill the tables with data.

       The given db_name must have already been created.

    '''
    connections = [connector.create_connection(db_name=db_name)
                   for connector in db_connectors]
    for table_idx in xrange(number_of_tables):
      table = self.create_random_table(
          'table_%s' % (table_idx + 1),
          allowed_data_types=allowed_data_types)
      for connection in connections:
        sql = self.make_create_table_sql(table, dialect=connection.db_type)
        LOG.info('Creating %s table %s', connection.db_type, table.name)
        if create_files:
          with open('%s_%s.sql' % (table.name, connection.db_type.lower()), 'w') \
              as f:
            f.write(sql + '\n')
        connection.execute(sql)
      LOG.info('Inserting data into %s', table.name)
      for _ in xrange(100):   # each iteration will insert 100 rows
        rows = self.generate_table_data(table)
        for connection in connections:
          sql = self.make_insert_sql_from_data(
              table, rows, dialect=connection.db_type)
          if create_files:
            with open('%s_%s.sql' %
                (table.name, connection.db_type.lower()), 'a') as f:
              f.write(sql + '\n')
          try:
            connection.execute(sql)
          except:
            LOG.error('Error executing SQL: %s', sql)
            raise

    self.index_tables_in_database(connections)

    for connection in connections:
      connection.close()

  def migrate_database(self,
      db_name,
      source_db_connector,
      destination_db_connectors,
      include_table_names=None):
    '''Read table metadata and data from the source database and create a replica in
       the destination databases. For example, the Impala funcal test database could
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
        sql = self.make_create_table_sql(
            table, dialect=destination_cursor.connection.db_type)
        destination_cursor.execute(sql)
      with source_connection.open_cursor() as source_cursor:
        try:
          source_cursor.execute('SELECT * FROM ' + table_name)
          while True:
            rows = source_cursor.fetchmany(size=100)
            if not rows:
              break
            for destination_cursor in cursors:
              sql = self.make_insert_sql_from_data(
                  table, rows, dialect=destination_cursor.connection.db_type)
              destination_cursor.execute(sql)
        except Exception as e:
          LOG.error('Error fetching data for %s: %s', table_name, e)
          continue

    self.index_tables_in_database([cursor.connection for cursor in cursors])

    for cursor in cursors:
      cursor.close()
      cursor.connection.close()

  def create_random_table(self, table_name, allowed_data_types):
    '''Create and return a Table with a random number of cols chosen from the
       given allowed_data_types.

    '''
    data_type_count = len(allowed_data_types)
    col_count = randint(data_type_count / 2, data_type_count * 2)
    table = Table(table_name)
    for col_idx in xrange(col_count):
      col_type = choice(allowed_data_types)
      col = Column(
          table,
          '%s_col_%s' % (col_type.__name__.lower(), col_idx + 1),
          col_type)
      table.cols.append(col)
    return table

  def make_create_table_sql(self, table, dialect=IMPALA):
    sql = 'CREATE TABLE %s (%s)' % (
        table.name,
        ', '.join('%s %s' %
            (col.name, self.get_sql_for_data_type(col.type, dialect)) +
            ('' if dialect == IMPALA else ' NULL')
            for col in table.cols))
    if dialect == MYSQL:
      sql += ' ENGINE = MYISAM'
    return sql

  def get_sql_for_data_type(self, data_type, dialect=IMPALA):
    # Check to see if there is an alias and if so, use the first one
    if hasattr(data_type, dialect):
      return getattr(data_type, dialect)[0]
    return data_type.__name__.upper()

  def make_insert_sql_from_data(self, table, rows, dialect=IMPALA):
    # TODO: Consider using parameterized inserts so the database connector handles
    #       formatting the data. For example the CAST to workaround IMPALA-803 can
    #       probably be removed. The vals were generated this way so a data file
    #       could be made and attached to jiras.
    if not rows:
      raise Exception('At least one row is required')
    if not table.cols:
      raise Exception('At least one col is required')

    sql = 'INSERT INTO %s VALUES ' % table.name
    for row_idx, row in enumerate(rows):
      if row_idx > 0:
        sql += ', '
      sql += '('
      for col_idx, col in enumerate(table.cols):
        if col_idx > 0:
          sql += ', '
        val = row[col_idx]
        if val is None:
          sql += 'NULL'
        elif issubclass(col.type, Timestamp):
          if dialect != IMPALA:
            sql += 'TIMESTAMP '
          sql += "'%s'" % val
        elif issubclass(col.type, String):
          val = val.replace("'", "''")
          if dialect == POSTGRESQL:
            val = val.replace('\\', '\\\\')
          sql += "'%s'" % val
        elif dialect == IMPALA \
            and issubclass(col.type, Float):
          # https://issues.cloudera.org/browse/IMPALA-803
          sql += 'CAST(%s AS FLOAT)' % val
        else:
          sql += str(val)
      sql += ')'
    return sql

  def generate_table_data(self, table, number_of_rows=100):
      rows = list()
      for row_idx in xrange(number_of_rows):
        row = list()
        for col in table.cols:
          row.append(self.val_generator.generate_val(col.type))
        rows.append(row)
      return rows

  def drop_and_create_database(self, db_name, db_connectors):
    for connector in db_connectors:
      with connector.open_connection() as connection:
        connection.drop_db_if_exists(db_name)
        connection.execute('CREATE DATABASE ' + db_name)

  def index_tables_in_database(self, connections):
    for connection in connections:
      if connection.supports_index_creation:
        for table_name in connection.list_table_names():
          LOG.info('Indexing %s on %s' % (table_name, connection.db_type))
          connection.index_table(table_name)

if __name__ == '__main__':
  from optparse import NO_DEFAULT, OptionGroup, OptionParser

  parser = OptionParser(
      usage='usage: \n'
            '  %prog [options] [populate]\n\n'
            '     Create and populate database(s). The Impala database will always be \n'
            '     included, the other database types are optional.\n\n'
            '  %prog [options] migrate\n\n'
            '     Migrate an Impala database to another database type. The destination \n'
            '     database will be dropped and recreated.')
  parser.add_option('--log-level', default='INFO',
      help='The log level to use.', choices=('DEBUG', 'INFO', 'WARN', 'ERROR'))
  parser.add_option('--db-name', default='randomness',
      help='The name of the database to use. Ex: functional.')

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

  group = OptionGroup(parser, 'Database Population Options')
  group.add_option('--randomization-seed', default=1, type='int',
      help='The randomization will be initialized with this seed. Using the same seed '
          'will produce the same results across runs.')
  group.add_option('--create-data-files', default=False, action='store_true',
      help='Create files that can be used to repopulate the databasese elsewhere.')
  group.add_option('--table-count', default=10, type='int',
      help='The number of tables to generate.')
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
  if command == 'migrate' and not any((options.use_mysql, options.use_postgresql)):
    raise Exception('At least one destination database must be chosen with '
          '--use-<database type>')

  basicConfig(level=options.log_level)

  seed(options.randomization_seed)

  impala_connector = DbConnector(IMPALA)
  db_connectors = []
  if options.use_postgresql:
    db_connectors.append(DbConnector(POSTGRESQL,
      user_name=options.postgresql_user,
      password=options.postgresql_password,
      host_name=options.postgresql_host,
      port=options.postgresql_port))
  if options.use_mysql:
    db_connectors.append(DbConnector(MYSQL,
      user_name=options.mysql_user,
      password=options.mysql_password,
      host_name=options.mysql_host,
      port=options.mysql_port))

  populator = DatabasePopulator()
  if command == 'populate':
    db_connectors.append(impala_connector)
    populator.drop_and_create_database(options.db_name, db_connectors)
    populator.populate_db_with_random_data(
        options.db_name,
        db_connectors,
        number_of_tables=options.table_count,
        create_files=options.create_data_files)
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
