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

'''This module is intended to standardize workflows when working with various databases
   such as Impala, Postgresql, etc. Even with pep-249 (DB API 2), workflows differ
   slightly. For example Postgresql does not allow changing databases from within a
   connection, instead a new connection must be made. However Impala does not allow
   specifying a database upon connection, instead a cursor must be created and a USE
   command must be issued.

'''

from contextlib import contextmanager
try:
  from impala.dbapi import connect as impala_connect
except:
  print('Error importing impyla. Please make sure it is installed. '
      'See the README for details.')
  raise
from itertools import izip
from logging import getLogger
from socket import getfqdn
from tests.comparison.model import Column, Table, TYPES, String

LOG = getLogger(__name__)

IMPALA = 'IMPALA'
POSTGRESQL = 'POSTGRESQL'
MYSQL = 'MYSQL'

DATABASES = [IMPALA, POSTGRESQL, MYSQL]

mysql_connect = None
postgresql_connect = None

class DbConnector(object):
  '''Wraps a DB API 2 implementation to provide a standard way of obtaining a
     connection and selecting a database.

     Any database that supports transactions will have auto-commit enabled.

  '''

  def __init__(self, db_type, user_name=None, password=None, host_name=None, port=None):
    self.db_type = db_type.upper()
    if self.db_type not in DATABASES:
      raise Exception('Unsupported database: %s' % db_type)
    self.user_name = user_name
    self.password = password
    self.host_name = host_name or getfqdn()
    self.port = port

  def create_connection(self, db_name=None):
    if self.db_type == IMPALA:
      connection_class = ImpalaDbConnection
      connection = impala_connect(host=self.host_name, port=self.port or 21050)
    elif self.db_type == POSTGRESQL:
      connection_class = PostgresqlDbConnection
      connection_args = {'user': self.user_name or 'postgres'}
      if self.password:
        connection_args['password'] = self.password
      if db_name:
        connection_args['database'] = db_name
      if self.host_name:
        connection_args['host'] = self.host_name
      if self.port:
        connection_args['port'] = self.port
      global postgresql_connect
      if not postgresql_connect:
        try:
          from psycopg2 import connect as postgresql_connect
        except:
          print('Error importing psycopg2. Please make sure it is installed. '
              'See the README for details.')
          raise
      connection = postgresql_connect(**connection_args)
      connection.autocommit = True
    elif self.db_type == MYSQL:
      connection_class = MySQLDbConnection
      connection_args = {'user': self.user_name or 'root'}
      if self.password:
        connection_args['passwd'] = self.password
      if db_name:
        connection_args['db'] = db_name
      if self.host_name:
        connection_args['host'] = self.host_name
      if self.port:
        connection_args['port'] = self.port
      global mysql_connect
      if not mysql_connect:
        try:
          from MySQLdb import connect as mysql_connect
        except:
          print('Error importing MySQLdb. Please make sure it is installed. '
              'See the README for details.')
          raise
      connection = mysql_connect(**connection_args)
    else:
      raise Exception('Unexpected database type: %s' % self.db_type)
    return connection_class(self, connection, db_name=db_name)

  @contextmanager
  def open_connection(self, db_name=None):
    connection = None
    try:
      connection = self.create_connection(db_name=db_name)
      yield connection
    finally:
      if connection:
        try:
          connection.close()
        except Exception as e:
          LOG.debug('Error closing connection: %s', e, exc_info=True)


class DbConnection(object):
  '''Wraps a DB API 2 connection. Instances should only be obtained through the
     DbConnector.create_connection(...) method.

  '''

  @staticmethod
  def describe_common_tables(db_connections, filter_col_types=[]):
    '''Find and return a list of Table objects that the given connections have in
       common.

       @param filter_col_types: Ignore any cols if they are of a data type contained
           in this collection.

    '''
    common_table_names = None
    for db_connection in db_connections:
      table_names = set(db_connection.list_table_names())
      if common_table_names is None:
        common_table_names = table_names
      else:
        common_table_names &= table_names
    common_table_names = sorted(common_table_names)

    tables = list()
    for table_name in common_table_names:
      common_table = None
      mismatch = False
      for db_connection in db_connections:
        table = db_connection.describe_table(table_name)
        table.cols = [col for col in table.cols if col.type not in filter_col_types]
        if common_table is None:
          common_table = table
          continue
        if len(common_table.cols) != len(table.cols):
          LOG.debug('Ignoring table %s.'
              ' It has a different number of columns across databases.', table_name)
          mismatch = True
          break
        for left, right in izip(common_table.cols, table.cols):
          if not left.name == right.name and left.type == right.type:
            LOG.debug('Ignoring table %s. It has different columns %s vs %s.' %
                (table_name, left, right))
            mismatch = True
            break
        if mismatch:
          break
      if not mismatch:
        tables.append(common_table)

    return tables

  def __init__(self, connector, connection, db_name=None):
    self.connector = connector
    self.connection = connection
    self.db_name = db_name

  @property
  def db_type(self):
    return self.connector.db_type

  def create_cursor(self):
    return DatabaseCursor(self.connection.cursor(), self)

  @contextmanager
  def open_cursor(self):
    '''Returns a new cursor for use in a "with" statement. When the "with" statement ends,
       the cursor will be closed.

    '''
    cursor = None
    try:
      cursor = self.create_cursor()
      yield cursor
    finally:
      self.close_cursor_quietly(cursor)

  def close_cursor_quietly(self, cursor):
    if cursor:
      try:
        cursor.close()
      except Exception as e:
        LOG.debug('Error closing cursor: %s', e, exc_info=True)

  def list_db_names(self):
    '''Return a list of database names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_list_db_names_sql())
    return [row[0].lower() for row in rows]

  def make_list_db_names_sql(self):
    return 'SHOW DATABASES'

  def list_table_names(self):
    '''Return a list of table names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_list_table_names_sql())
    return [row[0].lower() for row in rows]

  def make_list_table_names_sql(self):
    return 'SHOW TABLES'

  def describe_table(self, table_name):
    '''Return a Table with table and col names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_describe_table_sql(table_name))
    table = Table(table_name.lower())
    for row in rows:
      col_name, data_type = row[:2]
      table.cols.append(Column(table, col_name.lower(), self.parse_data_type(data_type)))
    return table

  def make_describe_table_sql(self, table_name):
    return 'DESCRIBE ' + table_name

  def parse_data_type(self, sql):
    sql = sql.upper()
    # Types may have declared a database specific alias
    for type_ in TYPES:
      if sql in getattr(type_, self.db_type, []):
        return type_
    for type_ in TYPES:
      if type_.__name__.upper() == sql:
        return type_
    if 'CHAR' in sql:
      return String
    raise Exception('Unknown data type: ' + sql)

  def create_database(self, db_name):
    db_name = db_name.lower()
    with self.open_cursor() as cursor:
      cursor.execute('CREATE DATABASE ' + db_name)

  def drop_db_if_exists(self, db_name):
    '''This should not be called from a connection to the database being dropped.'''
    db_name = db_name.lower()
    if db_name not in self.list_db_names():
      return
    if self.db_name and self.db_name.lower() == db_name:
      raise Exception('Cannot drop database while still connected to it')
    self.drop_database(db_name)

  def drop_database(self, db_name):
    db_name = db_name.lower()
    self.execute('DROP DATABASE ' + db_name)

  @property
  def supports_index_creation(self):
    return True

  def index_table(self, table_name):
    table = self.describe_table(table_name)
    with self.open_cursor() as cursor:
      for col in table.cols:
        index_name = '%s_%s' % (table_name, col.name)
        if self.db_name:
          index_name = '%s_%s' % (self.db_name, index_name)
        cursor.execute('CREATE INDEX %s ON %s(%s)' % (index_name, table_name, col.name))

  @property
  def supports_kill_connection(self):
    return False

  def kill_connection(self):
    '''Kill the current connection and any currently running queries assosiated with the
       connection.
    '''
    raise Exception('Killing connection is not supported')

  def materialize_query(self, query_as_text, table_name):
    self.execute('CREATE TABLE %s AS %s' % (table_name.lower(), query_as_text))

  def drop_table(self, table_name):
    self.execute('DROP TABLE ' + table_name.lower())

  def execute(self, sql):
    with self.open_cursor() as cursor:
      cursor.execute(sql)

  def execute_and_fetchall(self, sql):
    with self.open_cursor() as cursor:
      cursor.execute(sql)
      return cursor.fetchall()

  def close(self):
    '''Close the underlying connection.'''
    self.connection.close()

  def reconnect(self):
    self.close()
    other = self.connector.create_connection(db_name=self.db_name)
    self.connection = other.connection


class DatabaseCursor(object):
  '''Wraps a DB API 2 cursor to provide access to the related connection. This class
     implements the DB API 2 interface by delegation.

  '''

  def __init__(self, cursor, connection):
    self.cursor = cursor
    self.connection = connection

  def __getattr__(self, attr):
    return getattr(self.cursor, attr)


class ImpalaDbConnection(DbConnection):

  def create_cursor(self):
    cursor = DbConnection.create_cursor(self)
    if self.db_name:
      cursor.execute('USE %s' % self.db_name)
    return cursor

  def drop_database(self, db_name):
    '''This should not be called from a connection to the database being dropped.'''
    db_name = db_name.lower()
    with self.connector.open_connection(db_name) as list_tables_connection:
      with list_tables_connection.open_cursor() as drop_table_cursor:
        for table_name in list_tables_connection.list_table_names():
          drop_table_cursor.execute('DROP TABLE ' + table_name)
    self.execute('DROP DATABASE ' + db_name)

  @property
  def supports_index_creation(self):
    return False


class PostgresqlDbConnection(DbConnection):

  def make_list_db_names_sql(self):
    return 'SELECT datname FROM pg_database'

  def make_list_table_names_sql(self):
    return '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' '''

  def make_describe_table_sql(self, table_name):
    return '''
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '%s'
        ORDER BY ordinal_position''' % table_name


class MySQLDbConnection(DbConnection):

  def __init__(self, connector, connection, db_name=None):
    DbConnection.__init__(self, connector, connection, db_name=db_name)
    self.session_id = self.execute_and_fetchall('SELECT connection_id()')[0][0]

  def describe_table(self, table_name):
    '''Return a Table with table and col names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_describe_table_sql(table_name))
    table = Table(table_name.lower())
    for row in rows:
      col_name, data_type = row[:2]
      if data_type == 'tinyint(1)':
        # Just assume this is a boolean...
        data_type = 'boolean'
      if '(' in data_type:
        # Strip the size of the data type
        data_type = data_type[:data_type.index('(')]
      table.cols.append(Column(table, col_name.lower(), self.parse_data_type(data_type)))
    return table

  @property
  def supports_kill_connection(self):
    return True

  def kill_connection(self):
    with self.connector.open_connection(db_name=self.db_name) as connection:
      connection.execute('KILL %s' % (self.session_id))

  def index_table(self, table_name):
    table = self.describe_table(table_name)
    with self.open_cursor() as cursor:
      for col in table.cols:
        try:
          cursor.execute('ALTER TABLE %s ADD INDEX (%s)' % (table_name, col.name))
        except Exception as e:
          if 'Incorrect index name' not in str(e):
            raise
          # Some sort of MySQL bug...
          LOG.warn('Could not create index on %s.%s: %s' % (table_name, col.name, e))
