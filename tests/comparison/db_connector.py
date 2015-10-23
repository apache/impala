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

from contextlib import closing, contextmanager
from copy import copy
from decimal import Decimal as PyDecimal
try:
  from impala.dbapi import connect as impala_connect
except:
  print('Error importing impyla. Please make sure it is installed. '
      'See the README for details.')
  raise
from itertools import combinations, ifilter, izip
from json import dumps
from logging import getLogger
from os import chmod, symlink, unlink
from os.path import basename, dirname
from psycopg2 import connect as postgresql_connect
from pyparsing import (
    alphanums,
    delimitedList,
    Forward,
    Group,
    Literal,
    nums,
    Suppress,
    Word)
from random import randint
from re import compile
import shelve
from socket import getfqdn
from sys import maxint
from tempfile import NamedTemporaryFile, gettempdir
from threading import Lock
from time import time

from tests.comparison.common import (
    ArrayColumn,
    Column,
    MapColumn,
    StructColumn,
    Table,
    TableExprList)
from tests.comparison.types import (
    Char,
    Decimal,
    Double,
    EXACT_TYPES,
    Float,
    get_char_class,
    get_decimal_class,
    get_varchar_class,
    Int,
    String,
    Timestamp,
    TinyInt,
    VarChar)
from tests.util.hdfs_util import (
    create_default_hdfs_client,
    get_default_hdfs_config,
    get_hdfs_client)

LOG = getLogger(__name__)

HIVE = 'HIVE'
IMPALA = 'IMPALA'
MYSQL = 'MYSQL'
ORACLE = 'ORACLE'
POSTGRESQL = 'POSTGRESQL'

DATABASES = [HIVE, IMPALA, MYSQL, ORACLE, POSTGRESQL]

class DbConnector(object):
  '''Wraps a DB API 2 implementation to provide a standard way of obtaining a
     connection and selecting a database.

     Any database that supports transactions will have auto-commit enabled.

  '''

  lock = Lock()

  def __init__(self,
      db_type,
      user_name=None,
      password=None,
      host_name=None,
      port=None,
      service=None,
      hdfs_host=None,
      hdfs_port=None):
    self.db_type = db_type.upper()
    if self.db_type not in DATABASES:
      raise Exception('Unsupported database: %s' % db_type)
    self.user_name = user_name
    self.password = password
    self.host_name = host_name or getfqdn()
    self.port = port
    self.service = service
    self.hdfs_host = hdfs_host
    self.hdfs_port = hdfs_port
    try:
      DbConnector.lock.acquire()
      sql_log_path = gettempdir() + '/sql_log_%s_%s.sql' \
            % (self.db_type.lower(), time())
      self.sql_log = open(sql_log_path, 'w')
      link = gettempdir() + '/sql_log_%s.sql' % self.db_type.lower()
      try:
        unlink(link)
      except OSError as e:
        if not 'No such file' in str(e):
          raise e
      try:
        symlink(sql_log_path, link)
      except OSError as e:
        raise e
    finally:
      DbConnector.lock.release()

  def create_connection(self, db_name=None):
    if self.db_type == HIVE:
      connection_class = HiveDbConnection
      connection = impala_connect(
          host=self.host_name,
          port=self.port,
          user=self.user_name,
          password=self.password,
          timeout=maxint,
          auth_mechanism='PLAIN')
      return HiveDbConnection(self, connection, user_name=self.user_name,
          user_pass=self.password, db_name=db_name, hdfs_host=self.hdfs_host,
          hdfs_port=self.hdfs_port)
    elif self.db_type == IMPALA:
      connection_class = ImpalaDbConnection
      connection = impala_connect(
          host=self.host_name,
          port=self.port or 21050,
          timeout=maxint)
    elif self.db_type == ORACLE:
      connection_class = OracleDbConnection
      connection_str = '%(user)s/%(password)s@%(host)s:%(port)s/%(service)s'
      connection_args = {
        'user': self.user_name or 'system',
        'password': self.password or 'oracle',
        'host': self.host_name or 'localhost',
        'port': self.port or 1521,
        'service': self.service or 'XE'}
      try:
        from cx_Oracle import connect as oracle_connect
      except:
        print('Error importing cx_Oracle. Please make sure it is installed. '
            'See the README for details.')
        raise
      connection = oracle_connect(connection_str % connection_args)
      connection.outputtypehandler = OracleDbConnection.type_converter
      connection.autocommit = True
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
  def describe_common_tables(db_connections):
    '''Find and return a TableExprList containing Table objects that the given connections
       have in common.
    '''
    common_table_names = None
    for db_connection in db_connections:
      table_names = set(db_connection.list_table_names())
      if common_table_names is None:
        common_table_names = table_names
      else:
        common_table_names &= table_names
    common_table_names = sorted(common_table_names)

    tables = TableExprList()
    for table_name in common_table_names:
      common_table = None
      mismatch = False
      for db_connection in db_connections:
        table = db_connection.describe_table(table_name)
        if common_table is None:
          common_table = table
          continue
        if not table.cols:
          LOG.debug('%s has no remaining columns', table_name)
          mismatch = True
          break
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

  SQL_TYPE_PATTERN = compile(r'([^()]+)(\((\d+,? ?)*\))?')
  TYPE_NAME_ALIASES = \
      dict((type_.name().upper(), type_.name().upper()) for type_ in EXACT_TYPES)
  TYPES_BY_NAME =  dict((type_.name().upper(), type_) for type_ in EXACT_TYPES)
  EXACT_TYPES_TO_SQL = dict((type_, type_.name().upper()) for type_ in EXACT_TYPES)

  def __init__(self, connector, connection, db_name=None):
    self.connector = connector
    self.connection = connection
    self.db_name = db_name

    self._bulk_load_table = None
    self._bulk_load_data_file = None   # If not set by the user, a temp file will be used
    self._bulk_load_col_delimiter = b'\x01'
    self._bulk_load_row_delimiter = '\n'
    self._bulk_load_null_val = '\\N'

  @property
  def db_type(self):
    return self.connector.db_type

  @property
  def supports_kill_connection(self):
    return False

  def kill_connection(self):
    '''Kill the current connection and any currently running queries associated with the
       connection.
    '''
    raise Exception('Killing connection is not supported')

  @property
  def supports_index_creation(self):
    return True

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
    try:
      self.close()
    except Exception as e:
      LOG.warn('Error closing connection: %s' % e)
    other = self.connector.create_connection(db_name=self.db_name)
    self.connection = other.connection

  #########################################
  # Databases                             #
  #########################################

  def list_db_names(self):
    '''Return a list of database names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_list_db_names_sql())
    return [row[0].lower() for row in rows]

  def make_list_db_names_sql(self):
    return 'SHOW DATABASES'

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

  #########################################
  # Tables                                #
  #########################################

  def list_table_names(self):
    '''Return a list of table names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_list_table_names_sql())
    return [row[0].lower() for row in rows]

  def make_list_table_names_sql(self):
    return 'SHOW TABLES'

  def parse_col_desc(self, data_type):

    ''' Returns a prased output based on type describe output.

    data_type is string that should look like this:
    "bigint"
    or like this:
    "array<struct<
      field_51:int,
      field_52:bigint,
      field_53:int,
      field_54:boolean
    >>"

    In the first case, this method would return: 'bigint'

    In the second case, it would return

    ['array',
      ['struct',
        ['field_51', 'int'],
        ['field_52', 'bigint'],
        ['field_53', 'int'],
        ['field_54', 'boolean']]]

    This output is used to create the appropriate columns by self.create_column().
    '''

    COMMA, LPAR, RPAR, COLON, LBRA, RBRA = map(Suppress, ",<>:()")

    t_bigint = Literal('bigint')
    t_int = Literal('int')
    t_smallint = Literal('smallint')
    t_tinyint = Literal('tinyint')
    t_boolean = Literal('boolean')
    t_string = Literal('string')
    t_timestamp = Literal('timestamp')
    t_float = Literal('float')
    t_double = Literal('double')

    t_decimal = Group(Literal('decimal') + LBRA + Word(nums) + COMMA + Word(nums) + RBRA)
    t_char = Group(Literal('char') + LBRA + Word(nums) + RBRA)
    t_varchar = (Group(Literal('varchar') + LBRA + Word(nums) + RBRA) |
        Literal('varchar'))

    t_struct = Forward()
    t_array = Forward()
    t_map = Forward()

    complex_type = (t_struct | t_array | t_map)

    any_type = (
        complex_type |
        t_bigint |
        t_int |
        t_smallint |
        t_tinyint |
        t_boolean |
        t_string |
        t_timestamp |
        t_float |
        t_double |
        t_decimal |
        t_char |
        t_varchar)

    struct_field_name = Word(alphanums + '_')
    struct_field_pair = Group(struct_field_name + COLON + any_type)

    t_struct << Group(Literal('struct') + LPAR + delimitedList(struct_field_pair) + RPAR)
    t_array << Group(Literal('array') + LPAR + any_type + RPAR)
    t_map << Group(Literal('map') + LPAR + any_type + COMMA + any_type + RPAR)

    return any_type.parseString(data_type)[0]

  def create_column(self, col_name, col_type):
    ''' Takes the output from parse_col_desc and creates the right column type. This
    method returns one of Column, ArrayColumn, MapColumn, StructColumn.'''
    if isinstance(col_type, str):
      if col_type.upper() == 'VARCHAR':
        col_type = 'STRING'
      type_name = self.TYPE_NAME_ALIASES.get(col_type.upper())
      return Column(owner=None,
          name=col_name.lower(),
          exact_type=self.TYPES_BY_NAME[type_name])

    general_class = col_type[0]

    if general_class.upper() in ('DECIMAL', 'NUMERIC'):
      return Column(owner=None,
          name=col_name.lower(),
          exact_type=get_decimal_class(int(col_type[1]), int(col_type[2])))

    if general_class.upper() == 'CHAR':
      return Column(owner=None,
          name=col_name.lower(),
          exact_type=get_char_class(int(col_type[1])))

    if general_class.upper() == 'VARCHAR':
      type_size = int(col_type[1])
      if type_size <= VarChar.MAX:
        cur_type = get_varchar_class(type_size)
      else:
        cur_type = self.TYPES_BY_NAME['STRING']
      return Column(owner=None,
          name=col_name.lower(),
          exact_type=cur_type)

    if general_class.upper() == 'ARRAY':
      return ArrayColumn(
          owner=None,
          name=col_name.lower(),
          item=self.create_column(col_name='item', col_type=col_type[1]))

    if general_class.upper() == 'MAP':
      return MapColumn(
          owner=None,
          name=col_name.lower(),
          key=self.create_column(col_name='key', col_type=col_type[1]),
          value=self.create_column(col_name='value', col_type=col_type[2]))

    if general_class.upper() == 'STRUCT':
      struct_col = StructColumn(owner=None, name=col_name.lower())
      for field_name, field_type in col_type[1:]:
        struct_col.add_col(self.create_column(field_name, field_type))
      return struct_col

    raise Exception('unable to parse: {0}, type: {1}'.format(col_name, col_type))

  def create_table_from_describe(self, table_name, describe_rows):
    table = Table(table_name.lower())
    for row in describe_rows:
      col_name, data_type = row[:2]
      col_type = self.parse_col_desc(data_type)
      col = self.create_column(col_name, col_type)
      table.add_col(col)
    return table

  def describe_table(self, table_name):
    '''Return a Table with table and col names always in lowercase.'''
    describe_rows = self.execute_and_fetchall(self.make_describe_table_sql(table_name))
    table = self.create_table_from_describe(table_name, describe_rows)
    self.load_unique_col_metadata(table)
    return table

  def make_describe_table_sql(self, table_name):
    return 'DESCRIBE ' + table_name

  def parse_data_type(self, type_name, type_size):
    if type_name in ('DECIMAL', 'NUMERIC'):
      return get_decimal_class(*type_size)
    if type_name == 'CHAR':
      return get_char_class(*type_size)
    if type_name == 'VARCHAR':
      if type_size and type_size[0] <= VarChar.MAX:
        return get_varchar_class(*type_size)
      type_name = 'STRING'
    return self.TYPES_BY_NAME[type_name]

  def create_table(self, table):
    if not table.cols:
      raise Exception('At least one col is required')
    table_sql = self.make_create_table_sql(table)
    self.execute(table_sql)

  def make_create_table_sql(self, table):
    sql = 'CREATE TABLE %s (%s)' % (
        table.name,
        ', '.join('%s %s' %
            (col.name, self.get_sql_for_data_type(col.exact_type)) +
            ('' if (self.db_type == IMPALA or self.db_type == HIVE) else ' NULL')
            for col in table.cols))
    return sql

  def get_sql_for_data_type(self, data_type):
    if issubclass(data_type, VarChar):
      return 'VARCHAR(%s)' % data_type.MAX
    if issubclass(data_type, Char):
      return 'CHAR(%s)' % data_type.MAX
    if issubclass(data_type, Decimal):
      return 'DECIMAL(%s, %s)' % (data_type.MAX_DIGITS, data_type.MAX_FRACTIONAL_DIGITS)
    return self.EXACT_TYPES_TO_SQL[data_type]

  def drop_table(self, table_name, if_exists=True):
    self.execute('DROP TABLE IF EXISTS ' + table_name.lower())

  def drop_view(self, view_name, if_exists=True):
    self.execute('DROP VIEW IF EXISTS ' + view_name.lower())

  def index_table(self, table_name):
    table = self.describe_table(table_name)
    with self.open_cursor() as cursor:
      for col in table.cols:
        index_name = '%s_%s' % (table_name, col.name)
        if self.db_name:
          index_name = '%s_%s' % (self.db_name, index_name)
        cursor.execute('CREATE INDEX ON %s(%s)' % (table_name, col.name))

  #########################################
  # Data loading                          #
  #########################################

  def make_insert_sql_from_data(self, table, rows):
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
          sql += "TIMESTAMP '%s'" % val
        elif issubclass(col.type, Char):
          sql += "'%s'" % val.replace("'", "''")
        else:
          sql += str(val)
      sql += ')'

    return sql

  def begin_bulk_load_table(self, table, create_tables):
    if create_tables:
      self.create_table(table)
    self._bulk_load_table = table
    if not self._bulk_load_data_file:
      self._bulk_load_data_file = NamedTemporaryFile()

  def handle_bulk_load_table_data(self, rows):
    if not rows:
      raise Exception('At least one row is required')

    data = list()
    for row in rows:
      for col_idx, col in enumerate(self._bulk_load_table.cols):
        if col_idx > 0:
          data.append(self._bulk_load_col_delimiter)
        val = row[col_idx]
        if val is None:
          file_val = self._bulk_load_null_val
        else:
          file_val = str(val)
        data.append(file_val)
      data.append(self._bulk_load_row_delimiter)
    if data:
      self._bulk_load_data_file.writelines(data)

  def end_bulk_load_table(self, create_tables):
    self._bulk_load_data_file.flush()

  #########################################
  # Data analysis                         #
  #########################################

  def search_for_unique_cols(self, table=None, table_name=None, depth=2):
    if not table:
      table = self.describe_table(table_name)
    sql_templ = 'SELECT COUNT(*) FROM %s GROUP BY %%s HAVING COUNT(*) > 1' % table.name
    unique_cols = list()
    with self.open_cursor() as cursor:
      for current_depth in xrange(1, depth + 1):
        for cols in combinations(table.cols, current_depth):   # redundant combos excluded
          cols = set(cols)
          if any(ifilter(lambda unique_subset: unique_subset < cols, unique_cols)):
            # cols contains a combo known to be unique
            continue
          col_names = ', '.join(col.name for col in cols)
          sql = sql_templ % col_names
          LOG.debug('Checking column combo (%s) for uniqueness' % col_names)
          cursor.execute(sql)
          if not cursor.fetchone():
            LOG.debug('Found unique column combo (%s)' % col_names)
            unique_cols.append(cols)
    return unique_cols

  def persist_unique_col_metadata(self, table):
    if not table.unique_cols:
      return
    with closing(shelve.open('/tmp/query_generator.shelve', writeback=True)) as store:
      if self.db_type not in store:
        store[self.db_type] = dict()
      db_type_store = store[self.db_type]
      if self.db_name not in db_type_store:
        db_type_store[self.db_name] = dict()
      db_store = db_type_store[self.db_name]
      db_store[table.name] = [[col.name for col in cols] for cols in table.unique_cols]

  def load_unique_col_metadata(self, table):
    with closing(shelve.open('/tmp/query_generator.shelve')) as store:
      db_type_store = store.get(self.db_type)
      if not db_type_store:
        return
      db_store = db_type_store.get(self.db_name)
      if not db_store:
        return
      unique_col_names = db_store.get(table.name)
      if not unique_col_names:
        return
      unique_cols = list()
      for entry in unique_col_names:
        col_names = set(entry)
        cols = set((col for col in table.cols if col.name in col_names))
        if len(col_names) != len(cols):
          raise Exception("Incorrect unique column data for %s" % table.name)
        unique_cols.append(cols)
      table.unique_cols = unique_cols


class DatabaseCursor(object):
  '''Wraps a DB API 2 cursor to provide access to the related connection. This class
     implements the DB API 2 interface by delegation.

  '''

  def __init__(self, cursor, connection):
    self.cursor = cursor
    self.connection = connection

  def __getattr__(self, attr):
    return getattr(self.cursor, attr)

  def execute(self, sql):
    LOG.debug('%s: %s' % (self.connection.connector.db_type, sql))
    self.connection.connector.sql_log.write('\nQuery: %s' % sql)
    return self.cursor.execute(sql)


#########################################
## Impala                              ##
#########################################
class ImpalaDbConnection(DbConnection):

  def __init__(self, *args, **kwargs):
    DbConnection.__init__(self, *args, **kwargs)
    self.warehouse_dir = '/test-warehouse'

    # Data loading stuff
    self.natively_supported_writing_formats = ['PARQUET']
    self._bulk_load_non_text_table = None
    # A Hive connection is needed for writing data in formats that are unsupported in
    # Impala (Impala can read the data but not write it). Eventually this should removed.
    self.hive_connection = None

  @property
  def supports_index_creation(self):
    return False

  def create_cursor(self):
    cursor = ImpalaDbCursor(self.connection.cursor(), self)
    if self.db_name:
      cursor.execute('USE %s' % self.db_name)
    return cursor

  #########################################
  # Databases                             #
  #########################################

  def drop_database(self, db_name):
    '''This should not be called from a connection to the database being dropped.'''
    db_name = db_name.lower()
    with self.connector.open_connection(db_name) as list_tables_connection:
      with list_tables_connection.open_cursor() as drop_table_cursor:
        for table_name in list_tables_connection.list_table_names():
          drop_table_cursor.execute('DROP TABLE ' + table_name)
    self.execute('DROP DATABASE ' + db_name)

  #########################################
  # Tables                                #
  #########################################

  def make_create_table_sql(self, table):
    sql = super(ImpalaDbConnection, self).make_create_table_sql(table)
    if not self._bulk_load_table:
      return sql

    hdfs_url_base = get_default_hdfs_config().get('fs.defaultFS')
    sql += "\nLOCATION '%s%s'" % (hdfs_url_base, dirname(self.hdfs_file_path))
    if self._bulk_load_table.storage_format.upper() != 'TEXTFILE':
      sql += "\nSTORED AS " + table.storage_format

    if table.storage_format == 'avro':
      avro_schema = {'name': 'my_record', 'type': 'record', 'fields': []}
      for col in table.cols:
        if issubclass(col.type, Int):
          avro_type = 'int'
        else:
          avro_type = col.type.__name__.lower()
        avro_schema['fields'].append({'name': col.name, 'type': ['null', avro_type]})
      json_avro_schema = dumps(avro_schema)
      # The Hive metastore has a limit to the amount of schema it can store inline.
      # Beyond this limit, the schema needs to be stored in HDFS and Hive is given a
      # URL instead.
      if len(json_avro_schema) > 4000:
        avro_schema_url = 'foo'
        avro_schema_path = '%s/%s.avro' % (self.hdfs_db_dir, table.name)
        hdfs = create_default_hdfs_client()
        hdfs.create_file(avro_schema_path, json_avro_schema, overwrite=True)
        sql += "\nTBLPROPERTIES ('avro.schema.url' = '%s')" % avro_schema_url
      else:
        sql += "\nTBLPROPERTIES ('avro.schema.literal' = '%s')" % json_avro_schema

    return sql

  def get_sql_for_data_type(self, data_type):
    if issubclass(data_type, String):
      return 'STRING'
    return super(ImpalaDbConnection, self).get_sql_for_data_type(data_type)

  #########################################
  # Data loading                          #
  #########################################

  def make_insert_sql_from_data(self, table, rows):
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
          sql += "'%s'" % val
        elif issubclass(col.type, Char):
          val = val.replace("'", "''")
          sql += "'%s'" % val
        else:
          sql += str(val)
      sql += ')'

    return sql

  @property
  def hdfs_db_dir(self):
    return '%s/%s.db' % (self.warehouse_dir, self.db_name)

  @property
  def hdfs_file_path(self):
    return self.hdfs_db_dir + '/%s/data' % self._bulk_load_table.name

  def begin_bulk_load_table(self, table, create_tables):
    if create_tables and table.storage_format.upper() == 'TEXTFILE':
      # New Text table, simply copy the file over.
      self._bulk_load_table = table
      super(ImpalaDbConnection, self).begin_bulk_load_table(table, create_tables)
    else:
      # There is no python writer for all the various formats. Instead an additional text
      # table will be created then either Impala or Hive will be used to insert the data
      # in the desired format into the final table.
      # This is also used when inserting data to pre-existing table to avoid overriding.
      if not self.natively_supports_writing_format(table.storage_format) \
          and not self.hive_connection:
        raise Exception("Creating a " + table.storage_format + " table requires that"
            "the hive_connection property be set")
      self._bulk_load_non_text_table = table
      self._bulk_load_table = copy(table)
      table_sql = self.make_create_table_sql(self._bulk_load_table)
      self.execute(table_sql)
      self._bulk_load_table.name += "_temp_%03d" % randint(1, 999)
      self._bulk_load_table.storage_format = 'textfile'
      super(ImpalaDbConnection, self).begin_bulk_load_table(self._bulk_load_table, create_tables=True)

  def natively_supports_writing_format(self, storage_format):
    for supported_format in self.natively_supported_writing_formats:
      if supported_format == storage_format.upper():
        return True

  def end_bulk_load_table(self, create_tables):
    super(ImpalaDbConnection, self).end_bulk_load_table(create_tables)
    hdfs = create_default_hdfs_client()
    pywebhdfs_dirname = dirname(self.hdfs_file_path).lstrip('/')
    hdfs.make_dir(pywebhdfs_dirname)
    pywebhdfs_file_path = pywebhdfs_dirname + '/' + basename(self.hdfs_file_path)
    try:
      # TODO: Only delete the file if it exists
      hdfs.delete_file_dir(pywebhdfs_file_path)
    except Exception as e:
      LOG.debug(e)
    with open(self._bulk_load_data_file.name) as readable_file:
      hdfs.create_file(pywebhdfs_file_path, readable_file)
    self._bulk_load_data_file.close()
    self.execute("INVALIDATE METADATA %s" % self._bulk_load_table.name)
    if self._bulk_load_non_text_table:
      if create_tables:
        self.create_table(self._bulk_load_non_text_table)
      self.execute('INSERT INTO TABLE %s SELECT * FROM %s'
          % (self._bulk_load_non_text_table.name, self._bulk_load_table.name))
      self.drop_table(self._bulk_load_table.name)
      self.execute("INVALIDATE METADATA %s" % self._bulk_load_non_text_table)
    self._bulk_load_data_file = None


class ImpalaDbCursor(DatabaseCursor):

  def get_log(self):
    '''Return any messages relating to the most recently executed query or statement.'''
    return self.cursor.get_log()

#########################################
## Hive                                ##
#########################################
class HiveDbConnection(ImpalaDbConnection):

  def __init__(self, connector, connection, user_name, user_pass, db_name, hdfs_host, hdfs_port):
    ImpalaDbConnection.__init__(self, connector, connection, db_name)
    self.hdfs_host = hdfs_host
    self.hdfs_port = hdfs_port
    self.warehouse_dir = '/user/hive/warehouse'
    self.user_name = user_name
    self.user_pass = user_pass

  def create_cursor(self):
    cursor = ImpalaDbCursor(self.connection.cursor(user=self.user_name), self)
    if self.db_name:
      cursor.execute('USE %s' % self.db_name)
    return cursor

  #########################################
  # Tables                                #
  #########################################

  def make_create_table_sql(self, table):
    sql = DbConnection.make_create_table_sql(self, table)

    if table.storage_format.upper() not in ('TEXTFILE', 'PARQUET'):
      raise Exception('Only Textfile and Parquet currently supported')

    if table.storage_format.upper() != 'TEXTFILE':
      sql += "\nSTORED AS " + table.storage_format
    return sql

  def begin_bulk_load_table(self, table, create_tables):
    if create_tables and table.storage_format.upper() == 'TEXTFILE':
      # New Text table, simply copy the file over.
      self._bulk_load_table = table
      DbConnection.begin_bulk_load_table(self, table, create_tables)
    else:
      # There is no python writer for all the various formats. Instead an additional text
      # table will be created then either Impala or Hive will be used to insert the data
      # in the desired format into the final table.
      # This is also used when inserting data to pre-existing table to avoid overriding.
      self._bulk_load_non_text_table = table
      self._bulk_load_table = copy(table)
      self._bulk_load_table.name += "_temp_%03d" % randint(1, 999)
      self._bulk_load_table.storage_format = 'textfile'
      DbConnection.begin_bulk_load_table(self, self._bulk_load_table, create_tables=True)

  def end_bulk_load_table(self, create_tables):
    DbConnection.end_bulk_load_table(self, create_tables)
    if self.hdfs_host is None:
      hdfs = create_default_hdfs_client()
    else:
      hdfs = get_hdfs_client(self.hdfs_host, self.hdfs_port, user_name='hdfs')
    pywebhdfs_dirname = dirname(self.hdfs_file_path).lstrip('/')
    hdfs.make_dir(pywebhdfs_dirname)
    pywebhdfs_file_path = pywebhdfs_dirname + '/' + basename(self.hdfs_file_path)
    try:
      # TODO: Only delete the file if it exists
      hdfs.delete_file_dir(pywebhdfs_file_path)
    except Exception as e:
      LOG.debug(e)
    with open(self._bulk_load_data_file.name) as readable_file:
      hdfs.create_file(pywebhdfs_file_path, readable_file)
    self._bulk_load_data_file.close()
    if self._bulk_load_non_text_table:
      if create_tables:
        self.create_table(self._bulk_load_non_text_table)
      self.execute('INSERT INTO TABLE %s SELECT * FROM %s'
          % (self._bulk_load_non_text_table.name, self._bulk_load_table.name))
      self.drop_table(self._bulk_load_table.name)
    self._bulk_load_data_file = None


#########################################
## Postgresql                          ##
#########################################
class PostgresqlDbConnection(DbConnection):

  TYPE_NAME_ALIASES = dict(DbConnection.TYPE_NAME_ALIASES)
  TYPE_NAME_ALIASES.update({
      'INTEGER': 'INT',
      'NUMERIC': 'DECIMAL',
      'REAL': 'FLOAT',
      'DOUBLE PRECISION': 'DOUBLE',
      'CHARACTER': 'CHAR',
      'CHARACTER VARYING': 'VARCHAR',
      'TIMESTAMP WITHOUT TIME ZONE': 'TIMESTAMP'})

  EXACT_TYPES_TO_SQL = dict(DbConnection.EXACT_TYPES_TO_SQL)
  EXACT_TYPES_TO_SQL.update({
      Double: 'DOUBLE PRECISION',
      Float: 'REAL',
      Int: 'INTEGER',
      Timestamp: 'TIMESTAMP WITHOUT TIME ZONE',
      TinyInt: 'SMALLINT'})

  def __init__(self, *args, **kwargs):
    DbConnection.__init__(self, *args, **kwargs)
    self._bulk_load_col_delimiter = '\t'

  @property
  def supports_kill_connection(self):
    return True

  def kill_connection(self):
    self.connection.cancel()

  #########################################
  # Databases                             #
  #########################################

  def make_list_db_names_sql(self):
    return 'SELECT datname FROM pg_database'

  #########################################
  # Tables                                #
  #########################################

  def make_list_table_names_sql(self):
    return '''
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' '''

  def make_describe_table_sql(self, table_name):
    # When doing a CREATE TABLE AS SELECT... a column  may end up with type "Numeric".
    # We'll assume that's a DOUBLE.
    return '''
        SELECT column_name,
               CASE data_type
                 WHEN 'character' THEN
                     data_type || '(' || character_maximum_length || ')'
                 WHEN 'character varying' THEN
                     data_type || '(' || character_maximum_length || ')'
                 WHEN 'numeric' THEN
                     data_type
                     || '('
                     || numeric_precision
                     || ', '
                     || numeric_scale
                     || ')'
                 ELSE data_type
               END data_type
        FROM information_schema.columns
        WHERE table_name = '%s'
        ORDER BY ordinal_position''' % \
        table_name

  def get_sql_for_data_type(self, data_type):
    if issubclass(data_type, String):
      return 'VARCHAR(%s)' % String.MAX
    return super(PostgresqlDbConnection, self).get_sql_for_data_type(data_type)

  #########################################
  # Data loading                          #
  #########################################

  def make_insert_sql_from_data(self, table, rows):
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
          sql += "TIMESTAMP '%s'" % val
        elif issubclass(col.type, Char):
          val = val.replace("'", "''")
          val = val.replace('\\', '\\\\')
          sql += "'%s'" % val
        else:
          sql += str(val)
      sql += ')'

    return sql

  def end_bulk_load_table(self, create_tables):
    super(PostgresqlDbConnection, self).end_bulk_load_table(create_tables)
    chmod(self._bulk_load_data_file.name, 0666)
    self.execute("COPY %s FROM '%s'"
        % (self._bulk_load_table.name, self._bulk_load_data_file.name))
    self._bulk_load_data_file.close()
    self._bulk_load_data_file = None


#########################################
## MySQL                               ##
#########################################
class MySQLDbConnection(DbConnection):

  def __init__(self, connector, connection, db_name=None):
    DbConnection.__init__(self, connector, connection, db_name=db_name)
    self.session_id = self.execute_and_fetchall('SELECT connection_id()')[0][0]

  @property
  def supports_kill_connection(self):
    return True

  def kill_connection(self):
    with self.connector.open_connection(db_name=self.db_name) as connection:
      connection.execute('KILL %s' % (self.session_id))

  #########################################
  # Tables                                #
  #########################################

  def describe_table(self, table_name):
    '''Return a Table with table and col names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_describe_table_sql(table_name))
    table = Table(table_name.lower())
    for row in rows:
      col_name, data_type = row[:2]
      if data_type == 'tinyint(1)':
        # Just assume this is a boolean...
        data_type = 'boolean'
      if 'decimal' not in data_type and '(' in data_type:
        # Strip the size of the data type
        data_type = data_type[:data_type.index('(')]
      table.cols.append(Column(table, col_name.lower(), self.parse_data_type(data_type)))
    return table

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

  def make_create_table_sql(self, table):
    table_sql = super(MySQLDbConnection).make_create_table_sql(table)
    table_sql += ' ENGINE = MYISAM'
    return table_sql

  #########################################
  # Data loading                          #
  #########################################

  def begin_bulk_load_table(self):
    raise NotImplementedError()


#########################################
## Oracle                              ##
#########################################
class OracleDbConnection(DbConnection):

  __imported_types = False
  __char_type = None
  __number_type = None

  @classmethod
  def type_converter(cls, cursor, name, default_type, size, precision, scale):
    if not cls.__imported_types:
      from cx_Oracle import FIXED_CHAR, NUMBER
      cls.__char_type = FIXED_CHAR
      cls.__number_type = NUMBER
      cls.__imported_types = True

    if default_type == cls.__char_type and size == 1:
      return cursor.var(str, 1, cursor.arraysize, outconverter=cls.boolean_converter)
    if default_type == cls.__number_type and scale:
      return cursor.var(str, 100, cursor.arraysize, outconverter=PyDecimal)

  @classmethod
  def boolean_converter(cls, val):
    if not val:
      return
    return val == 'T'

  @property
  def schema(self):
    return self.db_name or self.connector.user_name

  def create_cursor(self):
    cursor = DbConnection.create_cursor(self)
    if self.db_name and self.db_name != self.connector.user_name:
      cursor.execute('ALTER SESSION SET CURRENT_SCHEMA =  %s' % self.db_name)
    return cursor

  def make_list_table_names_sql(self):
    return 'SELECT table_name FROM user_tables'

  def drop_database(self, db_name):
    self.execute('DROP USER %s CASCADE' % db_name)

  def drop_db_if_exists(self, db_name):
    '''This should not be called from a connection to the database being dropped.'''
    try:
      self.drop_database(db_name)
    except Exception as e:
      if 'ORA-01918' not in str(e):   # Ignore if the user doesn't exist
        raise

  def create_database(self, db_name):
    self.execute(
        'CREATE USER %s IDENTIFIED BY %s DEFAULT TABLESPACE USERS' % (db_name, db_name))
    self.execute('GRANT ALL PRIVILEGES TO %s' % db_name)

  def make_describe_table_sql(self, table_name):
    # Recreate the data types as defined in the model
    return '''
        SELECT
          column_name,
          CASE
            WHEN data_type = 'NUMBER' AND data_scale = 0  THEN
                data_type || '(' || data_precision  || ')'
            WHEN data_type = 'NUMBER' THEN
                data_type || '(' || data_precision || ', ' || data_scale || ')'
            WHEN data_type IN ('CHAR', 'VARCHAR2') THEN
                data_type || '(' || data_length  || ')'
            WHEN data_type LIKE 'TIMESTAMP%%' THEN
                'TIMESTAMP'
            ELSE data_type
          END
        FROM all_tab_columns
        WHERE owner = '%s' AND table_name = '%s'
        ORDER BY column_id''' \
        % (self.schema.upper(), table_name.upper())

  #########################################
  # Data loading                          #
  #########################################

  def begin_bulk_load_table(self):
    raise NotImplementedError()
