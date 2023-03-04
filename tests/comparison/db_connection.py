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

'''This module is intended to standardize workflows when working with various databases
   such as Impala, Postgresql, etc. Even with pep-249 (DB API 2), workflows differ
   slightly. For example Postgresql does not allow changing databases from within a
   connection.

'''
from __future__ import absolute_import, division, print_function
from builtins import filter, map, range, zip
from future.utils import with_metaclass
import hashlib
import impala.dbapi
import re
import shelve
from abc import ABCMeta, abstractmethod
from contextlib import closing
from decimal import Decimal as PyDecimal
from itertools import combinations
from logging import getLogger
from os import symlink, unlink
from pyparsing import (
    alphanums,
    delimitedList,
    Forward,
    Group,
    Literal,
    nums,
    Suppress,
    Word)
from tempfile import gettempdir
from textwrap import dedent
from threading import Lock
from time import time

from tests.comparison.common import (
    ArrayColumn,
    Column,
    MapColumn,
    StructColumn,
    Table,
    TableExprList)
from tests.comparison.db_types import (
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

LOG = getLogger(__name__)

HIVE = "HIVE"
IMPALA = "IMPALA"
MYSQL = "MYSQL"
ORACLE = "ORACLE"
POSTGRESQL = "POSTGRESQL"


class DbCursor(object):
  '''Wraps a DB API 2 cursor to provide access to the related conn. This class
     implements the DB API 2 interface by delegation.

  '''

  @staticmethod
  def describe_common_tables(cursors):
    '''Find and return a TableExprList containing Table objects that the given conns
       have in common.
    '''
    common_table_names = None
    for cursor in cursors:
      table_names = set(cursor.list_table_names())
      if common_table_names is None:
        common_table_names = table_names
      else:
        common_table_names &= table_names
    common_table_names = sorted(common_table_names)

    tables = TableExprList()
    for table_name in common_table_names:
      common_table = None
      mismatch = False
      for cursor in cursors:
        table = cursor.describe_table(table_name)
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
        if common_table.primary_key_names != table.primary_key_names:
          LOG.debug(
              'Ignoring table {name} because of differing primary keys: '
              '{common_table_keys} vs. {table_keys}'.format(
                  name=table_name, common_table_keys=common_table.primary_key_names,
                  table_keys=table.primary_key_names))
          mismatch = True
          break
        for left, right in zip(common_table.cols, table.cols):
          if not (left.name == right.name and left.type == right.type):
            LOG.debug('Ignoring table %s. It has different columns %s vs %s.' %
                (table_name, left, right))
            mismatch = True
            break
        if mismatch:
          break
      if not mismatch:
        tables.append(common_table)

    return tables

  SQL_TYPE_PATTERN = re.compile(r'([^()]+)(\((\d+,? ?)*\))?')
  TYPE_NAME_ALIASES = \
      dict((type_.name().upper(), type_.name().upper()) for type_ in EXACT_TYPES)
  TYPES_BY_NAME = dict((type_.name().upper(), type_) for type_ in EXACT_TYPES)
  EXACT_TYPES_TO_SQL = dict((type_, type_.name().upper()) for type_ in EXACT_TYPES)

  @classmethod
  def make_insert_sql_from_data(cls, table, rows):
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

  def __init__(self, conn, cursor):
    self._conn = conn
    self._cursor = cursor

  def __getattr__(self, attr):
    return getattr(self._cursor, attr)

  def __setattr__(self, attr, value):
    # Transfer unknown attributes to the underlying cursor.
    if attr not in ["_conn", "_cursor"]:
      setattr(self._cursor, attr, value)
    object.__setattr__(self, attr, value)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close(quiet=True)

  @property
  def db_type(self):
    return self._conn.db_type

  @property
  def conn(self):
    return self._conn

  @property
  def db_name(self):
    return self._conn.db_name

  def execute(self, sql, *args, **kwargs):
    LOG.debug('%s: %s' % (self.db_type, sql))
    if self.conn.sql_log:
      self.conn.sql_log.write('\nQuery: %s' % sql)
    return self._cursor.execute(sql, *args, **kwargs)

  def execute_and_fetchall(self, sql, *args, **kwargs):
    self.execute(sql, *args, **kwargs)
    return self.fetchall()

  def close(self, quiet=False):
    try:
      self._cursor.close()
    except Exception as e:
      if quiet:
        LOG.debug('Error closing cursor: %s', e, exc_info=True)
      else:
        raise e

  def reconnect(self):
    self.conn.reconnect()
    self._cursor = self.conn.cursor()._cursor

  def list_db_names(self):
    '''Return a list of database names always in lowercase.'''
    rows = self.execute_and_fetchall(self.make_list_db_names_sql())
    return [row[0].lower() for row in rows]

  def make_list_db_names_sql(self):
    return 'SHOW DATABASES'

  def create_db(self, db_name):
    LOG.info("Creating database %s", db_name)
    db_name = db_name.lower()
    self.execute('CREATE DATABASE ' + db_name)

  def create_db_if_not_exists(self, db_name):
    LOG.info("Creating database %s", db_name)
    db_name = db_name.lower()
    self.execute('CREATE DATABASE IF NOT EXISTS ' + db_name)

  def drop_db_if_exists(self, db_name):
    '''This should not be called from a conn to the database being dropped.'''
    db_name = db_name.lower()
    if db_name not in self.list_db_names():
      return
    if self.db_name and self.db_name.lower() == db_name:
      raise Exception('Cannot drop database while still connected to it')
    self.drop_db(db_name)

  def drop_db(self, db_name):
    LOG.info("Dropping database %s", db_name)
    db_name = db_name.lower()
    self.execute('DROP DATABASE ' + db_name)

  def ensure_empty_db(self, db_name):
    self.drop_db_if_exists(db_name)
    self.create_db(db_name)

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
    t_integer = Literal('integer')
    t_smallint = Literal('smallint')
    t_tinyint = Literal('tinyint')
    t_boolean = Literal('boolean')
    t_string = Literal('string')
    t_timestamp = Literal('timestamp')
    t_timestamp_without_time_zone = Literal('timestamp without time zone')
    t_float = Literal('float')
    t_double = Literal('double')
    t_real = Literal('real')
    t_double_precision = Literal('double precision')

    t_decimal = Group(Literal('decimal') + LBRA + Word(nums) + COMMA + Word(nums) + RBRA)
    t_numeric = Group(Literal('numeric') + LBRA + Word(nums) + COMMA + Word(nums) + RBRA)
    t_char = Group(Literal('char') + LBRA + Word(nums) + RBRA)
    t_character = Group(Literal('character') + LBRA + Word(nums) + RBRA)
    t_varchar = (Group(Literal('varchar') + LBRA + Word(nums) + RBRA) |
        Literal('varchar'))
    t_character_varying = Group(Literal('character varying') + LBRA + Word(nums) + RBRA)

    t_struct = Forward()
    t_array = Forward()
    t_map = Forward()

    complex_type = (t_struct | t_array | t_map)

    any_type = (
        complex_type |
        t_bigint |
        t_int |
        t_integer |
        t_smallint |
        t_tinyint |
        t_boolean |
        t_string |
        t_timestamp |
        t_timestamp_without_time_zone |
        t_float |
        t_double |
        t_real |
        t_double_precision |
        t_decimal |
        t_numeric |
        t_char |
        t_character |
        t_character_varying |
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

    general_class = self.TYPE_NAME_ALIASES.get(col_type[0].upper())

    if general_class.upper() == 'DECIMAL':
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

    raise Exception('unable to parse: {0}, type: {1}'.format(col_name, col_type))

  def create_table_from_describe(self, table_name, describe_rows):
    primary_key_names = self._fetch_primary_key_names(table_name)
    table = Table(table_name.lower())
    for row in describe_rows:
      col_name, data_type = row[:2]
      col_type = self.parse_col_desc(data_type)
      col = self.create_column(col_name, col_type)
      col.is_primary_key = col_name in primary_key_names
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
    LOG.info('Creating table %s', table.name)
    if not table.cols:
      raise Exception('At least one col is required')
    table_sql = self.make_create_table_sql(table)
    LOG.debug(table_sql)
    self.execute(table_sql)
    LOG.debug('Created table %s', table.name)

  def make_create_table_sql(self, table):
    column_declarations = []
    primary_key_names = []
    for col in table.cols:
      if col.is_primary_key:
        null_constraint = ''
        primary_key_names.append(col.name)
      elif self.conn.data_types_are_implictly_nullable:
        null_constraint = ''
      else:
        null_constraint = ' NULL'
      column_declaration = '{name} {col_type}{null_constraint}'.format(
          name=col.name, col_type=self.get_sql_for_data_type(col.exact_type),
          null_constraint=null_constraint)
      column_declarations.append(column_declaration)

    if primary_key_names:
      primary_key_constraint = ', PRIMARY KEY ({keys})'.format(
          keys=', '.join(primary_key_names))
    else:
      primary_key_constraint = ''

    create_table = ('CREATE TABLE {table_name} ('
                    '{all_columns}'
                    '{primary_key_constraint}'
                    ')'.format(
                        table_name=table.name, all_columns=', '.join(column_declarations),
                        primary_key_constraint=primary_key_constraint))
    return create_table

  def get_sql_for_data_type(self, data_type):
    if issubclass(data_type, VarChar):
      return 'VARCHAR(%s)' % data_type.MAX
    if issubclass(data_type, Char):
      return 'CHAR(%s)' % data_type.MAX
    if issubclass(data_type, Decimal):
      return 'DECIMAL(%s, %s)' % (data_type.MAX_DIGITS, data_type.MAX_FRACTIONAL_DIGITS)
    return self.EXACT_TYPES_TO_SQL[data_type]

  def drop_table(self, table_name, if_exists=True):
    LOG.info('Dropping table %s', table_name)
    self.execute('DROP TABLE IF EXISTS ' + table_name.lower())

  def drop_view(self, view_name, if_exists=True):
    LOG.info('Dropping view %s', view_name)
    self.execute('DROP VIEW IF EXISTS ' + view_name.lower())

  def index_table(self, table_name):
    LOG.info('Indexing table %s', table_name)
    table = self.describe_table(table_name)
    for col in table.cols:
      index_name = '%s_%s' % (table_name, col.name)
      if self.db_name:
        index_name = '%s_%s' % (self.db_name, index_name)
      # Older versions of Postgres require index name to be included in the statement.
      # In the past, there were some failures when trying to index a table with long
      # column names because there is a limit to how long index name is allowed to be,
      # This is why index_name is hashed and truncated to 10 characters.
      index_name = 'ind_' + hashlib.sha256(index_name).hexdigest()[:10]
      self.execute('CREATE INDEX %s ON %s(%s)' % (index_name, table_name, col.name))

  def search_for_unique_cols(self, table=None, table_name=None, depth=2):
    if not table:
      table = self.describe_table(table_name)
    sql_templ = 'SELECT COUNT(*) FROM %s GROUP BY %%s HAVING COUNT(*) > 1' % table.name
    unique_cols = list()
    for current_depth in range(1, depth + 1):
      for cols in combinations(table.cols, current_depth):   # redundant combos excluded
        cols = set(cols)
        if any(filter(lambda unique_subset: unique_subset < cols, unique_cols)):
          # cols contains a combo known to be unique
          continue
        col_names = ', '.join(col.name for col in cols)
        sql = sql_templ % col_names
        LOG.debug('Checking column combo (%s) for uniqueness' % col_names)
        self.execute(sql)
        if not self.fetchone():
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

  def _fetch_primary_key_names(self, table_name):
    """
    This must return a tuple of strings representing the primary keys of table_name,
    or an empty tuple if there are no primary keys.
    """
    # This is the base method. Since we haven't tested this on Oracle or Mysql or plan
    # to implement this for those databases, the base method needs to return an empty
    # tuple.
    return ()


class DbConnection(with_metaclass(ABCMeta, object)):

  LOCK = Lock()

  PORT = None
  USER_NAME = None
  PASSWORD = None

  _CURSOR_CLASS = DbCursor

  def __init__(self, host_name="localhost", port=None, user_name=None, password=None,
      db_name=None, log_sql=False):
    self._host_name = host_name
    self._port = port or self.PORT
    self._user_name = user_name or self.USER_NAME
    self._password = password or self.PASSWORD
    self.db_name = db_name
    self._conn = None
    self._connect()

    if log_sql:
      with DbConnection.LOCK:
        sql_log_path = gettempdir() + '/sql_log_%s_%s.sql' \
              % (self.db_type.lower(), time())
        self.sql_log = open(sql_log_path, 'w')
        link = gettempdir() + '/sql_log_%s.sql' % self.db_type.lower()
        try:
          unlink(link)
        except OSError as e:
          if 'No such file' not in str(e):
            raise e
        try:
          symlink(sql_log_path, link)
        except OSError as e:
          raise e
    else:
      self.sql_log = None

  def _clone(self, db_name, **kwargs):
    return type(self)(host_name=self._host_name, port=self._port,
        user_name=self._user_name, password=self._password, db_name=db_name, **kwargs)

  def clone(self, db_name):
    return self._clone(db_name)

  def __getattr__(self, attr):
    if attr == "_conn":
      raise AttributeError()
    return getattr(self._conn, attr)

  def __setattr__(self, attr, value):
    _conn = getattr(self, "_conn", None)
    if not _conn or not hasattr(_conn, attr):
      object.__setattr__(self, attr, value)
    else:
      setattr(self._conn, attr, value)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close(quiet=True)

  @property
  def db_type(self):
    return self._DB_TYPE

  @abstractmethod
  def _connect(self):
    pass

  def reconnect(self):
    self.close(quiet=True)
    self._connect()

  def close(self, quiet=False):
    '''Close the underlying conn.'''
    if not self._conn:
      return
    try:
      self._conn.close()
      self._conn = None
    except Exception as e:
      if quiet:
        LOG.debug('Error closing connection: %s' % e)
      else:
        raise e

  def cursor(self):
    return self._CURSOR_CLASS(self, self._conn.cursor())

  @property
  def supports_kill(self):
    return False

  def kill(self):
    '''Kill the current connection and any currently running queries associated with the
       conn.
    '''
    raise Exception('Killing connection is not supported')

  @property
  def supports_index_creation(self):
    return True

  @property
  def data_types_are_implictly_nullable(self):
    return False


class ImpalaCursor(DbCursor):

  PK_SEARCH_PATTERN = re.compile('PRIMARY KEY \((?P<keys>.*?)\)')
  STORAGE_FORMATS_WITH_PRIMARY_KEYS = ('KUDU',)

  @classmethod
  def make_insert_sql_from_data(cls, table, rows):
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
  def cluster(self):
    return self.conn.cluster

  def _fetch_primary_key_names(self, table_name):
    self.execute("SHOW CREATE TABLE {0}".format(table_name))
    # This returns 1 column with 1 multiline string row, resembling:
    #
    # CREATE TABLE db.table (
    #   pk1 BIGINT,
    #   pk2 BIGINT,
    #   col BIGINT,
    #   PRIMARY KEY (pk1, pk2)
    # )
    #
    # Even a 1-column primary key will be shown as a PRIMARY KEY constraint, like:
    #
    # CREATE TABLE db.table (
    #   pk1 BIGINT,
    #   col BIGINT,
    #   PRIMARY KEY (pk1)
    # )
    (raw_result,) = self.fetchone()
    search_result = ImpalaCursor.PK_SEARCH_PATTERN.search(raw_result)
    if search_result is None:
      return ()
    else:
      return tuple(search_result.group("keys").split(", "))

  def invalidate_metadata(self, table_name=None):
    self.execute("INVALIDATE METADATA %s" % (table_name or ""))

  def drop_db(self, db_name):
    '''This should not be called when connected to the database being dropped.'''
    LOG.info("Dropping database %s", db_name)
    self.execute('DROP DATABASE %s CASCADE' % db_name)

  def compute_stats(self, table_name=None):
    if table_name:
      self.execute("COMPUTE STATS %s" % table_name)
    else:
      for table_name in self.list_table_names():
        self.execute("COMPUTE STATS %s" % table_name)

  def create_table(self, table):
    # The Hive metastore has a limit to the amount of schema it can store inline.
    # Beyond this limit, the schema needs to be stored in HDFS and Hive is given a
    # URL instead.
    if table.storage_format == "AVRO" and not table.schema_location \
        and len(table.get_avro_schema()) > 4000:
      self.upload_avro_schema(table)
    super(ImpalaCursor, self).create_table(table)

  def upload_avro_schema(self, table):
    self.ensure_schema_location(table)
    avro_schema = table.get_avro_schema()
    self.cluster.hdfs.create_client().write(
        table.schema_location, data=avro_schema, overwrite=True)

  def ensure_schema_location(self, table):
    if not table.schema_location:
      db_name = self.conn.db_name or "default"
      table.schema_location = '%s/%s.db/%s.avsc' % (
          self.cluster.hive.warehouse_dir, db_name, table.name)

  def ensure_storage_location(self, table):
    if not table.storage_location:
      db_name = self.conn.db_name or "default"
      table.storage_location = '%s/%s.db/data/%s' % (
          self.cluster.hive.warehouse_dir, db_name, table.name)

  def make_create_table_sql(self, table):
    sql = super(ImpalaCursor, self).make_create_table_sql(table)

    if table.primary_keys:
      if table.storage_format in ImpalaCursor.STORAGE_FORMATS_WITH_PRIMARY_KEYS:
        # IMPALA-4424 adds support for parametrizing the partitions; for now, on our
        # small scale, this is ok, especially since the model is to migrate tables from
        # Impala into Postgres anyway. 3 was chosen for the buckets because our
        # minicluster tends to have 3 tablet servers, but otherwise it's arbitrary and
        # provides valid syntax for creating Kudu tables in Impala.
        sql += '\nPARTITION BY HASH ({col}) PARTITIONS 3'.format(
            col=table.primary_key_names[0])
      else:
        raise Exception(
            'table representation has primary keys {keys} but is not in a format that '
            'supports them: {storage_format}'.format(
                keys=str(table.primary_key_names),
                storage_format=table.storage_format))
    elif table.storage_format in ImpalaCursor.STORAGE_FORMATS_WITH_PRIMARY_KEYS:
      raise Exception(
          'table representation has storage format {storage_format} '
          'but does not have any primary keys'.format(
              storage_format=table.storage_format))

    if table.storage_format != 'TEXTFILE':
      sql += "\nSTORED AS " + table.storage_format
    if table.storage_location:
      sql = sql.replace("CREATE TABLE", "CREATE EXTERNAL TABLE")
      sql += "\nLOCATION '%s'" % table.storage_location
    if table.storage_format == 'AVRO':
      if table.schema_location:
        sql += "\nTBLPROPERTIES ('avro.schema.url' = '%s')" % table.schema_location
      else:
        avro_schema = table.get_avro_schema()
        if len(avro_schema) > 4000:
          raise Exception("Avro schema exceeds 4000 character limit. Create a file"
              " containing the schema instead and set 'table.schema_location'.")
        sql += "\nTBLPROPERTIES ('avro.schema.literal' = '%s')" % avro_schema
    return sql

  def get_sql_for_data_type(self, data_type):
    if issubclass(data_type, String):
      return 'STRING'
    return super(ImpalaCursor, self).get_sql_for_data_type(data_type)

  def close(self, quiet=False):
    try:
      # Explicitly close the operation to avoid issues like IMPALA-2562.
      # This can be remove if https://github.com/cloudera/impyla/pull/142 is merged.
      self._cursor.close_operation()
      self._cursor.close()
    except Exception as e:
      if quiet:
        LOG.debug('Error closing cursor: %s', e, exc_info=True)
      else:
        raise e


class ImpalaConnection(DbConnection):

  PORT = 21050   # For HS2

  _DB_TYPE = IMPALA
  _CURSOR_CLASS = ImpalaCursor
  _KERBEROS_SERVICE_NAME = 'impala'
  _NON_KERBEROS_AUTH_MECH = 'NOSASL'

  def __init__(self, use_kerberos=False, use_ssl=False, ca_cert=None, **kwargs):
    self._use_kerberos = use_kerberos
    self.cluster = None
    self._use_ssl = use_ssl
    self._ca_cert = ca_cert
    DbConnection.__init__(self, **kwargs)

  def clone(self, db_name):
    clone = self._clone(db_name, use_kerberos=self._use_kerberos)
    clone.cluster = self.cluster
    return clone

  @property
  def data_types_are_implictly_nullable(self):
    return True

  @property
  def supports_index_creation(self):
    return False

  def cursor(self):
    cursor = super(ImpalaConnection, self).cursor()
    cursor.arraysize = 1024   # Try to match the default batch size
    return cursor

  def _connect(self):
    self._conn = impala.dbapi.connect(
        host=self._host_name,
        port=self._port,
        user=self._user_name,
        password=self._password,
        database=self.db_name,
        timeout=(60 * 60),
        auth_mechanism=('GSSAPI' if self._use_kerberos else self._NON_KERBEROS_AUTH_MECH),
        kerberos_service_name=self._KERBEROS_SERVICE_NAME,
        use_ssl=self._use_ssl,
        ca_cert=self._ca_cert)


class HiveCursor(ImpalaCursor):

  def invalidate_metadata(self, table_name=None):
    # There is no equivalent of "INVALIDATE METADATA" in Hive
    pass

  def compute_stats(self, table_name=None):
    if table_name:
      self.execute("ANALYZE TABLE %s COMPUTE STATISTICS" % table_name)
      self.execute("ANALYZE TABLE %s COMPUTE STATISTICS FOR COLUMNS" % table_name)
    else:
      for table_name in self.list_table_names():
        self.execute("ANALYZE TABLE %s COMPUTE STATISTICS" % table_name)
        self.execute("ANALYZE TABLE %s COMPUTE STATISTICS FOR COLUMNS" % table_name)


class HiveConnection(ImpalaConnection):

  PORT = 11050

  _DB_TYPE = HIVE
  _CURSOR_CLASS = HiveCursor
  _KERBEROS_SERVICE_NAME = 'hive'
  _NON_KERBEROS_AUTH_MECH = 'PLAIN'


class PostgresqlCursor(DbCursor):

  TYPE_NAME_ALIASES = dict(DbCursor.TYPE_NAME_ALIASES)
  TYPE_NAME_ALIASES.update({
      'INTEGER': 'INT',
      'NUMERIC': 'DECIMAL',
      'REAL': 'FLOAT',
      'DOUBLE PRECISION': 'DOUBLE',
      'CHARACTER': 'CHAR',
      'CHARACTER VARYING': 'VARCHAR',
      'TIMESTAMP WITHOUT TIME ZONE': 'TIMESTAMP'})

  EXACT_TYPES_TO_SQL = dict(DbCursor.EXACT_TYPES_TO_SQL)
  EXACT_TYPES_TO_SQL.update({
      Double: 'DOUBLE PRECISION',
      Float: 'REAL',
      Int: 'INTEGER',
      Timestamp: 'TIMESTAMP WITHOUT TIME ZONE',
      TinyInt: 'SMALLINT'})

  @classmethod
  def make_insert_sql_from_data(cls, table, rows):
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

  def make_list_db_names_sql(self):
    return 'SELECT datname FROM pg_database'

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
    return super(PostgresqlCursor, self).get_sql_for_data_type(data_type)

  def _fetch_primary_key_names(self, table_name):
    # see:
    # https://www.postgresql.org/docs/9.5/static/infoschema-key-column-usage.html
    # https://www.postgresql.org/docs/9.5/static/infoschema-table-constraints.html
    sql = dedent('''
        SELECT
          key_cols.column_name AS column_name
        FROM
          information_schema.key_column_usage key_cols,
          information_schema.table_constraints table_constraints
        WHERE
          key_cols.constraint_catalog = table_constraints.constraint_catalog AND
          key_cols.table_name = table_constraints.table_name AND
          key_cols.constraint_name = table_constraints.constraint_name AND
          table_constraints.constraint_type = 'PRIMARY KEY' AND
          key_cols.table_name = '{table_name}'
        ORDER BY key_cols.ordinal_position'''.format(table_name=table_name))
    self.execute(sql)
    rows = self.fetchall()
    return tuple(row[0] for row in rows)


class PostgresqlConnection(DbConnection):

  PORT = 5432
  USER_NAME = "postgres"

  _DB_TYPE = POSTGRESQL
  _CURSOR_CLASS = PostgresqlCursor

  @property
  def supports_kill(self):
    return True

  def kill(self):
    self._conn.cancel()

  def _connect(self):
    try:
      import psycopg2
    except ImportError as e:
      if "No module named psycopg2" not in str(e):
        raise e
      import os
      import subprocess
      from tests.util.shell_util import shell
      LOG.info("psycopg2 module not found; attempting to install it...")
      pip_path = os.path.join(os.environ["IMPALA_HOME"], "infra", "python", "env",
          "bin", "pip")
      try:
        shell(pip_path + " install psycopg2", stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT)
        LOG.info("psycopg2 installation complete.")
      except Exception as e:
        LOG.error("psycopg2 installation failed. Try installing python-dev and"
            " libpq-dev then try again.")
        raise e
      import psycopg2
    self._conn = psycopg2.connect(
        host=self._host_name,
        port=self._port,
        user=self._user_name,
        password=self._password,
        database=self.db_name)
    self._conn.autocommit = True


class MySQLConnection(DbConnection):

  PORT = 3306
  USER_NAME = "root"

  def __init__(self, client, conn, db_name=None):
    DbConnection.__init__(self, client, conn, db_name=db_name)
    self._session_id = self.execute_and_fetchall('SELECT connection_id()')[0][0]

  def _connect(self):
    try:
      import MySQLdb
    except Exception:
      print('Error importing MySQLdb. Please make sure it is installed. '
          'See the README for details.')
      raise
    self._conn = MySQLdb.connect(
        host=self._host_name,
        port=self._port,
        user=self._user_name,
        passwd=self._password,
        db=self.db_name)
    self._conn.autocommit = True

  @property
  def supports_kill_connection(self):
    return True

  def kill_connection(self):
    with self._client.open_connection(db_name=self.db_name) as conn:
      conn.execute('KILL %s' % (self._session_id))


class MySQLCursor(DbCursor):

  def describe_table(self, table_name):
    '''Return a Table with table and col names always in lowercase.'''
    rows = self.conn.execute_and_fetchall(
        self.make_describe_table_sql(table_name))
    table = Table(table_name.lower())
    cols = table.cols   # This is a copy
    for row in rows:
      col_name, data_type = row[:2]
      if data_type == 'tinyint(1)':
        # Just assume this is a boolean...
        data_type = 'boolean'
      if 'decimal' not in data_type and '(' in data_type:
        # Strip the size of the data type
        data_type = data_type[:data_type.index('(')]
      cols.append(Column(table, col_name.lower(), self.parse_data_type(data_type)))
    table.cols = cols
    return table

  def index_table(self, table_name):
    table = self.describe_table(table_name)
    with self.conn.open_cursor() as cursor:
      for col in table.cols:
        try:
          cursor.execute('ALTER TABLE %s ADD INDEX (%s)' % (table_name, col.name))
        except Exception as e:
          if 'Incorrect index name' not in str(e):
            raise
          # Some sort of MySQL bug...
          LOG.warn('Could not create index on %s.%s: %s' % (table_name, col.name, e))

  def make_create_table_sql(self, table):
    table_sql = super(MySQLConnection, self).make_create_table_sql(table)
    table_sql += ' ENGINE = MYISAM'
    return table_sql


class OracleCursor(DbCursor):

  def make_list_table_names_sql(self):
    return 'SELECT table_name FROM user_tables'

  def drop_db(self, db_name):
    self.execute('DROP USER %s CASCADE' % db_name)

  def drop_db_if_exists(self, db_name):
    '''This should not be called from a conn to the database being dropped.'''
    try:
      self.drop_db(db_name)
    except Exception as e:
      if 'ORA-01918' not in str(e):   # Ignore if the user doesn't exist
        raise

  def create_db(self, db_name):
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


class OracleConnection(DbConnection):

  PORT = 1521
  USER_NAME = 'system'
  PASSWORD = 'oracle'

  _CURSOR_CLASS = OracleCursor

  def __init__(self, service='XE', **kwargs):
    self._service = service
    DbConnection.__init__(self, **kwargs)
    self._conn.outputtypehandler = OracleTypeConverter.convert_type
    self._conn.autocommit = True

  def clone(self, db_name):
    return self._clone(db_name, service=self._service)

  @property
  def schema(self):
    return self.db_name or self._user_name

  def _connect(self):
    try:
      import cx_Oracle
    except:
      print('Error importing cx_Oracle. Please make sure it is installed. '
          'See the README for details.')
      raise
    self._conn = cx_Oracle.connect('%(user)s/%(password)s@%(host)s:%(port)s/%(service)s'
        % (self._user_name, self._password, self._host_name, self._port, self._service))

  def cursor(self):
    cursor = super(OracleConnection, self).cursor()
    if self.db_name and self.db_name != self._user_name:
      cursor.execute('ALTER SESSION SET CURRENT_SCHEMA =  %s' % self.db_name)
    return cursor


class OracleTypeConverter(object):

  __imported_types = False
  __char_type = None
  __number_type = None

  @classmethod
  def convert_type(cls, cursor, name, default_type, size, precision, scale):
    if not cls.__imported_types:
      from cx_Oracle import FIXED_CHAR, NUMBER
      cls.__char_type = FIXED_CHAR
      cls.__number_type = NUMBER
      cls.__imported_types = True

    if default_type == cls.__char_type and size == 1:
      return cursor.var(str, 1, cursor.arraysize, outconverter=cls.convert_boolean)
    if default_type == cls.__number_type and scale:
      return cursor.var(str, 100, cursor.arraysize, outconverter=PyDecimal)

  @classmethod
  def convert_boolean(cls, val):
    if not val:
      return
    return val == 'T'
