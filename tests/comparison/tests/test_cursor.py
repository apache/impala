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


from __future__ import absolute_import, division, print_function
import pytest

from tests.comparison.db_connection import ImpalaConnection, PostgresqlConnection
from tests.comparison.db_connection import ImpalaCursor, PostgresqlCursor
from tests.comparison.db_types import Char, Int
from fake_query import FakeColumn, FakeTable


# These are fake connection objects that provide some data needed to "unit test" infra
# cursor implementations.
class FakeImpalaConnection(ImpalaConnection):
  def _connect(self):
    pass


class FakePostgresqlConnection(PostgresqlConnection):
  def _connect(self):
    pass


@pytest.fixture
def unittest_cursor_map(request):
  """
  Given a dictionary parameter to the fixture with the class key/value set as a cursor
  class, return a connectionless, "fake" cursor. The cursor is accessible via the
  fixture's 'cursor' key in the test methods.

  The fixture parameter dictionary can be annotated with other arbitrary key/values that
  describe expectations for that particular cursor and test case.
  """
  connection_map = {
      ImpalaCursor: FakeImpalaConnection,
      PostgresqlCursor: FakePostgresqlConnection
  }
  cursor_class = request.param['class']
  connection_class = connection_map[cursor_class]
  fake_connection_obj = connection_class()
  request.param['cursor'] = cursor_class(fake_connection_obj, None)
  return request.param


@pytest.yield_fixture
def postgresql_cursor():
  """
  Yield a PostgresqlCursor object needed for test infra system tests.
  """
  with PostgresqlConnection() as conn:
    cursor = conn.cursor()
    try:
      yield cursor
    finally:
      cursor.close()


@pytest.mark.parametrize(
    'unittest_cursor_map', [
        {'class': ImpalaCursor,
         'sql': {'two_cols': 'CREATE TABLE two_cols (col1 INT, col2 INT)',
                 'one_pk': 'CREATE TABLE one_pk (col1 INT, col2 INT, '
                           'PRIMARY KEY (col1))\n'
                           'PARTITION BY HASH (col1) PARTITIONS 3\n'
                           'STORED AS KUDU',
                 'three_pks': 'CREATE TABLE three_pks (col1 INT, col2 CHAR(255), '
                              'col3 INT, col4 INT, PRIMARY KEY (col1, col2, col3))\n'
                              'PARTITION BY HASH (col1) PARTITIONS 3\n'
                              'STORED AS KUDU'}},
        {'class': PostgresqlCursor,
         'sql': {'two_cols': 'CREATE TABLE two_cols (col1 INTEGER NULL, '
                             'col2 INTEGER NULL)',
                 'one_pk': 'CREATE TABLE one_pk (col1 INTEGER, col2 INTEGER NULL, '
                           'PRIMARY KEY (col1))',
                 'three_pks': 'CREATE TABLE three_pks (col1 INTEGER, col2 CHAR(255), '
                              'col3 INTEGER, col4 INTEGER NULL, '
                              'PRIMARY KEY (col1, col2, col3))'}},
    ], indirect=True)
@pytest.mark.parametrize(
    'table_data', [
        {'descr': 'two_cols',
         'data': FakeTable('two_cols', (FakeColumn('col1', Int),
                                        FakeColumn('col2', Int)))},
        {'descr': 'one_pk',
         'data': FakeTable('one_pk', (FakeColumn('col1', Int, is_primary_key=True),
                                      FakeColumn('col2', Int)),
                           storage_format='KUDU')},
        {'descr': 'three_pks',
         'data': FakeTable('three_pks', (FakeColumn('col1', Int, is_primary_key=True),
                                         FakeColumn('col2', Char, is_primary_key=True),
                                         FakeColumn('col3', Int, is_primary_key=True),
                                         FakeColumn('col4', Int)),
                           storage_format='KUDU')},
    ])
def test_make_create_table_sql(unittest_cursor_map, table_data):
  """
  Test that for a given Table representation, both the Impala and PostgreSQL cursor
  objects generate valid SQL for creating their tables.
  """
  which_sql = table_data['descr']
  table_object = table_data['data']
  expected_sql = unittest_cursor_map['sql'][which_sql]
  cursor = unittest_cursor_map['cursor']
  assert expected_sql == cursor.make_create_table_sql(table_object)


@pytest.mark.parametrize(
    'unittest_cursor_map', [
        {'class': ImpalaCursor,
         'exceptions_text': {
             'text_with_pk': "table representation has primary keys ('col1',) but is "
                             'not in a format that supports them: TEXTFILE',
             'kudu_without_pk': 'table representation has storage format KUDU but does '
                                'not have any primary keys'}}
    ], indirect=True)
@pytest.mark.parametrize(
    'table_data', [
        {'descr': 'text_with_pk',
         'data': FakeTable('two_cols', (FakeColumn('col1', Int, is_primary_key=True),
                                        FakeColumn('col2', Int)))},
        {'descr': 'kudu_without_pk',
         'data': FakeTable('two_cols', (FakeColumn('col1', Int),
                                        FakeColumn('col2', Int)),
                           storage_format='KUDU')},

    ])
def test_invalid_tables(unittest_cursor_map, table_data):
  """
  Test that for a given invalid Table representation considering primary keys and
  storage formats, the ImpalaCursor properly fails to generate the SQL needed to create
  the table.
  """
  table_object = table_data['data']
  cursor = unittest_cursor_map['cursor']
  which_test = table_data['descr']
  expected_exception_text = unittest_cursor_map['exceptions_text'][which_test]
  # http://doc.pytest.org/en/latest/assert.html#assertions-about-expected-exceptions
  with pytest.raises(Exception) as excinfo:
    cursor.make_create_table_sql(table_object)
  assert str(excinfo.value) == expected_exception_text


# TODO: It's not worth it now, but in the future, if we need to interact with postgresql
# with a bunch of infra tests, we should consider some more sophisticated data
# structures and fixtures to handle things in parallel and to reduce code reuse.
@pytest.mark.parametrize(
    'sql_primary_key_map', [
        {'sql': 'CREATE TABLE mytable (col1 INTEGER NULL, col2 INTEGER NULL)',
         'primary_keys': ()},
        {'sql': ('CREATE TABLE mytable (col1 INTEGER, col2 INTEGER NULL, '
                 'PRIMARY KEY (col1))'),
         'primary_keys': ('col1',)},
        {'sql': ('CREATE TABLE mytable (col1 INTEGER, col2 CHAR(255), col3 INTEGER, '
                 'col4 INTEGER NULL, PRIMARY KEY (col1, col2, col3))'),
         'primary_keys': ('col1', 'col2', 'col3')}
    ])
def test_postgres_table_reading(postgresql_cursor, sql_primary_key_map):
  """
  Test that when a postgres table is read by the Postgresql cursor, the Table object
  contains the correct primary keys. This tests interacts with the local PostgreSQL
  database that's part of the minicluster.
  """
  try:
    postgresql_cursor.execute('DROP TABLE IF EXISTS mytable')
    postgresql_cursor.execute(sql_primary_key_map['sql'])
    table = postgresql_cursor.describe_table('mytable')
    assert table.primary_key_names == sql_primary_key_map['primary_keys']
  finally:
    postgresql_cursor.execute('DROP TABLE mytable')
