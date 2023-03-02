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
from inspect import getmro
from logging import getLogger
from re import sub
from sqlparse import format

from tests.comparison.common import StructColumn, CollectionColumn
from tests.comparison.db_types import (
    Char,
    Decimal,
    Double,
    Float,
    Int,
    String,
    Timestamp,
    TinyInt,
    VarChar)
from tests.comparison.query import InsertClause, Query
from tests.comparison.query_flattener import QueryFlattener

LOG = getLogger(__name__)


class SqlWriter(object):
  '''Subclasses of SQLWriter will take a Query and provide the SQL representation for a
     specific database such as Impala or MySQL. The SqlWriter.create([dialect=])
     factory method may be used instead of specifying the concrete class.

  '''

  @staticmethod
  def create(dialect='impala', nulls_order_asc='DEFAULT'):
    '''Create and return a new SqlWriter appropriate for the given sql dialect. "dialect"
       refers to database specific deviations of sql, and the val should be one of
       "IMPALA", "MYSQL", or "POSTGRESQL".
    '''
    dialect = dialect.upper()
    if dialect == 'IMPALA':
      return ImpalaSqlWriter(nulls_order_asc)
    if dialect == 'MYSQL':
      return MySQLSqlWriter(nulls_order_asc)
    if dialect == 'ORACLE':
      return OracleSqlWriter(nulls_order_asc)
    if dialect == 'POSTGRESQL':
      return PostgresqlSqlWriter(nulls_order_asc)
    if dialect == 'HIVE':
      return HiveSqlWriter(nulls_order_asc)
    raise Exception('Unknown dialect: %s' % dialect)

  def __init__(self, nulls_order_asc):
    if nulls_order_asc not in ('BEFORE', 'AFTER', 'DEFAULT'):
      raise Exception('Unknown nulls order: %s' % nulls_order_asc)
    self.nulls_order_asc = nulls_order_asc

    # Functions that don't follow the usual call syntax of foo(bar, baz) can be listed
    # here. Parenthesis were added everywhere to avoid problems with operator precedence.
    # TODO: Account for operator precedence...
    self.operator_funcs = {
        'And': '({0}) AND ({1})',
        'Or': '({0}) OR ({1})',
        'Plus': '({0}) + ({1})',
        'Minus': '({0}) - ({1})',
        'Multiply': '({0}) * ({1})',
        'Divide': '({0}) / ({1})',
        'Equals': '({0}) = ({1})',
        'NotEquals': '({0}) != ({1})',
        'IsNotDistinctFrom': '({0}) IS NOT DISTINCT FROM ({1})',
        # If a database supports the operator version of 'IS NOT DISTINCT FROM', it
        # should overwrite the value of 'IsNotDistinctFromOp'.
        'IsNotDistinctFromOp': '({0}) IS NOT DISTINCT FROM ({1})',
        'IsDistinctFrom': '({0}) IS DISTINCT FROM ({1})',
        'LessThan': '({0}) < ({1})',
        'GreaterThan': '({0}) > ({1})',
        'LessThanOrEquals': '({0}) <= ({1})',
        'GreaterThanOrEquals': '({0}) >= ({1})',
        'IsNull': '({0}) IS NULL',
        'IsNotNull': '({0}) IS NOT NULL'}

  def write_query(self, statement, pretty=False):
    """
    Return SQL as a string for the given query.

    If "pretty" is True, the SQL will be formatted (though not very well) with new
    lines and indentation.
    """
    sql = self._write(statement)
    if pretty:
      sql = self.make_pretty_sql(sql)
    return sql

  def _write_query(self, query):
    """
    Taking in a Query object with some attributes set, return a string
    representation of the query in the correct dialect.

    This is just another dispatch destination of self._write(). When
    self._write(Query) is called, that's dispatched to self._write_query(Query)
    """
    sql = list()
    # Write out each section in the proper order
    for clause in (
        query.with_clause,
        query.select_clause,
        query.from_clause,
        query.where_clause,
        query.group_by_clause,
        query.having_clause,
        query.union_clause,
        query.order_by_clause,
        query.limit_clause
    ):
      if clause:
        sql.append(self._write(clause))
    sql = '\n'.join(sql)
    return sql

  def _write_insert_statement(self, insert_statement):
    """
    Taking in a InsertStatement object with some attributes set, return a string
    representation of the query in the correct dialect.
    """
    sql = list()

    if insert_statement.with_clause:
      sql.append(self._write(insert_statement.with_clause))

    if insert_statement.insert_clause:
      sql.append(self._write(insert_statement.insert_clause))
    else:
      raise Exception('InsertStatement is missing insert_clause attribute')

    if insert_statement.select_query and not insert_statement.values_clause:
      sql.append(self._write(insert_statement.select_query))
    elif not insert_statement.select_query and insert_statement.values_clause:
      sql.append(self._write(insert_statement.values_clause))
    else:
      raise Exception('InsertStatement must have a select_query xor a values clause')

    sql = '\n'.join(sql)
    return sql

  def make_pretty_sql(self, sql):
    try:
      sql = format(sql, reindent=True)
    except Exception as e:
      LOG.warn('Unable to format sql: %s', e)
    return sql

  def write_create_table_as(self, query, name, pretty=False):
    return 'CREATE TABLE %s AS %s' % (name, self.write_query(query, pretty=pretty))

  def write_create_view(self, query, name, pretty=False):
    return 'CREATE VIEW %s AS %s' % (name, self.write_query(query, pretty=pretty))

  def _write_with_clause(self, with_clause):
    return 'WITH ' + ',\n'.join('%s AS (%s)' % (view.identifier, self._write(view.query))
                                for view in with_clause.with_clause_inline_views)

  def _write_select_clause(self, select_clause):
    if hasattr(select_clause, 'star_prefix'):
      # This is a little hack to get query flattening to work (look at query_flattener.py
      # TODO: Add proper support for SELECT *, and SELECT table_name.*
      return 'SELECT %s.*' % select_clause.star_prefix
    sql = 'SELECT'
    if select_clause.distinct:
      sql += ' DISTINCT'
    sql += '\n' + ',\n'.join(self._write(item) for item in select_clause.items)
    return sql

  def _write_select_item(self, select_item):
    if select_item.alias:
      return '{0} AS {1}'.format(self._write(select_item.val_expr), select_item.alias)
    else:
      return self._write(select_item.val_expr)

  def _write_struct_column(self, struct_col):
    if isinstance(struct_col.owner, StructColumn) or \
       (isinstance(struct_col.owner, CollectionColumn) and not struct_col.owner.alias):
      return '%s.%s' % (self._write(struct_col.owner), struct_col.name)
    else:
      return '%s.%s' % (struct_col.owner.identifier, struct_col.name)

  def _write_collection_column(self, collection_col):
    if isinstance(collection_col.owner, (StructColumn, CollectionColumn)) and \
       not collection_col.owner.alias:
      if collection_col.alias:
        return '%s.%s %s' % (
            self._write(collection_col.owner),
            collection_col.name,
            collection_col.alias)
      else:
        return '%s.%s' % (self._write(collection_col.owner), collection_col.name)
    else:
      if collection_col.alias:
        return '%s.%s %s' % (
            collection_col.owner.identifier,
            collection_col.name,
            collection_col.alias)
      else:
        return '%s.%s' % (collection_col.owner.identifier, collection_col.name)

  def _write_column(self, col):
    if isinstance(col.owner, StructColumn):
      return '%s.%s' % (self._write(col.owner), col.name)
    return '%s.%s' % (col.owner.identifier, col.name)

  def _write_from_clause(self, from_clause):
    sql = 'FROM %s' % self._write(from_clause.table_expr)
    if from_clause.join_clauses:
      sql += '\n' + '\n'.join(self._write(join) for join in from_clause.join_clauses)
    return sql

  def _write_table(self, table):
    if table.alias:
      return '%s %s' % (table.name, table.identifier)
    return table.name

  def _write_inline_view(self, inline_view):
    if not inline_view.identifier:
      raise Exception('An inline view requires an identifier')
    return '(\n%s\n) %s' % (self._write(inline_view.query), inline_view.identifier)

  def _write_with_clause_inline_view(self, with_clause_inline_view):
    if not with_clause_inline_view.with_clause_alias:
      raise Exception('An with clause entry requires an identifier')
    sql = with_clause_inline_view.with_clause_alias
    if with_clause_inline_view.alias:
      sql += ' ' + with_clause_inline_view.alias
    return sql

  def _write_join_clause(self, join_clause):
    sql = '%s JOIN %s' % (join_clause.join_type, self._write(join_clause.table_expr))
    if join_clause.boolean_expr:
      sql += ' ON ' + self._write(join_clause.boolean_expr)
    return sql

  def _write_where_clause(self, where_clause):
    return 'WHERE\n' + self._write(where_clause.boolean_expr)

  def _write_group_by_clause(self, group_by_clause):
    return 'GROUP BY\n' + ',\n'.join(self._write(item.val_expr)
                                     for item in group_by_clause.group_by_items)

  def _write_having_clause(self, having_clause):
    return 'HAVING\n' + self._write(having_clause.boolean_expr)

  def _write_union_clause(self, union_clause):
    sql = 'UNION'
    if union_clause.all:
      sql += ' ALL'
    sql += '\n' + self._write(union_clause.query)
    return sql

  def _write_data_type(self, data_type):
    '''Write a literal value.'''
    if data_type.val is None:
      return 'NULL'
    if data_type.returns_char:
      return "'{0}'".format(data_type.val)
    if data_type.returns_timestamp:
      return "CAST('{0}' AS {1})".format(
          data_type.val, self._write_data_type_metaclass(Timestamp))
    return str(data_type.val)

  def _write_func(self, func):
    if func.name() in self.operator_funcs:
      sql = self.operator_funcs[func.name()].format(
          *[self._write(arg) for arg in func.args])
    else:
      sql = '%s(%s)' % \
          (self._to_sql_name(func.name()), self._write_as_comma_list(func.args))
    return sql

  def _write_exists(self, func):
    return 'EXISTS ' + self._write(func.args[0])

  def _write_not_exists(self, func):
    return 'NOT EXISTS ' + self._write(func.args[0])

  def _write_in(self, func, use_not=False):
    sql = '(%s) ' % self._write(func.args[0])
    if use_not:
      sql += 'NOT '
    sql += 'IN '
    if func.signature.args[1].is_subquery:
      sql += self._write(func.args[1])
    else:
      sql += '(' + self._write_as_comma_list(func.args[1:]) + ')'
    return sql

  def _write_not_in(self, func):
    return self._write_in(func, use_not=True)

  def _write_as_comma_list(self, items):
    return ', '.join([self._write(item) for item in items])

  def _write_cast_as_char(self, func):
    return 'CAST(%s AS %s)' % (self._write(func.args[0]), self._write(String))

  def _write_cast(self, arg, type):
    return 'CAST(%s AS %s)' % (self._write(arg), type)

  def _write_cast_func(self, func):
    val_expr = func.args[0]
    type_ = func.args[1]
    return 'CAST({val_expr} AS {type_})'.format(
        val_expr=self._write(val_expr), type_=self._write(type_))

  def _write_date_add_year(self, func):
    return "%s + INTERVAL %s YEAR" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_month(self, func):
    return "%s + INTERVAL %s MONTH" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_day(self, func):
    return "%s + INTERVAL %s DAY" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_hour(self, func):
    return "%s + INTERVAL %s HOUR" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_minute(self, func):
    return "%s + INTERVAL %s MINUTE" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_second(self, func):
    return "%s + INTERVAL %s SECOND" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_extract_year(self, func):
    return 'EXTRACT(YEAR FROM %s)' % self._write(func.args[0])

  def _write_extract_month(self, func):
    return 'EXTRACT(MONTH FROM %s)' % self._write(func.args[0])

  def _write_extract_day(self, func):
    return 'EXTRACT(DAY FROM %s)' % self._write(func.args[0])

  def _write_extract_hour(self, func):
    return 'EXTRACT(HOUR FROM %s)' % self._write(func.args[0])

  def _write_extract_minute(self, func):
    return 'EXTRACT(MINUTE FROM %s)' % self._write(func.args[0])

  def _write_extract_second(self, func):
    return 'EXTRACT(SECOND FROM %s)' % self._write(func.args[0])

  def _write_implicit_cast(self, func):
    return self._write(func.args[0])

  def _write_analytic_func(self, func):
    sql = self._to_sql_name(func.name()) \
        + '(' + self._write_as_comma_list(func.args) \
        + ') OVER ('
    options = []
    if func.partition_by_clause:
      options.append(self._write(func.partition_by_clause))
    if func.order_by_clause:
      options.append(self._write(func.order_by_clause))
    if func.window_clause:
      options.append(self._write(func.window_clause))
    return sql + ' '.join(options) + ')'

  def _write_partition_by_clause(self, partition_by_clause):
    return 'PARTITION BY ' + \
        ', '.join(self._write(expr) for expr in partition_by_clause.val_exprs)

  def _write_window_clause(self, window_clause):
    sql = window_clause.range_or_rows.upper() + ' '
    if window_clause.end_boundary:
      sql += 'BETWEEN '
    if window_clause.start_boundary.val_expr:
      sql += self._write(window_clause.start_boundary.val_expr) + ' '
    sql += window_clause.start_boundary.boundary_type.upper() + ' '
    if window_clause.end_boundary:
      sql += 'AND '
      if window_clause.end_boundary.val_expr:
        sql += self._write(window_clause.end_boundary.val_expr) + ' '
      sql += window_clause.end_boundary.boundary_type.upper()
    return sql

  def _write_agg_func(self, func):
    sql = self._to_sql_name(func.name()) + '('
    if func.distinct:
      sql += 'DISTINCT '
    # All agg funcs only have a single arg
    sql += self._write(func.args[0]) + ')'
    return sql

  def _write_data_type_metaclass(self, data_type_class):
    '''Write a data type class such as Int, Boolean, or Decimal(4, 2).'''
    if data_type_class == Char:
      return 'CHAR({0})'.format(data_type_class.MAX)
    elif data_type_class == VarChar:
      return 'VARCHAR({0})'.format(data_type_class.MAX)
    elif data_type_class == Decimal:
      return 'DECIMAL({scale},{precision})'.format(
          scale=data_type_class.MAX_DIGITS,
          precision=data_type_class.MAX_FRACTIONAL_DIGITS)
    else:
      return data_type_class.__name__.upper()

  def _write_subquery(self, subquery):
    return '({0})'.format(self._write(subquery.query))

  def _write_order_by_clause(self, order_by_clause):
    sql = 'ORDER BY '
    for idx, (expr, order) in enumerate(order_by_clause.exprs_to_order):
      if idx > 0:
        sql += ', '
      sql += self._write(expr)
      if order:
        sql += ' ' + order
        nulls_order = self.get_nulls_order(order)
        if nulls_order is not None:
          sql += ' ' + nulls_order
    return sql

  def _write_limit_clause(self, limit_clause):
    return 'LIMIT {0}'.format(limit_clause.limit)

  def _write_insert_clause(self, insert_clause):
    """
    Given an InsertClause, return a string representing that portion of the query. The
    InsertClause object may have the column_list attribute set, which is a
    sequence of columns.
    """
    if insert_clause.column_list is None:
      column_list = ''
    else:
      column_list = ' ({column_list})'.format(
          column_list=', '.join([col.name for col in insert_clause.column_list]))
    return 'INSERT INTO {table_name}{column_list}'.format(
        table_name=insert_clause.table.name, column_list=column_list)

  def _write_values_row(self, values_row):
    """
    Return a string representing 1 row of a VALUES clause.
    """
    return '({values_row})'.format(
        values_row=', '.join([self._write(item) for item in values_row.items]))

  def _write_values_clause(self, values_clause):
    """
    Return a string representing the VALUES clause of an INSERT query.
    """
    return 'VALUES\n{values_rows}'.format(
        values_rows=',\n'.join([self._write(values_row)
                                for values_row in values_clause.values_rows]))

  def _write(self, object_):
    '''Return a sql string representation of the given object.'''
    # What's below is effectively a giant switch statement. It works based on a func
    # naming and signature convention. It should match the incoming object with the
    # corresponding func defined, then call the func and return the result.
    #
    # Ex:
    #   a = model.And(...)
    #   _write(a) should call _write_func(a) because "And" is a subclass of "Func" and no
    #   other _writer_<class name> methods have been defined higher up the method
    #   resolution order (MRO). If _write_and(...) were to be defined, it would be called
    #   instead.
    for type_ in getmro(type(object_)):
      writer_func_name = '_write_' + self._to_py_name(type_.__name__)
      writer_func = getattr(self, writer_func_name, None)
      if writer_func:
        return writer_func(object_)

    # Handle any remaining cases
    if isinstance(object_, Query):
      return self.write_query(object_)

    raise Exception('Unsupported object: %s<%s>' % (type(object_).__name__, object_))

  def get_nulls_order(self, order):
    if self.nulls_order_asc is None:
      return None
    nulls_order_asc = self.nulls_order_asc
    if order == 'ASC':
      if nulls_order_asc == 'BEFORE':
        return 'NULLS FIRST'
      if nulls_order_asc == 'AFTER':
        return 'NULLS LAST'
    if order == 'DESC':
      if nulls_order_asc == 'BEFORE':
        return 'NULLS LAST'
      if nulls_order_asc == 'AFTER':
        return 'NULLS FIRST'

  def _to_py_name(self, name):
    return sub('([A-Z])', r'_\1', name).lower().lstrip('_')

  def _to_sql_name(self, name):
    return self._to_py_name(name).upper()


class ImpalaSqlWriter(SqlWriter):

  DIALECT = 'IMPALA'

  def __init__(self, *args, **kwargs):
    super(ImpalaSqlWriter, self).__init__(*args, **kwargs)
    self.operator_funcs['IsNotDistinctFromOp'] = '({0}) <=> ({1})'

  def _write_column(self, col):
    result = super(ImpalaSqlWriter, self)._write_column(col)
    if col.exact_type == Char:
      # TRIM is a temporary workaround for IMPALA-1652
      result = 'TRIM(%s)' % result
    return result

  def _write_insert_clause(self, insert_clause):
    sql = super(ImpalaSqlWriter, self)._write_insert_clause(insert_clause)
    if insert_clause.conflict_action == InsertClause.CONFLICT_ACTION_UPDATE:
      # The value of sql at this point would be something like:
      #
      # INSERT INTO <table name> [(column list)]
      #
      # If it happens that the table name or column list contains the text INSERT in an
      # identifier, we want to ensure that the replace() call below does not alter their
      # names but instead only modifiers the INSERT keyword to UPSERT.
      return sql.replace('INSERT', 'UPSERT', 1)
    else:
      return sql


class OracleSqlWriter(SqlWriter):

  DIALECT = 'ORACLE'


class HiveSqlWriter(SqlWriter):

  DIALECT = 'HIVE'

  def __init__(self, *args, **kwargs):
    super(HiveSqlWriter, self).__init__(*args, **kwargs)

    self.operator_funcs.update({
        'IsNotDistinctFrom': '({0}) <=> ({1})',
        'IsNotDistinctFromOp': '({0}) <=> ({1})',
        'IsDistinctFrom': 'NOT(({0}) <=> ({1}))'
    })

  # Hive greatest UDF is strict on type equality
  # Hive Profile already restricts to signatures with the same types,
  # but sometimes expression with UDF's like 'count'
  # return an unpredictable type like 'bigint' unlike
  # the query model, so cast is still necessary.
  def _write_greatest(self, func):
    args = func.args
    if args[0].type in (Int, Decimal, Float):
      argtype = args[0].type.__name__.lower()
      sql = '%s(%s)' % (
          self._to_sql_name(func.name()),
          self._write_cast(args[0], argtype) + ", " + self._write_cast(args[1], argtype))
    else:
      sql = self._write_func(func)
    return sql

  # Hive least UDF is strict on type equality
  # Hive Profile already restricts to signatures with the same types,
  # but sometimes expression with UDF's like 'count'
  # return an unpredictable type like 'bigint' unlike
  # the query model, so cast is still necessary.
  def _write_least(self, func):
    args = func.args
    if args[0].type in (Int, Decimal, Float):
      argtype = args[0].type.__name__.lower()
      sql = '%s(%s)' % (
          self._to_sql_name(func.name()),
          self._write_cast(args[0], argtype) + ", " + self._write_cast(args[1], argtype))
    else:
      sql = self._write_func(func)
    return sql

  # Workaround for tinyint casting issues when run against RefDb
  # that might only have larger integers.
  # This does all the arithmetic operations in terms of bigints.
  def _write_plus(self, func):
    return self.arithmetic_cast(func, '+')

  def _write_minus(self, func):
    return self.arithmetic_cast(func, '-')

  def _write_multiply(self, func):
    return self.arithmetic_cast(func, '*')

  def arithmetic_cast(self, func, symbol):
    args = func.args
    if args[0].type is Int and args[1].type is Int:
      return 'CAST (%s AS BIGINT) %s CAST (%s AS BIGINT)' % (
          self._write(args[0]), symbol, self._write(args[1]))
    else:
      return self._write_func(func)

  # Hive partition by clause throws exception if sorted by more than one key, unless
  # 'rows unbounded preceding' added.
  def _write_analytic_func(self, func):
    sql = self._to_sql_name(func.name()) \
        + '(' + self._write_as_comma_list(func.args) \
        + ') OVER ('
    options = []
    if func.partition_by_clause:
      options.append(self._write(func.partition_by_clause))
    if func.order_by_clause:
      options.append(self._write(func.order_by_clause))
    if func.window_clause:
      options.append(self._write(func.window_clause))
    if func.partition_by_clause and func.order_by_clause:
      if len(func.order_by_clause.exprs_to_order) > 1:
        if func.SUPPORTS_WINDOWING and func.window_clause is None:
          options.append('rows unbounded preceding')
    return sql + ' '.join(options) + ')'

  def _write_extract_year(self, func):
    return 'YEAR(%s)' % self._write(func.args[0])

  def _write_extract_month(self, func):
    return 'MONTH(%s)' % self._write(func.args[0])

  def _write_extract_day(self, func):
    return 'DAY(%s)' % self._write(func.args[0])

  def _write_extract_hour(self, func):
    return 'HOUR(%s)' % self._write(func.args[0])

  def _write_extract_minute(self, func):
    return 'MINUTE(%s)' % self._write(func.args[0])

  def _write_extract_second(self, func):
    return 'SECOND(%s)' % self._write(func.args[0])


class PostgresqlSqlWriter(SqlWriter):

  DIALECT = 'POSTGRESQL'

  def _write_insert_statement(self, insert_statement):
    sql = SqlWriter._write_insert_statement(self, insert_statement)
    if insert_statement.conflict_action == InsertClause.CONFLICT_ACTION_DEFAULT:
      pass
    elif insert_statement.conflict_action == InsertClause.CONFLICT_ACTION_IGNORE:
      sql += '\nON CONFLICT DO NOTHING'
    elif insert_statement.conflict_action == InsertClause.CONFLICT_ACTION_UPDATE:
      if insert_statement.updatable_column_names:
        primary_keys = insert_statement.primary_key_string
        columns = ',\n'.join('{name} = EXCLUDED.{name}'.format(name=name) for name in
                             insert_statement.updatable_column_names)
        sql += '\nON CONFLICT {primary_keys}\nDO UPDATE SET\n{columns}'.format(
            primary_keys=primary_keys, columns=columns)
      else:
        sql += '\nON CONFLICT DO NOTHING'
    else:
      raise Exception('InsertStatement has unsupported conflict_action: {0}'.format(
          insert_statement.conflict_action))
    return sql

  def _write_date_add_year(self, func):
    return "%s + (%s) * INTERVAL '1' YEAR" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_month(self, func):
    return "%s + (%s) * INTERVAL '1' MONTH" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_day(self, func):
    return "%s + (%s) * INTERVAL '1' DAY" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_hour(self, func):
    return "%s + (%s) * INTERVAL '1' HOUR" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_join_clause(self, join_clause):
    sql = '%s JOIN %s %s' % (
        join_clause.join_type,
        'LATERAL' if join_clause.is_lateral_join else '',
        self._write(join_clause.table_expr))
    if join_clause.boolean_expr:
      sql += ' ON ' + self._write(join_clause.boolean_expr)
    return sql

  def _write_date_add_minute(self, func):
    return "%s + (%s) * INTERVAL '1' MINUTE" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_date_add_second(self, func):
    return "%s + (%s) * INTERVAL '1' SECOND" \
        % (self._write(func.args[0]), self._write(func.args[1]))

  def _write_column(self, col):
    def first_non_struct_ancestor(col):
      col = col.owner
      while isinstance(col, StructColumn):
        col = col.owner
      return col
    return '%s.%s' % (first_non_struct_ancestor(col).identifier,
                      QueryFlattener.flat_column_name(col))

  def _write_collection_column(self, collection_col):
    return '%s %s' % (QueryFlattener.flat_collection_name(collection_col),
                      collection_col.identifier)

  def _write_extract_second(self, func):
    # For some reason Postgresql decided that extracting second should return a FLOAT...
    return 'FLOOR(EXTRACT(SECOND FROM %s))' % self._write(func.args[0])

  def _write_if(self, func):
    return 'CASE WHEN {0} THEN {1} ELSE {2} END' \
        .format(*[self._write(arg) for arg in func.args])

  def _write_data_type_metaclass(self, data_type_class):
    '''Write a data type class such as Int or Boolean.'''
    if data_type_class == Double:
      return 'DOUBLE PRECISION'
    elif data_type_class == Float:
      return 'REAL'
    elif data_type_class == String:
      return 'VARCHAR(%s)' % data_type_class.MAX
    elif data_type_class == Timestamp:
      return 'TIMESTAMP WITHOUT TIME ZONE'
    elif data_type_class == TinyInt:
      return 'SMALLINT'
    else:
      return super(PostgresqlSqlWriter, self)._write_data_type_metaclass(data_type_class)

  def _write_order_by_clause(self, order_by_clause):
    sql = 'ORDER BY '
    for idx, (expr, order) in enumerate(order_by_clause.exprs_to_order):
      if idx > 0:
        sql += ', '
      if expr.returns_char:
        sql += 'CAST({0} AS BYTEA)'.format(self._write(expr))
      else:
        sql += self._write(expr)
      if order:
        sql += ' ' + order
        nulls_order = self.get_nulls_order(order)
        if (nulls_order is not None):
          sql += ' ' + nulls_order
    return sql

  def _write_data_type(self, data_type):
    '''Write a literal value.'''
    if data_type.val is None:
      return 'NULL'
    if data_type.returns_char:
      # Literals sometimes produce an error 'could not determine polymorphic type
      # because input has type "unknown"', adding an "|| ''" avoids the problem.
      return "'%s' || ''" % data_type.val
    return SqlWriter._write_data_type(self, data_type)

  def _write_concat(self, func):
    # PostgreSQL CONCAT() doesn't behave like Impala CONCAT(). PostgreSQL || does.
    return '({concat_list})'.format(
        concat_list=' || '.join(['({written_item})'.format(written_item=self._write(item))
                                 for item in func.args]))


class MySQLSqlWriter(SqlWriter):

  DIALECT = 'MYSQL'

  def write_query(self, query, pretty=False):
    # MySQL doesn't support WITH clauses so they need to be converted into inline views.
    # We are going to cheat by making use of the fact that the query generator creates
    # with clause entries with unique aliases even considering nested queries.
    sql = list()
    for clause in (
        query.select_clause,
        query.from_clause,
        query.where_clause,
        query.group_by_clause,
        query.having_clause,
        query.union_clause,
        query.order_by_clause,
        query.limit_clause
    ):
      if clause:
        sql.append(self._write(clause))
    sql = '\n'.join(sql)
    if query.with_clause:
      # Just replace the named referenes with inline views. Go in reverse order because
      # entries at the bottom of the WITH clause definition may reference entries above.
      for with_clause_inline_view in reversed(query.with_clause.with_clause_inline_views):
        replacement_sql = '(' + self.write_query(with_clause_inline_view.query) + ')'
        sql = sql.replace(with_clause_inline_view.identifier, replacement_sql)
    if pretty:
      sql = self.make_pretty_sql(sql)
    return sql

  def _write_data_type_metaclass(self, data_type_class):
    '''Write a data type class such as Int or Boolean.'''
    if issubclass(data_type_class, Int):
      return 'INTEGER'
    if issubclass(data_type_class, Float):
      return 'DECIMAL(65, 15)'
    if issubclass(data_type_class, VarChar):
      return 'CHAR'
    if hasattr(data_type_class, 'MYSQL'):
      return data_type_class.MYSQL[0]
    return data_type_class.__name__.upper()

  def _write_data_type(self, data_type):
    '''Write a literal value.'''
    if data_type.returns_timestamp:
      return "CAST('{0}' AS DATETIME)".format(data_type.val)
    if data_type.returns_boolean:
      # MySQL will error if a data_type "FALSE" is used as a GROUP BY field
      return '(0 = 0)' if data_type.val else '(1 = 0)'
    return SqlWriter._write_data_type(self, data_type)

  def _write_order_by_clause(self, order_by_clause):
    sql = 'ORDER BY '
    for idx, (expr, order) in enumerate(order_by_clause.exprs_to_order):
      if idx > 0:
        sql += ', '
      sql += 'ISNULL({0}), {0}'.format(self._write(expr))
      if order:
        sql += ' ' + order
        nulls_order = self.get_nulls_order(order)
        if (nulls_order is not None):
          sql += ' ' + nulls_order
    return sql
