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

from inspect import getmro
from logging import getLogger
from re import sub
from sqlparse import format

from tests.comparison.model import (
    Boolean,
    Float,
    Int,
    Number,
    Query,
    String,
    Timestamp)

LOG = getLogger(__name__)

class SqlWriter(object):
  '''Subclasses of SQLWriter will take a Query and provide the SQL representation for a
     specific database such as Impala or MySQL. The SqlWriter.create([dialect=])
     factory method may be used instead of specifying the concrete class.

     Another important function of this class is to ensure that CASTs produce the same
     results across different databases. Sometimes the CASTs implemented here produce odd
     results. For example, the result of CAST(date_col AS INT) in MySQL may be an int of
     YYYYMMDD whereas in Impala it may be seconds since the epoch. For comparison purposes
     the CAST could be transformed into EXTRACT(DAY from date_col).
  '''

  @staticmethod
  def create(dialect='impala'):
    '''Create and return a new SqlWriter appropriate for the given sql dialect. "dialect"
       refers to database specific deviations of sql, and the val should be one of
       "IMPALA", "MYSQL", or "POSTGRESQL".
    '''
    dialect = dialect.upper()
    if dialect == 'IMPALA':
      return SqlWriter()
    if dialect == 'POSTGRESQL':
      return PostgresqlSqlWriter()
    if dialect == 'MYSQL':
      return MySQLSqlWriter()
    raise Exception('Unknown dialect: %s' % dialect)

  def write_query(self, query, pretty=False):
    '''Return SQL as a string for the given query.'''
    sql = list()
    # Write out each section in the proper order
    for clause in (
        query.with_clause,
        query.select_clause,
        query.from_clause,
        query.where_clause,
        query.group_by_clause,
        query.having_clause,
        query.union_clause):
      if clause:
        sql.append(self._write(clause))
    sql = '\n'.join(sql)
    if pretty:
      sql = self.make_pretty_sql(sql)
    return sql

  def make_pretty_sql(self, sql):
    try:
      sql = format(sql, reindent=True)
    except Exception as e:
      LOG.warn('Unable to format sql: %s', e)
    return sql

  def _write_with_clause(self, with_clause):
    return 'WITH ' + ',\n'.join('%s AS (%s)' % (view.identifier, self._write(view.query))
                                for view in with_clause.with_clause_inline_views)

  def _write_select_clause(self, select_clause):
    items = select_clause.non_agg_items + select_clause.agg_items
    sql = 'SELECT'
    if select_clause.distinct:
      sql += ' DISTINCT'
    sql += '\n' + ',\n'.join(self._write(item) for item in items)
    return sql

  def _write_select_item(self, select_item):
    # If the query is nested, the items will have aliases so that the outer query can
    # easily reference them.
    if not select_item.alias:
      raise Exception('An alias is required')
    return '%s AS %s' % (self._write(select_item.val_expr), select_item.alias)

  def _write_column(self, col):
    return '%s.%s' % (col.owner.identifier, col.name)

  def _write_from_clause(self, from_clause):
    sql = 'FROM %s' % self._write(from_clause.table_expr)
    if from_clause.join_clauses:
      sql += '\n' + '\n'.join(self._write(join) for join in from_clause.join_clauses)
    return sql

  def _write_table(self, table):
    if table.alias:
        return '%s AS %s' % (table.name, table.identifier)
    return table.name

  def _write_inline_view(self, inline_view):
    if not inline_view.identifier:
      raise Exception('An inline view requires an identifier')
    return '(\n%s\n) AS %s' % (self._write(inline_view.query), inline_view.identifier)

  def _write_with_clause_inline_view(self, with_clause_inline_view):
    if not with_clause_inline_view.with_clause_alias:
      raise Exception('An with clause entry requires an identifier')
    sql = with_clause_inline_view.with_clause_alias
    if with_clause_inline_view.alias:
      sql += ' AS ' + with_clause_inline_view.alias
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
    if data_type.returns_string:
      return "'{}'".format(data_type.val)
    if data_type.returns_timestamp:
      return "CAST('{}' AS TIMESTAMP)".format(data_type.val)
    return str(data_type.val)

  def _write_func(self, func):
    return func.FORMAT.format(*[self._write(arg) for arg in func.args])

  def _write_cast(self, cast):
    # Handle casts that produce different results across database types or just don't
    # make sense like casting a DATE as a BOOLEAN....
    if cast.val_expr.returns_boolean:
      if issubclass(cast.resulting_type, Timestamp):
        return "CAST(CASE WHEN {} THEN '2000-01-01' ELSE '1999-01-01' END AS TIMESTAMP)"\
            .format(self._write(cast.val_expr))
    elif cast.val_expr.returns_number:
      if issubclass(cast.resulting_type, Timestamp):
        return ("CAST(CONCAT('2000-01-', "
            "LPAD(CAST(ABS(FLOOR({})) % 31 + 1 AS STRING), 2, '0')) "
            "AS TIMESTAMP)").format(self._write(cast.val_expr))
    elif cast.val_expr.returns_string:
      if issubclass(cast.resulting_type, Boolean):
        return "(LENGTH({}) > 2)".format(self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Timestamp):
        return ("CAST(CONCAT('2000-01-', LPAD(CAST(LENGTH({}) % 31 + 1 AS STRING), "
            "2, '0')) AS TIMESTAMP)").format(self._write(cast.val_expr))
    elif cast.val_expr.returns_timestamp:
      if issubclass(cast.resulting_type, Boolean):
        return '(DAY({0}) > MONTH({0}))'.format(self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Number):
        return ('(DAY({0}) + 100 * MONTH({0}) + 100 * 100 * YEAR({0}))').format(
            self._write(cast.val_expr))
    return self._write_func(cast)

  def _write_agg_func(self, agg_func):
    sql = type(agg_func).__name__.upper() + '('
    if agg_func.distinct:
      sql += 'DISTINCT '
    # All agg funcs only have a single arg
    sql += self._write(agg_func.args[0]) + ')'
    return sql

  def _write_data_type_metaclass(self, data_type_class):
    '''Write a data type class such as Int or Boolean.'''
    return data_type_class.__name__.upper()

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
      writer_func_name = '_write' + sub('([A-Z])', r'_\1', type_.__name__).lower()
      writer_func = getattr(self, writer_func_name, None)
      if writer_func:
        return writer_func(object_)

    # Handle any remaining cases
    if isinstance(object_, Query):
      return self.write_query(object_)

    raise Exception('Unsupported object: %s<%s>' % (type(object_).__name__, object_))


class PostgresqlSqlWriter(SqlWriter):
  # TODO: This class is out of date since switching to MySQL. This is left here as is
  #       in case there is a desire to switch back in the future (it should be better than
  #       starting from nothing).

  def _write_divide(self, divide):
    # For ints, Postgresql does int division but Impala does float division.
    return 'CAST({} AS REAL) / {}' \
        .format(*[self._write(arg) for arg in divide.args])

  def _write_data_type_metaclass(self, data_type_class):
    '''Write a data type class such as Int or Boolean.'''
    if hasattr(data_type_class, 'POSTGRESQL'):
      return data_type_class.POSTGRESQL[0]
    return data_type_class.__name__.upper()

  def _write_cast(self, cast):
    # Handle casts that produce different results across database types or just don't
    # make sense like casting a DATE as a BOOLEAN....
    if cast.val_expr.returns_boolean:
      if issubclass(cast.resulting_type, Float):
        return "CASE {} WHEN TRUE THEN 1.0 WHEN FALSE THEN 0.0 END".format(
            self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Timestamp):
        return "CASE WHEN {} THEN '2000-01-01' ELSE '1999-01-01' END".format(
            self._write(cast.val_expr))
      if issubclass(cast.resulting_type, String):
        return "CASE {} WHEN TRUE THEN '1' WHEN FALSE THEN '0' END".format(
            self._write(cast.val_expr))
    elif cast.val_expr.returns_number:
      if issubclass(cast.resulting_type, Boolean):
        return 'CASE WHEN ({0}) != 0 THEN TRUE WHEN ({0}) = 0 THEN FALSE END'.format(
            self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Timestamp):
        return "CASE WHEN ({}) > 0 THEN '2000-01-01' ELSE '1999-01-01' END".format(
            self._write(cast.val_expr))
    elif cast.val_expr.returns_string:
      if issubclass(cast.resulting_type, Boolean):
        return "(LENGTH({}) > 2)".format(self._write(cast.val_expr))
    elif cast.val_expr.returns_timestamp:
      if issubclass(cast.resulting_type, Boolean):
        return '(EXTRACT(DAY FROM {0}) > EXTRACT(MONTH FROM {0}))'.format(
        self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Number):
        return ('(EXTRACT(DAY FROM {0}) '
            '+ 100 * EXTRACT(MONTH FROM {0}) '
            '+ 100 * 100 * EXTRACT(YEAR FROM {0}))').format(
                self._write(cast.val_expr))
    return self._write_func(cast)


class MySQLSqlWriter(SqlWriter):

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
        query.union_clause):
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
    if issubclass(data_type_class, String):
      return 'CHAR'
    if hasattr(data_type_class, 'MYSQL'):
      return data_type_class.MYSQL[0]
    return data_type_class.__name__.upper()

  def _write_data_type(self, data_type):
    '''Write a literal value.'''
    if data_type.returns_timestamp:
      return "CAST('{}' AS DATETIME)".format(data_type.val)
    if data_type.returns_boolean:
      # MySQL will error if a data_type "FALSE" is used as a GROUP BY field
      return '(0 = 0)' if data_type.val else '(1 = 0)'
    return SqlWriter._write_data_type(self, data_type)

  def _write_cast(self, cast):
    # Handle casts that produce different results across database types or just don't
    # make sense like casting a DATE as a BOOLEAN....
    if cast.val_expr.returns_boolean:
      if issubclass(cast.resulting_type, Timestamp):
        return "CAST(CASE WHEN {} THEN '2000-01-01' ELSE '1999-01-01' END AS DATETIME)"\
            .format(self._write(cast.val_expr))
    elif cast.val_expr.returns_number:
      if issubclass(cast.resulting_type, Boolean):
        return ("CASE WHEN ({0}) != 0 THEN TRUE WHEN ({0}) = 0 THEN FALSE END").format(
            self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Timestamp):
        return "CAST(CONCAT('2000-01-', ABS(FLOOR({})) % 31 + 1) AS DATETIME)"\
            .format(self._write(cast.val_expr))
    elif cast.val_expr.returns_string:
      if issubclass(cast.resulting_type, Boolean):
        return "(LENGTH({}) > 2)".format(self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Timestamp):
        return ("CAST(CONCAT('2000-01-', LENGTH({}) % 31 + 1) AS DATETIME)").format(
           self._write(cast.val_expr))
    elif cast.val_expr.returns_timestamp:
      if issubclass(cast.resulting_type, Number):
        return ('(EXTRACT(DAY FROM {0}) '
            '+ 100 * EXTRACT(MONTH FROM {0}) '
            '+ 100 * 100 * EXTRACT(YEAR FROM {0}))').format(
                self._write(cast.val_expr))
      if issubclass(cast.resulting_type, Boolean):
        return '(EXTRACT(DAY FROM {0}) > EXTRACT(MONTH FROM {0}))'.format(
            self._write(cast.val_expr))

    # MySQL uses different type names when casting...
    if issubclass(cast.resulting_type, Boolean):
      data_type = 'UNSIGNED'
    elif issubclass(cast.resulting_type, Float):
      data_type = 'DECIMAL(65, 15)'
    elif issubclass(cast.resulting_type, Int):
      data_type = 'SIGNED'
    elif issubclass(cast.resulting_type, String):
      data_type = 'CHAR'
    elif issubclass(cast.resulting_type, Timestamp):
      data_type = 'DATETIME'
    return cast.FORMAT.format(self._write(cast.val_expr), data_type)
