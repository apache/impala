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
from builtins import object, range
from future.utils import with_metaclass
from abc import ABCMeta, abstractproperty
from copy import deepcopy
from logging import getLogger

from tests.comparison.common import Column, TableExpr, TableExprList, ValExpr, ValExprList


LOG = getLogger(__name__)


class StatementExecutionMode(object):
  """
  Provide a name space for statement execution modes.
  """
  (
      # A SELECT statement is executed and results are compared.
      SELECT_STATEMENT,
      # If this is chosen, statement execution will run the CTAS statement and then
      # SELECT * on the table for comparision. The table is torn down after.
      CREATE_TABLE_AS,
      # Same as above, except with a few.
      CREATE_VIEW_AS,
      # a DML operation that isn't actually a test, but some setup operation that needs
      # to be run concurrently
      DML_SETUP,
      # a DML statement that's actually a test
      DML_TEST,
  ) = range(5)


class AbstractStatement(with_metaclass(ABCMeta, object)):
  """
  Abstract query representation
  """

  def __init__(self):
    # reference to statement's parent. For example the right side of a UNION clause
    # SELECT will have a parent as the SELECT on the left, which for the query
    # generator's purpose is the parent
    self.parent = None
    # optional WITH clause some statements may have
    self.with_clause = None
    self._execution = None

  @abstractproperty
  def table_exprs(self):
    """
    Return a list of all table expressions that are declared by this query. This is
    abstract as the clauses that do this differ across query types. Since all supported
    queries may have a WITH clause, getting table expressions from the WITH clause is
    supported here.
    """
    # This is an abstractproperty because it's only a *partial* implementation, however
    # for any statement or query that has a WITH clause, we can handle that here.
    table_exprs = TableExprList([])
    if self.with_clause:
      table_exprs.extend(self.with_clause.table_exprs)
    return table_exprs

  @abstractproperty
  def nested_queries(self):
    """
    Returns a list of queries contained within this query. Different queries may have
    different clauses containing subqueries, so this is an abtract property.
    """
    pass

  @property
  def execution(self):
    """
    one of the possible StatementExecutionMode values (see class definition for meaning)
    """
    if self._execution is None:
      raise Exception('execution is not set on this object')
    return self._execution

  @execution.setter
  def execution(self, val):
    self._execution = val


class Query(AbstractStatement):
  # TODO: This has to be called Query for as long as we want to unpickle old reports, or
  # we have to get into the legalese weeds. See:
  # https://gerrit.cloudera.org/#/c/5162/5/tests/comparison/query.py@61
  # https://gerrit.cloudera.org/#/c/5162/1/tests/comparison/leopard/custom_pickle.py@9
  # If we decide at some point we don't need to unpickle some of the recent reports,
  # then this can be renamed to something like SelectStatement.
  """
  A representation of the structure of a SQL SELECT query. Only the select_clause and
  from_clause are required for a valid query.
  """

  def __init__(self):
    super(Query, self).__init__()
    self.select_clause = None
    self.from_clause = None
    self.where_clause = None
    self.group_by_clause = None
    self.having_clause = None
    self.union_clause = None
    self.order_by_clause = None
    self.limit_clause = None
    # This is a fine default value, because any well-formed object will be a SELECT
    # statement. Only the discrepancy searcher makes the decision at run time to change
    # this.
    self.execution = StatementExecutionMode.SELECT_STATEMENT

  def __deepcopy__(self, memo):
    other = Query()
    memo[self] = other
    other.parent = memo[self.parent] if self.parent in memo else None
    other.with_clause = deepcopy(self.with_clause, memo)
    other.execution = self.execution
    other.from_clause = deepcopy(self.from_clause, memo)
    other.select_clause = deepcopy(self.select_clause, memo)
    other.where_clause = deepcopy(self.where_clause, memo)
    other.group_by_clause = deepcopy(self.group_by_clause, memo)
    other.having_clause = deepcopy(self.having_clause, memo)
    other.union_clause = deepcopy(self.union_clause, memo)
    other.order_by_clause = deepcopy(self.order_by_clause, memo)
    other.limit_clause = deepcopy(self.limit_clause, memo)
    return other

  @property
  def table_exprs(self):
    '''Provides a list of all table_exprs that are declared by this query. This
       includes table_exprs in the WITH and FROM sections.
    '''
    table_exprs = super(Query, self).table_exprs  # WITH clause
    table_exprs.extend(self.from_clause.table_exprs)
    return table_exprs

  @property
  def is_unioned_query(self):
    return self.parent \
        and self.parent.union_clause \
        and self.parent.union_clause.query is self

  @property
  def nested_queries(self):
    '''Returns a list of queries contained within this query.'''
    queries = list()
    if self.with_clause:
      for inline_view in self.with_clause.with_clause_inline_views:
        queries.append(inline_view.query)
    for table_expr in self.table_exprs:
      if isinstance(table_expr, InlineView):
        queries.append(table_expr.query)
    if self.union_clause:
      queries.append(self.union_clause.query)
    if self.where_clause:
      queries.extend(
          subquery.query for subquery in
          self.where_clause.boolean_expr.iter_exprs(lambda expr: expr.is_subquery))
    for query in list(queries):
      queries.extend(query.nested_queries)
    return queries


class SelectClause(object):
  '''This encapsulates the SELECT part of a query. It is convenient to separate
     non-agg items from agg items so that it is simple to know if the query
     is an agg query or not.
  '''

  def __init__(self, select_items):
    self.items = select_items
    self.distinct = False

  @property
  def basic_items(self):
    '''Returns a list of SelectItems that are also basic items. Deletions from
       this list will be propagated but additions will not be.
    '''
    return SelectItemSubList(self.items, lambda item: item.is_basic)

  @property
  def agg_items(self):
    '''Returns a list of SelectItems that are also aggregate items. Deletions from
       this list will be propagated but additions will not be.
    '''
    return SelectItemSubList(self.items, lambda item: item.is_agg)

  @property
  def analytic_items(self):
    '''Returns a list of SelectItems that are also analytic items. Deletions from
       this list will be propagated but additions will not be.
    '''
    return SelectItemSubList(self.items, lambda item: item.is_analytic)

  @property
  def contains_approximate_types(self):
    '''Returns true if there is a select item that is approximate (such as Float).'''
    return any(item.type.is_approximate() for item in self.items)

  def __deepcopy__(self, memo):
    other = SelectClause([deepcopy(item, memo) for item in self.items])
    other.distinct = self.distinct
    return other


# This is used in the query simplifier (not yet checked in) to simplify reduction
# of select items.
class SelectItemSubList(object):
  '''A list like object that propagates deletions.'''

  def __init__(self, select_items, filter):
    self.select_items = select_items
    self.filter = filter

  def __iter__(self):
    return (item for item in self.select_items if self.filter(item))

  def __len__(self):
    return sum(1 for _ in self)

  def __bool__(self):
    try:
      next(iter(self))
      return True
    except StopIteration:
      return False

  def __getitem__(self, key):
    if isinstance(key, int):
      if key < 0:
        key = len(self) + key
        if key < 0:
          raise IndexError()
      for idx, item in enumerate(self):
        if idx == key:
          return item
      raise IndexError()
    elif isinstance(key, slice):
      length = len(self)
      start, stop, step, reverse = self._get_start_stop_step_reverse(key, length)
      self_iter = enumerate(self)
      items = list()
      while start < stop:
        try:
          idx, item = next(self_iter)
        except StopIteration:
          break
        if idx < start:
          continue
        elif idx == start:
          items.append(item)
          start += step
        else:
          break
      if reverse:
        items.reverse()
      return items
    else:
      raise TypeError('Index must be a integer or slice, not %s' % key.__class.__name__)

  def __delitem__(self, key):
    if isinstance(key, int):
      if key < 0:
        key = len(self) + key
        if key < 0:
          raise IndexError()
      for idx, item in enumerate(self.select_items):
        if not self.filter(item):
          continue
        if key == 0:
          del self.select_items[idx]
          return
        key -= 1
      raise IndexError()
    elif isinstance(key, slice):
      length = len(self)
      start, stop, step, _ = self._get_start_stop_step_reverse(key, length)
      self_iter = enumerate(self.select_items)
      item_idxs = list()
      filtered_idx = 0
      while start < stop:
        try:
          idx, item = next(self_iter)
        except StopIteration:
          break
        if not self.filter(item):
          continue
        if filtered_idx < start:
          pass
        elif filtered_idx == start:
          item_idxs.append(idx)
          start += step
        else:
          break
        filtered_idx += 1
      item_idxs.reverse()
      for idx in item_idxs:
        del self.select_items[idx]
    else:
      raise TypeError('Index must be a integer or slice, not %s' % key.__class.__name__)

  def _get_start_stop_step_reverse(self, slice, length):
    step = slice.step or 1
    if step == 0:
      raise ValueError('Step cannot be zero')
    reverse = step < 0
    if reverse:
      step = step * -1

    if slice.start is None and slice.stop is None and reverse:
      return 0, length, step, True

    if slice.start is None:
      start = length if reverse else 0
    elif slice.start < 0:
      start = slice.start + length
      if start < 0:
        raise IndexError()
    elif slice.start >= length:
      start = length - 1
    else:
      start = slice.start

    if slice.stop is None:
      stop = 0 if reverse else length
    elif slice.stop < 0:
      stop = slice.stop + length
      if stop < 0:
        raise IndexError()
    elif slice.stop > length:
      stop = length
    else:
      stop = slice.stop

    return start, stop, step, reverse


class SelectItem(object):
  '''A representation of any possible expr than would be valid in

     SELECT <SelectItem>[, <SelectItem>...] FROM ...

     Each SelectItem contains a ValExpr which will either be a instance of a
     DataType (representing a constant), a Column, or a Func.

     Ex: "SELECT int_col + smallint_col FROM alltypes" would have a val_expr of
         Plus(Column(<alltypes.int_col>), Column(<alltypes.smallint_col>)).

  '''

  def __init__(self, val_expr, alias=None):
    self.val_expr = val_expr
    self.alias = alias

  @property
  def name(self):
    if self.alias:
      return self.alias
    if self.val_expr.is_col:
      return self.val_expr.name
    raise Exception('Could not determine name')

  @property
  def type(self):
    '''Returns the DataType of this item.'''
    return self.val_expr.type

  @property
  def base_type(self):
    '''Returns the base DataType of this item.'''
    return self.val_expr.base_type

  @property
  def is_basic(self):
    '''Evaluates to True if this item is neither an aggregate nor an analytic expression.
    '''
    return not self.is_agg and not self.is_analytic

  @property
  def is_agg(self):
    '''Evaluates to True if this item contains an aggregate expression and does not
       contain an analytic expression. If an expression contains both an aggregate
       and an analytic, it is considered an analytic expression.
    '''
    return not self.is_analytic and self.val_expr.contains_agg

  @property
  def is_analytic(self):
    '''Evaluates to True if this item contains an analytic expression.'''
    return self.val_expr.contains_analytic

  def __deepcopy__(self, memo):
    other = SelectItem(deepcopy(self.val_expr, memo))
    other.alias = self.alias
    return other


class Subquery(ValExpr):
  '''Represents both a scalar subquery and a subquery that returns a multi-row/column
     result set.

  '''
  # TODO: So far it seems fine to use this class for both scalar/non scalar cases but
  #       this could lead to unexpected behavior or be a silent cause of problems...

  def __init__(self, query):
    self.query = query

  @property
  def type(self):
    return self.query.select_clause.items[0].type

  def __deepcopy__(self, memo):
    return Subquery(deepcopy(self.query, memo))


class FromClause(object):
  '''A representation of a FROM clause. The member variable join_clauses may optionally
     contain JoinClause items.
  '''

  def __init__(self, table_expr, join_clauses=None):
    self.table_expr = table_expr
    self.join_clauses = join_clauses or list()

  @property
  def table_exprs(self):
    '''Provides a list of all table_exprs that are declared within this FROM
       block.
    '''
    table_exprs = \
        TableExprList(join_clause.table_expr for join_clause in self.join_clauses)
    table_exprs.append(self.table_expr)
    return table_exprs

  def __deepcopy__(self, memo):
    other = FromClause(deepcopy(self.table_expr, memo))
    other.join_clauses = [deepcopy(join_clause, memo)
                          for join_clause in self.join_clauses]
    return other

  @property
  def collections(self):
    result = self.table_expr.collections
    for join_clause in self.join_clauses:
      result.extend(join_clause.table_expr.collections)
    return result

  @property
  def visible_table_exprs(self):
    '''Provides a list of all table_exprs that are declared within this FROM
       block and may be referenced in other clauses such as SELECT or WHERE.
    '''
    return TableExprList(table_expr for table_expr in self.table_exprs
                         if table_expr.is_visible)

  @property
  def has_non_standard_joins(self):
    '''Evaluates to True if ANTI or SEMI JOINs are in use.'''
    if not self.join_clauses:
      return
    for join_clause in self.join_clauses:
      if 'ANTI' in join_clause.join_type or 'SEMI' in join_clause.join_type:
        return True


class InlineView(TableExpr):
  '''Represents an inline view.

     Ex: In the query "SELECT * FROM (SELECT * FROM foo) AS bar",
         "(SELECT * FROM foo) AS bar" would be an inline view.

  '''

  def __init__(self, query):
    self.query = query
    self.alias = None
    self.is_visible = True

  @property
  def identifier(self):
    return self.alias

  @property
  def cols(self):
    return ValExprList(Column(self, item.name, item.type) for item in
                       self.query.select_clause.items)

  @property
  def collections(self):
    return []

  def __repr__(self):
    return '%s<%s>' % (type(self).__name__, ', '.join(repr(col) for col in self.cols))

  def __deepcopy__(self, memo):
    other = InlineView(deepcopy(self.query, memo))
    other.alias = self.alias
    other.is_visible = self.is_visible
    return other


class WithClause(object):
  '''Represents a WITH clause.

     Ex: In the query "WITH bar AS (SELECT * FROM foo) SELECT * FROM bar",
         "WITH bar AS (SELECT * FROM foo)" would be the with clause.

  '''

  def __init__(self, with_clause_inline_views):
    self.with_clause_inline_views = with_clause_inline_views

  @property
  def table_exprs(self):
    return self.with_clause_inline_views

  def __deepcopy__(self, memo):
    return WithClause(deepcopy(self.with_clause_inline_views, memo))


class WithClauseInlineView(InlineView):
  '''Represents the entries in a WITH clause. These are very similar to InlineViews but
     may have an additional alias.

     Ex: WITH bar AS (SELECT * FROM foo)
         SELECT *
         FROM bar as r
         JOIN (SELECT * FROM baz) AS z ON ...

         The WithClauseInlineView has aliases "bar" and "r" while the InlineView has
         only the alias "z".

  '''

  def __init__(self, query, with_clause_alias):
    self.query = query
    self.with_clause_alias = with_clause_alias
    self.alias = None

  @property
  def identifier(self):
    return self.alias or self.with_clause_alias

  def __deepcopy__(self, memo):
    other = WithClauseInlineView(deepcopy(self.query, memo), self.with_clause_alias)
    other.alias = self.alias
    return other


class JoinClause(object):
  '''A representation of a JOIN clause.

     Ex: SELECT * FROM foo <join_type> JOIN <table_expr> [ON <boolean_expr>]

     The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  JOINS_TYPES = [
      'INNER',
      'LEFT',
      'RIGHT',
      'LEFT SEMI',
      'LEFT ANTI',
      'RIGHT SEMI',
      'RIGHT ANTI',
      'FULL OUTER',
      'CROSS']

  def __init__(self, join_type, table_expr, boolean_expr=None):
    self.join_type = join_type
    self.table_expr = table_expr
    self.boolean_expr = boolean_expr
    # This is used for nested types. It means that we are joining with an earlier aliased
    # element in the from clause. For example, "From customer t1 INNER JOIN t1.orders t2"
    # or "FROM customer t1 INNER JOIN t1.orders.lineitems t2 ON t1.comment = t2.comment"
    # are both lateral joins. However, "FROM customer t1 INNER JOIN customer.orders t2 ON
    # (t1.comment = t2.comment)" is not a lateral join.
    # TODO: consider renaming to is_nested_join
    self.is_lateral_join = False

  def __deepcopy__(self, memo):
    other = JoinClause(
        self.join_type,
        deepcopy(self.table_expr, memo),
        deepcopy(self.boolean_expr, memo))
    other.is_lateral_join = self.is_lateral_join
    return other


class WhereClause(object):
  '''The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  def __init__(self, boolean_expr):
    self.boolean_expr = boolean_expr

  def __deepcopy__(self, memo):
    return WhereClause(deepcopy(self.boolean_expr, memo))


class GroupByClause(object):

  def __init__(self, group_by_items):
    self.group_by_items = group_by_items

  def __deepcopy__(self, memo):
    return GroupByClause([deepcopy(item, memo) for item in self.group_by_items])


class HavingClause(object):
  '''The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  def __init__(self, boolean_expr):
    self.boolean_expr = boolean_expr

  def __deepcopy__(self, memo):
    return HavingClause(deepcopy(self.boolean_expr, memo))


class UnionClause(object):
  '''A representation of a UNION clause.

     If the member variable "all" is True, the instance represents a "UNION ALL".

  '''

  def __init__(self, query):
    self.query = query
    self.all = False

  @property
  def queries(self):
    queries = list()
    query = self.query
    while True:
      queries.append(query)
      if not query.union_clause:
        break
      query = query.union_clause.query
    return queries

  def __deepcopy__(self, memo):
    other = UnionClause(deepcopy(self.query, memo))
    other.all = self.all
    return other


class OrderByClause(object):

  def __init__(self, val_exprs):
    '''val_exprs must be a list containing either ValExprs or a tuple of (ValExpr,
       String). If plain ValExprs are used, the order will be ASC. If tuples are used,
       the string must be either ASC or DESC.
    '''
    self.exprs_to_order = list()
    for item in val_exprs:
      try:
        order = val_exprs[item]
      except TypeError:   # not a dict
        order = 'ASC'
      self.exprs_to_order.append((item, order))

  def __deepcopy__(self, memo):
    other = OrderByClause(val_exprs=list())
    for (item, order) in self.exprs_to_order:
      other.exprs_to_order.append((deepcopy(item, memo), order))
    return other


class LimitClause(object):

  def __init__(self, limit):
    self.limit = limit

  def __deepcopy__(self, memo):
    return LimitClause(deepcopy(self.limit, memo))


class InsertClause(object):

  # This enum represents possibilities for different types of INSERTs. A user of this
  # object, like StatementGenerator, is responsible for setting the conflict_action
  # value appropriately. These values are valid for the conflict_action parameter.
  # Because an InsertStatement is a single piece of data shared across multiple SQL
  # dialects, this setting can alter the written SQL in multiple dialects.
  #
  # CONLICT_ACTION_DEFAULT
  #
  # For Impala, this is a statement like INSERT INTO hdfs_table SELECT * FROM foo
  # For PostgreSQL, this is a statement like INSERT INTO hdfs_table SELECT * FROM foo
  #
  # Example uses cases: inserting into tables that do not have primary keys, or
  # inserting into PostgreSQL tables where you want to error if there are attempts to
  # insert duplicate primary keys
  #
  # CONFLICT_ACTION_IGNORE
  #
  # For Impala, this is a statement like INSERT INTO kudu_table SELECT * FROM foo
  # For PostgreSQL, this is a statement like INSERT INTO kudu_table SELECT * FROM foo
  #                                          ON CONFLICT DO NOTHING
  #
  # Example use case: inserting into Kudu tables, where attempts to insert duplicate
  # primary key rows are ignored by Impala, so they must also be ignored by PostgreSQL.
  # Note that the *syntax* for INSERT doesn't change with Impala, but because it's a
  # Kudu table, the behavior differs.
  #
  # CONFLICT_ACTION_UPDATE
  #
  # For Impala, this is a statement like UPSERT INTO kudu_table SELECT * FROM foo
  # For PostgreSQL, this is a statement like INSERT INTO kudu_table SELECT * FROM foo
  #                                          ON CONFLICT DO UPDATE SET
  #                                          (col1 = EXCLUDED.col1, ...)
  #
  # Example use case: upserting into Kudu tables, where attempts to insert duplicate
  # primary key rows will either insert a single row, or update a single row already
  # there, without error. In PostgreSQL, UPSERT is written via this "ON CONFLICT DO
  # UPDATE" clause.
  #
  # More on PostgreSQL INSERT/UPSERT syntax here:
  # https://www.postgresql.org/docs/9.5/static/sql-insert.html

  (CONFLICT_ACTION_DEFAULT,
   CONFLICT_ACTION_IGNORE,
   CONFLICT_ACTION_UPDATE) = range(3)

  def __init__(self, table, column_list=None, conflict_action=CONFLICT_ACTION_DEFAULT):
    """
    Represent an INSERT/UPSERT clause, which is the first half of an INSERT/UPSERT
    statement. Note that UPSERTs are very similar to INSERTs, so this data structure can
    easily deal with both.

    The table is a Table object.

    column_list is an optional list, tuple, or other sequence of
    tests.comparison.common.Column objects. In an Impala INSERT/UPSERT SQL statement,
    it's a sequence of column names. See
    http://www.cloudera.com/documentation/enterprise/latest/topics/impala_insert.html

    conflict_action takes in one of the CONFLICT_ACTION_* class attributes. See above.
    """
    self.table = table
    self.column_list = column_list
    self.conflict_action = conflict_action


class ValuesRow(object):
  def __init__(self, items):
    """
    Represent a single row in a VALUES clause. The items are literals or expressions.
    """
    self.items = items


class ValuesClause(object):
  def __init__(self, values_rows):
    """
    Represent the VALUES clause of an INSERT/UPSERT statement. The values_rows is a
    sequence of ValuesRow objects.
    """
    self.values_rows = values_rows


class InsertStatement(AbstractStatement):

  def __init__(self, with_clause=None, insert_clause=None, select_query=None,
               values_clause=None, execution=None):
    """
    Represent an INSERT/UPSERT statement. Note that UPSERTs are very similar to INSERTs,
    so this data structure can easily deal with both.

    The INSERT/UPSERT may have an optional WithClause, and then either a SELECT query
    (Query) object from whose rows we INSERT, or a VALUES clause, but not both.

    The execution attribute is used by the discrepancy_searcher to track whether this
    InsertStatement is some sort of setup operation or a true random statement test.
    """
    super(InsertStatement, self).__init__()
    self._select_query = None
    self._values_clause = None
    self.execution = execution
    self.select_query = select_query
    self.values_clause = values_clause
    self.with_clause = with_clause
    self.insert_clause = insert_clause

  @property
  def select_query(self):
    return self._select_query

  @select_query.setter
  def select_query(self, select_query):
    if self.values_clause is None or select_query is None:
      self._select_query = select_query
    else:
      raise Exception('An INSERT/UPSERT statement may not have both the select_query and '
                      'values_clause set: {select}; {values}'.format(
                          select=select_query, values=self.values_clause))

  @property
  def values_clause(self):
    return self._values_clause

  @values_clause.setter
  def values_clause(self, values_clause):
    if self.select_query is None or values_clause is None:
      self._values_clause = values_clause
    else:
      raise Exception('An INSERT/UPSERT statement may not have both the select_query and '
                      'values_clause set: {select}; {values}'.format(
                          select=self.select_query, values=values_clause))

  @property
  def table_exprs(self):
    table_exprs = super(InsertStatement, self).table_exprs  # WITH clause
    if self.select_query is not None:
      table_exprs.extend(self.select_query.table_exprs)
    return table_exprs

  @property
  def nested_queries(self):
    queries = list()
    if self.with_clause is not None:
      for inline_view in self.with_clause.with_clause_inline_views:
        queries.append(inline_view.query)
    if self.select_query is not None:
      queries.append(self.select_query)
      queries.extend(self.select_query.nested_queries)
    return queries

  @property
  def dml_table(self):
    return self.insert_clause.table

  @property
  def conflict_action(self):
    return self.insert_clause.conflict_action

  @property
  def primary_key_string(self):
    return '({primary_key_list})'.format(
        primary_key_list=', '.join(self.insert_clause.table.primary_key_names))

  @property
  def updatable_column_names(self):
    return self.insert_clause.table.updatable_column_names
