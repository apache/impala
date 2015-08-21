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

from copy import deepcopy
from logging import getLogger

from common import Column, TableExpr, TableExprList, ValExpr, ValExprList

LOG = getLogger(__name__)

class Query(object):
  '''A representation of the structure of a SQL query. Only the select_clause and
     from_clause are required for a valid query.
  '''

  def __init__(self):
    self.parent = None
    self.with_clause = None
    self.select_clause = None
    self.from_clause = None
    self.where_clause = None
    self.group_by_clause = None
    self.having_clause = None
    self.union_clause = None
    self.order_by_clause = None
    self.limit_clause = None

  def __deepcopy__(self, memo):
    other = Query()
    memo[self] = other
    other.parent = memo[self.parent] if self.parent in memo else None
    other.with_clause = deepcopy(self.with_clause, memo)
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
    table_exprs = self.from_clause.table_exprs
    if self.with_clause:
      table_exprs += self.with_clause.table_exprs
    return table_exprs

  @property
  def is_nested_query(self):
    return self.parent

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
      queries.extend(subquery.query for subquery in \
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

  def __nonzero__(self):
    try:
      iter(self).next()
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
          idx, item = self_iter.next()
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
          idx, item = self_iter.next()
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
  # XXX: So far it seems fine to use this class for both scalar/non scalar cases but
  #      this could lead to unexpected behavior or be a silent cause of problems...

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
    other.join_clauses = [deepcopy(join_clause, memo) for join_clause in self.join_clauses]
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
    other = OrderByClause(val_exprs = list())
    for (item, order) in self.exprs_to_order:
      other.exprs_to_order.append((deepcopy(item, memo), order))
    return other


class LimitClause(object):

  def __init__(self, limit):
    self.limit = limit

  def __deepcopy__(self, memo):
    return LimitClause(deepcopy(limit, memo))
