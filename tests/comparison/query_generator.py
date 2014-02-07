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

from collections import defaultdict
from copy import deepcopy
from itertools import chain
from random import choice, randint, shuffle

from tests.comparison.model import (
    AGG_FUNCS,
    AggFunc,
    And,
    BINARY_STRING_FUNCS,
    BigInt,
    Boolean,
    Cast,
    Column,
    Count,
    DataType,
    Double,
    Equals,
    Float,
    Floor,
    FromClause,
    Func,
    Greatest,
    GroupByClause,
    HavingClause,
    InlineView,
    Int,
    JoinClause,
    Length,
    MATH_OPERATORS,
    Number,
    Query,
    RELATIONAL_OPERATORS,
    SelectClause,
    SelectItem,
    String,
    Table,
    Timestamp,
    TYPES,
    UNARY_BOOLEAN_FUNCS,
    UnionClause,
    WhereClause,
    WithClause,
    WithClauseInlineView)

def random_boolean():
  '''Return a val that evaluates to True 50% of the time'''
  return randint(0, 1)


def zero_or_more():
  '''The chance of the return val of n is 1 / 2 ^ (n + 1)'''
  val = 0
  while random_boolean():
    val += 1
  return val


def one_or_more():
  return zero_or_more() + 1


def random_non_empty_split(iterable):
  '''Return two non-empty lists'''
  if len(iterable) < 2:
    raise Exception('The iterable must contain at least two items')
  split_index = randint(1, len(iterable) - 1)
  left, right = list(), list()
  for idx, item in enumerate(iterable):
    if idx < split_index:
      left.append(item)
    else:
      right.append(item)
  return left, right


class QueryGenerator(object):

  def create_query(self,
      table_exprs,
      allow_with_clause=True,
      select_item_data_types=None):
    '''Create a random query using various language features.

       The initial call to this method should only use tables in the table_exprs
       parameter, and not  inline views or "with" definitions. The other types of
       table exprs may be added as part of the query generation.

       If select_item_data_types is specified it must be a sequence or iterable of
       DataType. The generated query.select_clause.select_items will have data
       types suitable for use in a UNION.

    '''
    # Make a copy so tables can be added if a "with" clause is used
    table_exprs = list(table_exprs)

    with_clause = None
    if allow_with_clause and randint(1, 10) == 1:
      with_clause = self._create_with_clause(table_exprs)
      table_exprs.extend(with_clause.table_exprs)

    from_clause = self._create_from_clause(table_exprs)

    select_clause = self._create_select_clause(
        from_clause.table_exprs,
        select_item_data_types=select_item_data_types)

    query = Query(select_clause, from_clause)

    if with_clause:
      query.with_clause = with_clause

    if random_boolean():
      query.where_clause = self._create_where_clause(from_clause.table_exprs)

    if select_clause.agg_items and select_clause.non_agg_items:
      query.group_by_clause = GroupByClause(list(select_clause.non_agg_items))

    if randint(1, 10) == 1:
      if select_clause.agg_items:
        self._enable_distinct_on_random_agg_items(select_clause.agg_items)
      else:
        select_clause.distinct = True

    if random_boolean() and (query.group_by_clause or select_clause.agg_items):
      query.having_clause = self._create_having_clause(from_clause.table_exprs)

    if randint(1, 10) == 1:
      select_item_data_types = list()
      for select_item in select_clause.select_items:
        # For numbers, choose the largest possible data type in case a CAST is needed.
        if select_item.val_expr.returns_float:
          select_item_data_types.append(Double)
        elif select_item.val_expr.returns_int:
          select_item_data_types.append(BigInt)
        else:
          select_item_data_types.append(select_item.val_expr.type)
      query.union_clause = UnionClause(self.create_query(
          table_exprs,
          allow_with_clause=False,
          select_item_data_types=select_item_data_types))
      query.union_clause.all = random_boolean()

    return query

  def _create_with_clause(self, table_exprs):
    # Make a copy so newly created tables can be added and made availabele for use in
    # future table definitions.
    table_exprs = list(table_exprs)
    with_clause_inline_views = list()
    for with_clause_inline_view_idx in xrange(one_or_more()):
      query = self.create_query(table_exprs)
      # To help prevent nested WITH clauses from having entries with the same alias,
      # choose a random alias. Of course it would be much better to know which aliases
      # were already chosen but that information isn't easy to get from here.
      with_clause_alias = 'with_%s_%s' % \
          (with_clause_inline_view_idx + 1, randint(1, 1000))
      with_clause_inline_view = WithClauseInlineView(query, with_clause_alias)
      table_exprs.append(with_clause_inline_view)
      with_clause_inline_views.append(with_clause_inline_view)
    return WithClause(with_clause_inline_views)

  def _create_select_clause(self, table_exprs, select_item_data_types=None):
    while True:
      non_agg_items = [self._create_non_agg_select_item(table_exprs)
                       for _ in xrange(zero_or_more())]
      agg_items = [self._create_agg_select_item(table_exprs)
                   for _ in xrange(zero_or_more())]
      if non_agg_items or agg_items:
        if select_item_data_types:
          if len(select_item_data_types) > len(non_agg_items) + len(agg_items):
            # Not enough items generated, try again
            continue
          while len(select_item_data_types) < len(non_agg_items) + len(agg_items):
            items = choice([non_agg_items, agg_items])
            if items:
              items.pop()
          for data_type_idx, data_type in enumerate(select_item_data_types):
            if data_type_idx < len(non_agg_items):
              item = non_agg_items[data_type_idx]
            else:
              item = agg_items[data_type_idx - len(non_agg_items)]
            if not issubclass(item.type, data_type):
              item.val_expr = self.convert_val_expr_to_type(item.val_expr, data_type)
        for idx, item in enumerate(chain(non_agg_items, agg_items)):
          item.alias = '%s_col_%s' % (item.type.__name__.lower(), idx + 1)
        return SelectClause(non_agg_items=non_agg_items, agg_items=agg_items)

  def _choose_col(self, table_exprs):
    table_expr = choice(table_exprs)
    return choice(table_expr.cols)

  def _create_non_agg_select_item(self, table_exprs):
    return SelectItem(self._create_val_expr(table_exprs))

  def _create_val_expr(self, table_exprs):
    vals = [self._choose_col(table_exprs) for _ in xrange(one_or_more())]
    return self._combine_val_exprs(vals)

  def _create_agg_select_item(self, table_exprs):
    vals = [self._create_agg_val_expr(table_exprs) for _ in xrange(one_or_more())]
    return SelectItem(self._combine_val_exprs(vals))

  def _create_agg_val_expr(self, table_exprs):
    val = self._create_val_expr(table_exprs)
    if issubclass(val.type, Number):
      funcs = list(AGG_FUNCS)
    else:
      funcs = [Count]
    return choice(funcs)(val)

  def _create_from_clause(self, table_exprs):
    table_expr = self._create_table_expr(table_exprs)
    table_expr_count = 1
    table_expr.alias = 't%s' % table_expr_count
    from_clause = FromClause(table_expr)
    for join_idx in xrange(zero_or_more()):
      join_clause = self._create_join_clause(from_clause, table_exprs)
      table_expr_count += 1
      join_clause.table_expr.alias = 't%s' % table_expr_count
      from_clause.join_clauses.append(join_clause)
    return from_clause

  def _create_table_expr(self, table_exprs):
    if randint(1, 10) == 1:
      return self._create_inline_view(table_exprs)
    return self._choose_table(table_exprs)

  def _choose_table(self, table_exprs):
    return deepcopy(choice(table_exprs))

  def _create_inline_view(self, table_exprs):
    return InlineView(self.create_query(table_exprs))

  def _create_join_clause(self, from_clause, table_exprs):
    table_expr = self._create_table_expr(table_exprs)
    # Increase the chance of using the first join type which is INNER
    join_type_idx = (zero_or_more() / 2) % len(JoinClause.JOINS_TYPES)
    join_type = JoinClause.JOINS_TYPES[join_type_idx]
    join_clause = JoinClause(join_type, table_expr)

    # Prefer non-boolean cols for the first condition. Boolean cols produce too
    # many results so it's unlikely that someone would want to join tables only using
    # boolean cols.
    non_boolean_types = set(type_ for type_ in TYPES if not issubclass(type_, Boolean))

    if join_type != 'CROSS':
      join_clause.boolean_expr = self._combine_val_exprs(
          [self._create_relational_join_condition(
               table_expr,
               choice(from_clause.table_exprs),
               prefered_data_types=(non_boolean_types if idx == 0 else set()))
           for idx in xrange(one_or_more())],
          resulting_type=Boolean)
    return join_clause

  def _create_relational_join_condition(self,
      left_table_expr,
      right_table_expr,
      prefered_data_types):
    # "base type" means condense all int types into just int, same for floats
    left_cols_by_base_type = left_table_expr.cols_by_base_type
    right_cols_by_base_type = right_table_expr.cols_by_base_type
    common_col_types = set(left_cols_by_base_type) & set(right_cols_by_base_type)
    if prefered_data_types:
      common_col_types &= prefered_data_types
    if common_col_types:
      col_type = choice(list(common_col_types))
      left = choice(left_cols_by_base_type[col_type])
      right = choice(right_cols_by_base_type[col_type])
    else:
      col_type = None
      if prefered_data_types:
        for available_col_types in (left_cols_by_base_type, right_cols_by_base_type):
          prefered_available_col_types = set(available_col_types) & prefered_data_types
          if prefered_available_col_types:
            col_type = choice(list(prefered_available_col_types))
            break
      if not col_type:
        col_type = choice(left_cols_by_base_type.keys())

      if col_type in left_cols_by_base_type:
        left = choice(left_cols_by_base_type[col_type])
      else:
        left = choice(choice(left_cols_by_base_type.values()))
        left = self.convert_val_expr_to_type(left, col_type)
      if col_type in right_cols_by_base_type:
        right = choice(right_cols_by_base_type[col_type])
      else:
        right = choice(choice(right_cols_by_base_type.values()))
        right = self.convert_val_expr_to_type(right, col_type)
    return Equals(left, right)

  def _create_where_clause(self, table_exprs):
    boolean_exprs = list()
    # Create one boolean expr per iteration...
    for _ in xrange(one_or_more()):
      col_type = None
      cols = list()
      # ...using one or more cols...
      for _ in xrange(one_or_more()):
        # ...from any random table, inline view, etc.
        table_expr = choice(table_exprs)
        if not col_type:
          col_type = choice(list(table_expr.cols_by_base_type))
        if col_type in table_expr.cols_by_base_type:
          col = choice(table_expr.cols_by_base_type[col_type])
        else:
          col = choice(table_expr.cols)
        cols.append(col)
      boolean_exprs.append(self._combine_val_exprs(cols, resulting_type=Boolean))
    return WhereClause(self._combine_val_exprs(boolean_exprs))

  def _combine_val_exprs(self, vals, resulting_type=None):
    '''Combine the given vals into a single val.

       If resulting_type is specified, the returned val will be of that type. If
       the resulting data type was not specified, it will be randomly chosen from the
       types of the input vals.

    '''
    if not vals:
      raise Exception('At least one val is required')

    types_to_vals = DataType.group_by_base_type(vals)

    if not resulting_type:
      resulting_type = choice(types_to_vals.keys())

    vals_of_resulting_type = list()

    for val_type, vals in types_to_vals.iteritems():
      if issubclass(val_type, resulting_type):
        vals_of_resulting_type.extend(vals)
      elif resulting_type == Boolean:
        # To produce other result types, the vals will be aggd into a single val
        # then converted into the desired type. However to make a boolean, relational
        # operaters can be used on the vals to make a more realistic query.
        val = self._create_boolean_expr_from_vals_of_same_type(vals)
        vals_of_resulting_type.append(val)
      else:
        val = self._combine_vals_of_same_type(vals)
        if not (issubclass(val.type, Number) and issubclass(resulting_type, Number)):
          val = self.convert_val_expr_to_type(val, resulting_type)
        vals_of_resulting_type.append(val)

    return self._combine_vals_of_same_type(vals_of_resulting_type)

  def _create_boolean_expr_from_vals_of_same_type(self, vals):
    if not vals:
      raise Exception('At least one val is required')

    if len(vals) == 1:
      val = vals[0]
      if Boolean == val.type:
        return val
      # Convert a single non-boolean val into a boolean using a func like
      # IsNull or IsNotNull.
      return choice(UNARY_BOOLEAN_FUNCS)(val)
    if len(vals) == 2:
      left, right = vals
      if left.type == right.type:
        if left.type == String:
          # Databases may vary in how string comparisons are done. Results may differ
          # when using operators like > or <, so just always use =.
          return Equals(left, right)
        if left.type == Boolean:
          # TODO: Enable "OR" at some frequency, using OR at 50% will probably produce
          #       too many slow queries.
          return And(left, right)
        # At this point we've got two data points of the same type so any valid
        # relational operator is valid and will produce a boolean.
        return choice(RELATIONAL_OPERATORS)(left, right)
      elif issubclass(left.type, Number) and issubclass(right.type, Number):
        # Numbers need not be of the same type. SmallInt, BigInt, etc can all be compared.
        # Note: For now ints are the only numbers enabled and division is disabled
        #       though AVG() is in use. If floats are enabled this will likely need to be
        #       updated to do some rounding based comparison.
        return choice(RELATIONAL_OPERATORS)(left, right)
      raise Exception('Vals are not of the same type: %s<%s> vs %s<%s>'
          % (left, left.type, right, right.type))
    # Reduce the number of inputs and try again...
    left_subset, right_subset = random_non_empty_split(vals)
    return self._create_boolean_expr_from_vals_of_same_type([
        self._combine_vals_of_same_type(left_subset),
        self._combine_vals_of_same_type(right_subset)])

  def _combine_vals_of_same_type(self, vals):
    '''Combine the given vals into a single expr of the same type. The input
       vals must be of the same base data type. For example Int's must not be mixed
       with Strings.

    '''
    if not vals:
      raise Exception('At least one val is required')

    val_type = None
    for val in vals:
      if not val_type:
        if issubclass(val.type, Number):
          val_type = Number
        else:
          val_type = val.type
      elif not issubclass(val.type, val_type):
        raise Exception('Incompatable types %s and %s' % (val_type, val.type))

    if len(vals) == 1:
      return vals[0]

    if val_type == Number:
      funcs = MATH_OPERATORS
    elif val_type == Boolean:
      # TODO: Enable "OR" at some frequency
      funcs = [And]
    elif val_type == String:
      funcs = BINARY_STRING_FUNCS
      return vals[0]
    elif val_type == Timestamp:
      funcs = [Greatest]

    vals = list(vals)
    shuffle(vals)
    left = vals.pop()
    right = vals.pop()
    while True:
      func = choice(funcs)
      left = func(left, right)
      if not vals:
        return left
      right = vals.pop()

  def convert_val_expr_to_type(self, val_expr, resulting_type):
    if resulting_type not in TYPES:
      raise Exception('Unexpected type: {}'.format(resulting_type))
    val_type = val_expr.type
    if issubclass(val_type, resulting_type):
      return val_expr

    if issubclass(resulting_type, Int):
      if val_expr.returns_float:
        # Impala will FLOOR while Postgresql will ROUND. Use FLOOR to be conistent.
        return Floor(val_expr)
    if issubclass(resulting_type, Number):
      if val_expr.returns_string:
        return Length(val_expr)
    if issubclass(resulting_type, String):
      if val_expr.returns_float:
        # Different databases may use different precision.
        return Cast(Floor(val_expr), resulting_type)

    return Cast(val_expr, resulting_type)

  def _create_having_clause(self, table_exprs):
    boolean_exprs = list()
    # Create one boolean expr per iteration...
    for _ in xrange(one_or_more()):
      agg_items = list()
      # ...using one or more agg exprs...
      for _ in xrange(one_or_more()):
        vals = [self._create_agg_val_expr(table_exprs) for _ in xrange(one_or_more())]
        agg_items.append(self._combine_val_exprs(vals))
      boolean_exprs.append(self._combine_val_exprs(agg_items, resulting_type=Boolean))
    return HavingClause(self._combine_val_exprs(boolean_exprs))

  def _enable_distinct_on_random_agg_items(self, agg_items):
    '''Randomly choose an agg func and set it to use DISTINCT'''
    # Impala has a limitation where 'DISTINCT' may only be applied to one agg
    # expr. If an agg expr is used more than once, each usage may
    # or may not include DISTINCT.
    #
    # Examples:
    #   OK: SELECT COUNT(DISTINCT a) + SUM(DISTINCT a) + MAX(a)...
    #   Not OK: SELECT COUNT(DISTINCT a) + COUNT(DISTINCT b)...
    #
    # Given a select list like:
    #   COUNT(a), SUM(a), MAX(b)
    #
    # We want to ouput one of:
    #   COUNT(DISTINCT a), SUM(DISTINCT a), AVG(b)
    #   COUNT(DISTINCT a), SUM(a), AVG(b)
    #   COUNT(a), SUM(a), AVG(DISTINCT b)
    #
    # This will be done by first grouping all agg funcs by their inner
    # expr:
    #   {a: [COUNT(a), SUM(a)],
    #    b: [MAX(b)]}
    #
    # then choosing a random val (which is a list of aggs) in the above dict, and
    # finaly randomly adding DISTINCT to items in the list.
    exprs_to_funcs =  defaultdict(list)
    for item in agg_items:
      for expr, funcs in self._group_agg_funcs_by_expr(item.val_expr).iteritems():
        exprs_to_funcs[expr].extend(funcs)
    funcs = choice(exprs_to_funcs.values())
    for func in funcs:
      if random_boolean():
        func.distinct = True

  def _group_agg_funcs_by_expr(self, val_expr):
    '''Group exprs and return a dict mapping the expr to the agg items
       it is used in.

       Example: COUNT(a) * SUM(a) - MAX(b) + MIN(c) -> {a: [COUNT(a), SUM(a)],
                                                        b: [MAX(b)],
                                                        c: [MIN(c)]}

    '''
    exprs_to_funcs = defaultdict(list)
    if isinstance(val_expr, AggFunc):
      exprs_to_funcs[tuple(val_expr.args)].append(val_expr)
    elif isinstance(val_expr, Func):
      for arg in val_expr.args:
        for expr, funcs in self._group_agg_funcs_by_expr(arg).iteritems():
          exprs_to_funcs[expr].extend(funcs)
    # else: The remaining case could happen if the original expr was something like
    #       "SUM(a) + b + 1" where b is a GROUP BY field.
    return exprs_to_funcs


if __name__ == '__main__':
  '''Generate some queries for manual inspection. The query won't run anywhere because the
     tables used are fake. To make real queries, we'd need to connect to a database and
     read the table metadata and such.
  '''
  tables = list()
  data_types = TYPES
  data_types.remove(Float)
  data_types.remove(Double)
  for table_idx in xrange(5):
    table = Table('table_%s' % table_idx)
    tables.append(table)
    for col_idx in xrange(3):
      col_type = choice(data_types)
      col = Column(table, '%s_col_%s' % (col_type.__name__.lower(), col_idx), col_type)
      table.cols.append(col)

  query_generator = QueryGenerator()
  from model_translator import SqlWriter
  sql_writer = SqlWriter.create()
  for _ in range(3000):
    query = query_generator.create_query(tables)
    print(sql_writer.write_query(query) + '\n')
