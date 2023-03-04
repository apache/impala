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
from builtins import filter, range
from collections import defaultdict
from copy import deepcopy
from logging import getLogger
from random import shuffle, choice, randint, randrange

from tests.comparison.common import TableExprList, ValExpr, ValExprList, Table, Column
from tests.comparison.query_profile import DefaultProfile
from tests.comparison.funcs import (
    AGG_FUNCS,
    AggFunc,
    ANALYTIC_FUNCS,
    AnalyticFunc,
    And,
    Coalesce,
    Equals,
    Func,
    FUNCS,
    PartitionByClause,
    WindowBoundary,
    WindowClause)
from tests.comparison.db_types import (
    Boolean,
    Int,
    Float,
    TYPES)
from tests.comparison.query import (
    FromClause,
    GroupByClause,
    HavingClause,
    InlineView,
    JoinClause,
    LimitClause,
    OrderByClause,
    Query,
    SelectClause,
    SelectItem,
    Subquery,
    UnionClause,
    WhereClause,
    WithClause,
    WithClauseInlineView)

UNBOUNDED_PRECEDING = WindowBoundary.UNBOUNDED_PRECEDING
PRECEDING = WindowBoundary.PRECEDING
CURRENT_ROW = WindowBoundary.CURRENT_ROW
FOLLOWING = WindowBoundary.FOLLOWING
UNBOUNDED_FOLLOWING = WindowBoundary.UNBOUNDED_FOLLOWING

LOG = getLogger(__name__)


class QueryGenerator(object):

  def __init__(self, query_profile):
    self.profile = query_profile
    self.queries_under_construction = list()
    self.max_nested_query_count = None
    self.cur_id = 0

  def get_next_id(self):
    self.cur_id += 1
    return 'a' + str(self.cur_id)

  @property
  def current_query(self):
    if self.queries_under_construction:
      return self.queries_under_construction[-1]

  @property
  def root_query(self):
    if self.queries_under_construction:
      return self.queries_under_construction[0]

  def clear_state(self):
    # This function clears the state from the previous query.
    self.cur_id = 0
    self.profile.query = None
    self.max_nested_query_count = None

  def generate_statement(self,
      table_exprs,
      allow_with_clause=True,
      allow_union_clause=True,
      select_item_data_types=None,
      required_select_item_type=None,
      required_table_expr_col_type=None,
      require_aggregate=None,
      dml_table=None,
      table_alias_prefix='t'):
    '''Create a random query using various language features.

       The initial call to this method should only use tables in the table_exprs
       parameter, and not  inline views or WITH definitions. The other types of
       table exprs may be added as part of the query generation.

       Due to implementation limitations, nested queries are not distributed evenly by
       default even if the query profile assigns an equal likelihood of use to each
       possible nested query decision. This is because the implementation chooses nested
       queries in a fixed order (WITH -> FROM -> WHERE). For example if only one nested
       query were allowed and each clause had a 50% chance of using a subquery, the
       resulting set of generated queries would contain a subquery in the WITH clause
       50% of the time, in the FROM 25% of the time, and WHERE 12.5% of the time. If the
       max nested queries were two, it's most likely that the generated query would
       contain a WITH clause which has the second nested query within it....

       If select_item_data_types is specified it must be a sequence or iterable of
       DataType. The generated query.select_clause.select will have data types suitable
       for use in a UNION.

       required_select_item_type may be set to a DataType to force at least one of the
       SELECT items to be of the given type. This can be used to ensure that inline
       views will have at least one joinable column.

       required_table_expr_col_type may be set to ensure that at least one of the
       TableExprs used in the FROM clause will have a column of the given type. This
       can be used to unsure that a correlation condition can be made for a Subquery.

       require_aggregate can be set to True or False to force or disable the creation
       of an aggregate query. This is used during Subquery creation where the context
       may require an aggregate or non-aggregate.

       dml_table exists for kwargs compatibility with other statement generators but is
       ignored
    '''
    if not table_exprs:
      raise Exception("At least one TableExpr is needed")

    query = Query()
    query.parent = self.current_query
    self.queries_under_construction.append(query)
    self.profile.query = query

    # Make a copy so tables can be added if a WITH clause is used
    table_exprs = TableExprList(table_exprs)

    if self.max_nested_query_count is None:
      self.max_nested_query_count = self.profile.get_max_nested_query_count()
    elif self.max_nested_query_count == 0:
      raise Exception('Maximum nested query count exceeded')
    else:
      self.max_nested_query_count -= 1

    with_clause = None
    if allow_with_clause \
        and self.allow_more_nested_queries \
        and self.profile.use_with_clause():
      with_clause = self._create_with_clause(table_exprs)
      table_exprs.extend(with_clause.table_exprs)
      query.with_clause = with_clause

    from_clause = self._create_from_clause(
        table_exprs, table_alias_prefix, required_table_expr_col_type)
    query.from_clause = from_clause

    select_clause = self._create_select_clause(
        from_clause.visible_table_exprs,
        select_item_data_types,
        required_select_item_type,
        require_aggregate)
    query.select_clause = select_clause

    if self.profile.use_where_clause():
      query.where_clause = self._create_where_clause(
          from_clause.visible_table_exprs, table_alias_prefix)

    # If agg and non-agg SELECT items are present then a GROUP BY is required otherwise
    # it's optional and is effectively a "SELECT DISTINCT".
    if (select_clause.agg_items and select_clause.basic_items) \
        or (require_aggregate is None \
            and select_clause.basic_items \
            and not select_clause.analytic_items \
            and not select_clause.contains_approximate_types \
            and self.profile.use_group_by_clause()):
      group_by_items = [deepcopy(item) for item in select_clause.basic_items
                        if not item.val_expr.is_constant]
      # TODO: What if there are select_clause.analytic_items?
      if group_by_items:
        query.group_by_clause = GroupByClause(group_by_items)

    # Impala doesn't support DISTINCT with analytics or "SELECT DISTINCT" when
    # GROUP BY is used.
    if not select_clause.analytic_items \
        and not query.group_by_clause \
        and not select_clause.contains_approximate_types \
        and self.profile.use_distinct():
      if select_clause.agg_items:
        self._enable_distinct_on_random_agg_items(select_clause.agg_items)
      else:
        select_clause.distinct = True

    if self.profile.use_having_clause() \
        and (query.group_by_clause
             or (self.profile.use_having_without_groupby()
             and select_clause.agg_items)):
      basic_select_item_exprs = \
          ValExprList(item.val_expr for item in select_clause.basic_items)
      query.having_clause = self._create_having_clause(
          from_clause.visible_table_exprs, basic_select_item_exprs)

    if allow_union_clause \
        and self.allow_more_nested_queries \
        and self.profile.use_union_clause():
      data_type_candidates_by_base_type = defaultdict(list)
      for data_type in TYPES:
        data_type_candidates_by_base_type[data_type.get_base_type()].append(data_type)
      select_item_data_types = list()
      for select_item in select_clause.items:
        select_item_data_types.append(
            choice(data_type_candidates_by_base_type[select_item.val_expr.base_type]))
      query.union_clause = UnionClause(self.generate_statement(
          table_exprs,
          allow_with_clause=False,
          select_item_data_types=select_item_data_types))
      if select_clause.contains_approximate_types or any(
          q.select_clause.contains_approximate_types for q in query.union_clause.queries):
        query.union_clause.all = True
      else:
        query.union_clause.all = self.profile.use_union_all()

    self.queries_under_construction.pop()
    if self.queries_under_construction:
      self.profile.query = self.queries_under_construction[-1]
    else:
      self.clear_state()

    return query

  @property
  def allow_more_nested_queries(self):
    return self.max_nested_query_count > 0

  def _create_with_clause(self, table_exprs):
    # Make a copy so newly created tables can be added and made available for use in
    # future table definitions.
    table_exprs = TableExprList(table_exprs)
    with_clause_inline_views = TableExprList()
    for with_clause_inline_view_idx \
        in range(self.profile.get_with_clause_table_ref_count()):
      query = self.generate_statement(table_exprs,
                                      allow_with_clause=self.profile.use_nested_with())
      with_clause_alias_count = getattr(self.root_query, 'with_clause_alias_count', 0) + 1
      self.root_query.with_clause_alias_count = with_clause_alias_count
      with_clause_inline_view = \
          WithClauseInlineView(query, 'with_%s' % with_clause_alias_count)
      table_exprs.append(with_clause_inline_view)
      with_clause_inline_views.append(with_clause_inline_view)
      if not self.allow_more_nested_queries:
        break
    return WithClause(with_clause_inline_views)

  def _create_select_clause(self,
      table_exprs,
      select_item_data_types,
      required_select_item_type,
      require_aggregate):
    if select_item_data_types:
      select_item_data_types = tuple(select_item_data_types)
    if select_item_data_types \
        and required_select_item_type \
        and not issubclass(required_select_item_type, select_item_data_types):
      raise Exception('Required select item type is not in allowed types')

    # Generate column types for the select clause if not specified.
    if not select_item_data_types:
      select_item_data_types = []
      desired_item_count = self.profile.get_select_item_count()
      if required_select_item_type:
        select_item_data_types.append(required_select_item_type)
      while len(select_item_data_types) < desired_item_count:
        select_item_data_types.append(self.profile.choose_type(table_exprs.col_types))
      shuffle(select_item_data_types)
      select_item_data_types = tuple(select_item_data_types)

    # We want to assign BASIC, AGG or ANALYTIC to each column. We want to prevent GROUP BY
    # float_col, because the groupings will differ across databases because floats are
    # approximate values.
    column_categories = ['-' for _ in select_item_data_types]
    if require_aggregate:
      column_categories[randint(0, len(column_categories) - 1)] = 'AGG'
    # Assign AGG and ANALYTIC some columns
    for i in range(len(column_categories)):
      item_category = self.profile._choose_from_weights(
          self.profile.weights('SELECT_ITEM_CATEGORY'))
      if item_category in ('AGG', 'ANALYTIC'):
        column_categories[i] = item_category
    agg_present = 'AGG' in column_categories
    # Assign BASIC to the remaining columns
    for i in range(len(column_categories)):
      if column_categories[i] == '-':
        if select_item_data_types[i].is_approximate() and agg_present:
          column_categories[i] = 'AGG'
        else:
          # If AGG column is present, BASIC can only be assigned to a column if it's not a
          # Float.
          column_categories[i] = 'BASIC'

    select_items = []

    for i, column_category in enumerate(column_categories):
      if column_category == 'BASIC':
        select_items.append(self._create_basic_select_item(
          table_exprs, select_item_data_types[i]))
      else:
        select_items.append((column_category, select_item_data_types[i]))

    analytic_count = sum(1 for c in column_category if c == 'ANALYTIC')

    select_item_exprs = \
        ValExprList(item.val_expr for item in select_items if type(item) == SelectItem)
    for idx, item in enumerate(select_items):
      if type(item) == tuple and item[0] == 'AGG':
        select_items[idx] = \
            self._create_agg_select_item(table_exprs, select_item_exprs, item[1])

    for item in select_items:
      if type(item) == tuple:
        continue
      if item.is_agg:
        select_item_exprs.append(item.val_expr)
    for idx, item in enumerate(select_items):
      if type(item) == tuple:
        select_items[idx] = self._create_analytic_select_item(
            table_exprs,
            select_item_exprs,
            len(select_item_exprs) == 0 and analytic_count == 1,
            item[1])

    # So far all the SELECT items are defined and set but none of them have aliases. If
    # an item is a simple column reference, then it will only get an alias if there is a
    # conflict with another simple column ref. All other item types, such as functions
    # or constants, will always have an alias.
    #
    # item_names is the set of final names or aliases for each item in the SELECT list.
    item_names = set()
    for item in select_items:
      if not item.alias and not item.val_expr.is_col:
        continue
      if item.name in item_names:
        item.alias = '*CONFLICT*'
      else:
        item_names.add(item.name)

    # base_item_name_counts stores the number of conflicts that occurred for a name,
    # and hence the number of name to skip forward to create a non-conflicting name.
    base_item_name_counts = defaultdict(int)
    for item in select_items:
      if item.alias == '*CONFLICT*' or (not item.val_expr.is_col and not item.alias):
        # Use names close to the Impala functional test database so that bugs in
        # resolution will be more likely to surface.
        alias = base_alias = '%s_col' % item.type.__name__.lower()
        while alias in item_names:
          base_item_name_counts[base_alias] += 1
          alias = base_alias + '_' + str(base_item_name_counts[base_alias])
        item.alias = alias
        item_names.add(alias)
    return SelectClause(select_items)

  def _create_basic_select_item(self, table_exprs, return_type):
    max_children = self.profile.choose_nested_expr_count()
    if max_children:
      value = self.create_func_tree(return_type)
      value = self.populate_func_with_vals(value, table_exprs)
    elif return_type in table_exprs.col_types:
      value = self.profile.choose_val_expr(table_exprs.cols_by_type[return_type])
    else:
      value = self.profile.choose_constant(return_type)
    return SelectItem(value)

  def create_func_tree(self, return_type, allow_subquery=False):
    '''Returns an instance of a basic function that has all of it's arguments either set
       to None or another instance of a function that has it's arguments set likewise. The
       caller should replace the None values with column references or constants as
       desired. The depth of the tree is determined by the query profile (self.profile).
    '''
    signatures = self._funcs_to_allowed_signatures(FUNCS)
    root_signatures = self._find_matching_signatures(
        signatures, returns=return_type, allow_subquery=allow_subquery)
    root_signature = self.profile.choose_func_signature(root_signatures)
    func = root_signature.func(root_signature)   # An instance of a function
    max_children = self.profile.choose_nested_expr_count()
    if max_children:
      # Impala does not allow functions that contain subqueries to have arguments
      # that contain subqueries. Ex: ... WHERE (int_col IN (SELECT 1)) IN (SELECT TRUE)
      subquery_allowed_null_args = list()
      subquery_not_allowed_null_args = list()
      if func.contains_subquery:
        null_args = subquery_not_allowed_null_args
      else:
        null_args = subquery_allowed_null_args
      null_args.extend((func, idx) for idx, arg in enumerate(func.args)
                       if type(arg) != list and arg.val is None)
      while max_children \
          and (subquery_allowed_null_args or subquery_not_allowed_null_args):
        idx = randrange(
            len(subquery_allowed_null_args) + len(subquery_not_allowed_null_args))
        if idx < len(subquery_allowed_null_args):
          null_args = subquery_allowed_null_args
        else:
          null_args = subquery_not_allowed_null_args
        shuffle(null_args)
        parent_func, parent_arg_idx = null_args.pop()
        child_signatures = self._find_matching_signatures(
            signatures,
            returns=parent_func.args[parent_arg_idx].type,
            allow_subquery=(allow_subquery and null_args == subquery_allowed_null_args))
        child_signature = self.profile.choose_func_signature(child_signatures)
        child_func = child_signature.func(child_signature)
        parent_func.args[parent_arg_idx] = child_func
        if child_func.contains_subquery:
          null_args = subquery_not_allowed_null_args
        else:
          null_args = subquery_allowed_null_args
        null_args.extend((child_func, idx) for idx, arg in enumerate(child_func.args)
                         if type(arg) != list and arg.val is None)
        max_children -= 1
    return func

  def _funcs_to_allowed_signatures(self, funcs):
    '''Return a list of the signatures contained in "funcs" that are eligible for use
       based on the query profile.
    '''
    return [signature for func in funcs for signature in func.signatures()
            if self.profile.allow_func_signature(signature)]

  def _find_matching_signatures(self,
      signatures,
      returns=None,
      accepts=None,
      accepts_only=None,
      allow_subquery=False):
    '''Returns the subset of signatures matching the given criteria.
         returns: The signature must return this type.
         accepts: The signature must have at least one argument of this type.
         accepts_only: The signature must have arguments of only this type.
         allow_subquery: If False, the signature cannot contain a subquery.
    '''
    matching_signatures = list()
    for signature in signatures:
      if returns and not issubclass(signature.return_type, returns):
        continue
      if accepts and not any(not arg.is_subquery and issubclass(arg.type, accepts)
                             for arg in signature.args):
        continue
      if accepts_only and (not signature.args or any(
          not arg.is_subquery and not issubclass(arg.type, accepts_only)
          for arg in signature.args)):
        continue
      if not allow_subquery and any(arg.is_subquery for arg in signature.args):
        continue
      matching_signatures.append(signature)
    return matching_signatures

  def populate_func_with_vals(self,
      func,
      table_exprs=TableExprList(),
      val_exprs=ValExprList(),
      table_alias_prefix='',
      allow_subquery=False,
      _allow_table_exprs=None):
    if not _allow_table_exprs and func.is_agg:
      _allow_table_exprs = True
    elif _allow_table_exprs is None and func.contains_agg:
      _allow_table_exprs = False
    elif _allow_table_exprs is None:
      _allow_table_exprs = True
    # If a function's return type depends on some of its args then at least one of those
    # args must not be the NULL literal. Example: IF(false, NULL, NULL) is considered
    # invalid because the return type cannot be determined.
    has_non_null_literal_arg = False
    for idx, arg in enumerate(func.args):
      signature_arg = func.signature.args[idx]
      if signature_arg.is_subquery \
          or (allow_subquery \
              and self.allow_more_nested_queries \
              and self.profile.use_scalar_subquery()):
        usage = self.profile.choose_subquery_predicate_category(
            func.name(),
            self.current_query.from_clause.table_exprs.joinable_cols_by_type)
        if usage is not None \
            and self.allow_more_nested_queries \
            and (usage[1] == 'UNCORRELATED'
                or self.current_query.from_clause.table_exprs.joinable_cols_by_type):
          use_scalar_subquery = (usage[0] == 'Scalar')
          use_agg_subquery = (usage[1] == 'AGG')
          use_correlated_subquery = (usage[2] == 'CORRELATED')
          if use_correlated_subquery:
            # TODO: Sometimes this causes an exception because the list is empty
            join_expr_type = self.profile.choose_type(list(
                self.current_query.from_clause.table_exprs.joinable_cols_by_type))
          else:
            join_expr_type = None
          select_item_data_types = \
              [signature_arg.type] if use_scalar_subquery else signature_arg.type
          query = self.generate_statement(
              table_exprs,
              select_item_data_types=select_item_data_types,
              required_table_expr_col_type=join_expr_type,
              require_aggregate=use_agg_subquery,
              # Don't use UNION + LIMIT; IMPALA-1379
              allow_union_clause=(not signature_arg.is_subquery),
              table_alias_prefix=(table_alias_prefix +
                  ('t' if use_correlated_subquery else '')),
              allow_with_clause=self.profile.use_nested_with())
          if use_scalar_subquery and not use_agg_subquery:
            # Impala will assume the query will return more than one row unless a LIMIT 1
            # is added. An ORDER BY will also be added under the assumption that we want
            # deterministic results.
            query.order_by_clause = OrderByClause([Int(1)])
            query.limit_clause = LimitClause(Int(1))
          if use_correlated_subquery:
            outer_table_expr = choice(
                self.current_query.from_clause.table_exprs.by_col_type[join_expr_type])
            correlation_condition = self._create_relational_join_condition(
                outer_table_expr,
                query.from_clause.table_exprs.by_col_type[join_expr_type])
            if query.where_clause:
              query.where_clause.boolean_expr = And.create_from_args(
                  query.where_clause.boolean_expr, correlation_condition)
            else:
              query.where_clause = WhereClause(correlation_condition)
          func.args[idx] = Subquery(query)
        else:
          replacement_func = self.create_func_tree(func.type)
          return self.populate_func_with_vals(
              replacement_func,
              table_exprs=table_exprs,
              val_exprs=val_exprs,
              table_alias_prefix=table_alias_prefix,
              allow_subquery=allow_subquery,
              _allow_table_exprs=_allow_table_exprs)
      else:
        if arg.is_constant and arg.val is None:
          candidate_val_exprs = ValExprList()
          if val_exprs:
            candidate_val_exprs.extend(val_exprs.by_type[arg.type])
          if _allow_table_exprs:
            candidate_val_exprs.extend(table_exprs.cols_by_type[arg.type])
          if candidate_val_exprs:
            val = self.profile.choose_val_expr(candidate_val_exprs)
          else:
            val = self.profile.choose_constant(
                return_type=arg.type,
                allow_null=(signature_arg.can_be_null \
                    and signature_arg.can_be_null_literal \
                    and (has_non_null_literal_arg \
                        or not signature_arg.determines_signature)))
          func.args[idx] = val
          arg = val
        elif arg.is_func:
          func.args[idx] = self.populate_func_with_vals(
              arg,
              table_exprs=table_exprs,
              val_exprs=val_exprs,
              _allow_table_exprs=_allow_table_exprs)
        if not signature_arg.can_be_null and not arg.is_constant:
          val = self.profile.choose_constant(return_type=arg.type, allow_null=False)
          func.args[idx] = Coalesce.create_from_args(arg, val)
        if not arg.is_constant or arg.val is not None:
          has_non_null_literal_arg = True
    return func

  def _create_agg_select_item(self, table_exprs, basic_select_item_exprs, return_type):
    value = self._create_agg_func_tree(return_type)
    value = self.populate_func_with_vals(value, table_exprs, basic_select_item_exprs)
    return SelectItem(value)

  def _create_agg_func_tree(self, return_type):
    return self._create_agg_or_analytic_tree(return_type, agg_funcs=AGG_FUNCS)

  def _create_agg_or_analytic_tree(self, return_type, agg_funcs=[], analytic_funcs=[],
                                   basic_funcs=FUNCS):
    '''Returns an instance of a function that is guaranteed to either be or contain an
       aggregate or analytic function. The arguments of the returned function will either
       be None or an instance of a function as in create_func_tree.

       The chosen aggregate or analytic functions will be restricted to the list of
       functions in agg_funcs and analytic_funcs.

       If analytic_funcs is non-empty the returned function will be guaranteed to
       be an analytic or contain at least on analytic function.

       return_type must be set and refers to the data type of the function output.
       agg_funcs and analytic_funcs should be used to determine the class of the
       returned function. The caller is responsible for restricting the return_type
       to types that can be generated by permutations of available functions. If the max
       nested expr count in the query profile is at least one, then any return_type
       should be possible to generate but this is not guaranteed.

       basic_funcs is a list of allowed basic functions that will be used in the function
       tree. By default, all basic funcs defined in funcs.py will be allowed. A basic
       function is any non-aggregate, non-analytic function.
    '''
    # The creation of aggregate and analytic functions is so similar that they are
    # combined here. "basic" function creation is much simpler so that is kept separate.
    # What's going to happen is there will be essentially two important data structures:
    #
    #   1) A tree of functions, the root of which will be returned. The leaves of the
    #      tree are "place holders" which are actually instances of a concrete DataType,
    #      such as Int, with a value of None. In other words, the arguments to all
    #      functions are either other functions or SQL NULL.
    #
    #   2) A mapping to place holders from the type of function that they may be replaced
    #      with. The actual data structure is
    #      dict<func class> -> list<tuple<tree node, index of place holder in tree node>>
    #      where "func class" is one of "AggFunc", "AnalyticFunc" or "Func".
    #
    # This means once a child function is generated, a spot where it can be placed into
    # the tree can easily be identified. Although the work is actually done in reverse
    # order, a place holder is chosen, then a replacement is generated.
    if not agg_funcs and not analytic_funcs:
      raise Exception('At least one analytic or aggregate function is required')

    basic_signatures_by_return_type = self._group_signatures_by_return_type(
        self._find_matching_signatures(
            self._funcs_to_allowed_signatures(basic_funcs),
            allow_subquery=False))
    agg_signatures_by_return_type = self._group_signatures_by_return_type(
        self._funcs_to_allowed_signatures(agg_funcs))
    analytic_signatures_by_return_type = self._group_signatures_by_return_type(
        self._funcs_to_allowed_signatures(analytic_funcs))
    if analytic_funcs:
      return_class = AnalyticFunc
      return_signatures_by_return_type = analytic_signatures_by_return_type
    else:
      return_class = AggFunc
      return_signatures_by_return_type = agg_signatures_by_return_type

    min_children, max_children = self.profile.bounds('MAX_NESTED_EXPR_COUNT')
    if max_children == 0 and return_type not in return_signatures_by_return_type:
      raise Exception(('At least one child expr is required to create a %s expr'
          + ' using an %s function')
          % (return_type.__name__,
             'aggregate' if return_class == AggFunc else 'analytic'))
    if min_children == 0 and return_type not in return_signatures_by_return_type:
      min_children = 1
    max_children = self.profile._choose_from_bounds(min_children, max_children)

    if not max_children:
      signature = self.profile.choose_func_signature(
          return_signatures_by_return_type[return_type])
      return signature.func(signature)

    root_func = None

    # Every time a function is created its arguments will initially be NULL. Those NULL
    # arguments may be replaced by other functions up to "max_children". The types of
    # valid child functions depends on the type of parent function.
    #
    #  * Analytics may not contain other analytics
    #  * Analytics may contain aggregates or basic functions
    #  * Aggregates may not contain other aggregates or analytics
    #  * Aggregates may contain basic functions
    #  * Basic functions may contain any function so long as the above conditions are
    #    not broken (indirectly).
    null_args_by_func_allowed = {AggFunc: list(), AnalyticFunc: list(), Func: list()}
    while max_children:
      null_arg_pool = None
      parent_func, parent_arg_idx = None, None
      chosen_signature=None

      # Since aggregate (and let's assume analytic functions) return a limited set of
      # types, some prep work may be needed if the return type isn't in the set of
      # directly producible types. For example if a Boolean is desired and there are
      # only two children remaining, the next child must be something that accepts an
      # aggregate and returns a Boolean, such as GreaterThan.
      if (not root_func and max_children == 1) \
          or (root_func and max_children == 2 \
              and ((return_class == AggFunc and not root_func.contains_agg)
                  or (return_class == AnalyticFunc and not root_func.contains_analytic))):
        if root_func:
          idx = randrange(len(null_args_by_func_allowed[return_class])
              + len(null_args_by_func_allowed[Func]))
          use_return_class = idx < len(null_args_by_func_allowed[return_class])
          if use_return_class:
            null_arg_pool = null_args_by_func_allowed[return_class]
            signature_pools_by_return_type = return_signatures_by_return_type
          else:
            null_arg_pool = null_args_by_func_allowed[Func]
            signature_pools_by_return_type = basic_signatures_by_return_type
          shuffle(null_arg_pool)
          parent_func, parent_arg_idx = null_arg_pool.pop()
          parent_arg_type = parent_func.args[parent_arg_idx].type
          if use_return_class:
            signature_pool = signature_pools_by_return_type[parent_arg_type]
          else:
            # This is a basic functions so it needs to accept one of the types returned
            # by the desired return_class.
            signature_pool = self._find_matching_signatures(
                signature_pools_by_return_type[parent_arg_type],
                accepts=tuple(return_signatures_by_return_type))
        else:
          signature_pool = list()
          if return_type in return_signatures_by_return_type:
            signature_pool.extend(return_signatures_by_return_type[return_type])
          if return_type in basic_signatures_by_return_type:
            signature_pool.extend(self._find_matching_signatures(
                basic_signatures_by_return_type[return_type],
                accepts=tuple(return_signatures_by_return_type)))
        chosen_signature = self.profile.choose_func_signature(signature_pool)
      elif max_children == 1 \
          and ((return_class == AggFunc and not root_func.contains_agg)
              or (return_class == AnalyticFunc and not root_func.contains_analytic)):
        # This is the last iteration and the generated function still doesn't contain
        # an instance of the desired function class. From the setup above, at least
        # one of the available place holder arguments can be replaced by an aggregate or
        # analytic.
        null_arg_pool = null_args_by_func_allowed[return_class]
        if not null_arg_pool:
          raise Exception(
              'No leaves in the expr tree may be replaced by an %s function'
              % ('aggregate' if return_class == AggFunc else 'analytic'))
        shuffle(null_arg_pool)
        return_types = tuple(return_signatures_by_return_type)
        while null_arg_pool:
          parent_func, parent_arg_idx = null_arg_pool.pop()
          parent_arg_type = parent_func.args[parent_arg_idx].type
          if issubclass(parent_arg_type, return_types):
            break
          if not null_arg_pool:
            raise Exception('No functions could accept an %s function'
                % ('aggregate' if return_class == AggFunc else 'analytic'))
        chosen_signature = self.profile.choose_func_signature(
            return_signatures_by_return_type[parent_arg_type])
      elif not root_func:
        # No restrictions, just choose a root_func that returns the needed type.
        signature_pool = list()
        if return_type in return_signatures_by_return_type:
          signature_pool.extend(return_signatures_by_return_type[return_type])
        if return_type in basic_signatures_by_return_type:
          signature_pool.extend(basic_signatures_by_return_type[return_type])
        chosen_signature= self.profile.choose_func_signature(signature_pool)
      else:
        # A root_func was chosen and it's children are in one or more of the
        # null_args_by_func_allowed pools. A pool will be chosen, then a child function.
        null_arg_counts_by_pool = dict((pool_category, len(pool)) for pool_category, pool
                                       in null_args_by_func_allowed.items())
        # There is a special case that would lead to a dead end. If there is only one
        # distinct place holder across all the pools and an analytic is still needed,
        # then that place holder cannot be replaced by an aggregate since aggregates
        # are not allowed to contain analytics. Example: an analytic is impossible once
        # the tree is FLOOR(AVG(NULL)).
        if return_class == AnalyticFunc \
            and not root_func.contains_analytic \
            and len(null_args_by_func_allowed[AnalyticFunc]) == 1 \
            and null_args_by_func_allowed[AnalyticFunc][0] \
                in null_args_by_func_allowed[AggFunc]:
          null_arg_counts_by_pool[AggFunc] = 0

        null_arg_pool_category = \
            self.profile._choose_from_weights(null_arg_counts_by_pool)
        null_arg_pool = null_args_by_func_allowed[null_arg_pool_category]
        shuffle(null_arg_pool)
        parent_func, parent_arg_idx = null_arg_pool.pop()
        parent_arg_type = parent_func.args[parent_arg_idx].type

        if null_arg_pool_category == AggFunc:
          signature_pool = agg_signatures_by_return_type[parent_arg_type]
        elif null_arg_pool_category == AnalyticFunc:
          signature_pool = analytic_signatures_by_return_type[parent_arg_type]
        else:
          signature_pool = basic_signatures_by_return_type[parent_arg_type]
        chosen_signature = self.profile.choose_func_signature(signature_pool)

      chosen_func = chosen_signature.func(chosen_signature)
      if root_func:
        max_children -= 1
      else:
        root_func = chosen_func

      if parent_func:
        # Remove the place holder from all of the other pools.
        for pool_category, pool in null_args_by_func_allowed.items():
          for null_arg_idx, (func, arg_idx) in enumerate(pool):
            if func is parent_func and arg_idx == parent_arg_idx:
              del pool[null_arg_idx]

        # Replace the place holder with the child function
        parent_func.args[parent_arg_idx] = chosen_func
        parent_func.validate()
        chosen_func.parent = parent_func
      else:
        chosen_func.parent = None

      # Check if the func_tree contains any analytic functions returned by
      # profile.get_analytic_funcs_that_cannot_contain_aggs; if it does then aggregate
      # functions are not allowed in child operators
      allow_agg_func = not self._func_tree_contains_funcs(
          chosen_func, self.profile.get_analytic_funcs_that_cannot_contain_aggs())

      # Place the args of the chosen function into the appropriate pools. Aggregate and
      # analytic functions have different rules about which functions they accept as
      # arguments. If the chosen function is neither then the tree must be inspected to
      # find the first aggregate or analytic ancestor.
      child_null_arg_pools = set()
      node = chosen_func
      while True:
        if node.is_analytic:
          if allow_agg_func:
            child_null_arg_pools.add(AggFunc)
          child_null_arg_pools.add(Func)
          break
        elif node.is_agg:
          child_null_arg_pools.add(Func)
          break
        node = node.parent
        if not node:
          # This means the root_func is a non-aggregate and non-analytic, it can accept
          # anything.
          child_null_arg_pools.add(AggFunc)
          child_null_arg_pools.add(Func)
          if return_class == AnalyticFunc:
            child_null_arg_pools.add(AnalyticFunc)
          break
      for func_allowed in child_null_arg_pools:
        null_args = null_args_by_func_allowed[func_allowed]
        for idx, arg in enumerate(chosen_func.args):
          if arg.val is not None:
            # Some functions come with unusual arguments pre-populated.
            # Ex: The analytic function LEAD(foo, constant) will have "constant" non-NULL.
            continue
          if func_allowed == AggFunc:
            returnable_types = agg_signatures_by_return_type
          elif func_allowed == AnalyticFunc:
            returnable_types = analytic_signatures_by_return_type
          else:
            returnable_types = basic_signatures_by_return_type
          # Skip place holders that are impossible to replace. The function selection
          # logic depends on this.
          if arg.type not in returnable_types:
            continue
          null_args.append((chosen_func, idx))

      if not any(null_args_by_func_allowed.values()):
        # Some analytic functions take no arguments. Ex: ROW_NUM()
        break

    if (return_class == AggFunc and not root_func.contains_agg) \
        or (return_class == AnalyticFunc and not root_func.contains_analytic):
      raise Exception('Unable to create an %s function'
          % ('aggregate' if return_class == AggFunc else 'analytic'))

    return root_func

  def _func_tree_contains_funcs(self, func_tree, funcs):
    """
    Returns True if any of the funcs are in the given func_tree. A func_tree must have
    a parent property defined that links to the parent function. This method compares
    functions based on their name.
    """
    if not funcs:
      return False
    node = func_tree
    func_names = set(func.name() for func in funcs)
    while node:
      if node.name() in func_names:
        return True
      node = node.parent
    return False

  def _create_analytic_select_item(self,
      table_exprs,
      select_item_exprs,
      is_only_item,
      return_type):
    '''Create and return a list of SelectItems where each item includes an anayltic
       function in its val_expr.

       table_exprs must be a non-empty collection of tables exprs to choose columns from.

       select_items may be an empty collection. If not empty, anayltic function
       expressions will be restricted so that PARTITION BY and ORDER BY values are
       chosen from within this set. If empty, any random expr may be chosen.

       is_only_item should be True if this analytic item will be the only SelectItem in
       the SELECT clause. When True, an additional analytic variation that would
       otherwise produce a non-deterministic result set is enabled.
    '''
    # Results may not be deterministic when analytics have an ORDER BY. The code below
    # will assume we want deterministic results though we know this may not be true
    # for certain use cases such as just searching for crashes where results will be
    # thrown away.
    #
    # These cases will produce a deterministic result when ORDER BY is used
    #
    #   1) There is only one SELECT item and the analytic function has no argument
    #      or the ORDER BY includes the argument.
    #
    #   2) The analytic has set of ORDER BY expressions that has a deterministic
    #      result. Ex: ORDER BY primary_key. This is only reliable if there are no
    #      JOINs.
    #
    #   3) A window is explicitly specified as UNBOUNDED PRECEDING to UNBOUNDED
    #      FOLLOWING and either the analytic function is an aggregate or the
    #      analytic function is FIRST_VALUE or LAST_VALUE.
    #
    #   4) RANK is a special case, it is intrinsically deterministic
    excluded_designs = list()
    if len(self.queries_under_construction) > 1:
      excluded_designs.append('TOP_LEVEL_QUERY_WITHOUT_LIMIT')
    if not is_only_item:
      excluded_designs.append('ONLY_ITEM')
    if len(table_exprs) == 1:
      unique_col_combos = []
      if table_exprs[0].unique_cols:
        if select_item_exprs:
          select_cols = set(val_expr for val_expr in select_item_exprs if val_expr.is_col)
          for unique_cols in table_exprs[0].unique_cols:
            if unique_cols <= select_cols:
              unique_col_combos.append(unique_cols)
        else:
          unique_col_combos = table_exprs[0].unique_cols
      if not unique_col_combos:
        excluded_designs.append('DETERMINISTIC_ORDER')
    else:
      excluded_designs.append('DETERMINISTIC_ORDER')

    allow_agg = any(filter(lambda expr: expr.contains_agg, select_item_exprs))
    value = self._create_analytic_func_tree(return_type, excluded_designs, allow_agg)
    value = self.populate_func_with_vals(
        value,
        table_exprs=TableExprList(),
        val_exprs=ValExprList())
    self._populate_analytic_clauses(value, table_exprs, select_item_exprs)
    return SelectItem(value)

  def _create_analytic_func_tree(self, return_type, excluded_designs, allow_agg):
    # The results of queries with analytic functions may not be deterministic. Let's
    # assume deterministic results are required. To generate deterministic results, the
    # structure of queries will be limited to one of several "designs". The goal of each
    # design is to ensure that results are deterministic. See the notes in
    # _create_analytic_select_item for more info on designs.
    designs = set(self.profile.allowed_analytic_designs())
    for design in excluded_designs:
      if design in designs:
        designs.remove(design)
    window_weigts = self.profile.weights('ANALYTIC_WINDOW')
    if 'UNBOUNDED_WINDOW' in designs \
        and window_weigts[('ROWS', UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)] == 0 \
        and window_weigts[('RANGE', UNBOUNDED_PRECEDING, UNBOUNDED_FOLLOWING)] == 0:
      designs.remove('UNBOUNDED_WINDOW')
    if not designs:
      raise Exception('No options for creating a deterministic analytic function are'
          ' available')

    available_funcs = list()
    for func in ANALYTIC_FUNCS:
      if 'TOP_LEVEL_QUERY_WITHOUT_LIMIT' in designs \
          or 'DETERMINISTIC_ORDER_BY' in designs \
          or 'ONLY_SELECT_ITEM' in designs \
          or ('RANK_FUNC' in designs and func.name() == 'Rank') \
          or ('NO_ORDER_BY' in designs and not func.REQUIRES_ORDER_BY) \
          or ('UNBOUNDED_WINDOW' in designs and func.SUPPORTS_WINDOWING):
        available_funcs.append(func)
    if not available_funcs:
      raise Exception('No analytic functions available')

    # In general this may be a tree-like val expr that is guaranteed to contain at least
    # one analytic function. The arguments to the function are already outlined but the
    # analytic clause is still undetermined.
    root_func = self._create_agg_or_analytic_tree(
        return_type,
        analytic_funcs=available_funcs,
        agg_funcs=(AGG_FUNCS if allow_agg else []))

    # The following designs are preferred because they place no restrictions on the
    # analytic. From now on, if "designs" is non-empty this means that a design must
    # be chosen.
    if 'TOP_LEVEL_QUERY_WITHOUT_LIMIT' in designs \
        or 'DETERMINISTIC_ORDER_BY' in designs \
        or 'ONLY_SELECT_ITEM' in designs:
      designs.clear()
    else:
      designs = list(designs)

    # Set the analytic clauses, such as PARTITION BY, etc, for each analytic function.
    analytic_funcs = list()
    if root_func.is_analytic:
      analytic_funcs.append(root_func)
    for func in root_func.iter_exprs(lambda expr: expr.is_analytic):
      analytic_funcs.append(func)
    for func in analytic_funcs:
      if designs:
        func.design = choice([design for design in designs
                              if design != 'RANK' or func.name() != 'Rank'])
      else:
        func.design = 'NO_RESTRICTIONS'

      use_window = (func.design == 'UNBOUNDED_WINDOW') \
          or (func.design != 'NO_ORDER_BY'
              and (func.SUPPORTS_WINDOWING and self.profile.use_window_in_analytic()))
      if use_window:
        window_range_or_rows, window_start_boundary_type, window_end_boundary_type \
            = self.profile.choose_window_type()
        if window_start_boundary_type in (PRECEDING, FOLLOWING):
          start_val = Int(self.profile._choose_from_bounds('ANALYTIC_WINDOW_OFFSET'))
        else:
          start_val = None
        if window_end_boundary_type in (PRECEDING, FOLLOWING):
          end_val = Int(self.profile.get_window_offset())
          if window_start_boundary_type == PRECEDING \
              and window_end_boundary_type == PRECEDING:
            if start_val.val < end_val.val:
              start_val, end_val = end_val, start_val
            elif start_val.val == end_val.val:
              end_val.val -= 1
          elif window_start_boundary_type == FOLLOWING \
              and window_end_boundary_type == FOLLOWING:
            if start_val.val > end_val.val:
              start_val, end_val = end_val, start_val
            elif start_val.val == end_val.val:
              start_val.val -= 1
        else:
          end_val = None
        func.window_clause = WindowClause(
            window_range_or_rows,
            WindowBoundary(window_start_boundary_type, start_val),
            WindowBoundary(window_end_boundary_type, end_val) \
                if window_end_boundary_type else None)

      if self.profile.use_partition_by_clause_in_analytic():
        func.partition_by_clause = PartitionByClause([])

      if (func.design == 'DETERMINISTIC_ORDER') \
          or use_window \
          or func.REQUIRES_ORDER_BY \
          or (func.design != 'NO_ORDER_BY' \
              and self.profile.use_order_by_clause_in_analytic()):
        func.order_by_clause = OrderByClause([])

      if func.name() in ('Lead', 'Lag') and len(func.args) == 2:
        func.args[1].val = self.profile.get_offset_for_analytic_lead_or_lag()

    return root_func

  def _group_signatures_by_return_type(self, signatures):
    groups = defaultdict(list)
    for signature in signatures:
      groups[signature.return_type].append(signature)
    return dict(groups)

  def _populate_analytic_clauses(self, func, table_exprs, val_exprs):
    for arg in func.args:
      if arg.is_func:
        self._populate_analytic_clauses(arg, table_exprs, val_exprs)
    if func.is_analytic:
      if func.design == 'DETERMINISTIC_ORDER':
        unique_cols = shuffle(list(choice(table_exprs[0].unique_cols)))
      else:
        unique_cols = list()

      if func.partition_by_clause:
        if len(unique_cols) > 1:
          func.partition_by_clause.val_exprs.append(unique_cols.pop())
        elif len(val_exprs) > 1:
          shuffle(val_exprs)
          func.partition_by_clause.val_exprs.append(val_exprs[0])
        else:
          func.partition_by_clause = None

      if func.order_by_clause:
        if unique_cols:
          order_by_exprs = unique_cols
        elif val_exprs:
          order_by_exprs = val_exprs[:2]
        else:
          cols = list(table_exprs.cols)
          shuffle(cols)
          order_by_exprs = cols[:2]
        for order_by_expr in order_by_exprs:
          func.order_by_clause.exprs_to_order.append(
              (order_by_expr, choice([None, 'ASC', 'DESC', 'DESC'])))

  def _create_from_clause(self,
      table_exprs,
      table_alias_prefix,
      required_table_expr_col_type):
    from_clause = None
    table_count = self.profile.get_table_count()

    candidate_table_exprs = TableExprList(table_exprs)
    candidate_table_exprs.extend(candidate_table_exprs.collections)
    # If filter out table expressions with no scalar columns.
    candidate_table_exprs = TableExprList(
        table_expr for table_expr in candidate_table_exprs if table_expr.cols)

    if required_table_expr_col_type:
      candidate_table_exprs = \
          candidate_table_exprs.by_col_type[required_table_expr_col_type]
      if not candidate_table_exprs:
        raise Exception('No tables have the required column type')

    if table_count > 1:
      # If there are multiple multiple tables, the first chosen table must have joinable
      # columns.
      candidate_table_exprs = TableExprList(
          table_expr for table_expr in candidate_table_exprs
          if table_expr.joinable_cols_by_type)
      if not candidate_table_exprs:
        raise Exception('No tables have any joinable types')
      required_type = self.profile.choose_type(
          candidate_table_exprs.joinable_cols_by_type)
    else:
      required_type = None
    table_expr = self._create_table_expr(candidate_table_exprs,
        required_type=required_type)
    table_expr.alias = self.get_next_id()
    from_clause = FromClause(table_expr)

    for idx in range(1, table_count):
      join_clause = self._create_join_clause(from_clause, table_exprs)
      join_clause.table_expr.alias = self.get_next_id()
      from_clause.join_clauses.append(join_clause)

    return from_clause

  def _create_table_expr(self, table_exprs, required_type=None):
    if not table_exprs:
      raise Exception('At least one table_expr is required')
    if self.allow_more_nested_queries and self.profile.use_inline_view():
      return self._create_inline_view(table_exprs, required_type=required_type)
    if required_type:
      candidiate_table_exprs = TableExprList()
      for table_expr in table_exprs:
        for col in table_expr.cols:
          if issubclass(col.type, required_type):
            candidiate_table_exprs.append(table_expr)
            break
      if not candidiate_table_exprs:
        raise Exception('No tables have a column with the required data type')
      table_exprs = candidiate_table_exprs
    return deepcopy(self.profile.choose_table(table_exprs))

  def _create_inline_view(self, table_exprs, required_type=None):
    return InlineView(self.generate_statement(
        table_exprs, required_select_item_type=required_type,
        allow_with_clause=self.profile.use_nested_with()))

  def _create_join_clause(self, from_clause, table_exprs):
    join_type = self.profile.choose_join_type(JoinClause.JOINS_TYPES)
    use_lateral = self.profile.use_lateral_join()
    correlated_collections = from_clause.collections
    if use_lateral and correlated_collections:
      # TODO: Add correlated collections from ancestor queries
      if self.allow_more_nested_queries and self.profile.use_inline_view():
        collection = self._create_inline_view(correlated_collections)
      else:
        collection = deepcopy(choice(correlated_collections))
      join_clause = JoinClause(join_type, table_expr=collection)
      join_clause.is_lateral_join = True
      for _ in range(self.profile.get_num_boolean_exprs_for_lateral_join()):
        predicate = self._create_single_join_condition(collection, from_clause.table_exprs)
        if not predicate:
          break
        join_clause.boolean_expr = And.create_from_args(join_clause.boolean_expr,
            predicate) if join_clause.boolean_expr else predicate
      return join_clause
    else:
      candidate_table_exprs = TableExprList(table_exprs)
      candidate_table_exprs.extend(candidate_table_exprs.collections)
      if join_type == 'CROSS':
        table_expr = self._create_table_expr(candidate_table_exprs)
      else:
        available_join_expr_types = set(from_clause.table_exprs.joinable_cols_by_type) \
            & set(candidate_table_exprs.col_types)
        if not available_join_expr_types:
          raise Exception('No tables have any columns eligible for joining')
        join_expr_type = self.profile.choose_type(tuple(available_join_expr_types))
        table_expr = self._create_table_expr(
            candidate_table_exprs, required_type=join_expr_type)

      if join_type in ('LEFT ANTI', 'LEFT SEMI'):
        table_expr.is_visible = False

      join_clause = JoinClause(join_type, table_expr)

      if join_type == 'CROSS':
        return join_clause

      join_table_expr_candidates = from_clause.table_exprs.by_col_type[join_expr_type]
      if not join_table_expr_candidates:
        raise Exception('table_expr has no common joinable columns')
      join_clause.boolean_expr = self._create_relational_join_condition(
          table_expr, join_table_expr_candidates)
      return join_clause

  def _get_joinable_cols_in_common(self, table_expr, other_table_expr):
    common_col_types = set(table_expr.joinable_cols_by_type) \
        & set(other_table_expr.joinable_cols_by_type)
    return common_col_types

  _has_joinable_cols_in_common = _get_joinable_cols_in_common

  def _create_single_join_condition(self, cur_table_expr, available_table_exprs):
    candidates = []
    for possible_table_expr in available_table_exprs:
      if self._has_joinable_cols_in_common(cur_table_expr, possible_table_expr):
        candidates.append(possible_table_expr)

    if not candidates:
      return None

    target_table_expr = choice(candidates)
    common_col_types = set(cur_table_expr.joinable_cols_by_type) \
        & set(target_table_expr.joinable_cols_by_type)

    col_type = choice(list(common_col_types))
    left_col = choice(cur_table_expr.joinable_cols_by_type[col_type])
    right_col = choice(target_table_expr.joinable_cols_by_type[col_type])
    return Equals.create_from_args(left_col, right_col)

  def _create_relational_join_condition(self,
      right_table_expr, possible_left_table_exprs):
    assert possible_left_table_exprs

    table_exprs_by_col_types = defaultdict(list)
    candidiate_table_exprs = list()

    for left_table_expr in possible_left_table_exprs:
      joinable_col_types = self._get_joinable_cols_in_common(
          right_table_expr, left_table_expr)
      if joinable_col_types:
        for col_type in joinable_col_types:
          table_exprs_by_col_types[col_type].append(left_table_expr)
          candidiate_table_exprs.append(left_table_expr)
    if not table_exprs_by_col_types:
      raise Exception('Tables have no joinable columns in common')

    join_signatures = self._funcs_to_allowed_signatures(FUNCS)
    if self.profile.only_use_equality_join_predicates():
      join_signatures = [sig for sig in join_signatures if not
          self.profile.is_non_equality_join_predicate(sig.func)]
    else:
      join_signatures = self.profile.get_allowed_join_signatures(join_signatures)

    root_predicate, relational_predicates = self._create_boolean_func_tree(
      require_relational_func=True,
      relational_col_types=list(table_exprs_by_col_types.keys()),
      allowed_signatures=join_signatures)

    for predicate in relational_predicates:
      left_table_expr = choice(candidiate_table_exprs)
      joinable_col_types = self._get_joinable_cols_in_common(
          right_table_expr, left_table_expr)

      left_null_args = self._populate_null_args_list(
          predicate, tuple(joinable_col_types), arg_stop_idx=1)
      arg_idx, func = choice(left_null_args)
      arg_type = func.args[arg_idx].type
      func.args[arg_idx] = choice(left_table_expr.cols_by_type[arg_type])

      right_null_args = self._populate_null_args_list(
          predicate, tuple(joinable_col_types), arg_start_idx=1)
      arg_idx, func = choice(right_null_args)
      arg_type = func.args[arg_idx].type
      func.args[arg_idx] = choice(right_table_expr.cols_by_type[arg_type])

    table_exprs = TableExprList(possible_left_table_exprs)
    table_exprs.append(right_table_expr)
    self.populate_func_with_vals(root_predicate, table_exprs=table_exprs)
    return root_predicate

  def _populate_null_args_list(self,
      func,
      allowed_types,  # Only allow args of this type in the result list.
      arg_start_idx=0,  # Start at this arg index, only applies to "func".
      arg_stop_idx=None,  # Stop at this arg index, only applies to "func".
      null_args=None):  # Used internally for recursion.
    '''Walks through func and creates a list of function arguments that are null. The
       returned list contains tuples with values (arg index, parent func).
    '''
    if arg_stop_idx is None:
      arg_stop_idx = len(func.args)
    if arg_stop_idx < arg_start_idx:
      raise Exception("'arg_stop_idx' (%s) cannot be less than 'arg_start_idx' (%s)"
          % (arg_stop_idx, arg_start_idx))
    if null_args is None:
      null_args = list()
    for idx in range(arg_start_idx, arg_stop_idx):
      arg = func.args[idx]
      if arg.is_constant and issubclass(arg.type, allowed_types):
        assert arg.val is None
        null_args.append((idx, func))
      elif arg.is_func:
        self._populate_null_args_list(arg, allowed_types, null_args=null_args)
    return null_args

  def _create_where_clause(self,
      table_exprs,
      table_alias_prefix):
    predicate, _ = self._create_boolean_func_tree(allow_subquery=True)
    predicate = self.populate_func_with_vals(
        predicate,
        table_exprs=table_exprs,
        table_alias_prefix=table_alias_prefix)
    if predicate.contains_subquery and not table_exprs[0].alias:
      # TODO: Figure out if an alias is really needed.
      table_exprs[0].alias = self.get_next_id()
    return WhereClause(predicate)

  def _create_having_clause(self, table_exprs, basic_select_item_exprs):
    if self.profile.only_use_aggregates_in_having_clause():
      predicate = self._create_agg_func_tree(Boolean)
    else:
      predicate, _ = self._create_boolean_func_tree()
    predicate = self.populate_func_with_vals(
        predicate, val_exprs=basic_select_item_exprs)
    # IMPALA-1423
    # Make sure any cols used have a table identifier. As of this writing the only
    # single table FROM clauses don't use table aliases. Setting a table alias
    # automatically propagates as a column table identifier ("t1.col" instead of "col").
    for arg in predicate.iter_exprs():
      if isinstance(arg, ValExpr) and arg.is_col and not arg.owner.alias:
        # TODO: Figure out if an alias is really needed.
        arg.owner.alias = self.get_next_id()
    return HavingClause(predicate)

  def _enable_distinct_on_random_agg_items(self, agg_items):
    '''For each agg function, set it to be DISTINCT with some probability'''
    def _enable_distinct_helper(val_expr):
      if val_expr.is_agg:
        if self.profile.use_distinct_in_func():
          val_expr.distinct = True
      elif val_expr.is_func:
        for arg in val_expr.args:
          _enable_distinct_helper(arg)

    for item in agg_items:
      _enable_distinct_helper(item.val_expr)

  def _create_boolean_func_tree(self, require_relational_func=False,
                                relational_col_types=None, allow_subquery=False,
                                allowed_signatures=None):
    '''Creates a function that returns a Boolean. The returned value is a
       Tuple<Function, List<Function>> where the first function is the root of the tree
       and the list contains relational functions (though other relational functions
       may also be in the tree). The root function's arguments may be other functions
       depending on the query profile in use. The leaf arguments of the tree will all be
       set to a NULL literal.

       Args:
         require_relational_func: If True, at least one function in the tree will be a
         relational function such as Equals, LessThanOrEquals, etc.

         relational_col_types: Can be used to restrict the signature of the chosen
         relational function to the given types.

         allow_subquery: If True, a subquery may be added to the function tree.

         allowed_signatures: A list of allowable signatures to be used inside the function
         tree. This list is not definitive, functions outside this list may be added to
         the function tree if they are explicitly added by the QueryProfile.
    '''
    # To create a realistic expression, the nodes nearest the root of the tree will be
    # ANDs and ORs, their children will be relational functions, then other number of
    # functions requested by the profile will be added as children of the relational
    # functions. There is no requirement that all children of ANDs and ORs will be
    # relational functions so a wide variety of queries should be generated.

    # Determine the number of non-leaf nodes in the tree.
    if require_relational_func:
      # Just assume this is for a JOIN.
      func_count = self.profile.choose_join_condition_count()
      if func_count <= 0:
        raise Exception("Relational condition count must be greater than zero")
    else:
      func_count = self.profile.choose_nested_expr_count() + 1

    and_or_fill_ratio = self.profile.choose_conjunct_disjunct_fill_ratio()
    and_or_count = int(and_or_fill_ratio * func_count)
    if and_or_count == func_count and require_relational_func:
      # Leave a space for a relational function.
      and_or_count -= 1

    relational_fill_ratio = self.profile.choose_relational_func_fill_ratio()
    relational_count = int(relational_fill_ratio * (func_count - and_or_count))
    if relational_count > and_or_count + 1:
      # Reduce the AND/OR count to the number of boolean spaces that are guaranteed to
      # exist.
      relational_count = and_or_count + 1
    elif relational_count == 0 and require_relational_func:
      relational_count = 1

    root_func = None   # The root of the function tree.
    relational_funcs = list()  # These will be in the tree somewhere.

    # After a root function is chosen, each additional function will be chosen based on
    # the types required by the leaves (existing unused function args). For example if
    # the current tree is
    #      And
    #     /   \
    #   And  Equals(Int, Int)
    # Then the next function chosen must either return a Boolean or an Int. The dict
    # below tracks which arg types are available.
    #
    # In the example above, assuming Equals was chosen as a relational function, the
    # children of Equals must preserve the Int arg if the caller requested Ints using
    # 'relational_col_types'.
    null_args_by_type = defaultdict(list)

    if not allowed_signatures:
      allowed_signatures = self._funcs_to_allowed_signatures(FUNCS)

    if not relational_col_types:
      relational_col_types = tuple()

    for _ in range(func_count):
      is_relational = False

      if and_or_count > 0:
        and_or_count -= 1
        func_class = self.profile.choose_conjunct_disjunct()
        signature = self.profile.choose_func_signature(func_class.signatures())
      elif relational_count > 0:
        relational_count -= 1
        is_relational = True
        relational_signatures = self._find_matching_signatures(
          allowed_signatures, accepts_only=tuple(relational_col_types), returns=Boolean)
        signature = self.profile.choose_relational_func_signature(relational_signatures)
      else:
        if not root_func or Boolean in null_args_by_type:
          # Prefer to replace Boolean leaves to get a more realistic expression.
          return_type = Boolean
        else:
          return_type = choice(list(null_args_by_type.keys()))
        # Rather than track if this is a child of a relational function, in which case
        # the arg type needs to be preserved, just always assume that this is a child of
        # a relational function.
        accepts = return_type if return_type in relational_col_types else None
        matching_signatures = self._find_matching_signatures(
            allowed_signatures, returns=return_type, accepts=accepts,
            allow_subquery=allow_subquery)
        if matching_signatures:
          signature = self.profile.choose_func_signature(matching_signatures)
        else:
          continue

      func = signature.func(signature)
      if root_func:
        null_args = null_args_by_type[signature.return_type]
        shuffle(null_args)
        parent_func, parent_arg_idx = null_args.pop()
        parent_func.args[parent_arg_idx] = func
        if not null_args:
          del null_args_by_type[signature.return_type]
      else:
        root_func = func

      if is_relational:
        relational_funcs.append(func)

      for idx, arg in enumerate(func.args):
        if signature.args[idx].is_subquery:
          continue
        null_args = null_args_by_type[arg.type]
        null_args.append((func, idx))
      if not null_args_by_type:
        # Subqueries might not provide leaves, so stopping early may be necessary. Ex
        # WHERE EXISTS(...).
        assert func.contains_subquery
        break

    return root_func, relational_funcs


def generate_queries_for_manual_inspection():
  '''Generate some queries for manual inspection. The query won't run anywhere because the
     tables used are fake. To make real queries, we'd need to connect to a database and
     read the table metadata and such.
  '''
  from model_translator import SqlWriter

  NUM_TABLES = 5
  NUM_COLS_EACH_TYPE = 10
  NUM_QUERIES = 3000

  tables = list()
  data_types = list(TYPES)
  data_types.remove(Float)
  for table_idx in range(NUM_TABLES):
    table = Table('table_%s' % table_idx)
    tables.append(table)
    cols = table.cols
    col_idx = 0
    for _ in range(NUM_COLS_EACH_TYPE):
      for col_type in data_types:
        col = Column(table, '%s_col_%s' % (col_type.__name__.lower(), col_idx), col_type)
        cols.append(col)
        col_idx += 1
    table.cols = cols

  query_profile = DefaultProfile()
  query_generator = QueryGenerator(query_profile)
  sql_writer = SqlWriter.create(dialect='IMPALA')
  ref_writer = SqlWriter.create(dialect='POSTGRESQL',
      nulls_order_asc=query_profile.nulls_order_asc())
  for _ in range(NUM_QUERIES):
    query = query_generator.generate_statement(tables)
    print("Test db")
    print(sql_writer.write_query(query) + '\n')
    print("Ref db")
    print(ref_writer.write_query(query) + '\n')


if __name__ == '__main__':
  generate_queries_for_manual_inspection()
