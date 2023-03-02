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
from copy import deepcopy
from logging import getLogger

from tests.comparison.common import (
    CollectionColumn,
    Column,
    StructColumn,
    Table)
from tests.comparison.db_types import BigInt, Boolean
from tests.comparison.funcs import Equals, And
from tests.comparison.query import (
    FromClause,
    InlineView,
    JoinClause,
    Query,
    SelectClause,
    WhereClause)

LOG = getLogger(__name__)

class QueryFlattener(object):
  '''Converts a query that contains references to nested types, to an equivalent query for
     for a flattened dataset. This class depends on the dataset flattener implementation.
  '''

  def __init__(self):
    self.clear_state()

  def clear_state(self):
    self.tmp_alias = 0
    # Elements such as join clauses or columns that are not present in the original query.
    self.for_flattening = set()

  def contains_jump(self, table_expr):
    '''If some ancestor CollectionColumn or Table has an alias, but there is a closer
       ancestor CollectionColumn without an alias, then this function will return True.
       For example, suppose we have customer t1 and t1.orders.lineitems t2. If we call
       this function with t2 CollectionColumn as parameter, it will return True.
    '''
    result = False
    while True:
      if table_expr.owner.alias:
        return result
      if isinstance(table_expr.owner, Table):
        # We reached the root and did not encounter an ancestor with an alias
        return False
      if isinstance(table_expr.owner, CollectionColumn):
        # We encountered a CollectionColumn with no alias
        result = True
      table_expr = table_expr.owner

  def get_first_aliased_ancestor(self, table_expr):
    '''Finds the first ancestor that is not a struct. It is returned if it has an alias,
       otherwise, None is returned.
    '''
    while True:
      if table_expr.owner.alias:
        return table_expr.owner
      elif isinstance(table_expr.owner, StructColumn):
        table_expr = table_expr.owner
      else:
        return None

  def flatten_join_clause(self, join_clause, query):

    if join_clause.is_lateral_join:
      if isinstance(join_clause.table_expr, CollectionColumn):
        # All laterally joined Collecitons are converted to an inline view.
        join_clause.table_expr = self.convert_correlated_collection_to_inline_view(
            join_clause.table_expr)
      self.flatten(join_clause.table_expr.query, inner=True)
      join_clause.boolean_expr = join_clause.boolean_expr or Boolean(True)
    elif join_clause not in self.for_flattening:
      if isinstance(join_clause.table_expr, CollectionColumn) and \
          self.contains_jump(join_clause.table_expr):
        join_clause.table_expr = self.convert_correlated_collection_to_inline_view(
            join_clause.table_expr)
      if isinstance(join_clause.table_expr, CollectionColumn):
        aliased_ancestor = self.get_first_aliased_ancestor(join_clause.table_expr)
        if aliased_ancestor:
          predicate = self.create_join_predicate(aliased_ancestor, join_clause.table_expr)
          if query.where_clause:
            query.where_clause.boolean_expr = And.create_from_args(
                predicate, query.where_clause.boolean_expr)
          else:
            query.where_clause = WhereClause(predicate)
      elif isinstance(join_clause.table_expr, InlineView):
        self.flatten(join_clause.table_expr.query, inner=True)

  def flatten_from_clause(self, from_clause, query):

    if isinstance(from_clause.table_expr, CollectionColumn) and \
        self.contains_jump(from_clause.table_expr):
      from_clause.table_expr = self.convert_correlated_collection_to_inline_view(
          from_clause.table_expr)
    if isinstance(from_clause.table_expr, CollectionColumn):
      aliased_ancestor = self.get_first_aliased_ancestor(from_clause.table_expr)
      if aliased_ancestor:
        predicate = self.create_join_predicate(aliased_ancestor, from_clause.table_expr)
        if query.where_clause:
          query.where_clause.boolean_expr = And.create_from_args(
              predicate, query.where_clause.boolean_expr)
        else:
          query.where_clause = WhereClause(predicate)
    elif isinstance(from_clause.table_expr, InlineView):
      self.flatten(from_clause.table_expr.query, inner=True)
    for join_clause in from_clause.join_clauses:
      self.flatten_join_clause(join_clause, query)

  def flatten(self, query, inner=False):
    '''This function is idempotent.'''
    if not inner:
      self.clear_state()
    if not getattr(query, 'flattened', False):
      self.flatten_from_clause(query.from_clause, query)
      for nested_query in query.nested_queries:
        self.flatten(nested_query, inner=True)
      query.flattened = True

  def convert_correlated_collection_to_inline_view(self, original_collection):
    '''Converts the given collection to an inline view. The collection must be correlated,
       ie. it must have an aliased ancestor. This function is able to handle cases where
       there are unaliased collections between the given collection and the aliased
       ancestor.
       For example,

       SELECT ...
       FROM customer t1 INNER JOIN t1.orders.lineitem

       should be converted to:

       SELECT ...
       FROM customer t1 INNER JOIN LATERAL (
         SELECT
           t2.*
         FROM
           customer.orders tmp_alias_1
           INNER JOIN customer.orders.lineitems t2 ON ([tmp_alias_1 is parent of t2])
         WHERE
           [t1 is parent of tmp_alias_1]) t2 ON True

       This function does not add a where clause to the inline view in order to connect
       the first table expression in the from clause with it's parent. This is done in
       flatten_from_clause.
    '''

    def replace_col(container, new_col):
      for i, col in enumerate(container._cols):
        if col.name == new_col.name:
          container._cols[i] = new_col
          new_col.owner = container
          return

    def create_inner_join(parent, child):
      predicate = self.create_join_predicate(parent, child)
      join_clause = JoinClause('INNER', child)
      join_clause.boolean_expr = predicate
      self.for_flattening.add(join_clause)
      return join_clause

    query = Query()
    query.select_clause = SelectClause(None)
    query.select_clause.star_prefix = original_collection.alias

    # Create a list containing original_collection along with all of it's unaliased
    # ancestors
    cur = original_collection
    all_from_elements = []
    while True:
      all_from_elements.append(cur)
      if cur.owner.alias:
        break
      else:
        cur = cur.owner
    all_from_elements.reverse()

    num_collections = sum(1 for e in all_from_elements if isinstance(e, CollectionColumn))
    if num_collections == 1:
      query.from_clause = FromClause(original_collection)
    elif num_collections > 1:
      # Add multiple elements to the from clause which are joined together.
      for i in range(len(all_from_elements)):
        if isinstance(all_from_elements[i], CollectionColumn):
          first_collection = deepcopy(all_from_elements[i])
          first_collection.alias = self.get_tmp_alias()
          all_from_elements = all_from_elements[i + 1:]
          break

      query.from_clause = FromClause(first_collection)

      prev = first_collection
      cur = first_collection

      for table_expr in all_from_elements[:-1]:
        cur = cur.get_col_by_name(table_expr.name)
        if isinstance(cur, CollectionColumn):
          join_clause = create_inner_join(prev, cur)
          join_clause.table_expr.alias = self.get_tmp_alias()
          query.from_clause.join_clauses.append(join_clause)
          prev = cur
      # The last original_collection to be the original one.
      replace_col(cur, original_collection)
      join_clause = create_inner_join(prev, original_collection)
      query.from_clause.join_clauses.append(join_clause)
    else:
      # num_collections < 1
      assert False

    inline_view = InlineView(query)
    inline_view.alias = original_collection.alias

    return inline_view

  def create_join_predicate(self, parent_table, child_table):
    for col in parent_table.cols:
      if col.name == 'id':
        parent_id_col = col
        break
    else:
      parent_id_col = Column(parent_table, 'id', BigInt)
      parent_id_col.for_flattening = True
      parent_table.add_col(parent_id_col)

    child_col_name = self.flat_collection_name(parent_table) + '_id'
    child_col = Column(None, child_col_name, BigInt)
    child_table.add_col(child_col)

    return Equals.create_from_args(parent_id_col, child_col)

  def get_tmp_alias(self):
    self.tmp_alias += 1
    return 'tmp_alias_' + str(self.tmp_alias)

  @classmethod
  def flat_column_name(cls, col):
    if isinstance(col, StructColumn):
      name = 'item' if col.name == 'value' else col.name
      if isinstance(col.owner, StructColumn):
        return cls.flat_column_name(col.owner) + '_' + col.name
      return name
    elif isinstance(col, Column):
      name = col.name
      if col.name == 'item':
        name = 'value'
      elif col.name == 'pos':
        name = 'idx'
      if isinstance(col.owner, StructColumn) \
          and not getattr(col, 'for_flattening', False):
        # This is a struct field. To get the name in the flattened table, concatenate the
        # name of the struct with the field struct field name.
        return cls.flat_column_name(col.owner) + '_' + name
      return name

  @classmethod
  def flat_collection_name(cls, entity):
    '''Figures out the flat collection name for some descendent CollectionColumn.
       For example, we have <Table (table1): StructColumn(structcol1): ArrayCol(arrcol)>,
       and we want to know the name of the flattened arrcol table. This method should
       return "table1_arrcol". Notice StructColumn name is not included in the table name.
       This implementation depends on the implementation of the dataset flattener.
    '''
    if isinstance(entity, StructColumn):
      return cls.flat_collection_name(entity.owner)
    if isinstance(entity, CollectionColumn):
      # For example, customer.orders array column is converted to customer_orders table.
      name = '_values' if entity.name in ('item', 'value') else entity.name
      return cls.flat_collection_name(entity.owner) + '_' + name
    if isinstance(entity, Table):
      return entity.name
