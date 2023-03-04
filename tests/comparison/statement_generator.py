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
from builtins import range
from copy import deepcopy

from tests.comparison.common import Table
from tests.comparison.funcs import CastFunc
from tests.comparison.query import (
    InsertClause,
    InsertStatement,
    Query,
    StatementExecutionMode,
    ValuesClause,
    ValuesRow)
from tests.comparison.query_generator import QueryGenerator


class InsertStatementGenerator(object):
  def __init__(self, profile):
    # QueryProfile-like object
    self.profile = profile
    # used to generate SELECT queries for INSERT/UPSERT ... SELECT statements;
    # to ensure state is completely reset, this is created anew with each call to
    # generate_statement()
    self.select_stmt_generator = None

  def generate_statement(self, tables, dml_table):
    """
    Return a randomly generated INSERT or UPSERT statement. Note that UPSERTs are very
    similar to INSERTs, which is why this generator handles both.

    tables should be a list of Table objects. A typical source of such a list comes from
    db_connection.DbCursor.describe_common_tables(). This list describes the possible
    "sources" of the INSERT/UPSERT's WITH and FROM/WHERE clauses.

    dml_table is a required Table object. The INSERT/UPSERT will be into this table.
    """
    if not (isinstance(tables, list) and len(tables) > 0 and
            all((isinstance(t, Table) for t in tables))):
      raise Exception('tables must be a not-empty list of Table objects')

    if not isinstance(dml_table, Table):
      raise Exception('dml_table must be a Table')

    self.select_stmt_generator = QueryGenerator(self.profile)

    insert_statement = InsertStatement(execution=StatementExecutionMode.DML_TEST)

    # Choose whether this is a
    #   INSERT/UPSERT INTO table SELECT/VALUES
    # or
    #   INSERT/UPSERT INTO table (col1, col2, ...) SELECT/VALUES
    # If the method returns None, it's the former.
    insert_column_list = self.profile.choose_insert_column_list(dml_table)

    if dml_table.primary_keys:
      # Having primary keys implies the table is a Kudu table, which makes it subject to
      # both INSERTs (with automatic ignoring of primary key duplicates) and UPSERTs.
      conflict_action = self.profile.choose_insert_vs_upsert()
    else:
      conflict_action = InsertClause.CONFLICT_ACTION_DEFAULT
    insert_statement.insert_clause = InsertClause(
        dml_table, column_list=insert_column_list, conflict_action=conflict_action)
    # We still need to internally track the columns we're inserting. Keep in mind None
    # means "all" without an explicit column list. Since we've already created the
    # InsertClause object though, we can fill this in for ourselves.
    if insert_column_list is None:
      insert_column_list = dml_table.cols
    insert_item_data_types = [col.type for col in insert_column_list]

    # Decide whether this is INSERT/UPSERT VALUES or INSERT/UPSERT SELECT
    insert_source_clause = self.profile.choose_insert_source_clause()

    if issubclass(insert_source_clause, Query):
      # Use QueryGenerator()'s public interface to generate the SELECT.
      select_query = self.select_stmt_generator.generate_statement(
          tables, select_item_data_types=insert_item_data_types)
      # To avoid many loss-of-precision errors, explicitly cast the SelectItems. The
      # generator's type system is not near sophisticated enough to know how random
      # expressions will be implicitly casted in the databases. This requires less work
      # to implement. IMPALA-4693 considers alternative approaches.
      self._cast_select_items(select_query, insert_column_list)
      insert_statement.with_clause = deepcopy(select_query.with_clause)
      select_query.with_clause = None
      insert_statement.select_query = select_query
    elif issubclass(insert_source_clause, ValuesClause):
      insert_statement.values_clause = self._generate_values_clause(insert_column_list)
    else:
      raise Exception('unsupported INSERT/UPSERT source clause: {0}'.format(
          insert_source_clause))
    return insert_statement

  def _generate_values_clause(self, columns):
    """
    Return a VALUES clause containing a variable number of rows.

    The values corresponding to primary keys will be non-null constants. Any other
    columns could be null, constants, or function trees that may or may not evaluate to
    null.
    """
    values_rows = []
    for _ in range(self.profile.choose_insert_values_row_count()):
      values_row = []
      for col in columns:
        if col.is_primary_key:
          val = self.profile.choose_constant(return_type=col.exact_type, allow_null=False)
        elif 'constant' == self.profile.choose_values_item_expr():
          val = self.profile.choose_constant(return_type=col.exact_type, allow_null=True)
        else:
          func_tree = self.select_stmt_generator.create_func_tree(
              col.type, allow_subquery=False)
          val = self.select_stmt_generator.populate_func_with_vals(func_tree)
          # Only the generic type, not the exact type, of the value will be known. To
          # avoid a lot of failed queries due to precision errors, we cast the val to
          # the exact type of the column. This will still not prevent "out of range"
          # conditions, as we don't try to evaluate the random expressions.
          val = CastFunc(val, col.exact_type)
        values_row.append(val)
      values_rows.append(ValuesRow(values_row))
    return ValuesClause(values_rows)

  def _cast_select_items(self, select_query, column_list):
    """
    For a given Query select_query and a column_list (list of Columns), cast each select
    item in select_query to the exact type of the column.

    A Query may have a UNION, recursively do this down the line.
    """
    for col_idx, select_item in enumerate(select_query.select_clause.items):
      cast_val_expr = CastFunc(select_item.val_expr, column_list[col_idx].exact_type)
      select_item.val_expr = cast_val_expr
    if select_query.union_clause:
      self._cast_select_items(select_query.union_clause.query, column_list)


def get_generator(statement_type):
  """
  Given a statement type, return the proper statement generator.
  """
  STATEMENT_GENERATOR_MAP = {
      InsertStatement: InsertStatementGenerator,
      Query: QueryGenerator,
  }
  return STATEMENT_GENERATOR_MAP[statement_type]
