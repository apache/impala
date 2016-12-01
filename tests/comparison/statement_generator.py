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

from tests.comparison.common import Table
from tests.comparison.db_types import Char, Float, Int
from tests.comparison.model_translator import SqlWriter
from tests.comparison.query import (
    InsertClause,
    InsertStatement,
    Query,
    StatementExecutionMode,
    ValuesClause,
    ValuesRow)
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import DefaultProfile


class InsertStatementGenerator(object):
  def __init__(self, profile):
    # QueryProfile-like object
    self.profile = profile
    # used to generate SELECT queries for INSERT ... SELECT statements
    self.query_generator = QueryGenerator(profile)

  def generate_statement(self, tables, dml_table):
    """
    Return a randomly generated INSERT statement.

    tables should be a list of Table objects. A typical source of such a list comes from
    db_connection.DbCursor.describe_common_tables(). This list describes the possible
    "sources" of the INSERT's WITH and FROM/WHERE clauses.

    dml_table is a required Table object. The INSERT will be into this table.

    This is just a stub, good enough to generatea valid INSERT INTO ... VALUES
    statement. Actual implementation is tracked IMPALA-4353.
    """
    if not (isinstance(tables, list) and len(tables) > 0 and
            all((isinstance(t, Table) for t in tables))):
      raise Exception("tables must be a not-empty list of Table objects")

    if not isinstance(dml_table, Table):
      raise Exception('dml_table must be a Table')

    if dml_table.primary_keys:
      conflict_action = InsertStatement.CONFLICT_ACTION_IGNORE
    else:
      conflict_action = InsertStatement.CONFLICT_ACTION_DEFAULT

    return InsertStatement(
        insert_clause=InsertClause(dml_table),
        values_clause=self.generate_values_clause(dml_table.cols),
        conflict_action=conflict_action, execution=StatementExecutionMode.DML_TEST)

  def generate_values_clause(self, table_columns):
    constants = []
    for col in table_columns:
      val = self.profile.choose_constant(return_type=col.exact_type,
                                         allow_null=(not col.is_primary_key))
      constants.append(val)
    return ValuesClause([ValuesRow(constants)])


def get_generator(statement_type):
  """
  Given a statement type, return the proper statement generator.
  """
  STATEMENT_GENERATOR_MAP = {
      InsertStatement: InsertStatementGenerator,
      Query: QueryGenerator,
  }
  return STATEMENT_GENERATOR_MAP[statement_type]
