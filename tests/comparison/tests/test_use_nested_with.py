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
from tests.comparison.common import TableExprList, Column, Table
from tests.comparison.db_types import Int
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import DefaultProfile


def test_use_nested_width_subquery():
  """
  Tests that setting DefaultProfile.use_nested_with to False works properly. Setting this
  method to return False should prevent a WITH clause from being used inside a sub-query.
  """

  class MockQueryProfile(DefaultProfile):
    """
    A mock QueryProfile that sets use_nested_with to False and forces the
    QueryGenerator to created nested queries.
    """

    def __init__(self):
      super(MockQueryProfile, self).__init__()

      # Force the QueryGenerator to create nested queries
      self._bounds['MAX_NESTED_QUERY_COUNT'] = (4, 4)

      # Force the QueryGenerator to use WITH clauses whenever possible
      self._probabilities['OPTIONAL_QUERY_CLAUSES']['WITH'] = 1

      # Force the QueryGenerator to create inline views whenever possible
      self._probabilities['MISC']['INLINE_VIEW'] = 1

    def use_nested_with(self):
      return False

  mock_query_gen = QueryGenerator(MockQueryProfile())

  # Create two tables
  table_expr_list = TableExprList()

  right_table = Table("right_table")
  right_table.add_col(Column("right_table", "right_col", Int))
  table_expr_list.append(right_table)

  left_table = Table("left_table")
  left_table.add_col(Column("left_table", "left_col", Int))
  table_expr_list.append(left_table)

  # Check that each nested_query doesn't have a with clause
  for nested_query in mock_query_gen.generate_statement(table_expr_list).nested_queries:
    assert nested_query.with_clause is None
