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
from tests.comparison.funcs import Equals
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import HiveProfile


def test_hive_create_equality_only_joins():
  """
  Tests that QueryGenerator produces a join condition with only equality functions if the
  HiveProfile is used.
  """

  class FakeHiveQueryProfile(HiveProfile):
    """
    A fake QueryProfile that extends the HiveProfile, various weights are modified in
    order to ensure that this test is deterministic.
    """

    def choose_join_condition_count(self):
      """
      There should be only one operator in the JOIN condition
      """
      return 1

    def choose_conjunct_disjunct_fill_ratio(self):
      """
      There should be no AND or OR operators
      """
      return 0

    def choose_relational_func_fill_ratio(self):
      """
      Force all operators to be relational
      """
      return 1

  query_generator = QueryGenerator(FakeHiveQueryProfile())

  # Create two tables that have one joinable Column
  right_table_expr_list = TableExprList()
  right_table = Table("right_table")
  right_table.add_col(Column("right_table", "right_col", Int))
  right_table_expr_list.append(right_table)

  left_table_expr_list = TableExprList()
  left_table = Table("left_table")
  left_table.add_col(Column("left_table", "left_col", Int))
  left_table_expr_list.append(left_table)

  # Validate the root predicate is an Equals funcs
  assert isinstance(query_generator._create_relational_join_condition(
    right_table_expr_list, left_table_expr_list), Equals)
