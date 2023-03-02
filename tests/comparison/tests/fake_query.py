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

# This module is used to make instantation of Query() objects a little easier when
# building them for testing. In typical usage, Query objects and their attributes
# (clauses, expressions, etc.) are instantiated with little data and built up over time.
# That works for the query generator, because a lot of logical steps need to happen
# before the Query building is completed. For our testing purposes, though, we need
# completed, static Query() objects, and a way to build them up rather easily and
# expressively.
#
# Thus we have lightweight functions that handle initialization and any attribute
# setting as needed.
#
# TODO: As much as possible, it would be better to refactor our data structures to be
# more testable. But we have a chicken and egg problem in that we have no tests. We have
# chosen to leave the original datastructures alone, and after we build up some tests to
# gain confidence, we can modify them to be more testable, and we can remove items from
# here.

from __future__ import absolute_import, division, print_function
from tests.comparison.common import Column, Table
from tests.comparison.funcs import AnalyticFirstValue
from tests.comparison.query import Query, SelectClause, SelectItem


def FakeColumn(name, type_, is_primary_key=False):
  """
  Return a Column, the creation of which allows the user not to have to specify the
  first argument, which is the table to which the column belongs.

  Typical use should be when creating a FakeTable, use FakeColumns as arguments.
  """
  col = Column(None, name, type_)
  col.is_primary_key = is_primary_key
  return col


def FakeTable(name, fake_columns, storage_format='TEXTFILE'):
  """
  Return a Table consisting of one or more FakeColumns. Because Columns are added via
  method, we support nesting here instead.
  """
  table = Table(name)
  if not fake_columns:
    raise Exception('You must supply at least one FakeColumn argument')
  for fake_column in fake_columns:
    table.add_col(fake_column)
  table.storage_format = storage_format
  return table


def FakeSelectClause(*args):
  """
  Return a SelectClause from value expressions args. This abstracts away from the
  user the need to explicitly make the value expression items SelectItems.
  """
  return SelectClause([SelectItem(_) for _ in args])


def FakeQuery(
    with_clause=None,
    select_clause=None,
    from_clause=None,
    where_clause=None,
    group_by_clause=None,
    having_clause=None,
    union_clause=None,
    order_by_clause=None,
    limit_clause=None
):
  """
  Return a Query object constructed by the keyword args above. select_clause and
  from_clause are required.
  """
  query = Query()
  query.with_clause = with_clause
  query.select_clause = select_clause
  query.from_clause = from_clause
  query.where_clause = where_clause
  query.group_by_clause = group_by_clause
  query.having_clause = having_clause
  query.union_clause = union_clause
  query.order_by_clause = order_by_clause
  query.limit_clause = limit_clause
  if select_clause is None or from_clause is None:
    raise Exception('FakeQuery must at least contain a select_clause and a from_clause')
  return query


def FakeFirstValue(
    val_expr,
    partition_by_clause=None,
    order_by_clause=None,
    window_clause=None
):
  """
  Return an AnalyticFirstValue object based on val_expr and optional clauses. The
  clauses must be *Clause objects (see the funcs and query modules).
  """
  first_value = AnalyticFirstValue.create_from_args(val_expr)
  first_value.partition_by_clause = partition_by_clause
  first_value.order_by_clause = order_by_clause
  first_value.window_clause = window_clause
  return first_value
