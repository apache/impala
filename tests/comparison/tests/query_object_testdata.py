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

from collections import namedtuple

from fake_query import FakeColumn, FakeFirstValue, FakeQuery, FakeSelectClause, FakeTable
from tests.comparison.db_types import Char, Int
from tests.comparison.funcs import AggCount
from tests.comparison.query import FromClause, OrderByClause


QueryTest = namedtuple(
    # A QueryTest object contains a Query and all data to verify about it as other
    # attributes. This allows a new Query to be added without need to modify tests
    # themselves. The various tests cherry-pick which test attributes they need to
    # verify against the Query.
    #
    # If you add a new test, add a new attribute, or perhaps reuse one or more
    # attributes.
    #
    # If you add a new test case, add a new item to QUERY_TEST_CASESs array.
    #
    # All attributes are required.
    'QueryTest',
    [
        # string to represent readable pytest testid
        'testid',
        # Query object, formed via FakeQuery
        'query',
        # textual form of FakeQuery
        'impala_query_string',
        # hash representing various item counts (see SelectItem property methods)
        'select_item_counts',
    ]
)


# FakeTables must be declared for use by queries. Tables may be reused as needed for
# multiple FakeQueries.
SIMPLE_TABLE = FakeTable(
    'fake_table',
    [
        FakeColumn('int_col', Int),
        FakeColumn('char_col', Char),
    ]
)


# All tests involving queries should be written to use this dataset.
QUERY_TEST_CASES = [
    QueryTest(
        testid='select col from table',
        query=FakeQuery(
            select_clause=FakeSelectClause(*SIMPLE_TABLE.cols),
            from_clause=FromClause(SIMPLE_TABLE),
        ),
        impala_query_string=(
            'SELECT\n'
            'fake_table.int_col,\n'
            'TRIM(fake_table.char_col)\n'
            'FROM fake_table'
        ),
        select_item_counts={
            'items': 2,
            'basic_items': 2,
            'agg_items': 0,
            'analytic_items': 0,
        },
    ),
    QueryTest(
        testid='select count()',
        query=FakeQuery(
            select_clause=FakeSelectClause(
                AggCount.create_from_args(SIMPLE_TABLE.cols[0])),
            from_clause=FromClause(SIMPLE_TABLE),
        ),
        impala_query_string=(
            'SELECT\n'
            'COUNT(fake_table.int_col)\n'
            'FROM fake_table'
        ),
        select_item_counts={
            'items': 1,
            'basic_items': 0,
            'agg_items': 1,
            'analytic_items': 0,
        },
    ),
    QueryTest(
        testid='select first_value(col) over (order by col)',
        query=FakeQuery(
            select_clause=FakeSelectClause(
                FakeFirstValue(
                    SIMPLE_TABLE.cols[0],
                    order_by_clause=OrderByClause([SIMPLE_TABLE.cols[0]])
                ),
            ),
            from_clause=FromClause(SIMPLE_TABLE),
        ),
        impala_query_string=(
            'SELECT\n'
            'FIRST_VALUE(fake_table.int_col) OVER (ORDER BY fake_table.int_col ASC)\n'
            'FROM fake_table'
        ),
        select_item_counts={
            'items': 1,
            'basic_items': 0,
            'agg_items': 0,
            'analytic_items': 1,
        },
    ),
]
