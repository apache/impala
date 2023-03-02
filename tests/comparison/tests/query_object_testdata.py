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
from collections import namedtuple

from fake_query import (
    FakeColumn,
    FakeFirstValue,
    FakeQuery,
    FakeSelectClause,
    FakeTable)
from tests.comparison.common import TableExprList
from tests.comparison.db_types import Char, Int
from tests.comparison.funcs import AggCount
from tests.comparison.query import (
    FromClause,
    InsertClause,
    InsertStatement,
    OrderByClause,
    ValuesClause,
    ValuesRow,
    WithClause,
    WithClauseInlineView)


QueryTest = namedtuple(
    # A QueryTest object contains a SELECT query and all data to verify about it
    # as other attributes. This allows a new query to be added without need to modify
    # tests themselves. The various tests cherry-pick which test attributes they need to
    # verify against the Query.
    #
    # If you add a new test, add a new attribute, or perhaps reuse one or more
    # existing attributes.
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
        # textual form of query in Impala dialect
        'impala_query_string',
        # textual form of query in PostgreSQL dialect
        'postgres_query_string',
        # dictionary representing various item counts (see SelectItem property methods)
        'select_item_counts',
    ]
)

InsertStatementTest = namedtuple('InsertStatementTest',
                                 ['testid', 'query', 'impala_query_string',
                                  'postgres_query_string'])


# FakeTables must be declared for use by queries. Tables may be reused as needed for
# multiple FakeQueries.
SIMPLE_TABLE = FakeTable(
    'fake_table',
    [
        FakeColumn('int_col', Int),
        FakeColumn('char_col', Char),
    ]
)

KUDU_TABLE = FakeTable(
    'kudu_table',
    [
        FakeColumn('int_col', Int, is_primary_key=True),
        FakeColumn('char_col', Char),
    ]
)

FOUR_COL_KUDU_TABLE = FakeTable(
    'four_col_kudu_table',
    [
        FakeColumn('int_col1', Int, is_primary_key=True),
        FakeColumn('char_col1', Char, is_primary_key=True),
        FakeColumn('int_col2', Int),
        FakeColumn('char_col2', Char),
    ]
)


ONE_COL_KUDU_TABLE = FakeTable(
    'one_col_kudu_table',
    [
        FakeColumn('int_col', Int, is_primary_key=True),
    ]
)

# This can't be used inline because we need its table expressions later.
SIMPLE_WITH_CLAUSE = WithClause(
    TableExprList([
        WithClauseInlineView(
            FakeQuery(
                select_clause=FakeSelectClause(SIMPLE_TABLE.cols[0]),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
            'with_view'
        )
    ])
)

# All tests involving SELECT queries should be written to use this data set.
SELECT_QUERY_TEST_CASES = [
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
        postgres_query_string=(
            'SELECT\n'
            'fake_table.int_col,\n'
            'fake_table.char_col\n'
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
        postgres_query_string=(
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
        postgres_query_string=(
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

INSERT_QUERY_TEST_CASES = [
    InsertStatementTest(
        testid='insert into table select cols',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_TABLE.cols),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'TRIM(fake_table.char_col)\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'fake_table.char_col\n'
            'FROM fake_table'
        ),
    ),
    InsertStatementTest(
        testid='insert into table column permutations select cols',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE, column_list=KUDU_TABLE.cols),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_TABLE.cols),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table (int_col, char_col)\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'TRIM(fake_table.char_col)\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table (int_col, char_col)\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'fake_table.char_col\n'
            'FROM fake_table'
        ),
    ),
    InsertStatementTest(
        testid='insert into table partial column permutation select 1 col',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE,
                                       column_list=[KUDU_TABLE.cols[0]]),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(SIMPLE_TABLE.cols[0]),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table'
        ),
    ),
    InsertStatementTest(
        testid='insert into table select 1 col',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(SIMPLE_TABLE.cols[0]),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table'
        ),
    ),
    InsertStatementTest(
        testid='insert 2 value rows',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE),
            values_clause=ValuesClause((
                ValuesRow((Int(1), Char('a'))),
                ValuesRow((Int(2), Char('b'))),
            )),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a'),\n"
            "(2, 'b')"
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a' || ''),\n"
            "(2, 'b' || '')"
        ),
    ),
    InsertStatementTest(
        testid='insert 1 value',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE),
            values_clause=ValuesClause((
                ValuesRow((Int(1),)),
            )),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            '(1)'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            '(1)'
        ),
    ),
    InsertStatementTest(
        testid='insert value row with full column permutation',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE, column_list=KUDU_TABLE.cols),
            values_clause=ValuesClause((
                ValuesRow((Int(1), Char('a'))),
            )),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table (int_col, char_col)\n'
            'VALUES\n'
            "(1, 'a')"
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table (int_col, char_col)\n'
            'VALUES\n'
            "(1, 'a' || '')"
        ),
    ),
    InsertStatementTest(
        testid='insert value row with partial column permutation',
        query=InsertStatement(
            insert_clause=InsertClause(KUDU_TABLE,
                                       column_list=(KUDU_TABLE.cols[0],)),
            values_clause=ValuesClause((
                ValuesRow((Int(1),)),
            )),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table (int_col)\n'
            'VALUES\n'
            '(1)'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table (int_col)\n'
            'VALUES\n'
            '(1)'
        ),
    ),
    InsertStatementTest(
        testid='insert values seleted from with clause',
        query=InsertStatement(
            with_clause=SIMPLE_WITH_CLAUSE,
            insert_clause=InsertClause(KUDU_TABLE,
                                       column_list=(KUDU_TABLE.cols[0],)),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_WITH_CLAUSE.table_exprs[0].cols),
                from_clause=FromClause(SIMPLE_WITH_CLAUSE.table_exprs[0])
            ),
        ),
        impala_query_string=(
            'WITH with_view AS (SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table)\n'
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'with_view.int_col\n'
            'FROM with_view'
        ),
        postgres_query_string=(
            'WITH with_view AS (SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table)\n'
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'with_view.int_col\n'
            'FROM with_view'
        ),
    ),
    InsertStatementTest(
        testid='insert into table select cols ignore conflicts',
        query=InsertStatement(
            insert_clause=InsertClause(
                KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_IGNORE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_TABLE.cols),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'TRIM(fake_table.char_col)\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'fake_table.char_col\n'
            'FROM fake_table\n'
            'ON CONFLICT DO NOTHING'
        ),
    ),
    InsertStatementTest(
        testid='insert 2 value rows ignore conflicts',
        query=InsertStatement(
            insert_clause=InsertClause(
                KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_IGNORE,
            ),
            values_clause=ValuesClause((
                ValuesRow((Int(1), Char('a'))),
                ValuesRow((Int(2), Char('b'))),
            )),
        ),
        impala_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a'),\n"
            "(2, 'b')"
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a' || ''),\n"
            "(2, 'b' || '')\n"
            'ON CONFLICT DO NOTHING'
        ),
    ),
    InsertStatementTest(
        testid='insert values seleted from with clause ignore conflicts',
        query=InsertStatement(
            with_clause=SIMPLE_WITH_CLAUSE,
            insert_clause=InsertClause(
                KUDU_TABLE,
                column_list=(KUDU_TABLE.cols[0],),
                conflict_action=InsertClause.CONFLICT_ACTION_IGNORE,
            ),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_WITH_CLAUSE.table_exprs[0].cols),
                from_clause=FromClause(SIMPLE_WITH_CLAUSE.table_exprs[0])
            ),
        ),
        impala_query_string=(
            'WITH with_view AS (SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table)\n'
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'with_view.int_col\n'
            'FROM with_view'
        ),
        postgres_query_string=(
            'WITH with_view AS (SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table)\n'
            'INSERT INTO kudu_table (int_col)\n'
            'SELECT\n'
            'with_view.int_col\n'
            'FROM with_view\n'
            'ON CONFLICT DO NOTHING'
        ),
    ),
    InsertStatementTest(
        testid='upsert into table select cols',
        query=InsertStatement(
            insert_clause=InsertClause(
                KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_UPDATE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*SIMPLE_TABLE.cols),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'UPSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'TRIM(fake_table.char_col)\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'SELECT\n'
            'fake_table.int_col,\n'
            'fake_table.char_col\n'
            'FROM fake_table\n'
            'ON CONFLICT (int_col)\n'
            'DO UPDATE SET\n'
            'char_col = EXCLUDED.char_col'
        ),
    ),
    InsertStatementTest(
        testid='upsert 2 value rows',
        query=InsertStatement(
            insert_clause=InsertClause(
                KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_UPDATE,
            ),
            values_clause=ValuesClause((
                ValuesRow((Int(1), Char('a'))),
                ValuesRow((Int(2), Char('b'))),
            )),
        ),
        impala_query_string=(
            'UPSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a'),\n"
            "(2, 'b')"
        ),
        postgres_query_string=(
            'INSERT INTO kudu_table\n'
            'VALUES\n'
            "(1, 'a' || ''),\n"
            "(2, 'b' || '')\n"
            'ON CONFLICT (int_col)\n'
            'DO UPDATE SET\n'
            'char_col = EXCLUDED.char_col'
        ),
    ),
    InsertStatementTest(
        testid='upsert select into table with multiple pk / updatable columns',
        query=InsertStatement(
            insert_clause=InsertClause(
                FOUR_COL_KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_UPDATE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(*FOUR_COL_KUDU_TABLE.cols),
                from_clause=FromClause(FOUR_COL_KUDU_TABLE)
            ),
        ),
        impala_query_string=(
            'UPSERT INTO four_col_kudu_table\n'
            'SELECT\n'
            'four_col_kudu_table.int_col1,\n'
            'TRIM(four_col_kudu_table.char_col1),\n'
            'four_col_kudu_table.int_col2,\n'
            'TRIM(four_col_kudu_table.char_col2)\n'
            'FROM four_col_kudu_table'
        ),
        postgres_query_string=(
            'INSERT INTO four_col_kudu_table\n'
            'SELECT\n'
            'four_col_kudu_table.int_col1,\n'
            'four_col_kudu_table.char_col1,\n'
            'four_col_kudu_table.int_col2,\n'
            'four_col_kudu_table.char_col2\n'
            'FROM four_col_kudu_table\n'
            'ON CONFLICT (int_col1, char_col1)\n'
            'DO UPDATE SET\n'
            'int_col2 = EXCLUDED.int_col2,\n'
            'char_col2 = EXCLUDED.char_col2'
        ),
    ),
    InsertStatementTest(
        testid='upsert select into table with no updatable columns',
        query=InsertStatement(
            insert_clause=InsertClause(
                ONE_COL_KUDU_TABLE,
                conflict_action=InsertClause.CONFLICT_ACTION_UPDATE),
            select_query=FakeQuery(
                select_clause=FakeSelectClause(SIMPLE_TABLE.cols[0]),
                from_clause=FromClause(SIMPLE_TABLE)
            ),
        ),
        impala_query_string=(
            'UPSERT INTO one_col_kudu_table\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table'
        ),
        postgres_query_string=(
            'INSERT INTO one_col_kudu_table\n'
            'SELECT\n'
            'fake_table.int_col\n'
            'FROM fake_table\n'
            'ON CONFLICT DO NOTHING'
        ),
    ),
]
