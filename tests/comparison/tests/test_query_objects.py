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
import pytest

from tests.comparison.model_translator import (
    ImpalaSqlWriter,
    PostgresqlSqlWriter,
    SqlWriter)

from query_object_testdata import INSERT_QUERY_TEST_CASES, SELECT_QUERY_TEST_CASES


def _idfn(query_test):
  return query_test.testid


def verify_select_clause_items(query, expected_item_counts):
  """
  Verify that a well-formed SelectQuery() object's select_clause (SelectClause instance)
  reports correct item counts. expected_item_counts should be a dictionary with keys
  matching SelectItem property methods that report item counts and values for the
  counts.
  """
  attrs_to_check = [
      'items',
      'basic_items',
      'analytic_items',
      'agg_items',
  ]
  select_clause = query.select_clause
  for attr_name in attrs_to_check:
    select_clause_attr = getattr(select_clause, attr_name)
    expected_item_count_attr = expected_item_counts.get(attr_name)
    actual_item_count = len(select_clause_attr)
    assert len(select_clause_attr) == expected_item_count_attr, (
        'item count mismatch for item "{item}": expected: {expected}; actual: '
        '{actual}'.format(item=attr_name, expected=expected_item_count_attr,
                          actual=actual_item_count))


def verify_sql_matches(actual, expected, strip=True):
  """
  Assert that the actual and expected SQL queries match. Trailing white space is
  stripped by default.
  """
  if strip:
    actual = actual.strip()
    expected = expected.strip()
  assert actual == expected, 'actual SQL "{actual}" != expected SQL "{expected}"'.format(
      actual=actual, expected=expected)


@pytest.yield_fixture
def sql_writer(request):
  """
  Return a SqlWriter object that is torn down at the end of each test.
  """
  yield SqlWriter.create(dialect=request.param)


@pytest.mark.parametrize('query_test', SELECT_QUERY_TEST_CASES, ids=_idfn)
def test_select_clause_items(query_test):
  verify_select_clause_items(query_test.query, query_test.select_item_counts)


@pytest.mark.parametrize('query_test', SELECT_QUERY_TEST_CASES + INSERT_QUERY_TEST_CASES,
                         ids=_idfn)
@pytest.mark.parametrize('sql_writer', ['IMPALA', 'POSTGRESQL'], indirect=True)
def test_write_query(sql_writer, query_test):
  if isinstance(sql_writer, ImpalaSqlWriter):
    expected_string = getattr(query_test, 'impala_query_string')
  elif isinstance(sql_writer, PostgresqlSqlWriter):
    expected_string = getattr(query_test, 'postgres_query_string')
  else:
    raise Exception('unsupported writer: {0}'.format(sql_writer))
  verify_sql_matches(
      sql_writer.write_query(query_test.query),
      expected_string)
