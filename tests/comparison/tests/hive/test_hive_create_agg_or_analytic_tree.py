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

from tests.comparison.db_types import Int
from tests.comparison.funcs import AGG_FUNCS
from tests.comparison.query_generator import QueryGenerator
from tests.comparison.query_profile import HiveProfile, DefaultProfile


def _idfn(analytic_func):
  return analytic_func.name()

@pytest.mark.parametrize('analytic_func', HiveProfile()
                         .get_analytic_funcs_that_cannot_contain_aggs(), ids=_idfn)
def test_hive_analytics_cannot_contain_aggs(analytic_func):
  """
  Tests that the HiveProfile does not allow nested aggs inside specific analytic
  functions. The list of analytic functions that cannot contain aggs is defined by
  QueryProfile.test_hive_analytics_cannot_contain_aggs()
  """

  class FakeDefaultQueryProfile(DefaultProfile):
    """
    A DefaultProfile that forces only nested expression in any expression trees
    """

    def __init__(self):
      super(FakeDefaultQueryProfile, self).__init__()
      self._bounds.update({'MAX_NESTED_EXPR_COUNT': (1, 1)})

  class FakeHiveQueryProfile(HiveProfile):
    """
    A HiveProfile that forces only nested expression in any expression trees
    """

    def __init__(self):
      super(FakeHiveQueryProfile, self).__init__()
      self._bounds.update({'MAX_NESTED_EXPR_COUNT': (1, 1)})

  # Aggregate functions can only return specific types, such as Int, Number, or Float;
  # while certain AnalyticFuncs can return non-numeric types such as FirstValue or
  # LastValue. So for simplicity, these tests are only run against the Int return_type.
  if Int in [signature.return_type for signature in analytic_func.signatures()]:

    # Generate an agg + analytic func tree using the FakeDefaultQueryProfile and ensure
    # the root func is a analytic_func and that the tree contains an aggregate func. An
    # empty list of funcs is passed into the _create_agg_or_analytic_tree so that no
    # basic funcs are created. We can be sure that the root_func is an analytic_func
    # because agg_funcs cannot contain analytic_funcs.

    qgen = QueryGenerator(FakeDefaultQueryProfile())
    func_tree = qgen._create_agg_or_analytic_tree(Int, agg_funcs=AGG_FUNCS,
                                            analytic_funcs=[analytic_func],
                                            basic_funcs=[])
    assert isinstance(func_tree, analytic_func)
    assert func_tree.contains_agg

    # Do the same for the FakeHiveQueryProfile, but now check that the func_tree has no
    # aggregates.

    qgen = QueryGenerator(FakeHiveQueryProfile())
    func_tree = qgen._create_agg_or_analytic_tree(Int, agg_funcs=AGG_FUNCS,
                                              analytic_funcs=[analytic_func],
                                              basic_funcs=[])
    assert isinstance(func_tree, analytic_func)
    assert not func_tree.contains_agg
