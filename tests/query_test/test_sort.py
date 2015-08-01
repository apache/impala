# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
import sys
import re
import random
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

def transpose_results(result):
  """Given a query result (list of strings, each string represents a row), return a list
    of columns, where each column is a list of strings."""
  split_result = [row.split('\t') for row in result]
  return [list(l) for l in zip(*split_result)]

class TestQueryFullSort(ImpalaTestSuite):
  """Test class to do functional validation of sorting when data is spilled to disk."""

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryFullSort, cls).add_test_dimensions()

    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet')

  def test_multiple_mem_limits(self, vector):
    """Exercise the dynamic memory scaling functionality."""

    """Using lineitem table forces the multi-phase sort with low mem_limit. This test
       takes about a minute"""
    query = """select l_comment, l_partkey, l_orderkey, l_suppkey, l_commitdate
            from lineitem order by l_comment limit 100000"""
    exec_option = vector.get_value('exec_option')
    exec_option['disable_outermost_topn'] = 1
    table_format = vector.get_value('table_format')

    """The first run should fit in memory, the 300m run is a 2-phase disk sort,
       the 150m run is a multi-phase sort (i.e. with an intermediate merge)."""
    for mem_limit in ['-1', '300m', '150m']:
      exec_option['mem_limit'] = mem_limit
      result = transpose_results(self.execute_query(
        query, exec_option, table_format=table_format).data)
      assert(result[0] == sorted(result[0]))

  def test_sort_join(self, vector):
    """With 200m memory limit this should be a 2-phase sort"""

    query = """select o1.o_orderdate, o2.o_custkey, o1.o_comment from orders o1 join
    orders o2 on (o1.o_orderkey = o2.o_orderkey) order by o1.o_orderdate limit 100000"""

    exec_option = vector.get_value('exec_option')
    exec_option['disable_outermost_topn'] = 1
    exec_option['mem_limit'] = "1200m"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    assert(result[0] == sorted(result[0]))

  def test_sort_union(self, vector):
    pytest.xfail(reason="IMPALA-1346")
    query = """select o_orderdate, o_custkey, o_comment from (select * from orders union
    select * from orders union all select * from orders) as i
    order by o_orderdate limit 100000"""

    exec_option = vector.get_value('exec_option')
    exec_option['disable_outermost_topn'] = 1
    exec_option['mem_limit'] = "3000m"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    assert(result[0] == sorted(result[0]))
