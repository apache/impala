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

from copy import copy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfLocal

def transpose_results(result, map_fn=lambda x: x):
  """Given a query result (list of strings, each string represents a row), return a list
    of columns, where each column is a list of strings. Optionally, map_fn can be provided
    to be applied to every value, eg. to convert the strings to their underlying types."""
  split_result = [row.split('\t') for row in result]
  return [map(map_fn, list(l)) for l in zip(*split_result)]

class TestQueryFullSort(ImpalaTestSuite):
  """Test class to do functional validation of sorting when data is spilled to disk."""

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryFullSort, cls).add_test_dimensions()

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet')

  def test_multiple_mem_limits(self, vector):
    """Exercise the dynamic memory scaling functionality."""

    """Using lineitem table forces the multi-phase sort with low mem_limit. This test
       takes about a minute"""
    query = """select l_comment, l_partkey, l_orderkey, l_suppkey, l_commitdate
            from lineitem order by l_comment limit 100000"""
    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    table_format = vector.get_value('table_format')

    """The first run should fit in memory, the 300m run is a 2-phase disk sort,
       the 150m run is a multi-phase sort (i.e. with an intermediate merge)."""
    for mem_limit in ['-1', '300m', '150m']:
      exec_option['mem_limit'] = mem_limit
      result = transpose_results(self.execute_query(
        query, exec_option, table_format=table_format).data)
      assert(result[0] == sorted(result[0]))

  def test_multiple_mem_limits_full_output(self, vector):
    """ Exercise a range of memory limits, returning the full sorted input. """
    query = """select o_orderdate, o_custkey, o_comment
      from orders
      order by o_orderdate"""
    exec_option = copy(vector.get_value('exec_option'))
    table_format = vector.get_value('table_format')

    # The below memory value assume 8M pages.
    exec_option['default_spillable_buffer_size'] = '8M'
    buffer_pool_limit_values = ['-1', '48M'] # Unlimited and minimum memory.
    if self.exploration_strategy() == 'exhaustive' and \
        table_format.file_format == 'parquet':
      # Test some intermediate values for parquet on exhaustive.
      buffer_pool_limit_values += ['64M', '128M', '256M']
    for buffer_pool_limit in buffer_pool_limit_values:
      exec_option['buffer_pool_limit'] = buffer_pool_limit
      result = transpose_results(self.execute_query(
        query, exec_option, table_format=table_format).data)
      assert(result[0] == sorted(result[0]))

  def test_sort_join(self, vector):
    """With 200m memory limit this should be a 2-phase sort"""

    query = """select o1.o_orderdate, o2.o_custkey, o1.o_comment from orders o1 join
    orders o2 on (o1.o_orderkey = o2.o_orderkey) order by o1.o_orderdate limit 100000"""

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['mem_limit'] = "1200m"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    assert(result[0] == sorted(result[0]))

  def test_sort_union(self, vector):
    query = """select o_orderdate, o_custkey, o_comment from (select * from orders union
    select * from orders union all select * from orders) as i
    order by o_orderdate limit 100000"""

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['mem_limit'] = "3000m"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    assert(result[0] == sorted(result[0]))

  def test_pathological_input(self, vector):
    """ Regression test for stack overflow and poor performance on certain inputs where
    always selecting the middle element as a quicksort pivot caused poor performance. The
    trick is to concatenate two equal-size sorted inputs. If the middle element is always
    selected as the pivot (the old method), the sorter tends to get stuck selecting the
    minimum element as the pivot, which results in almost all of the tuples ending up
    in the right partition.
    """
    query = """select l_orderkey from (
      select * from lineitem limit 300000
      union all
      select * from lineitem limit 300000) t
    order by l_orderkey"""

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    # Run with a single scanner thread so that the input doesn't get reordered.
    exec_option['num_nodes'] = "1"
    exec_option['num_scanner_threads'] = "1"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    numeric_results = [int(val) for val in result[0]]
    assert(numeric_results == sorted(numeric_results))

  def test_spill_empty_strings(self, vector):
    """Test corner case of spilling sort with only empty strings. Spilling with var len
    slots typically means the sort must reorder blocks and convert pointers, but this case
    has to be handled differently because there are no var len blocks to point into."""

    query = """
    select empty_str, l_orderkey, l_partkey, l_suppkey,
        l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax
    from (select substr(l_comment, 1000, 0) empty_str, * from lineitem) t
    order by empty_str, l_orderkey, l_partkey, l_suppkey, l_linenumber
    limit 100000
    """

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['buffer_pool_limit'] = "256m"
    exec_option['num_nodes'] = "1"
    table_format = vector.get_value('table_format')

    result = transpose_results(self.execute_query(
      query, exec_option, table_format=table_format).data)
    assert(result[0] == sorted(result[0]))

  @SkipIfLocal.mem_usage_different
  def test_sort_reservation_usage(self, vector):
    """Tests for sorter reservation usage."""
    self.run_test_case('sort-reservation-usage', vector)

class TestRandomSort(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional'

  def test_order_by_random(self):
    """Tests that 'order by random()' works as expected."""
    # "order by random()" with different seeds should produce different orderings.
    seed_query = "select * from functional.alltypestiny order by random(%s)"
    results_seed0 = self.execute_query(seed_query % "0")
    results_seed1 = self.execute_query(seed_query % "1")
    assert results_seed0.data != results_seed1.data
    assert sorted(results_seed0.data) == sorted(results_seed1.data)

    # Include "random()" in the select list to check that it's sorted correctly.
    results = transpose_results(self.execute_query(
        "select random() as r from functional.alltypessmall order by r").data,
        lambda x: float(x))
    assert(results[0] == sorted(results[0]))

    # Like above, but with a limit.
    results = transpose_results(self.execute_query(
        "select random() as r from functional.alltypes order by r limit 100").data,
        lambda x: float(x))
    assert(results == sorted(results))

    # "order by random()" inside an inline view.
    query = "select r from (select random() r from functional.alltypessmall) v order by r"
    results = transpose_results(self.execute_query(query).data, lambda x: float(x))
    assert (results == sorted(results))

  def test_analytic_order_by_random(self):
    """Tests that a window function over 'order by random()' works as expected."""
    # Since we use the same random seed, the results should be returned in order.
    query = """select last_value(rand(2)) over (order by rand(2)) from
      functional.alltypestiny"""
    results = transpose_results(self.execute_query(query).data, lambda x: float(x))
    assert (results == sorted(results))
