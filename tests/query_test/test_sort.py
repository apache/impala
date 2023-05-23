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
import re
from copy import copy, deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfNotHdfsMinicluster
from tests.common.test_vector import ImpalaTestDimension

# Run sizes (number of pages per run) in sorter
MAX_SORT_RUN_SIZE = [0, 2, 20]


def split_result_rows(result):
  """Split result rows by tab to produce a list of lists. i.e.
     [[a1,a2], [b1, b2], [c1, c2]]"""
  return [row.split('\t') for row in result]


def transpose_results(result, map_fn=lambda x: x):
  """Given a query result (list of strings, each string represents a row), return a list
     of columns, where each column is a list of strings. Optionally, map_fn can be
     provided to be applied to every value, eg. to convert the strings to their
     underlying types."""

  split_result = split_result_rows(result)
  column_result = []
  for col in zip(*split_result):
    # col is the transposed result, i.e. a1, b1, c1
    # Apply map_fn to all elements
    column_result.append([map_fn(x) for x in col])

  return column_result


class TestQueryFullSort(ImpalaTestSuite):
  """Test class to do functional validation of sorting when data is spilled to disk."""

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryFullSort, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('max_sort_run_size',
        *MAX_SORT_RUN_SIZE))

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet')

  def test_multiple_buffer_pool_limits(self, vector):
    """Using lineitem table forces the multi-phase sort with low buffer_pool_limit.
       This test takes about a minute."""
    query = """select l_comment, l_partkey, l_orderkey, l_suppkey, l_commitdate
            from lineitem order by l_comment limit 100000"""
    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['num_nodes'] = 1
    table_format = vector.get_value('table_format')

    """The first run should fit in memory, the second run is a 2-phase disk sort,
       and the third run is a multi-phase sort (i.e. with an intermediate merge)."""
    for buffer_pool_limit in ['-1', '300m', '130m']:
      exec_option['buffer_pool_limit'] = buffer_pool_limit
      query_result = self.execute_query(
        query, exec_option, table_format=table_format)
      result = transpose_results(query_result.data)
      assert(result[0] == sorted(result[0]))

  def test_multiple_sort_run_bytes_limits(self, vector):
    """Using lineitem table forces the multi-phase sort with low sort_run_bytes_limit.
       This test takes about a minute."""
    query = """select l_comment, l_partkey, l_orderkey, l_suppkey, l_commitdate
            from lineitem order by l_comment limit 100000"""
    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['num_nodes'] = 1
    table_format = vector.get_value('table_format')

    """The first sort run is given a privilege to ignore sort_run_bytes_limit, except
       when estimate hints that spill is inevitable. The lower sort_run_bytes_limit of
       a query is, the more sort runs are likely to be produced and spilled.
       Case 1 : 0 SpilledRuns, because all rows fit within the maximum reservation.
                sort_run_bytes_limit is not enforced.
       Case 2 : 4 SpilledRuns, because sort node estimate that spill is inevitable.
                So all runs are capped to 130m, including the first one."""
    options = [('2g', '100m', '0'), ('400m', '130m', '4')]
    for (mem_limit, sort_run_bytes_limit, spilled_runs) in options:
      exec_option['mem_limit'] = mem_limit
      exec_option['sort_run_bytes_limit'] = sort_run_bytes_limit
      query_result = self.execute_query(
          query, exec_option, table_format=table_format)
      m = re.search(r'\s+\- SpilledRuns: .*', query_result.runtime_profile)
      assert "SpilledRuns: " + spilled_runs in m.group()
      result = transpose_results(query_result.data)
      assert(result[0] == sorted(result[0]))

  def test_multiple_mem_limits_full_output(self, vector):
    """ Exercise a range of memory limits, returning the full sorted input. """
    query = """select o_orderdate, o_custkey, o_comment
      from orders
      order by o_orderdate"""
    exec_option = copy(vector.get_value('exec_option'))
    table_format = vector.get_value('table_format')
    exec_option['default_spillable_buffer_size'] = '8M'

    # Minimum memory for different parts of the plan.
    buffered_plan_root_sink_reservation_mb = 16
    sort_reservation_mb = 48
    if table_format.file_format == 'parquet':
      scan_reservation_mb = 24
    else:
      scan_reservation_mb = 8
    total_reservation_mb = sort_reservation_mb + scan_reservation_mb \
                           + buffered_plan_root_sink_reservation_mb

    # The below memory value assume 8M pages.
    # Test with unlimited and minimum memory for all file formats.
    buffer_pool_limit_values = ['-1', '{0}M'.format(total_reservation_mb)]
    if self.exploration_strategy() == 'exhaustive' and \
        table_format.file_format == 'parquet':
      # Test some intermediate values for parquet on exhaustive.
      buffer_pool_limit_values += ['128M', '256M']
    for buffer_pool_limit in buffer_pool_limit_values:
      exec_option['buffer_pool_limit'] = buffer_pool_limit
      result = transpose_results(self.execute_query(
        query, exec_option, table_format=table_format).data)
      assert(result[0] == sorted(result[0]))

  def test_sort_join(self, vector):
    """With minimum memory limit this should be a 1-phase sort"""
    query = """select o1.o_orderdate, o2.o_custkey, o1.o_comment from orders o1 join
    orders o2 on (o1.o_orderkey = o2.o_orderkey) order by o1.o_orderdate limit 100000"""

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['mem_limit'] = "134m"
    exec_option['num_nodes'] = 1
    table_format = vector.get_value('table_format')

    query_result = self.execute_query(query, exec_option, table_format=table_format)
    assert "TotalMergesPerformed: 1" in query_result.runtime_profile
    result = transpose_results(query_result.data)
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

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_sort_reservation_usage(self, vector):
    """Tests for sorter reservation usage."""
    new_vector = deepcopy(vector)
    # Run with num_nodes=1 to make execution more deterministic.
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('sort-reservation-usage-single-node', new_vector)

class TestRandomSort(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestRandomSort, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('max_sort_run_size',
        *MAX_SORT_RUN_SIZE))

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'parquet')

  def test_order_by_random(self, vector):
    """Tests that 'order by random()' works as expected."""
    exec_option = copy(vector.get_value('exec_option'))
    # "order by random()" with different seeds should produce different orderings.
    seed_query = "select * from functional.alltypestiny order by random(%s)"
    results_seed0 = self.execute_query(seed_query % "0")
    results_seed1 = self.execute_query(seed_query % "1")
    assert results_seed0.data != results_seed1.data
    assert sorted(results_seed0.data) == sorted(results_seed1.data)

    # Include "random()" in the select list to check that it's sorted correctly.
    results = transpose_results(self.execute_query(
        "select random() as r from functional.alltypessmall order by r",
        exec_option).data, lambda x: float(x))
    assert(results[0] == sorted(results[0]))

    # Like above, but with a limit.
    results = transpose_results(self.execute_query(
        "select random() as r from functional.alltypes order by r limit 100").data,
        lambda x: float(x))
    assert(results == sorted(results))

    # "order by random()" inside an inline view.
    query = "select r from (select random() r from functional.alltypessmall) v order by r"
    results = transpose_results(self.execute_query(query, exec_option).data,
        lambda x: float(x))
    assert (results == sorted(results))

  def test_analytic_order_by_random(self, vector):
    """Tests that a window function over 'order by random()' works as expected."""
    exec_option = copy(vector.get_value('exec_option'))
    # Since we use the same random seed, the results should be returned in order.
    query = """select last_value(rand(2)) over (order by rand(2)) from
      functional.alltypestiny"""
    results = transpose_results(self.execute_query(query, exec_option).data,
        lambda x: float(x))
    assert (results == sorted(results))



class TestPartialSort(ImpalaTestSuite):
  """Test class to do functional validation of partial sorts."""

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestPartialSort, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('max_sort_run_size',
        *MAX_SORT_RUN_SIZE))

    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('table_format').file_format == 'parquet')

  def test_partial_sort_min_reservation(self, vector, unique_database):
    """Test that the partial sort node can operate if it only gets its minimum
    memory reservation."""
    table_name = "%s.kudu_test" % unique_database
    self.client.set_configuration_option(
        "debug_action", "-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@1.0")
    self.execute_query("""create table %s (col0 string primary key)
        partition by hash(col0) partitions 8 stored as kudu""" % table_name)
    exec_option = copy(vector.get_value('exec_option'))
    result = self.execute_query(
        "insert into %s select string_col from functional.alltypessmall" % table_name,
        exec_option)
    assert "PARTIAL SORT" in result.runtime_profile, result.runtime_profile

  def test_partial_sort_kudu_insert(self, vector, unique_database):
    table_name = "%s.kudu_partial_sort_test" % unique_database
    self.execute_query("""create table %s (l_linenumber INT, l_orderkey BIGINT,
      l_partkey BIGINT, l_shipdate STRING, l_quantity DECIMAL(12,2),
      l_comment STRING, PRIMARY KEY(l_linenumber, l_orderkey) )
      PARTITION BY RANGE (l_linenumber)
      (
        PARTITION VALUE = 1,
        PARTITION VALUE = 2,
        PARTITION VALUE = 3,
        PARTITION VALUE = 4,
        PARTITION VALUE = 5,
        PARTITION VALUE = 6,
        PARTITION VALUE = 7
      )
      STORED AS KUDU""" % table_name)
    exec_option = copy(vector.get_value('exec_option'))
    result = self.execute_query(
        """insert into %s SELECT l_linenumber, l_orderkey, l_partkey, l_shipdate,
        l_quantity, l_comment FROM tpch.lineitem limit 300000""" % table_name,
        exec_option)
    assert "NumModifiedRows: 300000" in result.runtime_profile, result.runtime_profile
    assert "NumRowErrors: 0" in result.runtime_profile, result.runtime_profile


class TestArraySort(ImpalaTestSuite):
  """Tests where there are arrays in the sorting tuple."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestArraySort, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('max_sort_run_size',
        *MAX_SORT_RUN_SIZE))
    # The table we use is a parquet table.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def test_array_sort(self, vector):
    self._run_test_sort_query(vector, None)

  def test_array_sort_with_limit(self, vector):
    self._run_test_sort_query(vector, 3000)

  def _run_test_sort_query(self, vector, limit):
    """Test sorting with spilling where an array that contains var-len data is in the
    sorting tuple. If 'limit' is set to an integer, applies that limit."""
    query = """select string_col, int_array, double_map, string_array, mixed
        from functional_parquet.arrays_big order by string_col"""

    if limit:
      assert isinstance(limit, int)
      limit_clause = " limit {}".format(limit)
      query = query + limit_clause

    exec_option = copy(vector.get_value('exec_option'))
    exec_option['disable_outermost_topn'] = 1
    exec_option['num_nodes'] = 1
    exec_option['buffer_pool_limit'] = '44m'
    table_format = vector.get_value('table_format')

    query_result = self.execute_query(query, exec_option, table_format=table_format)
    assert "SpilledRuns: 3" in query_result.runtime_profile

    # Split result rows (strings) into columns.
    result = split_result_rows(query_result.data)
    # Sort the result rows according to the first column.
    sorted_result = sorted(result, key=lambda row: row[0])
    assert(result == sorted_result)
