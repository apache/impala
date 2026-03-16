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
import time

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_parquet_dimension,
    create_single_exec_option_dimension,
)
from tests.util.test_file_parser import remove_comments

QUERY_OPTIONS = {'use_hbo_stats': True, 'store_hbo_stats': True}


class TestHBO(ImpalaTestSuite):
  """Tests for HBO (History-Based Optimization) cardinality tracking."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestHBO, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_parquet_dimension(cls.get_workload()))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def _run_hbo_explains(self, test_file):
    """Run EXPLAIN queries from a golden .test file and verify their full output
    against the ---- PLAN sections."""
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    test_cases = self.load_query_test_file(
        'functional-query', test_file,
        valid_section_names=['QUERY', 'PLAN'])
    self.client.set_configuration({'use_hbo_stats': True})
    for section in test_cases:
      query = remove_comments(section['QUERY'].strip())
      result = self.execute_query(query)
      actual_plan_lines = result.data[result.data.index('PLAN-ROOT SINK'):]
      expected_plan_lines = section['PLAN'].splitlines()
      # To avoid the test being fragile, we only compare the cardinality lines.
      actual_cardinality_lines = [line for line in actual_plan_lines
                                  if 'cardinality' in line]
      expected_cardinality_lines = [line for line in expected_plan_lines
                                    if 'cardinality' in line]
      assert actual_cardinality_lines == expected_cardinality_lines, (
          "EXPLAIN output mismatch for {0}.\nExpected:\n{1}\n\nActual:\n{2}".format(
              test_file, section['PLAN'], '\n'.join(actual_plan_lines)))

  def test_single_scan_cardinality_partitioned_with_stats(self):
    self.client.set_configuration(QUERY_OPTIONS)
    # Run query to populate HBO stats
    self.execute_query("""
        SELECT count(*) FROM functional.alltypes
        WHERE year=2009 AND int_col=1 AND string_col='1'""")
    self._run_hbo_explains('QueryTest/hbo-single-scan-partitioned-stats')

  def test_single_scan_cardinality_partitioned_no_stats(self):
    self.client.set_configuration(QUERY_OPTIONS)
    # Run query to populate HBO stats
    self.execute_query("""
        SELECT count(*) FROM functional_parquet.alltypes
        WHERE year=2009 AND int_col=1 AND string_col='1'""")
    self._run_hbo_explains('QueryTest/hbo-single-scan-partitioned-no-stats')
    # Refresh the table to bump the catalog version. So we can test HBO matches the input
    # file size when HMS stats (numRows) are missing.
    self.execute_query("refresh functional_parquet.alltypes")
    self._run_hbo_explains('QueryTest/hbo-single-scan-partitioned-no-stats')

  def test_single_scan_cardinality_non_partitioned_with_stats(self):
    self.client.set_configuration(QUERY_OPTIONS)
    # Run query to populate HBO stats
    self.execute_query("""
        SELECT count(*) FROM functional.alltypes_nonpartitioned
        WHERE year=2009 AND int_col=1 AND string_col='1'""")
    self._run_hbo_explains('QueryTest/hbo-single-scan-nonpartitioned-stats')

  def test_single_scan_cardinality_non_partitioned_no_stats(self):
    self.client.set_configuration(QUERY_OPTIONS)
    # Run query to populate HBO stats
    self.execute_query("""
        SELECT count(*) FROM functional_parquet.alltypes_nonpartitioned
        WHERE year=2009 AND int_col=1 AND string_col='1'""")
    # Refresh the table to bump the catalog version. So we can test HBO matches the input
    # file size when HMS stats (numRows) are missing.
    self.execute_query("refresh functional_parquet.alltypes_nonpartitioned")
    self._run_hbo_explains('QueryTest/hbo-single-scan-nonpartitioned-no-stats')

  def test_multiple_scans_cardinality(self):
    self.client.set_configuration(QUERY_OPTIONS)
    self.execute_query("""
      select count(*) from functional.alltypes a, functional.alltypes b
      where a.id = b.id
        and a.year = 2010 and a.month = 1 and a.int_col = 0
        and b.year = 2010 and b.int_col = 0 and b.string_col = '0'
    """)
    self._run_hbo_explains('QueryTest/hbo-multiple-scans')

  def test_disjunct_selectivity(self):
    self.client.set_configuration(QUERY_OPTIONS)
    # Using disjuncts on correlated columns. The planner estimates a wrong selectivity
    # since it considers the columns are independent.
    # TODO(IMPALA-15098): canonicalize the disjuncts so their order doesn't matter.
    self.execute_query("""
        select count(*) from functional.alltypes
        where tinyint_col = 0 or smallint_col = 0 or int_col = 0 or bigint_col = 0
          or string_col = '0'
    """)
    self._run_hbo_explains('QueryTest/hbo-disjunct-selectivity')

  def test_collection_scan_cardinality(self):
    stmt = "SELECT %s FROM functional_parquet.complextypestbl.int_array"
    self.client.set_configuration(QUERY_OPTIONS)
    where_exprs_cards = {
      "": "10",
      "pos = 0": "3",
      "pos > 1": "5",
      "item > 0": "6",
      "item > 0 and pos > 1": "3",
    }
    for where_expr in where_exprs_cards:
      where = " WHERE " + where_expr if len(where_expr) > 0 else ""
      # Run query to populate HBO stats
      self.execute_query(stmt % "count(*)" + where)
    self._run_hbo_explains('QueryTest/hbo-collection-scan')

  def test_iceberg_scan_cardinality(self):
    self.client.set_configuration(QUERY_OPTIONS)
    self.execute_query(
        "select count(id) from functional_parquet.iceberg_partitioned where id > 10")
    self._run_hbo_explains('QueryTest/hbo-iceberg-scan')

  def test_failed_queries(self):
    self.client.set_configuration({
        **QUERY_OPTIONS, 'abort_on_error': True})
    query = "select * from functional.alltypeserror"
    res = self.execute_query_expect_failure(self.client, query)
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    # EXPLAIN the same query shouldn't see HBO stats.
    res = self.execute_query("EXPLAIN " + query)
    assert "from HBO" not in '\n'.join(res.data)

  def test_cancellation(self):
    """Test that cancelled PlanNodes don't generate HBO stats"""
    self.client.set_configuration(QUERY_OPTIONS)
    # The following query scans lineitem 3 times. Two scans in the inline view 'l' are
    # cancelled due to reaching the LIMIT 125000.
    query = """
        with l as (select * from tpch.lineitem UNION ALL select * from tpch.lineitem)
        select STRAIGHT_JOIN count(*) from
          (select * from tpch.lineitem a LIMIT 1) a
        join
          (select * from l LIMIT 125000) b
        on a.l_orderkey = -b.l_orderkey"""
    self.execute_query(query)
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    # No HBO stats on scan node of the lineitem table.
    res = self.execute_query("explain select * from tpch.lineitem")
    assert "from HBO" not in '\n'.join(res.data)

  def test_effective_runtime_filters(self):
    self.client.set_configuration({
        **QUERY_OPTIONS, 'runtime_filter_wait_time_ms': 10000})
    self.execute_query("""
        select STRAIGHT_JOIN count(*)
        from functional.alltypes a join [SHUFFLE] functional.alltypestiny b
          on a.month = b.month + 10000
        where a.string_col < 'hbo_test' and b.string_col < 'hbo_test'""")
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    # Scan node of the alltypes table with predicate string_col < 'hbo_test' has an
    # effective runtime filter. HBO stats on this shouldn't exist.
    res = self.execute_query(
        "explain select * from functional.alltypes where string_col < 'hbo_test'")
    assert "from HBO" not in '\n'.join(res.data), '\n'.join(res.data)
    # The other scan node doesn't have any runtime filters. HBO stats on this should
    # exist.
    res = self.execute_query(
        "explain select * from functional.alltypestiny where string_col < 'hbo_test'")
    assert "cardinality=8 (from HBO)" in '\n'.join(res.data), '\n'.join(res.data)

  def test_runtime_filter_annotation_with_hbo_cardinality(self):
    self.client.set_configuration(QUERY_OPTIONS)
    lineitem_preds = """l_shipdate >= '1994-01-01'
        and l_shipdate < '1995-01-01'
        and l_discount between 0.05 and 0.07
        and l_quantity < 24"""
    self.execute_query(
        "select count(*) from tpch_parquet.lineitem where " + lineitem_preds)
    self.execute_query("select count(*) from tpch_parquet.orders where o_custkey < 1000")
    res = self.execute_query("""
        explain select STRAIGHT_JOIN count(l_orderkey)
        from tpch_parquet.lineitem join tpch_parquet.orders
        on l_orderkey = o_orderkey
        where o_custkey < 1000 and """ + lineitem_preds)
    assert "cardinality=37.88K(filtered from 114.16K from HBO)" in '\n'.join(res.data), \
        '\n'.join(res.data)
