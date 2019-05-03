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

# General Impala query tests

import pytest
import re
from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfEC, SkipIfDockerizedCluster
from tests.common.test_dimensions import (
    create_uncompressed_text_dimension, extend_exec_option_dimension,
    create_beeswax_hs2_dimension, hs2_parquet_constraint)
from tests.common.test_vector import ImpalaTestVector

class TestQueries(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestQueries, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet')
    # Run these queries through both beeswax and HS2 to get coverage of both protocols.
    # Don't run all combinations of table format and protocol - the dimensions should
    # be orthogonal.
    cls.ImpalaTestMatrix.add_dimension(create_beeswax_hs2_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_parquet_constraint)

    # Adding a test dimension here to test the small query opt in exhaustive.
    if cls.exploration_strategy() == 'exhaustive':
      extend_exec_option_dimension(cls, "exec_single_node_rows_threshold", "100")

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_analytic_fns(self, vector):
    # TODO: Enable some of these tests for Avro if possible
    # Don't attempt to evaluate timestamp expressions with Avro tables which don't
    # support a timestamp type
    table_format = vector.get_value('table_format')
    if table_format.file_format == 'avro':
      pytest.xfail("%s doesn't support TIMESTAMP" % (table_format.file_format))
    if table_format.file_format == 'hbase':
      pytest.xfail("A lot of queries check for NULLs, which hbase does not recognize")
    self.run_test_case('QueryTest/analytic-fns', vector)

  def test_limit(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("IMPALA-283 - select count(*) produces inconsistent results")
    if vector.get_value('table_format').file_format == 'kudu':
      pytest.xfail("Limit queries without order by clauses are non-deterministic")
    self.run_test_case('QueryTest/limit', vector)

  def test_top_n(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    # QueryTest/top-n is also run in test_sort with disable_outermost_topn = 1
    self.run_test_case('QueryTest/top-n', vector)

  def test_union(self, vector):
    self.run_test_case('QueryTest/union', vector)
    # IMPALA-3586: The passthrough and materialized children are interleaved. The batch
    # size is small to test the transition between materialized and passthrough children.
    query_string = ("select count(c) from ( "
        "select bigint_col + 1 as c from functional.alltypes limit 15 "
        "union all "
        "select bigint_col as c from functional.alltypes limit 15 "
        "union all "
        "select bigint_col + 1 as c from functional.alltypes limit 15 "
        "union all "
        "(select bigint_col as c from functional.alltypes limit 15)) t")
    vector.get_value('exec_option')['batch_size'] = 10
    result = self.execute_query(query_string, vector.get_value('exec_option'))
    assert result.data[0] == '60'

  def test_sort(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    vector.get_value('exec_option')['disable_outermost_topn'] = 1
    self.run_test_case('QueryTest/sort', vector)
    # We can get the sort tests for free from the top-n file
    self.run_test_case('QueryTest/top-n', vector)

  def test_inline_view(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("jointbl does not have columns with unique values, "
                   "hbase collapses them")
    self.run_test_case('QueryTest/inline-view', vector)

  def test_inline_view_limit(self, vector):
    self.run_test_case('QueryTest/inline-view-limit', vector)

  def test_subquery(self, vector):
    self.run_test_case('QueryTest/subquery', vector)

  def test_subquery_single_node(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/subquery-single-node', new_vector)

  def test_alias(self, vector):
    self.run_test_case('QueryTest/alias', vector)

  def test_subquery_in_constant_lhs(self, vector):
    self.run_test_case('QueryTest/subquery-in-constant-lhs', vector)

  def test_empty(self, vector):
    self.run_test_case('QueryTest/empty', vector)

  def test_views(self, vector):
    if vector.get_value('table_format').file_format == "hbase":
      pytest.xfail("TODO: Enable views tests for hbase")
    self.run_test_case('QueryTest/views', vector)

  def test_with_clause(self, vector):
    if vector.get_value('table_format').file_format == "hbase":
      pytest.xfail("TODO: Enable with clause tests for hbase")
    self.run_test_case('QueryTest/with-clause', vector)

  def test_misc(self, vector):
    table_format = vector.get_value('table_format')
    if table_format.file_format in ['hbase', 'rc', 'parquet', 'kudu']:
      msg = ("Failing on rc/snap/block despite resolution of IMP-624,IMP-503. "
             "Failing on kudu and parquet because tables do not exist")
      pytest.xfail(msg)
    self.run_test_case('QueryTest/misc', vector)

  def test_null_data(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("null data does not appear to work in hbase")
    self.run_test_case('QueryTest/null_data', vector)

# Tests in this class are only run against text/none either because that's the only
# format that is supported, or the tests don't exercise the file format.
class TestQueriesTextTables(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestQueriesTextTables, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_overflow(self, vector):
    self.run_test_case('QueryTest/overflow', vector)

  def test_strict_mode(self, vector):
    vector.get_value('exec_option')['strict_mode'] = 1
    vector.get_value('exec_option')['abort_on_error'] = 0
    self.run_test_case('QueryTest/strict-mode', vector)

    vector.get_value('exec_option')['abort_on_error'] = 1
    self.run_test_case('QueryTest/strict-mode-abort', vector)

  def test_data_source_tables(self, vector):
    self.run_test_case('QueryTest/data-source-tables', vector)

  def test_distinct_estimate(self, vector):
    # These results will vary slightly depending on how the values get split up
    # so only run with 1 node and on text.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/distinct-estimate', vector)

  @SkipIfEC.oom
  def test_random(self, vector):
    # These results will vary slightly depending on how the values get split up
    # so only run with 1 node and on text.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/random', vector)

  @SkipIfEC.oom
  def test_values(self, vector):
    self.run_test_case('QueryTest/values', vector)

# Tests in this class are only run against Parquet because the tests don't exercise the
# file format.
class TestQueriesParquetTables(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestQueriesParquetTables, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIfEC.oom
  @pytest.mark.execute_serially
  def test_very_large_strings(self, vector):
    """Regression test for IMPALA-1619. Doesn't need to be run on all file formats.
       Executes serially to avoid large random spikes in mem usage."""
    self.run_test_case('QueryTest/large_strings', vector)

  def test_single_node_large_sorts(self, vector):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("only run large sorts on exhaustive")

    vector.get_value('exec_option')['disable_outermost_topn'] = 1
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/single-node-large-sorts', vector)

# Tests for queries in HDFS-specific tables, e.g. AllTypesAggMultiFilesNoPart.
class TestHdfsQueries(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestHdfsQueries, cls).add_test_dimensions()
    # Kudu doesn't support AllTypesAggMultiFilesNoPart (KUDU-1271, KUDU-1570).
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format != 'kudu')

    # Adding a test dimension here to test the small query opt in exhaustive.
    if cls.exploration_strategy() == 'exhaustive':
      extend_exec_option_dimension(cls, "exec_single_node_rows_threshold", "100")

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @SkipIfEC.oom
  def test_hdfs_scan_node(self, vector):
    self.run_test_case('QueryTest/hdfs-scan-node', vector)

  def test_file_partitions(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

class TestTopNReclaimQuery(ImpalaTestSuite):
  """Test class to validate that TopN periodically reclaims tuple pool memory
   and runs with a lower memory footprint."""
  QUERY = "select * from tpch.lineitem order by l_orderkey desc limit 10;"

  # Mem limit empirically selected so that the query fails if tuple pool reclamation
  # is not implemented for TopN
  MEM_LIMIT = "60m"

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestTopNReclaimQuery, cls).add_test_dimensions()
    # The tpch tests take a long time to execute so restrict the combinations they
    # execute over.
    cls.ImpalaTestMatrix.add_dimension(
      create_uncompressed_text_dimension(cls.get_workload()))

  def test_top_n_reclaim(self, vector):
    exec_options = vector.get_value('exec_option')
    exec_options['mem_limit'] = self.MEM_LIMIT
    exec_options['num_scanner_threads'] = 1
    result = self.execute_query(self.QUERY, exec_options)
    runtime_profile = str(result.runtime_profile)
    num_of_times_tuple_pool_reclaimed = re.findall(
      'TuplePoolReclamations: ([0-9]*)', runtime_profile)
    # Confirm newly added counter is visible
    assert len(num_of_times_tuple_pool_reclaimed) > 0
    # Tuple pool is expected to be reclaimed for this query
    for n in num_of_times_tuple_pool_reclaimed:
      assert int(n) > 0
