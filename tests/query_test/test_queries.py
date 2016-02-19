# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# General Impala query tests
#
import copy
import logging
import os
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import *
from tests.common.test_dimensions import create_uncompressed_text_dimension
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.skip import SkipIfS3

class TestQueries(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestQueries, cls).add_test_dimensions()
    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format == 'parquet')

    # Manually adding a test dimension here to test the small query opt
    # in exhaustive.
    # TODO Cleanup required, allow adding values to dimensions without having to
    # manually explode them
    if cls.exploration_strategy() == 'exhaustive':
      dim = cls.TestMatrix.dimensions["exec_option"]
      new_value = []
      for v in dim:
        new_value.append(TestVector.Value(v.name, copy.copy(v.value)))
        new_value[-1].value["exec_single_node_rows_threshold"] = 100
      dim.extend(new_value)
      cls.TestMatrix.add_dimension(dim)

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_hdfs_scan_node(self, vector):
    self.run_test_case('QueryTest/hdfs-scan-node', vector)

  def test_analytic_fns(self, vector):
    # TODO: Enable some of these tests for Avro if possible
    # Don't attempt to evaluate timestamp expressions with Avro tables which doesn't
    # support a timestamp type yet
    table_format = vector.get_value('table_format')
    if table_format.file_format == 'avro':
      pytest.skip()
    if table_format.file_format == 'hbase':
      pytest.xfail("A lot of queries check for NULLs, which hbase does not recognize")
    self.run_test_case('QueryTest/analytic-fns', vector)

  def test_file_partitions(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

  def test_limit(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("IMPALA-283 - select count(*) produces inconsistent results")
    self.run_test_case('QueryTest/limit', vector)

  def test_top_n(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    # QueryTest/top-n is also run in test_sort with disable_outermost_topn = 1
    self.run_test_case('QueryTest/top-n', vector)

  def test_union(self, vector):
    self.run_test_case('QueryTest/union', vector)

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

  def test_subplans(self, vector):
    pytest.xfail("Disabled due to missing nested types functionality.")
    if vector.get_value('table_format').file_format != 'parquet':
      pytest.xfail("Nested TPCH only available in parquet.")
    self.run_test_case('QueryTest/subplannull_data', vector)

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
    if table_format.file_format in ['hbase', 'rc', 'parquet']:
      msg = ("Failing on rc/snap/block despite resolution of IMP-624,IMP-503. "
             "Failing on parquet because tables do not exist")
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
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_overflow(self, vector):
    self.run_test_case('QueryTest/overflow', vector)

  def test_data_source_tables(self, vector):
    self.run_test_case('QueryTest/data-source-tables', vector)

  def test_distinct_estimate(self, vector):
    # These results will vary slightly depending on how the values get split up
    # so only run with 1 node and on text.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/distinct-estimate', vector)

  def test_mixed_format(self, vector):
    self.run_test_case('QueryTest/mixed-format', vector)

  @SkipIfS3.insert
  def test_values(self, vector):
    self.run_test_case('QueryTest/values', vector)

# Tests in this class are only run against Parquet because the tests don't exercise the
# file format.
class TestQueriesParquetTables(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestQueriesParquetTables, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @pytest.mark.execute_serially
  def test_very_large_strings(self, vector):
    """Regression test for IMPALA-1619. Doesn't need to be run on all file formats.
       Executes serially to avoid large random spikes in mem usage."""
    self.run_test_case('QueryTest/large_strings', vector)
