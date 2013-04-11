#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# General Impala query tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite

class TestQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_distinct(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(("HBase returns columns in alphabetical order for select distinct *, "
                    "making result verication fail."))
    self.run_test_case('QueryTest/distinct', vector)

  def test_aggregation(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    self.run_test_case('QueryTest/aggregation', vector)

  def test_exprs(self, vector):
    # TODO: Enable some of these tests for Avro if possible
    # Don't attempt to evaluate timestamp expressions with Avro tables (which)
    # don't support a timestamp type)"
    if vector.get_value('table_format').file_format == 'avro':
      pytest.skip()
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("A lot of queries check for NULLs, which hbase does not recognize")
    self.run_test_case('QueryTest/exprs', vector)

  def test_hdfs_scan_node(self, vector):
    self.run_test_case('QueryTest/hdfs-scan-node', vector)

  def test_scan_range(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

  def test_file_partitions(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

  def test_limit(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail("IMPALA-283 - select count(*) produces inconsistant results")
    self.run_test_case('QueryTest/limit', vector)

  def test_top_n(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistant results")
    self.run_test_case('QueryTest/top-n', vector)

  def test_empty(self, vector):
    self.run_test_case('QueryTest/empty', vector)

  def test_subquery(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(("jointbl does not have columns with unique values, "
                    "hbase collapses them"))
    self.run_test_case('QueryTest/subquery', vector)

  def test_subquery_limit(self, vector):
    self.run_test_case('QueryTest/subquery-limit', vector)

  def test_mixed_format(self, vector):
    self.run_test_case('QueryTest/mixed-format', vector)

  def test_views(self, vector):
    if vector.get_value('table_format').file_format == "hbase":
      pytest.xfail("TODO: Enable views tests for hbase")
    self.run_test_case('QueryTest/views', vector)

  def test_with_clause(self, vector):
    if vector.get_value('table_format').file_format == "hbase":
      pytest.xfail("TODO: Enable with clause tests for hbase")
    self.run_test_case('QueryTest/with-clause', vector)

  def test_values(self, vector):
    # These tests do not read data from tables, so only run them a single time (text/none).
    table_format = vector.get_value('table_format')
    if (table_format.file_format == 'text' and table_format.compression_codec == 'none'):
      self.run_test_case('QueryTest/values', vector)

  def test_misc(self, vector):
    table_format = vector.get_value('table_format')
    if table_format.file_format in ['hbase', 'rc', 'parquet']:
      msg = ("Failing on rc/snap/block despite resolution of IMP-624,IMP-503. "
             "Failing on parquet because nulltable does not exist in parquet")
      pytest.xfail(msg)
    self.run_test_case('QueryTest/misc', vector)

  def test_overflow(self, vector):
    table_format = vector.get_value('table_format')
    if table_format.file_format != 'text' or table_format.compression_codec != 'none':
      pytest.xfail("Test limited to text/none")
    self.run_test_case('QueryTest/overflow', vector)
