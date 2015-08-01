# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala HBase Tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestHBaseQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHBaseQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format == 'hbase')

  def test_hbase_scan_node(self, vector):
    self.run_test_case('QueryTest/hbase-scan-node', vector)

  def test_hbase_row_key(self, vector):
    self.run_test_case('QueryTest/hbase-rowkeys', vector)

  def test_hbase_filters(self, vector):
    self.run_test_case('QueryTest/hbase-filters', vector)

  def test_hbase_subquery(self, vector):
    self.run_test_case('QueryTest/hbase-subquery', vector)

  def test_hbase_inline_views(self, vector):
    self.run_test_case('QueryTest/hbase-inline-view', vector)

  def test_hbase_top_n(self, vector):
    self.run_test_case('QueryTest/hbase-top-n', vector)

  def test_hbase_limits(self, vector):
    self.run_test_case('QueryTest/hbase-limit', vector)

  @pytest.mark.execute_serially
  def test_hbase_inserts(self, vector):
    self.run_test_case('QueryTest/hbase-inserts', vector)
