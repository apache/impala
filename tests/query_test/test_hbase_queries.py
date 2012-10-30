#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala HBase Tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestHBaseQueries(ImpalaTestSuite):
  @classmethod
  def get_dataset(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHBaseQueries, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format == 'text')

  def test_hbase_scan_node(self, vector):
    self.run_test_case('QueryTest/hbase-scan-node', vector)

  def test_hbase_row_key(self, vector):
    self.run_test_case('QueryTest/hbase-rowkeys', vector)

  def test_hbase_filters(self, vector):
    self.run_test_case('QueryTest/hbase-filters', vector)
