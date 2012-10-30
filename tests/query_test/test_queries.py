#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# General Impala query tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestQueries(ImpalaTestSuite):
  @classmethod
  def get_dataset(cls):
    return 'functional-query'

  def test_distinct(self, vector):
    self.run_test_case('QueryTest/distinct', vector)

  def test_aggregation(self, vector):
    self.run_test_case('QueryTest/aggregation', vector)

  def test_exprs(self, vector):
    self.run_test_case('QueryTest/exprs', vector)

  def test_hdfs_scan_node(self, vector):
    self.run_test_case('QueryTest/hdfs-scan-node', vector)

  def test_scan_range(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

  def test_file_partitions(self, vector):
    self.run_test_case('QueryTest/hdfs-partitions', vector)

  def test_limit(self, vector):
    self.run_test_case('QueryTest/limit', vector)

  def test_top_n(self, vector):
    self.run_test_case('QueryTest/top-n', vector)

  def test_empty(self, vector):
    self.run_test_case('QueryTest/empty', vector)

  def test_subquery(self, vector):
    self.run_test_case('QueryTest/subquery', vector)

  def test_union(self, vector):
    # TODO: Some queries are NYI - How did this work before?
    return
    self.run_test_case('QueryTest/union', vector)

  def test_misc(self, vector):
    return
    self.run_test_case('QueryTest/misc', vector)
