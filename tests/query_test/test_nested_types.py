#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestNestedTypes(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestNestedTypes, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet')

  def test_scanner_basic(self, vector):
    """Queries that do not materialize arrays."""
    self.run_test_case('QueryTest/nested-types-scanner-basic', vector)

  def test_scanner_array_materialization(self, vector):
    """Queries that materialize arrays."""
    self.run_test_case('QueryTest/nested-types-scanner-array-materialization', vector)

  def test_scanner_multiple_materialization(self, vector):
    """Queries that materialize the same array multiple times."""
    self.run_test_case('QueryTest/nested-types-scanner-multiple-materialization', vector)

  def test_scanner_position(self, vector):
    """Queries that materialize the artifical position element."""
    self.run_test_case('QueryTest/nested-types-scanner-position', vector)

  def test_scanner_map(self, vector):
    """Queries that materialize maps. (Maps looks like arrays of key/value structs, so
    most map functionality is already tested by the array tests.)"""
    self.run_test_case('QueryTest/nested-types-scanner-maps', vector)

  def test_runtime(self, vector):
    """Queries that send collections through the execution runtime."""
    pytest.skip("IMPALA-2295")
    self.run_test_case('QueryTest/nested-types-runtime', vector)

  def test_tpch(self, vector):
    """Queries over the larger nested TPCH dataset."""
    pytest.skip("IMPALA-2295")
    # This test takes a long time (minutes), only run in exhaustive
    if self.exploration_strategy() != 'exhaustive': pytest.skip()
    self.run_test_case('QueryTest/nested-types-tpch', vector)
