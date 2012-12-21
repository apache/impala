#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Tests for Union
#
import logging
import pytest
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.impala_test_suite import ImpalaTestSuite, SINGLE_NODE_ONLY

class TestUnion(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnion, cls).add_test_dimensions()
    # TODO: Not all the union tests can be executed in distributed mode because:
    # 1) Distributed UNION with constant SELECT clauses not implemented
    # 2) Order of results returned is different with different cluster sizes.
    #    The current test infrastructure just checks if an 'ORDER BY' clause is
    #    anywhere in the query.
    cls.TestMatrix.add_dimension(\
        create_exec_option_dimension(cluster_sizes=SINGLE_NODE_ONLY))

  def test_union(self, vector):
    self.run_test_case('QueryTest/union', vector)
