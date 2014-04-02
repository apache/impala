#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Tests for aggregation with codegen disabled

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_exec_option_dimension

class TestAggregationQueries(ImpalaTestSuite):
  """Run the aggregation test suite, with codegen enabled and disabled, to exercise our
  non-codegen code"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAggregationQueries, cls).add_test_dimensions()

    cls.TestMatrix.add_dimension(
      create_exec_option_dimension(disable_codegen_options=[False, True]))

  def test_non_codegen_tinyint_grouping(self, vector):
    # Regression for IMPALA-901. The test includes an INSERT statement, so can only be run
    # on INSERT-able formats - text only in this case, since the bug doesn't depend on the
    # file format.
    if vector.get_value('table_format').file_format == 'text' \
        and vector.get_value('table_format').compression_codec == 'none':
      self.run_test_case('QueryTest/aggregation_no_codegen_only', vector)

  def test_aggregation(self, vector):
    if vector.get_value('table_format').file_format == 'hbase':
      pytest.xfail(reason="IMPALA-283 - select count(*) produces inconsistent results")
    self.run_test_case('QueryTest/aggregation', vector)
