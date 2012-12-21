#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for queries that query metadata and set session settings
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

# TODO: For these tests to pass, all table metadata must be created exhaustively.
# the tests should be modified to remove that requirement.
class TestMetadataQueryStatements(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMetadataQueryStatements, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def test_show_tables(self, vector):
    self.run_test_case('QueryTest/show', vector)

  def test_describe_table(self, vector):
    self.run_test_case('QueryTest/describe', vector)

  def test_use_table(self, vector):
    self.run_test_case('QueryTest/use', vector)
