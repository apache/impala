# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension

class TestInsertQueriesWithPermutation(ImpalaTestSuite):
  """
  Tests for the column permutation feature of INSERT statements
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueriesWithPermutation, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    # TODO: do we need to run with multiple file formats? This seems to be really
    # targeting FE behavior.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def test_insert_permutation(self, vector):
    map(self.cleanup_db, ["insert_permutation_test"])
    self.run_test_case('QueryTest/insert_permutation', vector)

  def teardown_method(self, method):
    map(self.cleanup_db, ["insert_permutation_test"])
