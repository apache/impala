#!/usr/bin/env python
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
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))
    # Insert is currently only supported for text and parquet
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['text'])
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert_permutation(self, vector):
    self.run_test_case('QueryTest/insert_permutation', vector)

  def teardown_method(self, method):
    map(self.cleanup_db, ["insert_permutation_test"])

  def cleanup_db(cls, db_name):
    # TODO: Find a common place to put this method
    # To drop a db, we need to first drop all the tables in that db
    if db_name in cls.hive_client.get_all_databases():
      for table_name in cls.hive_client.get_all_tables(db_name):
        cls.hive_client.drop_table(db_name, table_name, True)
      cls.hive_client.drop_database(db_name, True, False)
    cls.client.refresh()
