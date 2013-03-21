#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for DDL statements
import logging
import pytest
from tests.common.test_vector import *
from tests.common.test_dimensions import ALL_NODES_ONLY
from tests.common.impala_test_suite import *

# Validates DDL statements (create, drop)
class TestDdlStatements(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDdlStatements, cls).add_test_dimensions()
    # There is no reason to run these tests using all dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    map(self.cleanup_db, ['ddl_test_db', 'alter_table_test_db', 'alter_table_test_db2'])

  def teardown_method(self, method):
    return
    map(self.cleanup_db, ['ddl_test_db', 'alter_table_test_db', 'alter_table_test_db2'])

  def cleanup_db(cls, db_name):
    # To drop a db, we need to first drop all the tables in that db
    if db_name in cls.hive_client.get_all_databases():
      for table_name in cls.hive_client.get_all_tables(db_name):
        cls.hive_client.drop_table(db_name, table_name, True)
      cls.hive_client.drop_database(db_name, True, False)
    cls.client.refresh()

  @pytest.mark.execute_serially
  def test_create(self, vector):
    self.run_test_case('QueryTest/create', vector)

  @pytest.mark.execute_serially
  def test_alter_table(self, vector):
    self.run_test_case('QueryTest/alter-table', vector)
