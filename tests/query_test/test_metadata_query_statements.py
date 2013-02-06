#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Impala tests for queries that query metadata and set session settings
import logging
import pytest
from subprocess import call
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

  def setup_method(self, method):
    self.cleanup_db('hive_test_db')

  def teardown_method(self, method):
    self.cleanup_db('hive_test_db')

  def test_show_tables(self, vector):
    self.run_test_case('QueryTest/show', vector)

  def test_describe_table(self, vector):
    self.run_test_case('QueryTest/describe', vector)

  def test_use_table(self, vector):
    self.run_test_case('QueryTest/use', vector)

  def cleanup_db(cls, db_name):
    # To drop a db, we need to first drop all the tables in that db
    if db_name in cls.hive_client.get_all_databases():
      for table_name in cls.hive_client.get_all_tables(db_name):
        cls.hive_client.drop_table(db_name, table_name, True)
      cls.hive_client.drop_database(db_name, True, False)

  @pytest.mark.execute_serially
  def test_impala_sees_hive_created_tables_and_databases(self, vector):
    db_name = 'hive_test_db'
    tbl_name = 'testtbl'
    self.client.refresh()
    result = self.execute_query("show databases");
    assert db_name not in result.data
    call(["hive", "-e", "CREATE DATABASE %s" % db_name])

    result = self.execute_query("show databases");
    assert db_name not in result.data

    self.client.refresh()
    result = self.execute_query("show databases");
    assert db_name in result.data

    # Make sure no tables show up in the new database
    result = self.execute_query("show tables in %s" % db_name);
    assert len(result.data) == 0

    self.client.refresh()
    result = self.execute_query("show tables in %s" % db_name);
    assert len(result.data) == 0

    call(["hive", "-e", "CREATE TABLE %s.%s (i int)" % (db_name, tbl_name)])

    result = self.execute_query("show tables in %s" % db_name)
    assert tbl_name not in result.data

    self.client.refresh()
    result = self.execute_query("show tables in %s" % db_name)
    assert tbl_name in result.data

    # Make sure we can actually use the table
    self.execute_query("insert overwrite table %s.%s select 1 from alltypes limit 5"\
        % (db_name, tbl_name))
    result = self.execute_scalar("select count(*) from %s.%s" % (db_name, tbl_name))
    assert int(result) == 5

    call(["hive", "-e", "DROP TABLE %s.%s " % (db_name, tbl_name)])
    call(["hive", "-e", "DROP DATABASE %s" % db_name])

    # Requires a refresh to see the dropped database
    result = self.execute_query("show databases");
    assert db_name in result.data

    self.client.refresh()
    result = self.execute_query("show databases");
    assert db_name not in result.data
