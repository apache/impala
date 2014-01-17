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
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=ALL_NODES_ONLY,
        disable_codegen_options=[False],
        batch_sizes=[0],
        sync_ddl=[0, 1]))

    # There is no reason to run these tests using all table format dimensions.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text' and\
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    self.cleanup_db('hive_test_db')

  def teardown_method(self, method):
    self.cleanup_db('hive_test_db')

  def test_show(self, vector):
    self.run_test_case('QueryTest/show', vector)

  def test_show_stats(self, vector):
    self.run_test_case('QueryTest/show-stats', vector)

  def test_describe_table(self, vector):
    self.run_test_case('QueryTest/describe', vector)

  def test_describe_formatted(self, vector):
    # Describe a partitioned table.
    try:
      self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.alltypes")
      self.exec_and_compare_hive_and_impala_hs2(
          "describe formatted functional_text_lzo.alltypes")
      # Describe an unpartitioned table.
      self.exec_and_compare_hive_and_impala_hs2("describe formatted tpch.lineitem")
      self.exec_and_compare_hive_and_impala_hs2("describe formatted functional.jointbl")
    except Exception:
      pytest.xfail(reason="IMPALA-1085, hiveserver2 does not start on occasion")

    try:
      # Describe a view
      self.exec_and_compare_hive_and_impala_hs2(\
          "describe formatted functional.alltypes_view_sub")
    except AssertionError:
      pytest.xfail("Investigate minor difference in displaying null vs empty values")

  def test_use_table(self, vector):
    self.run_test_case('QueryTest/use', vector)

  @pytest.mark.execute_serially
  def test_impala_sees_hive_created_tables_and_databases(self, vector):
    self.client.set_configuration(vector.get_value('exec_option'))
    db_name = 'hive_test_db'
    tbl_name = 'testtbl'
    call(["hive", "-e", "DROP DATABASE IF EXISTS %s CASCADE" % db_name])
    self.client.execute("invalidate metadata")

    result = self.client.execute("show databases")
    assert db_name not in result.data

    call(["hive", "-e", "CREATE DATABASE %s" % db_name])

    # Run 'invalidate metadata <table name>' when the parent database does not exist.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show databases")
    assert db_name not in result.data

    # Create a table external to Impala.
    call(["hive", "-e", "CREATE TABLE %s.%s (i int)" % (db_name, tbl_name)])

    # Impala does not know about this database or table.
    result = self.client.execute("show databases")
    assert db_name not in result.data

    # Run 'invalidate metadata <table name>'. It should add the database and table
    # in to Impala's catalog.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show databases")
    assert db_name in result.data

    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name in result.data
    assert len(result.data) == 1

    # Make sure we can actually use the table
    self.client.execute(("insert overwrite table %s.%s "
                        "select 1 from functional.alltypes limit 5"
                         % (db_name, tbl_name)))
    result = self.execute_scalar("select count(*) from %s.%s" % (db_name, tbl_name))
    assert int(result) == 5

    # Should be able to call invalidate metadata multiple times on the same table.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    self.client.execute("refresh %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name in result.data

    # Can still use the table.
    result = self.execute_scalar("select count(*) from %s.%s" % (db_name, tbl_name))
    assert int(result) == 5

    # Run 'invalidate metadata <table name>' when no table exists with that name.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
    result = self.client.execute("show tables in %s" % db_name);
    assert len(result.data) == 1
    assert tbl_name in result.data

    # Create another table
    call(["hive", "-e", "CREATE TABLE %s.%s (i int)" % (db_name, tbl_name + '2')])
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name in result.data

    # Drop the table, and then verify invalidate metadata <table name> removes the
    # table from the catalog.
    call(["hive", "-e", "DROP TABLE %s.%s " % (db_name, tbl_name)])
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name not in result.data

    # Should be able to call invalidate multiple times on the same table when the table
    # does not exist.
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name))
    result = self.client.execute("show tables in %s" % db_name)
    assert tbl_name + '2' in result.data
    assert tbl_name not in result.data

    # Drop the parent database (this will drop all tables). Then invalidate the table
    call(["hive", "-e", "DROP DATABASE %s CASCADE" % db_name])
    self.client.execute("invalidate metadata %s.%s"  % (db_name, tbl_name + '2'))
    result = self.client.execute("show tables in %s" % db_name);
    assert len(result.data) == 0

    # Requires a refresh to see the dropped database
    result = self.client.execute("show databases");
    assert db_name in result.data

    self.client.execute("invalidate metadata")
    result = self.client.execute("show databases");
    assert db_name not in result.data
