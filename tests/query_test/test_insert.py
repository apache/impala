#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# Targeted Impala insert tests
#
import logging
import pytest
from testdata.common import widetable
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_exec_option_dimension

# TODO: Add Gzip back.  IMPALA-424
PARQUET_CODECS = ['none', 'snappy']

class TestInsertQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertQueries, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value. This is needed should we decide
    # to run the insert tests in parallel (otherwise there will be two tests inserting
    # into the same table at the same time for the same file format).
    # TODO: When we do decide to run these tests in parallel we could create unique temp
    # tables for each test case to resolve the concurrency problems.
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[0, 1]))

    cls.TestMatrix.add_dimension(TestDimension("compression_codec", *PARQUET_CODECS));

    # Insert is currently only supported for text and parquet
    # For parquet, we want to iterate through all the compression codecs
    # TODO: each column in parquet can have a different codec.  We could
    # test all the codecs in one table/file with some additional flags.
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' or \
          (v.get_value('table_format').file_format == 'text' and \
           v.get_value('compression_codec') == 'none'))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def setup_class(cls):
    super(TestInsertQueries, cls).setup_class()

  @pytest.mark.execute_serially
  def test_insert(self, vector):
    vector.get_value('exec_option')['PARQUET_COMPRESSION_CODEC'] = \
        vector.get_value('compression_codec')
    self.run_test_case('QueryTest/insert', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

  @pytest.mark.execute_serially
  def test_insert_overwrite(self, vector):
    self.run_test_case('QueryTest/insert_overwrite', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)

class TestInsertWideTable(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertWideTable, cls).add_test_dimensions()

    # Only vary codegen
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[True, False], batch_sizes=[0]))

    # Inserts only supported on text and parquet
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet' or \
        v.get_value('table_format').file_format == 'text')
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

    # Don't run on core. This test is very slow (IMPALA-864) and we are unlikely to
    # regress here.
    if cls.exploration_strategy() == 'core':
      cls.TestMatrix.add_constraint(lambda v: False);

  def test_insert_wide_table(self, vector):
    table_format = vector.get_value('table_format')

    # Text can't handle as many columns as Parquet (codegen takes forever)
    num_cols = 1000 if table_format.file_format == 'text' else 2000

    db_name = QueryTestSectionReader.get_db_name(vector.get_value('table_format'))
    table_name = db_name + ".insert_widetable"
    if vector.get_value('exec_option')['disable_codegen']:
      table_name += "_codegen_disabled"
    self.client.execute("drop table if exists " + table_name)

    col_descs = widetable.get_columns(num_cols)
    create_stmt = "CREATE TABLE " + table_name + "(" + ','.join(col_descs) + ")"
    if vector.get_value('table_format').file_format == 'parquet':
      create_stmt += " stored as parquet"
    self.client.execute(create_stmt)

    # Get a single row of data
    col_vals = widetable.get_data(num_cols, 1, quote_strings=True)[0]
    insert_stmt = "INSERT INTO " + table_name + " VALUES(" + col_vals + ")"
    self.client.execute(insert_stmt)

    result = self.client.execute("select count(*) from " + table_name)
    assert result.data == ["1"]

    result = self.client.execute("select * from " + table_name)
    types = parse_column_types(result.schema)
    labels = parse_column_labels(result.schema)
    expected = QueryTestResult([col_vals], types, labels, order_matters=False)
    actual = QueryTestResult(parse_result_rows(result), types, labels, order_matters=False)
    assert expected == actual

class TestUnsupportedInsertFormats(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestUnsupportedInsertFormats, cls).add_test_dimensions()
    # Only run on file formats we can't write to
    cls.TestMatrix.add_constraint(lambda v: \
        not (v.get_value('table_format').file_format == 'parquet' or
             v.get_value('table_format').file_format == 'hbase' or
             (v.get_value('table_format').file_format == 'text' and
              v.get_value('table_format').compression_codec == 'none')))

  def test_unsupported_formats(self, vector):
    try:
      self.execute_query_using_client(
        self.client, 'insert into table tinytable values("hi", "there")', vector)
      assert False, 'Query was expected to fail'
    except ImpalaBeeswaxException, e: pass

class TestInsertPartKey(ImpalaTestSuite):
  """Regression test for IMPALA-875"""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestInsertPartKey, cls).add_test_dimensions()
    # Only run for a single table type
    cls.TestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))

    cls.TestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'text'))
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').compression_codec == 'none')

  @pytest.mark.execute_serially
  def test_insert_part_key(self, vector):
    """Test that partition column exprs are cast to the correct type. See IMPALA-875."""
    self.run_test_case('QueryTest/insert_part_key', vector,
        multiple_impalad=vector.get_value('exec_option')['sync_ddl'] == 1)
