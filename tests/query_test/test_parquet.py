#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

import pytest
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

# Tests specific to parquet.
class TestParquetManyColumns(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetManyColumns, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'parquet')
    if cls.exploration_strategy() == 'core':
      # Don't run on core. This test is very slow (IMPALA-864) and we are unlikely
      # to regress here.
      cls.TestMatrix.add_constraint(lambda v: False);

  def test_many_columns(self, vector):
    NUM_COLS = 2000
    TABLE_NAME = "functional_parquet.parquet_many_cols"
    self.client.execute("drop table if exists " + TABLE_NAME)

    col_descs = ["col" + str(i) + " int" for i in range(NUM_COLS)]
    create_stmt = "CREATE TABLE " + TABLE_NAME +\
        "(" + ', '.join(col_descs) + ") stored as parquet"
    col_vals = [str(i) for i in range(NUM_COLS)]
    insert_stmt = "INSERT INTO " + TABLE_NAME + " VALUES(" + ", ".join(col_vals) + ")"
    expected_result = "\t".join(col_vals)

    self.client.execute(create_stmt)
    self.client.execute(insert_stmt)

    result = self.client.execute("select count(*) from " + TABLE_NAME)
    assert result.data == ["1"]

    result = self.client.execute("select * from " + TABLE_NAME)
    assert result.data == [expected_result]
