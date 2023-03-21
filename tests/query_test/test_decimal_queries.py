# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Targeted tests for decimal type.

from __future__ import absolute_import, division, print_function
from copy import copy
import pytest

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_exec_option_dimension_from_dict,
    create_client_protocol_dimension, hs2_parquet_constraint)
from tests.common.test_vector import ImpalaTestDimension
from tests.util.filesystem_utils import IS_S3

class TestDecimalQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDecimalQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
      create_exec_option_dimension_from_dict({
        'decimal_v2' : ['false', 'true'],
        'batch_size' : [0, 1],
        'disable_codegen' : ['false', 'true'],
        'disable_codegen_rows_threshold' : [0]}))
    # Hive < 0.11 does not support decimal so we can't run these tests against the other
    # file formats.
    # TODO: Enable them on Hive >= 0.11.
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        (v.get_value('table_format').file_format == 'text' and
         v.get_value('table_format').compression_codec == 'none') or
         v.get_value('table_format').file_format in ['parquet', 'orc', 'kudu', 'json'])

    # Run these queries through both beeswax and HS2 to get coverage of decimals returned
    # via both protocols.
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())
    cls.ImpalaTestMatrix.add_constraint(hs2_parquet_constraint)

  def test_queries(self, vector):
    self.run_test_case('QueryTest/decimal', vector)

# Tests involving DECIMAL typed expressions. The results depend on whether DECIMAL
# version 1 or version 2 are enabled, so the .test file itself toggles the DECIMAL_V2
# query option.
class TestDecimalExprs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDecimalExprs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format in ['parquet', 'kudu']))

  def test_exprs(self, vector):
    self.run_test_case('QueryTest/decimal-exprs', vector)

# TODO: when we have a good way to produce Avro decimal data (e.g. upgrade Hive), we can
# run Avro through the same tests as above instead of using avro_decimal_tbl.
class TestAvroDecimalQueries(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAvroDecimalQueries, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format == 'avro' and
         v.get_value('table_format').compression_codec == 'snap'))

  def test_avro_queries(self, vector):
    self.run_test_case('QueryTest/decimal_avro', vector)


# Tests involving DECIMAL typed expressions with data overflow. The results depend on
# whether DECIMAL version 2 is enabled, so the .test file itself toggles the DECIMAL_V2
# query option.
@pytest.mark.execute_serially
class TestDecimalOverflowExprs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDecimalOverflowExprs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        (v.get_value('table_format').file_format in ['kudu', 'parquet', 'text']))

  def test_insert_select_exprs(self, vector, unique_database):
    TBL_NAME_1 = '`{0}`.`overflowed_decimal_tbl_1`'.format(unique_database)
    TBL_NAME_2 = '`{0}`.`overflowed_decimal_tbl_2`'.format(unique_database)
    # Create table with decimal data type of column.
    if 'parquet' in str(vector.get_value('table_format')):
      stmt = "CREATE TABLE {0} (i int, d_28 decimal(28,10)) STORED AS PARQUET"
    elif 'kudu' in str(vector.get_value('table_format')):
      stmt = "CREATE TABLE {0} (i int primary key, d_28 decimal(28,10)) STORED AS KUDU"
    else:
      stmt = "CREATE TABLE {0} (i int, d_28 decimal(28,10))"
    query_1 = stmt.format(TBL_NAME_1)
    query_2 = stmt.format(TBL_NAME_2)

    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME_1)
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME_2)
    self.execute_query_expect_success(self.client, query_1)
    self.execute_query_expect_success(self.client, query_2)

    # Run INSERT-SELECT queries.
    self.run_test_case('QueryTest/decimal-insert-overflow-exprs', vector,
        use_db=unique_database)

  def test_ctas_exprs(self, vector, unique_database):
    TBL_NAME_1 = '`{0}`.`overflowed_decimal_tbl_1`'.format(unique_database)
    TBL_NAME_2 = '`{0}`.`overflowed_decimal_tbl_2`'.format(unique_database)
    TBL_NAME_3 = '`{0}`.`overflowed_decimal_tbl_3`'.format(unique_database)
    if 'parquet' in str(vector.get_value('table_format')):
      stmt_1 = "CREATE TABLE {0} STORED AS PARQUET " \
          "AS SELECT 1 as i, cast(a*a*a as decimal (28,10)) as d_28 FROM " \
          "(SELECT cast(654964569154.9565 as decimal (28,7)) as a) q"
      stmt_2 = "CREATE TABLE {0} STORED AS PARQUET " \
          "AS SELECT i, cast(d_28*d_28*d_28 as decimal (28,10)) as d_28 FROM {1} " \
          "WHERE d_28 is not null"
      stmt_3 = "CREATE TABLE {0} (i int, d_28 decimal(28,10)) STORED AS PARQUET"
    elif 'kudu' in str(vector.get_value('table_format')):
      stmt_1 = "CREATE TABLE {0} PRIMARY KEY (i) STORED AS KUDU " \
          "AS SELECT 1 as i, cast(a*a*a as decimal (28,10)) as d_28 FROM " \
          "(SELECT cast(654964569154.9565 as decimal (28,7)) as a) q"
      stmt_2 = "CREATE TABLE {0} PRIMARY KEY (i) STORED AS KUDU " \
          "AS SELECT i, cast(d_28*d_28*d_28 as decimal (28,10)) as d_28 FROM {1} " \
          "WHERE d_28 is not null"
      stmt_3 = "CREATE TABLE {0} (i int primary key, d_28 decimal(28,10)) STORED AS KUDU"
    else:
      stmt_1 = "CREATE TABLE {0} " \
          "AS SELECT 1 as i, cast(a*a*a as decimal (28,10)) as d_28 FROM " \
          "(SELECT cast(654964569154.9565 as decimal (28,7)) as a) q"
      stmt_2 = "CREATE TABLE {0} " \
          "AS SELECT i, cast(d_28*d_28*d_28 as decimal (28,10)) as d_28 FROM {1} " \
          "WHERE d_28 is not null"
      stmt_3 = "CREATE TABLE {0} (i int, d_28 decimal(28,10))"
    query_1 = stmt_1.format(TBL_NAME_1)
    # CTAS with selection from another table.
    query_2 = stmt_2.format(TBL_NAME_2, TBL_NAME_3)
    query_3 = stmt_3.format(TBL_NAME_3)

    self.execute_query_expect_success(self.client, "SET decimal_v2=true")
    # Verify the table on s3a could be accessed after CTAS is finished with error and
    # NULL is not inserted into table if s3_skip_insert_staging is set as false.
    if IS_S3:
      self.execute_query_expect_success(self.client, "SET s3_skip_insert_staging=false")
      self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s"
          % TBL_NAME_1)
      try:
        self.execute_query_using_client(self.client, query_1, vector)
        assert False, "Query was expected to fail"
      except ImpalaBeeswaxException as e:
        assert "Decimal expression overflowed" in str(e)

      result = self.execute_query_expect_success(self.client,
          "SELECT count(*) FROM %s WHERE d_28 is null" % TBL_NAME_1)
      assert int(result.get_data()) == 0
      # Set s3_skip_insert_staging as default value.
      self.execute_query_expect_success(self.client, "SET s3_skip_insert_staging=true")

    # Verify query_1 is aborted with error message "Decimal expression overflowed" and
    # NULL is not inserted into table.
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME_1)
    try:
      self.execute_query_using_client(self.client, query_1, vector)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Decimal expression overflowed" in str(e)

    result = self.execute_query_expect_success(self.client,
        "SELECT count(*) FROM %s WHERE d_28 is null" % TBL_NAME_1)
    assert int(result.get_data()) == 0

    # Verify that valid data could be inserted into the new table which is created by
    # CTAS and the CTAS finished with an error.
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s VALUES(100, cast(654964569154.9565 as decimal (28,10)))" %
        TBL_NAME_1)
    result = self.execute_query_expect_success(self.client,
        "SELECT count(*) FROM %s WHERE d_28 is not null" % TBL_NAME_1)
    assert int(result.get_data()) == 1

    # Create table 3 and insert data to table 3.
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME_3)
    self.execute_query_expect_success(self.client, query_3)
    self.execute_query_expect_success(self.client,
        "INSERT INTO TABLE %s VALUES(100, cast(654964569154.9565 as decimal (28,10)))" %
        TBL_NAME_3)
    # Query_2 is aborted with error message "Decimal expression overflowed" and NULL is
    # not inserted into table.
    self.execute_query_expect_success(self.client, "DROP TABLE IF EXISTS %s" % TBL_NAME_2)
    try:
      self.execute_query_using_client(self.client, query_2, vector)
      assert False, "Query was expected to fail"
    except ImpalaBeeswaxException as e:
      assert "Decimal expression overflowed" in str(e)

    result = self.execute_query_expect_success(self.client,
        "SELECT count(*) FROM %s" % TBL_NAME_2)
    assert int(result.get_data()) == 0
