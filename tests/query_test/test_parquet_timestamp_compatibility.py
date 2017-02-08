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
#
import pytest
import time

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import WAREHOUSE, get_fs_path

class TestParquetTimestampCompatibility(ImpalaTestSuite):
  '''Hive adjusts timestamps by subtracting the local time zone's offset from all values
     when writing data to Parquet files. As a result of this adjustment, Impala may read
     "incorrect" timestamp values from Parquet files written by Hive. To fix the problem
     a table property ('parquet.mr.int96.write.zone') was introduced in IMPALA-2716 that
     specifies the time zone to convert the timesamp values to.

     This file tests the following scenarios:
     1. If the 'parquet.mr.int96.write.zone' table property is set to an invalid time zone
        (by Hive), Impala throws an error when analyzing a query against the table.
     2. If the 'parquet.mr.int96.write.zone' table property is set to a valid time zone:
        a. Impala adjusts timestamp values read from Parquet files created by Hive using
           the time zone from the table property.
        b. Impala does not adjust timestamp values read from Parquet files created by
           Impala.
  '''

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetTimestampCompatibility, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def _setup_env(self, hive_tbl_name, impala_tbl_name=None):
    parquet_loc = get_fs_path('/test-warehouse/alltypesagg_hive_13_1_parquet')
    parquet_fn = get_fs_path(
        '/test-warehouse/alltypesagg_hive_13_1_parquet/alltypesagg_hive_13_1.parquet')
    self.client.execute('''CREATE EXTERNAL TABLE {0}
        LIKE PARQUET "{1}"
        STORED AS PARQUET LOCATION "{2}"
        '''.format(hive_tbl_name, parquet_fn, parquet_loc))
    if impala_tbl_name:
      self.client.execute('''CREATE TABLE {0}
          STORED AS PARQUET AS
          SELECT * FROM {1}
          '''.format(impala_tbl_name, hive_tbl_name))

  def _set_tbl_timezone(self, tbl_name, tz_name):
    self.client.execute('''ALTER TABLE {0}
        SET TBLPROPERTIES ('parquet.mr.int96.write.zone'='{1}')
        '''.format(tbl_name, tz_name))

  def _get_parquet_mr_write_zone_tbl_prop(self, tbl_name):
    tbl_prop = self.get_table_properties(tbl_name)
    if 'parquet.mr.int96.write.zone' not in tbl_prop:
      return None
    return tbl_prop['parquet.mr.int96.write.zone']

  def test_invalid_parquet_mr_write_zone(self, vector, unique_database):
    # Hive doesn't allow setting 'parquet.mr.int96.write.zone' table property to an
    # invalid time zone anymore.
    pytest.skip()

    hive_tbl_name = '%s.hive_table' % unique_database
    self._setup_env(hive_tbl_name)
    # Hive sets the table property to an invalid time zone
    self.run_stmt_in_hive('''ALTER TABLE {0}
        SET TBLPROPERTIES ('parquet.mr.int96.write.zone'='garbage')
        '''.format(hive_tbl_name))
    self.client.execute('REFRESH %s' % hive_tbl_name)
    # Impala throws an error when the table is queried
    try:
      self.client.execute('SELECT timestamp_col FROM %s' % hive_tbl_name)
    except ImpalaBeeswaxException, e:
      if "Invalid time zone" not in str(e):
        raise e
    else:
      assert False, "Query was expected to fail"

  def test_parquet_timestamp_conversion(self, vector, unique_database):
    hive_tbl_name = '%s.hive_table' % unique_database
    impala_tbl_name = '%s.impala_table' % unique_database
    self._setup_env(hive_tbl_name, impala_tbl_name)
    for tz_name in ['UTC', 'EST', 'China Standard Time', 'CET']:
      # impala_table's underlying Parquet file was written by Impala. No conversion is
      # performed on the timestamp values, no matter what value
      # 'parquet.mr.int96.write.zone' is set to.
      self._set_tbl_timezone(impala_tbl_name, tz_name)
      data = self.execute_query_expect_success(self.client, """
          SELECT i2.id, i2.day, i2.timestamp_col, i1.timestamp_col
          FROM functional.alltypesagg i1
          JOIN {0} i2
            ON i1.id = i2.id AND i1.day = i2.day  -- serves as a unique key
          WHERE
            (i1.timestamp_col IS NULL) != (i2.timestamp_col IS NULL)
            OR i1.timestamp_col != i2.timestamp_col
          """.format(impala_tbl_name))\
          .get_data()
      assert len(data) == 0
      assert self._get_parquet_mr_write_zone_tbl_prop(impala_tbl_name) == tz_name
      # hive_table's underlying Parquet file was written by Hive. Setting the
      # 'parquet.mr.int96.write.zone' table property to tz_name triggers a 'UTC' ->
      # tz_name conversion on the timestamp values.
      self._set_tbl_timezone(hive_tbl_name, tz_name)
      data = self.execute_query_expect_success(self.client, """
          SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
          FROM functional.alltypesagg i
          JOIN {0} h
            ON i.id = h.id AND i.day = h.day  -- serves as a unique key
          WHERE
            (h.timestamp_col IS NULL) != (i.timestamp_col IS NULL)
            OR h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '{1}')
          """.format(hive_tbl_name, tz_name))\
          .get_data()
      assert len(data) == 0
      assert self._get_parquet_mr_write_zone_tbl_prop(hive_tbl_name) == tz_name
