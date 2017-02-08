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
# Tests for IMPALA-1658

import pytest
import time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path

class TestHiveParquetTimestampConversion(CustomClusterTestSuite):
  '''Hive writes timestamps in Parquet files by first converting values from local time
     to UTC. The conversion was not expected (other file formats don't convert) and a
     startup flag (-convert_legacy_hive_parquet_utc_timestamps) was later added to adjust
     for this (IMPALA-1658). IMPALA-2716 solves the issue in a more general way by
     introducing a table property ('parquet.mr.int96.write.zone') that specifies the time
     zone to convert the timestamp values to.

     This file tests that the table property and the startup option behave as expected in
     the following scenarios:
     1. If the 'parquet.mr.int96.write.zone' table property is set, Impala ignores the
        -convert_legacy_hive_parquet_utc_timestamps startup option. It reads Parquet
        timestamp data written by Hive and adjusts values using the time zone from the
        table property.
     2. If the 'parquet.mr.int96.write.zone' table property is not set, the
        -convert_legacy_hive_parquet_utc_timestamps startup option is taken into account.
        a. If the startup option is set to true, Impala reads Parquet timestamp data
           created by Hive and adjusts values using the local time zone.
        b. If the startup option is absent or set to false, no adjustment will be applied
           to timestamp values.

     IMPALA-2716 also introduces a startup option
     (-set_parquet_mr_int96_write_zone_to_utc_on_new_tables) that determines if the table
     property will be set on newly created tables. This file tests the basic behavior of the
     startup option:
     1. Tables created with the 'parquet.mr.int96.write.zone' table property explicitly
        set, will keep the value the property is set to.
     2. If -set_parquet_mr_int96_write_zone_to_utc_on_new_tables is set to true, tables
        created using CREATE TABLE, CREATE TABLE AS SELECT and CREATE TABLE LIKE <FILE>
        will set the table property to UTC.
     3. Tables created using CREATE TABLE LIKE <OTHER TABLE> will ignore the value of
        -set_parquet_mr_int96_write_zone_to_utc_on_new_tables and copy the property of
        the table that is copied.
  '''

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def check_sanity(self, expect_converted_result,
      tbl_name='functional_parquet.alltypesagg_hive_13_1'):
    data = self.execute_query_expect_success(self.client, """
        SELECT COUNT(timestamp_col), COUNT(DISTINCT timestamp_col),
               MIN(timestamp_col), MAX(timestamp_col)
        FROM {0}""".format(tbl_name))\
        .get_data()
    assert len(data) > 0
    rows = data.split("\n")
    assert len(rows) == 1
    values = rows[0].split("\t")
    assert len(values) == 4
    assert values[0] == "11000"
    assert values[1] == "10000"
    if expect_converted_result:
      # Doing easy time zone conversion in python seems to require a 3rd party lib,
      # so the only check will be that the value changed in some way.
      assert values[2] != "2010-01-01 00:00:00"
      assert values[3] != "2010-01-10 18:02:05.100000000"
    else:
      assert values[2] == "2010-01-01 00:00:00"
      assert values[3] == "2010-01-10 18:02:05.100000000"

  def get_parquet_mr_write_zone_tbl_prop(self,
      tbl_name='functional_parquet.alltypesagg_hive_13_1'):
    tbl_prop = self.get_table_properties(tbl_name)
    if 'parquet.mr.int96.write.zone' not in tbl_prop:
      return None
    return tbl_prop['parquet.mr.int96.write.zone']

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true")
  def test_conversion_to_tbl_prop_timezone(self, vector, unique_database):
    # Create table with 'parquet.mr.int96.write.zone' property set to China Standard Time.
    # The undelying parquet file has been written by Hive.
    hive_tbl = '%s.hive_tbl' % unique_database
    parquet_loc = get_fs_path('/test-warehouse/alltypesagg_hive_13_1_parquet')
    parquet_path = get_fs_path(
        '/test-warehouse/alltypesagg_hive_13_1_parquet/alltypesagg_hive_13_1.parquet')
    self.client.execute('''CREATE EXTERNAL TABLE {0} LIKE PARQUET "{1}"
        STORED AS PARQUET LOCATION "{2}"
        TBLPROPERTIES ('parquet.mr.int96.write.zone'='China Standard Time')
        '''.format(hive_tbl, parquet_path, parquet_loc))
    # Make sure that the table property has been properly set.
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=hive_tbl) ==\
        'China Standard Time'
    # Even though -convert_legacy_hive_parquet_utc_timestamps is set to true it is ignored
    # because the 'parquet.mr.int06.write.zone' table property is also set. The value read
    # from the Hive table should be the same as the corresponding Impala timestamp value
    # converted from UTC to China Standard Time.
    self.check_sanity(True, tbl_name=hive_tbl)
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM {0} h
        JOIN functional_parquet.alltypesagg i
          ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL) != (i.timestamp_col IS NULL)
          OR h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, 'China Standard Time')
        """.format(hive_tbl))\
        .get_data()
    assert len(data) == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true")
  def test_conversion_to_localtime(self, vector):
    tz_name = time.tzname[time.localtime().tm_isdst]
    self.check_sanity(tz_name not in ("UTC", "GMT"))
    # Make sure that the table property is not set
    assert self.get_parquet_mr_write_zone_tbl_prop() == None
    # The value read from the Hive table should be the same as reading a UTC converted
    # value from the Impala table.
    tz_name = time.tzname[time.localtime().tm_isdst]
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg i
          ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL) != (i.timestamp_col IS NULL)
          OR h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
        """ % tz_name)\
        .get_data()
    assert len(data) == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=false")
  def test_no_conversion(self, vector):
    self.check_sanity(False)
    # Make sure that the table property is not set
    assert self.get_parquet_mr_write_zone_tbl_prop() == None
    # Without conversion all the values will be different.
    tz_name = time.tzname[time.localtime().tm_isdst]
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg i
          ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
        """ % tz_name)\
        .get_data()
    if tz_name in ("UTC", "GMT"):
      assert len(data) == 0
    else:
      assert len(data.split('\n')) == 10000
    # A value should either stay null or stay not null.
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg i
          ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL) != (i.timestamp_col IS NULL)
        """)\
        .get_data()
    assert len(data) == 0

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "-set_parquet_mr_int96_write_zone_to_utc_on_new_tables=true")
  def test_new_table_enable_set_tbl_prop_to_utc(self, unique_database):
    # Table created with CREATE TABLE will set the table property to UTC.
    tbl1_name = '%s.table1' % unique_database
    self.client.execute('CREATE TABLE {0} (id int)'.format(tbl1_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl1_name) == 'UTC'
    # Table created with CREATE TABLE will honor the explicitly set property.
    tbl_est_name = '%s.table_est' % unique_database
    self.client.execute('''CREATE TABLE {0} (id int)
        TBLPROPERTIES ('parquet.mr.int96.write.zone'='EST')
        '''.format(tbl_est_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl_est_name) == 'EST'
    # Table created with CREATE TABLE AS SELECT will set the table property to UTC. Table
    # property is not copied from the other table.
    tbl2_name = '%s.table2' % unique_database
    self.client.execute('CREATE TABLE {0} AS SELECT * FROM {1}'.format(
        tbl2_name, tbl_est_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl2_name) == 'UTC'
    # Table created with CREATE TABLE LIKE <FILE> will set the table property to UTC.
    tbl3_name = '%s.tbl3_name' % unique_database
    parquet_path = get_fs_path(
        '/test-warehouse/alltypesagg_hive_13_1_parquet/alltypesagg_hive_13_1.parquet')
    self.client.execute('CREATE EXTERNAL TABLE {0} LIKE PARQUET "{1}"'.format(
        tbl3_name, parquet_path))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl3_name) == 'UTC'
    # Table created with CREATE TABLE LIKE <OTHER TABLE> will copy the property from the
    # other table.
    tbl4_name = '%s.tbl4_name' % unique_database
    self.client.execute('CREATE TABLE {0} LIKE {1}'.format(tbl4_name, tbl_est_name));
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl4_name) == 'EST'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      "-set_parquet_mr_int96_write_zone_to_utc_on_new_tables=false")
  def test_new_table_disable_set_tbl_prop_to_utc(self, unique_database):
    # Table created with CREATE TABLE will not set the table property.
    tbl1_name = '%s.table1' % unique_database
    self.client.execute('CREATE TABLE {0} (id int)'.format(tbl1_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl1_name) == None
    # Table created with CREATE TABLE will honor the explicitly set property.
    tbl_est_name = '%s.table_est' % unique_database
    self.client.execute('''CREATE TABLE {0} (id int)
        TBLPROPERTIES ('parquet.mr.int96.write.zone'='EST')
        '''.format(tbl_est_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl_est_name) == 'EST'
    # Table created with CREATE TABLE AS SELECT will not set the table property. Table
    # property is not copied from the other table.
    tbl2_name = '%s.table2' % unique_database
    self.client.execute('CREATE TABLE {0} AS SELECT * FROM {1}'.format(
        tbl2_name, tbl_est_name))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl2_name) == None
    # Table created with CREATE TABLE LIKE <FILE> will not set the table property.
    tbl3_name = '%s.tbl3_name' % unique_database
    parquet_path = get_fs_path(
        '/test-warehouse/alltypesagg_hive_13_1_parquet/alltypesagg_hive_13_1.parquet')
    self.client.execute('CREATE EXTERNAL TABLE {0} LIKE PARQUET "{1}"'.format(
        tbl3_name, parquet_path))
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl3_name) == None
    # Table created with CREATE TABLE LIKE <OTHER TABLE> will copy the property from the
    # other table.
    tbl4_name = '%s.tbl4_name' % unique_database
    self.client.execute('CREATE TABLE {0} LIKE {1}'.format(tbl4_name, tbl_est_name));
    assert self.get_parquet_mr_write_zone_tbl_prop(tbl_name=tbl4_name) == 'EST'
