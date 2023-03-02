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

from __future__ import absolute_import, division, print_function
import os
import pytest
from subprocess import check_call

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.file_utils import create_table_from_parquet
from tests.util.filesystem_utils import get_fs_path

class TestHiveParquetTimestampConversion(CustomClusterTestSuite):
  '''Hive writes timestamps in parquet files by first converting values from local time
     to UTC. The conversion was not expected (other file formats don't convert) and a
     startup flag was later added to adjust for this (IMPALA-1658). This file tests that
     the conversion and flag behave as expected.
  '''

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def check_sanity(self, expect_converted_result):
    data = self.execute_query_expect_success(self.client, """
        SELECT COUNT(timestamp_col), COUNT(DISTINCT timestamp_col),
               MIN(timestamp_col), MAX(timestamp_col)
        FROM functional_parquet.alltypesagg_hive_13_1""",
        query_options={"timezone": "PST8PDT"})\
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

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true "
      "-hdfs_zone_info_zip=%s" % get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_conversion(self, vector, unique_database):
    self.check_sanity(True)
    self._test_conversion_with_validation(vector, unique_database)
    # Override query option convert_legacy_hive_parquet_utc_timestamps.
    query_options = {"timezone": "PST8PDT",
        "convert_legacy_hive_parquet_utc_timestamps": "0"}
    self._test_no_conversion(vector, query_options, "PST8PDT")

    # Test with UTC too to check the optimizations added in IMPALA-9385.
    for tz_name in ["PST8PDT", "UTC"]:
      # The value read from the Hive table should be the same as reading a UTC converted
      # value from the Impala table.
      data = self.execute_query_expect_success(self.client, """
          SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
          FROM functional_parquet.alltypesagg_hive_13_1 h
          JOIN functional_parquet.alltypesagg
            i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
          WHERE
            (h.timestamp_col IS NULL AND i.timestamp_col IS NOT NULL)
            OR (h.timestamp_col IS NOT NULL AND i.timestamp_col IS NULL)
            OR h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
          """ % tz_name, query_options={"timezone": tz_name})\
          .get_data()
      assert len(data) == 0

  def _test_conversion_with_validation(self, vector, unique_database):
    """Test that timestamp validation also works as expected when converting timestamps.
    Runs as part of test_conversion() to avoid restarting the cluster."""
    create_table_from_parquet(self.client, unique_database,
                              "out_of_range_timestamp_hive_211")
    create_table_from_parquet(self.client, unique_database,
                              "out_of_range_timestamp2_hive_211")
    # Allow tests to override abort_or_error
    del vector.get_value('exec_option')['abort_on_error']
    self.run_test_case('QueryTest/out-of-range-timestamp-local-tz-conversion',
         vector, unique_database)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=false "
      "-hdfs_zone_info_zip=%s" % get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_no_conversion(self, vector):
    self.check_sanity(False)
    # Do not override query option convert_legacy_hive_parquet_utc_timestamps.
    query_options = {"timezone": "PST8PDT"}
    self._test_no_conversion(vector, query_options, "PST8PDT")

  def _test_no_conversion(self, vector, query_options, tz_name):
    # Without conversion all the values will be different.

    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg
          i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE h.timestamp_col != FROM_UTC_TIMESTAMP(i.timestamp_col, '%s')
        """ % tz_name, query_options=query_options)\
        .get_data()
    assert len(data.split('\n')) == 10000
    # A value should either stay null or stay not null.
    data = self.execute_query_expect_success(self.client, """
        SELECT h.id, h.day, h.timestamp_col, i.timestamp_col
        FROM functional_parquet.alltypesagg_hive_13_1 h
        JOIN functional_parquet.alltypesagg
          i ON i.id = h.id AND i.day = h.day  -- serves as a unique key
        WHERE
          (h.timestamp_col IS NULL AND i.timestamp_col IS NOT NULL)
          OR (h.timestamp_col IS NOT NULL AND i.timestamp_col IS NULL)
        """, query_options=query_options)\
        .get_data()
    assert len(data) == 0

  def _test_stat_filtering(self, vector, unique_database):
    """ IMPALA-7559: Check that Parquet stat filtering doesn't skip row groups
        incorrectly when timezone conversion is needed.
        Runs as part of test_conversion() to avoid restarting the cluster.
    """
    self.client.execute(
       "create table %s.t (i int, d timestamp) stored as parquet" % unique_database)

    tbl_loc = get_fs_path("/test-warehouse/%s.db/t" % unique_database)
    self.filesystem_client.copy_from_local(os.environ['IMPALA_HOME'] +
        "/testdata/data/hive_single_value_timestamp.parq", tbl_loc)

    # TODO: other tests in this file could also use query option 'timezone' to enable
    #       real data validation
    data = self.execute_query_expect_success(self.client,
        'select * from %s.t' % unique_database,
        query_options={"timezone": "CET"}).get_data()
    assert data == '1\t2018-10-01 02:30:00'

    # This query returned 0 rows before the fix for IMPALA-7559.
    data = self.execute_query_expect_success(self.client,
        'select * from %s.t where d = "2018-10-01 02:30:00"' % unique_database,
        query_options={"timezone": "CET"}).get_data()
    assert data == '1\t2018-10-01 02:30:00'
