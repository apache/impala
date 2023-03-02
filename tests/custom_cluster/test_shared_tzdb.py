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
# Tests for IMPALA-3307

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.filesystem_utils import get_fs_path

class TestSharedTimezoneDatabase(CustomClusterTestSuite):
  '''IMPALA-3307 adds two startup flags to impalad:
     1. --hdfs_zone_info_zip to specify an HDFS/S3/ADLS path to a zip file that contains
     the shared compiled time-zone database to use instead of the default
     /usr/share/zoneinfo.
     2. --hdfs_zone_alias_conf to specify an HDFS/S3/ADLS path to a shared config file
     that contains definitions for non-standard time-zone aliases.

     This file tests that the startup flags behave as expected.
  '''

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    # Timestamp conversion doesn't depend on file format and compression.
    # Cut down on testing time by limiting the file format and compression.
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'text' and
        v.get_value('table_format').compression_codec == 'none')


  @CustomClusterTestSuite.with_args("-hdfs_zone_info_zip=%s" %
      get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_shared_timezones(self, vector):
    result = self.client.execute("select timezone, utctime, localtime, \
        from_utc_timestamp(utctime,timezone) as impalaresult from \
        functional.alltimezones where localtime != from_utc_timestamp(utctime,timezone)")
    assert(len(result.data) == 0)


  @CustomClusterTestSuite.with_args("-hdfs_zone_info_zip=%s" %
      get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_invalid_aliases(self, vector):
    """ Test that conversions from/to invalid timezones return the timestamp
        without change and issue a warning.

        IMPALA-7060/IMPALA-3307 removed many abbreviations, aliases and some timezones,
        because these timezone names are not supported by Hive and Spark.
    """
    timestamp = '2018-04-19 13:07:48.891829000'
    for function in ['from_utc_timestamp', 'to_utc_timestamp']:
      for timezone in ['invalid timezone', 'CEST', 'Mideast/Riyadh87']:
        result = self.execute_query_expect_success(self.client,
            "select {0}('{1}', '{2}');".format(function, timestamp, timezone))
        assert "UDF WARNING: Unknown timezone '%s'" % timezone == result.log.strip()
        assert timestamp == result.get_data()


  @CustomClusterTestSuite.with_args("-hdfs_zone_info_zip=%s -hdfs_zone_alias_conf=%s" %
      (get_fs_path("/test-warehouse/tzdb/2017c.zip"),
      get_fs_path("/test-warehouse/tzdb/alias.conf")))
  def test_shared_timezone_aliases(self, vector):
    ts_utc = '2018-04-19 13:07:48.891829000'
    ts_utc_plus5400sec = '2018-04-19 14:37:48.891829000'
    ts_utc_min5400sec = '2018-04-19 11:37:48.891829000'

    # Test tz abbreviations that are not listed in the custom aliases config file.
    # Impala should issue a warning and fall back to UTC.
    for abbrev in ['PST', 'JST', 'ACT', 'VST']:
      result = self.execute_query_expect_success(self.client,
          "select from_utc_timestamp('{0}', '{1}');".format(ts_utc, abbrev))
      assert "UDF WARNING: Unknown timezone '%s'" % abbrev == result.log.strip()
      assert ts_utc == result.get_data()

    # Test custom tz aliases defined as aliases for time-zones.
    data = self.execute_query_expect_success(self.client, """
        select from_utc_timestamp('{0}', 'America/Cancun'),
               from_utc_timestamp('{0}', 'Cancun'),
               from_utc_timestamp('{0}', 'America/Argentina/Mendoza'),
               from_utc_timestamp('{0}', 'Mendoza'),
               from_utc_timestamp('{0}', 'Mendoza2'),
               from_utc_timestamp('{0}', 'Utc'),
               from_utc_timestamp('{0}', 'Etc/GMT+1'),
               from_utc_timestamp('{0}', 'GMT-01:00'),
               from_utc_timestamp('{0}', 'Asia/Riyadh'),
               from_utc_timestamp('{0}', 'Mideast/Riyadh89');
        """.format(ts_utc)).get_data()
    ts_cancun, ts_cancun_alias,\
        ts_mendoza, ts_mendoza_alias, ts_mendoza_alias2,\
        ts_utc_alias,\
        ts_gmt_plus_1hour, ts_gmt_plus_1hour_alias,\
        ts_riyadh, ts_riyadh_alias = data.split('\t')

    assert ts_cancun == ts_cancun_alias
    assert ts_mendoza == ts_mendoza_alias == ts_mendoza_alias2
    assert ts_utc == ts_utc_alias
    assert ts_gmt_plus_1hour == ts_gmt_plus_1hour_alias
    assert ts_riyadh == ts_riyadh_alias

    # Test custom tz reviations defined as UTC offsets.
    data = self.execute_query_expect_success(self.client, """
        select from_utc_timestamp('{0}', 'Fifty-Four-Hundred'),
               from_utc_timestamp('{0}', 'Min-Fifty-Four-Hundred');
        """.format(ts_utc)).get_data()
    assert [ts_utc_plus5400sec, ts_utc_min5400sec] == data.split('\t')
