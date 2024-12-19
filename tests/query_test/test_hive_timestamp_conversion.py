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

from __future__ import absolute_import, division, print_function

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.file_utils import create_table_and_copy_files, create_table_from_parquet


class TestHiveParquetTimestampConversion(ImpalaTestSuite):
  """Tests that Impala can read parquet files written by older versions of Hive or with
  Hive legacy conversion enabled. Tests use convert_legacy_hive_parquet_utc_timestamps,
  use_legacy_hive_timestamp_conversion, and timezone to test conversion."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHiveParquetTimestampConversion, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet'
        and v.get_value('table_format').compression_codec == 'none')

  def test_hive_4_legacy(self, vector, unique_database):
    """Test that legacy conversion uses the same timezone conversion as Hive when
    Parquet metadata contains writer.zone.conversion.legacy=true.

    Load test data generated via Hive with TZ=Asia/Kuala_Lumpur:

        create table t (d timestamp) stored as parquet;
        set hive.parquet.timestamp.write.legacy.conversion.enabled=true;
        insert into t values ("1900-01-01 00:00:00"), ("1910-01-01 00:00:00"),
            ("1935-01-01 00:00:00"), ("1940-01-01 00:00:00"), ("1942-01-01 00:00:00"),
            ("1944-01-01 00:00:00"), ("1969-01-29 00:00:00"), ("2000-01-01 00:00:00");
    """
    create_table_from_parquet(self.client, unique_database, "hive_kuala_lumpur_legacy")
    self.run_test_case("QueryTest/timestamp-conversion-hive-4", vector, unique_database)

  def test_hive_313(self, vector, unique_database):
    """The parquet file was written with Hive 3.1.3 using the new Date/Time APIs
    (legacy=false) to convert from US/Pacific to UTC. The presence of writer.time.zone in
    the metadata of the file allow us to infer that new Date/Time APIS should be used for
    the conversion. The use_legacy_hive_timestamp_conversion property shouldn't be taken
    into account in this case.

    Test file from https://github.com/apache/hive/blob/rel/release-4.0.1/data/files/
        employee_hive_3_1_3_us_pacific.parquet"""
    create_table_from_parquet(
        self.client, unique_database, "employee_hive_3_1_3_us_pacific")
    self.run_test_case("QueryTest/timestamp-conversion-hive-313", vector, unique_database)

  def test_hive_3_mixed(self, vector, unique_database):
    """Test table containing Hive legacy timestamps written with Hive prior to 3.1.3.

    Test files target timezone=Asia/Singapore, sourced from
    https://github.com/apache/hive/tree/rel/release-4.0.1/data/files/tbl_parq1."""
    create_stmt = "create table %s.t (d timestamp) stored as parquet" % unique_database
    create_table_and_copy_files(self.client, create_stmt, unique_database, "t",
        ["testdata/data/tbl_parq1/" + f for f in ["000000_0", "000000_1", "000000_2"]])
    self.run_test_case("QueryTest/timestamp-conversion-hive-3m", vector, unique_database)
