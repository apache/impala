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
# Tests for Hive-IMPALA parquet compression codec interoperability

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfS3
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal
from tests.util.filesystem_utils import get_fs_path

PARQUET_CODECS = ['none', 'snappy', 'gzip', 'zstd', 'zstd:7', 'lz4']


class TestParquetInterop(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  @SkipIfS3.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true "
      "-hdfs_zone_info_zip=%s" % get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_hive_impala_interop(self, vector, unique_database, cluster_properties):
    # Setup source table.
    source_table = "{0}.{1}".format(unique_database, "t1_source")
    # TODO: Once IMPALA-8721 is fixed add coverage for TimeStamp data type.
    self.execute_query_expect_success(self.client,
        "create table {0} as select id, bool_col, tinyint_col, smallint_col, int_col, "
        "bigint_col, float_col, double_col, date_string_col, string_col, year, month "
        "from functional_parquet.alltypes".format(source_table))
    self.execute_query_expect_success(self.client,
        "insert into {0}(id) values (7777), (8888), (9999), (11111), (22222), (33333)"
        .format(source_table))

    # Loop through the compression codecs and run interop tests.
    for codec in PARQUET_CODECS:
      # Write data in Impala.
      vector.get_value('exec_option')['compression_codec'] = codec
      impala_table = "{0}.{1}".format(unique_database, "t1_impala")
      self.execute_query_expect_success(self.client,
          "drop table if exists {0}".format(impala_table))
      self.execute_query_expect_success(self.client,
          "create table {0} stored as parquet as select * from {1}"
          .format(impala_table, source_table), vector.get_value('exec_option'))

      # Read data from Impala and write in Hive
      if (codec == 'none'): codec = 'uncompressed'
      elif (codec == 'zstd:7'): codec = 'zstd'
      hive_table = "{0}.{1}".format(unique_database, "t1_hive")
      self.run_stmt_in_hive("drop table if exists {0}".format(hive_table))
      self.run_stmt_in_hive("set parquet.compression={0};\
          create table {1} stored as parquet as select * from {2}"
          .format(codec, hive_table, impala_table))

      # Make sure Impala's metadata is in sync.
      if cluster_properties.is_catalog_v2_cluster():
        self.wait_for_table_to_appear(unique_database, hive_table, timeout_s=10)
      else:
        self.client.execute("invalidate metadata {0}".format(hive_table))

      # Read Hive data in Impala and verify results.
      base_result = self.execute_query_expect_success(self.client,
          "select * from {0} order by id".format(source_table))
      test_result = self.execute_query_expect_success(self.client,
          "select * from {0} order by id".format(hive_table))
      verify_query_result_is_equal(test_result.data, base_result.data)
