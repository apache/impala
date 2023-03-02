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

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.util.event_processor_utils import EventProcessorUtils
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal
from tests.util.filesystem_utils import get_fs_path

PARQUET_CODECS = ['none', 'snappy', 'gzip', 'zstd', 'zstd:7', 'lz4']


class TestParquetInterop(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestParquetInterop, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-convert_legacy_hive_parquet_utc_timestamps=true "
      "-hdfs_zone_info_zip=%s" % get_fs_path("/test-warehouse/tzdb/2017c.zip"))
  def test_hive_impala_interop(self, vector, unique_database, cluster_properties):
    # Setup source table.
    source_table = "{0}.{1}".format(unique_database, "t1_source")
    self.execute_query_expect_success(self.client,
        "create table {0} as select * from functional_parquet.alltypes"
        .format(source_table))
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
      # For Hive 3+, workaround for HIVE-22371 (CTAS puts files in the wrong place) by
      # explicitly creating an external table so that files are in the external warehouse
      # directory. Use external.table.purge=true so that it is equivalent to a Hive 2
      # managed table. Hive 2 stays the same.
      external = ""
      tblproperties = ""
      if HIVE_MAJOR_VERSION >= 3:
        external = "external"
        tblproperties = "TBLPROPERTIES('external.table.purge'='TRUE')"
      self.run_stmt_in_hive("set parquet.compression={0};\
          create {1} table {2} stored as parquet {3} as select * from {4}"
          .format(codec, external, hive_table, tblproperties, impala_table))

      # Make sure Impala's metadata is in sync.
      if cluster_properties.is_event_polling_enabled():
        assert EventProcessorUtils.get_event_processor_status() == "ACTIVE"
        EventProcessorUtils.wait_for_event_processing(self)
        self.confirm_table_exists(unique_database, "t1_hive")
      else:
        self.client.execute("invalidate metadata {0}".format(hive_table))

      # Read Hive data in Impala and verify results.
      base_result = self.execute_query_expect_success(self.client,
          "select * from {0} order by id".format(source_table))
      test_result = self.execute_query_expect_success(self.client,
          "select * from {0} order by id".format(hive_table))
      verify_query_result_is_equal(test_result.data, base_result.data)
