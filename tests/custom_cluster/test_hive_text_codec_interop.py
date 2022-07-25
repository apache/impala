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
# Tests for Hive-IMPALA text compression codec interoperability

import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal

# compression codecs impala support reading in text file type
TEXT_CODECS = ['snappy', 'gzip', 'zstd', 'bzip2', 'deflate', 'default']


class TestTextInterop(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestTextInterop, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0],
        sync_ddl=[1]))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'textfile')

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  def test_hive_impala_interop(self, unique_database, cluster_properties):
    """Tests compressed text file written by Hive with different codecs
    can be read from impala. And verify results."""
    # Setup source table.
    source_table = "{0}.{1}".format(unique_database, "t1_source")
    # TODO: Once IMPALA-8721 is fixed add coverage for TimeStamp data type.
    self.execute_query_expect_success(self.client,
        "create table {0} stored as textfile as select id, bool_col, tinyint_col, "
        "smallint_col, int_col, bigint_col, float_col, double_col, date_string_col,"
        "string_col, year, month from functional_parquet.alltypes".format(source_table))
    self.execute_query_expect_success(self.client,
        "insert into {0}(id) values (7777), (8888), (9999), (11111), (22222), (33333)"
        .format(source_table))

    # For Hive 3+, workaround for HIVE-22371 (CTAS puts files in the wrong place) by
    # explicitly creating an external table so that files are in the external warehouse
    # directory. Use external.table.purge=true so that it is equivalent to a Hive 2
    # managed table. Hive 2 stays the same.
    external = ""
    tblproperties = ""
    if HIVE_MAJOR_VERSION >= 3:
      external = "external"
      tblproperties = "TBLPROPERTIES('external.table.purge'='TRUE')"
    # Loop through the compression codecs and run interop tests.
    for codec in TEXT_CODECS:
      # Write data in Hive and read from Impala
      # switch codec to format hive can accept
      switcher = {
          'snappy': 'org.apache.hadoop.io.compress.SnappyCodec',
          'gzip': 'org.apache.hadoop.io.compress.GzipCodec',
          'zstd': 'org.apache.hadoop.io.compress.ZStandardCodec',
          'bzip2': 'org.apache.hadoop.io.compress.BZip2Codec',
          'deflate': 'org.apache.hadoop.io.compress.DeflateCodec',
          'default': 'org.apache.hadoop.io.compress.DefaultCodec'
      }
      hive_table = "{0}.{1}".format(unique_database, "t1_hive")
      self.run_stmt_in_hive("drop table if exists {0}".format(hive_table))
      self.run_stmt_in_hive("set hive.exec.compress.output=true;\
          set mapreduce.output.fileoutputformat.compress.codec={0};\
          create {1} table {2} stored as textfile {3} as select * from {4}"
          .format(switcher.get(codec, 'Invalid codec'), external, hive_table,
          tblproperties, source_table))

      # Make sure hive CTAS table is not empty
      assert self.run_stmt_in_hive("select count(*) from {0}".format(
          hive_table)).split("\n")[1] != "0", "CTAS created Hive table is empty."

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
