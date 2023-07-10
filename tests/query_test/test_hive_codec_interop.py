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
# Tests for Hive-IMPALA compression codec interoperability

from __future__ import absolute_import, division, print_function
import pytest

from tests.common.environ import HIVE_MAJOR_VERSION
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import verify_query_result_is_equal
from tests.common.test_vector import ImpalaTestDimension, ImpalaTestMatrix

# compression codecs impala support reading in text file type. Names use Hive convention.
TEXT_CODECS = ["Snappy", "Gzip", "ZStandard", "BZip2", "Deflate", "Default"]

# compression codecs impala support reading in sequence file type
SEQUENCE_CODECS = list(TEXT_CODECS)
# Omit zstd due to IMPALA-12276.
SEQUENCE_CODECS.remove("ZStandard")


class TestFileCodecInterop(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestFileCodecInterop, cls).setup_class()

  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix = ImpalaTestMatrix()
    # Fix the exec_option vector to have a single value.
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(sync_ddl=[1],
        cluster_sizes=[0], disable_codegen_options=[False], batch_sizes=[0]))

  def verify_codec(self, unique_database, cluster_properties, format, codec):
    # For Hive 3+, workaround for HIVE-22371 (CTAS puts files in the wrong place) by
    # explicitly creating an external table so that files are in the external warehouse
    # directory. Use external.table.purge=true so that it is equivalent to a Hive 2
    # managed table. Hive 2 stays the same.
    external = ""
    tblproperties = ""
    if HIVE_MAJOR_VERSION >= 3:
      external = "external"
      tblproperties = "TBLPROPERTIES('external.table.purge'='TRUE')"

    # Write data in Hive and read from Impala
    source_table = "functional_parquet.alltypes"
    hive_table = "{0}.{1}".format(unique_database, "t1_hive")
    self.run_stmt_in_hive("drop table if exists {0}".format(hive_table))
    self.run_stmt_in_hive("set hive.exec.compress.output=true;\
        set mapreduce.output.fileoutputformat.compress.codec=\
            org.apache.hadoop.io.compress.{0}Codec;\
        create {1} table {2} stored as {3} {4} as select * from {5}"
        .format(codec, external, hive_table, format, tblproperties, source_table))

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


class TestTextInterop(TestFileCodecInterop):
  @classmethod
  def add_test_dimensions(cls):
    super(TestTextInterop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('codec', *TEXT_CODECS))

  @SkipIfFS.hive
  def test_hive_impala_interop(self, vector, unique_database, cluster_properties):
    """Tests compressed text file written by Hive with different codecs
    can be read from impala. And verify results."""
    self.verify_codec(
        unique_database, cluster_properties, 'textfile', vector.get_value('codec'))


class TestSequenceInterop(TestFileCodecInterop):
  @classmethod
  def add_test_dimensions(cls):
    super(TestSequenceInterop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('codec', *SEQUENCE_CODECS))

  @SkipIfFS.hive
  def test_hive_impala_interop(self, vector, unique_database, cluster_properties):
    """Tests compressed sequence file written by Hive with different codecs
    can be read from impala. And verify results."""
    self.verify_codec(
        unique_database, cluster_properties, 'sequencefile', vector.get_value('codec'))
