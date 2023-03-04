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
# Tests for IMPALA-2273

from __future__ import absolute_import, division, print_function
from builtins import range
import os
import pytest
import random
import string
import subprocess

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.skip import SkipIfFS

class TestParquetMaxPageHeader(CustomClusterTestSuite):
  '''This tests large page headers in parquet files. Parquet page header size can
  run into megabytes as they store min/max stats of actual column data. We need to
  adjust --max_page_header_size, which is the maximum bytes of header data that the
  scanner reads before it bails out.
  '''
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestParquetMaxPageHeader, cls).setup_class()


  TEXT_TABLE_NAME = "parquet_test_data_text"
  PARQUET_TABLE_NAME = "large_page_header"
  TEXT_DATA_LOCATION = "/test-warehouse/large_page_header_text"
  PARQUET_DATA_LOCATION = "/test-warehouse/large_page_header"
  MAX_STRING_LENGTH = 10*1024*1024

  @classmethod
  def add_test_dimensions(cls):
    super(CustomClusterTestSuite, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'parquet' and
        v.get_value('table_format').compression_codec == 'none')

  def setup_method(self, method):
    super(TestParquetMaxPageHeader, self).setup_method(method)
    impalad = self.cluster.impalads[0]
    client = impalad.service.create_beeswax_client()
    self.client = client
    self.__create_test_tbls()

  def teardown_method(self, method):
    self.__drop_test_tbls()

  def __drop_test_tbls(self):
    self.client.execute("DROP TABLE IF EXISTS %s PURGE" % self.TEXT_TABLE_NAME)
    self.client.execute("DROP TABLE IF EXISTS %s PURGE" % self.PARQUET_TABLE_NAME)

  def __create_test_tbls(self):
    self.__drop_test_tbls()
    self.client.execute("CREATE TABLE {0} (col string) STORED AS TEXTFILE LOCATION \'{1}\'"
        .format(self.TEXT_TABLE_NAME, self.TEXT_DATA_LOCATION))
    self.client.execute("CREATE TABLE {0} (col string) STORED AS PARQUET LOCATION \'{1}\'"
        .format(self.PARQUET_TABLE_NAME, self.PARQUET_DATA_LOCATION))
    # Load two long rows into the text table and convert it to parquet
    self.__generate_test_data(self.TEXT_DATA_LOCATION, "data.txt")
    self.client.execute("REFRESH {0}".format(self.TEXT_TABLE_NAME))
    insert_cmd = "\"INSERT OVERWRITE TABLE {0} SELECT col FROM {1}\""\
        .format(self.PARQUET_TABLE_NAME, self.TEXT_TABLE_NAME)
    # Impala parquet-writer doesn't write/use page statistics. So we use hive
    # to write these files
    self.run_stmt_in_hive(insert_cmd)

  def __generate_test_data(self, dir, file):
    """Creates a file in HDFS containing two MAX_STRING_LENGTH lines."""
    file_name = os.path.join(dir, file)
    # Create two 10MB long strings.
    random_text1 = "".join([random.choice(string.ascii_letters)
        for i in range(self.MAX_STRING_LENGTH)])
    random_text2 = "".join([random.choice(string.ascii_letters)
        for i in range(self.MAX_STRING_LENGTH)])
    put = subprocess.Popen(["hdfs", "dfs", "-put", "-d", "-f", "-", file_name],
        stdin=subprocess.PIPE, bufsize=-1)
    put.stdin.write(random_text1 + "\n")
    put.stdin.write(random_text2)
    put.stdin.close()
    put.wait()

  @SkipIfFS.hive
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("-max_page_header_size=31457280")
  def test_large_page_header_config(self, vector):
    # IMPALA-9856: Since this test expect to read a row up to 10 MB in size, we
    # explicitly set 11 MB MAX_ROW_SIZE here so that it can fit in BufferedPlanRootSink.
    self.client.set_configuration_option("max_row_size", "11mb")
    result = self.client.execute("select length(max(col)) from {0}"\
        .format(self.PARQUET_TABLE_NAME))
    assert result.data == [str(self.MAX_STRING_LENGTH)]

