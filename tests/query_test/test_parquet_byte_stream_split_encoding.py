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

from __future__ import absolute_import

import os

from tests.common.file_utils import create_table_and_copy_files
from tests.common.impala_test_suite import ImpalaTestSuite


class TestParquetEncodings(ImpalaTestSuite):

  TEST_FILE_DIRECTORY = "testdata/parquet_byte_stream_split_encoding"

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetEncodings, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_byte_stream_split_encoding_float(self, vector, unique_database):
    self._parquet_byte_stream_split_encoding_helper(vector, unique_database, "float",
        os.path.join(self.TEST_FILE_DIRECTORY, "floats_byte_stream_split.parquet"))

  def test_parquet_byte_stream_split_encoding_double(self, vector, unique_database):
    self._parquet_byte_stream_split_encoding_helper(vector, unique_database, "double",
        os.path.join(self.TEST_FILE_DIRECTORY, "doubles_byte_stream_split.parquet"))

  def _parquet_byte_stream_split_encoding_helper(self, vector, unique_database, col_type,
      filename):
    table_name = "parquet_byte_stream_split_negative_test"
    create_stmt = "create table {}.{} (numbers {}) stored as parquet".format(
        unique_database, table_name, col_type)
    create_table_and_copy_files(self.client, create_stmt, unique_database, table_name,
                                [filename])
    query_stmt = "select * from {}.{}".format(unique_database, table_name)
    result = self.execute_query_expect_failure(self.client, query_stmt)
    assert "unsupported encoding: BYTE_STREAM_SPLIT" in str(result)
