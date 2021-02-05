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

from tests.common.file_utils import create_table_and_copy_files
from tests.common.impala_test_suite import ImpalaTestSuite


class TestParquetBloomFilter(ImpalaTestSuite):
  """
  This suite tests Parquet Bloom filter optimizations.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetBloomFilter, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_bloom_filtering(self, vector, unique_database):
    """ Tests that Parquet Bloom filtering works when it is enabled. """
    self.create_test_database(unique_database)

    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filtering'] = True
    self.run_test_case('QueryTest/parquet-bloom-filter', vector, unique_database)

  def test_parquet_bloom_filtering_off(self, vector, unique_database):
    """ Check that there is no Parquet Bloom filtering when it is disabled. """
    self.create_test_database(unique_database)

    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    vector.get_value('exec_option')['parquet_bloom_filtering'] = False
    self.run_test_case('QueryTest/parquet-bloom-filter-disabled', vector, unique_database)

  def create_test_database(self, unique_database):
    create_stmt = 'create table {db}.{tbl} (        '\
                  '  int8_col TINYINT,              '\
                  '  int16_col SMALLINT,            '\
                  '  int32_col INT,                 '\
                  '  int64_col BIGINT,              '\
                  '  float_col FLOAT,               '\
                  '  double_col DOUBLE,             '\
                  '  string_col STRING,             '\
                  '  char_col VARCHAR(3)            '\
                  ')                                '\
                  'stored as parquet                '
    create_table_and_copy_files(self.client, create_stmt,
                                unique_database, 'parquet_bloom_filter',
                                ['testdata/data/parquet-bloom-filtering.parquet'])
