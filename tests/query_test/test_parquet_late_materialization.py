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

from copy import deepcopy

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.file_utils import create_table_from_parquet


class TestParquetLateMaterialization(ImpalaTestSuite):
  """
  This suite tests late materialization optimization for parquet.
  """

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetLateMaterialization, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
      lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_late_materialization(self, vector):
    self.run_test_case('QueryTest/parquet-late-materialization', vector)

  def test_parquet_late_materialization_collections(self, vector):
    """IMPALA-15374: the row counter of a collection column reader must stay in sync
    when rows are skipped. The page index decides which symptom shows up, so run with it
    on and off."""
    new_vector = deepcopy(vector)
    # A small batch size makes whole scratch batches get filtered out between surviving
    # ones, which is what triggers the skip.
    new_vector.get_value('exec_option')['batch_size'] = 4
    for late_mat in [-1, 1]:
      new_vector.get_value('exec_option')['parquet_late_materialization_threshold'] = \
          late_mat
      for page_index in ['true', 'false']:
        new_vector.get_value('exec_option')['parquet_read_page_index'] = page_index
        self.run_test_case('QueryTest/parquet-late-materialization-collections',
            new_vector)

  def test_parquet_late_materialization_unique_db(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'decimals_1_10')
    create_table_from_parquet(self.client, unique_database, 'nested_decimals')
    self.run_test_case('QueryTest/parquet-late-materialization-unique-db', vector,
        unique_database)
