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
import os
import pytest
import shlex
from copy import deepcopy
from subprocess import check_call

from tests.common.file_utils import (
  create_table_from_parquet, create_table_and_copy_files)
from tests.common.test_vector import ImpalaTestDimension
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.util.filesystem_utils import get_fs_path

MT_DOP_VALUES = [0, 1, 2, 8]

class TestParquetStats(ImpalaTestSuite):
  """
  This suite tests runtime optimizations based on Parquet statistics.
  """

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestParquetStats, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('mt_dop', *MT_DOP_VALUES))
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_parquet_stats(self, vector, unique_database):
    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/parquet-stats', vector, use_db=unique_database)

  def test_deprecated_stats(self, vector, unique_database):
    """Test that reading parquet files with statistics with deprecated 'min'/'max' fields
    works correctly. The statistics will be used for known-good types (boolean, integral,
    float) and will be ignored for all other types (string, decimal, timestamp)."""

    # We use CTAS instead of "create table like" to convert the partition columns into
    # normal table columns.
    create_table_and_copy_files(self.client, 'create table {db}.{tbl} stored as parquet '
                                             'as select * from functional.alltypessmall '
                                             'limit 0',
                                unique_database, 'deprecated_stats',
                                ['testdata/data/deprecated_statistics.parquet'])
    # The test makes assumptions about the number of row groups that are processed and
    # skipped inside a fragment, so we ensure that the tests run in a single fragment.
    vector.get_value('exec_option')['num_nodes'] = 1
    self.run_test_case('QueryTest/parquet-deprecated-stats', vector, unique_database)

  def test_invalid_stats(self, vector, unique_database):
    """IMPALA-6538" Test that reading parquet files with statistics with invalid
    'min_value'/'max_value' fields works correctly. 'min_value' and 'max_value' are both
    NaNs, therefore we need to ignore them"""
    create_table_from_parquet(self.client, unique_database, 'min_max_is_nan')
    self.run_test_case('QueryTest/parquet-invalid-minmax-stats', vector, unique_database)

  def test_page_index(self, vector, unique_database):
    """Test that using the Parquet page index works well. The various test files
    contain queries that exercise the page selection and value-skipping logic against
    columns with different types and encodings."""
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['abort_on_error']
    create_table_from_parquet(self.client, unique_database, 'decimals_1_10')
    create_table_from_parquet(self.client, unique_database, 'nested_decimals')
    create_table_from_parquet(self.client, unique_database, 'double_nested_decimals')
    create_table_from_parquet(self.client, unique_database, 'alltypes_tiny_pages')
    create_table_from_parquet(self.client, unique_database, 'alltypes_tiny_pages_plain')
    create_table_from_parquet(self.client, unique_database, 'alltypes_empty_pages')
    create_table_from_parquet(self.client, unique_database, 'alltypes_invalid_pages')
    create_table_from_parquet(self.client, unique_database,
                              'customer_multiblock_page_index')

    for batch_size in [0, 1]:
      new_vector.get_value('exec_option')['batch_size'] = batch_size
      self.run_test_case('QueryTest/parquet-page-index', new_vector, unique_database)
      self.run_test_case('QueryTest/nested-types-parquet-page-index', new_vector,
                         unique_database)
      self.run_test_case('QueryTest/parquet-page-index-alltypes-tiny-pages', new_vector,
                         unique_database)
      self.run_test_case('QueryTest/parquet-page-index-alltypes-tiny-pages-plain',
                         new_vector, unique_database)

    for batch_size in [0, 1, 2, 3, 4, 8, 16, 32, 64, 128, 256, 512]:
      new_vector.get_value('exec_option')['batch_size'] = batch_size
      self.run_test_case('QueryTest/parquet-page-index-large', new_vector,
                         unique_database)
    # Test for the bugfix
    self.run_test_case('QueryTest/parquet-page-index-bugfix', vector, unique_database)
