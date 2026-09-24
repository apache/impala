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
from tests.common.skip import SkipIfFS
from tests.common.test_dimensions import create_single_exec_option_dimension

# Column schema shared by HLL, CPC, and Theta sketch tables.
# 'CREATE TABLE LIKE PARQUET' would infer these as STRING because the Parquet
# files use unannotated byte_array. We declare them explicitly as BINARY.
_SKETCH_COLS_9 = ('ti binary, i binary, bi binary, f binary, d binary, '
                  's binary, c binary, v binary, nc binary')

# Column schema for KLL sketch tables.
_KLL_SKETCH_COLS = ('f binary, repetitions binary, some_nulls binary, '
                    'all_nulls binary, some_nans binary, all_nans binary')


def _create_sketch_table(client, unique_database, table_name, col_schema):
  """Create a sketch table with explicit BINARY columns and load a Parquet file.

  'CREATE TABLE LIKE PARQUET' infers unannotated byte_array as STRING; this
  helper bypasses inference and declares all sketch columns as BINARY directly.
  """
  create_stmt = (
      'CREATE TABLE {{db}}.{{tbl}} ({cols}) STORED AS PARQUET'
      .format(cols=col_schema))
  parquet_file = 'testdata/data/{0}.parquet'.format(table_name)
  create_table_and_copy_files(client, create_stmt, unique_database,
                              table_name, [parquet_file])


class TestDatasketches(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestDatasketches, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_hll(self, vector, unique_database):
    _create_sketch_table(
        self.client, unique_database, 'hll_sketches_from_hive', _SKETCH_COLS_9)
    _create_sketch_table(
        self.client, unique_database, 'hll_sketches_from_impala', _SKETCH_COLS_9)
    self.run_test_case('QueryTest/datasketches-hll', vector, unique_database)

  def test_cpc(self, vector, unique_database):
    _create_sketch_table(
        self.client, unique_database, 'cpc_sketches_from_hive', _SKETCH_COLS_9)
    _create_sketch_table(
        self.client, unique_database, 'cpc_sketches_from_impala', _SKETCH_COLS_9)
    self.run_test_case('QueryTest/datasketches-cpc', vector, unique_database)

  def test_theta(self, vector, unique_database):
    _create_sketch_table(
        self.client, unique_database, 'theta_sketches_from_hive', _SKETCH_COLS_9)
    _create_sketch_table(
        self.client, unique_database, 'theta_sketches_from_impala', _SKETCH_COLS_9)
    self.run_test_case('QueryTest/datasketches-theta', vector, unique_database)

  def test_kll(self, vector, unique_database):
    _create_sketch_table(
        self.client, unique_database, 'kll_sketches_from_hive', _KLL_SKETCH_COLS)
    _create_sketch_table(
        self.client, unique_database, 'kll_sketches_from_impala', _KLL_SKETCH_COLS)
    self.run_test_case('QueryTest/datasketches-kll', vector, unique_database)


class TestDatasketchesOrcHiveInterop(ImpalaTestSuite):
  """Tests that ds_hll_sketch() returns BINARY and that consuming
  functions accept BINARY, enabling interop with Hive-written ORC sketch tables."""

  @classmethod
  def add_test_dimensions(cls):
    super(TestDatasketchesOrcHiveInterop, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format == 'orc')

  @SkipIfFS.hive
  def test_hll_sketch_orc_string_binary_mismatch(self, vector, unique_database):
    """Hive writes sketch as BINARY to ORC/HMS; Impala must
    accept BINARY sketch columns and pass them to ds_hll_estimate()."""
    self.run_test_case(
        'QueryTest/datasketches-hll-hive-orc', vector, unique_database)
