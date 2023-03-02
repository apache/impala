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
from tests.common.file_utils import create_table_from_parquet
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension


class TestDatasketches(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestDatasketches, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_hll(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'hll_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'hll_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-hll', vector, unique_database)

  def test_cpc(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'cpc_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'cpc_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-cpc', vector, unique_database)

  def test_theta(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'theta_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'theta_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-theta', vector, unique_database)

  def test_kll(self, vector, unique_database):
    create_table_from_parquet(self.client, unique_database, 'kll_sketches_from_hive')
    create_table_from_parquet(self.client, unique_database, 'kll_sketches_from_impala')
    self.run_test_case('QueryTest/datasketches-kll', vector, unique_database)
