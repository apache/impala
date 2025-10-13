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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIf

@SkipIf.not_hdfs
class TestCreatingPaimonTable(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestCreatingPaimonTable, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(
        lambda v: v.get_value('table_format').file_format == 'parquet')

  def test_paimon_ddl(self, vector, unique_database):
    self.run_test_case('QueryTest/paimon-ddl', vector, unique_database)

  def test_paimon_show_stats(self, vector, unique_database):
    self.run_test_case('QueryTest/paimon-show-stats', vector, unique_database)

  def test_paimon_show_file_stats(self):
    tables = ["paimon_non_partitioned", "paimon_partitioned"]
    for table in tables:
      result = self.client.execute("show files in functional_parquet." + table)
      assert len(result.data) == 5, "Expected 5 files"

  def test_create_paimon_ddl_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/paimon-ddl-negative',
                       vector, unique_database)

  def test_paimon_negative(self, vector, unique_database):
    self.run_test_case('QueryTest/paimon-negative',
                       vector, unique_database)
