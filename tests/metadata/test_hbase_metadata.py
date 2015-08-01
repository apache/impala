# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from tests.common.test_vector import *
from tests.common.impala_test_suite import *
from tests.common.test_dimensions import create_uncompressed_text_dimension

# Tests the COMPUTE STATS command for gathering table and column stats.
# TODO: Merge this test file with test_col_stats.py
@pytest.mark.execute_serially
class TestHbaseMetadata(ImpalaTestSuite):
  TEST_DB_NAME = "compute_stats_db_hbase"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHbaseMetadata, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format == 'hbase')

  def setup_method(self, method):
    # cleanup and create a fresh test database
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database %s" % (self.TEST_DB_NAME))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)

  def test_hbase_compute_stats(self, vector):
    self.run_test_case('QueryTest/hbase-compute-stats', vector)

  def test_hbase_compute_stats_incremental(self, vector):
    self.run_test_case('QueryTest/hbase-compute-stats-incremental', vector)
