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
from tests.common.skip import SkipIfS3, SkipIf
from tests.util.filesystem_utils import WAREHOUSE

# Tests the COMPUTE STATS command for gathering table and column stats.
# TODO: Merge this test file with test_col_stats.py
@SkipIfS3.insert # S3: missing coverage: compute stats
@SkipIf.not_default_fs # Isilon: Missing coverage: compute stats
class TestComputeStats(ImpalaTestSuite):
  TEST_DB_NAME = "compute_stats_db"
  TEST_ALIASING_DB_NAME = "parquet"

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestComputeStats, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    # Do not run these tests using all dimensions because the expected results
    # are different for different file formats.
    cls.TestMatrix.add_dimension(create_uncompressed_text_dimension(cls.get_workload()))

  def setup_method(self, method):
    # cleanup and create a fresh test database
    self.cleanup_db(self.TEST_DB_NAME)
    self.execute_query("create database {0} location '{1}/{0}.db'"
        .format(self.TEST_DB_NAME, WAREHOUSE))
    # cleanup and create a fresh test database whose name is a keyword
    self.cleanup_db(self.TEST_ALIASING_DB_NAME)
    self.execute_query("create database `{0}` location '{1}/{0}.db'"
        .format(self.TEST_ALIASING_DB_NAME, WAREHOUSE))

  def teardown_method(self, method):
    self.cleanup_db(self.TEST_DB_NAME)
    self.cleanup_db(self.TEST_ALIASING_DB_NAME)

  @pytest.mark.execute_serially
  def test_compute_stats(self, vector):
    self.run_test_case('QueryTest/compute-stats', vector)
    # Test compute stats on decimal columns separately so we can vary between CDH4/5
    self.run_test_case('QueryTest/compute-stats-decimal', vector)
    # To cut down on test execution time, only run the compute stats test against many
    # partitions if performing an exhaustive test run.
    if self.exploration_strategy() != 'exhaustive': return
    self.run_test_case('QueryTest/compute-stats-many-partitions', vector)

  @pytest.mark.execute_serially
  def test_compute_stats_incremental(self, vector):
    self.run_test_case('QueryTest/compute-stats-incremental', vector)
