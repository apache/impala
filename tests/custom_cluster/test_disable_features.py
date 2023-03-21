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
import pytest

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.parametrize import UniqueDatabase
from tests.common.skip import SkipIfFS


class TestDisableFeatures(CustomClusterTestSuite):
  """Tests that involve disabling features at startup."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @SkipIfFS.hdfs_caching
  @pytest.mark.execute_serially
  @UniqueDatabase.parametrize(sync_ddl=True)
  @CustomClusterTestSuite.with_args(
    catalogd_args="--enable_incremental_metadata_updates=false")
  def test_disable_incremental_metadata_updates(self, vector, unique_database):
    """Canary tests for disabling incremental metadata updates. Copy some partition
    related tests in metadata/test_ddl.py here."""
    vector.get_value('exec_option')['sync_ddl'] = True
    self.run_test_case('QueryTest/alter-table-hdfs-caching', vector,
        use_db=unique_database, multiple_impalad=True)
    self.run_test_case('QueryTest/alter-table-set-column-stats', vector,
        use_db=unique_database, multiple_impalad=True)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--allow_ordinals_in_having=true")
  def test_allow_ordinals_in_having(self, vector):
    """Mirror the FE tests in AnalyzeStmtsTest#TestHavingIntegers to make sure the flag
       can bring back the legacy feature"""
    self.client.execute("select not bool_col as nb from functional.alltypes having 1")
    self.execute_query_expect_failure(
        self.client, "select count(*) from functional.alltypes having 1")
    self.client.execute("select count(*) > 10 from functional.alltypes having 1")
    self.execute_query_expect_failure(
        self.client,
        "select sum(id) over(order by id) from functional.alltypes having 1")
    self.execute_query_expect_failure(
        self.client,
        "select sum(id) over(order by id) from functional.alltypes having -1")

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--enable_json_scanner=false")
  def test_disable_json_scanner(self, vector):
    self.run_test_case('QueryTest/disable-json-scanner', vector)
