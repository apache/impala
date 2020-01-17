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

import pytest
from copy import deepcopy

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfABFS, SkipIfNotHdfsMinicluster

WAIT_TIME_MS = build_flavor_timeout(60000, slow_build_timeout=100000)


class TestMtDopFlags(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDopFlags, cls).add_test_dimensions()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--unlock_mt_dop=true")
  @SkipIfABFS.file_or_folder_name_ends_with_period
  def test_mt_dop_all(self, vector, unique_database):
    """Test joins, inserts and runtime filters with mt_dop > 0"""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = 4
    self.run_test_case('QueryTest/joins', vector, use_db="functional_parquet")
    self.run_test_case('QueryTest/insert', vector)

    # Runtime filter tests assume 3 fragments, which we can get with one instance per
    # node.
    vector.get_value('exec_option')['mt_dop'] = 1
    self.run_test_case('QueryTest/runtime_filters', vector,
       test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})
    self.run_test_case('QueryTest/runtime_row_filters', vector,
        use_db="functional_parquet",
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS' : str(WAIT_TIME_MS)})

    # Allow test to override num_nodes.
    del vector.get_value('exec_option')['num_nodes']
    self.run_test_case('QueryTest/joins_mt_dop', vector,
       test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(impalad_args="--mt_dop_auto_fallback=true")
  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_mt_dop_fallback(self, vector, unique_database):
    """Test joins and inserts fall back to non-mt_dop correctly."""
    vector = deepcopy(vector)
    vector.get_value('exec_option')['mt_dop'] = 4
    # Targeted test case that verifies that the fallback actually switches to the
    # non-mt-dop plans.
    self.run_test_case('QueryTest/mt-dop-auto-fallback', vector, use_db=unique_database)

    # Check that the join and insert plans work as expected.
    self.run_test_case('QueryTest/joins', vector, use_db="functional_parquet")
    self.run_test_case('QueryTest/insert', vector)

  @CustomClusterTestSuite.with_args(impalad_args="--unlock_mt_dop=true", cluster_size=1)
  def test_mt_dop_runtime_filters_one_node(self, vector):
    """Runtime filter tests, which assume 3 fragment instances, can also be run on a single
    node cluster to test multiple filter sources/destinations per backend."""
    vector.get_value('exec_option')['mt_dop'] = 3
    self.run_test_case('QueryTest/runtime_filters', vector,
        test_file_vars={'$RUNTIME_FILTER_WAIT_TIME_MS': str(WAIT_TIME_MS)})
