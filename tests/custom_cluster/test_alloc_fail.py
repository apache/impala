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
from tests.common.skip import SkipIfBuildType

@SkipIfBuildType.not_dev_build
class TestAllocFail(CustomClusterTestSuite):
  """Tests for handling malloc() failure for UDF/UDA"""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_fn_ctx_alloc=1")
  def test_alloc_fail_init(self, vector):
    self.run_test_case('QueryTest/alloc-fail-init', vector)

  # TODO: Rewrite or remove the non-deterministic test.
  @pytest.mark.xfail(run=True, reason="IMPALA-2925: the execution is not deterministic "
                     "so some tests sometimes don't fail as expected")
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--stress_fn_ctx_alloc=3")
  def test_alloc_fail_update(self, vector, unique_database):
    # Note that this test relies on pre-aggregation to exercise the Serialize() path so
    # query option 'num_nodes' must not be 1. CustomClusterTestSuite.add_test_dimensions()
    # already filters out vectors with 'num_nodes' != 0 so just assert it here.
    assert vector.get_value('exec_option')['num_nodes'] == 0
    self.run_test_case('QueryTest/alloc-fail-update', vector, use_db=unique_database)
