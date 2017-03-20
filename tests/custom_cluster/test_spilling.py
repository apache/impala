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
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_parquet_dimension)

class TestSpilling(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpilling, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpch'))
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())

  # Reduce the IO read size. This reduces the memory required to trigger spilling.
  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--read_size=200000",
      catalogd_args="--load_catalog_in_background=false")
  def test_spilling(self, vector):
    new_vector = deepcopy(vector)
    # remove this. the test cases set this explicitly.
    del new_vector.get_value('exec_option')['num_nodes']
    self.run_test_case('QueryTest/spilling', new_vector)
