#!/usr/bin/env python
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

from copy import deepcopy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestJoinAggSpilling(ImpalaTestSuite):
  """Test class verify spilling. """
  # These query take 400mb-1gb if no mem limits are set
  # TODO: these tests take a long time to run. Add more vectors when we can trim them.
  MEM_LIMITS = ["200m"]

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestJoinAggSpilling, cls).add_test_dimensions()
    # add mem_limit as a test dimension.
    new_dimension = TestDimension('mem_limit', *TestJoinAggSpilling.MEM_LIMITS)
    cls.TestMatrix.add_dimension(new_dimension)
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format in ['parquet'])

  # Test running with different mem limits to exercise the dynamic memory
  # scaling functionality.
  def test_mem_usage_scaling(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['max_block_mgr_memory'] = \
        vector.get_value('mem_limit')
    self.run_test_case('join-agg-spilling', vector)

