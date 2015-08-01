# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

import logging
import pytest
from copy import deepcopy
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.test_dimensions import (TestDimension,
    create_single_exec_option_dimension,
    create_parquet_dimension)

class TestSpillStress(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def setup_class(cls):
    #start impala with args
    cls._start_impala_cluster(['--impalad_args=--"read_size=200000"',
        'catalogd_args="--load_catalog_in_background=false"'])
    super(CustomClusterTestSuite, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    pass

  @classmethod
  def setup_method(self, method):
    pass

  @classmethod
  def teardown_method(self, method):
    pass

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillStress, cls).add_test_dimensions()
    cls.TestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')

    # Each client will get a different test id.
    # TODO: this test takes extremely long so only run on exhaustive. It would
    # be good to configure it so we can run some version on core.
    TEST_IDS = xrange(0, 3)
    NUM_ITERATIONS = [0]
    if cls.exploration_strategy() == 'exhaustive':
      TEST_IDS = xrange(0, 3)
      NUM_ITERATIONS = [1]

    cls.TestMatrix.add_dimension(TestDimension('test_id', *TEST_IDS))
    cls.TestMatrix.add_dimension(TestDimension('iterations', *NUM_ITERATIONS))

  @pytest.mark.stress
  def test_spill_stress(self, vector):
    # Number of times to execute each query
    for i in xrange(vector.get_value('iterations')):
      self.run_test_case('agg_stress', vector)

class TestSpilling(CustomClusterTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpilling, cls).add_test_dimensions()
    cls.TestMatrix.clear_constraints()
    cls.TestMatrix.add_dimension(create_parquet_dimension('tpch'))
    cls.TestMatrix.add_dimension(create_single_exec_option_dimension())

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
