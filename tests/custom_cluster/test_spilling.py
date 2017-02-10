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
    ImpalaTestDimension,
    create_single_exec_option_dimension,
    create_parquet_dimension)


class TestSpillStress(CustomClusterTestSuite):

  # IMPALA-4914 part 3 TODO: In the agg_stress workload file used below, it's implied
  # that the workload needs to run concurrently. This appears to attempt to try to do
  # that by using TEST_IDS and NUM_ITERATIONS and assuming multiple instances of the
  # test will run concurrently. But custom cluster tests are explicitly never run
  # concurrently. If this test ought to run concurrently, then new test infra is needed
  # to achieve that.

  @classmethod
  def get_workload(self):
    return 'targeted-stress'

  @classmethod
  def setup_class(cls):
    # Don't do anything expensive if we're not running exhaustively for this workload.
    # This check comes first.
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    # Since test_spill_stress below runs TEST_IDS * NUM_ITERATIONS times, but we only
    # need Impala to start once, it's inefficient to use
    # @CustomClusterTestSuite.with_args() to restart Impala every time. Instead, start
    # Impala here, once. We start impalad before we try to make connections to it.
    #
    # Start with 256KB buffers to reduce data size required to force spilling.
    cls._start_impala_cluster([
        '--impalad_args=--"read_size=262144"',
        '--catalogd_args="--load_catalog_in_background=false"'])
    # This super() call leapfrogs CustomClusterTestSuite.setup_class() and calls
    # ImpalaTestSuite.setup_class(). This is done because this is an atypical custom
    # cluster test that doesn't restart impalad per test method, but it still needs
    # self.client (etc.) set up. IMPALA-4914 part 3 would address this.
    # This happens last because we need impalad started above before we try to establish
    # connections to it.
    super(CustomClusterTestSuite, cls).setup_class()

  @classmethod
  def teardown_class(cls):
    # This teardown leapfrog matches the setup leapfrog above, for equivalent reasons.
    # CustomClusterTestSuite.teardown_class() overrides ImpalaTestSuite.teardown_class()
    # and is a no-op. ImpalaTestSuite.teardown_class() stops client connections. The
    # *next* custom cluster test that py.test chooses to run is responsible for
    # restarting impalad.
    super(CustomClusterTestSuite, cls).teardown_class()

  def setup_method(self, method):
    # We don't need CustomClusterTestSuite.setup_method() or teardown_method() here
    # since we're not doing per-test method restart of Impala.
    pass

  def teardown_method(self, method):
    pass

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillStress, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:\
        v.get_value('table_format').file_format == 'text')
    # Each client will get a different test id.
    # TODO: this test takes extremely long so only run on exhaustive. It would
    # be good to configure it so we can run some version on core.
    TEST_IDS = xrange(0, 3)
    NUM_ITERATIONS = [1]
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('test_id', *TEST_IDS))
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('iterations', *NUM_ITERATIONS))

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
