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
from copy import deepcopy

from tests.common.environ import ImpalaTestClusterProperties
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfNotHdfsMinicluster
from tests.common.test_dimensions import (create_exec_option_dimension_from_dict,
    create_kudu_dimension, create_parquet_dimension)

IMPALA_TEST_CLUSTER_PROPERTIES = ImpalaTestClusterProperties.get_instance()

# Test with denial of reservations at varying frequency.
# Always test with the minimal amount of spilling and running with the absolute minimum
# memory requirement.
CORE_DEBUG_ACTION_DIMS = [None,
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@1.0']

# Test with different frequency of denial on exhaustive to try and exercise more
# interesting code paths.
EXHAUSTIVE_DEBUG_ACTION_DIMS = [
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@0.1',
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@0.5',
  '-1:OPEN:SET_DENY_RESERVATION_PROBABILITY@0.9']


@pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                   reason='Queries may not spill on larger clusters')
class TestSpillingDebugActionDimensions(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillingDebugActionDimensions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpch'))
    debug_action_dims = CORE_DEBUG_ACTION_DIMS
    if cls.exploration_strategy() == 'exhaustive':
      debug_action_dims = CORE_DEBUG_ACTION_DIMS + EXHAUSTIVE_DEBUG_ACTION_DIMS
    # Tests are calibrated so that they can execute and spill with this page size.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({'default_spillable_buffer_size': ['256k'],
          'debug_action': debug_action_dims, 'mt_dop': [0, 1]}))
    # Pare down the combinations of mt_dop and debug_action that run to reduce test time.
    # The MT code path for joins is more complex, so focus testing there.
    if cls.exploration_strategy() == 'exhaustive':
      debug_action_dims = CORE_DEBUG_ACTION_DIMS + EXHAUSTIVE_DEBUG_ACTION_DIMS
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('exec_option')['mt_dop'] == 1 or
          v.get_value('exec_option')['debug_action'] in CORE_DEBUG_ACTION_DIMS)
    elif cls.exploration_strategy() == 'core':
      cls.ImpalaTestMatrix.add_constraint(lambda v:
          v.get_value('exec_option')['mt_dop'] == 1 or
          v.get_value('exec_option')['debug_action'] is None)

  def test_spilling(self, vector):
    self.run_test_case('QueryTest/spilling', vector)

  def test_spilling_aggs(self, vector):
    self.run_test_case('QueryTest/spilling-aggs', vector)

  def test_spilling_large_rows(self, vector, unique_database):
    """Test that we can process large rows in spilling operators, with or without
       spilling to disk"""
    self.run_test_case('QueryTest/spilling-large-rows', vector, unique_database)

  def test_spilling_naaj(self, vector):
    """Test spilling null-aware anti-joins"""
    self.run_test_case('QueryTest/spilling-naaj', vector)

  @SkipIfNotHdfsMinicluster.tuned_for_minicluster
  def test_spilling_regression_exhaustive(self, vector):
    """Regression tests for spilling. mem_limits tuned for 3-node minicluster."""
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip("only run large sorts on exhaustive")
    self.run_test_case('QueryTest/spilling-regression-exhaustive', vector)
    new_vector = deepcopy(vector)
    del new_vector.get_value('exec_option')['default_spillable_buffer_size']
    self.run_test_case(
        'QueryTest/spilling-regression-exhaustive-no-default-buffer-size', new_vector)


@pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                   reason='Queries may not spill on larger clusters')
class TestSpillingNoDebugActionDimensions(ImpalaTestSuite):
  """Spilling tests to which we don't want to apply the debug_action dimension."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillingNoDebugActionDimensions, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpch'))
    # Tests are calibrated so that they can execute and spill with this page size.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({'default_spillable_buffer_size': ['64k'],
            'mt_dop': [0, 4]}))

  def test_spilling_naaj_no_deny_reservation(self, vector):
    """
    Null-aware anti-join tests that depend on getting more than the minimum reservation
    and therefore will not reliably pass with the deny reservation debug action enabled.
    """
    self.run_test_case('QueryTest/spilling-naaj-no-deny-reservation', vector)

  def test_spilling_query_options(self, vector):
    """Test that spilling-related query options work end-to-end. These tests rely on
      setting debug_action to alternative values via query options."""
    self.run_test_case('QueryTest/spilling-query-options', vector)

  def test_spilling_no_debug_action(self, vector):
    """Spilling tests that will not succeed if run with an arbitrary debug action.
       These tests either run with no debug action set or set their own debug action."""
    self.run_test_case('QueryTest/spilling-no-debug-action', vector)


@pytest.mark.xfail(IMPALA_TEST_CLUSTER_PROPERTIES.is_remote_cluster(),
                   reason='Queries may not spill on larger clusters')
class TestSpillingBroadcastJoins(ImpalaTestSuite):
  """Tests specifically targeted at shared broadcast joins for mt_dop."""
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpillingBroadcastJoins, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    # Use Kudu because it has 9 input splits for lineitem, hence can have a
    # higher effective dop than parquet, which only has 3 splits.
    cls.ImpalaTestMatrix.add_dimension(create_kudu_dimension('tpch'))
    debug_action_dims = CORE_DEBUG_ACTION_DIMS
    if cls.exploration_strategy() == 'exhaustive':
      debug_action_dims = CORE_DEBUG_ACTION_DIMS + EXHAUSTIVE_DEBUG_ACTION_DIMS
    # Tests are calibrated so that they can execute and spill with this page size.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({'default_spillable_buffer_size': ['256k'],
          'debug_action': debug_action_dims, 'mt_dop': [3]}))

  def test_spilling_broadcast_joins(self, vector):
    # Disable bloom-filter for Kudu since the number of probe rows could be reduced
    # if runtime bloom-filter is pushed to Kudu, hence change the spilling behavior.
    self.execute_query("SET ENABLED_RUNTIME_FILTER_TYPES=MIN_MAX")

    self.run_test_case('QueryTest/spilling-broadcast-joins', vector)
