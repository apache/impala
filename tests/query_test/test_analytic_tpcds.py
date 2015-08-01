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
#
# Targeted tests to validate analytic functions use TPCDS dataset.

import logging
import pytest
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_parquet_dimension, TestDimension

class TestAnalyticTpcds(ImpalaTestSuite):
  """Test class to do functional validation of analytic functions.

  Batch size parameter is varied if the exploration strategy is exhaustive.
  """
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAnalyticTpcds, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(create_parquet_dimension('tpcds'))

    if cls.exploration_strategy() == 'exhaustive':
      batch_size = [1, 3, 10, 100, 0]
    else:
      batch_size = [0]

    cls.TestMatrix.add_dimension(TestDimension('batch_size', *batch_size))

  def test_analytic_functions_tpcds(self, vector):
    vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/analytic-fns-tpcds', vector)
