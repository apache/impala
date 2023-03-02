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
#
# Targeted tests to validate analytic functions use TPCDS dataset.

from __future__ import absolute_import, division, print_function
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_parquet_dimension, ImpalaTestDimension

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
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpcds'))

    if cls.exploration_strategy() == 'exhaustive':
      batch_size = [1, 3, 10, 100, 0]
    else:
      batch_size = [0]

    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension('batch_size', *batch_size))

  def test_analytic_functions_tpcds(self, vector):
    vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/analytic-fns-tpcds', vector)

  def test_partitioned_topn(self, vector):
    """Targeted tests for the partitioned top-n operator."""
    vector.get_value('exec_option')['batch_size'] = vector.get_value('batch_size')
    self.run_test_case('QueryTest/analytic-fns-tpcds-partitioned-topn', vector)
