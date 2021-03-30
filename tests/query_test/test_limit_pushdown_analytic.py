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

# Test the limit pushdown to analytic sort in the presence
# of ranking functions

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_single_exec_option_dimension,
    extend_exec_option_dimension)


class TestLimitPushdownAnalyticTpch(ImpalaTestSuite):

  @classmethod
  def get_workload(cls):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimitPushdownAnalyticTpch, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_limit_pushdown_analytic(self, vector):
    self.run_test_case('limit-pushdown-analytic', vector)


class TestLimitPushdownAnalyticFunctional(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestLimitPushdownAnalyticFunctional, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    # Also run with num_nodes=1 because it's easier to reproduce IMPALA-10296.
    extend_exec_option_dimension(cls, 'num_nodes', 1)
    cls.ImpalaTestMatrix.add_constraint(lambda v:
        v.get_value('table_format').file_format in ['parquet'])

  def test_limit_pushdown_analytic(self, vector):
    self.run_test_case('limit-pushdown-analytic', vector)
