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

# Functional tests running the TPCH workload.
from __future__ import absolute_import, division, print_function

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
  create_parquet_dimension,
  create_single_exec_option_dimension
)


class TestProcessingCost(ImpalaTestSuite):
  """Test processing cost in non-dedicated coordinator environment."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestProcessingCost, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension(cls.get_workload()))

  def test_admission_slots(self, vector):
    self.run_test_case('QueryTest/processing-cost-admission-slots', vector)
