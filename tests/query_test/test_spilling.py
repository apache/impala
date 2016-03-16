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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (create_exec_option_dimension_from_dict,
    create_parquet_dimension)

class TestSpilling(ImpalaTestSuite):
  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestSpilling, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.clear_constraints()
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension('tpch'))
    # Tests are calibrated so that they can execute and spill with this page size.
    cls.ImpalaTestMatrix.add_dimension(
        create_exec_option_dimension_from_dict({'default_spillable_buffer_size' : ['256k']}))

  def test_spilling(self, vector):
    self.run_test_case('QueryTest/spilling', vector)
