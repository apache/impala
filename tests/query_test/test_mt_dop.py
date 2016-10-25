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

# Tests queries with the MT_DOP query option.

import pytest

from copy import deepcopy
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import TestDimension
from tests.common.test_vector import TestVector

MT_DOP_VALUES = [1, 2, 8]

class TestMtDop(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    super(TestMtDop, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('mt_dop', *MT_DOP_VALUES))
    # IMPALA-4332: The MT scheduler does not work for Kudu or HBase tables.
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format != 'hbase')
    cls.TestMatrix.add_constraint(\
        lambda v: v.get_value('table_format').file_format != 'kudu')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  def test_mt_dop(self, vector):
    new_vector = deepcopy(vector)
    new_vector.get_value('exec_option')['mt_dop'] = vector.get_value('mt_dop')
    self.run_test_case('QueryTest/mt-dop', new_vector)
