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
#
import pytest
import sys
import re
from copy import copy
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.test_vector import *
from tests.common.impala_test_suite import *

class TestQueryMemLimitScaling(ImpalaTestSuite):
  """Test class to do functional validation of per query memory limits. """
  QUERY = ["select * from lineitem where l_orderkey = -1",
           "select min(l_orderkey) from lineitem",
           "select * from lineitem order by l_orderkey limit 1"]

  # These query take 400mb-1gb if no mem limits are set
  MEM_LIMITS = ["-1", "400m", "150m"]

  @classmethod
  def get_workload(self):
    return 'tpch'

  @classmethod
  def add_test_dimensions(cls):
    super(TestQueryMemLimitScaling, cls).add_test_dimensions()
    # add mem_limit as a test dimension.
    new_dimension = TestDimension('mem_limit', *TestQueryMemLimitScaling.MEM_LIMITS)
    cls.TestMatrix.add_dimension(new_dimension)

  # Test running with different mem limits to exercise the dynamic memory
  # scaling functionality.
  def test_mem_usage_scaling(self, vector):
    mem_limit = copy(vector.get_value('mem_limit'))
    table_format = vector.get_value('table_format')
    exec_options = copy(vector.get_value('exec_option'))
    exec_options['mem_limit'] = mem_limit
    for query in self.QUERY:
      self.execute_query(query, exec_options, table_format=table_format)

