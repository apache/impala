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
from tests.common.impala_test_suite import ImpalaTestSuite


class TestSingleNodePlanCreated(ImpalaTestSuite):
  @pytest.mark.execute_serially
  def test_single_node_plan_created_time(self):
    """Regression test for IMPALA-10806: Create single node plan slowdown when hundreds
    of inline views are joined"""
    query = "with aa as (select * from (select * from functional.widetable_1000_cols) t \
      where int_col1 = 10) select t1.int_col1 from aa t1 join aa t2 on \
      t1.int_col1 = t2.int_col2 join aa t3 on t1.int_col1 = t3.int_col1 \
      join aa t4 on t1.int_col1 = t4.int_col1"
    profile = self.execute_query(query).runtime_profile
    key = "Single node plan created"
    for line in profile.splitlines():
      if key in line:
        values = line.split('(')[1].strip('ms)').split('s')
        value = 0.0
        if len(values) == 2:
          value = float(values[0]) * 1000 + float(values[1])
        assert value < 1000, "Took too long to create single node plan"
        break
