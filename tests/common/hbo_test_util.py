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

# Shared helpers for HBO (History-Based Optimization) tests. These are independent of
# which cache backend is under test, so both the default in-memory suite
# (tests/query_test/test_hbo.py) and the distributed-backend suite
# (tests/custom_cluster/test_hbo_redis.py) can reuse them.

from __future__ import absolute_import, division, print_function
import time

from tests.common.environ import IS_CALCITE_PLANNER
from tests.util.test_file_parser import remove_comments

# Query options that enable reading and writing HBO stats. Both HBO test suites set these
# before running queries that should populate or consult the cache.
QUERY_OPTIONS = {'use_hbo_stats': True, 'store_hbo_stats': True}


class HboTestMixin(object):
  """Mixin providing HBO test helpers on top of ImpalaTestSuite (self.client,
  self.execute_query, self.load_query_test_file). Kept backend-agnostic so the same
  golden-file comparison drives both the in-memory and distributed cache suites."""

  def _run_hbo_explains(self, test_file):
    """Run EXPLAIN queries from a golden .test file and verify their output against
    the plan sections. A test case may define any of PLAN, DISTRIBUTEDPLAN,
    CALCITE_PLANNER_PLAN and CALCITE_PLANNER_DISTRIBUTED_PLAN. Each present section is
    verified independently (only the cardinality lines are compared):
      - PLAN / CALCITE_PLANNER_PLAN: verified at the default explain level.
      - DISTRIBUTEDPLAN / CALCITE_PLANNER_DISTRIBUTED_PLAN: verified at EXTENDED
        (explain_level=2) so exchange cardinalities (e.g. MERGING-EXCHANGE) are shown.
    When IS_CALCITE_PLANNER is set, the CALCITE_PLANNER_* section overrides its
    counterpart when present; otherwise the non-Calcite section is used."""
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    test_cases = self.load_query_test_file(
        'functional-query', test_file,
        valid_section_names=['QUERY', 'PLAN', 'DISTRIBUTEDPLAN',
            'CALCITE_PLANNER_PLAN', 'CALCITE_PLANNER_DISTRIBUTED_PLAN'])
    plan_variants = [
        ('PLAN', 'CALCITE_PLANNER_PLAN', {'use_hbo_stats': True}),
        ('DISTRIBUTEDPLAN', 'CALCITE_PLANNER_DISTRIBUTED_PLAN',
         {'use_hbo_stats': True, 'explain_level': 2}),
    ]
    for section in test_cases:
      query = remove_comments(section['QUERY'].strip())
      verified_any = False
      for default_name, calcite_name, config in plan_variants:
        if IS_CALCITE_PLANNER and calcite_name in section:
          plan_section_name = calcite_name
        elif default_name in section:
          plan_section_name = default_name
        else:
          continue
        verified_any = True
        self.client.set_configuration(config)
        result = self.execute_query(query)
        actual_plan_lines = result.data[result.data.index('PLAN-ROOT SINK'):]
        expected_plan_lines = section[plan_section_name].splitlines()
        # To avoid the test being fragile, we only compare the cardinality lines.
        actual_cardinality_lines = [line for line in actual_plan_lines
                                    if 'cardinality' in line]
        expected_cardinality_lines = [line for line in expected_plan_lines
                                      if 'cardinality' in line]
        assert actual_cardinality_lines == expected_cardinality_lines, (
            "EXPLAIN output mismatch for {0} ({1}).\nExpected:\n{2}\n\n"
            "Actual:\n{3}".format(test_file, plan_section_name,
                section[plan_section_name], '\n'.join(actual_plan_lines)))
      assert verified_any, \
          "No plan section found for query in {0}:\n{1}".format(test_file, query)
