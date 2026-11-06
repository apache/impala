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
import re
import time

from tests.common.environ import IS_CALCITE_PLANNER
from tests.util.test_file_parser import remove_comments

# Query options that enable reading and writing HBO stats. Both HBO test suites set these
# before running queries that should populate or consult the cache.
QUERY_OPTIONS = {'use_hbo_stats': True, 'store_hbo_stats': True}

# Matches a plan-tree node header such as "01:AGGREGATE [STREAMING]",
# "|  00:SCAN HDFS [...]" or "|--04:EXCHANGE [BROADCAST]": an optional tree-drawing
# prefix followed by the "<id>:" node label. Fragment headers ("F02:...") and
# "PLAN-ROOT SINK" deliberately do not match.
_PLAN_NODE_HEADER = re.compile(r'^[\s|-]*\d+:')


def _hbo_cardinality_lines(plan_lines):
  """Return the cardinality portion (from 'row-size' onward) of every cardinality
  line in an EXPLAIN plan, in plan order.

  The cardinality of a pre-aggregation node ("AGGREGATE [STREAMING]") is
  non-deterministic across runs (IMPALA-15513), so for those nodes we drop the
  numeric cardinality value and keep only any HBO annotation (e.g. "(from HBO)").
  This still verifies that HBO stats were (or were not) applied to the
  pre-aggregation, without asserting the flaky cardinality value."""
  lines = []
  in_pre_agg = False
  for line in plan_lines:
    if _PLAN_NODE_HEADER.match(line):
      in_pre_agg = 'AGGREGATE [STREAMING]' in line
    if 'cardinality' in line:
      cardinality = line.split('row-size', 1)[1]
      if in_pre_agg:
        cardinality = re.sub(r'(cardinality=)\S+', r'\g<1>*', cardinality)
      lines.append(cardinality)
  return lines


class HboTestMixin(object):
  """Mixin providing HBO test helpers on top of ImpalaTestSuite (self.client,
  self.execute_query, self.load_query_test_file). Kept backend-agnostic so the same
  golden-file comparison drives both the in-memory and distributed cache suites."""

  def _run_hbo_explains(self, test_file, test_file_vars=None):
    """Run EXPLAIN queries from a golden .test file and verify their output against
    the plan sections. A test case may define any of PLAN, DISTRIBUTEDPLAN,
    CALCITE_PLANNER_PLAN and CALCITE_PLANNER_DISTRIBUTEDPLAN. Each present section is
    verified independently (only the cardinality lines are compared):
      - PLAN / CALCITE_PLANNER_PLAN: verified at the default explain level.
      - DISTRIBUTEDPLAN / CALCITE_PLANNER_DISTRIBUTEDPLAN: verified at EXTENDED
        (explain_level=2) so exchange cardinalities (e.g. MERGING-EXCHANGE) are shown.
    When IS_CALCITE_PLANNER is set, the CALCITE_PLANNER_* section overrides its
    counterpart when present; otherwise the non-Calcite section is used."""
    # Wait for 1 second to ensure the stats are written to the cache.
    time.sleep(1)
    test_cases = self.load_query_test_file(
        'functional-query', test_file,
        valid_section_names=['QUERY', 'PLAN', 'DISTRIBUTEDPLAN',
            'CALCITE_PLANNER_PLAN', 'CALCITE_PLANNER_DISTRIBUTEDPLAN'])
    plan_variants = [
        ('PLAN', 'CALCITE_PLANNER_PLAN', {'use_hbo_stats': True}),
        ('DISTRIBUTEDPLAN', 'CALCITE_PLANNER_DISTRIBUTEDPLAN',
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
        expected_plan = section[plan_section_name]
        for key, value in (test_file_vars or {}).items():
          expected_plan = expected_plan.replace(key, value)
        expected_plan_lines = expected_plan.splitlines()
        # To avoid the test being fragile, we only compare the cardinality lines.
        actual_cardinality_lines = _hbo_cardinality_lines(actual_plan_lines)
        expected_cardinality_lines = _hbo_cardinality_lines(expected_plan_lines)
        assert actual_cardinality_lines == expected_cardinality_lines, (
            "EXPLAIN output mismatch for {0} ({1}).\nExpected:\n{2}\n\n"
            "Actual:\n{3}".format(test_file, plan_section_name,
                expected_plan, '\n'.join(actual_plan_lines)))
      assert verified_any, \
          "No plan section found for query in {0}:\n{1}".format(test_file, query)
