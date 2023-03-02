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
import re

from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.util.parse_util import parse_duration_string_ms
from tests.verifiers.metric_verifier import MetricVerifier


class TestHashJoinTimer(ImpalaTestSuite):
  """Tests that the local time in hash join is correct in the ExecSummary, average
   reporting and individual fragment reporting."""

  # There are two cases that we are interested in verifying that the profile is returning
  # a correct timing:
  #   case 1: the LHS open() is much slower than the RHS
  #   case 2: the RHS is much slower than the LHS open()
  # In both cases, the time spent in the child shouldn't be counted. Because it's not
  # processing that many rows, the actual time should be less than 100ms even though
  # the scan is taking longer than that (due to sleeping).
  # Also, the local time spent in the join node in case 1 should be lower because the
  # RHS time is already absorbed by the LHS open().
  #
  # Test case 3 & 4 are the same as case 1 & 2 but using nested loop join.
  #
  # Fully hint the queries so that the plan will not change.

  # Each test case contain a query, the join type.
  TEST_CASES = [["select /*+straight_join*/ count(*) from"
              " (select distinct * from functional.alltypes where int_col >= sleep(5)) a"
              " join /* +SHUFFLE */ functional.alltypes b on (a.id=b.id)",
              "HASH JOIN"],
             ["select /*+straight_join*/ count(*) from functional.alltypes a"
              " join /* +SHUFFLE */ "
              " (select distinct * from functional.alltypes where int_col >= sleep(5)) b"
              " on (a.id=b.id)",
              "HASH JOIN"],
             ["select /*+straight_join*/ count(*) from"
              " (select distinct * from functional.alltypes where int_col >= sleep(5)) a"
              " cross join "
              " functional.alltypes b where a.id > b.id and b.id=99",
              "NESTED LOOP JOIN"],
             ["select /*+straight_join*/ count(*) from functional.alltypes a"
              " CROSS join "
              " (select distinct * from functional.alltypes where int_col >= sleep(5)) b"
              " where a.id>b.id and a.id=99",
              "NESTED LOOP JOIN"]
             ]
  # IMPALA-2973: For non-code-coverage builds, 1000 milliseconds are sufficient, but more
  # time is needed in code-coverage builds.
  HASH_JOIN_UPPER_BOUND_MS = 2000
  # IMPALA-2973: Temporary workaround: when timers are using Linux COARSE clockid_t, very
  # short times may be measured as zero.
  HASH_JOIN_LOWER_BOUND_MS = 0
  # Constant of nanoseconds per millisecond
  NANOS_PER_MILLI = 1000000

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHashJoinTimer, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(
        ImpalaTestDimension('test cases', *cls.TEST_CASES))
    cls.ImpalaTestMatrix.add_constraint(lambda v: cls.__is_valid_test_vector(v))

  @classmethod
  def __is_valid_test_vector(cls, vector):
    return vector.get_value('table_format').file_format == 'text' and \
        vector.get_value('table_format').compression_codec == 'none' and \
        vector.get_value('exec_option')['batch_size'] == 0 and \
        vector.get_value('exec_option')['disable_codegen'] == False and \
        vector.get_value('exec_option')['num_nodes'] == 0

  @pytest.mark.execute_serially
  def test_hash_join_timer(self, vector):
    # This test runs serially because it requires the query to come back within
    # some amount of time. Running this with other tests makes it hard to bound
    # that time. It also assumes that it will be able to get a thread token to
    # execute the join build in parallel.
    test_case = vector.get_value('test cases')
    query = test_case[0]
    join_type = test_case[1]

    # Ensure that the cluster is idle before starting the test query.
    for impalad in ImpalaCluster.get_e2e_test_cluster().impalads:
      verifier = MetricVerifier(impalad.service)
      verifier.wait_for_metric("impala-server.num-fragments-in-flight", 0)

    # Execute the query. The query summary and profile are stored in 'result'.
    result = self.execute_query(query, vector.get_value('exec_option'))

    # Parse the query summary; The join node is "id=3".
    # In the ExecSummary, search for the join operator's summary and verify the
    # avg and max times are within acceptable limits.
    exec_summary = result.exec_summary
    check_execsummary_count = 0
    join_node_name = "03:%s" % (join_type)
    for line in exec_summary:
      if line['operator'] == join_node_name:
        avg_time_ms = line['avg_time'] // self.NANOS_PER_MILLI
        self.__verify_join_time(avg_time_ms, "ExecSummary Avg")
        max_time_ms = line['max_time'] // self.NANOS_PER_MILLI
        self.__verify_join_time(max_time_ms, "ExecSummary Max")
        check_execsummary_count += 1
    assert (check_execsummary_count == 1), \
        "Unable to verify ExecSummary: {0}".format(exec_summary)

    # Parse the query profile; The join node is "id=3".
    # In the profiles, search for lines containing "(id=3)" and parse for the avg and
    # non-child times to verify that they are within acceptable limits. Also verify
    # that the build side is built in a different thread by searching for the string:
    # "Join Build-Side Prepared Asynchronously"
    profile = result.runtime_profile
    check_fragment_count = 0
    asyn_build = False
    for line in profile.split("\n"):
      if ("skew(s)" in line):
        # Sample line:
        # skew(s) found at: HASH_JOIN_NODE (id=3), EXCHANGE_NODE (id=8)
        continue
      if ("(id=3)" in line):
        # Sample line:
        # HASH_JOIN_NODE (id=3):(Total: 3s580ms, non-child: 11.89ms, % non-child: 0.31%)
        strip1 = re.split("non-child: ", line)[1]
        non_child_time = re.split(", ", strip1)[0]
        non_child_time_ms = parse_duration_string_ms(non_child_time)
        self.__verify_join_time(non_child_time_ms, "Fragment non-child")
        check_fragment_count += 1
      # Search for "Join Build-Side Prepared Asynchronously"
      if ("Join Build-Side Prepared Asynchronously" in line):
        asyn_build = True
    assert (asyn_build), "Join is not prepared asynchronously: {0}".format(profile)
    assert (check_fragment_count > 1), \
        "Unable to verify Fragment or Average Fragment: {0}".format(profile)

  def __verify_join_time(self, duration_ms, comment):
    if (duration_ms > self.HASH_JOIN_UPPER_BOUND_MS):
      assert False, "Hash join timing too high for %s: %s" % (comment, duration_ms)
    if (duration_ms < self.HASH_JOIN_LOWER_BOUND_MS):
      assert False, "Hash join timing too low for %s: %s" % (comment, duration_ms)
