# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

import pytest
import re
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import TestDimension

class TestHashJoinTimer(ImpalaTestSuite):
  """Tests that the local time in hash join is correct in the ExecSummary, average
   reporting and invidiaul fragment reporting."""

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
  HASH_JOIN_UPPER_BOUND_MS = 1000 
  HASH_JOIN_LOWER_BOUND_MS = 1

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestHashJoinTimer, cls).add_test_dimensions()
    cls.TestMatrix.add_dimension(TestDimension('test cases', *cls.TEST_CASES))
    cls.TestMatrix.add_constraint(lambda v: cls.__is_valid_test_vector(v))

  @classmethod
  def __is_valid_test_vector(cls, vector):
    return vector.get_value('table_format').file_format == 'text' and\
        vector.get_value('table_format').compression_codec == 'none' and\
        vector.get_value('exec_option')['batch_size'] == 0 and\
        vector.get_value('exec_option')['disable_codegen'] == False and\
        vector.get_value('exec_option')['num_nodes'] == 0

  @pytest.mark.execute_serially
  def test_hash_join_timer(self, vector):
    # This test runs serially because it requires the query to come back within
    # some amount of time. Running this with other tests makes it hard to bound
    # that time.
    test_case = vector.get_value('test cases')
    query = test_case[0]
    join_type = test_case[1]

    # Execute async to get a handle. Wait until the query has completed.
    handle = self.execute_query_async(query, vector.get_value('exec_option'))
    self.impalad_test_service.wait_for_query_state(self.client, handle,
        self.client.QUERY_STATES['FINISHED'], timeout=40)
    self.close_query(handle)

    # Parse the query profile
    # The hash join node is "id=3".
    # In the ExecSummary, search for "03:HASH JOIN" line, column 3 (avg) and 4 (max).
    # In the fragment (including average), search for "HASH_JOIN_NODE (id=2)" and the
    # non-child time.
    # Also verify that the build side is in a different thread by searching for:
    #     "Join Build-Side Prepared Asynchronously"
    profile = self.client.get_runtime_profile(handle)
    check_execsummary_count = 0
    check_fragment_count = 0
    async_build = False

    for line in profile.split("\n"):
        # Matching for ExecSummary
        if ("03:%s  " % (join_type) in line):
            # Sample line:
            # 03:HASH JOIN           3    11.89ms   12.543ms  6.57K  ...
            # Split using "JOIN +", then split the right side with space. This becomes:
            #   "3","11.89ms","12.543ms",...
            # The second column is the average, and the 3rd column is the max
            rhs = re.split("JOIN +", line)[1]
            columns = re.split(" +", rhs)
            self.__verify_join_time(columns[1], "ExecSummary Avg")
            self.__verify_join_time(columns[2], "ExecSummary Max")
            check_execsummary_count = 1
        # Matching for Fragment (including Average
        if ("(id=3)" in line):
            # Sample line:
            # HASH_JOIN_NODE (id=3):(Total: 3s580ms, non-child: 11.89ms, % non-child: 0.31%)
            strip1 = re.split("non-child: ", line)[1]
            non_child_time = re.split(", ", strip1)[0]
            self.__verify_join_time(non_child_time, "Fragment non-child")
            check_fragment_count = check_fragment_count + 1
        # Search for "Join Build-Side Prepared Asynchronously"
        if ("Join Build-Side Prepared Asynchronously" in line):
            asyn_build = True;

    assert (asyn_build), "Join is not prepared asynchronously"
    assert (check_fragment_count > 1), "Unable to verify Fragment or Average Fragment"
    assert (check_execsummary_count == 1), "Unable to verify ExecSummary" % profile

  def __verify_join_time(self, duration, comment):
    duration_ms = self.__parse_duration_ms(duration)
    if (duration_ms > self.HASH_JOIN_UPPER_BOUND_MS):
        assert False, "Hash join timing too high for %s: %s %s" %(comment, duration, duration_ms)
    if (duration_ms < self.HASH_JOIN_LOWER_BOUND_MS):
        assert False, "Hash join timing too low for %s: %s %s" %(comment, duration, duration_ms)

  def __parse_duration_ms(self, duration):
    """Parses a duration string of the form 1h2h3m4s5.6ms into milliseconds."""
    matches = re.findall(r'(?P<value>[0-9]+(\.[0-9]+)?)(?P<units>\D+)', duration)
    assert matches, 'Failed to parse duration string %s' % duration
    hours = 0
    minutes = 0
    seconds = 0
    milliseconds = 0
    for match in matches:
      if (match[2] == 'h'):
        hours = float(match[0])
      elif (match[2] == 'm'):
        minutes = float(match[0])
      elif (match[2] == 's'):
        seconds = float(match[0])
      elif (match[2] == 'ms'):
        milliseconds = float(match[0])
    return hours * 60 * 60 * 1000 + minutes * 60 * 1000 + seconds * 1000 + milliseconds
