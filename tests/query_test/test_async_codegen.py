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
#
from __future__ import absolute_import, division, print_function
import pytest

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import add_mandatory_exec_option
from tests.common.test_dimensions import create_exec_option_dimension
from tests.common.test_result_verifier import extract_event_sequence


class TestAsyncCodegen(ImpalaTestSuite):
  """
  This class tests the three relevant scenarios with respect to the timing of asynchronous
  code generation and query execution:

    1. Async codegen finishes before query execution starts (only codegen'd code runs)
    2. Query execution finishes before async codegen finishes (only interpreted code runs)
    3. Async codegen finishes during query execution (both interpreted and condegen'd code
       runs, switching to codegen from interpreted mode)

  This is achieved by inserting sleeps into execution threads by
    - Debug actions
      - in the codegen thread to delay codegen
      - in the fragment execution thread to delay the beginning of query execution
        relative to codegen
    - In the query itself to make the query take longer time so we can time codegen to
      finish during query execution.

  In case 1, query execution is delayed. In case 2, codegen is delayed. In case three, the
  query execution time is made longer and codegen is delayed to finish during query
  execution.
  """

  # Query template - the sleep time is filled in at runtime based on which case we are
  # testing. Sleeping is only necessary in case 3.
  query_template = """
select int_col, sleep(%i)
from functional.alltypessmall
where int_col > 5
limit 10;
"""

  # Async codegen is finished before starting execution. Only codegen'd code runs.
  DEBUG_ACTION_CODEGEN_FINISH_BEFORE_EXEC_START = \
      "AFTER_STARTING_ASYNC_CODEGEN_IN_FRAGMENT_THREAD:SLEEP@250"

  # Execution finishes before async codegen finishes. Only interpreted code runs.
  DEBUG_ACTION_EXEC_FINISH_BEFORE_CODEGEN = \
      "BEFORE_CODEGEN_IN_ASYNC_CODEGEN_THREAD:SLEEP@500"

  # Async codegen finishes when execution has already started but not finished yet. Both
  # interpreted and codegen'd code runs.
  DEBUG_ACTION_CODEGEN_FINISH_DURING_EXEC = \
      "BEFORE_CODEGEN_IN_ASYNC_CODEGEN_THREAD:SLEEP@250"

  sleep_times = {
      DEBUG_ACTION_CODEGEN_FINISH_BEFORE_EXEC_START: 0,
      DEBUG_ACTION_CODEGEN_FINISH_DURING_EXEC: 100,
      DEBUG_ACTION_EXEC_FINISH_BEFORE_CODEGEN: 0
  }

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestAsyncCodegen, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_exec_option_dimension(
        cluster_sizes=[1],
        disable_codegen_options=[False],
        batch_sizes=[0],
        disable_codegen_rows_threshold_options=[0],
        debug_action_options=[cls.DEBUG_ACTION_CODEGEN_FINISH_BEFORE_EXEC_START,
            cls.DEBUG_ACTION_CODEGEN_FINISH_DURING_EXEC,
            cls.DEBUG_ACTION_EXEC_FINISH_BEFORE_CODEGEN]))
    add_mandatory_exec_option(cls, "async_codegen", 1)

    cls.ImpalaTestMatrix.add_constraint(lambda vector: cls.__is_valid_test_vector(vector))

  @classmethod
  def __is_valid_test_vector(cls, vector):
    return (not vector.get_value('exec_option')['disable_codegen'] and
        vector.get_value('exec_option')['disable_codegen_rows_threshold'] == 0 and
        vector.get_value('exec_option')['exec_single_node_rows_threshold'] == 0 and
        vector.get_value('exec_option')['num_nodes'] == 1 and
        vector.get_value('table_format').file_format == 'text' and
        vector.get_value('table_format').compression_codec == 'none')

  # This test is run serially because it depends on timing.
  @pytest.mark.execute_serially
  def test_async_codegen(self, vector):
    debug_action = vector.get_value('exec_option')['debug_action']
    # Insert the sleep time into the query template
    query = self.query_template % self.sleep_times[debug_action]

    result = self.execute_query(query, vector.get_value('exec_option'))
    profile = result.runtime_profile

    events = extract_event_sequence(profile)

    self.__check_event_sequence(debug_action, events)

  def __check_event_sequence(self, debug_action, events):
    exec_start = events.index('First Batch Produced')
    exec_end = events.index('ExecInternal Finished')

    # TODO: Is it ok if the AsyncCodegenFinished event is not in the list when codegen is
    # not finished when execution finishes?
    codegen_end = events.index('AsyncCodegenFinished') \
        if 'AsyncCodegenFinished' in events else len(events)

    if debug_action == self.DEBUG_ACTION_CODEGEN_FINISH_BEFORE_EXEC_START:
      assert codegen_end < exec_start
    elif debug_action == self.DEBUG_ACTION_CODEGEN_FINISH_DURING_EXEC:
      assert exec_start < codegen_end < exec_end
    else:
      assert debug_action == self.DEBUG_ACTION_EXEC_FINISH_BEFORE_CODEGEN, \
          "Unrecognised debug action: '%s'." % debug_action
      assert exec_end < codegen_end
