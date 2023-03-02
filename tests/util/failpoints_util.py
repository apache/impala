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
from tests.common.impala_test_suite import ImpalaTestSuite, LOG


def execute_query_expect_debug_action_failure(impala_test_suite, query, vector):
  """Executes the given query with the configured debug_action and asserts that the
  query fails. Removes the debug_action from the exec options, re-runs the query, and
  assert that it succeeds."""
  assert 'debug_action' in vector.get_value('exec_option')
  # Run the query with the given debug_action and assert that the query fails.
  # execute_query_expect_failure either returns the client exception thrown when executing
  # the query, or the result of the query if it failed but did the client did not throw an
  # exception. Either way, log the result.
  LOG.debug(ImpalaTestSuite.execute_query_expect_failure(
      impala_test_suite.client, query, vector.get_value('exec_option')))

  # Assert that the query can be run without the debug_action.
  del vector.get_value('exec_option')['debug_action']
  result = impala_test_suite.execute_query(query, vector.get_value('exec_option'))
  assert result.success, "Failed to run {0} without debug action".format(query)
