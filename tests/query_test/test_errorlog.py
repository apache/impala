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

# Injects failures  at specific locations in each of the plan nodes. Currently supports
# two types of failures - cancellation of the query and a failure test hook.
#
from __future__ import absolute_import, division, print_function
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import (
    create_single_exec_option_dimension,
    create_uncompressed_text_dimension)
from time import sleep

# Test injecting error logs in prepare phase and status::OK(). This tests one of race
# conditions in error reporting (IMPALA-3385).
class TestErrorLogs(ImpalaTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestErrorLogs, cls).add_test_dimensions()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.ImpalaTestMatrix.add_dimension(
        create_uncompressed_text_dimension(cls.get_workload()))

  def test_errorlog(self, vector):
    query = 'select count(*) from tpch.lineitem;'
    action = 'INJECT_ERROR_LOG'
    location = 'PREPARE'
    # scan node has id=0
    node_id = 0
    # Without restarting impala, reporting thread reports every 5 seconds.
    # Having a lower scan range can make the query run more than 2*5 seconds
    # Select location in prepare to guarantee at least one cleared error maps will
    # be sent to coordinator.
    debug_action = '%d:%s:%s' % (node_id, location, action)
    vector.get_value('exec_option')['debug_action'] = debug_action
    vector.get_value('exec_option')['MAX_SCAN_RANGE_LENGTH'] = '1000'
    self.__execute_inject_error_log_action(query, vector)

  def __execute_inject_error_log_action(self, query, vector):
    try:
      handle = self.execute_query_async(query, vector.get_value('exec_option'))
      # sleep() is used to terminate the query; otherwise it runs for a long time. It is
      # large enough to further guarantee at least one cleared error maps to be sent to
      # coordinator.
      sleep(30)
      cancel_result = self.client.cancel(handle)
      self.client.close_query(handle)
      assert cancel_result.status_code == 0,\
          'Unexpected status code from cancel request: %s' % cancel_result
    # As long as impala did not crash we are good.
    except ImpalaBeeswaxException:
      return
