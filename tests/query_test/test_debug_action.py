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

from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import ImpalaTestDimension
from tests.common.test_dimensions import create_parquet_dimension


class TestDebugAction(ImpalaTestSuite):
  """Test different verification scenarios for DEBUG_ACTION query option."""

  _debug_actions = {
    # Debug action and its corresponding expected error.
    # Correct debug actions:
    'RECVR_ADD_BATCH:FAIL@0.8':
      'Debug Action: RECVR_ADD_BATCH:FAIL@0.8',
    '0:GETNEXT:FAIL':
      'Debug Action: FAIL',

    # Invalid global debug actions:
    'RECVR_ADD_BATCH:SLEEP':
      'Invalid debug_action RECVR_ADD_BATCH:SLEEP (expected SLEEP@<ms>)',
    'RECVR_ADD_BATCH:JITTER@8@8':
      'Invalid debug_action RECVR_ADD_BATCH:JITTER@8@8 (invalid probability)',
    'RECVR_ADD_BATCH:FAIL@8':
      'Invalid debug_action RECVR_ADD_BATCH:FAIL@8 (invalid probability)',
    'RECVR_ADD_BATCH:EXCEPTION@Unknown':
      'Invalid debug_action RECVR_ADD_BATCH:EXCEPTION@Unknown (Invalid exception type)',

    # Invalid ExecNode debug actions:
    '0:GETNEXT:DELAY@aa':
      'Invalid sleep duration: \'aa\'. Only non-negative numbers are allowed.',

    # Both global and ExecNode debug actions are valid
    'RECVR_ADD_BATCH:FAIL@0.8|0:GETNEXT:FAIL':
      'Debug Action: FAIL',

    # Both global and ExecNode debug actions are valid
    '0:GETNEXT:FAIL|RECVR_ADD_BATCH:FAIL@0.8':
      'Debug Action: FAIL',

    # Global debug action is invalid
    'RECVR_ADD_BATCH:FAIL@8|0:GETNEXT:FAIL':
      'Invalid debug_action RECVR_ADD_BATCH:FAIL@8 (invalid probability)',

    # Global debug action is invalid
    '0:GETNEXT:FAIL|RECVR_ADD_BATCH:FAIL@8':
      'Invalid debug_action RECVR_ADD_BATCH:FAIL@8 (invalid probability)',

    # ExecNode debug action is invalid
    'RECVR_ADD_BATCH:FAIL@0.8|0:GETNEXT:DELAY@aa':
      'Invalid sleep duration: \'aa\'. Only non-negative numbers are allowed.',

    # ExecNode debug action is invalid
    '0:GETNEXT:DELAY@aa|RECVR_ADD_BATCH:FAIL@0.8':
      'Invalid sleep duration: \'aa\'. Only non-negative numbers are allowed.',

    # Both global and ExecNode debug actions are invalid, global prevails
    'RECVR_ADD_BATCH:FAIL@8|0:GETNEXT:DELAY@aa':
      'Invalid debug_action RECVR_ADD_BATCH:FAIL@8 (invalid probability)',

    # Both ExecNode and global debug actions are invalid, ExecNode prevails
    '0:GETNEXT:DELAY@aa|RECVR_ADD_BATCH:FAIL@8':
      'Invalid sleep duration: \'aa\'. Only non-negative numbers are allowed.',
  }

  _query = "select * from functional.alltypes"

  @classmethod
  def add_test_dimensions(cls):
    super(TestDebugAction, cls).add_test_dimensions()
    # Pass only the keys (debug actions) to add_dimension()
    cls.ImpalaTestMatrix.add_dimension(ImpalaTestDimension(
        'debug_action', *cls._debug_actions.keys()))
    cls.ImpalaTestMatrix.add_dimension(create_parquet_dimension(cls.get_workload()))

  def test_failpoints(self, vector):
    vector.get_value('exec_option')['debug_action'] = vector.get_value('debug_action')
    result = self.execute_query_expect_failure(
        self.client, self._query, vector.get_value('exec_option'))
    assert self._debug_actions[vector.get_value('debug_action')] in str(result)
