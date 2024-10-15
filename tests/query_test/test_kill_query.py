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
from tests.common.test_dimensions import create_client_protocol_dimension
from tests.util.cancel_util import (
    QueryToKill,
    assert_kill_error,
    assert_kill_ok
)


class TestKillQuery(ImpalaTestSuite):
  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix.add_dimension(create_client_protocol_dimension())

  def test_same_coordinator(self, vector):
    protocol = vector.get_value('protocol')
    with self.create_client_for_nth_impalad(0, protocol) as client, \
        QueryToKill(self, protocol) as query_id_to_kill:
      assert_kill_ok(client, query_id_to_kill)

  def test_different_coordinator(self, vector):
    protocol = vector.get_value('protocol')
    with self.create_client_for_nth_impalad(1, protocol) as client, \
        QueryToKill(self, protocol, nth_impalad=0) as query_id_to_kill:
      assert_kill_ok(client, query_id_to_kill)

  def test_invalid_query_id(self, vector):
    with self.create_impala_client(protocol=vector.get_value('protocol')) as client:
      assert_kill_error(
          client,
          "ParseException: Syntax error",
          sql="KILL QUERY 123:456",
      )
      assert_kill_error(
          client,
          "AnalysisException: Invalid query id: ''",
          sql="KILL QUERY ''",
      )
      assert_kill_error(
          client,
          "AnalysisException: Invalid query id: 'f:g'",
          sql="KILL QUERY 'f:g'",
      )
      assert_kill_error(
          client,
          "AnalysisException: Invalid query id: '123'",
          sql="KILL QUERY '123'",
      )

  def test_idempotence(self, vector):
    protocol = vector.get_value('protocol')
    with self.create_impala_client(protocol=protocol) as client, \
        QueryToKill(self, protocol) as query_id_to_kill:
      assert_kill_ok(client, query_id_to_kill)
      assert_kill_error(
          client,
          "Could not find query on any coordinator",
          query_id=query_id_to_kill,
      )
      assert_kill_error(
          client,
          "Could not find query on any coordinator",
          query_id=query_id_to_kill,
      )
