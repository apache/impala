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
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite


class TestBeeswax(ImpalaTestSuite):

  def test_user_validation(self):
    """Test various scenarios with cross-connection operations with the same
    and different users."""
    USER1 = "user1"
    USER2 = "user2"
    client1 = self.client
    different_user_client = None
    unset_user_client = None
    try:
      same_user_client = self.create_impala_client(protocol='beeswax')
      different_user_client = self.create_impala_client(protocol='beeswax')
      unset_user_client = self.create_impala_client(protocol='beeswax')

      # Unauthenticated Beewax only sets user once the query is run.
      result = client1.execute("select effective_user()", user=USER1)
      assert result.data[0] == USER1
      result = same_user_client.execute("select effective_user()", user=USER1)
      assert result.data[0] == USER1
      # The user shouldn't change once set.
      result = client1.execute("select effective_user()", user=USER2)
      assert result.data[0] == USER1
      result = different_user_client.execute("select effective_user()", user=USER2)
      assert result.data[0] == USER2

      QUERY = "select * from functional.alltypes"
      handle = client1.execute_async(QUERY)
      client1.get_state(handle)
      # Connections with a effective user should not be able to access other users'
      # sessions.
      self._assert_invalid_handle(lambda: different_user_client.fetch(QUERY, handle))
      self._assert_invalid_handle(lambda: different_user_client.get_state(handle))
      self._assert_invalid_handle(lambda: different_user_client.get_log(handle))
      self._assert_profile_access_denied(
          lambda: different_user_client.get_runtime_profile(handle))
      self._assert_profile_access_denied(
          lambda: different_user_client.get_exec_summary(handle))
      self._assert_invalid_handle(lambda: different_user_client.close_dml(handle))

      # Connections with the same user can always access the requests. A connection
      # without an effective user (only possible with unauthenticated connections)
      # is allowed to access any query. Some Impala tests depend on this.
      for valid_client in [client1, same_user_client, unset_user_client]:
        valid_client.get_state(handle)
        valid_client.fetch(QUERY, handle)
        valid_client.get_log(handle)
        valid_client.get_runtime_profile(handle)
        valid_client.get_exec_summary(handle)

      # Validation of user should be skipped for closing and cancelling queries, to
      # avoid breaking administrative tools that clean up queries via Beeswax RPCs.
      different_user_client.cancel(handle)
      different_user_client.close_query(handle)
    finally:
      if different_user_client is not None:
        different_user_client.close()
      if unset_user_client is not None:
        unset_user_client.close()

  def _assert_invalid_handle(self, fn):
    """Assert that invoking fn() raises an ImpalaBeeswaxException with an invalid query
    handle error."""
    try:
      fn()
      assert False, "Expected invalid handle"
    except ImpalaBeeswaxException as e:
      assert "Query id" in str(e) and "not found" in str(e), str(e)

  def _assert_profile_access_denied(self, fn):
    """Assert that invoking fn() raises an ImpalaBeeswaxException with the error
    message indicating that profile access is denied."""
    try:
      fn()
      assert False, "Expected invalid handle"
    except ImpalaBeeswaxException as e:
      assert "is not authorized to access the runtime profile" in str(e), str(e)
