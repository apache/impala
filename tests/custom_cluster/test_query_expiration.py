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
# Tests for query expiration.

import pytest
import re
import threading
from time import sleep, time

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite

class TestQueryExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""

  def _check_num_executing(self, impalad, expected):
    in_flight_queries = impalad.service.get_in_flight_queries()
    actual = 0
    for query in in_flight_queries:
      if query["executing"]:
        actual += 1
      else:
        assert query["waiting"]
    assert actual == expected

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=6 --logbuflevel=-1")
  def test_query_expiration(self, vector):
    """Confirm that single queries expire if not fetched"""
    impalad = self.cluster.get_first_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    handles = []

    # This query will time out with the default idle timeout (6s).
    query1 = "SELECT SLEEP(1000000)"
    default_timeout_expire_handle = client.execute_async(query1)
    handles.append(default_timeout_expire_handle)

    # This query will hit a lower time limit.
    client.execute("SET EXEC_TIME_LIMIT_S=1")
    time_limit_expire_handle = client.execute_async(query1);
    handles.append(time_limit_expire_handle)

    # This query will hit a lower idle timeout instead of the default timeout or time
    # limit.
    client.execute("SET EXEC_TIME_LIMIT_S=5")
    client.execute("SET QUERY_TIMEOUT_S=1")
    short_timeout_expire_handle = client.execute_async("SELECT SLEEP(2000000)")
    handles.append(short_timeout_expire_handle)
    client.execute("SET EXEC_TIME_LIMIT_S=0")

    # Set a huge timeout, to check that the server bounds it by --idle_query_timeout
    client.execute("SET QUERY_TIMEOUT_S=1000")
    default_timeout_expire_handle2 = client.execute_async("SELECT SLEEP(3000000)")
    handles.append(default_timeout_expire_handle2)
    self._check_num_executing(impalad, len(handles))

    before = time()
    sleep(4)

    # Queries with timeout or time limit of 1 should have expired, other queries should
    # still be running.
    assert num_expired + 2 == impalad.service.get_metric_value(
      'impala-server.num-queries-expired')
    assert (client.get_state(short_timeout_expire_handle) ==
            client.QUERY_STATES['EXCEPTION'])
    assert (client.get_state(time_limit_expire_handle) ==
            client.QUERY_STATES['EXCEPTION'])
    assert (client.get_state(default_timeout_expire_handle) ==
            client.QUERY_STATES['FINISHED'])
    assert (client.get_state(default_timeout_expire_handle2) ==
            client.QUERY_STATES['FINISHED'])
    self.__expect_expired(client, query1, short_timeout_expire_handle,
        "Query [0-9a-f]+:[0-9a-f]+ expired due to client inactivity \(timeout is 1s000ms\)")
    self.__expect_expired(client, query1, time_limit_expire_handle,
        "Query [0-9a-f]+:[0-9a-f]+ expired due to execution time limit of 1s000ms")
    self._check_num_executing(impalad, 2)
    self.assert_impalad_log_contains('INFO', "Expiring query due to client inactivity: "
        "[0-9a-f]+:[0-9a-f]+, last activity was at: \d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d")
    self.assert_impalad_log_contains('INFO',
        "Expiring query [0-9a-f]+:[0-9a-f]+ due to execution time limit of 1s")

    # Wait until the remaining queries expire. The time limit query will have hit
    # expirations but only one should be counted.
    impalad.service.wait_for_metric_value('impala-server.num-queries-expired',
                                          num_expired + len(handles))
    # The metric and client state are not atomically maintained. Since the
    # expiration metric has just been reached, accessing the client state
    # is guarded in a loop to avoid flaky false negatives.
    self.__expect_client_state(client, default_timeout_expire_handle,
                               client.QUERY_STATES['EXCEPTION'])
    self.__expect_client_state(client, default_timeout_expire_handle2,
                               client.QUERY_STATES['EXCEPTION'])

    # Check that we didn't wait too long to be expired (double the timeout is sufficiently
    # large to avoid most noise in measurement)
    assert time() - before < 12

    client.execute("SET QUERY_TIMEOUT_S=0")
    # Synchronous execution; calls fetch() and query should not time out.
    # Note: could be flakey if execute() takes too long to call fetch() etc after the
    # query completes.
    handle = client.execute("SELECT SLEEP(2500)")

    # Confirm that no extra expirations happened
    assert impalad.service.get_metric_value('impala-server.num-queries-expired') \
        == len(handles)
    self._check_num_executing(impalad, 0)
    for handle in handles:
      try:
        client.close_query(handle)
      except Exception, e:
        # We fetched from some cancelled handles above, which unregistered the queries.
        assert 'Invalid or unknown query handle' in str(e)

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=0")
  def test_query_expiration_no_default(self, vector):
    """Confirm that single queries expire if no default is set, but a per-query
    expiration or time limit is set"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    query = "SELECT SLEEP(1000000)"
    client.execute("SET QUERY_TIMEOUT_S=1")
    timeout_handle = client.execute_async(query)
    client.execute("SET QUERY_TIMEOUT_S=0")

    client.execute("SET EXEC_TIME_LIMIT_S=1")
    time_limit_handle = client.execute_async(query)
    client.execute("SET EXEC_TIME_LIMIT_S=0")

    # Set a huge timeout, server should not expire the query while this test is running
    client.execute("SET QUERY_TIMEOUT_S=1000")
    no_timeout_handle = client.execute_async(query)

    before = time()
    sleep(4)

    # Query with timeout of 1 should have expired, other query should still be running.
    assert num_expired + 2 == impalad.service.get_metric_value(
      'impala-server.num-queries-expired')

    assert client.get_state(timeout_handle) == client.QUERY_STATES['EXCEPTION']
    assert client.get_state(time_limit_handle) == client.QUERY_STATES['EXCEPTION']
    assert client.get_state(no_timeout_handle) == client.QUERY_STATES['FINISHED']
    self.__expect_expired(client, query, timeout_handle,
        "Query [0-9a-f]+:[0-9a-f]+ expired due to client inactivity \(timeout is 1s000ms\)")
    self.__expect_expired(client, query, time_limit_handle,
        "Query [0-9a-f]+:[0-9a-f]+ expired due to execution time limit of 1s000ms")

  def __expect_expired(self, client, query, handle, exception_regex):
    """Check that the query handle expired, with an error containing exception_regex"""
    try:
      client.fetch(query, handle)
      assert False
    except Exception, e:
      assert re.search(exception_regex, str(e))

  def __expect_client_state(self, client, handle, expected_state, timeout=0.1):
    """Try to fetch 'expected_state' from 'client' within 'timeout' seconds.
    Fail if unable."""
    start_time = time()
    actual_state = None
    while (time() - start_time < timeout):
      actual_state = client.get_state(handle)
      if actual_state == expected_state:
        break
    assert expected_state == actual_state

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=1")
  def test_concurrent_query_expiration(self, vector):
    """Confirm that multiple concurrent queries are correctly expired if not fetched"""
    class ExpiringQueryThread(threading.Thread):
      """Thread that runs a query and does not fetch so it will time out."""
      def __init__(self, client):
        super(ExpiringQueryThread, self).__init__()
        self.client = client
        self.success = False

      def run(self):
        self.handle = self.client.execute_async("SELECT SLEEP(3000000)")

    class NonExpiringQueryThread(threading.Thread):
      """Thread that runs a query that does not hit the idle timeout."""
      def __init__(self, client):
        super(NonExpiringQueryThread, self).__init__()
        self.client = client
        self.success = False

      def run(self):
        result = self.client.execute("SELECT SLEEP(2500)")
        self.success = result.success

    class TimeLimitThread(threading.Thread):
      """Thread that runs a query that hits a time limit."""
      def __init__(self, client):
        super(TimeLimitThread, self).__init__()
        self.client = client
        self.success = False

      def run(self):
        # Query will not be idle but will hit time limit.
        self.client.execute("SET EXEC_TIME_LIMIT_S=1")
        try:
          result = self.client.execute("SELECT SLEEP(2500)")
          assert "Expected to hit time limit"
        except Exception, e:
          self.exception = e

    class NonExpiringTimeLimitThread(threading.Thread):
      """Thread that runs a query that finishes before time limit."""
      def __init__(self, client):
        super(NonExpiringTimeLimitThread, self).__init__()
        self.client = client
        self.success = False
        self.data = None

      def run(self):
        # Query will complete before time limit.
        self.client.execute("SET EXEC_TIME_LIMIT_S=10")
        result = self.client.execute("SELECT count(*) FROM functional.alltypes")
        self.success = result.success
        self.data = result.data

    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    non_expiring_threads = \
        [NonExpiringQueryThread(impalad.service.create_beeswax_client())
         for _ in xrange(5)]
    expiring_threads = [ExpiringQueryThread(impalad.service.create_beeswax_client())
                        for _ in xrange(5)]
    time_limit_threads = [TimeLimitThread(impalad.service.create_beeswax_client())
                        for _ in xrange(5)]
    non_expiring_time_limit_threads = [
        NonExpiringTimeLimitThread(impalad.service.create_beeswax_client())
        for _ in xrange(5)]
    all_threads = non_expiring_threads + expiring_threads + time_limit_threads +\
        non_expiring_time_limit_threads
    for t in all_threads:
      t.start()
    for t in all_threads:
      t.join()
    impalad.service.wait_for_metric_value('impala-server.num-queries-expired',
                                          num_expired + 10)
    for t in non_expiring_threads:
      assert t.success
    for t in expiring_threads:
      self.__expect_client_state(client, t.handle, client.QUERY_STATES['EXCEPTION'])
    for t in time_limit_threads:
      assert re.search(
          "Query [0-9a-f]+:[0-9a-f]+ expired due to execution time limit of 1s000ms",
          str(t.exception))
    for t in non_expiring_time_limit_threads:
      assert t.success
      assert t.data[0] == '7300' # Number of rows in alltypes.
