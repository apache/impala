# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tests for query expiration.

import pytest
import threading
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.custom_cluster_test_suite import NUM_SUBSCRIBERS, CLUSTER_SIZE
from time import sleep, time
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException

class TestQueryExpiration(CustomClusterTestSuite):
  """Tests query expiration logic"""

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=6")
  def test_query_expiration(self, vector):
    """Confirm that single queries expire if not fetched"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    handle = client.execute_async("SELECT SLEEP(1000000)")
    client.execute("SET QUERY_TIMEOUT_S=1")
    handle2 = client.execute_async("SELECT SLEEP(2000000)")

    # Set a huge timeout, to check that the server bounds it by --idle_query_timeout
    client.execute("SET QUERY_TIMEOUT_S=1000")
    handle3 = client.execute_async("SELECT SLEEP(3000000)")

    before = time()
    sleep(4)

    # Query with timeout of 1 should have expired, other query should still be running.
    assert num_expired + 1 == impalad.service.get_metric_value(
      'impala-server.num-queries-expired')
    impalad.service.wait_for_metric_value('impala-server.num-queries-expired',
                                          num_expired + 3)

    # Check that we didn't wait too long to be expired (double the timeout is sufficiently
    # large to avoid most noise in measurement)
    assert time() - before < 12
    assert client.get_state(handle) == client.QUERY_STATES['EXCEPTION']

    client.execute("SET QUERY_TIMEOUT_S=0")
    # Synchronous execution; calls fetch() and query should not time out.
    # Note: could be flakey if execute() takes too long to call fetch() etc after the
    # query completes.
    handle = client.execute("SELECT SLEEP(2500)")

    # Confirm that no extra expirations happened
    assert impalad.service.get_metric_value('impala-server.num-queries-expired') \
        == num_expired + 3

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=0")
  def test_query_expiration_no_default(self, vector):
    """Confirm that single queries expire if no default is set, but a per-query
    expiration is set"""
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    client.execute("SET QUERY_TIMEOUT_S=1")
    handle = client.execute_async("SELECT SLEEP(1000000)")

    # Set a huge timeout, server should not expire the query while this test is running
    client.execute("SET QUERY_TIMEOUT_S=1000")
    handle3 = client.execute_async("SELECT SLEEP(2000000)")

    before = time()
    sleep(4)

    # Query with timeout of 1 should have expired, other query should still be running.
    assert num_expired + 1 == impalad.service.get_metric_value(
      'impala-server.num-queries-expired')

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args("--idle_query_timeout=1")
  def test_concurrent_query_expiration(self, vector):
    """Confirm that multiple concurrent queries are correctly expired if not fetched"""
    class ExpiringQueryThread(threading.Thread):
      def __init__(self, client):
        super(ExpiringQueryThread, self).__init__()
        self.client = client
        self.success = False

      def run(self):
        self.handle = self.client.execute_async("SELECT SLEEP(3000000)")

    class NonExpiringQueryThread(threading.Thread):
      def __init__(self, client):
        super(NonExpiringQueryThread, self).__init__()
        self.client = client
        self.success = False

      def run(self):
        result = self.client.execute("SELECT SLEEP(2500)")
        self.success = result.success

    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    num_expired = impalad.service.get_metric_value('impala-server.num-queries-expired')
    non_expiring_threads = \
        [NonExpiringQueryThread(impalad.service.create_beeswax_client())
         for _ in xrange(5)]
    expiring_threads = [ExpiringQueryThread(impalad.service.create_beeswax_client())
                        for _ in xrange(5)]
    all_threads = zip(non_expiring_threads, expiring_threads)
    for n, e in all_threads:
      n.start()
      e.start()

    for n, e in all_threads:
      n.join()
      e.join()

    impalad.service.wait_for_metric_value('impala-server.num-queries-expired',
                                          num_expired + 5)

    for n, e in all_threads:
      assert n.success
      assert client.get_state(e.handle) == client.QUERY_STATES['EXCEPTION']
