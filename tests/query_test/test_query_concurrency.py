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
import time

from threading import Thread
from tests.common.impala_cluster import ImpalaCluster
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.skip import SkipIfBuildType


@SkipIfBuildType.not_dev_build
class TestQueryConcurrency(ImpalaTestSuite):
  """Tests if multiple queries are registered on the coordinator when
  submitted in parallel along with clients trying to access the web UI.
  The intention here is to check that the web server call paths don't hold
  global locks that can conflict with other requests and prevent the impalad
  from servicing them. It is done by simulating a metadata loading pause
  using the debug action 'EXECUTE_INTERNAL_REGISTERED' that
  makes the frontend hold the ClientRequestState::lock_ for longer duration."""

  TEST_QUERY = "select count(*) from tpch.supplier"
  POLLING_TIMEOUT_S = 15

  def poll_query_page(self, impalad, query_id):
    """Polls the debug plan page of a given query id in a loop till the timeout
    of POLLING_TIMEOUT_S is hit."""
    start = time.time()
    while time.time() - start < self.POLLING_TIMEOUT_S:
      try:
        impalad.service.read_debug_webpage("query_plan?query_id=" + query_id)
      except Exception:
        pass
      time.sleep(1)

  def check_registered_queries(self, impalad, count):
    """Asserts that the registered query count on a given impalad matches 'count'
    before POLLING_TIMEOUT_S is hit."""
    start = time.time()
    while time.time() - start < self.POLLING_TIMEOUT_S:
      inflight_query_ids = impalad.service.get_in_flight_queries()
      if inflight_query_ids is not None and len(inflight_query_ids) == count:
        return inflight_query_ids
      time.sleep(1)
    assert False, "Registered query count doesn't match: " + str(count)

  @pytest.mark.execute_serially
  def test_query_concurrency(self):
    if self.exploration_strategy() != 'exhaustive':
      pytest.skip('Runs only in exhaustive mode.')
    impalad = ImpalaCluster.get_e2e_test_cluster().get_any_impalad()
    # Inject 1 minute sleep right after "Query submitted" timeline shows up
    # and ClientRequestState::lock_ is being held.
    opts = {'debug_action': 'EXECUTE_INTERNAL_REGISTERED:SLEEP@60000'}
    client1 = impalad.service.create_hs2_client()
    client2 = impalad.service.create_hs2_client()
    q1 = Thread(target=self.execute_query_expect_success,
                args=(client1, self.TEST_QUERY, opts,))
    q2 = Thread(target=self.execute_query_expect_success,
                args=(client2, self.TEST_QUERY, opts,))
    q1.start()
    inflight_query_ids = self.check_registered_queries(impalad, 1)
    poll_thread = Thread(target=self.poll_query_page,
                         args=(impalad, inflight_query_ids[0]['query_id'],))
    poll_thread.start()
    time.sleep(2)
    q2.start()
    inflight_query_ids = self.check_registered_queries(impalad, 2)
    result = impalad.service.read_debug_webpage(
        "query_profile_encoded?query_id={}".format(inflight_query_ids[1]['query_id']))
    assert result.startswith("Could not obtain runtime profile")
    poll_thread.join()
    q1.join()
    q2.join()
