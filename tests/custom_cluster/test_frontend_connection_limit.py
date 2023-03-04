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

from threading import Thread
from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException

# This custom cluster test exercises the behavior of the front end thrift
# server on how a new client connection request is handled, after the maximum
# number of front end service threads (--fe_service_threads) has been
# allocated. If "--accepted_client_cnxn_timeout" > 0, new connection
# requests are rejected if they wait in the accepted queue for more than the
# the specified timeout.
# See IMPALA-7800.


class TestFrontendConnectionLimit(CustomClusterTestSuite):

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    super(TestFrontendConnectionLimit, cls).add_test_dimensions()

  def _connect_and_query(self, query, impalad):
    client = impalad.service.create_beeswax_client()
    try:
      client.execute(query)
    except Exception as e:
      client.close()
      raise ImpalaBeeswaxException(str(e))
    client.close()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--fe_service_threads=1 --accepted_client_cnxn_timeout=0")
  def test_no_connection_is_rejected(self):
    """ IMPALA-7800: New connection request should not be rejected if
        --accepted_client_cnxn_timeout=0"""

    query = "select sleep(2000)"
    impalad = self.cluster.get_any_impalad()
    q1 = Thread(target=self._connect_and_query, args=(query, impalad,))
    q2 = Thread(target=self._connect_and_query, args=(query, impalad,))
    q1.start()
    q2.start()
    q1.join()
    q2.join()

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
    impalad_args="--fe_service_threads=1 --accepted_client_cnxn_timeout=5000")
  def test_server_busy(self):
    """ IMPALA-7800: Reject new incoming connections if --accepted_client_cnxn_timeout > 0
        and the request spent too much time waiting in the accepted queue."""

    client = self.create_impala_client()
    client.execute_async("select sleep(7000)")

    # This step should fail to open a session.
    # create_impala_client() does not throw an error on connection failure
    # The only way to detect the connection is invalid is to perform a
    # query in it
    client1 = self.create_impala_client()
    caught_exception = False
    try:
      client1.execute("select sleep(8000)")
    except Exception:
      caught_exception = True
    client.close()
    assert caught_exception, 'Query on client1 did not fail as expected'
