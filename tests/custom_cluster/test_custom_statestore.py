
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

# Tests statestore with non-default startup options

import logging
import os
import pytest
import re
import sys
import uuid
import socket

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.impala_test_suite import ImpalaTestSuite

from Types.ttypes import TNetworkAddress
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

import StatestoreService.StatestoreSubscriber as Subscriber
import StatestoreService.StatestoreService as Statestore
from ErrorCodes.ttypes import TErrorCode

LOG = logging.getLogger('custom_statestore_test')
STATESTORE_SERVICE_PORT = 24000

# A simple wrapper class to launch a cluster where we can tune various
# startup parameters of the statestored to test correct boundary-value
# behavior.
class TestCustomStatestore(CustomClusterTestSuite):
  # Grab a port the statestore subscribers will use to connect.
  # Note that all subscribers we create below use this port to connect,
  # with different subscriber IDs.
  handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
  handle.bind(('localhost', 0))
  _, port = handle.getsockname()

  @classmethod
  def get_workload(self):
    return 'functional-query'

  def __register_subscriber(self):
    subscriber_id = "python-test-client-%s" % uuid.uuid4()
    topics = []
    request = Subscriber.TRegisterSubscriberRequest(topic_registrations=topics,
      subscriber_location=TNetworkAddress("localhost", self.port),
      subscriber_id=subscriber_id)
    client_transport = \
      TTransport.TBufferedTransport(TSocket.TSocket('localhost', STATESTORE_SERVICE_PORT))
    protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = Statestore.Client(protocol)
    client_transport.open()
    return client.RegisterSubscriber(request)

  @CustomClusterTestSuite.with_args(statestored_args="-statestore_max_subscribers=3")
  def test_statestore_max_subscribers(self):
    """Test that the statestored correctly handles the condition where the number
    of subscribers exceeds FLAGS_statestore_max_subscribers
    (see be/src/statestore/statestore.cc). The expected behavior is for the
    statestored to reject the subscription request once the threshold is
    exceeded."""
    # With a statestore_max_subscribers of 3, we should hit the registration error
    # pretty quick.
    for x in xrange(20):
      response = self.__register_subscriber()
      if response.status.status_code == TErrorCode.OK:
        self.registration_id = response.registration_id
        LOG.log(logging.INFO, "Registration id %s, x=%d" % (response.registration_id, x))
      else:
        assert 'Maximum subscriber limit reached:' in ''.join(response.status.error_msgs)
        return
