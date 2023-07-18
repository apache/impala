
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

from __future__ import absolute_import, division, print_function
from builtins import range
import logging
import os
import pytest
import re
import sys
import uuid
import socket

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite
from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfBuildType
from tests.common.impala_test_suite import ImpalaTestSuite
from time import sleep

from Types.ttypes import TNetworkAddress
from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

import StatestoreService.StatestoreSubscriber as Subscriber
import StatestoreService.StatestoreService as Statestore
import CatalogService.CatalogService as Catalog
from ErrorCodes.ttypes import TErrorCode

LOG = logging.getLogger('custom_statestore_test')
STATESTORE_SERVICE_PORT = 24000
CATALOG_SERVICE_PORT = 26000

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

  def __get_protocol_version(self):
    request = Subscriber.TGetProtocolVersionRequest(
        protocol_version=Subscriber.StatestoreServiceVersion.V2)
    client_transport = TTransport.TBufferedTransport(
        TSocket.TSocket('localhost', STATESTORE_SERVICE_PORT))
    trans_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = Statestore.Client(trans_protocol)
    client_transport.open()
    try:
      return client.GetProtocolVersion(request)
    except Exception as e:
      # For statestore with StatestoreServiceVersion.V1, 'GetProtocolVersion' is not
      # supported. Exception "Invalid method name: 'GetProtocolVersion'" is thrown
      # by Thrift client.
      assert False, str(e)

  def __register_subscriber(self, ss_protocol=Subscriber.StatestoreServiceVersion.V2,
                            in_v2_format=True, expect_exception=False):
    subscriber_id = "python-test-client-%s" % uuid.uuid4()
    topics = []
    if (ss_protocol == Subscriber.StatestoreServiceVersion.V1 and not in_v2_format):
      request = Subscriber.TRegisterSubscriberRequest(topic_registrations=topics,
          protocol_version=ss_protocol,
          subscriber_location=TNetworkAddress("localhost", self.port),
          subscriber_id=subscriber_id)
    else:
      request = Subscriber.TRegisterSubscriberRequest(topic_registrations=topics,
          protocol_version=ss_protocol,
          subscriber_location=TNetworkAddress("localhost", self.port),
          subscriber_type=Subscriber.TStatestoreSubscriberType.COORDINATOR_EXECUTOR,
          subscriber_id=subscriber_id)
    client_transport = TTransport.TBufferedTransport(
        TSocket.TSocket('localhost', STATESTORE_SERVICE_PORT))
    trans_protocol = TBinaryProtocol.TBinaryProtocol(client_transport)
    client = Statestore.Client(trans_protocol)
    client_transport.open()
    try:
      response = client.RegisterSubscriber(request)
      assert not expect_exception
      return response
    except Exception as e:
      assert expect_exception, str(e)

  @CustomClusterTestSuite.with_args(statestored_args="-statestore_max_subscribers=3")
  def test_statestore_max_subscribers(self):
    """Test that the statestored correctly handles the condition where the number
    of subscribers exceeds FLAGS_statestore_max_subscribers
    (see be/src/statestore/statestore.cc). The expected behavior is for the
    statestored to reject the subscription request once the threshold is
    exceeded."""
    # With a statestore_max_subscribers of 3, we should hit the registration error
    # pretty quick.
    for x in range(20):
      response = self.__register_subscriber()
      if response.status.status_code == TErrorCode.OK:
        self.registration_id = response.registration_id
        LOG.log(logging.INFO, "Registration id %s, x=%d" % (response.registration_id, x))
      else:
        assert 'Maximum subscriber limit reached:' in ''.join(response.status.error_msgs)
        return

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      impalad_args="--statestore_subscriber_use_resolved_address=true",
      catalogd_args="--statestore_subscriber_use_resolved_address=true")
  def test_subscriber_with_resolved_address(self, vector):
    # Ensure cluster has started up by running a query.
    result = self.execute_query("select count(*) from functional_parquet.alltypes")
    assert result.success, str(result)

    self.assert_impalad_log_contains("INFO",
        "Registering with statestore with resolved address")
    self.assert_catalogd_log_contains("INFO",
        "Registering with statestore with resolved address")

  @pytest.mark.execute_serially
  def test_statestore_compatible_subscriber(self):
    # Get protocol version of statestore.
    response = self.__get_protocol_version()
    assert response.status.status_code == TErrorCode.OK
    assert response.protocol_version == Subscriber.StatestoreServiceVersion.V2

    # Register compatible subscriber.
    response = self.__register_subscriber()
    # Verify response of normal registration for compatible subscriber.
    assert response.status.status_code == TErrorCode.OK
    assert response.protocol_version == Subscriber.StatestoreServiceVersion.V2
    assert response.catalogd_registration.protocol == Catalog.CatalogServiceVersion.V2
    assert response.catalogd_registration.address.port == CATALOG_SERVICE_PORT

  @pytest.mark.execute_serially
  @CustomClusterTestSuite.with_args(
      statestored_args="--debug_actions=START_STATESTORE_IN_PROTOCOL_V1")
  def test_statestore_incompatible_statestore(self):
    """Test that the old statestored refuses the registration request from new
    incompatible subscriber."""
    self.__register_subscriber(ss_protocol=Subscriber.StatestoreServiceVersion.V2,
        in_v2_format=True, expect_exception=True)

  @pytest.mark.execute_serially
  def test_statestore_incompatible_subscriber(self):
    """Test that the new statestored refuses the registration request from old
    incompatible subscriber."""
    response = self.__register_subscriber(
        ss_protocol=Subscriber.StatestoreServiceVersion.V1,
        in_v2_format=True, expect_exception=False)
    assert response.status.status_code == TErrorCode.STATESTORE_INCOMPATIBLE_PROTOCOL

    self.__register_subscriber(ss_protocol=Subscriber.StatestoreServiceVersion.V1,
        in_v2_format=False, expect_exception=True)

  def test_subscriber_fail_on_startup_register_failure(self):
    """The impalads are not expected to be able to tolerate the failure of registration
    on startup without starting flags FLAGS_tolerate_statestore_startup_delay set as
    true."""
    try:
      self._start_impala_cluster(
          ["--impalad_args=--debug_actions=REGISTER_STATESTORE_ON_STARTUP:FAIL@1.0"])
      assert False, "cluster startup should have failed"
    except Exception:
      self._stop_impala_cluster()

  @CustomClusterTestSuite.with_args(
    impalad_args="--debug_actions=REGISTER_STATESTORE_ON_STARTUP:FAIL@1.0 "
                 "--tolerate_statestore_startup_delay=true")
  def test_subscriber_tolerate_startup_register_failure(self):
    """The impalads are expected to be able to tolerate the failure of registration
    on startup with starting flags FLAGS_tolerate_statestore_startup_delay set as
    true.
    During rolling upgrade, registration failures could be caused by incompatible
    protocols between subscribers and statestore. For example, executors or coordinators
    are upgraded before statestore."""
    self.execute_query("select count(*) from functional.alltypes")

    # Verify that impalad entered recovery mode and tried to re-register with statestore.
    re_register_attempt = self.cluster.impalads[0].service.get_metric_value(
        "statestore-subscriber.num-re-register-attempt")
    assert re_register_attempt > 0


class TestStatestoreStartupDelay(CustomClusterTestSuite):
  """This test injects a real delay in statestored startup. The impalads and catalogd are
  expected to be able to tolerate this delay with FLAGS_tolerate_statestore_startup_delay
  set as true. This is not testing anything beyond successful startup."""

  @classmethod
  def get_workload(self):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('Statestore startup delay tests only run in exhaustive')
    super(TestStatestoreStartupDelay, cls).setup_class()

  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(
    impalad_args="--tolerate_statestore_startup_delay=true",
    catalogd_args="--tolerate_statestore_startup_delay=true",
    statestored_args="--stress_statestore_startup_delay_ms=60000")
  def test_subscriber_tolerate_startup_delay(self):
    """The impalads and catalogd are expected to be able to tolerate the delay of
    statestored startup with starting flags FLAGS_tolerate_statestore_startup_delay
    set as true."""
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before the coordinator and catalogd finish
    # starting up.
    self.execute_query("select count(*) from functional.alltypes")

    # Verify that impalad and catalogd entered recovery mode and tried to re-register
    # with statestore.
    re_register_attempt = self.cluster.impalads[0].service.get_metric_value(
        "statestore-subscriber.num-re-register-attempt")
    assert re_register_attempt > 0
    re_register_attempt = self.cluster.catalogd.service.get_metric_value(
        "statestore-subscriber.num-re-register-attempt")
    assert re_register_attempt > 0

  @SkipIfBuildType.not_dev_build
  def test_subscriber_fail_on_startup_delay(self):
    """The impalads and catalogd are not expected to be able to tolerate the delay of
    statestored startup without starting flags FLAGS_tolerate_statestore_startup_delay
    set as true."""
    try:
      self._start_impala_cluster(
          ["--state_store_args=--stress_statestore_startup_delay_ms=60000"])
      assert False, "cluster startup should have failed"
    except Exception:
      self._stop_impala_cluster()

  @CustomClusterTestSuite.with_args(
    statestored_args="--statestore_heartbeat_frequency_ms=10000")
  def test_subscriber_tolerate_restart_catalog(self):
    """Restart catalogd and verify update-catalogd RPCs are sent by statestore
    """
    statestore_service = self.cluster.statestored.service
    start_sent_rpc_count = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    impalad_service = self.cluster.impalads[0].service
    start_recv_rpc_count = impalad_service.get_metric_value(
        "statestore-subscriber.num-update-catalogd-rpc")

    # Restart catalog daemon.
    self.cluster.catalogd.restart()
    wait_time_s = build_flavor_timeout(15, slow_build_timeout=30)
    sleep(wait_time_s)
    # Verify update-catalogd RPCs are sent to coordinators from statestore.
    end_sent_rpc_count = statestore_service.get_metric_value(
        "statestore.num-successful-update-catalogd-rpc")
    assert end_sent_rpc_count > start_sent_rpc_count
    end_recv_rpc_count = impalad_service.get_metric_value(
        "statestore-subscriber.num-update-catalogd-rpc")
    assert end_recv_rpc_count > start_recv_rpc_count

  @SkipIfBuildType.not_dev_build
  @CustomClusterTestSuite.with_args(
    catalogd_args="--stress_catalog_startup_delay_ms=60000")
  def test_subscriber_tolerate_catalogd_startup_delay(self):
    """The impalads are expected to be able to tolerate the delay of catalog startup.
    """
    # The actual test here is successful startup, and we assume nothing about the
    # functionality of the impalads before catalogd finish starting up.
    self.execute_query("select count(*) from functional.alltypes")

    # Verify that impalad received notification of updating catalogd.
    recv_rpc_count = self.cluster.impalads[0].service.get_metric_value(
        "statestore-subscriber.num-update-catalogd-rpc")
    assert recv_rpc_count > 0
