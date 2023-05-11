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
from builtins import range
from collections import defaultdict
import json
import logging
from random import randint
import socket
import threading
import traceback
import time
import uuid

try:
  from urllib.request import urlopen
except ImportError:
  from urllib2 import urlopen

from Types.ttypes import TNetworkAddress
from thrift.protocol import TBinaryProtocol
from thrift.server.TServer import TServer
from thrift.transport import TSocket
from thrift.transport import TTransport

import StatestoreService.StatestoreSubscriber as Subscriber
import StatestoreService.StatestoreService as Statestore
from StatestoreService.StatestoreSubscriber import TUpdateStateResponse
from StatestoreService.StatestoreSubscriber import TTopicRegistration
from ErrorCodes.ttypes import TErrorCode
from Status.ttypes import TStatus

from tests.common.environ import build_flavor_timeout
from tests.common.skip import SkipIfDockerizedCluster

LOG = logging.getLogger('test_statestore')

# Tests for the statestore. The StatestoreSubscriber class is a skeleton implementation of
# a Python-based statestore subscriber with additional hooks to allow testing. Each
# StatestoreSubscriber runs its own server so that the statestore may contact it.
#
# All tests in this file may be run in parallel. They assume that a statestore instance is
# already running, and is configured with out-of-the-box defaults (as is the case in our
# usual test environment) which govern failure-detector timeouts etc.
#
# These tests do not yet provide sufficient coverage.
#    If no topic entries, do the first and second subscribers always get a callback?
#    Adding topic entries to non-existant topic
#    Test for from_version and to_version behavior
#    Test with many concurrent subscribers
#    Test that only the subscribed-to topics are sent
#    Test that topic deletions take effect correctly.

def get_statestore_subscribers(host='localhost', port=25010):
  response = urlopen("http://{0}:{1}/subscribers?json".format(host, port))
  page = response.read()
  return json.loads(page)

STATUS_OK = TStatus(TErrorCode.OK)
DEFAULT_UPDATE_STATE_RESPONSE = TUpdateStateResponse(status=STATUS_OK, topic_updates=[],
                                                     skipped=False)

# IMPALA-3501: the timeout needs to be higher in code coverage builds
WAIT_FOR_FAILURE_TIMEOUT = build_flavor_timeout(40, code_coverage_build_timeout=60)
WAIT_FOR_HEARTBEAT_TIMEOUT = build_flavor_timeout(
    40, code_coverage_build_timeout=60)
WAIT_FOR_UPDATE_TIMEOUT = build_flavor_timeout(40, code_coverage_build_timeout=60)

class WildcardServerSocket(TSocket.TSocketBase, TTransport.TServerTransportBase):
  """Specialised server socket that binds to a random port at construction"""
  def __init__(self, host=None, port=0):
    self.host = host
    self.handle = None
    self.handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.handle.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.handle.bind(('localhost', 0))
    _, self.port = self.handle.getsockname()

  def listen(self):
    self.handle.listen(128)

  def accept(self):
    client, addr = self.handle.accept()
    result = TSocket.TSocket()
    result.setHandle(client)
    return result


class KillableThreadedServer(TServer):
  """Based on TServer.TThreadedServer, this server may be shutdown (by calling
  shutdown()), after which no new connections may be made. Most of the implementation is
  directly copied from Thrift."""
  def __init__(self, *args, **kwargs):
    TServer.__init__(self, *args)
    self.daemon = kwargs.get("daemon", False)
    self.is_shutdown = False
    self.port = self.serverTransport.port

  def shutdown(self):
    self.is_shutdown = True
    self.serverTransport.close()
    self.wait_until_down()
    # The processor contains a reference to a StatestoreSubscriber. Clean up that
    # reference to avoid a circular reference that would prevent object deletion.
    self.processor = None

  def wait_until_up(self, num_tries=10):
    for i in range(num_tries):
      cnxn = TSocket.TSocket('localhost', self.port)
      try:
        cnxn.open()
        return
      except Exception as e:
        if i == num_tries - 1: raise
      time.sleep(0.1)

  def wait_until_down(self, num_tries=10):
    for i in range(num_tries):
      cnxn = TSocket.TSocket('localhost', self.port)
      try:
        cnxn.open()
        time.sleep(0.1)
      except Exception as e:
        return
    raise Exception("Server did not stop")

  def serve(self):
    self.serverTransport.listen()
    while not self.is_shutdown:
      client = self.serverTransport.accept()
      # Since accept() can take a while, check again if the server is shutdown to avoid
      # starting an unnecessary thread.
      if self.is_shutdown: return
      t = threading.Thread(target=self.handle, args=(client,))
      t.setDaemon(self.daemon)
      t.start()

  def handle(self, client):
    itrans = self.inputTransportFactory.getTransport(client)
    otrans = self.outputTransportFactory.getTransport(client)
    iprot = self.inputProtocolFactory.getProtocol(itrans)
    oprot = self.outputProtocolFactory.getProtocol(otrans)
    try:
      while not self.is_shutdown:
        self.processor.process(iprot, oprot)
    except TTransport.TTransportException as tx:
      pass
    except Exception as x:
      print(x)

    itrans.close()
    otrans.close()

class StatestoreSubscriber(object):
  """A bare-bones subscriber skeleton. Tests should create a new StatestoreSubscriber(),
  call start() and then register(). The subscriber will run a Thrift server on an unused
  port, and after registration the statestore will call Heartbeat() and UpdateState() via
  RPC. Tests can provide callbacks to the constructor that will be called during those
  RPCs, and this is the easiest way to check that the statestore protocol is being
  correctly followed. Tests should use wait_for_* methods to confirm that some event (like
  an RPC call) has happened asynchronously.

  Since RPC callbacks will execute on a different thread from the main one, any assertions
  there will not trigger a test failure without extra plumbing. What we do is simple: any
  exceptions during an RPC are caught and stored, and the check_thread_exceptions() method
  will re-raise them.

  The methods that may be called by a test deliberately return 'self' to allow for
  chaining, see test_failure_detected() for an example of how this makes the test flow
  more readable."""
  def __init__(self, heartbeat_cb=None, update_cb=None):
    self.heartbeat_event, self.heartbeat_count = threading.Condition(), 0
    # Track the number of updates received per topic.
    self.update_counts = defaultdict(lambda : 0)
    # Variables to notify for updates on each topic.
    self.update_event = threading.Condition()
    self.heartbeat_cb, self.update_cb = heartbeat_cb, update_cb
    self.subscriber_id = "python-test-client-%s" % uuid.uuid1()
    self.exception = None

  def __enter__(self):
    return self

  def __exit__(self, *args):
    self.kill()
    self.wait_for_failure()

  def Heartbeat(self, args):
    """Heartbeat RPC handler. Calls heartbeat callback if one exists."""
    self.heartbeat_event.acquire()
    try:
      self.heartbeat_count += 1
      response = Subscriber.THeartbeatResponse(status=STATUS_OK)
      if self.heartbeat_cb is not None and self.exception is None:
        try:
          response = self.heartbeat_cb(self, args)
        except Exception as e:
          self.exception = e
      self.heartbeat_event.notify()
    finally:
      self.heartbeat_event.release()
    return response

  def UpdateState(self, args):
    """UpdateState RPC handler. Calls update callback if one exists."""
    self.update_event.acquire()
    try:
      for topic_name in args.topic_deltas: self.update_counts[topic_name] += 1
      response = DEFAULT_UPDATE_STATE_RESPONSE
      if self.update_cb is not None and self.exception is None:
        try:
          response = self.update_cb(self, args)
        except Exception as e:
          # Print the original backtrace so it doesn't get lost.
          traceback.print_exc()
          self.exception = e
      self.update_event.notify()
    finally:
      self.update_event.release()
    return response

  def __init_server(self):
    processor = Subscriber.Processor(self)
    transport = WildcardServerSocket()
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    self.server = KillableThreadedServer(processor, transport, tfactory, pfactory,
                                         daemon=True)
    self.server_thread = threading.Thread(target=self.server.serve)
    self.server_thread.setDaemon(True)
    self.server_thread.start()
    self.server.wait_until_up()
    self.port = self.server.port

  def __init_client(self):
    self.client_transport = \
        TTransport.TBufferedTransport(TSocket.TSocket('localhost', 24000))
    self.protocol = TBinaryProtocol.TBinaryProtocol(self.client_transport)
    self.client = Statestore.Client(self.protocol)
    self.client_transport.open()

  def check_thread_exceptions(self):
    """Checks if an exception was raised and stored in a callback thread"""
    if self.exception is not None: raise self.exception

  def kill(self):
    """Closes both the server and client sockets, and waits for the server to become
    unavailable"""
    if self.client_transport:
      self.client_transport.close()
    if self.server:
      self.server.shutdown()
    return self

  def start(self):
    """Starts a subscriber server, and opens a client to the statestore. Returns only when
    the server is running."""
    self.__init_server()
    self.__init_client()
    return self

  def register(self, topics=None):
    """Call the Register() RPC"""
    if topics is None: topics = []
    request = Subscriber.TRegisterSubscriberRequest(
      topic_registrations=topics,
      subscriber_location=TNetworkAddress("localhost", self.port),
      subscriber_type=Subscriber.TStatestoreSubscriberType.COORDINATOR_EXECUTOR,
      subscriber_id=self.subscriber_id)
    response = self.client.RegisterSubscriber(request)
    if response.status.status_code == TErrorCode.OK:
      self.registration_id = response.registration_id
    else:
      raise Exception("Registration failed: %s, %s" %
                      (response.status.status_code,
                       '\n'.join(response.status.error_msgs)))
    return self

  def wait_for_heartbeat(self, count=None):
    """Waits for some number of heartbeats. If 'count' is provided, waits until the number
    of heartbeats seen by this subscriber exceeds count, otherwise waits for one further
    heartbeat."""
    self.heartbeat_event.acquire()
    try:
      if count is not None and self.heartbeat_count >= count: return self
      if count is None: count = self.heartbeat_count + 1
      while count > self.heartbeat_count:
        self.check_thread_exceptions()
        last_count = self.heartbeat_count
        self.heartbeat_event.wait(WAIT_FOR_HEARTBEAT_TIMEOUT)
        if last_count == self.heartbeat_count:
          raise Exception(
              "Heartbeat not received within {0}s (heartbeat count: {1})".format(
                WAIT_FOR_HEARTBEAT_TIMEOUT, self.heartbeat_count))
      self.check_thread_exceptions()
      return self
    finally:
      self.heartbeat_event.release()

  def wait_for_update(self, topic_name, count=None):
    """Waits for some number of updates of 'topic_name'. If 'count' is provided, waits
    until the number updates seen by this subscriber exceeds count, otherwise waits
    for one further update."""
    self.update_event.acquire()
    start_time = time.time()
    try:
      if count is not None and self.update_counts[topic_name] >= count: return self
      if count is None: count = self.update_counts[topic_name] + 1
      while count > self.update_counts[topic_name]:
        self.check_thread_exceptions()
        last_count = self.update_counts[topic_name]
        self.update_event.wait(WAIT_FOR_UPDATE_TIMEOUT)
        if (time.time() > start_time + WAIT_FOR_UPDATE_TIMEOUT and
              last_count == self.update_counts[topic_name]):
          raise Exception(
              "Update not received for {0} within {1} (update count: {2})".format(
                topic_name, WAIT_FOR_UPDATE_TIMEOUT, last_count))
      self.check_thread_exceptions()
      return self
    finally:
      self.update_event.release()


  def wait_for_failure(self, timeout=WAIT_FOR_FAILURE_TIMEOUT):
    """Waits until this subscriber no longer appears in the statestore's subscriber
    list. If 'timeout' seconds pass, throws an exception."""
    start = time.time()
    while True:
      subs = [s["id"] for s in get_statestore_subscribers()["subscribers"]]
      if self.subscriber_id not in subs: return self
      if time.time() - start > timeout:
        raise Exception("Subscriber {0} did not fail in {1}s".format(
            self.subscriber_id, timeout))
      time.sleep(0.2)


@SkipIfDockerizedCluster.statestore_not_exposed
class TestStatestore():
  def make_topic_update(self, topic_name, key_template="foo", value_template="bar",
                        num_updates=1, clear_topic_entries=False):
    topic_entries = [
      Subscriber.TTopicItem(key=key_template + str(x), value=value_template + str(x))
      for x in range(num_updates)]
    return Subscriber.TTopicDelta(topic_name=topic_name,
                                  topic_entries=topic_entries,
                                  is_delta=False,
                                  clear_topic_entries=clear_topic_entries)

  def test_registration_ids_different(self):
    """Test that if a subscriber with the same id registers twice, the registration ID is
    different"""
    with StatestoreSubscriber() as sub:
      sub.start().register()
      old_reg_id = sub.registration_id
      sub.register()
      assert old_reg_id != sub.registration_id

  def test_receive_heartbeats(self):
    """Smoke test to confirm that heartbeats get sent to a correctly registered
    subscriber"""
    with StatestoreSubscriber() as sub:
      sub.start().register().wait_for_heartbeat(5)

  def test_receive_updates(self):
    """Test that updates are correctly received when a subscriber alters a topic"""
    topic_name = "topic_delta_%s" % uuid.uuid1()

    def topic_update_correct(sub, args):
      delta = self.make_topic_update(topic_name)
      update_count = sub.update_counts[topic_name]
      if topic_name not in args.topic_deltas:
        # The update doesn't contain our topic.
        pass
      elif update_count == 1:
        return TUpdateStateResponse(status=STATUS_OK, topic_updates=[delta],
                                    skipped=False)
      elif update_count == 2:
        assert len(args.topic_deltas) == 1, args.topic_deltas
        assert args.topic_deltas[topic_name].topic_entries == delta.topic_entries
        assert args.topic_deltas[topic_name].topic_name == delta.topic_name
      elif update_count == 3:
        # After the content-bearing update was processed, the next delta should be empty
        assert len(args.topic_deltas[topic_name].topic_entries) == 0

      return DEFAULT_UPDATE_STATE_RESPONSE

    with StatestoreSubscriber(update_cb=topic_update_correct) as sub:
      reg = TTopicRegistration(topic_name=topic_name, is_transient=False)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 3)
      )

  def test_filter_prefix(self):
    topic_name = "topic_delta_%s" % uuid.uuid1()

    def topic_update_correct(sub, args):
      foo_delta = self.make_topic_update(topic_name, num_updates=1)
      bar_delta = self.make_topic_update(topic_name, num_updates=2, key_template='bar')

      update_count = sub.update_counts[topic_name]
      if topic_name not in args.topic_deltas:
        # The update doesn't contain our topic.
        pass
      elif update_count == 1:
        # Send some values with both prefixes.
        return TUpdateStateResponse(status=STATUS_OK,
                                    topic_updates=[foo_delta, bar_delta],
                                    skipped=False)
      elif update_count == 2:
        # We should only get the 'bar' entries back.
        assert len(args.topic_deltas) == 1, args.topic_deltas
        assert args.topic_deltas[topic_name].topic_entries == bar_delta.topic_entries
        assert args.topic_deltas[topic_name].topic_name == bar_delta.topic_name
      elif update_count == 3:
        # Send some more updates that only have 'foo' prefixes.
        return TUpdateStateResponse(status=STATUS_OK,
                                    topic_updates=[foo_delta],
                                    skipped=False)
      elif update_count == 4:
        # We shouldn't see any entries from the above update, but we should still see
        # the version number change due to the new entries in the topic.
        assert len(args.topic_deltas[topic_name].topic_entries) == 0
        assert args.topic_deltas[topic_name].from_version == 3
        assert args.topic_deltas[topic_name].to_version == 4
      elif update_count == 5:
        # After the content-bearing update was processed, the next delta should be empty
        assert len(args.topic_deltas[topic_name].topic_entries) == 0
        assert args.topic_deltas[topic_name].from_version == 4
        assert args.topic_deltas[topic_name].to_version == 4

      return DEFAULT_UPDATE_STATE_RESPONSE

    with StatestoreSubscriber(update_cb=topic_update_correct) as sub:
      reg = TTopicRegistration(topic_name=topic_name, is_transient=False,
                               filter_prefix="bar")
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 5)
      )

  def test_update_is_delta(self):
    """Test that the 'is_delta' flag is correctly set. The first update for a topic should
    always not be a delta, and so should all subsequent updates until the subscriber says
    it has not skipped the update."""
    topic_name = "test_update_is_delta_%s" % uuid.uuid1()

    def check_delta(sub, args):
      update_count = sub.update_counts[topic_name]
      if topic_name not in args.topic_deltas:
        # The update doesn't contain our topic.
        pass
      elif update_count == 1:
        assert args.topic_deltas[topic_name].is_delta == False
        delta = self.make_topic_update(topic_name)
        return TUpdateStateResponse(status=STATUS_OK, topic_updates=[delta],
                                    skipped=False)
      elif update_count == 2:
        assert args.topic_deltas[topic_name].is_delta == False
      elif update_count == 3:
        assert args.topic_deltas[topic_name].is_delta == True
        assert len(args.topic_deltas[topic_name].topic_entries) == 0
        assert args.topic_deltas[topic_name].to_version == 1

      return DEFAULT_UPDATE_STATE_RESPONSE

    with StatestoreSubscriber(update_cb=check_delta) as sub:
      reg = TTopicRegistration(topic_name=topic_name, is_transient=False)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 3)
      )

  def test_skipped(self):
    """Test that skipping an update causes it to be resent"""
    topic_name = "test_skipped_%s" % uuid.uuid1()

    def check_skipped(sub, args):
      # Ignore responses that don't contain our topic.
      if topic_name not in args.topic_deltas: return DEFAULT_UPDATE_STATE_RESPONSE
      update_count = sub.update_counts[topic_name]
      if update_count == 1:
        update = self.make_topic_update(topic_name)
        return TUpdateStateResponse(status=STATUS_OK, topic_updates=[update],
                                    skipped=False)
      # All subsequent updates: set skipped=True and expected the full topic to be resent
      # every time
      assert args.topic_deltas[topic_name].is_delta == False
      assert len(args.topic_deltas[topic_name].topic_entries) == 1
      return TUpdateStateResponse(status=STATUS_OK, skipped=True)

    with StatestoreSubscriber(update_cb=check_skipped) as sub:
      reg = TTopicRegistration(topic_name=topic_name, is_transient=False)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 3)
      )

  def test_failure_detected(self):
    with StatestoreSubscriber() as sub:
      topic_name = "test_failure_detected"
      reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 1)
           .kill()
           .wait_for_failure()
       )

  def test_hung_heartbeat(self):
    """Test for IMPALA-1712: If heartbeats hang (which we simulate by sleeping for five
    minutes) the statestore should time them out every 3s and then eventually fail after
    40s (10 times (3 + 1), where the 1 is the inter-heartbeat delay)"""
    with StatestoreSubscriber(heartbeat_cb=lambda sub, args: time.sleep(300)) as sub:
      topic_name = "test_hung_heartbeat"
      reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 1)
           .wait_for_failure(timeout=60)
       )

  def test_intermittent_hung_heartbeats(self):
    """Heartbeats that occasionally time out should not cause a failure to be detected."""
    heartbeat_count = [0]  # Use array to allow mutating from inside callback.

    def heartbeat_cb(sub, args):
      heartbeat_count[0] += 1
      # Delay every second heartbeat.
      if (heartbeat_count[0] % 2 == 1):
        time.sleep(4)
      return Subscriber.THeartbeatResponse(status=STATUS_OK)

    with StatestoreSubscriber(heartbeat_cb=heartbeat_cb) as sub:
      topic_name = "test_intermittent_hung_heartbeats"
      reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
      (
        sub.start()
           .register(topics=[reg])
           .wait_for_update(topic_name, 30)
           .kill()
           .wait_for_failure()
       )

  def test_slow_subscriber(self):
    """Test for IMPALA-6644: This test kills a healthy subscriber and sleeps for multiple
    intervals of about 1 second each, this lets the heartbeats to the subscriber fail.
    It polls the subscribers page of the statestore to ensure that the
    'secs_since_heartbeat' field is updated with an acceptable value. This test only
    checks for a strictly increasing value since the actual value of time might depend
    on the system load. It stops polling the page once the subscriber is removed from
    the set of active subscribers. It also checks that a valid heartbeat record of the
    subscriber is found at least once."""
    sub = StatestoreSubscriber()
    sub.start().register().wait_for_heartbeat(1)
    sub.kill()
    # secs_since_heartbeat is initially unknown.
    secs_since_heartbeat = -1
    valid_heartbeat_record = False
    while secs_since_heartbeat != 0:
      sleep_start_time = time.time()
      while time.time() - sleep_start_time < 1:
        time.sleep(0.1)
      prev_secs_since_heartbeat = secs_since_heartbeat
      secs_since_heartbeat = 0
      subscribers = get_statestore_subscribers()["subscribers"]
      for s in subscribers:
        if str(s["id"]) == sub.subscriber_id:
          secs_since_heartbeat = float(s["secs_since_heartbeat"])
          assert (secs_since_heartbeat > prev_secs_since_heartbeat)
          valid_heartbeat_record = True
    assert valid_heartbeat_record

  def test_topic_persistence(self):
    """Test that persistent topic entries survive subscriber failure, but transent topic
    entries are erased when the associated subscriber fails"""
    topic_id = str(uuid.uuid1())
    persistent_topic_name = "test_topic_persistence_persistent_%s" % topic_id
    transient_topic_name = "test_topic_persistence_transient_%s" % topic_id

    def add_entries(sub, args):
      # None of, one or both of the topics may be in the update.
      updates = []
      if (persistent_topic_name in args.topic_deltas and
          sub.update_counts[persistent_topic_name] == 1):
        updates.append(self.make_topic_update(persistent_topic_name))

      if (transient_topic_name in args.topic_deltas and
          sub.update_counts[transient_topic_name] == 1):
        updates.append(self.make_topic_update(transient_topic_name))

      if len(updates) > 0:
        return TUpdateStateResponse(status=STATUS_OK, topic_updates=updates,
                                    skipped=False)
      return DEFAULT_UPDATE_STATE_RESPONSE

    def check_entries(sub, args):
      # None of, one or both of the topics may be in the update.
      if (persistent_topic_name in args.topic_deltas and
          sub.update_counts[persistent_topic_name] == 1):
        assert len(args.topic_deltas[persistent_topic_name].topic_entries) == 1
        # Statestore should not send deletions when the update is not a delta, see
        # IMPALA-1891
        assert args.topic_deltas[persistent_topic_name].topic_entries[0].deleted == False
      if (transient_topic_name in args.topic_deltas and
          sub.update_counts[persistent_topic_name] == 1):
        assert len(args.topic_deltas[transient_topic_name].topic_entries) == 0
      return DEFAULT_UPDATE_STATE_RESPONSE

    reg = [TTopicRegistration(topic_name=persistent_topic_name, is_transient=False),
           TTopicRegistration(topic_name=transient_topic_name, is_transient=True)]

    with StatestoreSubscriber(update_cb=add_entries) as sub:
      (
        sub.start()
           .register(topics=reg)
           .wait_for_update(persistent_topic_name, 2)
           .wait_for_update(transient_topic_name, 2)
           .kill()
           .wait_for_failure()
      )

    with StatestoreSubscriber(update_cb=check_entries) as sub2:
      (
         sub2.start()
             .register(topics=reg)
             .wait_for_update(persistent_topic_name, 1)
             .wait_for_update(transient_topic_name, 1)
       )

  def test_update_with_clear_entries_flag(self):
    """Test that the statestore clears all topic entries when a subscriber
    sets the clear_topic_entries flag in a topic update message (IMPALA-6948)."""
    topic_name = "test_topic_%s" % str(uuid.uuid1())

    def add_entries(sub, args):
      updates = []
      if (topic_name in args.topic_deltas and sub.update_counts[topic_name] == 1):
        updates.append(self.make_topic_update(topic_name, num_updates=2,
            key_template="old"))

      if (topic_name in args.topic_deltas and sub.update_counts[topic_name] == 2):
        updates.append(self.make_topic_update(topic_name, num_updates=1,
            key_template="new", clear_topic_entries=True))

      if len(updates) > 0:
        return TUpdateStateResponse(status=STATUS_OK, topic_updates=updates,
            skipped=False)

      return DEFAULT_UPDATE_STATE_RESPONSE

    def check_entries(sub, args):
      if (topic_name in args.topic_deltas and sub.update_counts[topic_name] == 1):
        assert len(args.topic_deltas[topic_name].topic_entries) == 1
        assert args.topic_deltas[topic_name].topic_entries[0].key == "new0"

      return DEFAULT_UPDATE_STATE_RESPONSE

    reg = [TTopicRegistration(topic_name=topic_name, is_transient=False)]
    with StatestoreSubscriber(update_cb=add_entries) as sub1:
      (
        sub1.start()
            .register(topics=reg)
            .wait_for_update(topic_name, 1)
            .kill()
            .wait_for_failure()
            .start()
            .register(topics=reg)
            .wait_for_update(topic_name, 2)
      )

    with StatestoreSubscriber(update_cb=check_entries) as sub2:
      (
        sub2.start()
            .register(topics=reg)
            .wait_for_update(topic_name, 2)
      )

  def test_heartbeat_failure_reset(self):
    """Regression test for IMPALA-6785: the heartbeat failure count for the subscriber ID
    should be reset when it resubscribes, not after the first successful heartbeat. Delay
    the heartbeat to force the topic update to finish first."""

    with StatestoreSubscriber(heartbeat_cb=lambda sub, args: time.sleep(0.5)) as sub:
      topic_name = "test_heartbeat_failure_reset"
      reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
      sub.start()
      sub.register(topics=[reg])
      LOG.info("Registered with id {0}".format(sub.subscriber_id))
      sub.wait_for_heartbeat(1)
      sub.kill()
      LOG.info("Killed, waiting for statestore to detect failure via heartbeats")
      sub.wait_for_failure()
      # IMPALA-6785 caused only one topic update to be send. Wait for multiple updates to
      # be received to confirm that the subsequent updates are being scheduled repeatedly.
      target_updates = sub.update_counts[topic_name] + 5
      sub.start()
      sub.register(topics=[reg])
      LOG.info("Re-registered with id {0}, waiting for update".format(sub.subscriber_id))
      sub.wait_for_update(topic_name, target_updates)

  def test_min_subscriber_topic_version(self):
    self._do_test_min_subscriber_topic_version(False)

  def test_min_subscriber_topic_version_with_straggler(self):
    self._do_test_min_subscriber_topic_version(True)

  def _do_test_min_subscriber_topic_version(self, simulate_straggler):
    """Implementation of test that the 'min_subscriber_topic_version' flag is correctly
    set when requested. This tests runs two subscribers concurrently and tracks the
    minimum version each has processed. If 'simulate_straggler' is true, one subscriber
    rejects updates so that its version is not advanced."""
    topic_name = "test_min_subscriber_topic_version_%s" % uuid.uuid1()

    # This lock is held while processing the update to protect last_to_versions.
    update_lock = threading.Lock()
    last_to_versions = {}
    TOTAL_SUBSCRIBERS = 2
    def callback(sub, args, is_producer, sub_name):
      """Callback for subscriber to verify min_subscriber_topic_version behaviour.
      If 'is_producer' is true, this acts as the producer, otherwise it acts as the
      consumer. 'sub_name' is a name used to index into last_to_versions."""
      if topic_name not in args.topic_deltas:
        # The update doesn't contain our topic.
        pass
      with update_lock:
        LOG.info("{0} got update {1}".format(sub_name,
            repr(args.topic_deltas[topic_name])))
        LOG.info("Versions: {0}".format(last_to_versions))
        to_version = args.topic_deltas[topic_name].to_version
        from_version = args.topic_deltas[topic_name].from_version
        min_subscriber_topic_version = \
            args.topic_deltas[topic_name].min_subscriber_topic_version

        if is_producer:
          assert min_subscriber_topic_version is not None
          assert (to_version == 0 and min_subscriber_topic_version == 0) or\
              min_subscriber_topic_version < to_version,\
              "'to_version' hasn't been created yet by this subscriber."
          # Only validate version once all subscribers have processed an update.
          if len(last_to_versions) == TOTAL_SUBSCRIBERS:
            min_to_version = min(last_to_versions.values())
            assert min_subscriber_topic_version <= min_to_version,\
                "The minimum subscriber topic version seen by the producer cannot get " +\
                "ahead of the minimum version seem by the consumer, by definition."
            assert min_subscriber_topic_version >= min_to_version - 2,\
                "The min topic version can be two behind the last version seen by " + \
                "this subscriber because the updates for both subscribers are " + \
                "prepared in parallel and because it's possible that the producer " + \
                "processes two updates in-between consumer updates. This is not " + \
                "absolute but depends on updates not being delayed a large amount."
        else:
          # Consumer did not request topic version.
          assert min_subscriber_topic_version is None

        # Check the 'to_version' and update 'last_to_versions'.
        last_to_version = last_to_versions.get(sub_name, 0)
        if to_version > 0:
          # Non-empty update.
          assert from_version == last_to_version
        # Stragglers should accept the first update then skip later ones.
        skip_update = simulate_straggler and not is_producer and last_to_version > 0
        if not skip_update: last_to_versions[sub_name] = to_version

        if is_producer:
          delta = self.make_topic_update(topic_name)
          return TUpdateStateResponse(status=STATUS_OK, topic_updates=[delta],
                                      skipped=False)
        elif skip_update:
          return TUpdateStateResponse(status=STATUS_OK, topic_updates=[], skipped=True)
        else:
          return DEFAULT_UPDATE_STATE_RESPONSE

    # Two concurrent subscribers, which pushes out updates and checks the minimum
    # version, the other which just consumes the updates.
    def producer_callback(sub, args): return callback(sub, args, True, "producer")
    def consumer_callback(sub, args): return callback(sub, args, False, "consumer")
    with StatestoreSubscriber(update_cb=consumer_callback) as consumer_sub:
      with StatestoreSubscriber(update_cb=producer_callback) as producer_sub:
        consumer_reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
        producer_reg = TTopicRegistration(topic_name=topic_name, is_transient=True,
            populate_min_subscriber_topic_version=True)
        NUM_UPDATES = 6
        (
          consumer_sub.start()
                      .register(topics=[consumer_reg])
        )
        (
          producer_sub.start()
                      .register(topics=[producer_reg])
                      .wait_for_update(topic_name, NUM_UPDATES)
        )
        consumer_sub.wait_for_update(topic_name, NUM_UPDATES)

  def test_transient_entry_removal_race(self):
    """IMPALA-7306: transient entries were not deleted if the subscriber is unregistered
    while it is in the middle of a callback. This test exercises that case by blocking
    the update callback so that it is still running when the statestore unregisters the
    subscriber for failed heartbeats. It also confirms that non-transient entries are not
    removed."""
    transient_topic_name = "test_transient_entry_removal_race_transient"
    non_transient_topic_name = "test_transient_entry_removal_race_non_transient"
    topic_regs = [TTopicRegistration(topic_name=transient_topic_name, is_transient=True),
          TTopicRegistration(topic_name=non_transient_topic_name, is_transient=False)]
    # The heartbeat timeout is 3s, so sleep for long enough for it to expire
    HEARTBEAT_DELAY = 10

    def delayed_heartbeat(sub, args):
      LOG.info("Heartbeat callback called")
      time.sleep(HEARTBEAT_DELAY)
      LOG.debug("Heartbeat callback about to return")

    def add_transient_entries_after_hb_failure(sub, args):
      LOG.info("Update callback called")
      # Add an additional delay so that this returns after the heartbeat.
      time.sleep(WAIT_FOR_FAILURE_TIMEOUT)
      updates = [self.make_topic_update(transient_topic_name, "k", "v"),
                 self.make_topic_update(non_transient_topic_name, "k", "v")]
      LOG.debug("Update callback about to return")
      return TUpdateStateResponse(status=STATUS_OK, topic_updates=updates, skipped=False)

    # Subscriber with delay creates a transient entry, which should not be added since
    # the subscriber failed and was unregistered.
    with StatestoreSubscriber(heartbeat_cb=delayed_heartbeat,
                              update_cb=add_transient_entries_after_hb_failure) as sub:
      # Wait for the first update (which should happen after failure), then confirm
      # that the failure occurred.
      (
        sub.start()
           .register(topics=topic_regs)
           .wait_for_update(transient_topic_name, 1)
           .wait_for_failure(timeout=WAIT_FOR_FAILURE_TIMEOUT)
       )

    def verify_transient_entry_removed(sub, args):
      transient_delta = args.topic_deltas[transient_topic_name]
      assert len(transient_delta.topic_entries) == 0, args
      non_transient_delta = args.topic_deltas[non_transient_topic_name]
      # Non-transient update should include topic that was not removed
      assert len(non_transient_delta.topic_entries) == 1, args
      entry = non_transient_delta.topic_entries[0]
      assert entry.key == "k0"
      assert entry.value == "v0"
      assert not entry.deleted
      # Skip updates so that statestore will re-send non-transient entries and the above
      # assertions remain valid on subsequent callbacks.
      return TUpdateStateResponse(status=STATUS_OK, topic_updates=[], skipped=True)

    # Verify that the transient entry for the failed subscriber is not present.
    with StatestoreSubscriber(update_cb=verify_transient_entry_removed) as sub:
      (
        sub.start()
           .register(topics=topic_regs)
           .wait_for_update(transient_topic_name, 1)
       )
