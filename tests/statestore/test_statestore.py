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

from collections import defaultdict
import json
import socket
import threading
import traceback
import time
import urllib2
import uuid

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

from tests.common.environ import specific_build_type_timeout

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
  response = urllib2.urlopen("http://{0}:{1}/subscribers?json".format(host, port))
  page = response.read()
  return json.loads(page)

STATUS_OK = TStatus(TErrorCode.OK)
DEFAULT_UPDATE_STATE_RESPONSE = TUpdateStateResponse(status=STATUS_OK, topic_updates=[],
                                                     skipped=False)

# IMPALA-3501: the timeout needs to be higher in code coverage builds
WAIT_FOR_FAILURE_TIMEOUT = specific_build_type_timeout(40, code_coverage_build_timeout=60)

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

  def wait_until_up(self, num_tries=10):
    for i in xrange(num_tries):
      cnxn = TSocket.TSocket('localhost', self.port)
      try:
        cnxn.open()
        return
      except Exception, e:
        if i == num_tries - 1: raise
      time.sleep(0.1)

  def wait_until_down(self, num_tries=10):
    for i in xrange(num_tries):
      cnxn = TSocket.TSocket('localhost', self.port)
      try:
        cnxn.open()
        time.sleep(0.1)
      except Exception, e:
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
    except TTransport.TTransportException, tx:
      pass
    except Exception, x:
      print x

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
    self.exception = None

  def Heartbeat(self, args):
    """Heartbeat RPC handler. Calls heartbeat callback if one exists."""
    self.heartbeat_event.acquire()
    try:
      self.heartbeat_count += 1
      response = Subscriber.THeartbeatResponse()
      if self.heartbeat_cb is not None and self.exception is None:
        try:
          response = self.heartbeat_cb(self, args)
        except Exception, e:
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
        except Exception, e:
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
    self.client_transport.close()
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
    self.subscriber_id = "python-test-client-%s" % uuid.uuid1()
    if topics is None: topics = []
    request = Subscriber.TRegisterSubscriberRequest(
      topic_registrations=topics,
      subscriber_location=TNetworkAddress("localhost", self.port),
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
        self.heartbeat_event.wait(10)
        if last_count == self.heartbeat_count:
          raise Exception("Heartbeat not received within 10s (heartbeat count: %s)" %
                          self.heartbeat_count)
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
        self.update_event.wait(10)
        if (time.time() > start_time + 10 and
            last_count == self.update_counts[topic_name]):
          raise Exception("Update not received for %s within 10s (update count: %s)" %
                          (topic_name, last_count))
      self.check_thread_exceptions()
      return self
    finally:
      self.update_event.release()


  def wait_for_failure(self, timeout=WAIT_FOR_FAILURE_TIMEOUT):
    """Waits until this subscriber no longer appears in the statestore's subscriber
    list. If 'timeout' seconds pass, throws an exception."""
    start = time.time()
    while time.time() - start < timeout:
      subs = [s["id"] for s in get_statestore_subscribers()["subscribers"]]
      if self.subscriber_id not in subs: return self
      time.sleep(0.2)
    raise Exception("Subscriber %s did not fail in %ss" % (self.subscriber_id, timeout))

class TestStatestore():
  def make_topic_update(self, topic_name, key_template="foo", value_template="bar",
                        num_updates=1):
    topic_entries = [
      Subscriber.TTopicItem(key=key_template + str(x), value=value_template + str(x))
      for x in xrange(num_updates)]
    return Subscriber.TTopicDelta(topic_name=topic_name,
                                  topic_entries=topic_entries,
                                  is_delta=False)

  def test_registration_ids_different(self):
    """Test that if a subscriber with the same id registers twice, the registration ID is
    different"""
    sub = StatestoreSubscriber()
    sub.start().register()
    old_reg_id = sub.registration_id
    sub.register()
    assert old_reg_id != sub.registration_id

  def test_receive_heartbeats(self):
    """Smoke test to confirm that heartbeats get sent to a correctly registered
    subscriber"""
    sub = StatestoreSubscriber()
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

    sub = StatestoreSubscriber(update_cb=topic_update_correct)
    reg = TTopicRegistration(topic_name=topic_name, is_transient=False)
    (
      sub.start()
         .register(topics=[reg])
         .wait_for_update(topic_name, 3)
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

    sub = StatestoreSubscriber(update_cb=check_delta)
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

    sub = StatestoreSubscriber(update_cb=check_skipped)
    reg = TTopicRegistration(topic_name=topic_name, is_transient=False)
    (
      sub.start()
         .register(topics=[reg])
         .wait_for_update(topic_name, 3)
    )

  def test_failure_detected(self):
    sub = StatestoreSubscriber()
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
    sub = StatestoreSubscriber(heartbeat_cb=lambda sub, args: time.sleep(300))
    topic_name = "test_hung_heartbeat"
    reg = TTopicRegistration(topic_name=topic_name, is_transient=True)
    (
      sub.start()
         .register(topics=[reg])
         .wait_for_update(topic_name, 1)
         .wait_for_failure(timeout=60)
    )

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

    sub = StatestoreSubscriber(update_cb=add_entries)
    (
      sub.start()
         .register(topics=reg)
         .wait_for_update(persistent_topic_name, 2)
         .wait_for_update(transient_topic_name, 2)
         .kill()
         .wait_for_failure()
    )

    sub2 = StatestoreSubscriber(update_cb=check_entries)
    (
      sub2.start()
          .register(topics=reg)
          .wait_for_update(persistent_topic_name, 1)
          .wait_for_update(transient_topic_name, 1)
    )
