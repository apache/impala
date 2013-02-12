// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef STATESTORE_STATE_STORE_SUBSCRIBER_H
#define STATESTORE_STATE_STORE_SUBSCRIBER_H

#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "statestore/state-store.h"
#include "util/thrift-util.h"
#include "util/thrift-client.h"
#include "util/metrics.h"
#include "gen-cpp/StateStoreService.h"
#include "gen-cpp/StateStoreSubscriber.h"

namespace impala {

class TimeoutFailureDetector;
class Status;
class TNetworkAddress;
class ThriftServer;

// A StateStoreSubscriber communicates with a state-store periodically
// through the exchange of heartbeat messages. These messages contain
// updates from the state-store to a list of 'topics' that the
// subscriber is interested in; in response the subscriber sends a
// list of changes that it wishes to make to a topic.
//
// Clients of the subscriber register topics of interest, and a
// function to call once an update has been received. Each callback
// may optionally add one or more updates to a list of topic updates
// to be sent back to the state-store. See AddTopic for the
// requirements placed on these callbacks.
//
// Topics must be subscribed to before the subscriber is connected to
// the state-store: there is no way to add a new subscription after
// the subscriber has successfully registered.
//
// If the subscriber does not receive heartbeats from the state-store
// within a configurable period of time, the subscriber enters
// 'recovery mode', where it continually attempts to re-register with
// the state-store.
class StateStoreSubscriber {
 public:
  // Only constructor.
  //   subscriber_id - should be unique across the cluster, identifies this subscriber
  //
  //   heartbeat_address - the local address on which the heartbeat service which
  //                       communicates with the state-store should be started.
  //
  //   state_store_address - the address of the state-store to register with
  StateStoreSubscriber(const std::string& subscriber_id,
      const TNetworkAddress& heartbeat_address,
      const TNetworkAddress& state_store_address,
      Metrics* metrics);

  // A TopicDeltaMap is passed to each callback. See UpdateCallback for more details.
  typedef std::map<StateStore::TopicId, TTopicDelta> TopicDeltaMap;

  // Function called to update a service with new state. Called in a
  // separate thread to the one in which it is registered.
  //
  // Every UpdateCallback is invoked every time that an update is
  // received from the state-store. Therefore the callback should not
  // assume that the TopicDeltaMap contains an entry for their
  // particular topic of interest.
  //
  // If a delta for a particular topic does not have the 'is_delta'
  // flag set, clients should assume that the delta contains the
  // entire known state for that topic. This occurs particularly after
  // state-store failure, and usually clients will need to republish
  // any local state that is missing.
  //
  // Callbacks may publish new updates to any topic via the
  // topic_updates parameter, although updates for unknown topics
  // (i.e. those with no subscribers) will be ignored.
  typedef boost::function<void (const TopicDeltaMap& state,
                                std::vector<TTopicUpdate>* topic_updates)> UpdateCallback;

  // Adds a topic to the set of topics that updates will be received
  // for. When a topic update is received, the supplied UpdateCallback
  // will be invoked. Therefore clients should ensure that it is safe
  // to invoke callback for the entire lifetime of the subscriber; in
  // particular this means that the subscriber should be torn-down
  // before any objects that own callbacks.
  //
  // Must be called before Start(), in which case it will return
  // Status::OK. Otherwise an error will be returned.
  Status AddTopic(const StateStore::TopicId& topic_id, bool is_transient,
      const UpdateCallback& callback);

  // Registers this subscriber with the state-store, and starts the
  // heartbeat service, as well as a thread to check for failure and
  // initiate recovery mode.
  //
  // Returns OK unless some error occurred, like a failure to connect.
  Status Start();

 private:
  // Unique, but opaque, identifier for this subscriber.
  const std::string subscriber_id_;

  // Address that the heartbeat service should be started on.
  TNetworkAddress heartbeat_address_;

  // Address of the state-store
  TNetworkAddress state_store_address_;

  // Implementation of the heartbeat thrift interface, which proxies
  // calls onto this object.
  boost::shared_ptr<StateStoreSubscriberIf> thrift_iface_;

  // Container for the heartbeat server.
  boost::shared_ptr<ThriftServer> heartbeat_server_;

  // Failure detector that monitors heartbeats from the state-store.
  boost::scoped_ptr<impala::TimeoutFailureDetector> failure_detector_;

  // Thread in which RecoveryModeChecker runs.
  boost::scoped_ptr<boost::thread> recovery_mode_thread_;

  // Class-wide lock. Protects all subsequent members. Most private methods must
  // be called holding this lock; this is noted in the method comments.
  boost::mutex lock_;

  // Set to true after Register(...) is successful, after which no
  // more topics may be subscribed to.
  bool is_registered_;

  // Mapping of subscription ids to the associated callback. Because this mapping
  // stores a pointer to an UpdateCallback, memory errors will occur if an UpdateCallback
  // is deleted before being unregistered. The UpdateCallback destructor checks for
  // such problems, so that we will have an assertion failure rather than a memory error.
  typedef boost::unordered_map<StateStore::TopicId, std::vector<UpdateCallback> >
      UpdateCallbacks;

  // Callback for all services that have registered for updates (indexed by the
  // associated SubscriptionId), and associated lock.
  UpdateCallbacks update_callbacks_;

  // One entry for every topic subscribed to. The value is whether
  // this subscriber considers this topic to be 'transient', that is
  // any updates it makes will be deleted upon failure or
  // disconnection.
  std::map<StateStore::TopicId, bool> topic_registrations_;

  // Client to use to connect to the StateStore.
  boost::shared_ptr<impala::ThriftClient<StateStoreServiceClient> > client_;

  // Metric to indicate if we are successfully registered with the statestore
  Metrics::BooleanMetric* connected_to_statestore_metric_;

  // Subscriber thrift implementation, needs to access UpdateState
  friend class StateStoreSubscriberThriftIf;

  // Called when the state-store sends a heartbeat. Each registered
  // callback is called in turn with the given list of topic deltas,
  // and any updates are aggregated in topic_updates.
  void UpdateState(const TopicDeltaMap& topic_deltas,
      std::vector<TTopicUpdate>* topic_updates);

  // Run in a separate thread. In a loop, check failure_detector_ to see if the
  // state-store is still sending heartbeats. If not, enter 'recovery mode'
  // where a reconnection is repeatedly attempted. Once reconnected, all
  // existing subscriptions and services are reregistered and normal operation
  // resumes.
  //
  // During recovery mode, any public methods that are started will block on
  // lock_, which is only released when recovery finishes. In practice, all
  // registrations are made early in the life of an impalad before the
  // state-store could be detected as failed.
  void RecoveryModeChecker();

  // Creates a client of the remote state-store and sends a list of
  // topics to register for. Returns OK unless there is some problem
  // connecting, or the state-store reports an error.
  Status Register();
};

}

#endif
