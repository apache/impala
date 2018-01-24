// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef STATESTORE_STATESTORE_SUBSCRIBER_H
#define STATESTORE_STATESTORE_SUBSCRIBER_H

#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "statestore/statestore.h"
#include "util/stopwatch.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-client.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "util/metrics.h"
#include "gen-cpp/StatestoreService.h"
#include "gen-cpp/StatestoreSubscriber.h"

namespace impala {

class Status;
class TimeoutFailureDetector;
class Thread;
class ThriftServer;
class TNetworkAddress;

typedef ClientCache<StatestoreServiceClientWrapper> StatestoreClientCache;

/// A StatestoreSubscriber communicates with a statestore periodically through the exchange
/// of topic update messages. These messages contain updates from the statestore to a list
/// of 'topics' that the subscriber is interested in; in response the subscriber sends a
/// list of changes that it wishes to make to a topic. The statestore also sends more
/// frequent 'heartbeat' messages that confirm the connection between statestore and
/// subscriber is still active.
//
/// Clients of the subscriber register topics of interest, and a function to call once an
/// update has been received. Each callback may optionally add one or more updates to a
/// list of topic updates to be sent back to the statestore. See AddTopic for the
/// requirements placed on these callbacks.
//
/// Topics must be subscribed to before the subscriber is connected to the statestore:
/// there is no way to add a new subscription after the subscriber has successfully
/// registered.
//
/// If the subscriber does not receive heartbeats from the statestore within a configurable
/// period of time, the subscriber enters 'recovery mode', where it continually attempts to
/// re-register with the statestore. Recovery mode is not triggered if a heartbeat takes a
/// long time to process locally.
class StatestoreSubscriber {
 public:
  /// Only constructor.
  ///   subscriber_id - should be unique across the cluster, identifies this subscriber
  //
  ///   heartbeat_address - the local address on which the heartbeat service which
  ///                       communicates with the statestore should be started.
  //
  ///   statestore_address - the address of the statestore to register with
  StatestoreSubscriber(const std::string& subscriber_id,
      const TNetworkAddress& heartbeat_address,
      const TNetworkAddress& statestore_address,
      MetricGroup* metrics);

  /// A TopicDeltaMap is passed to each callback. See UpdateCallback for more details.
  typedef std::map<Statestore::TopicId, TTopicDelta> TopicDeltaMap;

  /// Function called to update a service with new state. Called in a
  /// separate thread to the one in which it is registered.
  //
  /// Every UpdateCallback is invoked every time that an update for the
  /// topic is received from the statestore. Therefore the callback should
  /// not assume that the TopicDeltaMap contains an entry for their
  /// particular topic of interest.
  //
  /// If a delta for a particular topic does not have the 'is_delta'
  /// flag set, clients should assume that the delta contains the
  /// entire known state for that topic. This occurs particularly after
  /// statestore failure, and usually clients will need to republish
  /// any local state that is missing.
  //
  /// Callbacks may publish new updates to any topic via the
  /// topic_updates parameter, although updates for unknown topics
  /// (i.e. those with no subscribers) will be ignored.
  typedef boost::function<void (const TopicDeltaMap& state,
                                std::vector<TTopicDelta>* topic_updates)> UpdateCallback;

  /// Adds a topic to the set of topics that updates will be received
  /// for. When a topic update is received, the supplied UpdateCallback
  /// will be invoked. Therefore clients should ensure that it is safe
  /// to invoke callback for the entire lifetime of the subscriber; in
  /// particular this means that the subscriber should be torn-down
  /// before any objects that own callbacks.
  //
  /// Must be called before Start(), in which case it will return
  /// Status::OK. Otherwise an error will be returned.
  Status AddTopic(const Statestore::TopicId& topic_id, bool is_transient,
      const UpdateCallback& callback);

  /// Registers this subscriber with the statestore, and starts the
  /// heartbeat service, as well as a thread to check for failure and
  /// initiate recovery mode.
  //
  /// Returns OK unless some error occurred, like a failure to connect.
  Status Start();

  const std::string& id() const { return subscriber_id_; }

 private:
  /// Unique, but opaque, identifier for this subscriber.
  const std::string subscriber_id_;

  /// Address that the heartbeat service should be started on.
  TNetworkAddress heartbeat_address_;

  /// Address of the statestore
  TNetworkAddress statestore_address_;

  /// Implementation of the heartbeat thrift interface, which proxies
  /// calls onto this object.
  boost::shared_ptr<StatestoreSubscriberIf> thrift_iface_;

  /// Container for the heartbeat server.
  std::shared_ptr<ThriftServer> heartbeat_server_;

  /// Failure detector that tracks heartbeat messages from the statestore.
  boost::scoped_ptr<impala::TimeoutFailureDetector> failure_detector_;

  /// Thread in which RecoveryModeChecker runs.
  std::unique_ptr<Thread> recovery_mode_thread_;

  /// statestore client cache - only one client is ever used. Initialized in constructor.
  boost::scoped_ptr<StatestoreClientCache> client_cache_;

  /// MetricGroup instance that all metrics are registered in. Not owned by this class.
  MetricGroup* metrics_;

  /// Metric to indicate if we are successfully registered with the statestore
  BooleanProperty* connected_to_statestore_metric_;

  /// Amount of time last spent in recovery mode
  DoubleGauge* last_recovery_duration_metric_;

  /// When the last recovery happened.
  StringProperty* last_recovery_time_metric_;

  /// Accumulated statistics on the frequency of topic-update messages, including samples
  /// from all topics.
  StatsMetric<double>* topic_update_interval_metric_;

  /// Accumulated statistics on the time taken to process each topic-update message from
  /// the statestore (that is, to call all callbacks)
  StatsMetric<double>* topic_update_duration_metric_;

  /// Accumulated statistics on the frequency of heartbeat messages
  StatsMetric<double>* heartbeat_interval_metric_;

  /// Tracks the time between heartbeat messages. Only updated by Heartbeat(), which
  /// should not run concurrently with itself.
  MonotonicStopWatch heartbeat_interval_timer_;

  /// Current registration ID, in string form.
  StringProperty* registration_id_metric_;

  /// Object-wide lock that protects the below members. Must be held exclusively when
  /// modifying the members, except when modifying TopicRegistrations - see
  /// TopicRegistration::update_lock for details of locking there. Held in shared mode
  /// when processing topic updates to prevent concurrent updates to other state. Most
  /// private methods must be called holding this lock; this is noted in the method
  /// comments.
  boost::shared_mutex lock_;

  /// Set to true after Register(...) is successful, after which no
  /// more topics may be subscribed to.
  bool is_registered_;

  /// Protects registration_id_. Must be taken after lock_ if both are to be taken
  /// together.
  boost::mutex registration_id_lock_;

  /// Set during Register(), this is the unique ID of the current registration with the
  /// statestore. If this subscriber must recover, or disconnects and then reconnects, the
  /// registration_id_ will change after Register() is called again. This allows the
  /// subscriber to reject communication from the statestore that pertains to a previous
  /// registration.
  RegistrationId registration_id_;

  struct TopicRegistration {
    /// Held when processing a topic update. 'StatestoreSubscriber::lock_' must be held in
    /// shared mode before acquiring this lock. If taking multiple update locks, they must
    /// be acquired in ascending order of topic name.
    boost::mutex update_lock;

    /// Whether the subscriber considers this topic to be "transient", that is any updates
    /// it makes will be deleted upon failure or disconnection.
    bool is_transient = false;

    /// The last version of the topic this subscriber processed.
    /// -1 if no updates have been processed yet.
    int64_t current_topic_version = -1;

    /// Owned by the MetricGroup instance. Tracks how long callbacks took to process this
    /// topic.
    StatsMetric<double>* processing_time_metric = nullptr;

    /// Tracks the time between topic-update messages to update 'update_interval_metric'.
    MonotonicStopWatch update_interval_timer;

    /// Owned by the MetricGroup instances. Tracks the time between the end of the last
    /// update RPC for this topic and the start of the next.
    StatsMetric<double>* update_interval_metric = nullptr;

    /// Callback for all services that have registered for updates.
    std::vector<UpdateCallback> callbacks;
  };

  /// One entry for every topic subscribed to. 'lock_' must be held exclusively to add or
  /// remove entries from the map or held as a shared lock to lookup entries in the map.
  /// Modifications to the contents of each TopicRegistration is protected by
  /// TopicRegistration::update_lock.
  boost::unordered_map<Statestore::TopicId, TopicRegistration> topic_registrations_;

  /// Subscriber thrift implementation, needs to access UpdateState
  friend class StatestoreSubscriberThriftIf;

  /// Called when the statestore sends a topic update. Each registered callback is called
  /// in turn with the given map of incoming_topic_deltas from the statestore. Each
  /// TTopicDelta sent from the statestore to the subscriber will contain the topic name, a
  /// list of additions to the topic, a list of deletions from the topic, and the version
  /// range the update covers. A from_version of 0 indicates a non-delta update.  In
  /// response, any updates to the topic by the subscriber are aggregated in
  /// subscriber_topic_updates and returned to the statestore. Each update is a TTopicDelta
  /// that contains a list of additions to the topic and a list of deletions from the
  /// topic. Additionally, if a subscriber has received an unexpected delta update version
  /// range, they can request a new delta update by setting the "from_version" field of the
  /// TTopicDelta response. The next statestore update will be based off the version the
  /// subscriber responds with.  If the subscriber is in recovery mode, this method returns
  /// immediately.
  //
  /// Returns an error if some error was encountered (e.g. the supplied registration ID was
  /// unexpected), and OK otherwise. The output parameter 'skipped' is set to true if the
  /// subscriber chose not to process this topic-update (if, for example, a concurrent
  /// update was being processed, or if the subscriber currently believes it is
  /// recovering). Doing so indicates that no topics were updated during this call.
  Status UpdateState(const TopicDeltaMap& incoming_topic_deltas,
      const RegistrationId& registration_id,
      std::vector<TTopicDelta>* subscriber_topic_updates, bool* skipped);

  /// Called when the statestore sends a heartbeat message. Updates the failure detector.
  void Heartbeat(const RegistrationId& registration_id);

  /// Run in a separate thread. In a loop, check failure_detector_ to see if the statestore
  /// is still sending heartbeat messages. If not, enter 'recovery mode' where a
  /// reconnection is repeatedly attempted. Once reconnected, all existing subscriptions
  /// and services are reregistered and normal operation resumes.
  //
  /// During recovery mode, any public methods that are started will block on lock_, which
  /// is only released when recovery finishes. In practice, all registrations are made
  /// early in the life of an impalad before the statestore could be detected as failed.
  [[noreturn]] void RecoveryModeChecker();

  /// Creates a client of the remote statestore and sends a list of
  /// topics to register for. Returns OK unless there is some problem
  /// connecting, or the statestore reports an error.
  Status Register();

  /// Returns OK if registration_id == registration_id_, or if registration_id_ is not yet
  /// set, an error otherwise. Used to confirm that RPCs from the statestore are intended
  /// for the current registration epoch.
  Status CheckRegistrationId(const RegistrationId& registration_id);
};

}

#endif
