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

#pragma once

#include <mutex>
#include <string>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "gen-cpp/StatestoreService.h"
#include "gen-cpp/StatestoreService_types.h"
#include "gen-cpp/StatestoreSubscriber.h"
#include "rpc/thrift-client.h"
#include "rpc/thrift-util.h"
#include "statestore/statestore.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "util/stopwatch.h"
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
  ///
  ///   heartbeat_address - the local address on which the heartbeat service which
  ///                       communicates with the statestore should be started.
  ///   statestore_address - the address of the statestore to register with
  ///   statestore2_address - the address of the second statestore to register with
  ///   subscriber_type - subscriber type
  StatestoreSubscriber(const std::string& subscriber_id,
      const TNetworkAddress& heartbeat_address,
      const TNetworkAddress& statestore_address,
      const TNetworkAddress& statestore2_address,
      MetricGroup* metrics,
      TStatestoreSubscriberType::type subscriber_type);

  virtual ~StatestoreSubscriber() {}

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
  /// for.
  Status AddTopic(const Statestore::TopicId& topic_id, bool is_transient,
      bool populate_min_subscriber_topic_version, std::string filter_prefix,
      const UpdateCallback& callback);

  /// UpdateCatalogdCallback is invoked every time that a notification of updating
  /// catalogd is received from the statestore. This callback function could be called
  /// for two events: receive registration RPC reply, receive UpdateCatalogD RPC request.
  ///   is_registration_reply: set it as true when the function is called for the event
  ///                          of registration reply.
  ///   active_catalogd_version: the version of active catalogd.
  ///   catalogd_registration: registration info of active catalogd, including its
  ///                          address and port.
  ///
  /// When receiving registration RPC reply for the attempt of re-registration, it's
  /// possible the active catalogd has NOT been elected by statestore yet. In this case,
  /// we still need to call this function so that coordinators and catalogds could reset
  /// their 'last_active_catalogd_version_'. Otherwise, the catalogd updates may be
  /// ignored by coordinators and catalogds after statestore is restarted. In this case,
  /// active_catalogd_version is set as invalid value -1. catalogd_registration has
  /// invalid value and should not be used by callee.
  typedef boost::function<void (
      bool is_registration_reply, int64_t active_catalogd_version,
      const TCatalogRegistration& catalogd_registration)> UpdateCatalogdCallback;

  /// Adds a callback for notification of updating catalogd.
  void AddUpdateCatalogdTopic(const UpdateCatalogdCallback& callback);

  /// CompleteRegistrationCallback is invoked when the registration with the statestore
  /// is completed.
  typedef boost::function<void ()>CompleteRegistrationCallback;

  /// Adds a callback for registration completion.
  void AddCompleteRegistrationTopic(const CompleteRegistrationCallback& callback);

  /// Registers this subscriber with the statestore, and starts the
  /// heartbeat service, as well as a thread to check for failure and
  /// initiate recovery mode.
  ///
  /// Returns OK unless some error occurred, like a failure to connect.
  virtual Status Start();

  /// Set Register Request
  virtual Status SetRegisterRequest(TRegisterSubscriberRequest* request);

  /// Return the port that the subscriber is listening on.
  int heartbeat_port() const { return heartbeat_address_.port; }

  const std::string& id() const { return subscriber_id_; }

  StatestoreServiceVersion::type GetProtocolVersion() const {
    return protocol_version_;
  }

  CatalogServiceVersion::type GetCatalogProtocolVersion() const {
    return catalog_protocol_version_;
  }

  /// Returns true if the statestore has recovered and the configurable post-recovery
  /// grace period has not yet elapsed.
  bool IsInPostRecoveryGracePeriod();

  /// Returns true if the registration with statestore is completed.
  bool IsRegistered();

 private:
  /// Unique, but opaque, identifier for this subscriber.
  const std::string subscriber_id_;

  /// Protocol version of the statestore service for this subscriber.
  StatestoreServiceVersion::type protocol_version_;

  /// Protocol version of the catalog service for this subscriber.
  CatalogServiceVersion::type catalog_protocol_version_;

  /// Address that the heartbeat service should be started on. Initialised in constructor,
  /// updated in Start() with the actual port if the wildcard port 0 was specified.
  /// If FLAGS_statestore_subscriber_use_resolved_address is true, this address needs to
  /// be resolved before calling Register() every time.
  TNetworkAddress heartbeat_address_;

  /// Subscriber type
  TStatestoreSubscriberType::type subscriber_type_;

  /// Implementation of the heartbeat thrift interface, which proxies
  /// calls onto this object.
  std::shared_ptr<StatestoreSubscriberIf> thrift_iface_;

  /// Container for the heartbeat server.
  std::shared_ptr<ThriftServer> heartbeat_server_;

  /// statestore client cache - only one client is ever used. Initialized in constructor.
  /// The StatestoreClientCache is created with num_retries = 1 and wait_ms = 0.
  /// Connections are still retried, but the retry mechanism is driven by DoRpcWithRetry.
  /// Clients should always use DoRpcWithRetry rather than DoRpc to ensure that both RPCs
  /// and connections are retried.
  boost::scoped_ptr<StatestoreClientCache> client_cache_;

  /// MetricGroup instance that all metrics are registered in. Not owned by this class.
  MetricGroup* metrics_;

  struct TopicRegistration {
    /// Held when processing a topic update. 'StatestoreSubscriber::lock_' must be held in
    /// shared mode before acquiring this lock. If taking multiple update locks, they must
    /// be acquired in ascending order of topic name.
    std::mutex update_lock;

    /// Whether the subscriber considers this topic to be "transient", that is any updates
    /// it makes will be deleted upon failure or disconnection.
    bool is_transient = false;

    /// Whether this subscriber needs the min_subscriber_topic_version field to be filled
    /// in on updates.
    bool populate_min_subscriber_topic_version = false;

    /// Only subscribe to keys with the provided prefix.
    std::string filter_prefix;

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

  /// The subscriber-side representation of an individual statestore instance, which
  /// tracks variety of bookkeeping information.
  class StatestoreStub {
   public:
    StatestoreStub(StatestoreSubscriber* subscriber, bool first_statestore,
        const TNetworkAddress& statestore_address, MetricGroup* metrics);

    /// Start to register with the statestore server.
    /// Returns OK unless some error occurred, like a failure to connect.
    /// Note: If the statestore server is not ready, the registration will fail and the
    /// subscriber enter recovery mode. In recovery mode, the class-wide lock_ is taken
    /// to ensure mutual exclusion with any operations in flight. To avoid accessing
    /// member variable is_active_, return its value in the given output parameter.
    Status Start(bool* startstore_is_active = nullptr);

    /// Adds a topic to the set of topics that updates will be received
    /// for. When a topic update is received, the supplied UpdateCallback
    /// will be invoked. Therefore clients should ensure that it is safe
    /// to invoke callback for the entire lifetime of the subscriber; in
    /// particular this means that the subscriber should be torn-down
    /// before any objects that own callbacks.
    ///
    /// Must be called before Start(), in which case it will return
    /// Status::OK. Otherwise an error will be returned.
    Status AddTopic(const Statestore::TopicId& topic_id, bool is_transient,
        bool populate_min_subscriber_topic_version, std::string filter_prefix,
        const UpdateCallback& callback);

    /// Adds a callback for notification of updating catalogd.
    void AddUpdateCatalogdTopic(const UpdateCatalogdCallback& callback);

    /// Adds a callback for registration completion.
    void AddCompleteRegistrationTopic(const CompleteRegistrationCallback& callback);

    /// Called when the statestore sends a topic update. Each registered callback is
    /// called in turn with the given map of incoming_topic_deltas from the statestore.
    /// Each TTopicDelta sent from the statestore to the subscriber will contain the
    /// topic name, a list of additions to the topic, a list of deletions from the topic,
    /// and the version range the update covers. A from_version of 0 indicates a non-
    /// delta update. In response, any updates to the topic by the subscriber are
    /// aggregated in subscriber_topic_updates and returned to the statestore. Each
    /// update is a TTopicDelta that contains a list of additions to the topic and a list
    /// of deletions from the topic. Additionally, if a subscriber has received an
    /// unexpected delta update version range, they can request a new delta update by
    /// setting the "from_version" field of the TTopicDelta response. The next statestore
    /// update will be based off the version the subscriber responds with. If the
    /// subscriber is in recovery mode, this method returns immediately.
    ///
    /// Returns an error if some error was encountered (e.g. the supplied registration ID
    /// was unexpected), and OK otherwise. The output parameter 'skipped' is set to true
    /// if the subscriber chose not to process this topic-update (if, for example, a
    /// concurrent update was being processed, or if the subscriber currently believes it
    /// is recovering). Doing so indicates that no topics were updated during this call.
    Status UpdateState(const TopicDeltaMap& incoming_topic_deltas,
        const RegistrationId& registration_id,
        std::vector<TTopicDelta>* subscriber_topic_updates, bool* skipped);

    /// Called when the statestore sends a heartbeat message. Updates the failure
    /// detector.
    void Heartbeat(const RegistrationId& registration_id);

    /// Called when the catalogd has been updated.
    void UpdateCatalogd(const TCatalogRegistration& catalogd_registration,
        const RegistrationId& registration_id, int64_t active_catalogd_version,
        bool statestore_failover, bool* update_skipped);

    /// Run in a separate thread. In a loop, check failure_detector_ to see if the
    /// statestore is still sending heartbeat messages. If not, enter 'recovery mode'
    /// where a reconnection is repeatedly attempted. Once reconnected, all existing
    /// subscriptions and services are reregistered and normal operation resumes.
    ///
    /// During recovery mode, any public methods that are started will block on lock_,
    /// which is only released when recovery finishes. In practice, all registrations are
    /// made early in the life of an impalad before the statestore could be detected as
    /// failed.
    [[noreturn]] void RecoveryModeChecker();

    /// Creates a client of the remote statestore and sends a list of
    /// topics to register for. Returns OK unless there is some problem
    /// connecting, or the statestore reports an error.
    Status Register(bool* has_active_catalogd, int64_t* active_catalogd_version,
        TCatalogRegistration* active_catalogd_registration);

    /// Returns OK if registration_id == registration_id_, or if registration_id_ is not
    /// yet set, an error otherwise. Used to confirm that RPCs from the statestore are
    /// intended for the current registration epoch.
    Status CheckRegistrationId(const RegistrationId& registration_id);

    /// Check if the given statestore ID is matching with the statestore ID of this
    /// statestore.
    bool IsMatchingStatestoreId(const TUniqueId statestore_id);

    int64_t MilliSecondsSinceLastRegistration() const {
      return MonotonicMillis() - last_registration_ms_.Load();
    }

    int64_t MilliSecondsSinceLastFailover() const {
      int64_t time_ms = MonotonicMillis() - last_failover_time_.Load();
      DCHECK_GE(time_ms, 0);
      return time_ms;
    }

    bool IsInPostRecoveryGracePeriod() const;

    /// Check if the subscriber is interesting to receive the notification of catalogd
    /// change.
    bool IsSubscribedCatalogdChange() { return !update_catalogd_callbacks_.empty(); }

    /// Returns true if the registration with statestore is completed.
    bool IsRegistered();

    /// Set the active state of the registered statestore instance.
    void SetStatestoreActive(bool is_active, int64_t active_statestored_version,
        bool has_failover = false);

    /// Return the version of active statestore.
    int64_t GetActiveVersion(bool* is_active);

    /// Return registration-Id and statestore-Id.
    void GetRegistrationIdAndStatestoreId(
        RegistrationId* registration_id, TUniqueId* statestore_id);

    /// Increase count for receiving UpdateStatestoredRole RPC.
    void IncCountForUpdateStatestoredRoleRPC();

    /// Get connection state with the registered statestore instance.
    TStatestoreConnState::type GetStatestoreConnState();

    std::string GetAddress() const;

   private:
    /// Pointer to parent StatestoreSubscriber object
    StatestoreSubscriber* subscriber_;

    /// Address of the statestore
    TNetworkAddress statestore_address_;

    /// True if the registered statestore instance is active.
    bool is_active_ = false;

    /// The version of active statestored.
    int64_t active_statestored_version_ = 0;

    /// Monotonic timestamp of the last failover.
    AtomicInt64 last_failover_time_{0};

    /// Protects is_active_ and active_statestored_version_. Must be taken after lock_
    /// if both are to be taken together.
    std::mutex active_lock_;

    /// Object-wide lock that protects the below members. Must be held exclusively when
    /// modifying the members, except when modifying TopicRegistrations - see
    /// TopicRegistration::update_lock for details of locking there. Held in shared mode
    /// when processing topic updates to prevent concurrent updates to other state. Most
    /// private methods must be called holding this lock; this is noted in the method
    /// comments.
    boost::shared_mutex lock_;

    /// Failure detector that tracks heartbeat messages from the statestore.
    boost::scoped_ptr<impala::TimeoutFailureDetector> failure_detector_;

    /// Thread in which RecoveryModeChecker runs.
    std::unique_ptr<Thread> recovery_mode_thread_;

    /// MetricGroup instance that all metrics are registered in. Not owned by this class.
    MetricGroup* metrics_;

    /// Metric to indicate if we are successfully registered with the statestore
    BooleanProperty* connected_to_statestore_metric_;

    /// Metric to count the total number of connection failures to the statestore
    IntCounter* connection_failure_metric_;

    /// Amount of time last spent in recovery mode
    DoubleGauge* last_recovery_duration_metric_;

    /// When the last recovery happened.
    StringProperty* last_recovery_time_metric_;

    /// Accumulated statistics on the frequency of topic-update messages, including
    /// samples from all topics.
    StatsMetric<double>* topic_update_interval_metric_;

    /// Accumulated statistics on the time taken to process each topic-update message
    /// from the statestore (that is, to call all callbacks)
    StatsMetric<double>* topic_update_duration_metric_;

    /// Accumulated statistics on the frequency of heartbeat messages
    StatsMetric<double>* heartbeat_interval_metric_;

    /// Current registration ID, in string form.
    StringProperty* registration_id_metric_;

    /// Current statestore ID, in string form.
    StringProperty* statestore_id_metric_;

    /// Metric to count the total number of UpdateCatalogd RPCs received by subscriber.
    IntCounter* update_catalogd_rpc_metric_;

    /// Metric that tracks if the statestored is active when statestored HA is enabled.
    BooleanProperty* active_status_metric_;

    /// Metric to count the total number of UpdateStatestoredRole RPCs received by
    /// subscriber.
    IntCounter* update_statestored_role_rpc_metric_;

    /// Metric to count the total number of re-registration attempt.
    IntCounter* re_registr_attempt_metric_;

    /// Tracks the time between heartbeat messages. Only updated by Heartbeat(), which
    /// should not run concurrently with itself. Use a ConcurrentStopWatch because the
    /// watch is started in one thread, but read in another.
    ConcurrentStopWatch heartbeat_interval_timer_;

    /// One entry for every topic subscribed to. 'lock_' must be held exclusively to add
    /// or remove entries from the map or held as a shared lock to lookup entries in the
    /// map. Modifications to the contents of each TopicRegistration is protected by
    /// TopicRegistration::update_lock.
    boost::unordered_map<Statestore::TopicId, TopicRegistration> topic_registrations_;

    /// Callback functions to handle the notification of updating catalogD.
    std::vector<UpdateCatalogdCallback> update_catalogd_callbacks_;

    /// Callback functions for registration completion.
    std::vector<CompleteRegistrationCallback> complete_registration_callbacks_;

    /// Set to true after Register(...) is successful, after which no
    /// more topics may be subscribed to.
    bool is_registered_;

    /// Protects registration_id_, protocol_version_ and statestore_id_. Must be taken
    /// after lock_ if both are to be taken together.
    std::mutex id_lock_;

    /// Set during Register(), this is the unique ID of the current registration with the
    /// statestore. If this subscriber must recover, or disconnects and then reconnects,
    /// the registration_id_ will change after Register() is called again. This allows
    /// the subscriber to reject communication from the statestore that pertains to a
    /// previous registration.
    RegistrationId registration_id_;

    /// Protocol version of the statestore, which is returned in the registration response
    /// from the statestore.
    StatestoreServiceVersion::type protocol_version_;

    /// Instance ID of the statestore, which is returned in the registration response
    /// from the statestore.
    TUniqueId statestore_id_;

    /// Monotonic timestamp of the last successful registration.
    AtomicInt64 last_registration_ms_{0};
  };

  /// Set to true if statestored HA is enabled.
  bool enable_statestored_ha_ = false;

  /// Pointers of two StatestoreStub objects
  StatestoreStub* statestore_ = nullptr;
  StatestoreStub* statestore2_ = nullptr;

  /// Lock to protect statestore HA related variables: active_statestore_,
  /// standby_statestore_
  std::mutex statestore_ha_lock_;

  /// Pointers for active and standby StatestoreStub objects. Subscriber find active
  /// statestore during registration, or get notification when statestore service
  /// fail over to standby statestore.
  StatestoreStub* active_statestore_ = nullptr;
  StatestoreStub* standby_statestore_ = nullptr;

  /// Called when the statestore sends a topic update.
  Status UpdateState(const TopicDeltaMap& incoming_topic_deltas,
      const RegistrationId& registration_id, const TUniqueId& statestore_id,
      std::vector<TTopicDelta>* subscriber_topic_updates, bool* skipped);

  /// Called when the statestore sends a heartbeat message.
  void Heartbeat(const RegistrationId& registration_id,
      const TUniqueId& statestore_id, bool request_active_conn_state,
      TStatestoreConnState::type* active_statestore_conn_state);

  /// Called when the active catalogd has been updated.
  void UpdateCatalogd(const TCatalogRegistration& catalogd_registration,
      const RegistrationId& registration_id, const TUniqueId& statestore_id,
      int64_t active_catalogd_version, bool* update_skipped);

  /// Called when the active statestore has been updated.
  void UpdateStatestoredRole(bool is_active,
      const RegistrationId& registration_id, const TUniqueId& statestore_id,
      int64_t active_statestored_version, bool update_active_catalogd,
      const TCatalogRegistration* catalogd_registration, int64_t active_catalogd_version,
      bool* update_skipped);

  /// Return the pointer of the active/standby StatestoreStub.
  StatestoreStub* GetActiveStatestore();
  StatestoreStub* GetStandbyStatestore();

  /// Subscriber thrift implementation, needs to access UpdateState
  friend class StatestoreSubscriberThriftIf;
};
}
