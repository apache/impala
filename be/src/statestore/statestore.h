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

#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/scoped_ptr.hpp>
#include <boost/thread/pthread/shared_mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "common/atomic.h"
#include "common/status.h"
#include "gen-cpp/StatestoreHaService.h"
#include "gen-cpp/StatestoreService.h"
#include "gen-cpp/StatestoreService_types.h"
#include "gen-cpp/StatestoreSubscriber.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-client.h"
#include "runtime/client-cache.h"
#include "statestore/failure-detector.h"
#include "statestore/statestore-catalogd-mgr.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "statestore/statestore-subscriber-client-wrapper.h"
#include "util/aligned-new.h"
#include "util/condition-variable.h"
#include "util/metrics-fwd.h"
#include "util/thread-pool.h"
#include "util/webserver.h"

using kudu::HttpStatusCode;

namespace impala {

class Status;

typedef ClientCache<StatestoreSubscriberClientWrapper> StatestoreSubscriberClientCache;
typedef ClientCache<StatestoreHaServiceClientWrapper> StatestoreHaClientCache;

std::string SubscriberTypeToString(TStatestoreSubscriberType::type t);

/// The Statestore is a soft-state key-value store that maintains a set of Topics, which
/// are maps from string keys to byte array values.
///
/// Topics are subscribed to by subscribers, which are remote clients of the statestore
/// which express an interest in some set of Topics. The statestore sends topic updates to
/// subscribers via periodic 'update' messages, and also sends periodic 'heartbeat'
/// messages, which are used to detect the liveness of a subscriber. Updates for each
/// topic are delivered sequentially to each subscriber per subscription. E.g. if a
/// subscriber is subscribed to a topic "foo", the statestore will not deliver topic
/// updates for "foo" out-of-order or concurrently, but the updates may be sent
/// concurrently or out-of-order with "bar".
///
/// In response to 'update' messages, subscribers, send topic updates to the statestore to
/// merge with the current topic. These updates are then sent to all other subscribers in
/// their next update message. The next message is scheduled update_frequency in the
/// future, unless the subscriber indicated that it skipped processing an update, in which
/// case the statestore will back off slightly before re-sending a new update. The
/// update frequency is determined by FLAGS_statestore_update_frequency_ms or
/// FLAGS_statestore_priority_update_frequency, depending on whether the topic is a
/// prioritized topic.
///
/// Prioritized topics are topics that are small but important to delivery in a timely
/// manner. Handling those topics in a separate threadpool prevents large updates of other
/// topics slowing or blocking dissemination of updates to prioritized topics.
///
/// Topic entries usually have human-readable keys, and values which are some serialised
/// representation of a data structure, e.g. a Thrift struct. The contents of a value's
/// byte string is opaque to the statestore, which maintains no information about how to
/// deserialise it. Subscribers must use convention to interpret each other's updates.
///
/// A subscriber may have marked some updates that it made as 'transient', which implies
/// that those entries should be deleted once the subscriber is no longer connected (this
/// is judged by the statestore's failure-detector, which will mark a subscriber as failed
/// when it has not responded to a number of successive heartbeat messages). Transience
/// is tracked per-topic-per-subscriber, so two different subscribers may treat the same
/// topic differently wrt to the transience of their updates.
///
/// The statestore tracks the history of updates to each topic, with each topic update
/// getting a sequentially increasing version number that is unique across the topic.
///
/// Subscribers also track the max version of each topic which they have have successfully
/// processed. The statestore can use this information to send a delta of updates to a
/// subscriber, rather than all items in the topic.  For non-delta updates, the statestore
/// will send an update that includes all values in the topic.
///
/// Subscribers may filter the keys within a subscribed topic by an optional prefix. If
/// a key filter prefix is specified, only entries matching that prefix will be sent to
/// the subscriber in updates. Note that this may result in empty updates being sent
/// to subscribers in the case that all updated keys have been excluded by the filter.
/// These empty updates are important so that subscribers can keep track of the current
/// version number and report back their progress in receiving the topic contents.
///
/// The Statestore is the central manager of the cluster. Subscribers join in the cluster
/// through registration. To avoid incompatibility issues between the different components
/// in the cluster, statestore uses protocol version to isolate cluster components, and
/// only accepts the registration requests from the subscribers with compatible protocol
/// versions. This ensure all of components in a cluster could communicate with compatible
/// APIs and message format in the wire.
///
/// Statestore HA:
/// To support statestore HA, we add the preemptive behavior for statestored. When HA is
/// enabled, the preemptive behavior allows the statestored with the higher priority to
/// become active and the paired statestored becomes standby. The active statestored acts
/// as the owner of the Impala cluster and provides statestore service for the cluster
/// members.
/// By default, preemption is disabled on the statestored which is running as single
/// statestore instance in an Impala cluster. For deployment with statestore HA enabled,
/// the preemption must be enabled with starting flag "enable_statestored_ha" on two
/// statestoreds in the HA pair and all the subscribers.
/// To avoid introduce external dependency like Apache ZooKeeper, standby statestored is
/// used to monitor the health of active statestored. The basic idea is to have two
/// statestore instances as HA pair in Active-Passive mode. The both primary and standby
/// statestored send heartbeats to all subscribers. Subscribers send heartbeat response
/// to standby statestored with their connection state with active statestored so that
/// standby statestored could monitor the connection state between active statestore and
/// the subscribers. Active statestored also sends heartbeats to standby statestored.
/// Automatic fail-over will be triggered if standby statestored does not receive
/// heartbeats from active statestored and the majority of the subscribers do not receive
/// heartbeats from active statestored.
/// In the scenario where standby statestored is still receiving heartbeats from active
/// statestored, but majority of subscribers are unable to reach the active statestored,
/// warning messages will be logged on standby statestored.
/// The statestored in an Active-Passive HA pair can be assigned an instance priority
/// value to indicate a preference for which statestored should assume the active role.
/// The statestore ID which is generated for each statestored can be used as instance
/// priority value. The lower numerical value in statestore ID corresponds to a higher
/// priority. The statestored with the higher priority is designated as active, the other
/// statestored is designated as standby after initial HA handshake. Only the active
/// statestored propagates the IMPALA_CATALOG_TOPIC and elected active catalogd to the
/// cluster members.
/// To make a specific statestored in the HA pair as active instance, the statestored must
/// be started with starting flag "statestore_force_active" so that the statestored will
/// be assigned with active role in HA handshake. This allows administrator to manually
/// perform statestore service fail over.
///
/// +================+
/// | Implementation |
/// +================+
///
/// Locking:
/// --------
/// The lock acquisition order is:
/// 1. 'ha_lock_'
/// 2. 'subscribers_lock_'
/// 3. 'topics_map_lock_'
/// 4. Subscriber::transient_entry_lock_
/// 5. Topic::lock_ (terminal)
class Statestore : public CacheLineAligned {
 public:
  /// A TopicId uniquely identifies a single topic
  typedef std::string TopicId;

  /// A TopicEntryKey uniquely identifies a single entry in a topic
  typedef std::string TopicEntryKey;

  /// The only constructor; initialises member variables only.
  Statestore(MetricGroup* metrics);

  /// Destructor, should not be called once the Statestore is initialized.
  ~Statestore();

  /// Initialize and start the backing ThriftServer with port 'state_store_port'.
  /// Initialize the ThreadPools used for updates and heartbeats. Returns an error if
  /// any of the above initialization fails.
  Status Init(int32_t state_store_port) WARN_UNUSED_RESULT;

  /// Registers a new subscriber with the given unique subscriber ID, running a subscriber
  /// service at the given location, with the provided list of topic subscriptions.
  /// The registration_id output parameter is the unique ID for this registration, used to
  /// distinguish old registrations from new ones for the same subscriber. On successful
  /// registration, the subscriber is added to the update queue, with an immediate
  /// schedule.
  ///
  /// If a registration already exists for this subscriber, the old registration is
  /// removed
  /// and a new one is created. Subscribers may receive an update intended for the old
  /// registration, since one may be in flight when a new RegisterSubscriber() is
  /// received.
  Status RegisterSubscriber(const SubscriberId& subscriber_id,
      const TNetworkAddress& location,
      const std::vector<TTopicRegistration>& topic_registrations,
      TStatestoreSubscriberType::type subscriber_type,
      bool subscribe_catalogd_change,
      const TCatalogRegistration& catalogd_registration,
      RegistrationId* registration_id,
      bool* has_active_catalogd,
      int64_t* active_catalogd_version,
      TCatalogRegistration* active_catalogd_registration);

  /// Registers webpages for the input webserver. If metrics_only is set then only
  /// '/healthz' page is registered.
  void RegisterWebpages(Webserver* webserver, bool metrics_only);

  /// The main processing loop. Runs infinitely.
  void MainLoop();

  /// Shut down some background threads. Only used for testing. Note that this is not
  /// a clean shutdown because we can't correctly tear down 'thrift_server_', so
  /// not all background threads are stopped and this object cannot be destroyed.
  void ShutdownForTesting();

  /// Returns the Thrift API interface that proxies requests onto the local Statestore.
  const std::shared_ptr<StatestoreServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }

  /// Returns the Thrift API interface that proxies requests onto the local Statestore
  /// HA Service.
  const std::shared_ptr<StatestoreHaServiceIf>& ha_thrift_iface() const {
    return ha_thrift_iface_;
  }

  /// Names of prioritized topics that are handled in a separate threadpool. The topic
  /// names are hardcoded here for expediency. Ideally we would have a more generic
  /// interface for specifying prioritized topics, but for now we only have a small
  /// fixed set of topics.
  /// Topic tracking the set of live Impala daemon instances.
  static const char* IMPALA_MEMBERSHIP_TOPIC;
  /// Topic tracking the state of admission control on all coordinators.
  static const char* IMPALA_REQUEST_QUEUE_TOPIC;

  int32_t port() { return thrift_server_->port(); }

  /// Amount of time in ms that it takes the statestore to decide that a executor is down
  /// after it stops responding to heartbeats.
  static int64_t FailedExecutorDetectionTimeMs();

  /// Return statestore-ID.
  const TUniqueId& GetStateStoreId() const { return statestore_id_; }

  /// Return protocol version of statestore service.
  StatestoreServiceVersion::type GetProtocolVersion() const {
    return protocol_version_;
  }

  /// Disable network if the input parameter disable_network is set as true.
  /// statestored will not send heartbeat and HA handshake messages when network is
  /// disabled. This is used to simulate network failure for unit-test.
  void SetStatestoreDebugAction(bool disable_network);

  /// Initialize statestore HA.
  Status InitStatestoreHa(
      int32_t state_store_ha_port, const TNetworkAddress& peer_statestore_addr);

  // Return true if this statestore instance is in active role.
  bool IsActive();

  // Return the version of active status.
  int64_t GetActiveVersion(bool* is_active);

  /// Send RPC to negotiate the role with peer statestored for Statestore HA.
  Status SendHaHandshake(TStatestoreHaHandshakeResponse* response);

  /// Called to process HA handshake request from peer statstore
  Status ReceiveHaHandshakeRequest(const TUniqueId& subscriber_id,
      const string& src_statestore_address, bool src_force_active,
      bool* dst_statestore_active);

  /// Called to process HA heartbeat from active statestore
  void HaHeartbeatRequest(const TUniqueId& dst_statestore_id,
      const TUniqueId& src_statestore_id);

  /// Sends and monitors the HA heartbeats between two statestore instances.
  [[noreturn]] void MonitorStatestoredHaHeartbeat();

  /// Send HA heartbeat to peer statestore instance.
  Status SendHaHeartbeat();

  /// Return the network address of peer statestore instance.
  std::string GetPeerStatestoreHaAddress() {
    return TNetworkAddressToString(peer_statestore_ha_addr_);
  }

  // Update the subscriber's catalog information.
  void UpdateSubscriberCatalogInfo(const SubscriberId& subscriber_id);

 private:
  /// A TopicEntry is a single entry in a topic, and logically is a <string, byte string>
  /// pair.
  class TopicEntry {
   public:
    /// A Value is a string of bytes, for which std::string is a convenient representation.
    typedef std::string Value;

    /// A version is a monotonically increasing counter. Each update to a topic has its own
    /// unique version with the guarantee that sequentially later updates have larger
    /// version numbers.
    typedef int64_t Version;

    /// The Version value used to initialize a new TopicEntry.
    static const Version TOPIC_ENTRY_INITIAL_VERSION = 1L;

    /// Sets the value of this entry to the byte / length pair. The caller is responsible
    /// for ensuring, if required, that the version parameter is larger than the
    /// current version() TODO: Consider enforcing version monotonicity here.
    void SetValue(const Value& bytes, Version version);

    /// Sets a new version for this entry.
    void SetVersion(Version version) { version_ = version; }

    /// Sets the is_deleted_ flag for this entry.
    void SetDeleted(bool is_deleted) { is_deleted_ = is_deleted; }

    TopicEntry() : version_(TOPIC_ENTRY_INITIAL_VERSION),
        is_deleted_(false) { }

    const Value& value() const { return value_; }
    uint64_t version() const { return version_; }
    uint32_t length() const { return value_.size(); }
    bool is_deleted() const { return is_deleted_; }

   private:
    /// Byte string value, owned by this TopicEntry. The value is opaque to the
    /// statestore, and is interpreted only by subscribers.
    Value value_;

    /// The version of this entry. Every update is assigned a monotonically increasing
    /// version number so that only the minimal set of changes can be sent from the
    /// statestore to a subscriber.
    Version version_;

    /// Indicates if the entry has been deleted. If true, the entry will still be
    /// retained to track changes to send to subscribers.
    bool is_deleted_;
  };

  /// Map from TopicEntryKey to TopicEntry, maintained by a Topic object.
  typedef boost::unordered_map<TopicEntryKey, TopicEntry> TopicEntryMap;

  /// Map from Version to TopicEntryKey, maintained by a Topic object. Effectively a log of
  /// the updates made to a Topic, ordered by version.
  typedef std::map<TopicEntry::Version, TopicEntryKey> TopicUpdateLog;

  /// A Topic is logically a map between a string key and a sequence of bytes. A <string,
  /// bytes> pair is a TopicEntry.
  //
  /// Each topic has a unique version number that tracks the number of updates made to the
  /// topic. This is to support sending only the delta of changes on every update.
  class Topic {
   public:
    Topic(const TopicId& topic_id, IntGauge* key_size_metric,
        IntGauge* value_size_metric, IntGauge* topic_size_metric)
        : topic_id_(topic_id), last_version_(0L), total_key_size_bytes_(0L),
          total_value_size_bytes_(0L), key_size_metric_(key_size_metric),
          value_size_metric_(value_size_metric), topic_size_metric_(topic_size_metric) { }

    /// Add entries with the given keys and values. If is_deleted is true for an entry,
    /// it is considered deleted, and may be garbage collected in the future. Each entry
    /// is assigned a new version number by the Topic, and the version numbers are
    /// returned.
    ///
    /// Safe to call concurrently from multiple threads (for different subscribers).
    /// Acquires an exclusive write lock for the topic.
    std::vector<TopicEntry::Version> Put(const std::vector<TTopicItem>& entries);

    /// Deletes all the topic entries and updates the topic metrics. It doesn't
    /// reset the last_version_ to ensure that versions are monotonically
    /// increasing.
    ///
    /// Safe to call concurrently from multiple threads (for different
    /// subscribers). Acquires an exclusive lock for the topic.
    void ClearAllEntries();

    /// Utility method to support removing transient entries. We track the version numbers
    /// of entries added by subscribers, and remove entries with the same version number
    /// when that subscriber fails (the same entry may exist, but may have been updated by
    /// another subscriber giving it a new version number)
    //
    /// Deletion means marking the entry as deleted and incrementing its version
    /// number.
    ///
    /// Safe to call concurrently from multiple threads (for different subscribers).
    /// Acquires an exclusive write lock for the topic.
    void DeleteIfVersionsMatch(TopicEntry::Version version, const TopicEntryKey& key);

    /// Build a delta update to send to 'subscriber_id' including the deltas greater
    /// than 'last_processed_version' (not inclusive). Only those items whose keys
    /// start with 'filter_prefix' are included in the update.
    ///
    /// Safe to call concurrently from multiple threads (for different subscribers).
    /// Acquires a shared read lock for the topic.
    void BuildDelta(const SubscriberId& subscriber_id,
        TopicEntry::Version last_processed_version, const std::string& filter_prefix,
        TTopicDelta* delta);

    /// Adds entries representing the current topic state to 'topic_json'.
    void ToJson(rapidjson::Document* document, rapidjson::Value* topic_json);
   private:
    /// Unique identifier for this topic. Should be human-readable.
    const TopicId topic_id_;

    /// Reader-writer lock to protect state below. This is a terminal lock - no
    /// other locks should be acquired while holding this one. boost::shared_mutex
    /// gives writers priority over readers in acquiring the lock, which prevents
    /// starvation.
    boost::shared_mutex lock_;

    /// Map from topic entry key to topic entry.
    TopicEntryMap entries_;

    /// Tracks the last version that was assigned to an entry in this Topic. Incremented
    /// every time an entry is added in Put() so each TopicEntry is tagged with a unique
    /// version value.
    TopicEntry::Version last_version_;

    /// Contains a history of updates to this Topic, with each key being a Version and the
    /// value being a TopicEntryKey. Used to look up the TopicEntry in the entries_ map
    /// corresponding to each version update.
    //
    /// TODO: Looking up a TopicEntry from the topic_update_log_ requires two reads - one
    /// to get the TopicEntryKey and another to use that key to look up the corresponding
    /// TopicEntry in the entries_ map. Based on performance needs, we may need to revisit
    /// this to find a way to do the look up using a single read.
    TopicUpdateLog topic_update_log_;

    /// Total memory occupied by the key strings, in bytes
    int64_t total_key_size_bytes_;

    /// Total memory occupied by the value byte strings, in bytes.
    int64_t total_value_size_bytes_;

    /// Metrics shared across all topics to sum the size in bytes of keys, values and both
    IntGauge* key_size_metric_;
    IntGauge* value_size_metric_;
    IntGauge* topic_size_metric_;
  };

  /// Protects the 'topics_' map. Should be held shared when reading or holding a
  /// reference to entries in the map and exclusively when modifying the map.
  /// See the class comment for the lock acquisition order. boost::shared_mutex
  /// gives writers priority over readers in acquiring the lock, which prevents
  /// starvation.
  boost::shared_mutex topics_map_lock_;

  /// The entire set of topics tracked by the statestore
  typedef boost::unordered_map<TopicId, Topic> TopicMap;
  TopicMap topics_;

  /// The statestore-side representation of an individual subscriber client, which tracks a
  /// variety of bookkeeping information. This includes the list of subscribed topics (and
  /// whether updates to them should be deleted on failure), the list of updates made by
  /// this subscriber (in order to be able to efficiently delete them on failure), and the
  /// subscriber's ID and network location.
  class Subscriber {
   public:
    Subscriber(const SubscriberId& subscriber_id, const RegistrationId& registration_id,
        const TNetworkAddress& network_address,
        const std::vector<TTopicRegistration>& subscribed_topics,
        TStatestoreSubscriberType::type subscriber_type,
        bool subscribe_catalogd_change);

    /// Information about a subscriber's subscription to a specific topic.
    struct TopicSubscription {
      TopicSubscription(bool is_transient, bool populate_min_subscriber_topic_version,
          std::string filter_prefix)
        : is_transient(is_transient),
          populate_min_subscriber_topic_version(populate_min_subscriber_topic_version),
          filter_prefix(std::move(filter_prefix)) {}

      /// Whether entries written by this subscriber should be considered transient.
      const bool is_transient;

      /// Whether min_subscriber_topic_version needs to be filled in for this
      /// subscription.
      const bool populate_min_subscriber_topic_version;

      /// The prefix for which the subscriber wants to see updates.
      const std::string filter_prefix;

      /// The last topic entry version successfully processed by this subscriber. Only
      /// written by a single thread at a time but can be read concurrently.
      AtomicInt64 last_version{TOPIC_INITIAL_VERSION};

      /// Map from the key to the version of a transient update made by this subscriber.
      /// protected by Subscriber:: 'transient_entries_lock_'.
      boost::unordered_map<TopicEntryKey, TopicEntry::Version> transient_entries_;
    };

    /// The set of topics subscribed to, and current state (as seen by this subscriber) of
    /// the topic.
    typedef boost::unordered_map<TopicId, TopicSubscription> Topics;

    /// The Version value used to initialize new Topic subscriptions for this Subscriber.
    static const TopicEntry::Version TOPIC_INITIAL_VERSION;

    const Topics& non_priority_subscribed_topics() const {
      return non_priority_subscribed_topics_;
    }
    const Topics& priority_subscribed_topics() const { return priority_subscribed_topics_; }
    const TNetworkAddress& network_address() const { return network_address_; }
    const SubscriberId& id() const { return subscriber_id_; }
    const RegistrationId& registration_id() const { return registration_id_; }
    TStatestoreSubscriberType::type subscriber_type() const { return subscriber_type_; }
    int64_t catalogd_version() const { return catalogd_version_; }
    const TNetworkAddress& catalogd_address() const { return catalogd_address_; }
    int64_t last_update_catalogd_time() const { return last_update_catalogd_time_; }

    /// Returns the time elapsed (in seconds) since the last heartbeat.
    double SecondsSinceHeartbeat() const {
      return (static_cast<double>(MonotonicMillis() - last_heartbeat_ts_.Load()))
          / 1000.0;
    }

    /// Get the Topics map that would be used to store 'topic_id'.
    const Topics& GetTopicsMapForId(const TopicId& topic_id) const {
      return IsPrioritizedTopic(topic_id) ? priority_subscribed_topics_
                                          : non_priority_subscribed_topics_;
    }
    Topics* GetTopicsMapForId(const TopicId& topic_id) {
      return IsPrioritizedTopic(topic_id) ? &priority_subscribed_topics_
                                          : &non_priority_subscribed_topics_;
    }

    /// Records the fact that updates to this topic are owned by this subscriber.  The
    /// version number of each update (which must be at the corresponding index in
    /// 'versions' is saved so that only those updates which are made most recently by
    /// this subscriber - and not overwritten by another subscriber - are deleted on
    /// failure. If the topic each entry belongs to is not marked as transient, no update
    /// will be recorded. Should not be called concurrently from multiple threads for a
    /// given 'topic_id'.
    ///
    /// Returns false if DeleteAllTransientEntries() was called and 'topic_id' entries
    /// are transient, in which case the caller should delete the entries themselves.
    bool AddTransientEntries(const TopicId& topic_id,
        const std::vector<TTopicItem>& entries,
        const std::vector<TopicEntry::Version>& entry_versions) WARN_UNUSED_RESULT;

    /// Delete all transient topic entries for this subscriber from 'global_topics'.
    ///
    /// Statestore::topics_map_lock_ (in shared mode) must be held by the caller.
    void DeleteAllTransientEntries(TopicMap* global_topics);

    /// Returns the number of transient entries.
    int64_t NumTransientEntries();

    /// Returns the last version of the topic which this subscriber has successfully
    /// processed. Will never decrease.
    TopicEntry::Version LastTopicVersionProcessed(const TopicId& topic_id) const;

    /// Sets the subscriber's last processed version of the topic to the given value. This
    /// should only be set when once a subscriber has succesfully processed the given
    /// update corresponding to this version. Should not be called concurrently from
    /// multiple threads for a given 'topic_id'.
    void SetLastTopicVersionProcessed(const TopicId& topic_id,
        TopicEntry::Version version);

    /// Refresh the subscriber's last heartbeat timestamp to the current monotonic time.
    void RefreshLastHeartbeatTimestamp();

    /// Check if the subscriber is Catalog daemon.
    bool IsCatalogd() const {
      return subscriber_type_ == TStatestoreSubscriberType::CATALOGD;
    }

    /// Check if the subscriber wants to receive the notification of catalogd change.
    bool IsSubscribedCatalogdChange() const {
      return subscribe_catalogd_change_;
    }

    /// The subscriber updates the catalog information.
    void UpdateCatalogInfo(
        int64_t catalogd_version, const TNetworkAddress& catalogd_address);

   private:
    /// Unique human-readable identifier for this subscriber, set by the subscriber itself
    /// on a Register call.
    const SubscriberId subscriber_id_;

    /// Unique identifier for the current registration of this subscriber. A new
    /// registration ID is handed out every time a subscriber successfully calls
    /// RegisterSubscriber() to distinguish between distinct connections from subscribers
    /// with the same subscriber_id_.
    const RegistrationId registration_id_;

    /// The location of the subscriber service that this subscriber runs.
    const TNetworkAddress network_address_;

    /// Subscriber type
    TStatestoreSubscriberType::type subscriber_type_;

    /// Indicate if the subscriber subscribe to the notification of updating catalogd.
    bool subscribe_catalogd_change_;

    /// Maps of topic subscriptions to current TopicSubscription, with separate maps for
    /// priority and non-priority topics. The state describes whether updates on the
    /// topic are 'transient' (i.e., to be deleted upon subscriber failure) or not
    /// and contains the version number of the last update processed by this Subscriber
    /// on the topic. The set of keys is not modified after construction.
    Topics priority_subscribed_topics_;
    Topics non_priority_subscribed_topics_;

    /// The timestamp of the last successful heartbeat in milliseconds. A timestamp much
    /// older than the heartbeat frequency implies an unresponsive subscriber.
    AtomicInt64 last_heartbeat_ts_{0};

    /// Lock held when adding or deleting transient entries. See class comment for lock
    /// acquisition order.
    std::mutex transient_entry_lock_;

    /// True once DeleteAllTransientEntries() has been called during subscriber
    /// unregisteration. Protected by 'transient_entry_lock_'
    bool unregistered_ = false;

    // Version of catalogd.
    int64_t catalogd_version_ = 0L;

    // Address of catalogd.
    TNetworkAddress catalogd_address_;

    // The last time to update the catalogd.
    int64_t last_update_catalogd_time_ = 0L;
  };

  /// Unique identifier for this statestore instance.
  TUniqueId statestore_id_;

  /// Protocol version of the statestore.
  StatestoreServiceVersion::type protocol_version_;

  /// Protects access to subscribers_ and subscriber_uuid_generator_. See the class
  /// comment for the lock acquisition order.
  std::mutex subscribers_lock_;

  /// Map of subscribers currently connected; upon failure their entry is removed from this
  /// map. Subscribers must only be removed by UnregisterSubscriber() which ensures that
  /// the correct cleanup is done. If a subscriber re-registers, it must be unregistered
  /// prior to re-entry into this map.
  //
  /// Subscribers are held in shared_ptrs so that RegisterSubscriber() may overwrite their
  /// entry in this map while UpdateSubscriber() tries to update an existing registration
  /// without risk of use-after-free.
  typedef boost::unordered_map<SubscriberId, std::shared_ptr<Subscriber>> SubscriberMap;
  SubscriberMap subscribers_;

  /// CatalogD Manager
  StatestoreCatalogdMgr catalog_manager_;

  /// Condition variable for sending the notifications of updating catalogd.
  ConditionVariable update_catalod_cv_;

  /// Condition variable for sending the notifications of updating role of statestored.
  ConditionVariable update_statestored_cv_;

  /// Used to generated unique IDs for each new registration.
  boost::uuids::random_generator subscriber_uuid_generator_;

  /// Work item passed to both kinds of subscriber update threads.
  struct ScheduledSubscriberUpdate {
    /// *Earliest* time (in Unix time) that the next message should be sent.
    int64_t deadline;

    /// SubscriberId and RegistrationId of the registered subscriber instance this message
    /// is intended for.
    SubscriberId subscriber_id;
    RegistrationId registration_id;

    ScheduledSubscriberUpdate() {}

    ScheduledSubscriberUpdate(int64_t next_update_time, SubscriberId s_id,
        RegistrationId r_id): deadline(next_update_time), subscriber_id(s_id),
        registration_id(r_id) {}
  };

  /// The statestore has three pools of threads that send messages to subscribers
  /// one-by-one. One pool deals with 'heartbeat' messages that update failure detection
  /// state, and the remaining pools send 'topic update' messages that contain the
  /// actual topic data that a subscriber does not yet have, with one pool dedicated to
  /// a set of special "prioritized" topics.
  ///
  /// Each message is scheduled for some time in the future and each worker thread
  /// will sleep until that time has passed to rate-limit messages. Subscribers are
  /// placed back into the queue once they have been processed. A subscriber may have many
  /// entries in a queue, but no more than one for each registration associated with that
  /// subscriber. Since at most one registration is considered 'live' per subscriber, this
  /// guarantees that subscribers_.size() - 1 'live' subscribers ahead of any subscriber in
  /// the queue.
  ///
  /// Messages may be delayed for any number of reasons, including scheduler
  /// interference, lock unfairness when submitting to the thread pool and head-of-line
  /// blocking when threads are occupied sending messages to slow subscribers
  /// (subscribers are not guaranteed to be in the queue in next-update order).
  ///
  /// Delays for heartbeat messages can result in the subscriber that is kept waiting
  /// assuming that the statestore has failed. Correct configuration of heartbeat message
  /// frequency and subscriber timeout is therefore very important, and depends upon the
  /// cluster size. See --statestore_heartbeat_frequency_ms and
  /// --statestore_subscriber_timeout_seconds. We expect that the provided defaults will
  /// work up to clusters of several hundred nodes.
  ///
  /// Subscribers are therefore not processed in lock-step, and one subscriber may have
  /// seen many more messages than another during the same interval (if the second
  /// subscriber runs slow for any reason).
  enum class UpdateKind {
    TOPIC_UPDATE,
    PRIORITY_TOPIC_UPDATE,
    HEARTBEAT
  };
  ThreadPool<ScheduledSubscriberUpdate> subscriber_topic_update_threadpool_;

  ThreadPool<ScheduledSubscriberUpdate> subscriber_priority_topic_update_threadpool_;

  ThreadPool<ScheduledSubscriberUpdate> subscriber_heartbeat_threadpool_;

  /// Thread that monitors the heartbeats of all subscribers.
  std::unique_ptr<Thread> heartbeat_monitoring_thread_;

  /// Thread to send notification of updating catalogd.
  std::unique_ptr<Thread> update_catalogd_thread_;

  /// Thread to send notification of updating role of statestored.
  std::unique_ptr<Thread> update_statestored_thread_;

  /// Indicates whether the statestore has been initialized and the service is ready.
  std::atomic_bool service_started_{false};

  /// Cache of subscriber clients used for UpdateState() RPCs. Only one client per
  /// subscriber should be used, but the cache helps with the client lifecycle on failure.
  boost::scoped_ptr<StatestoreSubscriberClientCache> update_state_client_cache_;

  /// Cache of subscriber clients used for Heartbeat() RPCs. Separate from
  /// update_state_client_cache_ because we enable TCP-level timeouts for these calls,
  /// whereas they are not safe for UpdateState() RPCs which can take an unbounded amount
  /// of time.
  boost::scoped_ptr<StatestoreSubscriberClientCache> heartbeat_client_cache_;

  /// Cache of subscriber clients used for UpdateCatalogd() RPCs. Only one client per
  /// subscriber should be used, but the cache helps with the client lifecycle on
  /// failure.
  boost::scoped_ptr<StatestoreSubscriberClientCache> update_catalogd_client_cache_;

  /// Cache of subscriber clients used for UpdateStatestoredRole() RPCs. Only one client
  /// per subscriber should be used, but the cache helps with the client lifecycle on
  /// failure.
  boost::scoped_ptr<StatestoreSubscriberClientCache> update_statestored_client_cache_;

  /// statestore client cache - only one client is ever used. Initialized in constructor.
  /// The StatestoreHaClientCache is created with num_retries = 1 and wait_ms = 0.
  /// Connections are still retried, but the retry mechanism is driven by DoRpcWithRetry.
  /// Clients should always use DoRpcWithRetry rather than DoRpc to ensure that both RPCs
  /// and connections are retried.
  boost::scoped_ptr<StatestoreHaClientCache> ha_client_cache_;

  /// Container for the internal statestore service.
  boost::scoped_ptr<ThriftServer> thrift_server_;

  /// Container for the internal statestore HA service.
  boost::scoped_ptr<ThriftServer> ha_thrift_server_;

  /// Pointer to the MetricGroup for this statestore. Not owned.
  MetricGroup* metrics_;

  /// Thrift API implementation which proxies requests onto this Statestore Service
  std::shared_ptr<StatestoreServiceIf> thrift_iface_;

  /// Thrift API implementation which proxies requests onto this Statestore HA Service
  std::shared_ptr<StatestoreHaServiceIf> ha_thrift_iface_;

  /// Failure detector for subscribers. If a subscriber misses a configurable number of
  /// consecutive heartbeat messages, it is considered failed and a) its transient topic
  /// entries are removed and b) its entry in the subscriber map is erased. The
  /// subscriber ID is used to identify peers for failure detection purposes. Subscriber
  /// state is evicted from the failure detector when the subscriber is unregistered,
  /// so old subscribers do not occupy memory and the failure detection state does not
  /// carry over to any new registrations of the previous subscriber.
  boost::scoped_ptr<MissedHeartbeatFailureDetector> failure_detector_;

  /// Lock to protect statestored HA related variables:
  std::mutex ha_lock_;

  /// Network address of peer statestore instance for StatestoreHaService.
  TNetworkAddress peer_statestore_ha_addr_;

  /// Unique identifier for peer statestore instance.
  TUniqueId peer_statestore_id_;

  /// Local network address for StatestoreHaService.
  TNetworkAddress local_statestore_ha_addr_;

  /// True if the statestore instance is active.
  bool is_active_ = false;

  /// The version of active statestore. It's set as the number of microseconds that have
  /// passed since the Unix epoch.
  /// The HA notifications could be received by subscribers out of order due to network
  /// delays. This variable is sent in HA notifications to guarantee that only latest
  /// notification takes effect on subscribers. We can not use an incrementing counter
  /// for this purpose, otherwise the version will be reset when statestored is restarted.
  int64_t active_version_ = 0;

  /// True if the peer statestore instance has been detected.
  bool found_peer_ = false;

  /// True if the statestore instance is in recovery mode.
  bool in_recovery_mode_ = false;

  /// Starting time to enter recovery mode.
  int64_t recovery_start_time_;

  /// Number of HA heartbeat received in active state.
  /// Reset this variable whenever `is_active_` is set to true.
  int num_received_heartbeat_in_active_ = 0;

  /// Disable network if this variable is set as true by statestore service API.
  /// This is only used for unit-test.
  AtomicBool disable_network_{false};

  /// Failure detector for active statestore. If the standby statestore misses a
  /// configurable number of consecutive heartbeat messages, it is considered failed and
  /// if it does not receive heartbeat responses from more than 50% of subscribers,
  /// active statestore enter "looking" state .
  boost::scoped_ptr<MissedHeartbeatFailureDetector> ha_standby_ss_failure_detector_;

  /// Failure detector for standby statestore. It tracks heartbeat messages from the
  /// active statestore.
  boost::scoped_ptr<impala::TimeoutFailureDetector> ha_active_ss_failure_detector_;

  /// Record of connection state between active statestore and subscribers.
  typedef boost::unordered_map<SubscriberId, TStatestoreConnState::type>
      ActiveConnStateMap;
  ActiveConnStateMap active_conn_states_;

  /// Number of subscriber which lost connection with active statestored.
  int failed_conn_state_count_ = 0;

  /// Thread that sends and monitors the HA heartbeats between two statestore instances.
  std::unique_ptr<Thread> ha_monitoring_thread_;

  /// Metric that track the registered, non-failed subscribers.
  IntGauge* num_subscribers_metric_;
  SetMetric<std::string>* subscriber_set_metric_;

  /// Metrics shared across all topics to sum the size in bytes of keys, values and both
  IntGauge* key_size_metric_;
  IntGauge* value_size_metric_;
  IntGauge* topic_size_metric_;

  /// Tracks the distribution of topic-update durations for regular and prioritized topic
  /// updates. This measures the time spent in calling the UpdateState() RPC which
  /// includes network transmission cost and subscriber-side processing time.
  StatsMetric<double>* topic_update_duration_metric_;
  StatsMetric<double>* priority_topic_update_duration_metric_;

  /// Same as above, but for SendHeartbeat() RPCs.
  StatsMetric<double>* heartbeat_duration_metric_;

  /// Metric to count the total number of successful UpdateCatalogd RPCs sent by
  /// statestore.
  IntCounter* successful_update_catalogd_rpc_metric_;

  /// Metric to count the total number of failed UpdateCatalogd RPCs sent by statestore.
  IntCounter* failed_update_catalogd_rpc_metric_;

  /// Metric to count the total number of successful UpdateCStatestoredRole RPCs sent by
  /// statestore.
  IntCounter* successful_update_statestored_role_rpc_metric_;

  /// Metric to count the total number of failed UpdateStatestoredRole RPCs sent by
  /// statestore.
  IntCounter* failed_update_statestored_role_rpc_metric_;

  /// Metric to count the total number of requests for clearing topic entries from
  /// catalogd. Catalogd indicates to clear topic entries when it is restarted or its
  /// role has been changed from standby to active.
  IntCounter* clear_topic_entries_metric_;

  /// Metric that tracks the address of active catalogd for catalogd HA
  StringProperty* active_catalogd_address_metric_;

  /// Metric that tracks if the statestored is active when statestored HA is enabled.
  BooleanProperty* active_status_metric_;

  /// Metric that tracks if the statestore service is started and ready to accept
  /// connections.
  BooleanProperty* service_started_metric_;

  /// Metric that tracks if the statestored is in HA recovery mode.
  BooleanProperty* in_ha_recovery_mode_metric_;

  /// Metric that tracks if the statestored is connected with peer statestored.
  BooleanProperty* connected_peer_metric_;

  /// Utility method to add an update to the given thread pool, and to fail if the thread
  /// pool is already at capacity. Assumes that subscribers_lock_ is held by the caller.
  Status OfferUpdate(const ScheduledSubscriberUpdate& update,
      ThreadPool<ScheduledSubscriberUpdate>* thread_pool) WARN_UNUSED_RESULT;

  /// Sends either a heartbeat or topic update message to the subscriber in 'update' at
  /// the closest possible time to the first member of 'update'. If 'update_kind' is
  /// HEARTBEAT, sends a heartbeat update, otherwise the set of priority/non-priority
  /// pending topic updates is sent. Once complete, the next update is scheduled and
  /// added to the appropriate queue.
  void DoSubscriberUpdate(UpdateKind update_kind, int thread_id,
      const ScheduledSubscriberUpdate& update);

  /// Does the work of updating a single subscriber, by calling UpdateState() on the client
  /// to send a list of topic deltas to the subscriber. If that call fails (either because
  /// the RPC could not be completed, or the subscriber indicated an error), this method
  /// returns a non-OK status immediately without further processing.
  ///
  /// The subscriber may indicated that it skipped processing the message, either because
  /// it was not ready to do so or because it was busy. In that case, the UpdateState() RPC
  /// will return OK (since there was no error) and the output parameter update_skipped is
  /// set to true. Otherwise, any updates returned by the subscriber are applied to their
  /// target topics.
  Status SendTopicUpdate(Subscriber* subscriber, UpdateKind update_kind,
      bool* update_skipped) WARN_UNUSED_RESULT;

  /// Sends a heartbeat message to subscriber. Returns false if there was some error
  /// performing the RPC.
  Status SendHeartbeat(Subscriber* subscriber) WARN_UNUSED_RESULT;

  /// Returns true (and sets subscriber to the corresponding Subscriber object) if a
  /// registered subscriber exists in the subscribers_ map with the given subscriber_id
  /// and registration_id. False otherwise.
  bool FindSubscriber(const SubscriberId& subscriber_id,
      const RegistrationId& registration_id, std::shared_ptr<Subscriber>* subscriber)
      WARN_UNUSED_RESULT;

  /// Unregister a subscriber, removing all of its transient entries and evicting it from
  /// the subscriber map. Callers must hold subscribers_lock_ prior to calling this
  /// method.
  void UnregisterSubscriber(Subscriber* subscriber);

  /// Populates a TUpdateStateRequest with the update state for this subscriber. Iterates
  /// over all updates in all priority or non-priority subscribed topics, based on
  /// 'update_kind'. The given TUpdateStateRequest object is populated with the
  /// changes to the subscribed topics. Takes the topics_map_lock_ and subscribers_lock_.
  void GatherTopicUpdates(const Subscriber& subscriber, UpdateKind update_kind,
      TUpdateStateRequest* update_state_request);

  /// Returns the minimum last processed topic version across all subscribers for the given
  /// topic ID. Calculated by enumerating all subscribers and looking at their
  /// LastTopicVersionProcessed() for this topic. The value returned will always be <=
  /// topics_[topic_id].last_version_. Returns TOPIC_INITIAL_VERSION if no subscribers are
  /// registered to the topic. The subscriber ID to whom the min version belongs can also
  /// be retrieved using the optional subscriber_id output parameter. If multiple
  /// subscribers have the same min version, the subscriber_id may be set to any one of the
  /// matching subscribers.
  //
  /// Must be called holding the subscribers_ lock.
  //
  /// TODO: Update the min subscriber version only when a topic is updated, rather than
  /// each time a subscriber is updated. One way to do this would be to keep a priority
  /// queue in Topic of each subscriber's last processed version of the topic.
  TopicEntry::Version GetMinSubscriberTopicVersion(
      const TopicId& topic_id, SubscriberId* subscriber_id = NULL);

  /// Returns true if this topic should be handled by the priority pool.
  static bool IsPrioritizedTopic(const std::string& topic);

  /// Return human-readable name for 'kind'.
  static const char* GetUpdateKindName(UpdateKind kind);

  /// Return the thread pool to process updates of 'kind'.
  ThreadPool<ScheduledSubscriberUpdate>* GetThreadPool(UpdateKind kind);

  /// Webpage handler: upon return, 'document' will contain a list of topics as follows:
  /// "topics": [
  /// {
  ///   "topic_id": "catalog-update",
  ///   "num_entries": 1165,
  ///   "version": 2476,
  ///   "oldest_version": 2476,
  ///   "oldest_id": "henry-impala:26000",
  ///   "key_size": "42.94 KB",
  ///   "value_size": "9.54 MB",
  ///   "total_size": "9.58 MB"
  /// }, ]
  void TopicsHandler(const Webserver::WebRequest& req, rapidjson::Document* document);

  /// Webpage handler: upon return 'document' will contain a list of subscribers as
  /// follows:
  /// "subscribers": [
  /// {
  ///   "id": "henry-impala:26000",
  ///   "address": "henry-impala:23020",
  ///   "num_topics": 1,
  ///   "num_transient": 0,
  ///   "registration_id": "414d28c84930d987:abcffd70b3346fb7"
  ///   }
  /// ]
  void SubscribersHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);

  /// Monitors the heartbeats of all subscribers every
  /// FLAGS_heartbeat_monitoring_frequency_ms milliseconds. If a subscriber's
  /// last_heartbeat_ts_ has not been updated in that interval, it logs the subscriber's
  /// id.
  [[noreturn]] void MonitorSubscriberHeartbeat();

  /// Monitors the notification of updating catalogd.
  [[noreturn]] void MonitorUpdateCatalogd();

  /// Monitors the notification of updating statestored's role.
  [[noreturn]] void MonitorUpdateStatestoredRole();

  /// Send notification of updating catalogd to the coordinators.
  void SendUpdateCatalogdNotification(int64_t* last_active_catalogd_version,
      vector<std::shared_ptr<Subscriber>>& receivers);

  /// Send notification of updating statestored's role to all subscribers.
  void SendUpdateStatestoredRoleNotification(int64_t* last_active_statestored_version,
      vector<std::shared_ptr<Subscriber>>& receivers);

  /// Raw callback to indicate whether the service is ready.
  void HealthzHandler(const Webserver::WebRequest& req, std::stringstream* data,
      HttpStatusCode* response);

  // Return true if this statestore instance is in recovery mode.
  bool IsInRecoveryMode();

  /// Json callback for /catalog_ha_info.
  void CatalogHAInfoHandler(const Webserver::WebRequest& req,
      rapidjson::Document* document);
};

} // namespace impala
