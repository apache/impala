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

#ifndef STATESTORE_STATESTORE_H
#define STATESTORE_STATESTORE_H

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include <boost/thread/shared_mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/uuid/uuid_generators.hpp>

#include "common/atomic.h"
#include "common/status.h"
#include "gen-cpp/StatestoreService.h"
#include "gen-cpp/StatestoreSubscriber.h"
#include "gen-cpp/Types_types.h"
#include "rpc/thrift-client.h"
#include "runtime/client-cache.h"
#include "runtime/timestamp-value.h"
#include "statestore/failure-detector.h"
#include "statestore/statestore-subscriber-client-wrapper.h"
#include "util/aligned-new.h"
#include "util/collection-metrics.h"
#include "util/metrics.h"
#include "util/thread-pool.h"
#include "util/webserver.h"

namespace impala {

class Status;

typedef ClientCache<StatestoreSubscriberClientWrapper> StatestoreSubscriberClientCache;
typedef TUniqueId RegistrationId;

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
/// +================+
/// | Implementation |
/// +================+
///
/// Locking:
/// --------
/// The lock acquisition order is:
/// 1. 'subscribers_lock_'
/// 2. 'topics_map_lock_'
/// 3. Subscriber::transient_entry_lock_
/// 4. Topic::lock_ (terminal)
class Statestore : public CacheLineAligned {
 public:
  /// A SubscriberId uniquely identifies a single subscriber, and is
  /// provided by the subscriber at registration time.
  typedef std::string SubscriberId;

  /// A TopicId uniquely identifies a single topic
  typedef std::string TopicId;

  /// A TopicEntryKey uniquely identifies a single entry in a topic
  typedef std::string TopicEntryKey;

  /// The only constructor; initialises member variables only.
  Statestore(MetricGroup* metrics);

  /// Initialize the ThreadPools used for updates and heartbeats. Returns an error if
  /// ThreadPool initialization fails.
  Status Init() WARN_UNUSED_RESULT;

  /// Registers a new subscriber with the given unique subscriber ID, running a subscriber
  /// service at the given location, with the provided list of topic subscriptions.
  /// The registration_id output parameter is the unique ID for this registration, used to
  /// distinguish old registrations from new ones for the same subscriber.
  //
  /// If a registration already exists for this subscriber, the old registration is removed
  /// and a new one is created. Subscribers may receive an update intended for the old
  /// registration, since one may be in flight when a new RegisterSubscriber() is received.
  Status RegisterSubscriber(const SubscriberId& subscriber_id,
      const TNetworkAddress& location,
      const std::vector<TTopicRegistration>& topic_registrations,
      RegistrationId* registration_id) WARN_UNUSED_RESULT;

  void RegisterWebpages(Webserver* webserver);

  /// The main processing loop. Blocks until the exit flag is set.
  void MainLoop();

  /// Returns the Thrift API interface that proxies requests onto the local Statestore.
  const boost::shared_ptr<StatestoreServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }

  /// Names of prioritized topics that are handled in a separate threadpool. The topic
  /// names are hardcoded here for expediency. Ideally we would have a more generic
  /// interface for specifying prioritized topics, but for now we only have a small
  /// fixed set of topics.
  /// Topic tracking the set of live Impala daemon instances.
  static const std::string IMPALA_MEMBERSHIP_TOPIC;
  /// Topic tracking the state of admission control on all coordinators.
  static const std::string IMPALA_REQUEST_QUEUE_TOPIC;
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
    /// than 'last_processed_version' (not inclusive).
    ///
    /// Safe to call concurrently from multiple threads (for different subscribers).
    /// Acquires a shared read lock for the topic.
    void BuildDelta(const SubscriberId& subscriber_id,
        TopicEntry::Version last_processed_version, TTopicDelta* delta);

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
        const std::vector<TTopicRegistration>& subscribed_topics);

    /// Information about a subscriber's subscription to a specific topic.
    struct TopicSubscription {
      TopicSubscription(bool is_transient) : is_transient(is_transient) {}

      /// Whether entries written by this subscriber should be considered transient.
      const bool is_transient;

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

    /// Sets the subscriber's last processed version of the topic to the given value.  This
    /// should only be set when once a subscriber has succesfully processed the given
    /// update corresponding to this version. Should not be called concurrently from
    /// multiple threads for a given 'topic_id'.
    void SetLastTopicVersionProcessed(const TopicId& topic_id,
        TopicEntry::Version version);

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

    /// Maps of topic subscriptions to current TopicSubscription, with separate maps for
    /// priority and non-priority topics. The state describes whether updates on the
    /// topic are 'transient' (i.e., to be deleted upon subscriber failure) or not
    /// and contains the version number of the last update processed by this Subscriber
    /// on the topic. The set of keys is not modified after construction.
    Topics priority_subscribed_topics_;
    Topics non_priority_subscribed_topics_;

    /// Lock held when adding or deleting transient entries. See class comment for lock
    /// acquisition order.
    boost::mutex transient_entry_lock_;

    /// True once DeleteAllTransientEntries() has been called during subscriber
    /// unregisteration. Protected by 'transient_entry_lock_'
    bool unregistered_ = false;
  };

  /// Protects access to subscribers_ and subscriber_uuid_generator_. See the class
  /// comment for the lock acquisition order.
  boost::mutex subscribers_lock_;

  /// Map of subscribers currently connected; upon failure their entry is removed from this
  /// map. Subscribers must only be removed by UnregisterSubscriber() which ensures that
  /// the correct cleanup is done. If a subscriber re-registers, it must be unregistered
  /// prior to re-entry into this map.
  //
  /// Subscribers are held in shared_ptrs so that RegisterSubscriber() may overwrite their
  /// entry in this map while UpdateSubscriber() tries to update an existing registration
  /// without risk of use-after-free.
  typedef boost::unordered_map<SubscriberId, std::shared_ptr<Subscriber>>
    SubscriberMap;
  SubscriberMap subscribers_;

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

  /// Cache of subscriber clients used for UpdateState() RPCs. Only one client per
  /// subscriber should be used, but the cache helps with the client lifecycle on failure.
  boost::scoped_ptr<StatestoreSubscriberClientCache> update_state_client_cache_;

  /// Cache of subscriber clients used for Heartbeat() RPCs. Separate from
  /// update_state_client_cache_ because we enable TCP-level timeouts for these calls,
  /// whereas they are not safe for UpdateState() RPCs which can take an unbounded amount
  /// of time.
  boost::scoped_ptr<StatestoreSubscriberClientCache> heartbeat_client_cache_;

  /// Thrift API implementation which proxies requests onto this Statestore
  boost::shared_ptr<StatestoreServiceIf> thrift_iface_;

  /// Failure detector for subscribers. If a subscriber misses a configurable number of
  /// consecutive heartbeat messages, it is considered failed and a) its transient topic
  /// entries are removed and b) its entry in the subscriber map is erased.
  boost::scoped_ptr<MissedHeartbeatFailureDetector> failure_detector_;

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
  void TopicsHandler(const Webserver::ArgumentMap& args, rapidjson::Document* document);

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
  void SubscribersHandler(const Webserver::ArgumentMap& args,
      rapidjson::Document* document);

};

}

#endif
