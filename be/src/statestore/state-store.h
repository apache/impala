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

#ifndef STATESTORE_STATE_STORE2_H
#define STATESTORE_STATE_STORE2_H

#include <stdint.h>
#include <string>
#include <vector>
#include <map>
#include <boost/unordered_map.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>

#include "gen-cpp/Types_types.h"
#include "gen-cpp/StateStoreSubscriber.h"
#include "gen-cpp/StateStoreService.h"
#include "util/metrics.h"
#include "util/non-primitive-metrics.h"
#include "util/thrift-client.h"
#include "util/webserver.h"
#include "runtime/client-cache.h"
#include "statestore/failure-detector.h"

namespace impala {

using namespace impala;

class Status;

// The StateStore is a soft-state key-value store that maintains a set
// of Topics, which are maps from string keys to byte array values.
//
// Topics are subscribed to by subscribers, which are remote clients
// of the state-store which express an interest in some set of
// Topics. The state-store sends topic updates to subscribers via
// periodic heartbeat messages, which are also used to detect the
// liveness of a subscriber.
//
// Subscribers, in return, send topic updates to the state-store to
// merge with the current topic. These updates are then sent to all
// other subscribers in the next heartbeat.
//
// Topic entries usually have human-readable keys, and values which
// are some serialised representation of a data structure, e.g. a
// Thrift struct. The contents of a value's bye string is opaque to
// the state-store, which maintains no information about how to
// deserialise it. Subscribers must use convention to interpret each
// other's updates.
//
// A subscriber may have marked some updates that it made as
// 'transient', which implies that those entries should be deleted
// once the subscriber is no longer connected (this is judged by the
// state-store's failure-detector, which will mark a subscriber as
// failed when it has not responded to a number of successive
// heartbeats). Transience is tracked per-topic-per-subscriber, so two
// different subscribers may treat the same topic differently wrt to
// the transience of their updates.
//
// TODO: Compute deltas based on subscriber last-seen version numbers
// and only send the changes.
class StateStore {
 public:
  // A SubscriberId uniquely identifies a single subscriber, and is
  // provided by the susbcriber at registration time.
  typedef std::string SubscriberId;

  // A TopicId uniquely identifies a single topic
  typedef std::string TopicId;

  // A TopicEntryKey uniquely identifies a single entry in a topic
  typedef std::string TopicEntryKey;

  // The only constructor; initialises member variables only.
  StateStore(Metrics* metrics);

  // Registers a new subscriber with the given unique subscriber ID,
  // running a heartbeat service at the given location, with the
  // provided list of topic subscriptions.
  Status RegisterSubscriber(const SubscriberId& subscriber_id,
      const TNetworkAddress& location,
      const std::vector<TTopicRegistration>& topic_registrations);

  void RegisterWebpages(Webserver* webserver);

  // The main processing loop. Starts a number of threads to send
  // heartbeats to subscribers, and coordinates handing work to them
  // until told to exit via SetExitFlag. This method blocks until the
  // exit flag is set.
  //
  // Returns OK unless there is an unrecoverable error.
  Status MainLoop();

  // Returns the Thrift API interface that proxies requests onto the local StateStore.
  const boost::shared_ptr<StateStoreServiceIf>& thrift_iface() const {
    return thrift_iface_;
  }

  // Tells the StateStore to shut down. Does not wait for the
  // processing loop to exit before returning.
  void SetExitFlag();

 private:
  // A TopicEntry is a single entry in a topic, and logically is a
  // <string, byte string> pair. If the byte string is NULL, the entry
  // has been deleted, but may be retained to track changes to send to
  // subscribers.
  class TopicEntry {
   public:
    // A Value is a string of bytes, for which std::string is a
    // convenient representation.
    typedef std::string Value;

    // A version is a monotonically increasing counter. Each update to
    // a topic has its own unique version with the guarantee that
    // sequentially later updates have larger version numbers.
    typedef uint64_t Version;

    // Representation of an empty Value
    static const Value NULL_VALUE;

    // Sets the value of this entry to the byte / length
    // pair. NULL_VALUE implies this entry has been deleted.  The
    // caller is responsible for ensuring, if required, that the
    // version parameter is larger than the current version()
    // TODO: Consider enforcing version monotonicity here.
    void SetValue(const Value& bytes, Version version);

    TopicEntry() : value_(NULL_VALUE), version_(0L) { }

    const Value& value() const { return value_; }
    uint64_t version() const { return version_; }
    uint32_t length() const { return value_.size(); }

   private:
    // Byte string value, owned by this TopicEntry. The value is
    // opaque to the state-store, and is interpreted only by
    // subscribers.
    Value value_;

    // The version of this entry. Every update is assigned a
    // monotonically increasing version number so that only the
    // minimal set of changes can be sent from the state-store to a
    // subscriber.
    Version version_;
  };

  // Map from TopicEntryKey to TopicEntry, maintained by a Topic object.
  typedef boost::unordered_map<TopicEntryKey, TopicEntry> TopicEntryMap;

  // A Topic is logically a map between a string key and a sequence of
  // bytes. A <string, bytes> pair is a TopicEntry.
  //
  // Each topic has a unique version number that tracks the number of
  // updates made to the topic. This is to support sending only the
  // delta of changes on every update.
  class Topic {
   public:
    Topic(const TopicId& topic_id) : topic_id_(topic_id) { }

    // Adds an entry with the given key. If bytes == NULL_VALUE, the entry
    // is considered deleted, and may be garbage collected in the
    // future. The entry is assigned a new version number by the Topic,
    // and that version number is returned.
    //
    // Must be called holding the topic lock
    TopicEntry::Version Put(const TopicEntryKey& key, const TopicEntry::Value& bytes);

    // Utility method to support removing transient entries. We track
    // the version numbers of entries added by subscribers, and remove
    // entries with the same version number when that subscriber fails
    // (the same entry may exist, but may have been updated by another
    // subscriber giving it a new version number)
    //
    // Deletion means setting the entry's value to NULL and
    // incrementing its version number.
    //
    // Must be called holding the topic lock
    void DeleteIfVersionsMatch(TopicEntry::Version version, const TopicEntryKey& key);

    const TopicId& id() const { return topic_id_; }
    const TopicEntryMap& entries() const { return entries_; }

   private:
    // Map from topic entry key to topic entry.
    TopicEntryMap entries_;

    // Unique identifier for this topic. Should be human-readable.
    const TopicId topic_id_;

    // Incremented on every Put(..), and each TopicEntry is tagged
    // with the current version.
    TopicEntry::Version last_version_;
  };

  // A note about locking: no two mutexes should be held at the same
  // time. It has so far been possible to implement mutual exclusion
  // without requiring more than one lock to be held
  // concurrently. Subscribers and Topics should be accessed under
  // their own coarse locks, and worker threads will use worker_lock_
  // to ensure safe access to the subscriber work queue.

  // Protects access to exit_flag_, but is used mostly to ensure
  // visibility of updates between threads..
  boost::mutex exit_flag_lock_;
  bool exit_flag_;

  // Controls access to topics_
  boost::mutex topic_lock_;

  // The entire set of topics tracked by the state-store
  typedef boost::unordered_map<TopicId, Topic> TopicMap;
  TopicMap topics_;

  // The state-store-side representation of an individual subscriber
  // client, which tracks a variety of bookkeeping information. This
  // includes the list of subscribed topics (and whether updates to
  // them should be deleted on failure), the list of updates made by
  // this subscriber (in order to be able to efficiently delete them
  // on failure), and the subscriber's ID and network location.
  class Subscriber {
   public:
    Subscriber(const SubscriberId& subscriber_id, const TNetworkAddress& network_address,
               const std::vector<TTopicRegistration>& subscribed_topics);

    // The set of topics subscribed to, and whether entries to each
    // topic written by this subcriber should be considered transient.
    typedef boost::unordered_map<TopicId, bool> Topics;

    const Topics& subscribed_topics() const { return subscribed_topics_; }
    const TNetworkAddress& network_address() const { return network_address_; }
    const SubscriberId& id() const { return subscriber_id_; }

    // Records the fact that an update to this topic is owned by this
    // subscriber.  The version number of the update is saved so that
    // only those updates which are made most recently by this
    // subscriber - and not overwritten by another subscriber - are
    // deleted on failure. If the topic the entry belongs to is not
    // marked as transient, no update will be recorded.
    void AddTransientUpdate(const TopicId& topic_id, const TopicEntryKey& topic_key,
        TopicEntry::Version version);

    // Map from the topic / key pair to the version of a transient
    // update made by this subscriber.
    typedef boost::unordered_map<std::pair<TopicId, TopicEntryKey>, TopicEntry::Version>
        TransientEntryMap;

    const TransientEntryMap& transient_entries() const { return transient_entries_; }

   private:
    // Unique human-readable identifier for this subscriber, set by
    // the subscriber itself on a Register call
    const SubscriberId subscriber_id_;

    // The location of the heartbeat service that this subscriber runs
    const TNetworkAddress network_address_;

    // List of subscriber topics, with a boolean for each describing
    // whether updates on that topic are 'transient' (i.e., to be
    // deleted upon subscriber failure) or not.
    boost::unordered_map<TopicId, bool> subscribed_topics_;

    // List of updates made by this subscriber so that transient
    // entries may be deleted on failure
    TransientEntryMap transient_entries_;
  };

  // Protects access to subscribers_
  boost::mutex subscribers_lock_;

  // Map of subscribers currently connected; upon failure their entry
  // is removed from this map.
  typedef boost::unordered_map<SubscriberId, Subscriber> SubscriberMap;
  SubscriberMap subscribers_;

  // Cache of subscriber clients. Only one client per subscriber
  // should be used, but the cache helps with the client lifecycle on
  // failure.
  boost::scoped_ptr<ClientCache<StateStoreSubscriberClient> > client_cache_;

  // Thrift API implementation which proxies requests onto this StateStore
  boost::shared_ptr<StateStoreServiceIf> thrift_iface_;

  // Failure detector for subscribers. If a subscriber misses a
  // configurable number of consecutive heartbeats, it is considered
  // failed and a) its transient topic entries are removed and b) its
  // entry in the subscriber map is erased.
  boost::scoped_ptr<MissedHeartbeatFailureDetector> failure_detector_;

  // Metric that track the registered, non-failed subscribers.
  Metrics::IntMetric* num_subscribers_metric_;
  SetMetric<std::string>* subscriber_set_metric_;

  // Shared mutex that protects all subsequent members, which are used
  // to coordinate work between the master thread and the worker
  // threads that send heartbeats to subscribers.
  boost::mutex worker_lock_;

  // Shared work queue between all worker threads. Each entry is a
  // subscriber to which to send a heartbeat.
  std::vector<Subscriber*> subscriber_work_queue_;

  // number of subscribers remaining to be processed
  uint32_t num_subscribers_remaining_;

  // Both subsequent condition variables should be wait()'ed on whilst
  // worker_lock_ is held.

  // Condition variable that indicates more work to do on the queue
  boost::condition_variable work_available_;

  // Condition variable that indicates the last subscriber has been
  // processed, so the main loop should wake up to re-fill the
  // subscriber queue.
  boost::condition_variable last_subscriber_processed_;

  // This method is executed by several worker threads, which
  // collectively read a list of active subscribers and send
  // heartbeats to each one (by calling ProcessOneSubscriber).
  void SubscriberUpdateLoop();

  // Does the work of updating a single subscriber. Tries to call
  // UpdateState on the client. If that fails, informs the failure
  // detector, and if the client is failed, adds it to the list of
  // failed subscribers. Otherwise, it sends the deltas to the
  // subscriber, and receives and processes a list of updates.
  Status ProcessOneSubscriber(Subscriber* subscriber);

  // True if the shutdown flag has been set true, false otherwise.
  bool ShouldExit();

  // Webpage handler: prints the list of all topics and their
  // subscription counts
  void TopicsHandler(const Webserver::ArgumentMap& args, std::stringstream* output);

  // Webpage handler: prints the list of subscribers
  void SubscribersHandler(const Webserver::ArgumentMap& args, std::stringstream* output);

};

}

#endif
