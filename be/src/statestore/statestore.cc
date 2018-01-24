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

#include "statestore/statestore.h"

#include <algorithm>
#include <tuple>
#include <utility>

#include <boost/lexical_cast.hpp>
#include <boost/thread.hpp>
#include <thrift/Thrift.h>
#include <gutil/strings/substitute.h>

#include "common/status.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rpc/thrift-util.h"
#include "statestore/failure-detector.h"
#include "statestore/statestore-subscriber-client-wrapper.h"
#include "util/debug-util.h"
#include "util/logging-support.h"
#include "util/openssl-util.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

using boost::shared_lock;
using boost::shared_mutex;
using boost::upgrade_lock;
using boost::upgrade_to_unique_lock;
using std::forward_as_tuple;
using std::piecewise_construct;
using namespace apache::thrift;
using namespace impala;
using namespace rapidjson;
using namespace strings;

DEFINE_int32(statestore_max_missed_heartbeats, 10, "Maximum number of consecutive "
    "heartbeat messages an impalad can miss before being declared failed by the "
    "statestore.");

DEFINE_int32(statestore_num_update_threads, 10, "(Advanced) Number of threads used to "
    " send topic updates in parallel to all registered subscribers.");
DEFINE_int32(statestore_update_frequency_ms, 2000, "(Advanced) Frequency (in ms) with"
    " which the statestore sends topic updates to subscribers.");

// Priority updates are sent out much more frequently. They are assumed to be small
// amounts of data that take a small amount of time to process. Assuming each update
// takes < 1ms to process, sending out an update every 100ms will consume less than
// 1% of a CPU on each subscriber.
DEFINE_int32(statestore_num_priority_update_threads, 10, "(Advanced) Number of threads "
    "used to send prioritized topic updates in parallel to all registered subscribers.");
DEFINE_int32(statestore_priority_update_frequency_ms, 100, "(Advanced) Frequency (in ms) "
    "with which the statestore sends prioritized topic updates to subscribers.");

DEFINE_int32(statestore_num_heartbeat_threads, 10, "(Advanced) Number of threads used to "
    " send heartbeats in parallel to all registered subscribers.");
DEFINE_int32(statestore_heartbeat_frequency_ms, 1000, "(Advanced) Frequency (in ms) with"
    " which the statestore sends heartbeat heartbeats to subscribers.");

DEFINE_int32(state_store_port, 24000, "port where StatestoreService is running");

DEFINE_int32(statestore_heartbeat_tcp_timeout_seconds, 3, "(Advanced) The time after "
    "which a heartbeat RPC to a subscriber will timeout. This setting protects against "
    "badly hung machines that are not able to respond to the heartbeat RPC in short "
    "order");

DEFINE_int32(statestore_max_subscribers, 10000, "Used to control the maximum size "
    "of the pending topic-update queue. There is at most one entry per subscriber.");

// If this value is set too low, it's possible that UpdateState() might timeout during a
// working invocation, and only a restart of the statestore with a change in value would
// allow progress to be made. If set too high, a hung subscriber will waste an update
// thread for much longer than it needs to. We choose 5 minutes as a safe default because
// large catalogs can take a very long time to process, but rarely more than a minute. The
// loss of a single thread for five minutes should usually be invisible to the user; if
// there is a correlated set of machine hangs that exhausts most threads the cluster can
// already be said to be in a bad state. Note that the heartbeat mechanism will still
// evict those subscribers, so many queries will continue to operate.
DEFINE_int32(statestore_update_tcp_timeout_seconds, 300, "(Advanced) The time after "
    "which an update RPC to a subscriber will timeout. This setting protects against "
    "badly hung machines that are not able to respond to the update RPC in short "
    "order.");

// Metric keys
// TODO: Replace 'backend' with 'subscriber' when we can coordinate a change with CM
const string STATESTORE_LIVE_SUBSCRIBERS = "statestore.live-backends";
const string STATESTORE_LIVE_SUBSCRIBERS_LIST = "statestore.live-backends.list";
const string STATESTORE_TOTAL_KEY_SIZE_BYTES = "statestore.total-key-size-bytes";
const string STATESTORE_TOTAL_VALUE_SIZE_BYTES = "statestore.total-value-size-bytes";
const string STATESTORE_TOTAL_TOPIC_SIZE_BYTES = "statestore.total-topic-size-bytes";
const string STATESTORE_UPDATE_DURATION = "statestore.topic-update-durations";
const string STATESTORE_PRIORITY_UPDATE_DURATION =
    "statestore.priority-topic-update-durations";
const string STATESTORE_HEARTBEAT_DURATION = "statestore.heartbeat-durations";

// Initial version for each Topic registered by a Subscriber. Generally, the Topic will
// have a Version that is the MAX() of all entries in the Topic, but this initial
// value needs to be less than TopicEntry::TOPIC_ENTRY_INITIAL_VERSION to distinguish
// between the case where a Topic is empty and the case where the Topic only contains
// an entry with the initial version.
const Statestore::TopicEntry::Version Statestore::Subscriber::TOPIC_INITIAL_VERSION = 0;

// Updates or heartbeats that miss their deadline by this much are logged.
const uint32_t DEADLINE_MISS_THRESHOLD_MS = 2000;

const string Statestore::IMPALA_MEMBERSHIP_TOPIC("impala-membership");
const string Statestore::IMPALA_REQUEST_QUEUE_TOPIC("impala-request-queue");

typedef ClientConnection<StatestoreSubscriberClientWrapper> StatestoreSubscriberConn;

class StatestoreThriftIf : public StatestoreServiceIf {
 public:
  StatestoreThriftIf(Statestore* statestore)
      : statestore_(statestore) {
    DCHECK(statestore_ != NULL);
  }

  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& params) {
    RegistrationId registration_id;
    Status status = statestore_->RegisterSubscriber(params.subscriber_id,
        params.subscriber_location, params.topic_registrations, &registration_id);
    status.ToThrift(&response.status);
    response.__set_registration_id(registration_id);
  }
 private:
  Statestore* statestore_;
};

void Statestore::TopicEntry::SetValue(const Statestore::TopicEntry::Value& bytes,
    TopicEntry::Version version) {
  DCHECK_GT(bytes.size(), 0);
  value_ = bytes;
  version_ = version;
}

vector<Statestore::TopicEntry::Version> Statestore::Topic::Put(
    const std::vector<TTopicItem>& entries) {
  vector<Statestore::TopicEntry::Version> versions;
  versions.reserve(entries.size());

  // Acquire exclusive lock - we are modifying the topic.
  lock_guard<shared_mutex> write_lock(lock_);
  for (const TTopicItem& entry: entries) {
    TopicEntryMap::iterator entry_it = entries_.find(entry.key);
    int64_t key_size_delta = 0;
    int64_t value_size_delta = 0;
    if (entry_it == entries_.end()) {
      entry_it = entries_.emplace(entry.key, TopicEntry()).first;
      key_size_delta += entry.key.size();
    } else {
      // Delete the old entry from the version history. There is no need to search the
      // version_history because there should only be at most a single entry in the
      // history at any given time.
      topic_update_log_.erase(entry_it->second.version());
      value_size_delta -= entry_it->second.value().size();
    }
    value_size_delta += entry.value.size();

    entry_it->second.SetValue(entry.value, ++last_version_);
    entry_it->second.SetDeleted(entry.deleted);
    topic_update_log_.emplace(entry_it->second.version(), entry.key);

    total_key_size_bytes_ += key_size_delta;
    total_value_size_bytes_ += value_size_delta;
    DCHECK_GE(total_key_size_bytes_, static_cast<int64_t>(0));
    DCHECK_GE(total_value_size_bytes_, static_cast<int64_t>(0));
    key_size_metric_->Increment(key_size_delta);
    value_size_metric_->Increment(value_size_delta);
    topic_size_metric_->Increment(key_size_delta + value_size_delta);
    versions.push_back(entry_it->second.version());
  }
  return versions;
}

void Statestore::Topic::DeleteIfVersionsMatch(TopicEntry::Version version,
    const Statestore::TopicEntryKey& key) {
  // Acquire exclusive lock - we are modifying the topic.
  lock_guard<shared_mutex> write_lock(lock_);
  TopicEntryMap::iterator entry_it = entries_.find(key);
  if (entry_it != entries_.end() && entry_it->second.version() == version) {
    // Add a new entry with the the version history for this deletion and remove the old
    // entry
    topic_update_log_.erase(version);
    topic_update_log_.emplace(++last_version_, key);
    value_size_metric_->Increment(entry_it->second.value().size());
    topic_size_metric_->Increment(entry_it->second.value().size());
    entry_it->second.SetDeleted(true);
    entry_it->second.SetVersion(last_version_);
  }
}

void Statestore::Topic::BuildDelta(const SubscriberId& subscriber_id,
    TopicEntry::Version last_processed_version, TTopicDelta* delta) {
  // If the subscriber version is > 0, send this update as a delta. Otherwise, this is
  // a new subscriber so send them a non-delta update that includes all entries in the
  // topic.
  delta->is_delta = last_processed_version > Subscriber::TOPIC_INITIAL_VERSION;
  delta->__set_from_version(last_processed_version);
  {
    // Acquire shared lock - we are not modifying the topic.
    shared_lock<shared_mutex> read_lock(lock_);
    TopicUpdateLog::const_iterator next_update =
        topic_update_log_.upper_bound(last_processed_version);

    uint64_t topic_size = 0;
    for (; next_update != topic_update_log_.end(); ++next_update) {
      TopicEntryMap::const_iterator itr = entries_.find(next_update->second);
      DCHECK(itr != entries_.end());
      const TopicEntry& topic_entry = itr->second;
      // Don't send deleted entries for non-delta updates.
      if (!delta->is_delta && topic_entry.is_deleted()) {
        continue;
      }
      delta->topic_entries.push_back(TTopicItem());
      TTopicItem& delta_entry = delta->topic_entries.back();
      delta_entry.key = itr->first;
      delta_entry.value = topic_entry.value();
      delta_entry.deleted = topic_entry.is_deleted();
      topic_size += delta_entry.key.size() + delta_entry.value.size();
    }

    if (!delta->is_delta &&
        last_version_ > Subscriber::TOPIC_INITIAL_VERSION) {
      VLOG_QUERY << "Preparing initial " << delta->topic_name
                 << " topic update for " << subscriber_id << ". Size = "
                 << PrettyPrinter::Print(topic_size, TUnit::BYTES);
    }

    if (topic_update_log_.size() > 0) {
      // The largest version for this topic will be the last entry in the version history
      // map.
      delta->__set_to_version(topic_update_log_.rbegin()->first);
    } else {
      // There are no updates in the version history
      delta->__set_to_version(Subscriber::TOPIC_INITIAL_VERSION);
    }
  }
}
void Statestore::Topic::ToJson(Document* document, Value* topic_json) {
  // Acquire shared lock - we are not modifying the topic.
  shared_lock<shared_mutex> read_lock(lock_);
  Value topic_id(topic_id_.c_str(), document->GetAllocator());
  topic_json->AddMember("topic_id", topic_id, document->GetAllocator());
  topic_json->AddMember("num_entries",
      static_cast<uint64_t>(entries_.size()),
      document->GetAllocator());
  topic_json->AddMember("version", last_version_, document->GetAllocator());

  int64_t key_size = total_key_size_bytes_;
  int64_t value_size = total_value_size_bytes_;
  Value key_size_json(PrettyPrinter::Print(key_size, TUnit::BYTES).c_str(),
      document->GetAllocator());
  topic_json->AddMember("key_size", key_size_json, document->GetAllocator());
  Value value_size_json(PrettyPrinter::Print(value_size, TUnit::BYTES).c_str(),
      document->GetAllocator());
  topic_json->AddMember("value_size", value_size_json, document->GetAllocator());
  Value total_size_json(
      PrettyPrinter::Print(key_size + value_size, TUnit::BYTES).c_str(),
      document->GetAllocator());
  topic_json->AddMember("total_size", total_size_json, document->GetAllocator());
  topic_json->AddMember("prioritized", IsPrioritizedTopic(topic_id_),
      document->GetAllocator());
}

Statestore::Subscriber::Subscriber(const SubscriberId& subscriber_id,
    const RegistrationId& registration_id, const TNetworkAddress& network_address,
    const vector<TTopicRegistration>& subscribed_topics)
    : subscriber_id_(subscriber_id),
      registration_id_(registration_id),
      network_address_(network_address) {
  for (const TTopicRegistration& topic: subscribed_topics) {
    GetTopicsMapForId(topic.topic_name)->emplace(piecewise_construct,
        forward_as_tuple(topic.topic_name), forward_as_tuple(topic.is_transient));
  }
}

bool Statestore::Subscriber::AddTransientEntries(const TopicId& topic_id,
    const vector<TTopicItem>& entries,
    const vector<TopicEntry::Version>& entry_versions) {
  lock_guard<mutex> l(transient_entry_lock_);
  DCHECK_EQ(entries.size(), entry_versions.size());
  // Only record the update if the topic is transient
  Topics* subscribed_topics = GetTopicsMapForId(topic_id);
  Topics::iterator topic_it = subscribed_topics->find(topic_id);
  DCHECK(topic_it != subscribed_topics->end());
  if (topic_it->second.is_transient) {
    if (unregistered_) return false;
    for (int i = 0; i < entries.size(); ++i) {
      topic_it->second.transient_entries_[entries[i].key] = entry_versions[i];
    }
  }
  return true;
}

void Statestore::Subscriber::DeleteAllTransientEntries(TopicMap* global_topics) {
  lock_guard<mutex> l(transient_entry_lock_);
  for (const Topics* subscribed_topics :
      {&priority_subscribed_topics_, &non_priority_subscribed_topics_}) {
    for (const auto& topic : *subscribed_topics) {
      auto global_topic_it = global_topics->find(topic.first);
      DCHECK(global_topic_it != global_topics->end());
      for (auto& transient_entry : topic.second.transient_entries_) {
        global_topic_it->second.DeleteIfVersionsMatch(transient_entry.second,
            transient_entry.first);
      }
    }
  }
  unregistered_ = true;
}

int64_t Statestore::Subscriber::NumTransientEntries() {
  lock_guard<mutex> l(transient_entry_lock_);
  int64_t num_entries = 0;
  for (const Topics* subscribed_topics :
      {&priority_subscribed_topics_, &non_priority_subscribed_topics_}) {
    for (const auto& topic : *subscribed_topics) {
      num_entries += topic.second.transient_entries_.size();
    }
  }
  return num_entries;
}

Statestore::TopicEntry::Version Statestore::Subscriber::LastTopicVersionProcessed(
    const TopicId& topic_id) const {
  const Topics& subscribed_topics = GetTopicsMapForId(topic_id);
  Topics::const_iterator itr = subscribed_topics.find(topic_id);
  return itr == subscribed_topics.end() ? TOPIC_INITIAL_VERSION
                                        : itr->second.last_version.Load();
}

void Statestore::Subscriber::SetLastTopicVersionProcessed(const TopicId& topic_id,
    TopicEntry::Version version) {
  // Safe to call concurrently for different topics because 'subscribed_topics' is not
  // modified.
  Topics* subscribed_topics = GetTopicsMapForId(topic_id);
  Topics::iterator topic_it = subscribed_topics->find(topic_id);
  DCHECK(topic_it != subscribed_topics->end());
  topic_it->second.last_version.Store(version);
}

Statestore::Statestore(MetricGroup* metrics)
  : subscriber_topic_update_threadpool_("statestore-update",
        "subscriber-update-worker",
        FLAGS_statestore_num_update_threads,
        FLAGS_statestore_max_subscribers,
        bind<void>(mem_fn(&Statestore::DoSubscriberUpdate), this,
          UpdateKind::TOPIC_UPDATE, _1, _2)),
    subscriber_priority_topic_update_threadpool_("statestore-priority-update",
        "subscriber-priority-update-worker",
        FLAGS_statestore_num_priority_update_threads,
        FLAGS_statestore_max_subscribers,
        bind<void>(mem_fn(&Statestore::DoSubscriberUpdate), this,
          UpdateKind::PRIORITY_TOPIC_UPDATE, _1, _2)),
    subscriber_heartbeat_threadpool_("statestore-heartbeat",
        "subscriber-heartbeat-worker",
        FLAGS_statestore_num_heartbeat_threads,
        FLAGS_statestore_max_subscribers,
        bind<void>(mem_fn(&Statestore::DoSubscriberUpdate), this,
          UpdateKind::HEARTBEAT, _1, _2)),
    update_state_client_cache_(new StatestoreSubscriberClientCache(1, 0,
        FLAGS_statestore_update_tcp_timeout_seconds * 1000,
        FLAGS_statestore_update_tcp_timeout_seconds * 1000, "",
        IsInternalTlsConfigured())),
    heartbeat_client_cache_(new StatestoreSubscriberClientCache(1, 0,
        FLAGS_statestore_heartbeat_tcp_timeout_seconds * 1000,
        FLAGS_statestore_heartbeat_tcp_timeout_seconds * 1000, "",
        IsInternalTlsConfigured())),
    thrift_iface_(new StatestoreThriftIf(this)),
    failure_detector_(new MissedHeartbeatFailureDetector(
        FLAGS_statestore_max_missed_heartbeats,
        FLAGS_statestore_max_missed_heartbeats / 2)) {

  DCHECK(metrics != NULL);
  num_subscribers_metric_ = metrics->AddGauge(STATESTORE_LIVE_SUBSCRIBERS, 0);
  subscriber_set_metric_ = SetMetric<string>::CreateAndRegister(metrics,
      STATESTORE_LIVE_SUBSCRIBERS_LIST, set<string>());
  key_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_KEY_SIZE_BYTES, 0);
  value_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_VALUE_SIZE_BYTES, 0);
  topic_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_TOPIC_SIZE_BYTES, 0);

  topic_update_duration_metric_ =
      StatsMetric<double>::CreateAndRegister(metrics, STATESTORE_UPDATE_DURATION);
  priority_topic_update_duration_metric_ = StatsMetric<double>::CreateAndRegister(
      metrics, STATESTORE_PRIORITY_UPDATE_DURATION);
  heartbeat_duration_metric_ =
      StatsMetric<double>::CreateAndRegister(metrics, STATESTORE_HEARTBEAT_DURATION);

  update_state_client_cache_->InitMetrics(metrics, "subscriber-update-state");
  heartbeat_client_cache_->InitMetrics(metrics, "subscriber-heartbeat");
}

Status Statestore::Init() {
  RETURN_IF_ERROR(subscriber_topic_update_threadpool_.Init());
  RETURN_IF_ERROR(subscriber_priority_topic_update_threadpool_.Init());
  RETURN_IF_ERROR(subscriber_heartbeat_threadpool_.Init());
  return Status::OK();
}

void Statestore::RegisterWebpages(Webserver* webserver) {
  Webserver::UrlCallback topics_callback =
      bind<void>(mem_fn(&Statestore::TopicsHandler), this, _1, _2);
  webserver->RegisterUrlCallback("/topics", "statestore_topics.tmpl",
      topics_callback);

  Webserver::UrlCallback subscribers_callback =
      bind<void>(&Statestore::SubscribersHandler, this, _1, _2);
  webserver->RegisterUrlCallback("/subscribers", "statestore_subscribers.tmpl",
      subscribers_callback);

  RegisterLogLevelCallbacks(webserver, false);
}

void Statestore::TopicsHandler(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(subscribers_lock_);
  shared_lock<shared_mutex> t(topics_map_lock_);

  Value topics(kArrayType);

  for (TopicMap::value_type& topic: topics_) {
    Value topic_json(kObjectType);
    topic.second.ToJson(document, &topic_json);
    SubscriberId oldest_subscriber_id;
    TopicEntry::Version oldest_subscriber_version =
        GetMinSubscriberTopicVersion(topic.first, &oldest_subscriber_id);
    topic_json.AddMember("oldest_version", oldest_subscriber_version,
        document->GetAllocator());
    Value oldest_id(oldest_subscriber_id.c_str(), document->GetAllocator());
    topic_json.AddMember("oldest_id", oldest_id, document->GetAllocator());
        topics.PushBack(topic_json, document->GetAllocator());
  }
  document->AddMember("topics", topics, document->GetAllocator());
}

void Statestore::SubscribersHandler(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(subscribers_lock_);
  Value subscribers(kArrayType);
  for (const SubscriberMap::value_type& subscriber: subscribers_) {
    Value sub_json(kObjectType);

    Value subscriber_id(subscriber.second->id().c_str(), document->GetAllocator());
    sub_json.AddMember("id", subscriber_id, document->GetAllocator());

    Value address(lexical_cast<string>(subscriber.second->network_address()).c_str(),
        document->GetAllocator());
    sub_json.AddMember("address", address, document->GetAllocator());

    int64_t num_priority_topics =
        subscriber.second->priority_subscribed_topics().size();
    int64_t num_non_priority_topics =
        subscriber.second->non_priority_subscribed_topics().size();
    sub_json.AddMember("num_topics",
        static_cast<uint64_t>(num_priority_topics + num_non_priority_topics),
        document->GetAllocator());
    sub_json.AddMember("num_priority_topics",
        static_cast<uint64_t>(num_priority_topics),
        document->GetAllocator());
    sub_json.AddMember("num_transient",
        static_cast<uint64_t>(subscriber.second->NumTransientEntries()),
        document->GetAllocator());

    Value registration_id(PrintId(subscriber.second->registration_id()).c_str(),
        document->GetAllocator());
    sub_json.AddMember("registration_id", registration_id, document->GetAllocator());

    subscribers.PushBack(sub_json, document->GetAllocator());
  }
  document->AddMember("subscribers", subscribers, document->GetAllocator());
}

Status Statestore::OfferUpdate(const ScheduledSubscriberUpdate& update,
    ThreadPool<ScheduledSubscriberUpdate>* threadpool) {
  // Somewhat confusingly, we're checking the number of entries in a particular
  // threadpool's work queue to decide whether or not we have too many
  // subscribers. The number of subscribers registered can be actually more
  // than statestore_max_subscribers. This is because RegisterSubscriber() adds
  // the new subscriber to subscribers_ first before scheduling its updates.
  // Should we be stricter in enforcing this limit on subscribers_.size() itself?
  if (threadpool->GetQueueSize() >= FLAGS_statestore_max_subscribers
      || !threadpool->Offer(update)) {
    stringstream ss;
    ss << "Maximum subscriber limit reached: " << FLAGS_statestore_max_subscribers;
    ss << ", subscribers_ size: " << subscribers_.size();
    SubscriberMap::iterator subscriber_it = subscribers_.find(update.subscriber_id);
    DCHECK(subscriber_it != subscribers_.end());
    subscribers_.erase(subscriber_it);
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  return Status::OK();
}

Status Statestore::RegisterSubscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& location,
    const vector<TTopicRegistration>& topic_registrations,
    RegistrationId* registration_id) {
  if (subscriber_id.empty()) return Status("Subscriber ID cannot be empty string");

  // Create any new topics first, so that when the subscriber is first sent a topic update
  // by the worker threads its topics are guaranteed to exist.
  {
    // Start with a shared read lock when checking the map. In the common case the topic
    // will already exist, so we don't need to immediately get the exclusive lock and
    // block other threads.
    upgrade_lock<shared_mutex> topic_read_lock(topics_map_lock_);
    for (const TTopicRegistration& topic: topic_registrations) {
      TopicMap::iterator topic_it = topics_.find(topic.topic_name);
      if (topic_it == topics_.end()) {
        // Upgrade to an exclusive lock when modifying the map.
        upgrade_to_unique_lock<shared_mutex> topic_write_lock(topic_read_lock);
        LOG(INFO) << "Creating new topic: ''" << topic.topic_name
                  << "' on behalf of subscriber: '" << subscriber_id;
        topics_.emplace(piecewise_construct, forward_as_tuple(topic.topic_name),
            forward_as_tuple(topic.topic_name, key_size_metric_, value_size_metric_,
            topic_size_metric_));
      }
    }
  }
  LOG(INFO) << "Registering: " << subscriber_id;
  {
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator subscriber_it = subscribers_.find(subscriber_id);
    if (subscriber_it != subscribers_.end()) {
      UnregisterSubscriber(subscriber_it->second.get());
    }

    UUIDToTUniqueId(subscriber_uuid_generator_(), registration_id);
    shared_ptr<Subscriber> current_registration(
        new Subscriber(subscriber_id, *registration_id, location, topic_registrations));
    subscribers_.emplace(subscriber_id, current_registration);
    failure_detector_->UpdateHeartbeat(
        PrintId(current_registration->registration_id()), true);
    num_subscribers_metric_->SetValue(subscribers_.size());
    subscriber_set_metric_->Add(subscriber_id);

    // Add the subscriber to the update queue, with an immediate schedule.
    ScheduledSubscriberUpdate update(0, subscriber_id, *registration_id);
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_topic_update_threadpool_));
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_priority_topic_update_threadpool_));
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_heartbeat_threadpool_));
  }

  LOG(INFO) << "Subscriber '" << subscriber_id << "' registered (registration id: "
            << PrintId(*registration_id) << ")";
  return Status::OK();
}

bool Statestore::FindSubscriber(const SubscriberId& subscriber_id,
    const RegistrationId& registration_id, shared_ptr<Subscriber>* subscriber) {
  DCHECK(subscriber != nullptr);
  lock_guard<mutex> l(subscribers_lock_);
  SubscriberMap::iterator it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end() ||
      it->second->registration_id() != registration_id) return false;
  *subscriber = it->second;
  return true;
}

Status Statestore::SendTopicUpdate(Subscriber* subscriber, UpdateKind update_kind,
    bool* update_skipped) {
  // Time any successful RPCs (i.e. those for which UpdateState() completed, even though
  // it may have returned an error.)
  MonotonicStopWatch sw;
  sw.Start();

  // First thing: make a list of updates to send
  TUpdateStateRequest update_state_request;
  GatherTopicUpdates(*subscriber, update_kind, &update_state_request);
  // 'subscriber' may not be subscribed to any updates of 'update_kind'.
  if (update_state_request.topic_deltas.empty()) {
    *update_skipped = false;
    return Status::OK();
  }

  // Set the expected registration ID, so that the subscriber can reject this update if
  // they have moved on to a new registration instance.
  update_state_request.__set_registration_id(subscriber->registration_id());

  // Second: try and send it
  Status status;
  StatestoreSubscriberConn client(update_state_client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  TUpdateStateResponse response;
  RETURN_IF_ERROR(client.DoRpc(
      &StatestoreSubscriberClientWrapper::UpdateState, update_state_request, &response));

  StatsMetric<double>* update_duration_metric =
      update_kind == UpdateKind::PRIORITY_TOPIC_UPDATE ?
      priority_topic_update_duration_metric_ : topic_update_duration_metric_;
  status = Status(response.status);
  if (!status.ok()) {
    update_duration_metric->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    return status;
  }

  *update_skipped = (response.__isset.skipped && response.skipped);
  if (*update_skipped) {
    // The subscriber skipped processing this update. We don't consider this a failure
    // - subscribers can decide what they do with any update - so, return OK and set
    // update_skipped so the caller can compensate.
    update_duration_metric->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    return Status::OK();
  }

  // At this point the updates are assumed to have been successfully processed by the
  // subscriber. Update the subscriber's max version of each topic.
  for (const auto& topic_delta: update_state_request.topic_deltas) {
    subscriber->SetLastTopicVersionProcessed(topic_delta.first,
        topic_delta.second.to_version);
  }

  // Thirdly: perform any / all updates returned by the subscriber
  {
    shared_lock<shared_mutex> l(topics_map_lock_);
    for (const TTopicDelta& update: response.topic_updates) {
      TopicMap::iterator topic_it = topics_.find(update.topic_name);
      if (topic_it == topics_.end()) {
        VLOG(1) << "Received update for unexpected topic:" << update.topic_name;
        continue;
      }

      VLOG_RPC << "Received update for topic " << update.topic_name
               << " from  " << subscriber->id() << ", number of entries: "
               << update.topic_entries.size();

      // The subscriber sent back their from_version which indicates that they want to
      // reset their max version for this topic to this value. The next update sent will
      // be from this version.
      if (update.__isset.from_version) {
        LOG(INFO) << "Received request for different delta base of topic: "
                  << update.topic_name << " from: " << subscriber->id()
                  << " subscriber from_version: " << update.from_version;
        subscriber->SetLastTopicVersionProcessed(topic_it->first, update.from_version);
      }

      Topic& topic = topic_it->second;
      // Update the topic and add transient entries separately to avoid holding both
      // locks at the same time and preventing concurrent topic updates.
      vector<TopicEntry::Version> entry_versions = topic.Put(update.topic_entries);
      if (!subscriber->AddTransientEntries(
          update.topic_name, update.topic_entries, entry_versions)) {
        // Subscriber was unregistered - clean up the transient entries.
        for (int i = 0; i < update.topic_entries.size(); ++i) {
          topic.DeleteIfVersionsMatch(entry_versions[i], update.topic_entries[i].key);
        }
      }
    }
  }
  update_duration_metric->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

void Statestore::GatherTopicUpdates(const Subscriber& subscriber,
    UpdateKind update_kind, TUpdateStateRequest* update_state_request) {
  {
    DCHECK(update_kind == UpdateKind::TOPIC_UPDATE
        || update_kind ==  UpdateKind::PRIORITY_TOPIC_UPDATE)
        << static_cast<int>(update_kind);
    const bool is_priority = update_kind == UpdateKind::PRIORITY_TOPIC_UPDATE;
    const Subscriber::Topics& subscribed_topics = is_priority
        ? subscriber.priority_subscribed_topics()
        : subscriber.non_priority_subscribed_topics();
    shared_lock<shared_mutex> l(topics_map_lock_);
    for (const auto& subscribed_topic: subscribed_topics) {
      auto topic_it = topics_.find(subscribed_topic.first);
      DCHECK(topic_it != topics_.end());
      TopicEntry::Version last_processed_version =
          subscriber.LastTopicVersionProcessed(topic_it->first);

      TTopicDelta& topic_delta =
          update_state_request->topic_deltas[subscribed_topic.first];
      topic_delta.topic_name = subscribed_topic.first;
      topic_it->second.BuildDelta(subscriber.id(), last_processed_version, &topic_delta);
    }
  }

  // Fill in the min subscriber topic version. This must be done after releasing
  // topics_map_lock_.
  lock_guard<mutex> l(subscribers_lock_);
  typedef map<TopicId, TTopicDelta> TopicDeltaMap;
  for (TopicDeltaMap::value_type& topic_delta: update_state_request->topic_deltas) {
    topic_delta.second.__set_min_subscriber_topic_version(
        GetMinSubscriberTopicVersion(topic_delta.first));
  }
}

Statestore::TopicEntry::Version Statestore::GetMinSubscriberTopicVersion(
    const TopicId& topic_id, SubscriberId* subscriber_id) {
  TopicEntry::Version min_topic_version = numeric_limits<int64_t>::max();
  bool found = false;
  // Find the minimum version processed for this topic across all topic subscribers.
  for (const SubscriberMap::value_type& subscriber: subscribers_) {
    auto subscribed_topics = subscriber.second->GetTopicsMapForId(topic_id);
    if (subscribed_topics->find(topic_id) != subscribed_topics->end()) {
      found = true;
      TopicEntry::Version last_processed_version =
          subscriber.second->LastTopicVersionProcessed(topic_id);
      if (last_processed_version < min_topic_version) {
        min_topic_version = last_processed_version;
        if (subscriber_id != NULL) *subscriber_id = subscriber.second->id();
      }
    }
  }
  return found ? min_topic_version : Subscriber::TOPIC_INITIAL_VERSION;
}

bool Statestore::IsPrioritizedTopic(const string& topic) {
  return topic == IMPALA_MEMBERSHIP_TOPIC || topic == IMPALA_REQUEST_QUEUE_TOPIC;
}

const char* Statestore::GetUpdateKindName(UpdateKind kind) {
  switch (kind) {
    case UpdateKind::TOPIC_UPDATE:
      return "topic update";
    case UpdateKind::PRIORITY_TOPIC_UPDATE:
      return "priority topic update";
    case UpdateKind::HEARTBEAT:
      return "heartbeat";
  }
  DCHECK(false);
}

ThreadPool<Statestore::ScheduledSubscriberUpdate>* Statestore::GetThreadPool(
    UpdateKind kind) {
  switch (kind) {
    case UpdateKind::TOPIC_UPDATE:
      return &subscriber_topic_update_threadpool_;
    case UpdateKind::PRIORITY_TOPIC_UPDATE:
      return &subscriber_priority_topic_update_threadpool_;
    case UpdateKind::HEARTBEAT:
      return &subscriber_heartbeat_threadpool_;
  }
  DCHECK(false);
}

Status Statestore::SendHeartbeat(Subscriber* subscriber) {
  MonotonicStopWatch sw;
  sw.Start();

  Status status;
  StatestoreSubscriberConn client(heartbeat_client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  THeartbeatRequest request;
  THeartbeatResponse response;
  request.__set_registration_id(subscriber->registration_id());
  RETURN_IF_ERROR(
      client.DoRpc(&StatestoreSubscriberClientWrapper::Heartbeat, request, &response));

  heartbeat_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

void Statestore::DoSubscriberUpdate(UpdateKind update_kind, int thread_id,
    const ScheduledSubscriberUpdate& update) {
  const bool is_heartbeat = update_kind == UpdateKind::HEARTBEAT;
  int64_t update_deadline = update.deadline;
  shared_ptr<Subscriber> subscriber;
  // Check if the subscriber has re-registered, in which case we can ignore
  // this scheduled update.
  if (!FindSubscriber(update.subscriber_id, update.registration_id, &subscriber)) {
    return;
  }
  const char* update_kind_str = GetUpdateKindName(update_kind);
  if (update_deadline != 0) {
    // Wait until deadline.
    int64_t diff_ms = update_deadline - UnixMillis();
    while (diff_ms > 0) {
      SleepForMs(diff_ms);
      diff_ms = update_deadline - UnixMillis();
    }
    // The subscriber can potentially reconnect by the time this thread wakes
    // up. In such case, we can ignore this update.
    if (UNLIKELY(!FindSubscriber(
        subscriber->id(), subscriber->registration_id(), &subscriber))) {
      return;
    }
    diff_ms = std::abs(diff_ms);
    VLOG(3) << "Sending " << update_kind_str << " message to: " << update.subscriber_id
        << " (deadline accuracy: " << diff_ms << "ms)";

    if (diff_ms > DEADLINE_MISS_THRESHOLD_MS && is_heartbeat) {
      const string& msg = Substitute(
          "Missed subscriber ($0) $1 deadline by $2ms, "
          "consider increasing --statestore_heartbeat_frequency_ms (currently $3) on "
          "this Statestore, and --statestore_subscriber_timeout_seconds "
          "on subscribers (Impala daemons and the Catalog Server)",
          update.subscriber_id, update_kind_str, diff_ms,
          FLAGS_statestore_heartbeat_frequency_ms);
      LOG(WARNING) << msg;
    }
    // Don't warn for topic updates - they can be slow and still correct. Recommending an
    // increase in update period will just confuse (as will increasing the thread pool
    // size) because it's hard for users to pick a good value, they may still see these
    // messages and it won't be a correctness problem.
  } else {
    // The first update is scheduled immediately and has a deadline of 0. There's no need
    // to wait.
    VLOG(3) << "Initial " << update_kind_str << " message for: " << update.subscriber_id;
  }

  // Send the right message type, and compute the next deadline
  int64_t deadline_ms = 0;
  Status status;
  if (is_heartbeat) {
    status = SendHeartbeat(subscriber.get());
    if (status.code() == TErrorCode::RPC_RECV_TIMEOUT) {
      // Add details to status to make it more useful, while preserving the stack
      status.AddDetail(Substitute(
          "Subscriber $0 timed-out during heartbeat RPC. Timeout is $1s.",
          subscriber->id(), FLAGS_statestore_heartbeat_tcp_timeout_seconds));
    }

    deadline_ms = UnixMillis() + FLAGS_statestore_heartbeat_frequency_ms;
  } else {
    bool update_skipped;
    status = SendTopicUpdate(subscriber.get(), update_kind, &update_skipped);
    if (status.code() == TErrorCode::RPC_RECV_TIMEOUT) {
      // Add details to status to make it more useful, while preserving the stack
      status.AddDetail(Substitute(
          "Subscriber $0 timed-out during topic-update RPC. Timeout is $1s.",
          subscriber->id(), FLAGS_statestore_update_tcp_timeout_seconds));
    }
    // If the subscriber responded that it skipped a topic in the last update sent,
    // we assume that it was busy doing something else, and back off slightly before
    // sending another.
    int64_t update_frequency = update_kind == UpdateKind::PRIORITY_TOPIC_UPDATE
        ? FLAGS_statestore_priority_update_frequency_ms
        : FLAGS_statestore_update_frequency_ms;
    int64_t update_interval = update_skipped ? (2 * update_frequency)
                                                 : update_frequency;
    deadline_ms = UnixMillis() + update_interval;
  }

  {
    lock_guard<mutex> l(subscribers_lock_);
    // Check again if this registration has been removed while we were processing the
    // message.
    SubscriberMap::iterator it = subscribers_.find(update.subscriber_id);
    if (it == subscribers_.end() ||
        it->second->registration_id() != update.registration_id) return;
    if (!status.ok()) {
      LOG(INFO) << "Unable to send " << update_kind_str << " message to subscriber "
                << update.subscriber_id << ", received error: " << status.GetDetail();
    }

    FailureDetector::PeerState state = is_heartbeat ?
        failure_detector_->UpdateHeartbeat(update.subscriber_id, status.ok()) :
        failure_detector_->GetPeerState(update.subscriber_id);

    if (state == FailureDetector::FAILED) {
      if (is_heartbeat) {
        // TODO: Consider if a metric to track the number of failures would be useful.
        LOG(INFO) << "Subscriber '" << subscriber->id() << "' has failed, disconnected "
                  << "or re-registered (last known registration ID: "
                  << update.registration_id << ")";
        UnregisterSubscriber(subscriber.get());
      }
    } else {
      // Schedule the next message.
      VLOG(3) << "Next " << (is_heartbeat ? "heartbeat" : "update") << " deadline for: "
              << subscriber->id() << " is in " << deadline_ms << "ms";
      status = OfferUpdate(ScheduledSubscriberUpdate(deadline_ms, subscriber->id(),
          subscriber->registration_id()), GetThreadPool(update_kind));
      if (!status.ok()) {
        LOG(INFO) << "Unable to send next " << update_kind_str
                  << " message to subscriber '" << subscriber->id() << "': "
                  << status.GetDetail();
      }
    }
  }
}

void Statestore::UnregisterSubscriber(Subscriber* subscriber) {
  SubscriberMap::const_iterator it = subscribers_.find(subscriber->id());
  if (it == subscribers_.end() ||
      it->second->registration_id() != subscriber->registration_id()) {
    // Already failed and / or replaced with a new registration
    return;
  }

  // Close all active clients so that the next attempt to use them causes a Reopen()
  update_state_client_cache_->CloseConnections(subscriber->network_address());
  heartbeat_client_cache_->CloseConnections(subscriber->network_address());

  // Prevent the failure detector from growing without bound
  failure_detector_->EvictPeer(PrintId(subscriber->registration_id()));

  // Delete all transient entries
  {
    shared_lock<shared_mutex> topic_lock(topics_map_lock_);
    subscriber->DeleteAllTransientEntries(&topics_);
  }

  num_subscribers_metric_->Increment(-1L);
  subscriber_set_metric_->Remove(subscriber->id());
  subscribers_.erase(subscriber->id());
}

void Statestore::MainLoop() {
  subscriber_topic_update_threadpool_.Join();
  subscriber_priority_topic_update_threadpool_.Join();
  subscriber_heartbeat_threadpool_.Join();
}
