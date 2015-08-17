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

#include "statestore/statestore.h"

#include <boost/foreach.hpp>
#include <boost/thread.hpp>
#include <thrift/Thrift.h>
#include <gutil/strings/substitute.h>

#include "common/status.h"
#include "gen-cpp/StatestoreService_types.h"
#include "statestore/failure-detector.h"
#include "rpc/thrift-util.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

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

DEFINE_int32(statestore_num_heartbeat_threads, 10, "(Advanced) Number of threads used to "
    " send heartbeats in parallel to all registered subscribers.");
DEFINE_int32(statestore_heartbeat_frequency_ms, 1000, "(Advanced) Frequency (in ms) with"
    " which the statestore sends heartbeat heartbeats to subscribers.");

DEFINE_int32(state_store_port, 24000, "port where StatestoreService is running");

DEFINE_int32(statestore_heartbeat_tcp_timeout_seconds, 3, "(Advanced) The time after "
    "which a heartbeat RPC to a subscriber will timeout. This setting protects against "
    "badly hung machines that are not able to respond to the heartbeat RPC in short "
    "order");

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
const string STATESTORE_HEARTBEAT_DURATION = "statestore.heartbeat-durations";

const Statestore::TopicEntry::Value Statestore::TopicEntry::NULL_VALUE = "";

// Initial version for each Topic registered by a Subscriber. Generally, the Topic will
// have a Version that is the MAX() of all entries in the Topic, but this initial
// value needs to be less than TopicEntry::TOPIC_ENTRY_INITIAL_VERSION to distinguish
// between the case where a Topic is empty and the case where the Topic only contains
// an item with the initial version.
const Statestore::TopicEntry::Version Statestore::Subscriber::TOPIC_INITIAL_VERSION = 0L;

// Used to control the maximum size of the pending topic-update queue, in which there is
// at most one entry per subscriber.
const int32_t STATESTORE_MAX_SUBSCRIBERS = 10000;

// Updates or heartbeats that miss their deadline by this much are logged.
const uint32_t DEADLINE_MISS_THRESHOLD_MS = 2000;

typedef ClientConnection<StatestoreSubscriberClient> StatestoreSubscriberConnection;

class StatestoreThriftIf : public StatestoreServiceIf {
 public:
  StatestoreThriftIf(Statestore* statestore)
      : statestore_(statestore) {
    DCHECK(statestore_ != NULL);
  }

  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& params) {
    TUniqueId registration_id;
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
  DCHECK(bytes == Statestore::TopicEntry::NULL_VALUE || bytes.size() > 0);
  value_ = bytes;
  version_ = version;
}

Statestore::TopicEntry::Version Statestore::Topic::Put(const string& key,
    const Statestore::TopicEntry::Value& bytes) {
  TopicEntryMap::iterator entry_it = entries_.find(key);
  int64_t key_size_delta = 0L;
  int64_t value_size_delta = 0L;
  if (entry_it == entries_.end()) {
    entry_it = entries_.insert(make_pair(key, TopicEntry())).first;
    key_size_delta += key.size();
  } else {
    // Delete the old item from the version history. There is no need to search the
    // version_history because there should only be at most a single item in the history
    // at any given time
    topic_update_log_.erase(entry_it->second.version());
    value_size_delta -= entry_it->second.value().size();
  }
  value_size_delta += bytes.size();

  entry_it->second.SetValue(bytes, ++last_version_);
  topic_update_log_.insert(make_pair(entry_it->second.version(), key));

  total_key_size_bytes_ += key_size_delta;
  total_value_size_bytes_ += value_size_delta;
  DCHECK_GE(total_key_size_bytes_, 0L);
  DCHECK_GE(total_value_size_bytes_, 0L);
  key_size_metric_->Increment(key_size_delta);
  value_size_metric_->Increment(value_size_delta);
  topic_size_metric_->Increment(key_size_delta + value_size_delta);

  return entry_it->second.version();
}

void Statestore::Topic::DeleteIfVersionsMatch(TopicEntry::Version version,
    const Statestore::TopicEntryKey& key) {
  TopicEntryMap::iterator entry_it = entries_.find(key);
  if (entry_it != entries_.end() && entry_it->second.version() == version) {
    // Add a new entry with the the version history for this deletion and remove the old
    // entry
    topic_update_log_.erase(version);
    topic_update_log_.insert(make_pair(++last_version_, key));
    total_value_size_bytes_ -= entry_it->second.value().size();
    DCHECK_GE(total_value_size_bytes_, 0L);

    value_size_metric_->Increment(entry_it->second.value().size());
    topic_size_metric_->Increment(entry_it->second.value().size());
    entry_it->second.SetValue(Statestore::TopicEntry::NULL_VALUE, last_version_);
  }
}

Statestore::Subscriber::Subscriber(const SubscriberId& subscriber_id,
    const TUniqueId& registration_id, const TNetworkAddress& network_address,
    const vector<TTopicRegistration>& subscribed_topics)
    : subscriber_id_(subscriber_id),
      registration_id_(registration_id),
      network_address_(network_address) {
  BOOST_FOREACH(const TTopicRegistration& topic, subscribed_topics) {
    TopicState topic_state;
    topic_state.is_transient = topic.is_transient;
    topic_state.last_version = TOPIC_INITIAL_VERSION;
    subscribed_topics_[topic.topic_name] = topic_state;
  }
}

void Statestore::Subscriber::AddTransientUpdate(const TopicId& topic_id,
    const TopicEntryKey& topic_key, TopicEntry::Version version) {
  // Only record the update if the topic is transient
  const Topics::const_iterator topic_it = subscribed_topics_.find(topic_id);
  DCHECK(topic_it != subscribed_topics_.end());
  if (topic_it->second.is_transient == true) {
    transient_entries_[make_pair(topic_id, topic_key)] = version;
  }
}

const Statestore::TopicEntry::Version Statestore::Subscriber::LastTopicVersionProcessed(
    const TopicId& topic_id) const {
  Topics::const_iterator itr = subscribed_topics_.find(topic_id);
  return itr == subscribed_topics_.end() ?
      TOPIC_INITIAL_VERSION : itr->second.last_version;
}

void Statestore::Subscriber::SetLastTopicVersionProcessed(const TopicId& topic_id,
    TopicEntry::Version version) {
  subscribed_topics_[topic_id].last_version = version;
}

Statestore::Statestore(MetricGroup* metrics)
  : exit_flag_(false),
    subscriber_topic_update_threadpool_("statestore-update",
        "subscriber-update-worker",
        FLAGS_statestore_num_update_threads,
        STATESTORE_MAX_SUBSCRIBERS,
        bind<void>(mem_fn(&Statestore::DoSubscriberUpdate), this, false, _1, _2)),
    subscriber_heartbeat_threadpool_("statestore-heartbeat",
        "subscriber-heartbeat-worker",
        FLAGS_statestore_num_heartbeat_threads,
        STATESTORE_MAX_SUBSCRIBERS,
        bind<void>(mem_fn(&Statestore::DoSubscriberUpdate), this, true, _1, _2)),
    update_state_client_cache_(new ClientCache<StatestoreSubscriberClient>(1, 0,
        FLAGS_statestore_update_tcp_timeout_seconds * 1000,
        FLAGS_statestore_update_tcp_timeout_seconds * 1000)),
    heartbeat_client_cache_(new ClientCache<StatestoreSubscriberClient>(1, 0,
        FLAGS_statestore_heartbeat_tcp_timeout_seconds * 1000,
        FLAGS_statestore_heartbeat_tcp_timeout_seconds * 1000)),
    thrift_iface_(new StatestoreThriftIf(this)),
    failure_detector_(new MissedHeartbeatFailureDetector(
        FLAGS_statestore_max_missed_heartbeats,
        FLAGS_statestore_max_missed_heartbeats / 2)) {

  DCHECK(metrics != NULL);
  num_subscribers_metric_ =
      metrics->AddGauge(STATESTORE_LIVE_SUBSCRIBERS, 0L);
  subscriber_set_metric_ = SetMetric<string>::CreateAndRegister(metrics,
      STATESTORE_LIVE_SUBSCRIBERS_LIST, set<string>());
  key_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_KEY_SIZE_BYTES, 0L);
  value_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_VALUE_SIZE_BYTES, 0L);
  topic_size_metric_ = metrics->AddGauge(STATESTORE_TOTAL_TOPIC_SIZE_BYTES, 0L);

  topic_update_duration_metric_ =
      StatsMetric<double>::CreateAndRegister(metrics, STATESTORE_UPDATE_DURATION);
  heartbeat_duration_metric_ =
      StatsMetric<double>::CreateAndRegister(metrics, STATESTORE_HEARTBEAT_DURATION);

  update_state_client_cache_->InitMetrics(metrics, "subscriber-update-state");
  heartbeat_client_cache_->InitMetrics(metrics, "subscriber-heartbeat");
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
}

void Statestore::TopicsHandler(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(subscribers_lock_);
  lock_guard<mutex> t(topic_lock_);

  Value topics(kArrayType);

  BOOST_FOREACH(const TopicMap::value_type& topic, topics_) {
    Value topic_json(kObjectType);

    Value topic_id(topic.second.id().c_str(), document->GetAllocator());
    topic_json.AddMember("topic_id", topic_id, document->GetAllocator());
    topic_json.AddMember("num_entries", topic.second.entries().size(),
        document->GetAllocator());
    topic_json.AddMember("version", topic.second.last_version(), document->GetAllocator());

    SubscriberId oldest_subscriber_id;
    TopicEntry::Version oldest_subscriber_version =
        GetMinSubscriberTopicVersion(topic.first, &oldest_subscriber_id);

    topic_json.AddMember("oldest_version", oldest_subscriber_version,
        document->GetAllocator());
    Value oldest_id(oldest_subscriber_id.c_str(), document->GetAllocator());
    topic_json.AddMember("oldest_id", oldest_id, document->GetAllocator());

    int64_t key_size = topic.second.total_key_size_bytes();
    int64_t value_size = topic.second.total_value_size_bytes();
    Value key_size_json(PrettyPrinter::Print(key_size, TUnit::BYTES).c_str(),
        document->GetAllocator());
    topic_json.AddMember("key_size", key_size_json, document->GetAllocator());
    Value value_size_json(PrettyPrinter::Print(value_size, TUnit::BYTES).c_str(),
        document->GetAllocator());
    topic_json.AddMember("value_size", value_size_json, document->GetAllocator());
    Value total_size_json(
        PrettyPrinter::Print(key_size + value_size, TUnit::BYTES).c_str(),
        document->GetAllocator());
    topic_json.AddMember("total_size", total_size_json, document->GetAllocator());
    topics.PushBack(topic_json, document->GetAllocator());
  }
  document->AddMember("topics", topics, document->GetAllocator());
}

void Statestore::SubscribersHandler(const Webserver::ArgumentMap& args,
    Document* document) {
  lock_guard<mutex> l(subscribers_lock_);
  Value subscribers(kArrayType);
  BOOST_FOREACH(const SubscriberMap::value_type& subscriber, subscribers_) {
    Value sub_json(kObjectType);

    Value subscriber_id(subscriber.second->id().c_str(), document->GetAllocator());
    sub_json.AddMember("id", subscriber_id, document->GetAllocator());

    Value address(lexical_cast<string>(subscriber.second->network_address()).c_str(),
        document->GetAllocator());
    sub_json.AddMember("address", address, document->GetAllocator());

    sub_json.AddMember("num_topics", subscriber.second->subscribed_topics().size(),
        document->GetAllocator());
    sub_json.AddMember("num_transient", subscriber.second->transient_entries().size(),
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
  if (threadpool->GetQueueSize() >= STATESTORE_MAX_SUBSCRIBERS
      || !threadpool->Offer(update)) {
    stringstream ss;
    ss << "Maximum subscriber limit reached: " << STATESTORE_MAX_SUBSCRIBERS;
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator subscriber_it = subscribers_.find(update.second);
    DCHECK(subscriber_it != subscribers_.end());
    subscribers_.erase(subscriber_it);
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  return Status::OK();
}

Status Statestore::RegisterSubscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& location,
    const vector<TTopicRegistration>& topic_registrations, TUniqueId* registration_id) {
  if (subscriber_id.empty()) return Status("Subscriber ID cannot be empty string");

  // Create any new topics first, so that when the subscriber is first sent a topic update
  // by the worker threads its topics are guaranteed to exist.
  {
    lock_guard<mutex> l(topic_lock_);
    BOOST_FOREACH(const TTopicRegistration& topic, topic_registrations) {
      TopicMap::iterator topic_it = topics_.find(topic.topic_name);
      if (topic_it == topics_.end()) {
        LOG(INFO) << "Creating new topic: ''" << topic.topic_name
                  << "' on behalf of subscriber: '" << subscriber_id;
        topics_.insert(make_pair(topic.topic_name, Topic(topic.topic_name,
            key_size_metric_, value_size_metric_, topic_size_metric_)));
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
    subscribers_.insert(make_pair(subscriber_id, current_registration));
    failure_detector_->UpdateHeartbeat(
        PrintId(current_registration->registration_id()), true);
    num_subscribers_metric_->set_value(subscribers_.size());
    subscriber_set_metric_->Add(subscriber_id);
  }

  // Add the subscriber to the update queue, with an immediate schedule.
  ScheduledSubscriberUpdate update = make_pair(0L, subscriber_id);
  RETURN_IF_ERROR(OfferUpdate(update, &subscriber_topic_update_threadpool_));
  RETURN_IF_ERROR(OfferUpdate(update, &subscriber_heartbeat_threadpool_));

  LOG(INFO) << "Subscriber '" << subscriber_id << "' registered (registration id: "
            << PrintId(*registration_id) << ")";
  return Status::OK();
}

Status Statestore::SendTopicUpdate(Subscriber* subscriber, bool* update_skipped) {
  // Time any successful RPCs (i.e. those for which UpdateState() completed, even though
  // it may have returned an error.)
  MonotonicStopWatch sw;
  sw.Start();

  // First thing: make a list of updates to send
  TUpdateStateRequest update_state_request;
  GatherTopicUpdates(*subscriber, &update_state_request);

  // Set the expected registration ID, so that the subscriber can reject this update if
  // they have moved on to a new registration instance.
  update_state_request.__set_registration_id(subscriber->registration_id());

  // Second: try and send it
  Status status;
  StatestoreSubscriberConnection client(update_state_client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  TUpdateStateResponse response;
  RETURN_IF_ERROR(client.DoRpc(
      &StatestoreSubscriberClient::UpdateState, update_state_request, &response));

  status = Status(response.status);
  if (!status.ok()) {
    topic_update_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    return status;
  }

  *update_skipped = (response.__isset.skipped && response.skipped);
  if (*update_skipped) {
    // The subscriber skipped processing this update. We don't consider this a failure
    // - subscribers can decide what they do with any update - so, return OK and set
    // update_skipped so the caller can compensate.
    topic_update_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    return Status::OK();
  }

  // At this point the updates are assumed to have been successfully processed by the
  // subscriber. Update the subscriber's max version of each topic.
  map<TopicEntryKey, TTopicDelta>::const_iterator topic_delta =
      update_state_request.topic_deltas.begin();
  for (; topic_delta != update_state_request.topic_deltas.end(); ++topic_delta) {
    subscriber->SetLastTopicVersionProcessed(topic_delta->first,
        topic_delta->second.to_version);
  }

  // Thirdly: perform any / all updates returned by the subscriber
  {
    lock_guard<mutex> l(topic_lock_);
    BOOST_FOREACH(const TTopicDelta& update, response.topic_updates) {
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

      Topic* topic = &topic_it->second;
      BOOST_FOREACH(const TTopicItem& item, update.topic_entries) {
        TopicEntry::Version version = topic->Put(item.key, item.value);
        subscriber->AddTransientUpdate(update.topic_name, item.key, version);
      }

      BOOST_FOREACH(const string& key, update.topic_deletions) {
        TopicEntry::Version version =
            topic->Put(key, Statestore::TopicEntry::NULL_VALUE);
        subscriber->AddTransientUpdate(update.topic_name, key, version);
      }
    }
  }
  topic_update_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

void Statestore::GatherTopicUpdates(const Subscriber& subscriber,
    TUpdateStateRequest* update_state_request) {
  {
    lock_guard<mutex> l(topic_lock_);
    BOOST_FOREACH(const Subscriber::Topics::value_type& subscribed_topic,
        subscriber.subscribed_topics()) {
      TopicMap::const_iterator topic_it = topics_.find(subscribed_topic.first);
      DCHECK(topic_it != topics_.end());

      TopicEntry::Version last_processed_version =
          subscriber.LastTopicVersionProcessed(topic_it->first);
      const Topic& topic = topic_it->second;

      TTopicDelta& topic_delta =
          update_state_request->topic_deltas[subscribed_topic.first];
      topic_delta.topic_name = subscribed_topic.first;

      // If the subscriber version is > 0, send this update as a delta. Otherwise, this is
      // a new subscriber so send them a non-delta update that includes all items in the
      // topic.
      topic_delta.is_delta = last_processed_version > Subscriber::TOPIC_INITIAL_VERSION;
      topic_delta.__set_from_version(last_processed_version);

      if (!topic_delta.is_delta &&
          topic.last_version() > Subscriber::TOPIC_INITIAL_VERSION) {
        int64_t topic_size =
            topic.total_key_size_bytes() + topic.total_value_size_bytes();
        VLOG_QUERY << "Preparing initial " << topic_delta.topic_name
                   << " topic update for " << subscriber.id() << ". Size = "
                   << PrettyPrinter::Print(topic_size, TUnit::BYTES);
      }

      TopicUpdateLog::const_iterator next_update =
          topic.topic_update_log().upper_bound(last_processed_version);

      for (; next_update != topic.topic_update_log().end(); ++next_update) {
        TopicEntryMap::const_iterator itr = topic.entries().find(next_update->second);
        DCHECK(itr != topic.entries().end());
        const TopicEntry& topic_entry = itr->second;
        if (topic_entry.value() == Statestore::TopicEntry::NULL_VALUE) {
          topic_delta.topic_deletions.push_back(itr->first);
        } else {
          topic_delta.topic_entries.push_back(TTopicItem());
          TTopicItem& topic_item = topic_delta.topic_entries.back();
          topic_item.key = itr->first;
          // TODO: Does this do a needless copy?
          topic_item.value = topic_entry.value();
        }
      }

      if (topic.topic_update_log().size() > 0) {
        // The largest version for this topic will be the last item in the version history
        // map.
        topic_delta.__set_to_version(topic.topic_update_log().rbegin()->first);
      } else {
        // There are no updates in the version history
        topic_delta.__set_to_version(Subscriber::TOPIC_INITIAL_VERSION);
      }
    }
  }

  // Fill in the min subscriber topic version. This must be done after releasing
  // topic_lock_.
  lock_guard<mutex> l(subscribers_lock_);
  typedef map<TopicId, TTopicDelta> TopicDeltaMap;
  BOOST_FOREACH(TopicDeltaMap::value_type& topic_delta,
      update_state_request->topic_deltas) {
    topic_delta.second.__set_min_subscriber_topic_version(
        GetMinSubscriberTopicVersion(topic_delta.first));
  }
}

const Statestore::TopicEntry::Version Statestore::GetMinSubscriberTopicVersion(
    const TopicId& topic_id, SubscriberId* subscriber_id) {
  TopicEntry::Version min_topic_version = numeric_limits<int64_t>::max();
  bool found = false;
  // Find the minimum version processed for this topic across all topic subscribers.
  BOOST_FOREACH(const SubscriberMap::value_type& subscriber, subscribers_) {
    if (subscriber.second->subscribed_topics().find(topic_id) !=
        subscriber.second->subscribed_topics().end()) {
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

bool Statestore::ShouldExit() {
  lock_guard<mutex> l(exit_flag_lock_);
  return exit_flag_;
}

void Statestore::SetExitFlag() {
  lock_guard<mutex> l(exit_flag_lock_);
  exit_flag_ = true;
  subscriber_topic_update_threadpool_.Shutdown();
}

Status Statestore::SendHeartbeat(Subscriber* subscriber) {
  MonotonicStopWatch sw;
  sw.Start();

  Status status;
  StatestoreSubscriberConnection client(heartbeat_client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  THeartbeatRequest request;
  THeartbeatResponse response;
  request.__set_registration_id(subscriber->registration_id());
  RETURN_IF_ERROR(
      client.DoRpc(&StatestoreSubscriberClient::Heartbeat, request, &response));

  heartbeat_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

void Statestore::DoSubscriberUpdate(bool is_heartbeat, int thread_id,
    const ScheduledSubscriberUpdate& update) {
  int64_t update_deadline = update.first;
  const string hb_type = is_heartbeat ? "heartbeat" : "topic update";
  if (update_deadline != 0L) {
    // Wait until deadline.
    int64_t diff_ms = update_deadline - UnixMillis();
    while (diff_ms > 0) {
      SleepForMs(diff_ms);
      diff_ms = update_deadline - UnixMillis();
    }
    diff_ms = abs(diff_ms);
    VLOG(3) << "Sending " << hb_type << " message to: " << update.second
            << " (deadline accuracy: " << diff_ms << "ms)";

    if (diff_ms > DEADLINE_MISS_THRESHOLD_MS) {
      // TODO: This should be a healthcheck in a monitored metric in CM, which would
      // require a 'rate' metric type.
      const string& msg = Substitute("Missed subscriber ($0) $1 deadline by $2ms, "
          "consider increasing --$3 (currently $4)", update.second, hb_type, diff_ms,
          is_heartbeat ? "statestore_heartbeat_frequency_ms" :
              "statestore_update_frequency_ms",
          is_heartbeat ? FLAGS_statestore_heartbeat_frequency_ms :
              FLAGS_statestore_update_frequency_ms);
      if (is_heartbeat) {
        LOG(WARNING) << msg;
      } else {
        VLOG_QUERY << msg;
      }
    }
  } else {
    // The first update is scheduled immediately and has a deadline of 0. There's no need
    // to wait.
    VLOG(3) << "Initial " << hb_type << " message for: " << update.second;
  }
  shared_ptr<Subscriber> subscriber;
  {
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator it = subscribers_.find(update.second);
    if (it == subscribers_.end()) return;
    subscriber = it->second;
  }
  // Send the right message type, and compute the next deadline
  int64_t deadline_ms = 0;
  Status status;
  if (is_heartbeat) {
    status = SendHeartbeat(subscriber.get());
    if (status.code() == TErrorCode::RPC_TIMEOUT) {
      // Rewrite status to make it more useful, while preserving the stack
      status.SetErrorMsg(ErrorMsg(TErrorCode::RPC_TIMEOUT, Substitute(
          "Subscriber $0 ($1) timed-out during heartbeat RPC. Timeout is $2s.",
          subscriber->id(), lexical_cast<string>(subscriber->network_address()),
          FLAGS_statestore_heartbeat_tcp_timeout_seconds)));
    }

    deadline_ms = UnixMillis() + FLAGS_statestore_heartbeat_frequency_ms;
  } else {
    bool update_skipped;
    status = SendTopicUpdate(subscriber.get(), &update_skipped);
    if (status.code() == TErrorCode::RPC_TIMEOUT) {
      // Rewrite status to make it more useful, while preserving the stack
      status.SetErrorMsg(ErrorMsg(TErrorCode::RPC_TIMEOUT, Substitute(
          "Subscriber $0 ($1) timed-out during topic-update RPC. Timeout is $2s.",
          subscriber->id(), lexical_cast<string>(subscriber->network_address()),
          FLAGS_statestore_update_tcp_timeout_seconds)));
    }
    // If the subscriber responded that it skipped the last update sent, we assume that
    // it was busy doing something else, and back off slightly before sending another.
    int64_t update_interval = update_skipped ?
        (2 * FLAGS_statestore_update_frequency_ms) :
        FLAGS_statestore_update_frequency_ms;
    deadline_ms = UnixMillis() + update_interval;
  }

  {
    lock_guard<mutex> l(subscribers_lock_);
    // Check again if this registration has been removed while we were processing the
    // message.
    SubscriberMap::iterator it = subscribers_.find(update.second);
    if (it == subscribers_.end()) return;
    if (!status.ok()) {
      LOG(INFO) << "Unable to send " << hb_type << " message to subscriber "
                << update.second << ", received error: " << status.GetDetail();
    }

    const string& registration_id = PrintId(subscriber->registration_id());
    FailureDetector::PeerState state = is_heartbeat ?
        failure_detector_->UpdateHeartbeat(registration_id, status.ok()) :
        failure_detector_->GetPeerState(registration_id);

    if (state == FailureDetector::FAILED) {
      if (is_heartbeat) {
        // TODO: Consider if a metric to track the number of failures would be useful.
        LOG(INFO) << "Subscriber '" << subscriber->id() << "' has failed, disconnected "
                  << "or re-registered (last known registration ID: " << update.second
                  << ")";
        UnregisterSubscriber(subscriber.get());
      }
    } else {
      // Schedule the next message.
      VLOG(3) << "Next " << (is_heartbeat ? "heartbeat" : "update") << " deadline for: "
              << subscriber->id() << " is in " << deadline_ms << "ms";
      OfferUpdate(make_pair(deadline_ms, subscriber->id()), is_heartbeat ?
          &subscriber_heartbeat_threadpool_ : &subscriber_topic_update_threadpool_);
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
  lock_guard<mutex> topic_lock(topic_lock_);
  BOOST_FOREACH(Statestore::Subscriber::TransientEntryMap::value_type entry,
      subscriber->transient_entries()) {
    Statestore::TopicMap::iterator topic_it = topics_.find(entry.first.first);
    DCHECK(topic_it != topics_.end());
    topic_it->second.DeleteIfVersionsMatch(entry.second, // version
        entry.first.second); // key
  }
  num_subscribers_metric_->Increment(-1L);
  subscriber_set_metric_->Remove(subscriber->id());
  subscribers_.erase(subscriber->id());
}

Status Statestore::MainLoop() {
  subscriber_topic_update_threadpool_.Join();
  return Status::OK();
}
