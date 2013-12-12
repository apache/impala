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

#include <boost/foreach.hpp>

#include "common/status.h"
#include "statestore/state-store.h"
#include "gen-cpp/StateStoreService_types.h"
#include "statestore/failure-detector.h"
#include "rpc/thrift-util.h"
#include "util/debug-util.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include <boost/thread.hpp>

using namespace impala;
using namespace std;
using namespace boost;

DEFINE_int32(statestore_max_missed_heartbeats, 5, "Maximum number of consecutive "
    "heartbeats an impalad can miss before being declared failed by the "
    "state-store.");
DEFINE_int32(statestore_suspect_heartbeats, 2, "(Advanced) Number of consecutive "
    "heartbeats an impalad can miss before being suspected of failure by the"
    " state-store");
DEFINE_int32(statestore_num_heartbeat_threads, 10, "(Advanced) Number of threads used to "
    " send heartbeats in parallel to all registered subscribers.");
DEFINE_int32(statestore_heartbeat_frequency_ms, 500, "(Advanced) Frequency (in ms) with"
    " which state-store sends heartbeats to subscribers.");

DEFINE_int32(state_store_port, 24000, "port where StateStoreService is running");

DECLARE_int32(statestore_subscriber_timeout_seconds);

// Metric keys
// TODO: Replace 'backend' with 'subscriber' when we can coordinate a change with CM
const string STATESTORE_LIVE_SUBSCRIBERS = "statestore.live-backends";
const string STATESTORE_LIVE_SUBSCRIBERS_LIST = "statestore.live-backends.list";
const string STATESTORE_TOTAL_KEY_SIZE_BYTES = "statestore.total-key-size-bytes";
const string STATESTORE_TOTAL_VALUE_SIZE_BYTES = "statestore.total-value-size-bytes";
const string STATESTORE_TOTAL_TOPIC_SIZE_BYTES = "statestore.total-topic-size-bytes";

const StateStore::TopicEntry::Value StateStore::TopicEntry::NULL_VALUE = "";

// Initial version for each Topic registered by a Subscriber. Generally, the Topic will
// have a Version that is the MAX() of all entries in the Topic, but this initial
// value needs to be less than TopicEntry::TOPIC_ENTRY_INITIAL_VERSION to distinguish
// between the case where a Topic is empty and the case where the Topic only contains
// an item with the initial version.
const StateStore::TopicEntry::Version StateStore::Subscriber::TOPIC_INITIAL_VERSION = 0L;

// Used to control the maximum size of the heartbeat input queue, in which there is at
// most one entry per subscriber.
const int32_t STATESTORE_MAX_SUBSCRIBERS = 10000;

// Heartbeats that miss their deadline by this much are logged.
const uint32_t HEARTBEAT_WARN_THRESHOLD_MS = 2000;

typedef ClientConnection<StateStoreSubscriberClient> StateStoreSubscriberConnection;

class StateStoreThriftIf : public StateStoreServiceIf {
 public:
  StateStoreThriftIf(StateStore* state_store)
      : state_store_(state_store) {
    DCHECK(state_store_ != NULL);
  }

  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& params) {
    TUniqueId registration_id;
    Status status = state_store_->RegisterSubscriber(params.subscriber_id,
        params.subscriber_location, params.topic_registrations, &registration_id);
    status.ToThrift(&response.status);
    response.__set_registration_id(registration_id);
  }
 private:
  StateStore* state_store_;
};

void StateStore::TopicEntry::SetValue(const StateStore::TopicEntry::Value& bytes,
    TopicEntry::Version version) {
  DCHECK(bytes == StateStore::TopicEntry::NULL_VALUE || bytes.size() > 0);
  value_ = bytes;
  version_ = version;
}

StateStore::TopicEntry::Version StateStore::Topic::Put(const string& key,
    const StateStore::TopicEntry::Value& bytes) {
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

void StateStore::Topic::DeleteIfVersionsMatch(TopicEntry::Version version,
    const StateStore::TopicEntryKey& key) {
  TopicEntryMap::iterator entry_it = entries_.find(key);
  if (entry_it != entries_.end() && entry_it->second.version() == version) {
    // Add a new entry with the the version history for this deletion and remove
    // the old entry
    topic_update_log_.erase(version);
    topic_update_log_.insert(make_pair(++last_version_, key));
    total_value_size_bytes_ -= entry_it->second.value().size();
    DCHECK_GE(total_value_size_bytes_, 0L);

    value_size_metric_->Increment(entry_it->second.value().size());
    topic_size_metric_->Increment(entry_it->second.value().size());
    entry_it->second.SetValue(StateStore::TopicEntry::NULL_VALUE, last_version_);
  }
}

StateStore::Subscriber::Subscriber(const SubscriberId& subscriber_id,
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

void StateStore::Subscriber::AddTransientUpdate(const TopicId& topic_id,
    const TopicEntryKey& topic_key, TopicEntry::Version version) {
  // Only record the update if the topic is transient
  const Topics::const_iterator topic_it = subscribed_topics_.find(topic_id);
  DCHECK(topic_it != subscribed_topics_.end());
  if (topic_it->second.is_transient == true) {
    transient_entries_[make_pair(topic_id, topic_key)] = version;
  }
}

const StateStore::TopicEntry::Version StateStore::Subscriber::LastTopicVersionProcessed(
    const TopicId& topic_id) const {
  Topics::const_iterator itr = subscribed_topics_.find(topic_id);
  return itr == subscribed_topics_.end() ?
      TOPIC_INITIAL_VERSION : itr->second.last_version;
}

void StateStore::Subscriber::SetLastTopicVersionProcessed(const TopicId& topic_id,
    TopicEntry::Version version) {
  subscribed_topics_[topic_id].last_version = version;
}

StateStore::StateStore(Metrics* metrics)
  : exit_flag_(false),
    subscriber_heartbeat_threadpool_("statestore",
        "subscriber-heartbeat-worker",
        FLAGS_statestore_num_heartbeat_threads,
        STATESTORE_MAX_SUBSCRIBERS,
        bind<void>(mem_fn(&StateStore::UpdateSubscriber), this, _1, _2)),
    client_cache_(new ClientCache<StateStoreSubscriberClient>()),
    thrift_iface_(new StateStoreThriftIf(this)),
    failure_detector_(
        new MissedHeartbeatFailureDetector(FLAGS_statestore_max_missed_heartbeats,
                                           FLAGS_statestore_suspect_heartbeats)) {

  DCHECK(metrics != NULL);
  num_subscribers_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(STATESTORE_LIVE_SUBSCRIBERS, 0L);
  subscriber_set_metric_ =
      metrics->RegisterMetric(new SetMetric<string>(STATESTORE_LIVE_SUBSCRIBERS_LIST,
                                                    set<string>()));
  key_size_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(STATESTORE_TOTAL_KEY_SIZE_BYTES, 0L);
  value_size_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(STATESTORE_TOTAL_VALUE_SIZE_BYTES, 0L);
  topic_size_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(STATESTORE_TOTAL_TOPIC_SIZE_BYTES, 0L);
  client_cache_->InitMetrics(metrics, "subscriber");
}

void StateStore::RegisterWebpages(Webserver* webserver) {
  Webserver::PathHandlerCallback topics_callback =
      bind<void>(mem_fn(&StateStore::TopicsHandler), this, _1, _2);
  webserver->RegisterPathHandler("/topics", topics_callback);

  Webserver::PathHandlerCallback subscribers_callback =
      bind<void>(mem_fn(&StateStore::SubscribersHandler), this, _1, _2);
  webserver->RegisterPathHandler("/subscribers", subscribers_callback);
}

void StateStore::TopicsHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  (*output) << "<h2>Topics</h2>";
  (*output) << "<table class='table table-striped'>"
            << "<tr><th>Topic Id</th>"
            << "<th>Number of entries</th>"
            << "<th>Version</th>"
            << "<th>Oldest subscriber version</th>"
            << "<th>Oldest subscriber Id</th>"
            << "<th>Size (keys/values/total)</th>"
            << "</tr>";

  lock_guard<mutex> l(subscribers_lock_);
  lock_guard<mutex> t(topic_lock_);
  BOOST_FOREACH(const TopicMap::value_type& topic, topics_) {
    SubscriberId oldest_subscriber_id;
    TopicEntry::Version oldest_subscriber_version =
        GetMinSubscriberTopicVersion(topic.first, &oldest_subscriber_id);
    int64_t key_size = topic.second.total_key_size_bytes();
    int64_t value_size = topic.second.total_value_size_bytes();
    (*output) << "<tr><td>" << topic.second.id() << "</td><td>"
              << topic.second.entries().size() << "</td><td>"
              << topic.second.last_version() << "</td><td>"
              << oldest_subscriber_version  << "</td><td>"
              << oldest_subscriber_id << "</td><td>"
              << PrettyPrinter::Print(key_size, TCounterType::BYTES) << " / "
              << PrettyPrinter::Print(value_size, TCounterType::BYTES) << " / "
              << PrettyPrinter::Print(key_size + value_size, TCounterType::BYTES)
              << "</td></tr>";
  }
  (*output) << "</table>";
}

void StateStore::SubscribersHandler(const Webserver::ArgumentMap& args,
    stringstream* output) {
  (*output) << "<h2>Subscribers</h2>";
  (*output) << "<table class ='table table-striped'>"
            << "<tr><th>Id</th><th>Address</th><th>Subscribed topics</th>"
            << "<th>Transient entries</th>"
            << "<th>Registration Id</th></tr>";

  lock_guard<mutex> l(subscribers_lock_);
  BOOST_FOREACH(const SubscriberMap::value_type& subscriber, subscribers_) {
    (*output) << "<tr><td>" << subscriber.second->id() << "</td><td>"
              << subscriber.second->network_address() << "</td><td>"
              << subscriber.second->subscribed_topics().size() << "</td><td>"
              << subscriber.second->transient_entries().size() << "</td><td>"
              << subscriber.second->registration_id() << "</td></tr>";
  }
  (*output) << "</table>";
}

Status StateStore::RegisterSubscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& location,
    const vector<TTopicRegistration>& topic_registrations, TUniqueId* registration_id) {
  if (subscriber_id.empty()) return Status("Subscriber ID cannot be empty string");

  // Create any new topics first, so that when the subscriber is first
  // sent a heartbeat by the worker threads its topics are guaranteed
  // to exist.
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
    num_subscribers_metric_->Update(subscribers_.size());
    subscriber_set_metric_->Add(subscriber_id);
  }

  // Add the subscriber to the update queue, with an immediate schedule.
  if (subscriber_heartbeat_threadpool_.GetQueueSize() >= STATESTORE_MAX_SUBSCRIBERS
      || !subscriber_heartbeat_threadpool_.Offer(make_pair(0L, subscriber_id))) {
    stringstream ss;
    ss << "Maximum subscriber limit reached: " << STATESTORE_MAX_SUBSCRIBERS;
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator subscriber_it = subscribers_.find(subscriber_id);
    DCHECK(subscriber_it != subscribers_.end());
    subscribers_.erase(subscriber_it);
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  LOG(INFO) << "Subscriber '" << subscriber_id << "' registered (registration id: "
            << PrintId(*registration_id) << ")";
  return Status::OK;
}

Status StateStore::ProcessOneSubscriber(Subscriber* subscriber) {
  // First thing: make a list of updates to send
  TUpdateStateRequest update_state_request;
  GatherTopicUpdates(*subscriber, &update_state_request);

  // Set the expected registration ID, so that the subscriber can reject this update if
  // they have moved on to a new registration instance.
  update_state_request.__set_registration_id(subscriber->registration_id());

  // Second: try and send it
  Status status;
  StateStoreSubscriberConnection client(client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  TUpdateStateResponse response;

  // TODO: Rework the client-cache API so that this dance isn't necessary.
  try {
    client->UpdateState(response, update_state_request);
  } catch (apache::thrift::transport::TTransportException& e) {
    // Client may have been closed due to a failure
    RETURN_IF_ERROR(client.Reopen());
    try {
      client->UpdateState(response, update_state_request);
    } catch (apache::thrift::transport::TTransportException& e) {
      return Status(e.what());
    }
  }
  RETURN_IF_ERROR(Status(response.status));

  // At this point the updates are assumed to have been successfully processed
  // by the subscriber. Update the subscriber's max version of each topic.
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

      // The subscriber sent back their from_version which indicates that they
      // want to reset their max version for this topic to this value. The next update
      // sent will be from this version.
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
            topic->Put(key, StateStore::TopicEntry::NULL_VALUE);
        subscriber->AddTransientUpdate(update.topic_name, key, version);
      }
    }
  }
  return Status::OK;
}

void StateStore::GatherTopicUpdates(const Subscriber& subscriber,
    TUpdateStateRequest* update_state_request) {
  {
    lock_guard<mutex> l(topic_lock_);
    BOOST_FOREACH(const Subscriber::Topics::value_type& subscribed_topic,
        subscriber.subscribed_topics()) {
      TopicMap::const_iterator topic_it = topics_.find(subscribed_topic.first);
      DCHECK(topic_it != topics_.end());

      TTopicDelta& topic_delta =
          update_state_request->topic_deltas[subscribed_topic.first];
      topic_delta.topic_name = subscribed_topic.first;

      // If the subscriber version is > 0, send this update as a delta. Otherwise, this
      // is a new subscriber so send them a non-delta update that includes all items
      // in the topic.
      TopicEntry::Version last_processed_version =
          subscriber.LastTopicVersionProcessed(topic_it->first);
      topic_delta.is_delta = last_processed_version > Subscriber::TOPIC_INITIAL_VERSION;
      topic_delta.__set_from_version(last_processed_version);

      const Topic& topic = topic_it->second;
      TopicUpdateLog::const_iterator next_update =
          topic.topic_update_log().upper_bound(last_processed_version);

      for (; next_update != topic.topic_update_log().end(); ++next_update) {
        TopicEntryMap::const_iterator itr = topic.entries().find(next_update->second);
        DCHECK(itr != topic.entries().end());
        const TopicEntry& topic_entry = itr->second;
        if (topic_entry.value() == StateStore::TopicEntry::NULL_VALUE) {
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
        // The largest version for this topic will be the last item in the version
        // history map.
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

const StateStore::TopicEntry::Version StateStore::GetMinSubscriberTopicVersion(
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

bool StateStore::ShouldExit() {
  lock_guard<mutex> l(exit_flag_lock_);
  return exit_flag_;
}

void StateStore::SetExitFlag() {
  lock_guard<mutex> l(exit_flag_lock_);
  exit_flag_ = true;
  subscriber_heartbeat_threadpool_.Shutdown();
}

void StateStore::UpdateSubscriber(int thread_id,
    const ScheduledSubscriberUpdate& update) {
  int64_t update_deadline = update.first;
  if (update_deadline != 0L) {
    // Wait until deadline.
    int64_t diff_ms = update_deadline - ms_since_epoch();
    while (diff_ms > 0) {
      SleepForMs(diff_ms);
      diff_ms = update_deadline - ms_since_epoch();
    }
    VLOG(3) << "Sending update to: " << update.second << " (deadline accuracy: "
            << abs(diff_ms) << "ms)";

    if (diff_ms > HEARTBEAT_WARN_THRESHOLD_MS) {
      // TODO: This should be a healthcheck in a monitored metric in CM, which would
      // require a 'rate' metric type.
      LOG(WARNING) << "Missed subscriber (" << update.second << ") deadline by "
                   << diff_ms << "ms";
    }
  } else {
    // The first update is scheduled immediately and has a deadline of 0. There's no need
    // to wait.
    VLOG(3) << "Initial update to: " << update.second;
  }
  shared_ptr<Subscriber> subscriber;
  {
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator it = subscribers_.find(update.second);
    if (it == subscribers_.end()) return;
    subscriber = it->second;
  }
  // Give up the lock here so that others can get to the queue
  Status status = ProcessOneSubscriber(subscriber.get());
  {
    lock_guard<mutex> l(subscribers_lock_);
    // Check again if this registration has been removed while we were processing the
    // heartbeat.
    SubscriberMap::iterator it = subscribers_.find(update.second);
    if (it == subscribers_.end()) return;

    if (!status.ok()) {
      LOG(INFO) << "Unable to update subscriber at " << subscriber->network_address()
                << ", received error " << status.GetErrorMsg();
    }

    if (failure_detector_->UpdateHeartbeat(PrintId(
        subscriber->registration_id()), status.ok()) == FailureDetector::FAILED) {
      // TODO: Consider if a metric to track the number of failures would be useful.
      LOG(INFO) << "Subscriber '" << subscriber->id() << "' has failed, disconnected "
                << "or re-registered (last known registration ID: "
                << PrintId(subscriber->registration_id()) << ")";
      UnregisterSubscriber(subscriber.get());
    } else {
      // Schedule the next heartbeat.
      int64_t deadline_ms = ms_since_epoch() + FLAGS_statestore_heartbeat_frequency_ms;
      VLOG(3) << "Next deadline for: " << subscriber->id() << " is in "
              << FLAGS_statestore_heartbeat_frequency_ms << "ms";
      subscriber_heartbeat_threadpool_.Offer(make_pair(deadline_ms, subscriber->id()));
    }
  }
}

void StateStore::UnregisterSubscriber(Subscriber* subscriber) {
  SubscriberMap::const_iterator it = subscribers_.find(subscriber->id());
  if (it == subscribers_.end() ||
      it->second->registration_id() != subscriber->registration_id()) {
    // Already failed and / or replaced with a new registration
    return;
  }

  // Close all active clients so that the next attempt to use them causes a Reopen()
  client_cache_->CloseConnections(subscriber->network_address());

  // Prevent the failure detector from growing without bound
  failure_detector_->EvictPeer(PrintId(subscriber->registration_id()));

  // Delete all transient entries
  lock_guard<mutex> topic_lock(topic_lock_);
  BOOST_FOREACH(StateStore::Subscriber::TransientEntryMap::value_type entry,
      subscriber->transient_entries()) {
    StateStore::TopicMap::iterator topic_it = topics_.find(entry.first.first);
    DCHECK(topic_it != topics_.end());
    topic_it->second.DeleteIfVersionsMatch(entry.second, // version
        entry.first.second); // key
  }
  num_subscribers_metric_->Increment(-1L);
  subscriber_set_metric_->Remove(subscriber->id());
  subscribers_.erase(subscriber->id());
}

Status StateStore::MainLoop() {
  subscriber_heartbeat_threadpool_.Join();
  return Status::OK;
}
