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
#include "util/stopwatch.h"
#include "util/thrift-util.h"
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

DEFINE_int32(state_store_port, 24000, "port where StateStoreService is running");

// Metric keys
// TODO: Replace 'backend' with 'subscriber' when we can coordinate a change with CM
const string STATESTORE_LIVE_SUBSCRIBERS = "statestore.live-backends";
const string STATESTORE_LIVE_SUBSCRIBERS_LIST = "statestore.live-backends.list";
const string STATESTORE_LAST_UPDATE_LOOP_TIME =
    "statestore.last-update-loop-time.seconds";

const StateStore::TopicEntry::Value StateStore::TopicEntry::NULL_VALUE = "";

typedef ClientConnection<StateStoreSubscriberClient> StateStoreSubscriberConnection;

class StateStoreThriftIf : public StateStoreServiceIf {
 public:
  StateStoreThriftIf(StateStore* state_store)
      : state_store_(state_store) {
    DCHECK(state_store_ != NULL);
  }

  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
                                  const TRegisterSubscriberRequest& params) {
    Status status = state_store_->RegisterSubscriber(params.subscriber_id,
        params.subscriber_location, params.topic_registrations);
    status.ToThrift(&response.status);
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
  if (entry_it == entries_.end()) {
    entry_it = entries_.insert(make_pair(key, TopicEntry())).first;
  }
  entry_it->second.SetValue(bytes, last_version_++);
  return entry_it->second.version();
}

void StateStore::Topic::DeleteIfVersionsMatch(TopicEntry::Version version,
    const StateStore::TopicEntryKey& key) {
  TopicEntryMap::iterator entry_it = entries_.find(key);
  if (entry_it != entries_.end() && entry_it->second.version() == version) {
    entry_it->second.SetValue(StateStore::TopicEntry::NULL_VALUE, last_version_++);
  }
}

StateStore::Subscriber::Subscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& network_address,
    const vector<TTopicRegistration>& subscribed_topics)
      : subscriber_id_(subscriber_id), network_address_(network_address) {
  BOOST_FOREACH(const TTopicRegistration& topic, subscribed_topics) {
    subscribed_topics_[topic.topic_name] = topic.is_transient;
  }
}

void StateStore::Subscriber::AddTransientUpdate(const TopicId& topic_id,
    const TopicEntryKey& topic_key, TopicEntry::Version version) {
  // Only record the update if the topic is transient
  const Topics::const_iterator topic_it = subscribed_topics_.find(topic_id);
  DCHECK(topic_it != subscribed_topics_.end());
  if (topic_it->second == true) {
    transient_entries_[make_pair(topic_id, topic_key)] = version;
  }
}

StateStore::StateStore(Metrics* metrics)
  : exit_flag_(false),
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
  last_heartbeat_loop_time_metric_ = metrics->RegisterMetric(
      new StatsMetric<double>(STATESTORE_LAST_UPDATE_LOOP_TIME));
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
            << "<tr><th>Topic Id</th><th>Number of entries</th></tr>";

  lock_guard<mutex> l(topic_lock_);
  BOOST_FOREACH(const TopicMap::value_type& topic, topics_) {
    (*output) << "<tr><td>" << topic.second.id() << "</td>";
    (*output) << "<td>" << topic.second.entries().size() << "</td></tr>";
  }
  (*output) << "</table>";
}

void StateStore::SubscribersHandler(const Webserver::ArgumentMap& args,
                                     stringstream* output) {
  (*output) << "<h2>Subscribers</h2>";
  (*output) << "<table class ='table table-striped'>"
            << "<tr><th>Id</th><th>Address</th><th>Subscribed topics</th>"
            << "<th>Transient entries</th></tr>";
  lock_guard<mutex> l(subscribers_lock_);
  BOOST_FOREACH(const SubscriberMap::value_type& subscriber, subscribers_) {
    (*output) << "<tr><td>" << subscriber.second.id() << "</td><td>"
              << subscriber.second.network_address() << "</td><td>"
              << subscriber.second.subscribed_topics().size() << "</td><td>"
              << subscriber.second.transient_entries().size() << "</td></tr>";
  }
  (*output) << "</table>";
}

Status StateStore::RegisterSubscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& location,
    const vector<TTopicRegistration>& topic_registrations) {
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
        topics_.insert(make_pair(topic.topic_name, Topic(topic.topic_name)));
      }
    }
  }
  LOG(INFO) << "Registering: " << subscriber_id;
  {
    lock_guard<mutex> l(subscribers_lock_);
    SubscriberMap::iterator subscriber_it = subscribers_.find(subscriber_id);
    if (subscriber_it != subscribers_.end()) {
      LOG(INFO) << "Re-registering subscriber: " << subscriber_id
                << ", possible duplicate subscriber IDs";
    }

    subscribers_.insert(make_pair(subscriber_id,
        Subscriber(subscriber_id, location, topic_registrations)));
    failure_detector_->UpdateHeartbeat(subscriber_id, true);
    num_subscribers_metric_->Update(subscribers_.size());
    subscriber_set_metric_->Add(subscriber_id);
  }

  return Status::OK;
}

Status StateStore::ProcessOneSubscriber(Subscriber* subscriber) {
  // First thing: make a list of updates to send
  TUpdateStateRequest update_state_request;
  {
    lock_guard<mutex> l(topic_lock_);

    BOOST_FOREACH(const Subscriber::Topics::value_type& topic,
        subscriber->subscribed_topics()) {
      TopicMap::const_iterator topic_it = topics_.find(topic.first);
      DCHECK(topic_it != topics_.end());

      TTopicDelta& topic_delta = update_state_request.topic_deltas[topic.first];
      topic_delta.topic_name = topic.first;
      BOOST_FOREACH(const TopicEntryMap::value_type& entry, topic_it->second.entries()) {
        if (entry.second.value() == StateStore::TopicEntry::NULL_VALUE) {
          // NULL -> deletion
          topic_delta.topic_deletions.push_back(entry.first);
        } else {
          topic_delta.topic_entries.push_back(TTopicItem());
          TTopicItem& topic_item = topic_delta.topic_entries.back();
          topic_item.key = entry.first;
          // TODO: Does this do a needless copy?
          topic_item.value = entry.second.value();
        }
      }
      // TODO: Compute deltas
      topic_delta.is_delta = false;
    }
  }

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

  // Thirdly: perform any / all updates returned by the subscriber
  {
    lock_guard<mutex> l(topic_lock_);
    BOOST_FOREACH(const TTopicUpdate& update, response.topic_updates) {
      TopicMap::iterator topic_it = topics_.find(update.topic_name);
      if (topic_it == topics_.end()) {
        VLOG(1) << "Received update for unexpected topic:" << update.topic_name;
        continue;
      }

      Topic* topic = &topic_it->second;

      BOOST_FOREACH(const TTopicItem& item, update.topic_updates) {
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

bool StateStore::ShouldExit() {
  lock_guard<mutex> l(exit_flag_lock_);
  return exit_flag_;
}

void StateStore::SetExitFlag() {
  lock_guard<mutex> l(exit_flag_lock_);
  exit_flag_ = true;
}

void StateStore::SubscriberUpdateLoop() {
  while (!ShouldExit()) {
    Subscriber* subscriber = NULL;
    {
      // Wait for work. Not all threads will necessarily exit this
      // loop on every main-loop iteration, if the other threads are
      // quick and process the entire queue before this thread can
      // take the lock.
      unique_lock<mutex> l(worker_lock_);
      while (subscriber_work_queue_.empty() && !ShouldExit()) {
        work_available_.wait(l);
      }

      if (ShouldExit()) break;

      subscriber = subscriber_work_queue_.back();
      subscriber_work_queue_.pop_back();
    }
    DCHECK(subscriber != NULL);

    // Give up the lock here so that others can get to the queue
    Status status = ProcessOneSubscriber(subscriber);
    if (!status.ok()) {
      if (failure_detector_->GetPeerState(subscriber->id()) == FailureDetector::OK) {
        LOG(INFO) << "Unable to update subscriber at " << subscriber->network_address()
                  << ",  received error " << status.GetErrorMsg();
      }
    }

    if (failure_detector_->UpdateHeartbeat(subscriber->id(), status.ok()) ==
        FailureDetector::FAILED) {
      lock_guard<mutex> l(subscribers_lock_);
      // TODO: Make clear that 'failure' isn't necessarily an error
      LOG(INFO) << "Subscriber: " << subscriber->id()
                << " has either failed or disconnected.";
      // Close all active clients so that the next attempt to use them causes a Reopen
      client_cache_->CloseConnections(subscriber->network_address());

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

    {
      unique_lock<mutex> l(worker_lock_);
      if (--num_subscribers_remaining_ == 0) {
        // Wake up the master loop. Only one thread will ever set
        // num_subscribers_remaining_ to 0, and only then when all
        // subscribers have been correctly processed.
        last_subscriber_processed_.notify_one();
      }
    }
  }
}

Status StateStore::MainLoop() {
  // We create some number of worker threads to process subscriber
  // connections in parallel.
  vector<thread*> worker_threads;

  for (int i = 0; i < FLAGS_statestore_num_heartbeat_threads; ++i) {
    worker_threads.push_back(
        new thread(&StateStore::SubscriberUpdateLoop, this));
  }

  // The main loop works as follows. This method puts a single loop's
  // worth of work in subscriber_work_queue_, and then notifies all
  // worker threads to start processing. The loop then waits for the
  // threads to collectively process the entire queue, then the last
  // worker thread notifies this master thread, which then wakes up
  // and re-populates the queue. In this way, new subscribers are
  // picked up on the first loop after they are registered.

  // The work queue is protected by a shared lock, so all the worker
  // threads are idle while work is being added to it. The idea here
  // is not to avoid idleness, but to make sure that the minimal
  // amount of time is spent waiting for socket communication. It is
  // cheap to contend for a mutex compared to the cost of opening and
  // writing to a socket.

  // subscribers_lock_ is also held while the work queue is being
  // built, to avoid concurrent modification of the subscriber set.

  while (!ShouldExit()) {
    MonotonicStopWatch loop_timer;
    loop_timer.Start();
    {
      unique_lock<mutex> subscriber_lock(subscribers_lock_);
      unique_lock<mutex> worker_lock(worker_lock_);
      BOOST_FOREACH(SubscriberMap::value_type& subscriber, subscribers_) {
        subscriber_work_queue_.push_back(&subscriber.second);
      }
      // Initial condition - num_subscribers_remaining_ is the number
      // of subscribers to process.
      num_subscribers_remaining_ = subscriber_work_queue_.size();
    }

    // Wind 'em up and let them go
    work_available_.notify_all();

    {
      // Wait until all subscribers have been processed.
      unique_lock<mutex> l(worker_lock_);
      while (num_subscribers_remaining_ > 0) {
        last_subscriber_processed_.wait(l);
      }
    }
    loop_timer.Stop();
    last_heartbeat_loop_time_metric_->Update(
        loop_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    // TODO: configure this
    usleep(500 * 1000);
  }

  work_available_.notify_all();

  BOOST_FOREACH(thread* worker_thread, worker_threads) {
    worker_thread->join();
  }

  return Status::OK;
}
