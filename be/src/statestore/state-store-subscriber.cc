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

#include "statestore/state-store-subscriber.h"

#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/shared_mutex.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StateStoreService_types.h"
#include "rpc/thrift-util.h"
#include "util/time.h"
#include "util/debug-util.h"

using namespace std;
using namespace boost;
using namespace boost::posix_time;
using namespace ::apache::thrift;
using namespace ::apache::thrift::transport;

DEFINE_int32(statestore_subscriber_timeout_seconds, 10, "The amount of time (in seconds)"
     " that may elapse before the connection with the state-store is considered lost.");
DEFINE_int32(statestore_subscriber_cnxn_attempts, 10, "The number of times to retry an "
    "RPC connection to the statestore. A setting of 0 means retry indefinitely");
DEFINE_int32(statestore_subscriber_cnxn_retry_interval_ms, 3000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the state-store.");

namespace impala {

// Used to identify the statestore in the failure detector
const string STATE_STORE_ID = "STATESTORE";

// Duration, in ms, to sleep between attempts to reconnect to the
// state-store after a failure.
const int32_t SLEEP_INTERVAL_MS = 5000;

typedef ClientConnection<StateStoreServiceClient> StateStoreConnection;

// Proxy class for the subscriber heartbeat thrift API, which
// translates RPCs into method calls on the local subscriber object.
class StateStoreSubscriberThriftIf : public StateStoreSubscriberIf {
 public:
  StateStoreSubscriberThriftIf(StateStoreSubscriber* subscriber)
      : subscriber_(subscriber) { DCHECK(subscriber != NULL); }
  virtual void UpdateState(TUpdateStateResponse& response,
                           const TUpdateStateRequest& params) {
    TUniqueId registration_id;
    if (params.__isset.registration_id) {
      registration_id = params.registration_id;
    }

    subscriber_->UpdateState(params.topic_deltas, registration_id,
        &response.topic_updates).ToThrift(&response.status);
  }

 private:
  StateStoreSubscriber* subscriber_;
};

StateStoreSubscriber::StateStoreSubscriber(const std::string& subscriber_id,
    const TNetworkAddress& heartbeat_address, const TNetworkAddress& state_store_address,
    Metrics* metrics)
    : subscriber_id_(subscriber_id), heartbeat_address_(heartbeat_address),
      state_store_address_(state_store_address),
      thrift_iface_(new StateStoreSubscriberThriftIf(this)),
      failure_detector_(new TimeoutFailureDetector(
          seconds(FLAGS_statestore_subscriber_timeout_seconds),
          seconds(FLAGS_statestore_subscriber_timeout_seconds / 2))),
      is_registered_(false),
      client_cache_(new StateStoreClientCache(FLAGS_statestore_subscriber_cnxn_attempts,
          FLAGS_statestore_subscriber_cnxn_retry_interval_ms)) {
  connected_to_statestore_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric("statestore-subscriber.connected", false);
  last_recovery_duration_metric_ = metrics->CreateAndRegisterPrimitiveMetric(
      "statestore-subscriber.last-recovery-duration", 0.0);
  last_recovery_time_metric_ = metrics->CreateAndRegisterPrimitiveMetric<string>(
      "statestore-subscriber.last-recovery-time", "N/A");
  heartbeat_interval_metric_ = metrics->RegisterMetric(
      new StatsMetric<double>("statestore-subscriber.heartbeat-interval-time"));
  heartbeat_duration_metric_ = metrics->RegisterMetric(
      new StatsMetric<double>("statestore-subscriber.heartbeat-duration"));
  registration_id_metric_ = metrics->CreateAndRegisterPrimitiveMetric<string>(
      "statestore-susbcriber.registration-id", "N/A");
  client_cache_->InitMetrics(metrics, "statestore-subscriber.statestore");
}

Status StateStoreSubscriber::AddTopic(const StateStore::TopicId& topic_id,
    bool is_transient, const UpdateCallback& callback) {
  lock_guard<mutex> l(lock_);
  if (is_registered_) return Status("Subscriber already started, can't add new topic");
  update_callbacks_[topic_id].push_back(callback);
  topic_registrations_[topic_id] = is_transient;
  return Status::OK;
}

Status StateStoreSubscriber::Register() {
  Status client_status;
  StateStoreConnection client(client_cache_.get(), state_store_address_, &client_status);
  RETURN_IF_ERROR(client_status);

  TRegisterSubscriberRequest request;
  request.topic_registrations.reserve(update_callbacks_.size());
  BOOST_FOREACH(const UpdateCallbacks::value_type& topic, update_callbacks_) {
    TTopicRegistration thrift_topic;
    thrift_topic.topic_name = topic.first;
    thrift_topic.is_transient = topic_registrations_[topic.first];
    request.topic_registrations.push_back(thrift_topic);
  }

  request.subscriber_location = heartbeat_address_;
  request.subscriber_id = subscriber_id_;
  TRegisterSubscriberResponse response;
  try {
    client->RegisterSubscriber(response, request);
  } catch (apache::thrift::transport::TTransportException& e) {
    // Client may have been closed due to a failure
    RETURN_IF_ERROR(client.Reopen());
    try {
      client->RegisterSubscriber(response, request);
    } catch (apache::thrift::transport::TTransportException& e) {
      return Status(e.what());
    }
  }
  Status status = Status(response.status);
  if (status.ok()) connected_to_statestore_metric_->Update(true);
  if (response.__isset.registration_id) {
    lock_guard<mutex> l(registration_id_lock_);
    registration_id_ = response.registration_id;
    const string& registration_string = PrintId(registration_id_);
    registration_id_metric_->Update(registration_string);
    VLOG(1) << "Subscriber registration ID: " << registration_string;
  } else {
    VLOG(1) << "No subscriber registration ID received from statestore";
  }
  heartbeat_interval_timer_.Start();
  return status;
}

Status StateStoreSubscriber::Start() {
  Status status;
  {
    // Take the lock to ensure that, if a heartbeat is received during registration
    // (perhaps because Register() has succeeded, but we haven't finished setting up state
    // on the client side), UpdateState() will reject the heartbeat.
    lock_guard<mutex> l(lock_);
    LOG(INFO) << "Starting state-store subscriber";

    // Backend must be started before registration
    shared_ptr<TProcessor> processor(new StateStoreSubscriberProcessor(thrift_iface_));
    heartbeat_server_.reset(new ThriftServer("StateStoreSubscriber", processor,
        heartbeat_address_.port, NULL, NULL, 5));
    heartbeat_server_->Start();
    LOG(INFO) << "Registering with state-store";
    status = Register();
    if (status.ok()) {
      is_registered_ = true;
      LOG(INFO) << "State-store registration successful";
    } else {
      LOG(INFO) << "State-store registration unsuccessful: " << status.GetErrorMsg();
    }
  }

  // Registration is finished at this point, so it's fine to release the lock.
  recovery_mode_thread_.reset(new Thread("statestore-subscriber", "recovery-mode-thread",
      &StateStoreSubscriber::RecoveryModeChecker, this));

  return status;
}

void StateStoreSubscriber::RecoveryModeChecker() {
  failure_detector_->UpdateHeartbeat(STATE_STORE_ID, true);

  // Every few seconds, wake up and check if the failure detector has determined
  // that the state-store has failed from our perspective. If so, enter recovery
  // mode and try to reconnect, followed by reregistering all subscriptions.
  while (true) {
    if (failure_detector_->GetPeerState(STATE_STORE_ID) == FailureDetector::FAILED) {
      // When entering recovery mode, the class-wide lock_ is taken to
      // ensure mutual exclusion with any operations in flight.
      lock_guard<mutex> l(lock_);
      MonotonicStopWatch recovery_timer;
      recovery_timer.Start();
      connected_to_statestore_metric_->Update(false);
      LOG(INFO) << subscriber_id_
                << ": Connection with state-store lost, entering recovery mode";
      uint32_t attempt_count = 1;
      while (true) {
        LOG(INFO) << "Trying to re-register with state-store, attempt: "
                  << attempt_count++;
        Status status = Register();
        if (status.ok()) {
          LOG(INFO) << "Reconnected to state-store. Exiting recovery mode";
          // Make sure to update failure detector so that we don't
          // immediately fail on the next loop while we're waiting for
          // heartbeats to resume.
          failure_detector_->UpdateHeartbeat(STATE_STORE_ID, true);
          // Break out of enclosing while (true) to top of outer-scope loop.
          break;
        } else {
          // Don't exit recovery mode, continue
          LOG(WARNING) << "Failed to re-register with state-store: "
                       << status.GetErrorMsg();
          SleepForMs(SLEEP_INTERVAL_MS);
        }
        last_recovery_duration_metric_->Update(
            recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      }
      // When we're successful in re-registering, we don't do anything
      // to re-send our updates to the state-store. It is the
      // responsibility of individual clients to post missing updates
      // back to the state-store. This saves a lot of complexity where
      // we would otherwise have to cache updates here.
      last_recovery_duration_metric_->Update(
          recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      last_recovery_time_metric_->Update(TimestampValue::local_time().DebugString());
    }

    SleepForMs(SLEEP_INTERVAL_MS);
  }
}

Status StateStoreSubscriber::UpdateState(const TopicDeltaMap& incoming_topic_deltas,
    const TUniqueId& registration_id, vector<TTopicDelta>* subscriber_topic_updates) {
  failure_detector_->UpdateHeartbeat(STATE_STORE_ID, true);

  // We don't want to block here because this is an RPC, and delaying the return causes
  // the state-store to delay sending the next batch of heartbeats. The only time that
  // lock_ will be taken once UpdateState() might be called is in RecoveryModeChecker();
  // if we're in recovery mode we don't want to process the update.
  try_mutex::scoped_try_lock l(lock_);
  if (l) {
    {
      lock_guard<mutex> r(registration_id_lock_);
      // If this subscriber has just started, the registration_id_ may not have been set
      // despite the state-store starting to send updates. The 'unset' TUniqueId is 0:0,
      // so we can differentiate between a) an early UpdateState() from an eager
      // statestore, and b) an UpdateState() that's targeted to a previous registration.
      if (registration_id_ != TUniqueId() && registration_id != registration_id_) {
        stringstream ss;
        ss << "Unexpected registration ID: " << PrintId(registration_id)
           << ", was expecting: " << registration_id_;
        return Status(ss.str());
      }
    }

    // Only record heartbeats received when not in recovery mode
    heartbeat_interval_metric_->Update(
        heartbeat_interval_timer_.Reset() / (1000.0 * 1000.0 * 1000.0));
    MonotonicStopWatch sw;
    sw.Start();

    // Check the version ranges of all delta updates to ensure they can be applied
    // to this subscriber. If any invalid ranges are found, request new update(s) with
    // version ranges applicable to this subscriber.
    bool found_unexpected_delta = false;
    BOOST_FOREACH(const TopicDeltaMap::value_type& delta, incoming_topic_deltas) {
      TopicVersionMap::const_iterator itr = current_topic_versions_.find(delta.first);
      if (itr != current_topic_versions_.end()) {
        if (delta.second.is_delta && delta.second.from_version != itr->second) {
          LOG(ERROR) << "Unexpected delta update to topic '" << delta.first << "' of "
                     << "version range (" << delta.second.from_version << ":"
                     << delta.second.to_version << "]. Expected delta start version: "
                     << itr->second;

          subscriber_topic_updates->push_back(TTopicDelta());
          TTopicDelta& update = subscriber_topic_updates->back();
          update.topic_name = delta.second.topic_name;
          update.__set_from_version(itr->second);
          found_unexpected_delta = true;
        } else {
          // Update the current topic version
          current_topic_versions_[delta.first] = delta.second.to_version;
        }
      }
    }

    // Skip calling the callbacks when an unexpected delta update is found.
    if (!found_unexpected_delta) {
      BOOST_FOREACH(const UpdateCallbacks::value_type& callbacks, update_callbacks_) {
        BOOST_FOREACH(const UpdateCallback& callback, callbacks.second) {
          // TODO: Consider filtering the topics to only send registered topics to
          // callbacks
          callback(incoming_topic_deltas, subscriber_topic_updates);
        }
      }
    }
    sw.Stop();
    heartbeat_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
    return Status::OK;
  } else {
    stringstream ss;
    ss << "Subscriber '" << subscriber_id_
       << "' is registering with statestore, ignoring update.";
    return Status(ss.str());
  }
}


}
