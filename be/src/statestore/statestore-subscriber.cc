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

#include "statestore/statestore-subscriber.h"

#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-util.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "util/time.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::posix_time::seconds;
using namespace apache::thrift;
using namespace strings;

DEFINE_int32(statestore_subscriber_timeout_seconds, 30, "The amount of time (in seconds)"
     " that may elapse before the connection with the statestore is considered lost.");
DEFINE_int32(statestore_subscriber_cnxn_attempts, 10, "The number of times to retry an "
    "RPC connection to the statestore. A setting of 0 means retry indefinitely");
DEFINE_int32(statestore_subscriber_cnxn_retry_interval_ms, 3000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the statestore.");
DECLARE_string(ssl_client_ca_certificate);

DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);

namespace impala {

// Used to identify the statestore in the failure detector
const string STATESTORE_ID = "STATESTORE";

// Template for metrics that measure the processing time for individual topics.
const string CALLBACK_METRIC_PATTERN = "statestore-subscriber.topic-$0.processing-time-s";

// Duration, in ms, to sleep between attempts to reconnect to the
// statestore after a failure.
const int32_t SLEEP_INTERVAL_MS = 5000;

typedef ClientConnection<StatestoreServiceClientWrapper> StatestoreServiceConn;

// Proxy class for the subscriber heartbeat thrift API, which
// translates RPCs into method calls on the local subscriber object.
class StatestoreSubscriberThriftIf : public StatestoreSubscriberIf {
 public:
  StatestoreSubscriberThriftIf(StatestoreSubscriber* subscriber)
      : subscriber_(subscriber) { DCHECK(subscriber != NULL); }
  virtual void UpdateState(TUpdateStateResponse& response,
                           const TUpdateStateRequest& params) {
    TUniqueId registration_id;
    if (params.__isset.registration_id) {
      registration_id = params.registration_id;
    }

    subscriber_->UpdateState(params.topic_deltas, registration_id,
        &response.topic_updates, &response.skipped).ToThrift(&response.status);
    // Make sure Thrift thinks the field is set.
    response.__set_skipped(response.skipped);
  }

  virtual void Heartbeat(THeartbeatResponse& response, const THeartbeatRequest& request) {
    subscriber_->Heartbeat(request.registration_id);
  }

 private:
  StatestoreSubscriber* subscriber_;
};

StatestoreSubscriber::StatestoreSubscriber(const std::string& subscriber_id,
    const TNetworkAddress& heartbeat_address, const TNetworkAddress& statestore_address,
    MetricGroup* metrics)
    : subscriber_id_(subscriber_id), heartbeat_address_(heartbeat_address),
      statestore_address_(statestore_address),
      thrift_iface_(new StatestoreSubscriberThriftIf(this)),
      failure_detector_(new TimeoutFailureDetector(
          seconds(FLAGS_statestore_subscriber_timeout_seconds),
          seconds(FLAGS_statestore_subscriber_timeout_seconds / 2))),
      is_registered_(false),
      client_cache_(new StatestoreClientCache(FLAGS_statestore_subscriber_cnxn_attempts,
          FLAGS_statestore_subscriber_cnxn_retry_interval_ms, 0, 0, "",
          !FLAGS_ssl_client_ca_certificate.empty())),
      metrics_(metrics->GetOrCreateChildGroup("statestore-subscriber")) {
  connected_to_statestore_metric_ =
      metrics_->AddProperty("statestore-subscriber.connected", false);
  last_recovery_duration_metric_ = metrics_->AddGauge(
      "statestore-subscriber.last-recovery-duration", 0.0);
  last_recovery_time_metric_ = metrics_->AddProperty<string>(
      "statestore-subscriber.last-recovery-time", "N/A");
  topic_update_interval_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "statestore-subscriber.topic-update-interval-time");
  topic_update_duration_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "statestore-subscriber.topic-update-duration");
  heartbeat_interval_metric_ = StatsMetric<double>::CreateAndRegister(metrics,
      "statestore-subscriber.heartbeat-interval-time");

  registration_id_metric_ = metrics->AddProperty<string>(
      "statestore-subscriber.registration-id", "N/A");

  client_cache_->InitMetrics(metrics, "statestore-subscriber.statestore");
}

Status StatestoreSubscriber::AddTopic(const Statestore::TopicId& topic_id,
    bool is_transient, const UpdateCallback& callback) {
  lock_guard<mutex> l(lock_);
  if (is_registered_) return Status("Subscriber already started, can't add new topic");
  Callbacks* cb = &(update_callbacks_[topic_id]);
  cb->callbacks.push_back(callback);
  if (cb->processing_time_metric == NULL) {
    cb->processing_time_metric = StatsMetric<double>::CreateAndRegister(metrics_,
        CALLBACK_METRIC_PATTERN, topic_id);
  }
  topic_registrations_[topic_id] = is_transient;
  return Status::OK();
}

Status StatestoreSubscriber::Register() {
  Status client_status;
  StatestoreServiceConn client(client_cache_.get(), statestore_address_, &client_status);
  RETURN_IF_ERROR(client_status);

  TRegisterSubscriberRequest request;
  request.topic_registrations.reserve(update_callbacks_.size());
  for (const UpdateCallbacks::value_type& topic: update_callbacks_) {
    TTopicRegistration thrift_topic;
    thrift_topic.topic_name = topic.first;
    thrift_topic.is_transient = topic_registrations_[topic.first];
    request.topic_registrations.push_back(thrift_topic);
  }

  request.subscriber_location = heartbeat_address_;
  request.subscriber_id = subscriber_id_;
  TRegisterSubscriberResponse response;
  RETURN_IF_ERROR(client.DoRpc(&StatestoreServiceClientWrapper::RegisterSubscriber,
      request, &response));
  Status status = Status(response.status);
  if (status.ok()) connected_to_statestore_metric_->set_value(true);
  if (response.__isset.registration_id) {
    lock_guard<mutex> l(registration_id_lock_);
    registration_id_ = response.registration_id;
    const string& registration_string = PrintId(registration_id_);
    registration_id_metric_->set_value(registration_string);
    VLOG(1) << "Subscriber registration ID: " << registration_string;
  } else {
    VLOG(1) << "No subscriber registration ID received from statestore";
  }
  topic_update_interval_timer_.Start();
  heartbeat_interval_timer_.Start();
  return status;
}

Status StatestoreSubscriber::Start() {
  Status status;
  {
    // Take the lock to ensure that, if a topic-update is received during registration
    // (perhaps because Register() has succeeded, but we haven't finished setting up state
    // on the client side), UpdateState() will reject the message.
    lock_guard<mutex> l(lock_);
    LOG(INFO) << "Starting statestore subscriber";

    // Backend must be started before registration
    boost::shared_ptr<TProcessor> processor(
        new StatestoreSubscriberProcessor(thrift_iface_));
    boost::shared_ptr<TProcessorEventHandler> event_handler(
        new RpcEventHandler("statestore-subscriber", metrics_));
    processor->setEventHandler(event_handler);

    heartbeat_server_.reset(new ThriftServer("StatestoreSubscriber", processor,
        heartbeat_address_.port, NULL, NULL, 5));
    if (EnableInternalSslConnections()) {
      LOG(INFO) << "Enabling SSL for Statestore subscriber";
      RETURN_IF_ERROR(heartbeat_server_->EnableSsl(FLAGS_ssl_server_certificate,
          FLAGS_ssl_private_key, FLAGS_ssl_private_key_password_cmd));
    }
    RETURN_IF_ERROR(heartbeat_server_->Start());

    LOG(INFO) << "Registering with statestore";
    status = Register();
    if (status.ok()) {
      is_registered_ = true;
      LOG(INFO) << "statestore registration successful";
    } else {
      LOG(INFO) << "statestore registration unsuccessful: " << status.GetDetail();
    }
  }

  // Registration is finished at this point, so it's fine to release the lock.
  recovery_mode_thread_.reset(new Thread("statestore-subscriber", "recovery-mode-thread",
      &StatestoreSubscriber::RecoveryModeChecker, this));

  return status;
}

void StatestoreSubscriber::RecoveryModeChecker() {
  failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);

  // Every few seconds, wake up and check if the failure detector has determined
  // that the statestore has failed from our perspective. If so, enter recovery
  // mode and try to reconnect, followed by reregistering all subscriptions.
  while (true) {
    if (failure_detector_->GetPeerState(STATESTORE_ID) == FailureDetector::FAILED) {
      // When entering recovery mode, the class-wide lock_ is taken to
      // ensure mutual exclusion with any operations in flight.
      lock_guard<mutex> l(lock_);
      MonotonicStopWatch recovery_timer;
      recovery_timer.Start();
      connected_to_statestore_metric_->set_value(false);
      LOG(INFO) << subscriber_id_
                << ": Connection with statestore lost, entering recovery mode";
      uint32_t attempt_count = 1;
      while (true) {
        LOG(INFO) << "Trying to re-register with statestore, attempt: "
                  << attempt_count++;
        Status status = Register();
        if (status.ok()) {
          // Make sure to update failure detector so that we don't immediately fail on the
          // next loop while we're waiting for heartbeat messages to resume.
          failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
          LOG(INFO) << "Reconnected to statestore. Exiting recovery mode";

          // Break out of enclosing while (true) to top of outer-scope loop.
          break;
        } else {
          // Don't exit recovery mode, continue
          LOG(WARNING) << "Failed to re-register with statestore: "
                       << status.GetDetail();
          SleepForMs(SLEEP_INTERVAL_MS);
        }
        last_recovery_duration_metric_->set_value(
            recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      }
      // When we're successful in re-registering, we don't do anything
      // to re-send our updates to the statestore. It is the
      // responsibility of individual clients to post missing updates
      // back to the statestore. This saves a lot of complexity where
      // we would otherwise have to cache updates here.
      last_recovery_duration_metric_->set_value(
          recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      last_recovery_time_metric_->set_value(TimestampValue::LocalTime().ToString());
    }

    SleepForMs(SLEEP_INTERVAL_MS);
  }
}

Status StatestoreSubscriber::CheckRegistrationId(const TUniqueId& registration_id) {
  {
    lock_guard<mutex> r(registration_id_lock_);
    // If this subscriber has just started, the registration_id_ may not have been set
    // despite the statestore starting to send updates. The 'unset' TUniqueId is 0:0, so
    // we can differentiate between a) an early message from an eager statestore, and b)
    // a message that's targeted to a previous registration.
    if (registration_id_ != TUniqueId() && registration_id != registration_id_) {
      return Status(Substitute("Unexpected registration ID: $0, was expecting $1",
          PrintId(registration_id), PrintId(registration_id_)));
    }
  }

  return Status::OK();
}

void StatestoreSubscriber::Heartbeat(const TUniqueId& registration_id) {
  const Status& status = CheckRegistrationId(registration_id);
  if (status.ok()) {
    heartbeat_interval_metric_->Update(
        heartbeat_interval_timer_.Reset() / (1000.0 * 1000.0 * 1000.0));
    failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
  } else {
    VLOG_RPC << "Heartbeat: " << status.GetDetail();
  }
}

Status StatestoreSubscriber::UpdateState(const TopicDeltaMap& incoming_topic_deltas,
    const TUniqueId& registration_id, vector<TTopicDelta>* subscriber_topic_updates,
    bool* skipped) {
  // We don't want to block here because this is an RPC, and delaying the return causes
  // the statestore to delay sending further messages. The only time that lock_ might be
  // taken concurrently is if:
  //
  // a) another update is still being processed (i.e. is still in UpdateState()). This
  // could happen only when the subscriber has re-registered, and the statestore is still
  // sending an update for the previous registration. In this case, return OK but set
  // *skipped = true to tell the statestore to retry this update in the future.
  //
  // b) the subscriber is recovering, and has the lock held during
  // RecoveryModeChecker(). Similarly, we set *skipped = true.
  // TODO: Consider returning an error in this case so that the statestore will eventually
  // stop sending updates even if re-registration fails.
  try_mutex::scoped_try_lock l(lock_);
  if (l) {
    *skipped = false;
    RETURN_IF_ERROR(CheckRegistrationId(registration_id));

    // Only record updates received when not in recovery mode
    topic_update_interval_metric_->Update(
        topic_update_interval_timer_.Reset() / (1000.0 * 1000.0 * 1000.0));
    MonotonicStopWatch sw;
    sw.Start();

    // Check the version ranges of all delta updates to ensure they can be applied
    // to this subscriber. If any invalid ranges are found, request new update(s) with
    // version ranges applicable to this subscriber.
    bool found_unexpected_delta = false;
    for (const TopicDeltaMap::value_type& delta: incoming_topic_deltas) {
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
      for (const UpdateCallbacks::value_type& callbacks: update_callbacks_) {
        MonotonicStopWatch sw;
        sw.Start();
        for (const UpdateCallback& callback: callbacks.second.callbacks) {
          // TODO: Consider filtering the topics to only send registered topics to
          // callbacks
          callback(incoming_topic_deltas, subscriber_topic_updates);
        }
        callbacks.second.processing_time_metric->Update(
            sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      }
    }
    sw.Stop();
    topic_update_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  } else {
    *skipped = true;
  }
  return Status::OK();
}


}
