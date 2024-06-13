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

#include <mutex>
#include <sstream>
#include <utility>

#include <boost/algorithm/string/join.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread/lock_options.hpp>
#include <boost/thread/pthread/shared_mutex.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "common/status.h"
#include "statestore/failure-detector.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-util.h"
#include "statestore/statestore-service-client-wrapper.h"
#include "util/container-util.h"
#include "util/collection-metrics.h"
#include "util/debug-util.h"
#include "util/metrics.h"
#include "util/openssl-util.h"
#include "util/collection-metrics.h"
#include "util/time.h"

#include "common/names.h"

using boost::posix_time::seconds;
using boost::shared_lock;
using boost::shared_mutex;
using std::string;

using namespace apache::thrift;
using namespace apache::thrift::transport;
using namespace strings;

DEFINE_int32(statestore_subscriber_timeout_seconds, 30, "The amount of time (in seconds)"
     " that may elapse before the connection with the statestore is considered lost.");
DEFINE_int32(statestore_subscriber_cnxn_attempts, 10, "The number of times to retry an "
    "RPC connection to the statestore. A setting of 0 means retry indefinitely");
DEFINE_int32(statestore_subscriber_cnxn_retry_interval_ms, 3000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the statestore.");
DEFINE_bool(statestore_subscriber_use_resolved_address, false, "If set to true, the "
    "subscriber will register with statestore using its resolved IP address. Note that "
    "using resolved IP address may cause mismatch with the TLS certificate.");
DEFINE_int64_hidden(statestore_subscriber_recovery_grace_period_ms, 30000L, "Period "
    "after the last successful subscription attempt until the subscriber will be "
    "considered fully recovered. After a successful reconnect attempt, updates to the "
    "cluster membership will only become effective after this period has elapsed.");
DEFINE_int32(statestore_client_rpc_timeout_ms, 300000, "(Advanced) The underlying "
    "TSocket send/recv timeout in milliseconds for a catalog client RPC.");

DECLARE_bool(enable_statestored_ha);
DECLARE_bool(tolerate_statestore_startup_delay);
DECLARE_string(debug_actions);
DECLARE_string(ssl_client_ca_certificate);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);

namespace impala {

// Used to identify the statestore in the failure detector
const string STATESTORE_ID = "STATESTORE";

// Template for metrics that measure the processing time for individual topics.
const string CALLBACK_METRIC_PATTERN = "statestore-subscriber.topic-$0.processing-time-s";

// Template for metrics that measure the interval between updates for individual topics.
const string UPDATE_INTERVAL_METRIC_PATTERN = "statestore-subscriber.topic-$0.update-interval";

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
    Status status = CheckProtocolVersion(params.protocol_version);
    if (status.ok()) {
      TUniqueId registration_id, statestore_id;
      if (params.__isset.registration_id) {
        registration_id = params.registration_id;
      }
      if (params.__isset.statestore_id) {
        statestore_id = params.statestore_id;
      }

      subscriber_
          ->UpdateState(params.topic_deltas, registration_id, statestore_id,
              &response.topic_updates, &response.skipped)
          .ToThrift(&response.status);
      // Make sure Thrift thinks the field is set.
      response.__set_skipped(response.skipped);
    } else {
      LOG(WARNING) << Substitute("Receive UpdateState RPC request from incompatible "
          "statestored (protocol version $0), the request is ignored.",
          params.protocol_version);
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    response.__set_status(thrift_status);
  }

  virtual void Heartbeat(THeartbeatResponse& response, const THeartbeatRequest& request) {
    Status status = CheckProtocolVersion(request.protocol_version);
    if (status.ok()) {
      TUniqueId registration_id, statestore_id;
      bool request_statestore_conn_state = false;
      TStatestoreConnState::type active_statestore_conn_state =
          TStatestoreConnState::UNKNOWN;
      if (request.__isset.registration_id) {
        registration_id = request.registration_id;
      }
      if (request.__isset.statestore_id) {
        statestore_id = request.statestore_id;
      }
      if (request.__isset.request_statestore_conn_state) {
        request_statestore_conn_state = request.request_statestore_conn_state;
      }
      subscriber_->Heartbeat(registration_id, statestore_id,
          request_statestore_conn_state, &active_statestore_conn_state);
      if (request_statestore_conn_state) {
        // Send connection state with active statestore instance to standby statestore
        // instance.
        response.__set_statestore_conn_state(active_statestore_conn_state);
      }
    } else {
      LOG(WARNING) << Substitute("Receive Heartbeat RPC request from incompatible "
          "statestored (protocol version $0), the request is ignored.",
          request.protocol_version);
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    response.__set_status(thrift_status);
  }

  virtual void UpdateCatalogd(TUpdateCatalogdResponse& response,
      const TUpdateCatalogdRequest& request) {
    bool update_skipped = false;
    Status status = CheckProtocolVersion(request.protocol_version);
    if (status.ok()) {
      subscriber_->UpdateCatalogd(request.catalogd_registration,
          request.registration_id, request.statestore_id, request.catalogd_version,
          &update_skipped);
      response.__set_skipped(update_skipped);
    } else {
      LOG(WARNING) << Substitute("Receive UpdateCatalogd RPC request from incompatible "
          "statestored (protocol version $0), the request is ignored.",
          request.protocol_version);
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    response.__set_status(thrift_status);
  }

  virtual void UpdateStatestoredRole(TUpdateStatestoredRoleResponse& response,
      const TUpdateStatestoredRoleRequest& request) {
    bool update_skipped = false;
    Status status = CheckProtocolVersion(request.protocol_version);
    if (status.ok()) {
      bool update_active_catalogd = false;
      int64_t active_catalogd_version = 0;
      TCatalogRegistration catalogd_registration;
      if (request.__isset.catalogd_version) {
        update_active_catalogd = true;
        active_catalogd_version = request.catalogd_version;
        catalogd_registration = request.catalogd_registration;
      }
      subscriber_->UpdateStatestoredRole(request.is_active, request.registration_id,
          request.statestore_id, request.active_statestored_version,
          update_active_catalogd, &catalogd_registration, active_catalogd_version,
          &update_skipped);
      response.__set_skipped(update_skipped);
    } else {
      LOG(WARNING) << Substitute("Receive UpdateStatestoredRole RPC request from "
          "incompatible statestored (protocol version $0), the request is ignored.",
          request.protocol_version);
    }
    TStatus thrift_status;
    status.ToThrift(&thrift_status);
    response.__set_status(thrift_status);
  }

 private:
  StatestoreSubscriber* subscriber_;

  Status CheckProtocolVersion(StatestoreServiceVersion::type statestore_version) {
    Status status = Status::OK();
    if (statestore_version < subscriber_->GetProtocolVersion()) {
      status = Status(TErrorCode::STATESTORE_INCOMPATIBLE_PROTOCOL, subscriber_->id(),
          subscriber_->GetProtocolVersion() + 1, statestore_version + 1);
    }
    return status;
  }
};

StatestoreSubscriber::StatestoreSubscriber(const string& subscriber_id,
    const TNetworkAddress& heartbeat_address, const TNetworkAddress& statestore_address,
    const TNetworkAddress& statestore2_address, MetricGroup* metrics,
    TStatestoreSubscriberType::type subscriber_type)
  : subscriber_id_(subscriber_id),
    protocol_version_(StatestoreServiceVersion::V2),
    catalog_protocol_version_(CatalogServiceVersion::V2),
    heartbeat_address_(heartbeat_address),
    subscriber_type_(subscriber_type),
    thrift_iface_(new StatestoreSubscriberThriftIf(this)),
    client_cache_(new StatestoreClientCache(1, 0, FLAGS_statestore_client_rpc_timeout_ms,
        FLAGS_statestore_client_rpc_timeout_ms, "",
        !FLAGS_ssl_client_ca_certificate.empty())),
    metrics_(metrics->GetOrCreateChildGroup("statestore-subscriber")),
    enable_statestored_ha_(FLAGS_enable_statestored_ha) {
  statestore_ =
      new StatestoreStub(this, /* first_statestore */true, statestore_address, metrics);
  if (!enable_statestored_ha_) {
    statestore_->SetStatestoreActive(true, 1);
    active_statestore_ = statestore_;
  } else {
    DCHECK(!statestore2_address.hostname.empty() && statestore2_address.port != 0);
    statestore2_ = new StatestoreStub(
        this, /* first_statestore */false, statestore2_address, metrics);
  }
  client_cache_->InitMetrics(metrics, "statestore-subscriber.statestore");
}

Status StatestoreSubscriber::AddTopic(const Statestore::TopicId& topic_id,
    bool is_transient, bool populate_min_subscriber_topic_version,
    string filter_prefix, const UpdateCallback& callback) {
  RETURN_IF_ERROR(statestore_->AddTopic(topic_id, is_transient,
      populate_min_subscriber_topic_version, filter_prefix, callback));
  if (statestore2_ != nullptr) {
    RETURN_IF_ERROR(statestore2_->AddTopic(topic_id, is_transient,
        populate_min_subscriber_topic_version, filter_prefix, callback));
  }
  return Status::OK();
}

void StatestoreSubscriber::AddUpdateCatalogdTopic(
    const UpdateCatalogdCallback& callback) {
  statestore_->AddUpdateCatalogdTopic(callback);
  if (statestore2_ != nullptr) {
    statestore2_->AddUpdateCatalogdTopic(callback);
  }
}

void StatestoreSubscriber::AddCompleteRegistrationTopic(
    const CompleteRegistrationCallback& callback) {
  statestore_->AddCompleteRegistrationTopic(callback);
  if (statestore2_ != nullptr) {
    statestore2_->AddCompleteRegistrationTopic(callback);
  }
}

bool StatestoreSubscriber::IsInPostRecoveryGracePeriod() {
  StatestoreStub* active_statestore = GetActiveStatestore();
  return (active_statestore != nullptr) ?
      active_statestore->IsInPostRecoveryGracePeriod() : false;
}

bool StatestoreSubscriber::IsRegistered() {
  StatestoreStub* active_statestore = GetActiveStatestore();
  return (active_statestore != nullptr) ? active_statestore->IsRegistered() : false;
}

Status StatestoreSubscriber::Start() {
  // Backend must be started before registration
  std::shared_ptr<TProcessor> processor(
      new StatestoreSubscriberProcessor(thrift_iface_));
  // Logging statestore subscriber heartbeats at VLOG(3) to avoid overwhelming lower log
  // levels.
  std::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("statestore-subscriber", metrics_, 3 /*vlog_level*/));
  processor->setEventHandler(event_handler);

  ThriftServerBuilder builder(
      "StatestoreSubscriber", processor, heartbeat_address_.port);
  // Mark this as an internal service to use a more permissive Thrift max message size
  builder.is_external_facing(false);
  if (IsInternalTlsConfigured()) {
    SSLProtocol ssl_version;
    RETURN_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
    LOG(INFO) << "Enabling SSL for Statestore subscriber";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
        .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
        .ssl_version(ssl_version)
        .cipher_list(FLAGS_ssl_cipher_list);
  }

  ThriftServer* server;
  RETURN_IF_ERROR(builder.Build(&server));
  heartbeat_server_.reset(server);
  RETURN_IF_ERROR(heartbeat_server_->Start());
  RETURN_IF_ERROR(WaitForLocalServer(
      *heartbeat_server_, /* num_retries */ 10, /* retry_interval_ms */ 1000));

  // Specify the port which the heartbeat server is listening on.
  heartbeat_address_.port = heartbeat_server_->port();

  if (!enable_statestored_ha_) {
    return statestore_->Start();
  } else {
    DCHECK(statestore2_ != nullptr);
    bool statestore_is_active = false;
    bool statestore2_is_active = false;
    RETURN_IF_ERROR(statestore_->Start(&statestore_is_active));
    RETURN_IF_ERROR(statestore2_->Start(&statestore2_is_active));
    lock_guard<mutex> r(statestore_ha_lock_);
    DCHECK(!statestore_is_active || !statestore2_is_active);
    if (statestore_is_active) {
      active_statestore_ = statestore_;
      standby_statestore_ = statestore2_;
      LOG(INFO) << "Set active statestored as " << statestore_->GetAddress();
    } else if (statestore2_is_active) {
      active_statestore_ = statestore2_;
      standby_statestore_ = statestore_;
      LOG(INFO) << "Set active statestored as " << statestore2_->GetAddress();
    } else {
      // Both statestoreds are not ready. If FLAGS_tolerate_statestore_startup_delay is
      // as true, subscriber enter recovery mode.
      active_statestore_ = nullptr;
      standby_statestore_ = nullptr;
    }
  }
  return Status::OK();
}

/// Set Register Request
Status StatestoreSubscriber::SetRegisterRequest(
    TRegisterSubscriberRequest* request) {
  // Resolve the heartbeat address if necessary. Don't re-register with statestore
  // if the heartbeat address cannot be resolved.
  TNetworkAddress heartbeat_address = heartbeat_address_;
  if (FLAGS_statestore_subscriber_use_resolved_address) {
    IpAddr ip_address;
    RETURN_IF_ERROR(HostnameToIpAddr(heartbeat_address.hostname, &ip_address));
    heartbeat_address.hostname = ip_address;
    LOG(INFO) << Substitute(
        "Registering with statestore with resolved address $0", ip_address);
  }
  request->__set_subscriber_location(heartbeat_address);
  request->__set_subscriber_id(subscriber_id_);
  request->__set_subscriber_type(subscriber_type_);
  return Status::OK();
}

void StatestoreSubscriber::Heartbeat(const RegistrationId& registration_id,
    const TUniqueId& statestore_id, bool request_active_conn_state,
    TStatestoreConnState::type* active_statestore_conn_state) {
  // It's possible the heartbeat is received for previous registration.
  DCHECK(statestore_ != nullptr);
  if (statestore_->IsMatchingStatestoreId(statestore_id) || statestore2_ == nullptr) {
    // Due to race, subscriber could receive heartbeat before receiving registration
    // response. "statestore2_ == nullptr" means HA is not enabled. In that case,
    // we still handle the heartbeat request from statestore even registration response
    // is not received.
    statestore_->Heartbeat(registration_id);
    // Report connection state with active statestore instance for the request from
    // standby statestore. It's possible that the notification of active statestored
    // change has not been received.
    if (request_active_conn_state && statestore2_ != nullptr) {
      *active_statestore_conn_state = statestore2_->GetStatestoreConnState();
    }
  } else if (statestore2_ != nullptr
      && statestore2_->IsMatchingStatestoreId(statestore_id)) {
    statestore2_->Heartbeat(registration_id);
    // Report connection state with active statestore instance for the request from
    // standby statestore. It's possible that the notification of active statestored
    // change has not been received.
    if (request_active_conn_state) {
      *active_statestore_conn_state = statestore_->GetStatestoreConnState();
    }
  } else {
    VLOG(3) << "Ignore heartbeat message from unknown statestored: "
            << PrintId(statestore_id);
  }
}

void StatestoreSubscriber::UpdateCatalogd(
    const TCatalogRegistration& catalogd_registration,
    const RegistrationId& registration_id, const TUniqueId& statestore_id,
    int64_t active_catalogd_version, bool* update_skipped) {
  // Accept UpdateCatalogd RPC from active statestore
  StatestoreStub* active_statestore = GetActiveStatestore();
  if (active_statestore != nullptr
      && active_statestore->IsMatchingStatestoreId(statestore_id)) {
    active_statestore->UpdateCatalogd(catalogd_registration, registration_id,
        active_catalogd_version, /* statestore_failover */false, update_skipped);
  } else {
    // It's possible the catalogd update RPC is received before the registration response
    // is received. Skip this update so that the statestore will retry this update in
    // the future.
    *update_skipped = true;
    LOG(INFO) << "Skipped updating catalogd message from unknown or inactive "
              << "statestored: " << PrintId(statestore_id);
  }
}

void StatestoreSubscriber::UpdateStatestoredRole(bool is_active,
    const RegistrationId& registration_id, const TUniqueId& statestore_id,
    int64_t active_statestored_version, bool update_active_catalogd,
    const TCatalogRegistration* catalogd_registration, int64_t active_catalogd_version,
    bool* update_skipped) {
  DCHECK(enable_statestored_ha_);
  // Accept UpdateStatestoredRole RPC from standby statestored
  StatestoreStub* active_statestore = GetActiveStatestore();
  StatestoreStub* standby_statestore = GetStandbyStatestore();
  if (standby_statestore != nullptr
      && standby_statestore->IsMatchingStatestoreId(statestore_id)) {
    LOG(INFO) << "Receive UpdateStatestoredRole message from standby statestored";
    // Receive notification of statestore service fail over, switch active and standby
    // statestoreds.
    standby_statestore->IncCountForUpdateStatestoredRoleRPC();
    DCHECK(is_active);
    {
      lock_guard<mutex> r(statestore_ha_lock_);
      StatestoreStub* tmp = active_statestore_;
      active_statestore_ = standby_statestore_;
      standby_statestore_ = tmp;
      active_statestore_->SetStatestoreActive(
          is_active, active_statestored_version, /* has_failover */ true);
      standby_statestore_->SetStatestoreActive(
          !is_active, active_statestored_version, /* has_failover */ true);
      LOG(INFO) << "Updated active statestored as " << active_statestore_->GetAddress();
    }

    if (update_active_catalogd) {
      active_statestore = GetActiveStatestore();
      active_statestore->UpdateCatalogd(*catalogd_registration, registration_id,
          active_catalogd_version, /* statestore_failover */true, update_skipped);
      DCHECK(!(*update_skipped));
    }
  } else if (active_statestore == nullptr) {
    {
      lock_guard<mutex> r(statestore_ha_lock_);
      if (active_statestore_ == nullptr) {
        LOG(INFO) << "Subscriber was started before both statestore instances were "
                     "ready to accept registration requests.";
        DCHECK(standby_statestore_ == nullptr);
        // Active/standby statestored are not set. This could happen if statestoreds were
        // started after subscribers' registration attemption.
        if (statestore_->IsMatchingStatestoreId(statestore_id)) {
          active_statestore_ = statestore_;
          standby_statestore_ = statestore2_;
        } else {
          DCHECK(statestore2_->IsMatchingStatestoreId(statestore_id));
          active_statestore_ = statestore2_;
          standby_statestore_ = statestore_;
        }
        active_statestore_->SetStatestoreActive(is_active, active_statestored_version);
        standby_statestore_->SetStatestoreActive(!is_active, active_statestored_version);
        LOG(INFO) << "Updated active statestored as " << active_statestore_->GetAddress();
      } else {
        LOG(INFO) << "Active statestored " << active_statestore_->GetAddress()
                  << " has been updated.";
      }
    }

    if (update_active_catalogd) {
      active_statestore = GetActiveStatestore();
      DCHECK(active_statestore != nullptr);
      active_statestore->UpdateCatalogd(*catalogd_registration, registration_id,
          active_catalogd_version, /* statestore_failover */true, update_skipped);
      DCHECK(!(*update_skipped));
    }
  } else if (active_statestore->IsMatchingStatestoreId(statestore_id)) {
    LOG(INFO) << "statestored " << active_statestore->GetAddress()
              << " is in active state.";
  } else {
    // It's possible the statestored update RPC is received before the registration
    // response is received. Skip this update so that the statestore will retry this
    // update in the future.
    *update_skipped = true;
    LOG(INFO) << "Skipped updating statestored message from unknown statestored: "
              << PrintId(statestore_id);
  }
}

StatestoreSubscriber::StatestoreStub* StatestoreSubscriber::GetActiveStatestore() {
  lock_guard<mutex> r(statestore_ha_lock_);
  return active_statestore_;
}

StatestoreSubscriber::StatestoreStub* StatestoreSubscriber::GetStandbyStatestore() {
  lock_guard<mutex> r(statestore_ha_lock_);
  return standby_statestore_;
}

Status StatestoreSubscriber::UpdateState(const TopicDeltaMap& incoming_topic_deltas,
    const RegistrationId& registration_id, const TUniqueId& statestore_id,
    vector<TTopicDelta>* subscriber_topic_updates, bool* skipped) {
  // Accept UpdateState RPC from active statestore.
  StatestoreStub* active_statestore = GetActiveStatestore();
  if (active_statestore != nullptr
      && active_statestore->IsMatchingStatestoreId(statestore_id)) {
    return active_statestore->UpdateState(
        incoming_topic_deltas, registration_id, subscriber_topic_updates, skipped);
  } else {
    // It's possible the topic update is received before the registration response is
    // received. Skip this update so that the statestore will retry this update in the
    // future.
    *skipped = true;
    VLOG(3) << "Skipped topic update message from unknown or inactive statestored: "
            << PrintId(statestore_id);
    return Status::OK();
  }
}

StatestoreSubscriber::StatestoreStub::StatestoreStub(StatestoreSubscriber* subscriber,
    bool first_statestore, const TNetworkAddress& statestore_address,
    MetricGroup* metrics)
  : subscriber_(subscriber),
    statestore_address_(statestore_address),
    failure_detector_(
        new TimeoutFailureDetector(seconds(FLAGS_statestore_subscriber_timeout_seconds),
            seconds(FLAGS_statestore_subscriber_timeout_seconds / 2))),
    is_registered_(false) {
  std::string name_prefix = first_statestore ? "statestore" : "statestore2";
  metrics_ = metrics->GetOrCreateChildGroup(name_prefix + "-subscriber");
  connected_to_statestore_metric_ = metrics_->AddProperty(
      "statestore-subscriber.connected", false);
  connection_failure_metric_ = metrics_->AddCounter(
      "statestore-subscriber.num-connection-failures", 0);
  last_recovery_duration_metric_ = metrics_->AddDoubleGauge(
      "statestore-subscriber.last-recovery-duration", 0.0);
  last_recovery_time_metric_ = metrics_->AddProperty<string>(
      "statestore-subscriber.last-recovery-time", "N/A");
  topic_update_interval_metric_ = StatsMetric<double>::CreateAndRegister(
      metrics_, "statestore-subscriber.topic-update-interval-time");
  topic_update_duration_metric_ = StatsMetric<double>::CreateAndRegister(
      metrics_, "statestore-subscriber.topic-update-duration");
  heartbeat_interval_metric_ = StatsMetric<double>::CreateAndRegister(
      metrics_, "statestore-subscriber.heartbeat-interval-time");
  registration_id_metric_ = metrics_->AddProperty<string>(
      "statestore-subscriber.registration-id", "N/A");
  statestore_id_metric_ = metrics_->AddProperty<string>(
      "statestore-subscriber.statestore-id", "N/A");
  update_catalogd_rpc_metric_ = metrics_->AddCounter(
      "statestore-subscriber.num-update-catalogd-rpc", 0);
  update_statestored_role_rpc_metric_ = metrics_->AddCounter(
      "statestore-subscriber.num-update-statestored-role-rpc", 0);
  active_status_metric_ = metrics_->AddProperty(
      name_prefix + "-subscriber.statestore-active-status", is_active_);
  re_registr_attempt_metric_ = metrics_->AddCounter(
      "statestore-subscriber.num-re-register-attempt", 0);
}

Status StatestoreSubscriber::StatestoreStub::AddTopic(
    const Statestore::TopicId& topic_id, bool is_transient,
    bool populate_min_subscriber_topic_version, string filter_prefix,
    const UpdateCallback& callback) {
  lock_guard<shared_mutex> exclusive_lock(lock_);
  if (is_registered_) return Status("Subscriber already started, can't add new topic");
  TopicRegistration& registration = topic_registrations_[topic_id];
  registration.callbacks.push_back(callback);
  if (registration.processing_time_metric == nullptr) {
    registration.processing_time_metric = StatsMetric<double>::CreateAndRegister(
        metrics_, CALLBACK_METRIC_PATTERN, topic_id);
    registration.update_interval_metric = StatsMetric<double>::CreateAndRegister(
        metrics_, UPDATE_INTERVAL_METRIC_PATTERN, topic_id);
    registration.update_interval_timer.Start();
  }
  registration.is_transient = is_transient;
  registration.populate_min_subscriber_topic_version =
      populate_min_subscriber_topic_version;
  registration.filter_prefix = std::move(filter_prefix);
  return Status::OK();
}

void StatestoreSubscriber::StatestoreStub::AddUpdateCatalogdTopic(
    const UpdateCatalogdCallback& callback) {
  update_catalogd_callbacks_.push_back(callback);
}

void StatestoreSubscriber::StatestoreStub::AddCompleteRegistrationTopic(
    const CompleteRegistrationCallback& callback) {
  complete_registration_callbacks_.push_back(callback);
}

Status StatestoreSubscriber::StatestoreStub::Register(bool* has_active_catalogd,
    int64_t* active_catalogd_version,
    TCatalogRegistration* active_catalogd_registration) {
  // Check protocol version of the statestore first.
  TGetProtocolVersionRequest get_protocol_request;
  TGetProtocolVersionResponse get_protocol_response;
  get_protocol_request.__set_protocol_version(subscriber_->GetProtocolVersion());
  int attempt = 0; // Used for debug action only.
  StatestoreServiceConn::RpcStatus rpc_status =
      StatestoreServiceConn::DoRpcWithRetry(subscriber_->client_cache_.get(),
          statestore_address_,
          &StatestoreServiceClientWrapper::GetProtocolVersion,
          get_protocol_request,
          FLAGS_statestore_subscriber_cnxn_attempts,
          FLAGS_statestore_subscriber_cnxn_retry_interval_ms,
          [&attempt]() {
            return attempt++ == 0 ?
                DebugAction(FLAGS_debug_actions, "GET_PROTOCOL_VERSION_FIRST_ATTEMPT") :
                Status::OK();
          },
          &get_protocol_response);
  RETURN_IF_ERROR(rpc_status.status);
  Status status = Status(get_protocol_response.status);
  if (status.ok()) {
    connected_to_statestore_metric_->SetValue(true);
    if (get_protocol_response.protocol_version < subscriber_->GetProtocolVersion()) {
      // Return error for incompatible statestore.
      return Status(TErrorCode::STATESTORE_INCOMPATIBLE_PROTOCOL, subscriber_->id(),
          subscriber_->GetProtocolVersion() + 1,
          get_protocol_response.protocol_version + 1);
    }
  } else {
    return status;
  }

  // Register subscriber
  TRegisterSubscriberRequest request;
  for (const auto& registration : topic_registrations_) {
    TTopicRegistration thrift_topic;
    thrift_topic.topic_name = registration.first;
    thrift_topic.is_transient = registration.second.is_transient;
    thrift_topic.populate_min_subscriber_topic_version =
        registration.second.populate_min_subscriber_topic_version;
    thrift_topic.__set_filter_prefix(registration.second.filter_prefix);
    request.topic_registrations.push_back(thrift_topic);
  }
  request.__set_subscribe_catalogd_change(IsSubscribedCatalogdChange());

  {
    // Reset registration_id_ and statestore_id_ before registering with statestore
    // so that RPC messages for previous registration are not accepted.
    lock_guard<mutex> l(id_lock_);
    registration_id_ = TUniqueId();
    statestore_id_ = TUniqueId();
  }
  RETURN_IF_ERROR(subscriber_->SetRegisterRequest(&request));
  TRegisterSubscriberResponse response;
  attempt = 0; // Used for debug action only.
  rpc_status =
      StatestoreServiceConn::DoRpcWithRetry(subscriber_->client_cache_.get(),
          statestore_address_,
          &StatestoreServiceClientWrapper::RegisterSubscriber,
          request,
          FLAGS_statestore_subscriber_cnxn_attempts,
          FLAGS_statestore_subscriber_cnxn_retry_interval_ms,
          [&attempt]() {
            return attempt++ == 0 ?
                DebugAction(FLAGS_debug_actions, "REGISTER_SUBSCRIBER_FIRST_ATTEMPT") :
                Status::OK();
          },
          &response);
  RETURN_IF_ERROR(rpc_status.status);
  status = Status(response.status);
  if (status.ok()) {
    connected_to_statestore_metric_->SetValue(true);
    last_registration_ms_.Store(MonotonicMillis());
  }
  {
    lock_guard<mutex> l(id_lock_);
    if (response.__isset.protocol_version) {
      protocol_version_ = response.protocol_version;
      VLOG(1) << "Statestore protocol version: " << protocol_version_;
    } else {
      VLOG(1) << "No service_version received from statestore";
      protocol_version_ = StatestoreServiceVersion::V1;
    }
    if (response.__isset.registration_id) {
      registration_id_ = response.registration_id;
      const string& registration_string = PrintId(registration_id_);
      registration_id_metric_->SetValue(registration_string);
      VLOG(1) << "Subscriber registration ID: " << registration_string;
    } else {
      VLOG(1) << "No subscriber registration ID received from statestore";
    }
    if (response.__isset.statestore_id) {
      statestore_id_ = response.statestore_id;
      const string& statestore_id_string = PrintId(statestore_id_);
      statestore_id_metric_->SetValue(statestore_id_string);
      VLOG(1) << "Statestore ID: " << statestore_id_string;
    } else {
      VLOG(1) << "No statestore ID received from statestore";
    }
  }
  {
    lock_guard<mutex> l(active_lock_);
    if (status.ok() && response.__isset.statestore_is_active) {
      is_active_ = response.statestore_is_active;
      if (is_active_) {
        DCHECK(response.active_statestored_version > 0
            && active_statestored_version_ <= response.active_statestored_version);
      }
      active_statestored_version_ = response.active_statestored_version;
      active_status_metric_->SetValue(is_active_);
    }
  }
  if (status.ok() && response.__isset.catalogd_registration) {
    VLOG(1) << "Active catalogd address: "
            << TNetworkAddressToString(response.catalogd_registration.address);
    if (has_active_catalogd != nullptr) *has_active_catalogd = true;
    if (active_catalogd_version != nullptr && response.__isset.catalogd_version) {
      *active_catalogd_version = response.catalogd_version;
    }
    if (active_catalogd_registration != nullptr) {
      *active_catalogd_registration = response.catalogd_registration;
    }
  }
  heartbeat_interval_timer_.Start();
  return status;
}

Status StatestoreSubscriber::StatestoreStub::Start(bool* startstore_is_active) {
  Status status;
  {
    bool has_active_catalogd = false;
    int64_t active_catalogd_version = 0;
    TCatalogRegistration active_catalogd_registration;
    // Take the lock to ensure that, if a topic-update is received during registration
    // (perhaps because Register() has succeeded, but we haven't finished setting up
    // state on the client side), UpdateState() will reject the message.
    lock_guard<shared_mutex> exclusive_lock(lock_);
    LOG(INFO) << "Starting statestore subscriber";
    // Inject failure before registering to statestore.
    status = DebugAction(FLAGS_debug_actions, "REGISTER_STATESTORE_ON_STARTUP");
    if (status.ok()) {
      status = Register(
          &has_active_catalogd, &active_catalogd_version, &active_catalogd_registration);
    }
    if (status.ok()) {
      is_registered_ = true;
      if (startstore_is_active != nullptr) *startstore_is_active = is_active_;
      LOG(INFO) << "statestore registration successful on startup";
      if (has_active_catalogd) {
        DCHECK(active_catalogd_version >= 0);
        for (const UpdateCatalogdCallback& callback : update_catalogd_callbacks_) {
          callback(true, active_catalogd_version, active_catalogd_registration);
        }
      }
    } else {
      LOG(INFO) << "statestore registration unsuccessful on startup: "
                << status.GetDetail();
      if (FLAGS_tolerate_statestore_startup_delay && !TestInfo::is_be_test()) {
        LOG(INFO) << "Tolerate the delay of the statestore's availability on startup";
        status = Status::OK();
      }
    }
  }

  if (status.ok()) {
    // Registration is finished at this point, so it's fine to release the lock.
    status = Thread::Create("statestore-subscriber", "recovery-mode-thread",
        &StatestoreSubscriber::StatestoreStub::RecoveryModeChecker, this,
        &recovery_mode_thread_);
  }
  return status;
}

void StatestoreSubscriber::StatestoreStub::RecoveryModeChecker() {
  // Define a local variable is_registered since we need to hold lock when accessing
  // class member variable is_registered_.
  bool is_registered;
  {
    lock_guard<shared_mutex> exclusive_lock(lock_);
    is_registered = is_registered_;
  }
  if (is_registered) {
    failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
  }

  // Every few seconds, wake up and check if the failure detector has determined
  // that the statestore has failed from our perspective. If so, enter recovery
  // mode and try to reconnect, followed by reregistering all subscriptions.
  while (true) {
    bool updateStatestoredRole = false;
    FailureDetector::PeerState state = failure_detector_->GetPeerState(STATESTORE_ID);
    if (state == FailureDetector::FAILED
        || (state == FailureDetector::UNKNOWN && !is_registered)) {
      // When entering recovery mode, the class-wide lock_ is taken to
      // ensure mutual exclusion with any operations in flight.
      lock_guard<shared_mutex> exclusive_lock(lock_);
      MonotonicStopWatch recovery_timer;
      recovery_timer.Start();
      connected_to_statestore_metric_->SetValue(false);
      connection_failure_metric_->Increment(1);
      LOG(INFO) << subscriber_->subscriber_id_
                << ": Connection with statestore lost, entering recovery mode";
      uint32_t attempt_count = 1;
      bool has_active_catalogd = false;
      int64_t active_catalogd_version = 0;
      TCatalogRegistration active_catalogd_registration;
      while (true) {
        LOG(INFO) << "Trying to re-register with statestore, attempt: "
                  << attempt_count++;
        re_registr_attempt_metric_->Increment(1);
        Status status = Register(&has_active_catalogd, &active_catalogd_version,
            &active_catalogd_registration);
        if (status.ok()) {
          if (!is_registered_) {
            is_registered_ = true;
            is_registered = true;
            for (const CompleteRegistrationCallback& callback
                : complete_registration_callbacks_) {
              callback();
            }
          }
          if (is_active_) {
            updateStatestoredRole = true;
            LOG(INFO) << "Statestored " << TNetworkAddressToString(statestore_address_)
                      << " is active.";
          }
          // Make sure to update failure detector so that we don't immediately fail on
          // the next loop while we're waiting for heartbeat messages to resume.
          failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
          LOG(INFO) << "Reconnected to statestore. Exiting recovery mode";
          if (!has_active_catalogd) {
            // Need to reset version of last received active catalogd for new
            // registration. Note that active_catalogd_registration is invalid when
            // active_catalogd_version is negative.
            active_catalogd_version = -1;
          } else {
            DCHECK(active_catalogd_version >= 0);
          }
          for (const UpdateCatalogdCallback& callback : update_catalogd_callbacks_) {
            callback(true, active_catalogd_version, active_catalogd_registration);
          }
          // Break out of enclosing while (true) to top of outer-scope loop.
          break;
        } else {
          // Don't exit recovery mode, continue
          LOG(WARNING) << "Failed to re-register with statestore: "
                       << status.GetDetail();
          SleepForMs(SLEEP_INTERVAL_MS);
        }
        last_recovery_duration_metric_->SetValue(
            recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      }
      // When we're successful in re-registering, we don't do anything
      // to re-send our updates to the statestore. It is the
      // responsibility of individual clients to post missing updates
      // back to the statestore. This saves a lot of complexity where
      // we would otherwise have to cache updates here.
      last_recovery_duration_metric_->SetValue(
          recovery_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
      last_recovery_time_metric_->SetValue(CurrentTimeString());
    }

    if (FLAGS_enable_statestored_ha && updateStatestoredRole) {
      LOG(INFO) << "Update the roles of statestoreds";
      bool update_skipped = false;
      bool is_active = false;
      RegistrationId registration_id;
      TUniqueId statestore_id;
      int64_t active_statestored_version = GetActiveVersion(&is_active);
      GetRegistrationIdAndStatestoreId(&registration_id, &statestore_id);
      subscriber_->UpdateStatestoredRole(is_active, registration_id,
          statestore_id, active_statestored_version, /* update_active_catalogd */false,
          nullptr, 0, &update_skipped);
      DCHECK(!update_skipped);
    }
    SleepForMs(SLEEP_INTERVAL_MS);
  }
}

Status StatestoreSubscriber::StatestoreStub::CheckRegistrationId(
    const RegistrationId& registration_id) {
  lock_guard<mutex> r(id_lock_);
  // If this subscriber has just started, the registration_id_ may not have been set
  // despite the statestore starting to send updates. The 'unset' RegistrationId is 0:0,
  // so we can differentiate between a) an early message from an eager statestore, and
  // b) a message that's targeted to a previous registration.
  if (registration_id_ != TUniqueId() && registration_id != registration_id_) {
    return Status(Substitute("Unexpected registration ID: $0, was expecting $1",
        PrintId(registration_id), PrintId(registration_id_)));
  }
  return Status::OK();
}

bool StatestoreSubscriber::StatestoreStub::IsMatchingStatestoreId(
    const TUniqueId statestore_id) {
  lock_guard<mutex> r(id_lock_);
  // It's possible the topic update messages are received before receiving the
  // registration response. In the case, statestore_id_ and is_registered_ are not set.
  // TODO: need to revisit this when supporting statestored HA.
  return statestore_id == statestore_id_ ||
      (!is_registered_ && statestore_id_.hi == 0 && statestore_id_.lo == 0);
}

void StatestoreSubscriber::StatestoreStub::Heartbeat(
    const RegistrationId& registration_id) {
  const Status& status = CheckRegistrationId(registration_id);
  if (status.ok()) {
    heartbeat_interval_metric_->Update(
        heartbeat_interval_timer_.LapTime() / (1000.0 * 1000.0 * 1000.0));
    failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
  } else {
    VLOG_RPC << "Heartbeat: " << status.GetDetail();
  }
}

void StatestoreSubscriber::StatestoreStub::UpdateCatalogd(
    const TCatalogRegistration& catalogd_registration,
    const RegistrationId& registration_id, int64_t active_catalogd_version,
    bool statestore_failover, bool* update_skipped) {
  const Status& status = CheckRegistrationId(registration_id);
  if (status.ok()) {
    if (statestore_failover) {
      lock_guard<shared_mutex> exclusive_lock(lock_);
      DCHECK(active_catalogd_version >= 0);
      for (const UpdateCatalogdCallback& callback : update_catalogd_callbacks_) {
        // Set is_registration_reply as true like new registration if statestore service
        // fail over to standby statestored.
        callback(/* is_registration_reply */true, active_catalogd_version,
            catalogd_registration);
      }
    } else {
      // Try to acquire lock to avoid race with updating catalogd from registration
      // thread.
      shared_lock<shared_mutex> l(lock_, boost::try_to_lock);
      if (!l.owns_lock()) {
        LOG(INFO) << "Unable to acquire the lock, skip UpdateCatalogd RPC notification.";
        *update_skipped = true;
        return;
      }
      update_catalogd_rpc_metric_->Increment(1);
      DCHECK(active_catalogd_version >= 0);
      for (const UpdateCatalogdCallback& callback : update_catalogd_callbacks_) {
        callback(false, active_catalogd_version, catalogd_registration);
      }
    }
  } else {
    // It's possible the registration is not completed.
    LOG(INFO) << "Skip UpdateCatalogd RPC notification due to unknown registration_id. "
              << "It's likely the registration reply is not received.";
    *update_skipped = true;
  }
}

Status StatestoreSubscriber::StatestoreStub::UpdateState(
    const TopicDeltaMap& incoming_topic_deltas, const RegistrationId& registration_id,
    vector<TTopicDelta>* subscriber_topic_updates, bool* skipped) {
  RETURN_IF_ERROR(CheckRegistrationId(registration_id));

  // Put the updates into ascending order of topic name to match the lock acquisition
  // order of TopicRegistration::update_lock.
  vector<const TTopicDelta*> deltas_to_process;
  deltas_to_process.reserve(incoming_topic_deltas.size());
  for (auto& delta : incoming_topic_deltas) deltas_to_process.push_back(&delta.second);
  sort(deltas_to_process.begin(), deltas_to_process.end(),
      [](const TTopicDelta* left, const TTopicDelta* right) {
        return left->topic_name < right->topic_name;
      });
  // Unique locks to hold the 'update_lock' for each entry in 'deltas_to_process'. Locks
  // are held until we finish processing the update to prevent any races with concurrent
  // updates for the same topic.
  vector<unique_lock<mutex>> topic_update_locks(deltas_to_process.size());

  // We don't want to block here because this is an RPC, and delaying the return causes
  // the statestore to delay sending further messages. The only time that lock_ might be
  // taken exclusively is if the subscriber is recovering, and has the lock held during
  // RecoveryModeChecker(). In this case we skip all topics and don't update any metrics.
  //
  // UpdateState() may run concurrently with itself in two cases:
  // a) disjoint sets of topics are being updated. In that case the updates can proceed
  // concurrently.
  // b) another update for the same topics is still being processed (i.e. is still in
  // UpdateState()). This could happen only when the subscriber has re-registered, and
  // the statestore is still sending an update for the previous registration. In this
  // case, we notices that the per-topic 'update_lock' is held, skip processing all
  // of the topic updates and set *skipped = true so that the statestore will retry this
  // update in the future.
  //
  // TODO: Consider returning an error in this case so that the statestore will eventually
  // stop sending updates even if re-registration fails.
  shared_lock<shared_mutex> l(lock_, boost::try_to_lock);
  if (!l.owns_lock()) {
    *skipped = true;
    return Status::OK();
  }

  // First, acquire all the topic locks and update the interval metrics
  // Record the time we received the update before doing any processing to avoid including
  // processing time in the interval metrics.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    // Skip updates to unregistered topics.
    if (it == topic_registrations_.end()) {
      LOG(ERROR) << "Unexpected delta update for unregistered topic: "
                 << delta.topic_name;
      continue;
    }
    TopicRegistration& registration = it->second;
    unique_lock<mutex> ul(registration.update_lock, std::try_to_lock);
    if (!ul.owns_lock()) {
      // Statestore sent out concurrent topic updates. Avoid blocking the RPC by skipping
      // the topic.
      LOG(ERROR) << "Could not acquire lock for topic " << delta.topic_name << ". "
                 << "Skipping update.";
      *skipped = true;
      return Status::OK();
    }
    double interval =
        registration.update_interval_timer.ElapsedTime() / (1000.0 * 1000.0 * 1000.0);
    registration.update_interval_metric->Update(interval);
    topic_update_interval_metric_->Update(interval);

    // Hold onto lock until we've finished processing the update.
    topic_update_locks[i].swap(ul);
  }

  MonotonicStopWatch sw;
  sw.Start();
  // Second, do the actual processing of topic updates that we validated and acquired
  // locks for above.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    if (!topic_update_locks[i].owns_lock()) continue;

    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    DCHECK(it != topic_registrations_.end());
    TopicRegistration& registration = it->second;
    if (delta.is_delta && registration.current_topic_version != -1
      && delta.from_version != registration.current_topic_version) {
      // Received a delta update for the wrong version. Log an error and send back the
      // expected version to the statestore to request a new update with the correct
      // version range.
      LOG(ERROR) << "Unexpected delta update to topic '" << delta.topic_name << "' of "
                 << "version range (" << delta.from_version << ":"
                 << delta.to_version << "]. Expected delta start version: "
                 << registration.current_topic_version;

      subscriber_topic_updates->push_back(TTopicDelta());
      TTopicDelta& update = subscriber_topic_updates->back();
      update.topic_name = delta.topic_name;
      update.__set_from_version(registration.current_topic_version);
      continue;
    }
    // The topic version in the update is valid, process the update.
    MonotonicStopWatch update_callback_sw;
    update_callback_sw.Start();
    for (const UpdateCallback& callback : registration.callbacks) {
      callback(incoming_topic_deltas, subscriber_topic_updates);
    }
    update_callback_sw.Stop();
    registration.current_topic_version = delta.to_version;
    registration.processing_time_metric->Update(
        sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  }

  // Third and finally, reset the interval timers so they correctly measure the
  // time between RPCs, excluding processing time.
  for (int i = 0; i < deltas_to_process.size(); ++i) {
    if (!topic_update_locks[i].owns_lock()) continue;

    const TTopicDelta& delta = *deltas_to_process[i];
    auto it = topic_registrations_.find(delta.topic_name);
    DCHECK(it != topic_registrations_.end());
    TopicRegistration& registration = it->second;
    registration.update_interval_timer.Reset();
  }
  sw.Stop();
  topic_update_duration_metric_->Update(
      sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  return Status::OK();
}

bool StatestoreSubscriber::StatestoreStub::IsInPostRecoveryGracePeriod() const {
  bool has_disconnect_before = connection_failure_metric_->GetValue() > 0;
  bool in_disconnect_grace_period = MilliSecondsSinceLastRegistration()
      < FLAGS_statestore_subscriber_recovery_grace_period_ms;
  bool in_failover_grace_period = MilliSecondsSinceLastFailover()
      < FLAGS_statestore_subscriber_recovery_grace_period_ms;
  return (has_disconnect_before && in_disconnect_grace_period)
      || in_failover_grace_period;
}

bool StatestoreSubscriber::StatestoreStub::IsRegistered() {
  lock_guard<shared_mutex> exclusive_lock(lock_);
  return is_registered_;
}

void StatestoreSubscriber::StatestoreStub::SetStatestoreActive(
    bool is_active, int64_t active_statestored_version, bool has_failover) {
  lock_guard<mutex> l(active_lock_);
  is_active_ = is_active;
  DCHECK(active_statestored_version_ <= active_statestored_version);
  active_statestored_version_ = active_statestored_version;
  if (has_failover) {
    last_failover_time_.Store(MonotonicMillis());
  }
  active_status_metric_->SetValue(is_active);
}

int64_t StatestoreSubscriber::StatestoreStub::GetActiveVersion(bool* is_active) {
  lock_guard<mutex> l(active_lock_);
  *is_active = is_active_;
  return active_statestored_version_;
}

void StatestoreSubscriber::StatestoreStub::GetRegistrationIdAndStatestoreId(
    RegistrationId* registration_id, TUniqueId* statestore_id) {
  lock_guard<mutex> r(id_lock_);
  *registration_id = registration_id_;
  *statestore_id = statestore_id_;
}

void StatestoreSubscriber::StatestoreStub::IncCountForUpdateStatestoredRoleRPC() {
  lock_guard<shared_mutex> exclusive_lock(lock_);
  update_statestored_role_rpc_metric_->Increment(1);
}

TStatestoreConnState::type
StatestoreSubscriber::StatestoreStub::GetStatestoreConnState() {
  lock_guard<shared_mutex> exclusive_lock(lock_);
  FailureDetector::PeerState state = failure_detector_->GetPeerState(STATESTORE_ID);
  switch (state) {
    case FailureDetector::FAILED:
      return TStatestoreConnState::FAILED;
    case FailureDetector::UNKNOWN:
      return TStatestoreConnState::UNKNOWN;
    case FailureDetector::SUSPECTED:
    case FailureDetector::OK:
    default:
      return TStatestoreConnState::OK;
  }
}

std::string StatestoreSubscriber::StatestoreStub::GetAddress() const {
  return TNetworkAddressToString(statestore_address_);
}

}
