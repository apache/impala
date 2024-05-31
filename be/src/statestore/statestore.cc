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

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/lexical_cast.hpp>
#include <thrift/Thrift.h>
#include <thrift/protocol/TProtocolException.h>
#include <gutil/strings/substitute.h>
#include <gutil/strings/util.h>

#include "common/status.h"
#include "gen-cpp/StatestoreService_types.h"
#include "rpc/rpc-trace.h"
#include "rpc/thrift-util.h"
#include "statestore/failure-detector.h"
#include "statestore/statestore-subscriber-client-wrapper.h"
#include "util/collection-metrics.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/logging-support.h"
#include "util/metrics.h"
#include "util/openssl-util.h"
#include "util/pretty-printer.h"
#include "util/test-info.h"
#include "util/time.h"
#include "util/uid-util.h"
#include "util/webserver.h"

#include "common/names.h"

using boost::posix_time::seconds;
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
DEFINE_double_hidden(heartbeat_monitoring_frequency_ms, 60000, "(Advanced) Frequency (in "
    "ms) with which the statestore monitors heartbeats from a subscriber.");

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

DEFINE_int32(statestore_update_catalogd_tcp_timeout_seconds, 3, "(Advanced) The "
    "time after which a UpdateCatalogd RPC to a subscriber will timeout. This setting "
    "protects against badly hung machines that are not able to respond to the "
    "UpdateCatalogd RPC in short order");

// Flags for Statestore HA
// Host and port of peer's StatestoreHaService for Statestore HA.
DEFINE_string(state_store_peer_host, "localhost",
    "hostname where peer's StatestoreHaService is running");
DEFINE_int32(state_store_peer_ha_port, 24021,
    "port where peer's StatestoreHaService is running");
DEFINE_bool(enable_statestored_ha, false, "Set to true to enable Statestore HA");
DEFINE_bool(statestore_force_active, false, "Set to true to force this statestored "
    "instance to take active role. It's used to perform manual fail over for statestore "
    "service.");
// Use network address as priority value of statestored instance when designating active
// statestored. The lower network address corresponds to a higher priority.
// This is mainly used in unit-test for predictable results.
DEFINE_bool(use_network_address_as_statestore_priority, false, "Network address is "
    "used as priority value of statestored instance if this is set as true. Otherwise, "
    "statestored_id which is generated as random number will be used as priority value "
    "of statestored instance.");
// Waiting period in ms for HA preemption. It should be set with proper value based on the
// time to take for bringing a statestored instance in-line in the deployment environment.
DEFINE_int64(statestore_ha_preemption_wait_period_ms, 10000, "(Advanced) The time after "
    "which statestored designates itself as active role if the statestore does not "
    "receive HA handshake request/response from peer statestored.");
DEFINE_double_hidden(statestore_ha_heartbeat_monitoring_frequency_ms, 1000, "(Advanced) "
    "Frequency (in ms) with which the statestore monitors HA heartbeats from active "
    "statestore.");
DEFINE_int32(statestore_update_statestore_tcp_timeout_seconds, 3, "(Advanced) The "
    "time after which a UpdateStatestoredRole RPC to a subscriber will timeout. This "
    "setting protects against badly hung machines that are not able to respond to the "
    "UpdateStatestoredRole RPC in short order.");
DEFINE_int32(statestore_peer_timeout_seconds, 30, "The amount of time (in seconds) that "
    "may elapse before the connection with the peer statestore is considered lost.");
DEFINE_int32(statestore_peer_cnxn_attempts, 10, "The number of times to retry an "
    "RPC connection to the peer statestore. A setting of 0 means retry indefinitely");
DEFINE_int32(statestore_peer_cnxn_retry_interval_ms, 1000, "The interval, in ms, "
    "to wait between attempts to make an RPC connection to the peer statestore. "
    "It's set as statestore_ha_preemption_wait_period_ms/statestore_peer_cnxn_attempts "
    "if statestore_peer_cnxn_attempts > 0, default value if "
    "statestore_peer_cnxn_attempts == 0.");
DEFINE_int32(statestore_ha_client_rpc_timeout_ms, 300000, "(Advanced) The underlying "
    "TSocket send/recv timeout in milliseconds for a client RPC of Statestore HA "
    "service.");
DEFINE_int64(update_statestore_rpc_resend_interval_ms, 100, "(Advanced) Interval "
    "(in ms) with which the statestore resends the RPCs of updating statestored's role "
    "to subscribers if the statestore has failed to send the RPCs to the subscribers.");

DECLARE_string(hostname);
DECLARE_bool(enable_catalogd_ha);
DECLARE_int64(active_catalogd_designation_monitoring_interval_ms);
DECLARE_int64(update_catalogd_rpc_resend_interval_ms);
DECLARE_string(debug_actions);
DECLARE_string(ssl_server_certificate);
DECLARE_string(ssl_private_key);
DECLARE_string(ssl_private_key_password_cmd);
DECLARE_string(ssl_cipher_list);
DECLARE_string(ssl_minimum_version);
#ifndef NDEBUG
DECLARE_int32(stress_statestore_startup_delay_ms);
#endif

// Used to identify the statestore in the failure detector
const string STATESTORE_ID = "STATESTORE";

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
const string STATESTORE_SUCCESSFUL_UPDATE_CATALOGD_RPC_NUM =
    "statestore.num-successful-update-catalogd-rpc";
const string STATESTORE_FAILED_UPDATE_CATALOGD_RPC_NUM =
    "statestore.num-failed-update-catalogd-rpc";
const string STATESTORE_SUCCESSFUL_UPDATE_STATESTORED_ROLE_RPC_NUM =
    "statestore.num-successful-update-statestored-role-rpc";
const string STATESTORE_FAILED_UPDATE_STATESTORED_ROLE_RPC_NUM =
    "statestore.num-failed-update-statestored-role-rpc";
const string STATESTORE_CLEAR_TOPIC_ENTRIES_NUM =
    "statestore.num-clear-topic-entries-requests";
const string STATESTORE_ACTIVE_CATALOGD_ADDRESS = "statestore.active-catalogd-address";
const string STATESTORE_ACTIVE_STATUS = "statestore.active-status";
const string STATESTORE_SERVICE_STARTED = "statestore.service-started";
const string STATESTORE_IN_HA_RECOVERY = "statestore.in-ha-recovery-mode";
const string STATESTORE_CONNECTED_PEER = "statestore.connected-with-peer-statestored";

// Initial version for each Topic registered by a Subscriber. Generally, the Topic will
// have a Version that is the MAX() of all entries in the Topic, but this initial
// value needs to be less than TopicEntry::TOPIC_ENTRY_INITIAL_VERSION to distinguish
// between the case where a Topic is empty and the case where the Topic only contains
// an entry with the initial version.
const Statestore::TopicEntry::Version Statestore::Subscriber::TOPIC_INITIAL_VERSION = 0;

// If statestore instance in active state receives more than 10 heartbeats from its peer,
// enter recovery mode to re-negotiate role with its peer.
// Heartbeat period is set by FALGS_statestore_ha_heartbeat_monitoring_frequency_ms, its
// default value is 1000 ms. That means statestore instance in active state will enter
// recovery mode in 10 seconds if it repeatedly receives heartbeats from its peer.
#define MAX_NUM_RECEIVED_HEARTBEAT_IN_ACTIVE 10

// Updates or heartbeats that miss their deadline by this much are logged.
const uint32_t DEADLINE_MISS_THRESHOLD_MS = 2000;

const char* Statestore::IMPALA_MEMBERSHIP_TOPIC = "impala-membership";
const char* Statestore::IMPALA_REQUEST_QUEUE_TOPIC = "impala-request-queue";

typedef ClientConnection<StatestoreSubscriberClientWrapper> StatestoreSubscriberConn;
typedef ClientConnection<StatestoreHaServiceClientWrapper> StatestoreHaServiceConn;

namespace impala {

std::string SubscriberTypeToString(TStatestoreSubscriberType::type t) {
  switch (t) {
    case TStatestoreSubscriberType::ADMISSIOND:
      return "ADMISSIOND";
    case TStatestoreSubscriberType::CATALOGD:
      return "CATALOGD";
    case TStatestoreSubscriberType::COORDINATOR:
      return "COORDINATOR";
    case TStatestoreSubscriberType::EXECUTOR:
      return "EXECUTOR";
    case TStatestoreSubscriberType::COORDINATOR_EXECUTOR:
      return "COORDINATOR AND EXECUTOR";
    case TStatestoreSubscriberType::UNKNOWN:
    default:
      return "UNKNOWN";
  };
}

}

class StatestoreThriftIf : public StatestoreServiceIf {
 public:
  StatestoreThriftIf(Statestore* statestore)
      : statestore_(statestore) {
    DCHECK(statestore_ != NULL);
  }

  virtual void RegisterSubscriber(TRegisterSubscriberResponse& response,
      const TRegisterSubscriberRequest& params) {
    if (FLAGS_debug_actions == "START_STATESTORE_IN_PROTOCOL_V1"
        && strncmp(params.subscriber_id.c_str(), "python-test-client", 18) == 0
        && params.protocol_version > StatestoreServiceVersion::V1) {
      // Simulate the behaviour of old statestore to throw exception for new version
      // of subscribers.
      throw apache::thrift::protocol::TProtocolException(
          apache::thrift::protocol::TProtocolException::INVALID_DATA, "Invalid data");
    }
    if (params.protocol_version < statestore_->GetProtocolVersion()) {
      // Refuse old version of subscribers
      response.__set_protocol_version(statestore_->GetProtocolVersion());
      response.__set_statestore_id(statestore_->GetStateStoreId());
      Status status = Status(TErrorCode::STATESTORE_INCOMPATIBLE_PROTOCOL,
          params.subscriber_id, params.protocol_version + 1,
          statestore_->GetProtocolVersion() + 1);
      status.ToThrift(&response.status);
      return;
    }
    TStatestoreSubscriberType::type subscriber_type = TStatestoreSubscriberType::UNKNOWN;
    if (params.__isset.subscriber_type) {
      subscriber_type = params.subscriber_type;
    }
    bool subscribe_catalogd_change = false;
    if (params.__isset.subscribe_catalogd_change) {
      subscribe_catalogd_change = params.subscribe_catalogd_change;
    }
    TCatalogRegistration catalogd_registration;
    if (params.__isset.catalogd_registration) {
      catalogd_registration = params.catalogd_registration;
      catalogd_registration.__set_registration_time(UnixMillis());
    }

    RegistrationId registration_id;
    bool has_active_catalogd;
    int64_t active_catalogd_version;
    TCatalogRegistration active_catalogd_registration;
    Status status = statestore_->RegisterSubscriber(params.subscriber_id,
        params.subscriber_location, params.topic_registrations, subscriber_type,
        subscribe_catalogd_change, catalogd_registration, &registration_id,
        &has_active_catalogd, &active_catalogd_version, &active_catalogd_registration);
    status.ToThrift(&response.status);
    response.__set_registration_id(registration_id);
    response.__set_statestore_id(statestore_->GetStateStoreId());
    response.__set_protocol_version(statestore_->GetProtocolVersion());
    bool is_active_statestored = false;
    int64_t active_statestored_version =
        statestore_->GetActiveVersion(&is_active_statestored);
    response.__set_statestore_is_active(is_active_statestored);
    response.__set_active_statestored_version(active_statestored_version);
    if (is_active_statestored && has_active_catalogd) {
      response.__set_catalogd_registration(active_catalogd_registration);
      response.__set_catalogd_version(active_catalogd_version);
      statestore_->UpdateSubscriberCatalogInfo(params.subscriber_id);
    }
  }

  virtual void GetProtocolVersion(TGetProtocolVersionResponse& response,
      const TGetProtocolVersionRequest& params) {
    LOG(INFO) << "Subscriber protocol version: " << params.protocol_version;
    response.__set_protocol_version(statestore_->GetProtocolVersion());
    response.__set_statestore_id(statestore_->GetStateStoreId());
    Status status = Status::OK();
    status.ToThrift(&response.status);
    return;
  }

  virtual void SetStatestoreDebugAction(TSetStatestoreDebugActionResponse& response,
      const TSetStatestoreDebugActionRequest& params) {
    if (params.__isset.disable_network) {
      bool disable_network = params.disable_network;
      LOG(INFO) << (disable_network ? "Disable" : "Enable")
                << " statestored network";
      statestore_->SetStatestoreDebugAction(disable_network);
    }
    Status status = Status::OK();
    status.ToThrift(&response.status);
    return;
  }

 private:
  Statestore* statestore_;
};

class StatestoreHaThriftIf : public StatestoreHaServiceIf {
 public:
  StatestoreHaThriftIf(Statestore* statestore) : statestore_(statestore) {
    DCHECK(statestore_ != NULL);
  }

  // Receive HA handshake request from peer statestore instance.
  // Each statestore instance start this StatestoreHaService server and each has client
  // cache. They use client to negotiate active-standby role with peer. If retry failed
  // for 3 times, assume its peer is not started and the instance take the active role.
  // Otherwise, designate the role based on the priorities.
  virtual void StatestoreHaHandshake(TStatestoreHaHandshakeResponse& response,
      const TStatestoreHaHandshakeRequest& params) {
    if (params.src_protocol_version < statestore_->GetProtocolVersion()) {
      // Refuse old version of statestore
      Status status = Status(TErrorCode::STATESTORE_INCOMPATIBLE_PROTOCOL,
          statestore_->GetPeerStatestoreHaAddress(), params.src_protocol_version + 1,
          statestore_->GetProtocolVersion() + 1);
      status.ToThrift(&response.status);
      return;
    } else if (params.src_statestore_id == statestore_->GetStateStoreId()) {
      // Ignore request sent by itself.
      return;
    }
    bool dst_statestore_active = false;
    Status status = statestore_->ReceiveHaHandshakeRequest(params.src_statestore_id,
        params.src_statestore_address, params.src_force_active, &dst_statestore_active);
    status.ToThrift(&response.status);
    response.__set_dst_protocol_version(statestore_->GetProtocolVersion());
    response.__set_dst_statestore_id(statestore_->GetStateStoreId());
    response.__set_dst_statestore_active(dst_statestore_active);
  }

  // Receive HA heartbeat from peer statestore instance.
  virtual void StatestoreHaHeartbeat(TStatestoreHaHeartbeatResponse& response,
      const TStatestoreHaHeartbeatRequest& params) {
    if (params.protocol_version < statestore_->GetProtocolVersion()) {
      // Refuse old version of statestore
      Status status =
          Status(TErrorCode::STATESTORE_INCOMPATIBLE_PROTOCOL,
              statestore_->GetPeerStatestoreHaAddress(), params.protocol_version + 1,
              statestore_->GetProtocolVersion() + 1);
      status.ToThrift(&response.status);
      return;
    }
    statestore_->HaHeartbeatRequest(params.dst_statestore_id, params.src_statestore_id);
    Status status = Status::OK();
    status.ToThrift(&response.status);
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
    entry_it->second.SetDeleted(true);
    entry_it->second.SetVersion(last_version_);
  }
}

void Statestore::Topic::ClearAllEntries() {
  lock_guard<shared_mutex> write_lock(lock_);
  entries_.clear();
  topic_update_log_.clear();
  int64_t key_size_metric_val = key_size_metric_->GetValue();
  key_size_metric_->SetValue(std::max(static_cast<int64_t>(0),
      key_size_metric_val - total_key_size_bytes_));
  int64_t value_size_metric_val = value_size_metric_->GetValue();
  value_size_metric_->SetValue(std::max(static_cast<int64_t>(0),
      value_size_metric_val - total_value_size_bytes_));
  int64_t topic_size_metric_val = topic_size_metric_->GetValue();
  topic_size_metric_->SetValue(std::max(static_cast<int64_t>(0),
      topic_size_metric_val - (total_value_size_bytes_ + total_key_size_bytes_)));
  total_value_size_bytes_ = 0;
  total_key_size_bytes_ = 0;
}

void Statestore::Topic::BuildDelta(const SubscriberId& subscriber_id,
    TopicEntry::Version last_processed_version,
    const string& filter_prefix, TTopicDelta* delta) {
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
      // Skip any entries that don't match the requested prefix.
      if (!HasPrefixString(itr->first, filter_prefix)) continue;

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

  topic_json->AddMember("key_size_bytes", key_size, document->GetAllocator());
  topic_json->AddMember("value_size_bytes", value_size, document->GetAllocator());
  topic_json->AddMember("total_size_bytes", key_size + value_size,
      document->GetAllocator());
  topic_json->AddMember("prioritized", IsPrioritizedTopic(topic_id_),
      document->GetAllocator());
}

Statestore::Subscriber::Subscriber(const SubscriberId& subscriber_id,
    const RegistrationId& registration_id, const TNetworkAddress& network_address,
    const vector<TTopicRegistration>& subscribed_topics,
    TStatestoreSubscriberType::type subscriber_type, bool subscribe_catalogd_change)
  : subscriber_id_(subscriber_id),
    registration_id_(registration_id),
    network_address_(network_address),
    subscriber_type_(subscriber_type),
    subscribe_catalogd_change_(subscribe_catalogd_change) {
  LOG(INFO) << "Subscriber '" << subscriber_id_
            << "' with type " << SubscriberTypeToString(subscriber_type_)
            << " registered (registration id: " << PrintId(registration_id_) << ")";
  RefreshLastHeartbeatTimestamp();
  for (const TTopicRegistration& topic : subscribed_topics) {
    GetTopicsMapForId(topic.topic_name)
        ->emplace(piecewise_construct, forward_as_tuple(topic.topic_name),
            forward_as_tuple(
                topic.is_transient, topic.populate_min_subscriber_topic_version,
                topic.filter_prefix));
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
  // IMPALA-7714: log warning to aid debugging in release builds without DCHECK.
  if (UNLIKELY(topic_it == subscribed_topics->end())) {
    LOG(ERROR) << "Couldn't find subscribed topic " << topic_id;
  }
  DCHECK(topic_it != subscribed_topics->end()) << topic_id;
  topic_it->second.last_version.Store(version);
}

void Statestore::Subscriber::RefreshLastHeartbeatTimestamp() {
  DCHECK_GE(MonotonicMillis(), last_heartbeat_ts_.Load());
  last_heartbeat_ts_.Store(MonotonicMillis());
}

void Statestore::Subscriber::UpdateCatalogInfo(
    int64_t catalogd_version, const TNetworkAddress& catalogd_address) {
  catalogd_version_ = catalogd_version;
  catalogd_address_ = catalogd_address;
  last_update_catalogd_time_ = UnixMillis();
}

Statestore::Statestore(MetricGroup* metrics)
  : protocol_version_(StatestoreServiceVersion::V2),
    catalog_manager_(FLAGS_enable_catalogd_ha),
    subscriber_topic_update_threadpool_("statestore-update",
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
    update_catalogd_client_cache_(new StatestoreSubscriberClientCache(1, 0,
        FLAGS_statestore_update_catalogd_tcp_timeout_seconds * 1000,
        FLAGS_statestore_update_catalogd_tcp_timeout_seconds * 1000, "",
        IsInternalTlsConfigured())),
    thrift_iface_(new StatestoreThriftIf(this)),
    ha_thrift_iface_(new StatestoreHaThriftIf(this)),
    failure_detector_(new MissedHeartbeatFailureDetector(
        FLAGS_statestore_max_missed_heartbeats,
        FLAGS_statestore_max_missed_heartbeats / 2)) {
  UUIDToTUniqueId(boost::uuids::random_generator()(), &statestore_id_);
  LOG(INFO) << "Statestore ID: " << PrintId(statestore_id_);
  DCHECK(metrics != NULL);
  metrics_ = metrics;
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
  successful_update_catalogd_rpc_metric_ =
      metrics->AddCounter(STATESTORE_SUCCESSFUL_UPDATE_CATALOGD_RPC_NUM, 0);
  failed_update_catalogd_rpc_metric_ =
      metrics->AddCounter(STATESTORE_FAILED_UPDATE_CATALOGD_RPC_NUM, 0);
  successful_update_statestored_role_rpc_metric_ =
      metrics->AddCounter(STATESTORE_SUCCESSFUL_UPDATE_STATESTORED_ROLE_RPC_NUM, 0);
  failed_update_statestored_role_rpc_metric_ =
      metrics->AddCounter(STATESTORE_FAILED_UPDATE_STATESTORED_ROLE_RPC_NUM, 0);
  clear_topic_entries_metric_ =
      metrics->AddCounter(STATESTORE_CLEAR_TOPIC_ENTRIES_NUM, 0);
  active_catalogd_address_metric_ = metrics->AddProperty<string>(
      STATESTORE_ACTIVE_CATALOGD_ADDRESS, "");
  active_status_metric_ = metrics->AddProperty(STATESTORE_ACTIVE_STATUS, true);
  service_started_metric_ = metrics->AddProperty(STATESTORE_SERVICE_STARTED, false);

  update_state_client_cache_->InitMetrics(metrics, "subscriber-update-state");
  heartbeat_client_cache_->InitMetrics(metrics, "subscriber-heartbeat");
  update_catalogd_client_cache_->InitMetrics(
      metrics, "subscriber-update-catalogd");
  if (!FLAGS_enable_statestored_ha) {
    is_active_ = true;
    active_status_metric_->SetValue(is_active_);
    active_version_ = UnixMicros();
    num_received_heartbeat_in_active_ = 0;
  } else {
    is_active_ = false;
    active_status_metric_->SetValue(is_active_);
    active_version_ = 0;
    update_statestored_client_cache_.reset(new StatestoreSubscriberClientCache(
        1, 0, FLAGS_statestore_update_statestore_tcp_timeout_seconds * 1000,
        FLAGS_statestore_update_statestore_tcp_timeout_seconds * 1000, "",
        IsInternalTlsConfigured()));
    update_statestored_client_cache_->InitMetrics(
        metrics, "subscriber-update-statestored");
    ha_client_cache_.reset(new StatestoreHaClientCache(1, 0,
        FLAGS_statestore_ha_client_rpc_timeout_ms,
        FLAGS_statestore_ha_client_rpc_timeout_ms, "",
        IsInternalTlsConfigured()));
    ha_client_cache_->InitMetrics(metrics, "statestored-ha");
    ha_standby_ss_failure_detector_.reset(new MissedHeartbeatFailureDetector(
        FLAGS_statestore_max_missed_heartbeats,
        FLAGS_statestore_max_missed_heartbeats / 2));
    ha_active_ss_failure_detector_.reset(new TimeoutFailureDetector(
        seconds(FLAGS_statestore_peer_timeout_seconds),
        seconds(FLAGS_statestore_peer_timeout_seconds / 2)));

    in_ha_recovery_mode_metric_ = metrics->AddProperty(STATESTORE_IN_HA_RECOVERY, false);
    connected_peer_metric_ = metrics->AddProperty(STATESTORE_CONNECTED_PEER, false);
  }
}

Statestore::~Statestore() {
  CHECK(service_started_) << "Cannot shutdown Statestore once initialized and started.";
}

Status Statestore::Init(int32_t state_store_port) {
#ifndef NDEBUG
  if (FLAGS_stress_statestore_startup_delay_ms > 0) {
    LOG(INFO) << "Stress statestore startup delay: "
              << FLAGS_stress_statestore_startup_delay_ms << " ms";
    SleepForMs(FLAGS_stress_statestore_startup_delay_ms);
  }
#endif
  std::shared_ptr<TProcessor> processor(new StatestoreServiceProcessor(thrift_iface()));
  std::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("statestore", metrics_));
  processor->setEventHandler(event_handler);
  ThriftServerBuilder builder("StatestoreService", processor, state_store_port);
  // Mark this as an internal service to use a more permissive Thrift max message size
  builder.is_external_facing(false);
  if (IsInternalTlsConfigured()) {
    SSLProtocol ssl_version;
    RETURN_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
    LOG(INFO) << "Enabling SSL for Statestore";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
        .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
        .ssl_version(ssl_version)
        .cipher_list(FLAGS_ssl_cipher_list);
  }
  RETURN_IF_ERROR(subscriber_topic_update_threadpool_.Init());
  RETURN_IF_ERROR(subscriber_priority_topic_update_threadpool_.Init());
  RETURN_IF_ERROR(subscriber_heartbeat_threadpool_.Init());

  ThriftServer* server;
  RETURN_IF_ERROR(builder.metrics(metrics_).Build(&server));
  thrift_server_.reset(server);
  RETURN_IF_ERROR(thrift_server_->Start());

  RETURN_IF_ERROR(Thread::Create("statestore-heartbeat", "heartbeat-monitoring-thread",
      &Statestore::MonitorSubscriberHeartbeat, this, &heartbeat_monitoring_thread_));
  RETURN_IF_ERROR(Thread::Create("statestore-update-catalogd", "update-catalogd-thread",
      &Statestore::MonitorUpdateCatalogd, this, &update_catalogd_thread_));
  service_started_ = true;
  service_started_metric_->SetValue(true);
  return Status::OK();
}

void Statestore::RegisterWebpages(Webserver* webserver, bool metrics_only) {
  Webserver::RawUrlCallback healthz_callback =
      [this](const auto& req, auto* data, auto* response) {
        return this->HealthzHandler(req, data, response);
      };
  webserver->RegisterUrlCallback("/healthz", healthz_callback);

  if (metrics_only) return;

  Webserver::UrlCallback topics_callback =
      bind<void>(mem_fn(&Statestore::TopicsHandler), this, _1, _2);
  webserver->RegisterUrlCallback("/topics", "statestore_topics.tmpl",
      topics_callback, true);

  Webserver::UrlCallback subscribers_callback =
      bind<void>(&Statestore::SubscribersHandler, this, _1, _2);
  webserver->RegisterUrlCallback("/subscribers", "statestore_subscribers.tmpl",
      subscribers_callback, true);

  if (FLAGS_enable_catalogd_ha) {
    Webserver::UrlCallback show_catalog_ha_callback =
        bind<void>(&Statestore::CatalogHAInfoHandler, this, _1, _2);
    webserver->RegisterUrlCallback(
        "/catalog_ha_info", "catalog_ha_info.tmpl", show_catalog_ha_callback, true);
  }

  RegisterLogLevelCallbacks(webserver, false);
}

void Statestore::TopicsHandler(const Webserver::WebRequest& req,
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

void Statestore::SubscribersHandler(const Webserver::WebRequest& req,
    Document* document) {
  lock_guard<mutex> l(subscribers_lock_);
  Value subscribers(kArrayType);
  for (const SubscriberMap::value_type& subscriber: subscribers_) {
    Value sub_json(kObjectType);

    Value subscriber_id(subscriber.second->id().c_str(), document->GetAllocator());
    sub_json.AddMember("id", subscriber_id, document->GetAllocator());

    Value address(TNetworkAddressToString(subscriber.second->network_address()).c_str(),
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

    Value secs_since_heartbeat(
        StringPrintf("%.3f", subscriber.second->SecondsSinceHeartbeat()).c_str(),
        document->GetAllocator());
    sub_json.AddMember(
        "secs_since_heartbeat", secs_since_heartbeat, document->GetAllocator());

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
    if (FLAGS_enable_statestored_ha) {
      ActiveConnStateMap::iterator conn_states_it =
          active_conn_states_.find(update.subscriber_id);
      if (conn_states_it != active_conn_states_.end()) {
        if (conn_states_it->second == TStatestoreConnState::FAILED) {
          --failed_conn_state_count_;
        }
        active_conn_states_.erase(conn_states_it);
      }
    }
    LOG(ERROR) << ss.str();
    return Status(ss.str());
  }

  return Status::OK();
}

Status Statestore::RegisterSubscriber(const SubscriberId& subscriber_id,
    const TNetworkAddress& location,
    const vector<TTopicRegistration>& topic_registrations,
    TStatestoreSubscriberType::type subscriber_type,
    bool subscribe_catalogd_change,
    const TCatalogRegistration& catalogd_registration,
    RegistrationId* registration_id,
    bool* has_active_catalogd,
    int64_t* active_catalogd_version,
    TCatalogRegistration* active_catalogd_registration) {
  bool is_catalogd = subscriber_type == TStatestoreSubscriberType::CATALOGD;
  if (subscriber_id.empty()) {
    return Status("Subscriber ID cannot be empty string");
  } else if (is_catalogd
      && FLAGS_enable_catalogd_ha != catalogd_registration.enable_catalogd_ha) {
    return Status("CalaogD HA enabling flag from catalogd does not match.");
  } else if (disable_network_.Load()) {
    return Status("Reject registration since network is disabled.");
  }

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
  bool is_reregistering = false;
  {
    lock_guard<mutex> l(subscribers_lock_);
    UUIDToTUniqueId(subscriber_uuid_generator_(), registration_id);
    SubscriberMap::iterator subscriber_it = subscribers_.find(subscriber_id);
    if (subscriber_it != subscribers_.end()) {
      shared_ptr<Subscriber> subscriber = subscriber_it->second;
      UnregisterSubscriber(subscriber.get());
      // Check if the subscriber's network addresses are matching.
      if (subscriber->network_address().hostname != location.hostname
          || subscriber->network_address().port != location.port) {
        LOG(INFO) << "Subscriber " << subscriber_id
                  << " re-register with different address, old address: "
                  << TNetworkAddressToString(subscriber->network_address())
                  << " , new address: " << TNetworkAddressToString(location);
      }
      is_reregistering = true;
    }

    if (is_catalogd
        && catalog_manager_.RegisterCatalogd(is_reregistering, subscriber_id,
            *registration_id, catalogd_registration)) {
      LOG(INFO) << "Active catalogd role is designated to "
                << catalog_manager_.GetActiveCatalogdSubscriberId();
      update_catalod_cv_.NotifyAll();
    }

    shared_ptr<Subscriber> current_registration(new Subscriber(
        subscriber_id, *registration_id, location, topic_registrations,
        subscriber_type, subscribe_catalogd_change));
    subscribers_.emplace(subscriber_id, current_registration);
    if (FLAGS_enable_statestored_ha) {
      active_conn_states_.emplace(subscriber_id, TStatestoreConnState::OK);
    }
    failure_detector_->UpdateHeartbeat(subscriber_id, true);
    num_subscribers_metric_->SetValue(subscribers_.size());
    subscriber_set_metric_->Add(subscriber_id);

    // Add the subscriber to the update queue, with an immediate schedule.
    ScheduledSubscriberUpdate update(0, subscriber_id, *registration_id);
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_topic_update_threadpool_));
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_priority_topic_update_threadpool_));
    RETURN_IF_ERROR(OfferUpdate(update, &subscriber_heartbeat_threadpool_));
    *active_catalogd_registration =
        catalog_manager_.GetActiveCatalogRegistration(
            has_active_catalogd, active_catalogd_version);
  }

  return Status::OK();
}

void Statestore::SetStatestoreDebugAction(bool disable_network) {
  if (FLAGS_debug_actions == "DISABLE_STATESTORE_NETWORK") {
    disable_network_.Store(disable_network);
  }
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
  if (!IsActive()) {
    // Don't send topic update if the statestored is not active.
    return Status::OK();
  } else if (FLAGS_enable_catalogd_ha && subscriber->IsCatalogd()
      && !catalog_manager_.IsActiveCatalogd(subscriber->id())) {
    // Don't send topic update to inactive catalogd.
    VLOG(3) << "Skip sending topic update to inactive catalogd";
    return Status::OK();
  }

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
  update_state_request.__set_statestore_id(statestore_id_);

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
      // Check if the subscriber indicated that the topic entries should be
      // cleared.
      if (update.__isset.clear_topic_entries && update.clear_topic_entries) {
        DCHECK(!update.__isset.from_version);
        LOG(INFO) << "Received request for clearing the entries of topic: "
                  << update.topic_name << " from: " << subscriber->id();
        clear_topic_entries_metric_->Increment(1);
        topic.ClearAllEntries();
      }

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

void Statestore::GatherTopicUpdates(const Subscriber& subscriber, UpdateKind update_kind,
    TUpdateStateRequest* update_state_request) {
  DCHECK(update_kind == UpdateKind::TOPIC_UPDATE
      || update_kind == UpdateKind::PRIORITY_TOPIC_UPDATE)
      << static_cast<int>(update_kind);
  // Indices into update_state_request->topic_deltas where we need to populate
  // 'min_subscriber_topic_version'. GetMinSubscriberTopicVersion() is somewhat
  // expensive so we want to avoid calling it unless necessary.
  vector<TTopicDelta*> deltas_needing_min_version;
  {
    const bool is_priority = update_kind == UpdateKind::PRIORITY_TOPIC_UPDATE;
    const Subscriber::Topics& subscribed_topics = is_priority ?
        subscriber.priority_subscribed_topics() :
        subscriber.non_priority_subscribed_topics();
    shared_lock<shared_mutex> l(topics_map_lock_);
    for (const auto& subscribed_topic : subscribed_topics) {
      auto topic_it = topics_.find(subscribed_topic.first);
      DCHECK(topic_it != topics_.end());
      TopicEntry::Version last_processed_version =
          subscriber.LastTopicVersionProcessed(topic_it->first);

      TTopicDelta& topic_delta =
          update_state_request->topic_deltas[subscribed_topic.first];
      topic_delta.topic_name = subscribed_topic.first;
      topic_it->second.BuildDelta(subscriber.id(), last_processed_version,
          subscribed_topic.second.filter_prefix, &topic_delta);
      if (subscribed_topic.second.populate_min_subscriber_topic_version) {
        deltas_needing_min_version.push_back(&topic_delta);
      }
    }
  }

  // Fill in the min subscriber topic version. This must be done after releasing
  // topics_map_lock_.
  if (!deltas_needing_min_version.empty()) {
    lock_guard<mutex> l(subscribers_lock_);
    for (TTopicDelta* delta : deltas_needing_min_version) {
      delta->__set_min_subscriber_topic_version(
          GetMinSubscriberTopicVersion(delta->topic_name));
    }
  }
}

Statestore::TopicEntry::Version Statestore::GetMinSubscriberTopicVersion(
    const TopicId& topic_id, SubscriberId* subscriber_id) {
  TopicEntry::Version min_topic_version = numeric_limits<int64_t>::max();
  bool found = false;
  // Find the minimum version processed for this topic across all topic subscribers.
  for (const SubscriberMap::value_type& subscriber: subscribers_) {
    if (FLAGS_enable_catalogd_ha && subscriber.second->IsCatalogd()
        && !catalog_manager_.IsActiveCatalogd(subscriber.second->id())) {
      // Skip inactive catalogd since it does not apply catalog updates from the active
      // catalogd.
      continue;
    }
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
  return "";
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
  return nullptr;
}

Status Statestore::SendHeartbeat(Subscriber* subscriber) {
  if (disable_network_.Load()) {
    return Status("Don't send heartbeat since network is disabled.");
  }

  MonotonicStopWatch sw;
  sw.Start();

  Status status;
  StatestoreSubscriberConn client(heartbeat_client_cache_.get(),
      subscriber->network_address(), &status);
  RETURN_IF_ERROR(status);

  THeartbeatRequest request;
  THeartbeatResponse response;
  request.__set_registration_id(subscriber->registration_id());
  request.__set_statestore_id(statestore_id_);
  if (FLAGS_enable_statestored_ha && !IsActive()) {
    // Send heartbeat to subscriber with request for connection state between active
    // statestore and subscriber.
    request.__set_request_statestore_conn_state(true);
  }
  RETURN_IF_ERROR(
      client.DoRpc(&StatestoreSubscriberClientWrapper::Heartbeat, request, &response));

  heartbeat_duration_metric_->Update(sw.ElapsedTime() / (1000.0 * 1000.0 * 1000.0));
  if (FLAGS_enable_statestored_ha && !IsActive()
      && response.__isset.statestore_conn_state) {
    TStatestoreConnState::type conn_state = response.statestore_conn_state;
    lock_guard<mutex> l(subscribers_lock_);
    if (active_conn_states_.find(subscriber->id()) != active_conn_states_.end()
        && active_conn_states_[subscriber->id()] != conn_state) {
      if (conn_state == TStatestoreConnState::FAILED) {
        DCHECK(active_conn_states_[subscriber->id()] == TStatestoreConnState::OK
            || active_conn_states_[subscriber->id()] == TStatestoreConnState::UNKNOWN);
        ++failed_conn_state_count_;
      } else if (active_conn_states_[subscriber->id()] == TStatestoreConnState::FAILED) {
        DCHECK(conn_state == TStatestoreConnState::OK
            || conn_state == TStatestoreConnState::UNKNOWN);
        --failed_conn_state_count_;
      } else {
        DCHECK((active_conn_states_[subscriber->id()] == TStatestoreConnState::OK &&
            conn_state == TStatestoreConnState::UNKNOWN)
            || (conn_state == TStatestoreConnState::OK &&
            active_conn_states_[subscriber->id()] == TStatestoreConnState::UNKNOWN));
      }
      // Save the connection state between active statestore and subscriber.
      active_conn_states_[subscriber->id()] = conn_state;
    }
  }
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

  DebugActionNoFail(FLAGS_debug_actions, "DO_SUBSCRIBER_UPDATE");

  // Send the right message type, and compute the next deadline
  int64_t deadline_ms = 0;
  Status status;
  if (is_heartbeat) {
    status = SendHeartbeat(subscriber.get());
    if (status.ok()) {
      subscriber->RefreshLastHeartbeatTimestamp();
    } else if (status.code() == TErrorCode::RPC_RECV_TIMEOUT) {
      // Add details to status to make it more useful, while preserving the stack
      status.AddDetail(Substitute(
          "Subscriber $0 timed-out during heartbeat RPC. Timeout is $1s.",
          subscriber->id(), FLAGS_statestore_heartbeat_tcp_timeout_seconds));
    }

    deadline_ms = UnixMillis() + FLAGS_statestore_heartbeat_frequency_ms;
  } else {
    // Initialize to false so that we don't consider the update skipped when
    // SendTopicUpdate() fails.
    bool update_skipped = false;
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
                  << PrintId(update.registration_id) << ")";
        UnregisterSubscriber(subscriber.get());
        if (subscriber->IsCatalogd()) {
          if (catalog_manager_.UnregisterCatalogd(subscriber->id())) {
            update_catalod_cv_.NotifyAll();
          }
        }
      } else {
        LOG(INFO) << "Failure was already detected for subscriber '" << subscriber->id()
                  << "'. Won't send another " << update_kind_str;
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

[[noreturn]] void Statestore::MonitorSubscriberHeartbeat() {
  while (1) {
    int num_subscribers;
    vector<SubscriberId> inactive_subscribers;
    SleepForMs(FLAGS_heartbeat_monitoring_frequency_ms);
    {
      lock_guard<mutex> l(subscribers_lock_);
      num_subscribers = subscribers_.size();
      for (const auto& subscriber : subscribers_) {
        if (subscriber.second->SecondsSinceHeartbeat()
            > FLAGS_heartbeat_monitoring_frequency_ms) {
          inactive_subscribers.push_back(subscriber.second->id());
        }
      }
    }
    if (inactive_subscribers.empty()) {
      LOG(INFO) << "All " << num_subscribers
                << " subscribers successfully heartbeat in the last "
                << FLAGS_heartbeat_monitoring_frequency_ms << "ms.";
    } else {
      int num_active_subscribers = num_subscribers - inactive_subscribers.size();
      LOG(WARNING) << num_active_subscribers << "/" << num_subscribers
                   << " subscribers successfully heartbeat in the last "
                   << FLAGS_heartbeat_monitoring_frequency_ms << "ms."
                   << " Slow subscribers: " << boost::join(inactive_subscribers, ", ");
    }
  }
}

[[noreturn]] void Statestore::MonitorUpdateCatalogd() {
  int64_t last_active_catalogd_version = 0;
  // rpc_receivers is used to track subscribers to which statestore need to send RPCs
  // when there is a change in the elected active catalogd. It is updated from
  // subscribers_, and the subscribers will be removed from this list if the RPCs are
  // successfully sent to them.
  vector<std::shared_ptr<Subscriber>> rpc_receivers;
  int64_t timeout_us =
      FLAGS_active_catalogd_designation_monitoring_interval_ms * MICROS_PER_MILLI;
  // Check if the first registered one should be designated with active role.
  while (!catalog_manager_.CheckActiveCatalog()) {
    unique_lock<mutex> l(*catalog_manager_.GetLock());
    update_catalod_cv_.WaitFor(l, timeout_us);
  }
  SendUpdateCatalogdNotification(&last_active_catalogd_version, rpc_receivers);

  // Wait for notification. If new leader is elected due to catalogd is registered or
  // unregistered, send notification to all coordinators and catalogds.
  timeout_us = FLAGS_update_catalogd_rpc_resend_interval_ms * MICROS_PER_MILLI;
  while (1) {
    {
      unique_lock<mutex> l(*catalog_manager_.GetLock());
      update_catalod_cv_.WaitFor(l, timeout_us);
    }
    SendUpdateCatalogdNotification(&last_active_catalogd_version, rpc_receivers);
  }
}

void Statestore::SendUpdateCatalogdNotification(int64_t* last_active_catalogd_version,
    vector<std::shared_ptr<Subscriber>>& rpc_receivers) {
  // Don't send UpdateCatalogd RPC if the statestore is not active.
  if (!IsActive()) return;

  bool has_active_catalogd;
  int64_t active_catalogd_version = 0;
  TCatalogRegistration catalogd_registration =
      catalog_manager_.GetActiveCatalogRegistration(
          &has_active_catalogd, &active_catalogd_version);
  if (!has_active_catalogd ||
      (active_catalogd_version == *last_active_catalogd_version
          && rpc_receivers.empty())) {
    // Don't resend RPCs if there is no change in Active Catalogd and no RPC failure in
    // last round.
    return;
  }

  bool resend_rpc = false;
  if (active_catalogd_version > *last_active_catalogd_version) {
    // Send notification for the latest elected active catalogd.
    LOG(INFO) << "Send notification for active catalogd version: "
              << active_catalogd_version;
    active_catalogd_address_metric_->SetValue(
        TNetworkAddressToString(catalogd_registration.address));
    rpc_receivers.clear();
    {
      lock_guard<mutex> l(subscribers_lock_);
      for (const auto& subscriber : subscribers_) {
        if (subscriber.second->IsSubscribedCatalogdChange()) {
          rpc_receivers.push_back(subscriber.second);
        }
      }
    }
    *last_active_catalogd_version = active_catalogd_version;
  } else {
    DCHECK(!rpc_receivers.empty());
    lock_guard<mutex> l(subscribers_lock_);
    for (std::vector<std::shared_ptr<Subscriber>>::iterator it = rpc_receivers.begin();
         it != rpc_receivers.end();) {
      // Don't resend RPC to subscribers which have been removed from subscriber list.
      std::shared_ptr<Subscriber> subscriber = *it;
      if (subscribers_.find(subscriber->id()) == subscribers_.end()) {
        it = rpc_receivers.erase(it);
      } else {
        ++it;
      }
    }
    if (rpc_receivers.empty()) return;
    resend_rpc = true;
  }

  for (std::vector<std::shared_ptr<Subscriber>>::iterator it = rpc_receivers.begin();
       it != rpc_receivers.end();) {
    std::shared_ptr<Subscriber> subscriber = *it;
    bool update_skipped = false;
    Status status;
    if (!resend_rpc) {
      status = DebugAction(
          FLAGS_debug_actions, "SEND_UPDATE_CATALOGD_RPC_FIRST_ATTEMPT");
    }
    if (status.ok()) {
      StatestoreSubscriberConn client(update_catalogd_client_cache_.get(),
          subscriber->network_address(), &status);
      if (status.ok()) {
        TUpdateCatalogdRequest request;
        TUpdateCatalogdResponse response;
        request.__set_registration_id(subscriber->registration_id());
        request.__set_statestore_id(statestore_id_);
        request.__set_catalogd_version(active_catalogd_version);
        request.__set_catalogd_registration(catalogd_registration);
        status = client.DoRpc(
            &StatestoreSubscriberClientWrapper::UpdateCatalogd, request, &response);
        if (!status.ok()) {
          if (status.code() == TErrorCode::RPC_RECV_TIMEOUT) {
            // Add details to status to make it more useful, while preserving the stack
            status.AddDetail(Substitute(
                "Subscriber $0 timed-out during update catalogd RPC. Timeout is $1s.",
                subscriber->id(), FLAGS_statestore_update_catalogd_tcp_timeout_seconds));
          }
        } else {
          update_skipped = (response.__isset.skipped && response.skipped);
        }
      }
    }
    if (status.ok()) {
      if (update_skipped) {
        // The subscriber skipped processing this update. It's not considered as a failure
        // since subscribers can decide what they do with any update. The subscriber is
        // left in the receiver list so that RPC will be resent to it in next round.
        ++it;
      } else {
        UpdateSubscriberCatalogInfo(it->get()->id());
        successful_update_catalogd_rpc_metric_->Increment(1);
        // Remove the subscriber from the receiver list so that Statestore will not resend
        // RPC to it in next round.
        it = rpc_receivers.erase(it);
      }
    } else {
      LOG(ERROR) << "Couldn't send UpdateCatalogd RPC,  " << status.GetDetail();
      failed_update_catalogd_rpc_metric_->Increment(1);
      // Leave the subscriber in the receiver list. Statestore will resend RPC to it in
      // next round.
      ++it;
    }
  }
  if (rpc_receivers.empty()) {
    LOG(INFO) << "Successfully sent UpdateCatalogd RPCs to all subscribers";
  }
}

[[noreturn]] void Statestore::MonitorUpdateStatestoredRole() {
  // rpc_receivers is used to track subscribers to which statestore need to send RPCs
  // when there is a change in the elected active statestored. It is updated from
  // subscribers_, and the subscribers will be removed from this list if the RPCs are
  // successfully sent to them.
  vector<std::shared_ptr<Subscriber>> rpc_receivers;
  // Wait for notification. If new statestored leader is elected, send notification
  // to all subscribers.
  int64_t timeout_us = FLAGS_update_statestore_rpc_resend_interval_ms * MICROS_PER_MILLI;
  int64_t last_active_statestored_version = 0;
  while (1) {
    {
      unique_lock<mutex> l(ha_lock_);
      update_statestored_cv_.WaitFor(l, timeout_us);
    }
    SendUpdateStatestoredRoleNotification(
        &last_active_statestored_version, rpc_receivers);
  }
}

void Statestore::SendUpdateStatestoredRoleNotification(
    int64_t* last_active_statestored_version,
    vector<std::shared_ptr<Subscriber>>& rpc_receivers) {
  bool is_active_statestored = false;
  int64_t active_statestored_version = GetActiveVersion(&is_active_statestored);
  if (!is_active_statestored ||
      (active_statestored_version == *last_active_statestored_version
          && rpc_receivers.empty())) {
    // Don't resend RPCs if there is no change in active statestored and no RPC failure
    // in last round.
    return;
  }
  bool has_active_catalogd;
  int64_t active_catalogd_version = 0;
  TCatalogRegistration catalogd_registration =
      catalog_manager_.GetActiveCatalogRegistration(
          &has_active_catalogd, &active_catalogd_version);
  if (has_active_catalogd) {
    active_catalogd_address_metric_->SetValue(
        TNetworkAddressToString(catalogd_registration.address));
  }

  bool resend_rpc = false;
  if (active_statestored_version > *last_active_statestored_version) {
    // Send notification for the latest elected active statestored.
    LOG(INFO) << "Send notification for active statestored version: "
              << active_statestored_version;
    // statestored_active_status_metric_->SetValue(true);
    rpc_receivers.clear();
    {
      lock_guard<mutex> l(subscribers_lock_);
      for (const auto& subscriber : subscribers_) {
        rpc_receivers.push_back(subscriber.second);
      }
    }
    *last_active_statestored_version = active_statestored_version;
  } else {
    DCHECK(!rpc_receivers.empty());
    lock_guard<mutex> l(subscribers_lock_);
    for (std::vector<std::shared_ptr<Subscriber>>::iterator it = rpc_receivers.begin();
         it != rpc_receivers.end();) {
      // Don't resend RPC to subscribers which have been removed from subscriber list.
      std::shared_ptr<Subscriber> subscriber = *it;
      if (subscribers_.find(subscriber->id()) == subscribers_.end()) {
        it = rpc_receivers.erase(it);
      } else {
        ++it;
      }
    }
    if (rpc_receivers.empty()) return;
    resend_rpc = true;
  }

  for (std::vector<std::shared_ptr<Subscriber>>::iterator it = rpc_receivers.begin();
       it != rpc_receivers.end();) {
    std::shared_ptr<Subscriber> subscriber = *it;
    Status status;
    if (!resend_rpc) {
      status = DebugAction(
          FLAGS_debug_actions, "SEND_UPDATE_STATESTORED_RPC_FIRST_ATTEMPT");
    }
    if (status.ok()) {
      StatestoreSubscriberConn client(update_statestored_client_cache_.get(),
          subscriber->network_address(), &status);
      if (status.ok()) {
        TUpdateStatestoredRoleRequest request;
        TUpdateStatestoredRoleResponse response;
        request.__set_registration_id(subscriber->registration_id());
        request.__set_statestore_id(statestore_id_);
        request.__set_active_statestored_version(active_statestored_version);
        request.__set_is_active(true);
        if (has_active_catalogd && subscriber->IsSubscribedCatalogdChange()) {
          request.__set_catalogd_version(active_catalogd_version);
          request.__set_catalogd_registration(catalogd_registration);
        }
        status = client.DoRpc(&StatestoreSubscriberClientWrapper::UpdateStatestoredRole,
            request, &response);
        if (!status.ok() && status.code() == TErrorCode::RPC_RECV_TIMEOUT) {
          // Add details to status to make it more useful, while preserving the stack
          status.AddDetail(Substitute(
              "Subscriber $0 timed-out during update catalogd RPC. Timeout is $1s.",
              subscriber->id(), FLAGS_statestore_update_catalogd_tcp_timeout_seconds));
        }
      }
    }
    if (status.ok()) {
      // Remove the subscriber from the receiver list so that Statestore will not resend
      // RPC to it in next round.
      successful_update_statestored_role_rpc_metric_->Increment(1);
      it = rpc_receivers.erase(it);
    } else {
      LOG(ERROR) << "Couldn't send UpdateStatestoredRole RPC,  " << status.GetDetail();
      failed_update_statestored_role_rpc_metric_->Increment(1);
      // Leave the subscriber in the receiver list. Statestore will resend RPC to it in
      // next round.
      ++it;
    }
  }
  if (rpc_receivers.empty()) {
    LOG(INFO) << "Successfully sent UpdateStatestoredRole RPCs to all subscribers";
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
  update_catalogd_client_cache_->CloseConnections(subscriber->network_address());
  if (FLAGS_enable_statestored_ha) {
    update_statestored_client_cache_->CloseConnections(subscriber->network_address());
  }

  // Prevent the failure detector from growing without bound
  failure_detector_->EvictPeer(subscriber->id());

  // Delete all transient entries
  {
    shared_lock<shared_mutex> topic_lock(topics_map_lock_);
    subscriber->DeleteAllTransientEntries(&topics_);
  }

  num_subscribers_metric_->Increment(-1L);
  subscriber_set_metric_->Remove(subscriber->id());
  subscribers_.erase(subscriber->id());
  if (FLAGS_enable_statestored_ha) {
    ActiveConnStateMap::iterator conn_states_it =
        active_conn_states_.find(subscriber->id());
    if (conn_states_it != active_conn_states_.end()) {
      if (conn_states_it->second == TStatestoreConnState::FAILED) {
        --failed_conn_state_count_;
      }
      active_conn_states_.erase(conn_states_it);
    }
  }
}

void Statestore::MainLoop() {
  subscriber_topic_update_threadpool_.Join();
  subscriber_priority_topic_update_threadpool_.Join();
  subscriber_heartbeat_threadpool_.Join();
}

void Statestore::ShutdownForTesting() {
  CHECK(TestInfo::is_be_test()) << "Only valid to call in backend tests.";
  subscriber_topic_update_threadpool_.Shutdown();
  subscriber_priority_topic_update_threadpool_.Shutdown();
  subscriber_heartbeat_threadpool_.Shutdown();
  subscriber_topic_update_threadpool_.Join();
  subscriber_priority_topic_update_threadpool_.Join();
  subscriber_heartbeat_threadpool_.Join();
}

int64_t Statestore::FailedExecutorDetectionTimeMs() {
  return FLAGS_statestore_max_missed_heartbeats * FLAGS_statestore_heartbeat_frequency_ms;
}

void Statestore::HealthzHandler(
    const Webserver::WebRequest& req, std::stringstream* data, HttpStatusCode* response) {
  if (service_started_) {
    (*data) << "OK";
    *response = HttpStatusCode::Ok;
    return;
  }
  *(data) << "Not Available";
  *response = HttpStatusCode::ServiceUnavailable;
}

Status Statestore::InitStatestoreHa(
    int32_t statestore_ha_port, const TNetworkAddress& peer_statestore_ha_addr) {
  local_statestore_ha_addr_ = MakeNetworkAddress(FLAGS_hostname, statestore_ha_port);
  peer_statestore_ha_addr_ = peer_statestore_ha_addr;

  std::shared_ptr<TProcessor> processor(
      new StatestoreHaServiceProcessor(ha_thrift_iface()));
  std::shared_ptr<TProcessorEventHandler> event_handler(
      new RpcEventHandler("StatestoreHa", metrics_));
  processor->setEventHandler(event_handler);
  ThriftServerBuilder builder("StatestoreHaService", processor, statestore_ha_port);
  // Mark this as an internal service to use a more permissive Thrift max message size
  builder.is_external_facing(false);
  if (IsInternalTlsConfigured()) {
    SSLProtocol ssl_version;
    RETURN_IF_ERROR(
        SSLProtoVersions::StringToProtocol(FLAGS_ssl_minimum_version, &ssl_version));
    LOG(INFO) << "Enabling SSL for Statestore";
    builder.ssl(FLAGS_ssl_server_certificate, FLAGS_ssl_private_key)
        .pem_password_cmd(FLAGS_ssl_private_key_password_cmd)
        .ssl_version(ssl_version)
        .cipher_list(FLAGS_ssl_cipher_list);
  }
  ThriftServer* ha_server;
  RETURN_IF_ERROR(builder.metrics(metrics_).Build(&ha_server));
  ha_thrift_server_.reset(ha_server);
  RETURN_IF_ERROR(ha_thrift_server_->Start());
  // Wait till Thrift server is ready.
  RETURN_IF_ERROR(WaitForLocalServer(
      *ha_thrift_server_, /* num_retries */ 10, /* retry_interval_ms */ 1000));

  RETURN_IF_ERROR(Thread::Create("statestore-ha-heartbeat",
      "ha-heartbeat-monitoring-thread", &Statestore::MonitorStatestoredHaHeartbeat,
      this, &ha_monitoring_thread_));
  RETURN_IF_ERROR(Thread::Create("statestore-update-statestored",
      "update-statestored-thread",&Statestore::MonitorUpdateStatestoredRole, this,
      &update_statestored_thread_));

  // Negotiate role for HA with peer statestore instance on startup.
  TStatestoreHaHandshakeResponse response;
  Status status = SendHaHandshake(&response);
  if (!status.ok()) {
    // statestored designates itself as active if the statestore can not connect to the
    // peer statestored.
    lock_guard<mutex> l(ha_lock_);
    is_active_ = true;
    active_status_metric_->SetValue(is_active_);
    active_version_ = UnixMicros();
    num_received_heartbeat_in_active_ = 0;
    LOG(INFO) << "Set Statestore as active since it does not receive handshake "
              << "response in HA preemption waiting period";
    found_peer_ = false;
    connected_peer_metric_->SetValue(found_peer_);
  } else {
    status = Status(response.status);
    DCHECK(status.ok());
    lock_guard<mutex> l(ha_lock_);
    peer_statestore_id_ = response.dst_statestore_id;
    is_active_ = !response.dst_statestore_active;
    active_status_metric_->SetValue(is_active_);
    if (is_active_) active_version_ = UnixMicros();
    found_peer_ = true;
    connected_peer_metric_->SetValue(found_peer_);
    // connected_to_peer_statestore_metric_->SetValue(true);
    LOG(INFO) << "Receive Statestore HA handshake response, set the statestore as "
              << (is_active_ ? "active" : "standby");
  }
  return Status::OK();
}

bool Statestore::IsActive() {
  lock_guard<mutex> l(ha_lock_);
  return is_active_;
}

int64_t Statestore::GetActiveVersion(bool* is_active) {
  lock_guard<mutex> l(ha_lock_);
  *is_active = is_active_;
  return active_version_;
}

bool Statestore::IsInRecoveryMode() {
  lock_guard<mutex> l(ha_lock_);
  return in_recovery_mode_;
}

Status Statestore::SendHaHandshake(TStatestoreHaHandshakeResponse* response) {
  if (disable_network_.Load()) {
    return Status("Don't send HA handshake since network is disabled.");
  }
  // Negotiate the role for HA with peer statestore instance in client mode.
  LOG(INFO) << "Send Statestore HA handshake request";
  TStatestoreHaHandshakeRequest request;
  request.__set_src_statestore_id(statestore_id_);
  request.__set_src_statestore_address(
      TNetworkAddressToString(local_statestore_ha_addr_));
  request.__set_src_force_active(FLAGS_statestore_force_active);
  int attempt = 0; // Used for debug action only.
  if (FLAGS_statestore_peer_cnxn_attempts > 0) {
    FLAGS_statestore_peer_cnxn_retry_interval_ms =
        FLAGS_statestore_ha_preemption_wait_period_ms /
        FLAGS_statestore_peer_cnxn_attempts;
  }
  StatestoreHaServiceConn::RpcStatus rpc_status =
      StatestoreHaServiceConn::DoRpcWithRetry(ha_client_cache_.get(),
          peer_statestore_ha_addr_,
          &StatestoreHaServiceClientWrapper::StatestoreHaHandshake,
          request,
          FLAGS_statestore_peer_cnxn_attempts,
          FLAGS_statestore_peer_cnxn_retry_interval_ms,
          [&attempt]() {
            return attempt++ == 0 ?
                DebugAction(FLAGS_debug_actions, "STATESTORE_HA_HANDSHAKE_FIRST_ATTEMPT")
                : Status::OK();
          },
          response);
  return rpc_status.status;
}

Status Statestore::ReceiveHaHandshakeRequest(const TUniqueId& peer_statestore_id,
    const string& peer_statestore_address, bool peer_force_active,
    bool* statestore_active) {
  // Process HA handshake request from peer statstore
  LOG(INFO) << "Receive Statestore HA handshake request";
  lock_guard<mutex> l(ha_lock_);
  peer_statestore_id_ = peer_statestore_id;
  if (peer_force_active && !FLAGS_statestore_force_active) {
    is_active_ = false;
    active_status_metric_->SetValue(is_active_);
    ha_active_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
    LOG(INFO) << "Set the statestored as standby since the peer is started with force "
              << "active flag";
  } else if (!is_active_) {
    if (FLAGS_statestore_force_active && !peer_force_active) {
      is_active_ = true;
      active_status_metric_->SetValue(is_active_);
      active_version_ = UnixMicros();
      num_received_heartbeat_in_active_ = 0;
      ha_standby_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
      LOG(INFO) << "Set the statestored as active since it's started with force active "
                << "flag";
    } else {
      // Compare priority and assign the statestored with high priority as active
      // statestored.
      is_active_ = FLAGS_use_network_address_as_statestore_priority ?
          TNetworkAddressToString(local_statestore_ha_addr_) < peer_statestore_address :
          statestore_id_ < peer_statestore_id;
      active_status_metric_->SetValue(is_active_);
      if (is_active_) {
        active_version_ = UnixMicros();
        ha_standby_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
      } else {
        ha_active_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
      }
      LOG(INFO) << "Set the statestored as " << (is_active_ ? "active" : "standby");
    }
  } else {
    LOG(INFO) << "Active state of statestored is not changed";
  }
  *statestore_active = is_active_;
  if (!found_peer_) {
    found_peer_ = true;
    connected_peer_metric_->SetValue(found_peer_);
  }
  return Status::OK();
}

void Statestore::HaHeartbeatRequest(const TUniqueId& dst_statestore_id,
    const TUniqueId& src_statestore_id) {
  // Don't process HA heartbeat if network is disabled.
  if (disable_network_.Load()) return;
  lock_guard<mutex> l(ha_lock_);
  if (!is_active_) {
    // process HA heartbeat from active statestore
    ha_active_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, true);
  } else {
    num_received_heartbeat_in_active_++;
    if (num_received_heartbeat_in_active_ <= MAX_NUM_RECEIVED_HEARTBEAT_IN_ACTIVE) {
      return;
    }
    // Repeatedly receive heartbeat from its peer statestored. That means both statestored
    // designate themselves as active. Enter recovery mode to restart negotiation.
    LOG(WARNING)
        << "Both statestoreds designate themselves as active, restart negotiation.";
    in_recovery_mode_ = true;
    recovery_start_time_ = MonotonicMillis();
    in_ha_recovery_mode_metric_->SetValue(in_recovery_mode_);
    is_active_ = false;
    active_status_metric_->SetValue(is_active_);
    LOG(WARNING) << "Enter HA recovery mode.";
  }
}

// TODO: break this function to 3 functions for each branch: recovery-mode, active state,
// and standby state.
[[noreturn]] void Statestore::MonitorStatestoredHaHeartbeat() {
  bool sleep_between_processing = true;
  while (1) {
    if (sleep_between_processing) {
      SleepForMs(FLAGS_statestore_ha_heartbeat_monitoring_frequency_ms);
    } else {
      sleep_between_processing = true;
    }
    if (IsInRecoveryMode()) {
      // Keep sending HA handshake request to its peer periodically until receiving
      // response. Don't hold the ha_lock_ when sending HA handshake.
      TStatestoreHaHandshakeResponse response;
      Status status = SendHaHandshake(&response);
      if (!status.ok()) continue;
      status = Status(response.status);
      DCHECK(status.ok());

      lock_guard<mutex> l(ha_lock_);
      if (!in_recovery_mode_) {
        sleep_between_processing = false;
        continue;
      }
      // Exit "recovery" mode.
      in_recovery_mode_ = false;
      in_ha_recovery_mode_metric_->SetValue(in_recovery_mode_);
      peer_statestore_id_ = response.dst_statestore_id;
      is_active_ = !response.dst_statestore_active;
      active_status_metric_->SetValue(is_active_);
      found_peer_ = true;
      connected_peer_metric_->SetValue(found_peer_);
      int64_t elapsed_ms = MonotonicMillis() - recovery_start_time_;
      LOG(INFO) << "Receive Statestore HA handshake response, exit HA recovery mode in "
                << PrettyPrinter::Print(elapsed_ms, TUnit::TIME_MS)
                << ". Set the statestored as " << (is_active_ ? "active" : "standby");
      if (is_active_) {
        active_version_ = UnixMicros();
        // Send notification to all subscribers.
        update_statestored_cv_.NotifyAll();
      }
    } else if (IsActive()) {
      // Statestored in active state
      // Send HA heartbeat to standby statestored.
      bool send_heartbeat = false;
      {
        lock_guard<mutex> l(ha_lock_);
        if (is_active_ && found_peer_) send_heartbeat = true;
      }
      if (send_heartbeat) {
        Status status = SendHaHeartbeat();
        if (status.ok()) continue;
      }
      lock_guard<mutex> l(ha_lock_);
      if (!is_active_) {
        sleep_between_processing = false;
        continue;
      }
      // Check if standby statestored is reachable.
      FailureDetector::PeerState state =
          ha_standby_ss_failure_detector_->GetPeerState(STATESTORE_ID);
      if (state != FailureDetector::FAILED) {
        continue;
      } else if (found_peer_) {
        // Stop sending HA heartbeat.
        found_peer_ = false;
        connected_peer_metric_->SetValue(found_peer_);
        LOG(INFO) << "Statestored lost connection with peer statestored";
      }

      lock_guard<mutex> l2(subscribers_lock_);
      if (subscribers_.size() == 0) {
        // To avoid race with new active statestored, original active statestored enter
        // "recovery" mode if it does not receive heartbeat responses from standby
        // statestored and all subscribers.
        in_recovery_mode_ = true;
        recovery_start_time_ = MonotonicMillis();
        in_ha_recovery_mode_metric_->SetValue(in_recovery_mode_);
        is_active_ = false;
        active_status_metric_->SetValue(is_active_);
        LOG(WARNING) << "Enter HA recovery mode.";
      }
      // TODO: IMPALA-12507 Need better approach to handle split-brain in network.
      // In the scenario, active statestored still can reach some subscribers.
    } else {
      // Statestored in standby state
      // Monitor connection state with its peer statestored.
      lock_guard<mutex> l(ha_lock_);
      if (is_active_) {
        sleep_between_processing = false;
        continue;
      }
      FailureDetector::PeerState state =
          ha_active_ss_failure_detector_->GetPeerState(STATESTORE_ID);
      // Check if the majority of subscribers lost connection with active statestored.
      int failed_conn_state_count = 0;
      int total_subscribers = 0;
      {
        lock_guard<mutex> l(subscribers_lock_);
        failed_conn_state_count = failed_conn_state_count_;
        total_subscribers = active_conn_states_.size();
      }
      bool majority_failed = failed_conn_state_count > 0 &&
          failed_conn_state_count > total_subscribers / 2;
      if (state != FailureDetector::FAILED) {
        if (majority_failed) {
          LOG(WARNING) << "Active statestored may have network issue. "
                       << failed_conn_state_count << " out of " << total_subscribers
                       << " subscribers lost connections with active statestored.";
        }
        continue;
      }

      found_peer_ = false;
      connected_peer_metric_->SetValue(found_peer_);
      if (majority_failed) {
        // When standby statestored lost connection with active statestored, take over
        // active role if the majority of subscribers lost connections with active
        // statestored.
        LOG(INFO) << "Statestore change to active state, " << failed_conn_state_count
                  << " out of " << total_subscribers
                  << " subscribers lost connections with active statestored.";
        is_active_ = true;
        active_status_metric_->SetValue(is_active_);
        active_version_ = UnixMicros();
        num_received_heartbeat_in_active_ = 0;
        // Send notification to all subscribers.
        update_statestored_cv_.NotifyAll();
      } else if (total_subscribers == 0) {
        // If there is no subscriber, it means this statestored lost connection with
        // other nodes in the cluster, enter "recovery" mode.
        in_recovery_mode_ = true;
        recovery_start_time_ = MonotonicMillis();
        in_ha_recovery_mode_metric_->SetValue(in_recovery_mode_);
        LOG(WARNING) << "Enter HA recovery mode.";
      } else {
        VLOG(3) << "Standby statestored missed HA heartbeat from active statestored, "
                << failed_conn_state_count << " out of " << total_subscribers
                << " subscribers lost connections with active statestored.";
      }
    }
  }
}

Status Statestore::SendHaHeartbeat() {
  if (disable_network_.Load()) {
    ha_standby_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, false);
    return Status("Don't send HA heartbeat since network is disabled.");
  }

  Status status;
  StatestoreHaServiceConn client(ha_client_cache_.get(),
      peer_statestore_ha_addr_, &status);
  RETURN_IF_ERROR(status);

  TStatestoreHaHeartbeatRequest request;
  TStatestoreHaHeartbeatResponse response;
  {
    lock_guard<mutex> l(ha_lock_);
    request.__set_dst_statestore_id(peer_statestore_id_);
  }
  request.__set_src_statestore_id(statestore_id_);
  status = client.DoRpc(&StatestoreHaServiceClientWrapper::StatestoreHaHeartbeat,
      request, &response);
  ha_standby_ss_failure_detector_->UpdateHeartbeat(STATESTORE_ID, status.ok());
  if (status.ok()) {
    status = Status(response.status);
  }
  return status;
}

void Statestore::UpdateSubscriberCatalogInfo(const SubscriberId& subscriber_id) {
  lock_guard<mutex> l(subscribers_lock_);
  SubscriberMap::iterator it = subscribers_.find(subscriber_id);
  if (it == subscribers_.end()) return;
  std::shared_ptr<Subscriber> subscriber = it->second;
  bool has_active_catalogd;
  int64_t active_catalogd_version = 0;
  TCatalogRegistration catalogd_registration =
      catalog_manager_.GetActiveCatalogRegistration(
          &has_active_catalogd, &active_catalogd_version);
  if (has_active_catalogd) {
    subscriber->UpdateCatalogInfo(active_catalogd_version, catalogd_registration.address);
  }
}

void Statestore::CatalogHAInfoHandler(
    const Webserver::WebRequest& req, Document* document) {
  if (FLAGS_enable_statestored_ha && !is_active_) {
    document->AddMember("is_active_statestored", false, document->GetAllocator());
    return;
  }
  document->AddMember("is_active_statestored", true, document->GetAllocator());
  // HA INFO
  bool has_active_catalogd;
  int64_t active_catalogd_version = 0;
  TCatalogRegistration active_catalog_registration =
      catalog_manager_.GetActiveCatalogRegistration(&has_active_catalogd,
          &active_catalogd_version);

  document->AddMember("has_active_catalogd", has_active_catalogd,
      document->GetAllocator());
  document->AddMember("active_catalogd_version", active_catalogd_version,
      document->GetAllocator());
  if (active_catalogd_version > 0) {
    Value last_update_catalogd_time_(ToStringFromUnixMillis(
        catalog_manager_.GetLastUpdateCatalogTime(),
        TimePrecision::Millisecond).c_str(), document->GetAllocator());
    document->AddMember("last_update_catalogd_time", last_update_catalogd_time_,
        document->GetAllocator());
  }

  if (has_active_catalogd) {
    // Active catalogd information.
    document->AddMember("active_catalogd_enable_catalogd_ha",
        active_catalog_registration.enable_catalogd_ha, document->GetAllocator());
    Value active_catalogd_address(
        TNetworkAddressToString(active_catalog_registration.address).c_str(),
        document->GetAllocator());
    document->AddMember("active_catalogd_address", active_catalogd_address,
        document->GetAllocator());
    document->AddMember("active_catalogd_force_catalogd_active",
        active_catalog_registration.force_catalogd_active, document->GetAllocator());
    Value active_catalogd_registration_time(ToStringFromUnixMillis(
        active_catalog_registration.registration_time,
        TimePrecision::Millisecond).c_str(), document->GetAllocator());
    document->AddMember("active_catalogd_registration_time",
        active_catalogd_registration_time, document->GetAllocator());
  }

  // Standby catalogd information.
  TCatalogRegistration standby_catalog_registration =
      catalog_manager_.GetStandbyCatalogRegistration();
  if (standby_catalog_registration.__isset.registration_time) {
    document->AddMember("standby_catalogd_enable_catalogd_ha",
        standby_catalog_registration.enable_catalogd_ha, document->GetAllocator());
    Value standby_catalogd_address(
        TNetworkAddressToString(standby_catalog_registration.address).c_str(),
        document->GetAllocator());
    document->AddMember(
        "standby_catalogd_address", standby_catalogd_address, document->GetAllocator());
    document->AddMember("standby_catalogd_force_catalogd_active",
        standby_catalog_registration.force_catalogd_active, document->GetAllocator());
    Value standby_catalogd_registration_time(ToStringFromUnixMillis(
        standby_catalog_registration.registration_time,
        TimePrecision::Millisecond).c_str(), document->GetAllocator());
    document->AddMember("standby_catalogd_registration_time",
        standby_catalogd_registration_time, document->GetAllocator());
  }

  lock_guard<mutex> l(subscribers_lock_);
  Value notified_subscribers(kArrayType);
  for (const SubscriberMap::value_type& subscriber : subscribers_) {
    // Only subscribers of type COORDINATOR, COORDINATOR_EXECUTOR, or CATALOGD
    // need to be returned.
    if (subscriber.second->IsSubscribedCatalogdChange()) {
      Value sub_json(kObjectType);
      Value subscriber_id(subscriber.second->id().c_str(), document->GetAllocator());
      sub_json.AddMember("id", subscriber_id, document->GetAllocator());
      Value address(TNetworkAddressToString(
          subscriber.second->network_address()).c_str(), document->GetAllocator());
      sub_json.AddMember("address", address, document->GetAllocator());
      Value registration_id(PrintId(subscriber.second->registration_id()).c_str(),
          document->GetAllocator());
      sub_json.AddMember("registration_id", registration_id, document->GetAllocator());
      Value subscriber_type(SubscriberTypeToString(
          subscriber.second->subscriber_type()).c_str(), document->GetAllocator());
      sub_json.AddMember("subscriber_type", subscriber_type, document->GetAllocator());
      if (subscriber.second->catalogd_version() > 0) {
        sub_json.AddMember("catalogd_version", subscriber.second->catalogd_version(),
            document->GetAllocator());
        Value catalogd_address(TNetworkAddressToString(
            subscriber.second->catalogd_address()).c_str(), document->GetAllocator());
        sub_json.AddMember("catalogd_address", catalogd_address,
            document->GetAllocator());
        Value last_subscriber_update_catalogd_time(ToStringFromUnixMillis(
            subscriber.second->last_update_catalogd_time(),
            TimePrecision::Millisecond).c_str(), document->GetAllocator());
        sub_json.AddMember("last_subscriber_update_catalogd_time",
            last_subscriber_update_catalogd_time, document->GetAllocator());
      }

      notified_subscribers.PushBack(sub_json, document->GetAllocator());
    }
  }
  document->AddMember(
      "notified_subscribers", notified_subscribers, document->GetAllocator());
  return;
}
