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

namespace cpp impala
namespace java com.cloudera.impala.thrift

include "Status.thrift"
include "Types.thrift"

enum StatestoreServiceVersion {
   V1
}

// Structure serialized for the topic AdmissionController::IMPALA_REQUEST_QUEUE_TOPIC.
// Statistics for a single admission control pool. The topic key is of the form
// "<pool_name>!<backend_id>".
struct TPoolStats {
  // The current number of requests admitted by this host's admission controller
  // and are currently running. This is an instantaneous value (as opposed to a
  // cumulative sum).
  1: required i64 num_admitted_running;

  // The current number of queued requests. This is an instantaneous value.
  2: required i64 num_queued;

  // The memory (in bytes) currently reserved on this backend for use by queries in this
  // pool. E.g.  when a query which reserves 10G/host admitted to this pool begins
  // execution on this impalad, this value increases by 10G. Any other impalads executing
  // this query will also increment their backend_mem_reserved by 10G.
  3: required i64 backend_mem_reserved;
}

// Structure serialised in the Impala backend topic. Each Impalad
// constructs one TBackendDescriptor, and registers it in the backend
// topic. Impalads subscribe to this topic to learn of the location of
// all other Impalads in the cluster.
struct TBackendDescriptor {
  // Network address of the Impala service on this backend
  1: required Types.TNetworkAddress address;

  // IP address corresponding to address.hostname. Explicitly including this saves the
  // cost of resolution at every Impalad (since IP addresses are needed for scheduling)
  2: required string ip_address;

  // The address of the debug HTTP server
  3: optional Types.TNetworkAddress debug_http_address;

  // True if the debug webserver is secured (for correctly generating links)
  4: optional bool secure_webserver;
}

// Description of a single entry in a topic
struct TTopicItem {
  // Human-readable topic entry identifier
  1: required string key;

  // Byte-string value for this topic entry. May not be null-terminated (in that it may
  // contain null bytes)
  2: required string value;
}

// Set of changes to a single topic, sent from the statestore to a subscriber as well as
// from a subscriber to the statestore.
struct TTopicDelta {
  // Name of the topic this delta applies to
  1: required string topic_name;

  // List of changes to topic entries
  2: required list<TTopicItem> topic_entries;

  // List of topic item keys whose entries have been deleted
  3: required list<string> topic_deletions;

  // True if entries / deletions are to be applied to in-memory state,
  // otherwise topic_entries contains entire topic state.
  4: required bool is_delta;

  // The Topic version range this delta covers. If there have been changes to the topic,
  // the update will include all changes in the range: [from_version, to_version).
  // If there have been no changes in the topic the from_version will match the
  // to_version. The from_version will always be 0 for non-delta updates.
  // If this is an update being sent from a subscriber to the statestore, the from_version
  // is set only when recovering from an inconsistent state, to the last version of the
  // topic the subscriber successfully processed.
  5: optional i64 from_version
  6: optional i64 to_version

  // The minimum topic version of all subscribers to the topic. This can be used to
  // determine when all subscribers have successfully processed a specific update.
  // This is guaranteed because no subscriber will ever be sent a topic containing
  // keys with a version < min_subscriber_topic_version. Only used when sending an update
  // from the statestore to a subscriber.
  7: optional i64 min_subscriber_topic_version
}

// Description of a topic to subscribe to as part of a RegisterSubscriber call
struct TTopicRegistration {
  // Human readable key for this topic
  1: required string topic_name;

  // True if updates to this topic from this subscriber should be removed upon the
  // subscriber's failure or disconnection
  2: required bool is_transient;
}

struct TRegisterSubscriberRequest {
  1: required StatestoreServiceVersion protocol_version =
      StatestoreServiceVersion.V1

  // Unique, human-readable identifier for this subscriber
  2: required string subscriber_id;

  // Location of the StatestoreSubscriberService that this subscriber runs
  3: required Types.TNetworkAddress subscriber_location;

  // List of topics to subscribe to
  4: required list<TTopicRegistration> topic_registrations;
}

struct TRegisterSubscriberResponse {
  // Whether the call was executed correctly at the application level
  1: required Status.TStatus status;

  // Unique identifier for this registration. Changes with every call to
  // RegisterSubscriber().
  2: optional Types.TUniqueId registration_id;
}

service StatestoreService {
  // Register a single subscriber. Note that after a subscriber is registered, no new
  // topics may be added.
  TRegisterSubscriberResponse RegisterSubscriber(1: TRegisterSubscriberRequest params);
}

struct TUpdateStateRequest {
  1: required StatestoreServiceVersion protocol_version =
      StatestoreServiceVersion.V1

  // Map from topic name to a list of changes for that topic.
  2: required map<string, TTopicDelta> topic_deltas;

  // Registration ID for the last known registration from this subscriber.
  3: optional Types.TUniqueId registration_id;
}

struct TUpdateStateResponse {
  // Whether the call was executed correctly at the application level
  1: required Status.TStatus status;

  // List of updates published by the subscriber to be made centrally by the statestore
  2: required list<TTopicDelta> topic_updates;

  // True if this update was skipped by the subscriber. This is distinguished from a
  // non-OK status since the former indicates an error which contributes to the
  // statestore's view of a subscriber's liveness.
  3: optional bool skipped;
}

struct THeartbeatRequest {
  1: optional Types.TUniqueId registration_id;
}

struct THeartbeatResponse {

}

service StatestoreSubscriber {
  // Called when the statestore sends a topic update. The request contains a map of
  // topic names to TTopicDelta updates, sent from the statestore to the subscriber. Each
  // of these delta updates will contain a list of additions to the topic and a list of
  // deletions from the topic.
  // In response, the subscriber returns an aggregated list of updates to topic(s) to
  // the statestore. Each update is a TTopicDelta that contains a list of additions to
  // the topic and a list of deletions from the topic. Additionally, if a subscriber has
  // received an unexpected delta update version range, they can request a new delta
  // update based off a specific version from the statestore. The next statestore
  // delta update will be based off of the version the subscriber requested.
  TUpdateStateResponse UpdateState(1: TUpdateStateRequest params);

  // Called when the statestore sends a heartbeat.
  THeartbeatResponse Heartbeat(1: THeartbeatRequest params);
}
