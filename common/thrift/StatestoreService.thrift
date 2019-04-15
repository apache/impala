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

namespace cpp impala
namespace java org.apache.impala.thrift

include "Status.thrift"
include "Types.thrift"

enum StatestoreServiceVersion {
   V1 = 0
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
// constructs one TBackendDescriptor, and registers it in the cluster-membership
// topic. Impalads subscribe to this topic to learn of the location of
// all other Impalads in the cluster. Impalads can act as coordinators, executors or
// both.
struct TBackendDescriptor {
  // Network address of the thrift based ImpalaInternalService on this backend
  1: required Types.TNetworkAddress address;

  // IP address corresponding to address.hostname. Explicitly including this saves the
  // cost of resolution at every Impalad (since IP addresses are needed for scheduling)
  2: required string ip_address;

  // True if this is a coordinator node
  3: required bool is_coordinator;

  // True if this is an executor node
  4: required bool is_executor;

  // The address of the debug HTTP server
  5: optional Types.TNetworkAddress debug_http_address;

  // True if the debug webserver is secured (for correctly generating links)
  6: optional bool secure_webserver;

  // IP address + port of KRPC based ImpalaInternalService on this backend
  7: optional Types.TNetworkAddress krpc_address;

  // The amount of memory that can be admitted to this backend (in bytes).
  8: required i64 admit_mem_limit;

  // True if fragment instances should not be scheduled on this daemon because the
  // daemon has been quiescing, e.g. if it shutting down.
  9: required bool is_quiescing;
}

// Description of a single entry in a topic
struct TTopicItem {
  // Human-readable topic entry identifier
  1: required string key;

  // Byte-string value for this topic entry. May not be null-terminated (in that it may
  // contain null bytes). It can be non-empty when deleted is true. This is needed when
  // subscribers require additional information not captured in the item key to process
  // the deleted item (e.g., catalog version of deleted catalog object).
  2: required string value;

  // If true, this item was deleted. When false, this TTopicItem need not be included in
  // non-delta TTopicDelta's since the latest version of every still-present topic item
  // will be included.
  3: required bool deleted = false;
}

// Set of changes to a single topic, sent from the statestore to a subscriber as well as
// from a subscriber to the statestore.
struct TTopicDelta {
  // Name of the topic this delta applies to
  1: required string topic_name;

  // When is_delta=true, a list of changes to topic entries, including deletions, within
  // [from_version, to_version].
  // When is_delta=false, this is the list of all non-delete topic entries for
  // [0, to_version], which can be used to reconstruct the topic from scratch.
  2: required list<TTopicItem> topic_entries;

  // True if entries / deletions are relative to the topic at versions [0, from_version].
  3: required bool is_delta;

  // The Topic version range this delta covers. If there have been changes to the topic,
  // the update will include all changes in the range: [from_version, to_version).
  // If there have been no changes in the topic the from_version will match the
  // to_version. The from_version will always be 0 for non-delta updates.
  // If this is an update being sent from a subscriber to the statestore, the from_version
  // is set only when recovering from an inconsistent state, to the last version of the
  // topic the subscriber successfully processed. The value of to_version doesn't depend
  // on whether the update is delta or not.
  4: optional i64 from_version
  5: optional i64 to_version

  // The minimum topic version of all subscribers to the topic. This can be used to
  // determine when all subscribers have successfully processed a specific update.
  // This is guaranteed because no subscriber will ever be sent a topic containing
  // keys with a version < min_subscriber_topic_version. Only used when sending an update
  // from the statestore to a subscriber.
  6: optional i64 min_subscriber_topic_version

  // If set and true the statestore must clear the existing topic entries (if any) before
  // applying the entries in topic_entries.
  7: optional bool clear_topic_entries
}

// Description of a topic to subscribe to as part of a RegisterSubscriber call
struct TTopicRegistration {
  // Human readable key for this topic
  1: required string topic_name;

  // True if updates to this topic from this subscriber should be removed upon the
  // subscriber's failure or disconnection
  2: required bool is_transient;

  // If true, min_subscriber_topic_version is computed and set in topic updates sent
  // to this subscriber to this subscriber. Should only be set to true if this is
  // actually required - computing the version is relatively expensive compared to
  // other aspects of preparing topic updates - see IMPALA-6816.
  3: required bool populate_min_subscriber_topic_version = false;

  // Restrict the items to receive on this subscription to only those items
  // starting with the given prefix.
  //
  // If this is not specified, all items will be subscribed to.
  4: optional string filter_prefix
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
