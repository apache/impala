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

enum StateStoreServiceVersion {
   V1
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
}

// Description of a single entry in a topic
struct TTopicItem {
  // Human-readable topic entry identifier
  1: required string key;

  // Byte-string value for this topic entry. May not be null-terminated (in that it may
  // contain null bytes)
  2: required string value;
}

// Sent from a client to the statestore to update a single topic
// TODO: Can we junk this and use TTopicDelta everywhere?
struct TTopicUpdate {
  // Name of the topic this update applies to
  1: required string topic_name;

  // List of changes to topic entries
  2: required list<TTopicItem> topic_updates;

  // List of topic entries to be deleted.
  3: required list<string> topic_deletions;
}

// Set of changes to a single topic, sent to a subscriber.
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
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Unique, human-readable identifier for this subscriber
  2: required string subscriber_id;

  // Location of the StateStoreSubscriberService that this subscriber runs
  3: required Types.TNetworkAddress subscriber_location;

  // List of topics to subscribe to
  4: required list<TTopicRegistration> topic_registrations;
}

struct TRegisterSubscriberResponse {
  // Whether the call was executed correctly at the application level
  1: required Status.TStatus status;
}

service StateStoreService {
  // Register a single subscriber. Note that after a subscriber is registered, no new
  // topics may be added.
  TRegisterSubscriberResponse RegisterSubscriber(1: TRegisterSubscriberRequest params);
}

struct TUpdateStateRequest {
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Map from topic name to a list of changes for that topic.
  2: required map<string, TTopicDelta> topic_deltas;
}

struct TUpdateStateResponse {
  // Whether the call was executed correctly at the application level
  1: required Status.TStatus status;

  // List of updates published by the subscriber to be made centrally by the state-store
  2: required list<TTopicUpdate> topic_updates;
}

service StateStoreSubscriber {
  // Heartbeat message sent by the state-store, containing a list of topic updates.
  // Response is list of updates published by the subscriber.
  TUpdateStateResponse UpdateState(1: TUpdateStateRequest params);
}
