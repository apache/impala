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

include "StatestoreTypes.thrift"
include "Status.thrift"
include "Types.thrift"

enum StateStoreSubscriberServiceVersion {
  V1
}

struct TUpdateStateRequest {
  1: required StateStoreSubscriberServiceVersion protocolVersion =
      StateStoreSubscriberServiceVersion.V1
 
  // Membership information for each service that the subscriber subscribed to. 
  // Required in V1.
  2: optional list<StatestoreTypes.TServiceMembership> service_memberships
  
  // Object updates for each service that the subscriber subscribed to.  Required in V1.
  3: optional list<StatestoreTypes.TVersionedObject> updated_objects

  // Objects that have been deleted, for each service that the subscriber has subscribed
  // to. Required in V1.
  4: optional list<string> deleted_object_keys
}

struct TUpdateStateResponse {
  // Required in V1.
  1: optional Status.TStatus status

  // For each service running on the subscriber, the object updates.  Required in V1,
  // but not yet implemented.
  2: optional list<StatestoreTypes.TVersionedObject> updated_objects

  // Objects that have been deleted, for each service that the subscriber has subscribed
  // to. Required in V1, but not yet implemented.
  3: optional list<string> deleted_object_keys
}

// The StateStoreSubscriber runs on all servers that need to connect to the StateStore,
// and provides an interface between the StateStore and services that need to either push
// updates to or receive updates from the StateStore.
service StateStoreSubscriberService {
  // The UpdateState() call serves two purposes: first, it allows the StateStore to push
  // new updates to the subscriber; and second, it allows the StateStore to collect new
  // updates from services running local with the StateStoreSubscriber.
  TUpdateStateResponse UpdateState(1: TUpdateStateRequest request);
}
