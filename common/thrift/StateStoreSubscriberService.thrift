// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp sparrow
namespace java com.cloudera.sparrow.thrift

include "SparrowTypes.thrift"
include "Types.thrift"

enum StateStoreSubscriberServiceVersion {
  V1
}

struct TUpdateStateRequest {
  1: required StateStoreSubscriberServiceVersion protocolVersion =
      StateStoreSubscriberServiceVersion.V1
 
  // Membership information for each service that the subscriber subscribed to. 
  // Required in V1.
  2: optional list<SparrowTypes.TServiceMembership> service_memberships
  
  // Object updates for each service that the subscriber subscribed to.  Required in V1.
  3: optional list<SparrowTypes.TVersionedObject> updated_objects

  // Objects that have been deleted, for each service that the subscriber has subscribed
  // to. Required in V1.
  4: optional list<string> deleted_object_keys
}

struct TUpdateStateResponse {
  // Required in V1.
  1: optional Types.TStatus status

  // For each service running on the subscriber, the object updates.  Required in V1,
  // but not yet implemented.
  2: optional list<SparrowTypes.TVersionedObject> updated_objects

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
