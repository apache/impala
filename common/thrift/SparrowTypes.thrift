// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp sparrow
namespace java com.cloudera.sparrow.thrift

include "Types.thrift"

struct TVersionedObject {
  // Service that the object is associated with.
  1: string service_id

  // Unique (within the given service) identifier for the object.
  2: string key

  // Type of the value.
  3: string type

  // Unique (within the given key) identifier for the value.
  4: i64 version

  5: binary value
}

// Information about a running instance of a particular service.
struct TServiceInstance {
  // Unique identifier for the corresponding StateStoreSubscriber.
  1: required i32 subscriber_id

  2: required Types.THostPort host_port
}

// Information about all running instances of the service identified by service_id. 
struct TServiceMembership {
  1: required string service_id

  2: required list<TServiceInstance> service_instances
}
