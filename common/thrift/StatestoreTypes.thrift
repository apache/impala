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

  2: required Types.TNetworkAddress host_port
}

// Information about all running instances of the service identified by service_id. 
struct TServiceMembership {
  1: required string service_id

  2: required list<TServiceInstance> service_instances
}
