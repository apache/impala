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
include "Llama.thrift"

enum TResourceBrokerServiceVersion {
   V1
}

struct TResourceBrokerRegisterRequest {
  1: required TResourceBrokerServiceVersion version;
  2: optional i32 client_process_id;
  3: optional Types.TNetworkAddress notification_callback_service;
}

struct TResourceBrokerRegisterResponse {
  1: optional Status.TStatus status;
  2: optional Types.TUniqueId irb_handle;
}

struct TResourceBrokerUnregisterRequest {
  1: optional TResourceBrokerServiceVersion version;
  2: optional Types.TUniqueId irb_handle;
}

struct TResourceBrokerUnregisterResponse {
  1: optional Status.TStatus status;
}

struct TResourceBrokerExpansionRequest {
  1: required TResourceBrokerServiceVersion version;
  2: optional Llama.TResource resource;
  3: optional Types.TUniqueId reservation_id;
  4: optional i64 request_timeout;
}

struct TResourceBrokerExpansionResponse {
  1: optional Types.TUniqueId reservation_id;
  2: optional map<Types.TNetworkAddress, Llama.TAllocatedResource> allocated_resources;
}

struct TResourceBrokerReservationRequest {
  1: required TResourceBrokerServiceVersion version;
  2: optional Types.TUniqueId irb_handle;
  3: optional string queue;
  4: optional list<Llama.TResource> resources;

  // If true, requires a reservation response to either grant or deny all resources
  // in this request. If false, reservation responses may deliver partially
  // granted/denied resources.
  5: optional bool gang;

  // Max time in milliseconds the resource broker should wait for
  // a resource request to be granted by Llama/Yarn.
  6: optional i64 request_timeout;

  // Used to allow Llama to grant or deny access to the requested queue.
  7: optional string user;
}

struct TResourceBrokerReservationResponse {
  1: optional Status.TStatus status;
  2: optional Types.TUniqueId reservation_id;
  3: optional map<Types.TNetworkAddress, Llama.TAllocatedResource> allocated_resources;
}

struct TResourceBrokerReleaseRequest {
  1: optional TResourceBrokerServiceVersion version;
  2: optional Types.TUniqueId irb_handle;
  3: optional Types.TUniqueId reservation_id;
}

struct TResourceBrokerReleaseResponse {
  1: optional Status.TStatus status;
}

service ResourceBrokerService {
  TResourceBrokerRegisterResponse
      Register(1: TResourceBrokerRegisterRequest request);
  TResourceBrokerUnregisterResponse
      Unregister(1: TResourceBrokerUnregisterRequest request);
  TResourceBrokerReservationResponse
      Reserve(1: TResourceBrokerReservationRequest request);
  TResourceBrokerReleaseResponse
      Release(1: TResourceBrokerReleaseRequest request);
}

struct TResourceBrokerNotificationPreemptionRequest {
  1: required TResourceBrokerServiceVersion version;
  2: optional list<string> preempted_rm_resource_ids;
}

struct TResourceBrokerNotificationPreemptionResponse {
  1: optional Status.TStatus status;
}

service ResourceBrokerNotificationService {
  TResourceBrokerRegisterResponse
      Preempt(1: TResourceBrokerRegisterRequest request);
}
