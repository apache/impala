// Copyright 2013 Cloudera Inc.
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

namespace cpp llama
namespace java com.cloudera.llama.thrift

////////////////////////////////////////////////////////////////////////////////
// DATA TYPES

enum TLlamaServiceVersion {
   V1
}

struct TUniqueId {
  1: required i64 hi;
  2: required i64 lo;
}

struct TNetworkAddress {
  1: required string hostname;
  2: required i32    port;
}

enum TStatusCode {
  OK,
  REQUEST_ERROR,
  INTERNAL_ERROR
}

struct TStatus {
  1: required TStatusCode status_code;
  2: i16 error_code;
  3: list<string> error_msgs;
}

enum TLocationEnforcement {
  MUST,
  PREFERRED,
  DONT_CARE
}

struct TResource {
  1: required TUniqueId            client_resource_id;
  2: required i16                  v_cpu_cores;
  3: required i32                  memory_mb;
  4: required string               askedLocation;
  5: required TLocationEnforcement enforcement;
}

struct TAllocatedResource {
  1: required TUniqueId reservation_id;
  2: required TUniqueId client_resource_id;
  3: required string    rm_resource_id;
  4: required i16       v_cpu_cores;
  5: required i32       memory_mb;
  6: required string    location;
}

struct TNodeCapacity {
  1: required i16 total_v_cpu_cores;
  2: required i32 total_memory_mb;
  3: required i16 free_v_cpu_cores;
  4: required i32 free_memory_mb;
}

////////////////////////////////////////////////////////////////////////////////
// Llama AM Service

struct TLlamaAMRegisterRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            client_id;
  3: required TNetworkAddress      notification_callback_service;
}

struct TLlamaAMRegisterResponse {
  1: required TStatus   status;
  2: optional TUniqueId am_handle;
}

struct TLlamaAMUnregisterRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId am_handle;
}

struct TLlamaAMUnregisterResponse {
  1: required TStatus status;
}

struct TLlamaAMReservationRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            am_handle;
  3: required string               user;
  4: optional string               queue;
  5: required list<TResource>      resources;
  6: required bool                 gang;
  7: optional TUniqueId            reservation_id;
}

struct TLlamaAMReservationResponse {
  1: required TStatus   status;
  2: optional TUniqueId reservation_id;
}

struct TLlamaAMReservationExpansionRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            am_handle;
  3: required TUniqueId            expansion_of;
  4: required TResource            resource;
  5: optional TUniqueId            expansion_id;
}

struct TLlamaAMReservationExpansionResponse {
  1: required TStatus   status;
  2: optional TUniqueId expansion_id;
}

struct TLlamaAMReleaseRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            am_handle;
  3: required TUniqueId            reservation_id;
}

struct TLlamaAMReleaseResponse {
  1: required TStatus status;
}

struct TLlamaAMGetNodesRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            am_handle;
}

struct TLlamaAMGetNodesResponse {
  1: required TStatus status;
  2: optional list<string> nodes;
}

service LlamaAMService {

  TLlamaAMRegisterResponse Register(1: TLlamaAMRegisterRequest request);

  TLlamaAMUnregisterResponse Unregister(1: TLlamaAMUnregisterRequest request);

  TLlamaAMReservationResponse Reserve(1: TLlamaAMReservationRequest request);

    TLlamaAMReservationExpansionResponse Expand(
    1: TLlamaAMReservationExpansionRequest request);

  TLlamaAMReleaseResponse Release(1: TLlamaAMReleaseRequest request);

  TLlamaAMGetNodesResponse GetNodes(1: TLlamaAMGetNodesRequest request);

}

////////////////////////////////////////////////////////////////////////////////
// Llama AM Admin Service

struct TLlamaAMAdminReleaseRequest {
  1: required TLlamaServiceVersion version;
  2: optional bool                 do_not_cache = false;
  3: optional list<string>         queues;
  4: optional list<TUniqueId>      handles;
  5: optional list<TUniqueId>      reservations;
}

struct TLlamaAMAdminReleaseResponse {
  1: required TStatus status;
}

struct TLlamaAMAdminEmptyCacheRequest {
  1: required TLlamaServiceVersion version;
  2: optional bool                 allQueues = false;
  3: optional list<string>         queues;
}

struct TLlamaAMAdminEmptyCacheResponse {
  1: required TStatus status;
}

service LlamaAMAdminService {

  TLlamaAMAdminReleaseResponse Release
  (1: TLlamaAMAdminReleaseRequest request);

  TLlamaAMAdminEmptyCacheResponse EmptyCache
  (1: TLlamaAMAdminEmptyCacheRequest request);

}

////////////////////////////////////////////////////////////////////////////////
// Llama NM Service

struct TLlamaNMRegisterRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            client_id;
  3: required TNetworkAddress      notification_callback_service;
}

struct TLlamaNMRegisterResponse {
  1: required TStatus   status;
  2: optional TUniqueId nm_handle;
}

struct TLlamaNMUnregisterRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            nm_handle;
}

struct TLlamaNMUnregisterResponse {
  1: required TStatus status;
}

service LlamaNMService {

  TLlamaNMRegisterResponse Register(1: TLlamaNMRegisterRequest request);

  TLlamaNMUnregisterResponse Unregister(1: TLlamaNMUnregisterRequest request);

}

////////////////////////////////////////////////////////////////////////////////
// Llama Notification Callback Service

struct TLlamaAMNotificationRequest {
  1: required TLlamaServiceVersion     version;
  2: required TUniqueId                am_handle;
  3: required bool                     heartbeat;
  4: optional list<TUniqueId>          allocated_reservation_ids;
  5: optional list<TAllocatedResource> allocated_resources;
  6: optional list<TUniqueId>          rejected_reservation_ids;
  7: optional list<TUniqueId>          rejected_client_resource_ids;
  8: optional list<TUniqueId>          lost_client_resource_ids;
  9: optional list<TUniqueId>          preempted_reservation_ids;
  10: optional list<TUniqueId>         preempted_client_resource_ids;
  11: optional list<TUniqueId>         admin_released_reservation_ids;
  12: optional list<TUniqueId>         lost_reservation_ids;
}

struct TLlamaAMNotificationResponse {
  1: required TStatus status;
}

struct TLlamaNMNotificationRequest {
  1: required TLlamaServiceVersion version;
  2: required TUniqueId            nm_handle;
  3: required TNodeCapacity        node_capacity;
  4: list<string>                  preempted_rm_resource_ids;
}

struct TLlamaNMNotificationResponse {
  1: required TStatus status;
}

service LlamaNotificationService {

  TLlamaAMNotificationResponse AMNotification(
    1: TLlamaAMNotificationRequest request);

  TLlamaNMNotificationResponse NMNotification(
    1: TLlamaNMNotificationRequest request);
}

////////////////////////////////////////////////////////////////////////////////
