// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

namespace cpp sparrow
namespace java com.cloudera.sparrow.thrift

include "SparrowTypes.thrift"
include "Status.thrift"
include "Types.thrift"

enum StateStoreServiceVersion {
  V1
}

struct TRegisterServiceRequest {
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Address where the StateStoreSubscriberService is running. Required in V1.
  2: optional Types.THostPort subscriber_address

  // Service running on the node. Multiple services can be registered using multiple
  // RegisterService() calls from the same subscriber. Currently, we assume that at most
  // one instance of a particular service will be registered with each subscriber.
  // Required in V1.
  3: optional string service_id
  4: optional Types.THostPort service_address
}

struct TRegisterServiceResponse {
  // Required in V1.
  1: optional Status.TStatus status
}

struct TUnregisterServiceRequest {
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Address of the subscriber. Required in V1.
  2: optional Types.THostPort subscriber_address

  // Service that should be unregistered. Required in V1.
  3: optional string service_id
}

struct TUnregisterServiceResponse {
  // Required in V1.
  1: optional Status.TStatus status
}

struct TRegisterSubscriptionRequest {
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Address where the StateStoreSubscriberService is running. Required in V1.
  2: optional Types.THostPort subscriber_address

  // Services for which updates should be pushed to the given subscriber. Required in V1.
  3: optional set<string> services
}

struct TRegisterSubscriptionResponse {
  // Required in V1.
  1: optional Status.TStatus status

  // Unique identifier for the subscription, which is needed to unsubscribe.
  // Required in V1.
  2: optional i64 subscription_id
}

struct TUnregisterSubscriptionRequest {
  1: required StateStoreServiceVersion protocol_version =
      StateStoreServiceVersion.V1

  // Address of the subscriber. Required in V1.
  2: optional Types.THostPort subscriber_address

  // Identifier for the subscription that should be unregistered. Required in V1.
  3: optional i64 subscription_id
}

struct TUnregisterSubscriptionResponse {
  // Required in V1.
  1: optional Status.TStatus status
}

// A repository and distribution mechanism for global system state. Stored state is not
// made persistent and is considered soft-state (i.e., it needs to be re-supplied
// when a StateStore restarts). Updates to the global state are distributed to
// subscribers asynchronously and with an arbitrary (but typically reasonably small)
// delay.
service StateStoreService {
  // Registers an instance of a service.
  TRegisterServiceResponse RegisterService(1: TRegisterServiceRequest request);

  // Unregisters an instance of a service.
  TUnregisterServiceResponse UnregisterService(1: TUnregisterServiceRequest request);

  // Registers to receive updates for a set of services.
  TRegisterSubscriptionResponse RegisterSubscription(
      1: TRegisterSubscriptionRequest request);

  // Unregisters the given subscription. A subscriber will be updated at most one more
  // time after unregistering.
  TUnregisterSubscriptionResponse UnregisterSubscription(
      1: TUnregisterSubscriptionRequest request);
}
