// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifndef SPARROW_UTIL_H
#define SPARROW_UTIL_H

#include <string>
#include <vector>

#include <boost/format.hpp>
#include <boost/unordered_map.hpp>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/SparrowTypes_types.h"

namespace sparrow {

class TUpdateStateRequest;

// Identifier for services
typedef char const* ServiceId;

// Identifer for a StateStoreSubscriber.
typedef int SubscriberId;

// Identifier for a particular subscription to the state store. Note that there is not
// a one to one mapping between subscriptions and subscribers; a given subscriber
// may facilitate more than one subscription to the state store.
typedef int SubscriptionId;

// Describes a set of running instances.  The key is the ID of the StateStoreSubscriber
// associated with the running instance, and the value is the network address of the
// running instance.
typedef boost::unordered_map<SubscriberId, impala::THostPort> Membership;

// Information about a particular service, including the full membership information
// and a delta for objects.
struct ServiceState {
  Membership membership;
  std::vector<TVersionedObject> object_updates;
  std::vector<std::string> deleted_object_keys;

  bool operator==(const ServiceState& that) const;
};

// Mapping of service ids to the corresponding state.
typedef boost::unordered_map<std::string, ServiceState> ServiceStateMap;

// Converts a TUpdateStateRequest to a ServiceStateMap.
void StateFromThrift(const TUpdateStateRequest& request, ServiceStateMap* state);

// Converts a Membership to a list of TServiceInstances.
void MembershipToThrift(const Membership& from_membership,
                        std::vector<TServiceInstance>* to_membership);

// Sets the status in the given Thrift response to be an error message, specifying that
// the given required field is not set.
template<typename T>
void SetInvalidRequest(T* response, const std::string& missing_field_name) {
  response->status.status_code = impala::TStatusCode::INTERNAL_ERROR;
  boost::format error_format =
      boost::format("Invalid Thrift request: %1% not set") % missing_field_name;
  response->status.error_msgs.push_back(error_format.str());
  response->__isset.status = true;
};

// Checks if the given field of the given request is set, and if not, sets the
// status in the response to describe the missing field and returns.
#define RETURN_IF_UNSET(request, field, response) \
  do { \
    if (UNLIKELY(!request.__isset.field)) { \
      SetInvalidRequest(&response, #field); \
      return; \
    } \
  } while (false)

// Returns and sets the status in the response to be TStatusCode::OK.
#define RETURN_AND_SET_STATUS_OK(response) \
  do { \
    response.status.status_code = TStatusCode::OK; \
    response.__isset.status = true; \
    return; \
  } while (false)

// Returns and sets the status in the response to be an error, with the given
// error message.
#define RETURN_AND_SET_ERROR(error_msg, response) \
  do { \
    Status status(error_msg); \
    status.ToThrift(&response.status); \
    LOG(ERROR) << error_msg; \
    return; \
  } while (false)

}

#endif
