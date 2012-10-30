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


#ifndef SPARROW_UTIL_H
#define SPARROW_UTIL_H

#include <sstream>
#include <string>
#include <vector>

#include <boost/unordered_map.hpp>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gen-cpp/Status_types.h"
#include "gen-cpp/SparrowTypes_types.h"

namespace sparrow {

class TUpdateStateRequest;

// Identifier for services
typedef std::string ServiceId;

// Identifer for a StateStoreSubscriber.
typedef int SubscriberId;

// Identifier for a particular subscription to the state store. Note that there is not
// a one to one mapping between subscriptions and subscribers; a given subscriber
// may facilitate more than one subscription to the state store.
typedef std::string SubscriptionId;

// Subscribers can use this SubscriptionId to distinguish between a not-yet-made
// subscription and an active one.
extern const SubscriptionId INVALID_SUBSCRIPTION_ID;

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
  std::stringstream error_message;
  error_message << "Invalid Thrift request: " << missing_field_name << " not set";
  response->status.error_msgs.push_back(error_message.str());
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
