// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.


#ifndef IMPALA_SERVICE_IMPALA_SERVER_INLINE_H
#define IMPALA_SERVICE_IMPALA_SERVER_INLINE_H

#include "service/impala-server.h"

namespace impala {

inline Status ImpalaServer::THandleIdentifierToTUniqueId(
    const apache::hive::service::cli::thrift::THandleIdentifier& handle,
    TUniqueId* unique_id, TUniqueId* secret) {
  if (handle.guid.length() != 16 || handle.secret.length() != 16) {
    std::stringstream ss;
    ss << "Malformed THandleIdentifier (guid size: " << handle.guid.length()
       << ", expected 16, secret size: " << handle.secret.length() << ", expected 16)";
    return Status(ss.str());
  }
  memcpy(&(unique_id->hi), handle.guid.c_str(), 8);
  memcpy(&(unique_id->lo), handle.guid.c_str() + 8, 8);
  memcpy(&(secret->hi), handle.secret.c_str(), 8);
  memcpy(&(secret->lo), handle.secret.c_str() + 8, 8);

  return Status::OK();
}

inline void ImpalaServer::TUniqueIdToTHandleIdentifier(
    const TUniqueId& unique_id, const TUniqueId& secret,
    apache::hive::service::cli::thrift::THandleIdentifier* handle) {
  char uuid[16];
  memcpy((void*)uuid, &unique_id.hi, 8);
  memcpy((void*)(uuid + 8), &unique_id.lo, 8);
  handle->guid.assign(uuid, 16);
  memcpy((void*)uuid, &secret.hi, 8);
  memcpy((void*)(uuid + 8), &secret.lo, 8);
  handle->secret.assign(uuid, 16);
}

}

#endif
