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

#pragma once

#include "common/status.h"
#include "gen-cpp/Status_types.h"
#include "gen-cpp/TCLIService_types.h"

namespace impala {

class StatusPB;

/// Retains the TErrorCode value and the message.
Status StatusFromThrift(const TStatus& status);

/// Converts from StatusPB, a protobuf serialized version of Status.
Status StatusFromProto(const StatusPB& status);

/// Converts from HS2 TStatus, retaining the mapped TErrorCode and message.
Status StatusFromHS2Status(
    const apache::hive::service::cli::thrift::TStatus& hs2_status);

/// Convert into TStatus.
void StatusToThrift(const Status& status, TStatus* thrift_status);

/// Serialize into StatusPB.
void StatusToProto(const Status& status, StatusPB* proto_status);

/// Convert into TStatus. Call this if 'status_container' contains an optional TStatus
/// field named 'status'. This also sets status_container->__isset.status.
template <typename T>
void SetTStatus(const Status& status, T* status_container) {
  StatusToThrift(status, &status_container->status);
  status_container->__isset.status = true;
}

}
