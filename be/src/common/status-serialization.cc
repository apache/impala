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

#include "common/status-serialization.h"

#include <algorithm>

#include "gen-cpp/common.pb.h"
#include "util/error-util.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

Status StatusFromThrift(const TStatus& status) {
  if (status.status_code == TErrorCode::OK) return Status::OK();

  ErrorMsg msg;
  msg.SetErrorCode(status.status_code);
  if (status.error_msgs.size() > 0) {
    // The first message is the actual error message. (See StatusToThrift()).
    msg.SetErrorMsg(status.error_msgs.front());
    // The following messages are details.
    std::for_each(status.error_msgs.begin() + 1, status.error_msgs.end(),
        [&](string const& detail) { msg.AddDetail(detail); });
  }
  return Status(msg);
}

Status StatusFromProto(const StatusPB& status) {
  if (status.status_code() == TErrorCode::OK) return Status::OK();

  ErrorMsg msg;
  msg.SetErrorCode(static_cast<TErrorCode::type>(status.status_code()));
  if (status.error_msgs().size() > 0) {
    // The first message is the actual error message. (See StatusToThrift()).
    msg.SetErrorMsg(status.error_msgs().Get(0));
    // The following messages are details.
    std::for_each(status.error_msgs().begin() + 1, status.error_msgs().end(),
        [&](string const& detail) { msg.AddDetail(detail); });
  }
  return Status(msg);
}

Status StatusFromHS2Status(
    const apache::hive::service::cli::thrift::TStatus& hs2_status) {
  if (hs2_status.statusCode == TStatusCode::SUCCESS_STATUS
      || hs2_status.statusCode == TStatusCode::STILL_EXECUTING_STATUS) {
    return Status::OK();
  }
  return Status(ErrorMsg(HS2TStatusCodeToTErrorCode(hs2_status.statusCode),
      hs2_status.errorMessage));
}

void StatusToThrift(const Status& status, TStatus* thrift_status) {
  thrift_status->error_msgs.clear();
  if (status.ok()) {
    thrift_status->status_code = TErrorCode::OK;
  } else {
    thrift_status->status_code = status.code();
    thrift_status->error_msgs.push_back(status.msg().msg());
    for (const string& s: status.msg().details()) {
      thrift_status->error_msgs.push_back(s);
    }
    thrift_status->__isset.error_msgs = true;
  }
}

void StatusToProto(const Status& status, StatusPB* proto_status) {
  proto_status->Clear();
  if (status.ok()) {
    proto_status->set_status_code(TErrorCode::OK);
  } else {
    proto_status->set_status_code(status.code());
    proto_status->add_error_msgs(status.msg().msg());
    for (const string& s : status.msg().details()) proto_status->add_error_msgs(s);
  }
}

}
