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

#include "common/status.h"

#include <ostream>

#include <boost/algorithm/string/join.hpp>

#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/common.pb.h"
#include "util/debug-util.h"
#include "util/error-util.h"

#include "common/names.h"

using namespace apache::hive::service::cli::thrift;

namespace impala {

const char* Status::LLVM_CLASS_NAME = "class.impala::Status";

// NOTE: this is statically initialized and we must be very careful what
// functions these constructors call.  In particular, we cannot call
// glog functions which also rely on static initializations.
// TODO: is there a more controlled way to do this.
const Status Status::CANCELLED(ErrorMsg::Init(TErrorCode::CANCELLED));

const Status Status::DEPRECATED_RPC(ErrorMsg::Init(
    TErrorCode::NOT_IMPLEMENTED_ERROR, "Deprecated RPC; please update your client"));

Status Status::MemLimitExceeded() {
  return Status(ErrorMsg(TErrorCode::MEM_LIMIT_EXCEEDED, "Memory limit exceeded"), true);
}

Status Status::MemLimitExceeded(const std::string& details) {
  return Status(ErrorMsg(TErrorCode::MEM_LIMIT_EXCEEDED,
      Substitute("Memory limit exceeded: $0", details)), true);
}

Status Status::CancelledInternal(const char* subsystem) {
  return Status(ErrorMsg::Init(TErrorCode::CANCELLED_INTERNALLY, subsystem));
}

Status::Status(bool silent, TErrorCode::type code) : msg_(new ErrorMsg(code)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0)
  : msg_(new ErrorMsg(code, arg0)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(
    bool silent, TErrorCode::type code, const ArgType& arg0, const ArgType& arg1)
  : msg_(new ErrorMsg(code, arg0, arg1)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}
Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7, const ArgType& arg8)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(bool silent, TErrorCode::type code, const ArgType& arg0,
    const ArgType& arg1, const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7, const ArgType& arg8,
    const ArgType& arg9)
  : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9)) {
  if (!silent) VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(const string& error_msg)
  : msg_(new ErrorMsg(TErrorCode::GENERAL, error_msg)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(const ErrorMsg& error_msg, bool silent)
  : msg_(new ErrorMsg(error_msg)) {
  if (!silent) {
    VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
  }
}

Status::Status(const string& error_msg, bool silent)
  : msg_(new ErrorMsg(TErrorCode::GENERAL, error_msg)) {
  if (!silent) {
    VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
  }
}

Status::Status(const ErrorMsg& message)
  : msg_(new ErrorMsg(message)) { }

Status::Status(const TStatus& status) {
  FromThrift(status);
}

Status::Status(const StatusPB& status) {
  FromProto(status);
}

Status& Status::operator=(const TStatus& status) {
  delete msg_;
  FromThrift(status);
  return *this;
}

Status::Status(const apache::hive::service::cli::thrift::TStatus& hs2_status)
  : msg_(hs2_status.statusCode == TStatusCode::SUCCESS_STATUS
                || hs2_status.statusCode == TStatusCode::STILL_EXECUTING_STATUS ?
            NULL :
            new ErrorMsg(HS2TStatusCodeToTErrorCode(hs2_status.statusCode),
                hs2_status.errorMessage)) {}

Status Status::Expected(const ErrorMsg& error_msg) {
  return Status(error_msg, true);
}

Status Status::Expected(const std::string& error_msg) {
  return Status(error_msg, true);
}

Status Status::Expected(TErrorCode::type code) {
  return Status(true, code);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0) {
  return Status(true, code, arg0);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1) {
  return Status(true, code, arg0, arg1);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2) {
  return Status(true, code, arg0, arg1, arg2);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3) {
  return Status(true, code, arg0, arg1, arg2, arg3);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4, arg5);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4, arg5, arg6);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7, const ArgType& arg8) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8);
}

Status Status::Expected(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4, const ArgType& arg5,
    const ArgType& arg6, const ArgType& arg7, const ArgType& arg8, const ArgType& arg9) {
  return Status(true, code, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9);
}

void Status::AddDetail(const std::string& msg) {
  DCHECK(msg_ != NULL);
  msg_->AddDetail(msg);
  VLOG(2) << msg;
}

void Status::MergeStatus(const Status& status) {
  if (status.ok()) return;
  if (msg_ == NULL) {
    msg_ = new ErrorMsg(*status.msg_);
  } else {
    msg_->AddDetail(status.msg().msg());
    for (const string& s: status.msg_->details()) msg_->AddDetail(s);
  }
}

const string Status::GetDetail() const {
  return msg_ != NULL ? msg_->GetFullMessageDetails() : "";
}

void Status::ToThrift(TStatus* status) const {
  status->error_msgs.clear();
  if (msg_ == nullptr) {
    status->status_code = TErrorCode::OK;
  } else {
    status->status_code = msg_->error();
    status->error_msgs.push_back(msg_->msg());
    for (const string& s: msg_->details()) status->error_msgs.push_back(s);
    status->__isset.error_msgs = true;
  }
}

void Status::ToProto(StatusPB* status) const {
  status->Clear();
  if (msg_ == nullptr) {
    status->set_status_code(TErrorCode::OK);
  } else {
    status->set_status_code(msg_->error());
    status->add_error_msgs(msg_->msg());
    for (const string& s : msg_->details()) status->add_error_msgs(s);
  }
}

void Status::FromThrift(const TStatus& status) {
  if (status.status_code == TErrorCode::OK) {
    msg_ = NULL;
  } else {
    msg_ = new ErrorMsg();
    msg_->SetErrorCode(status.status_code);
    if (status.error_msgs.size() > 0) {
      // The first message is the actual error message. (See Status::ToThrift()).
      msg_->SetErrorMsg(status.error_msgs.front());
      // The following messages are details.
      std::for_each(status.error_msgs.begin() + 1, status.error_msgs.end(),
          [&](string const& detail) { msg_->AddDetail(detail); });
    }
  }
}

void Status::FromProto(const StatusPB& status) {
  if (status.status_code() == TErrorCode::OK) {
    msg_ = nullptr;
  } else {
    msg_ = new ErrorMsg();
    msg_->SetErrorCode(static_cast<TErrorCode::type>(status.status_code()));
    if (status.error_msgs().size() > 0) {
      // The first message is the actual error message. (See Status::ToThrift()).
      msg_->SetErrorMsg(status.error_msgs().Get(0));
      // The following messages are details.
      std::for_each(status.error_msgs().begin() + 1, status.error_msgs().end(),
          [&](string const& detail) { msg_->AddDetail(detail); });
    }
  }
}

void Status::FreeMessage() noexcept {
  delete msg_;
}

void Status::CopyMessageFrom(const Status& status) noexcept {
  delete msg_;
  msg_ = status.msg_ == NULL ? NULL : new ErrorMsg(*status.msg_);
}

ostream& operator<<(ostream& os, const Status& status) {
  os << _TErrorCode_VALUES_TO_NAMES.at(status.code());
  if (!status.ok()) os << ": " << status.GetDetail();
  return os;
}

}
