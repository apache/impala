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

#include <boost/foreach.hpp>
#include <boost/algorithm/string/join.hpp>

#include "common/status.h"
#include "util/debug-util.h"

#include "common/names.h"

namespace impala {

// NOTE: this is statically initialized and we must be very careful what
// functions these constructors call.  In particular, we cannot call
// glog functions which also rely on static initializations.
// TODO: is there a more controlled way to do this.
const Status Status::CANCELLED(ErrorMsg::Init(TErrorCode::CANCELLED, "Cancelled"));

const Status Status::DEPRECATED_RPC(ErrorMsg::Init(TErrorCode::NOT_IMPLEMENTED_ERROR,
    "Deprecated RPC; please update your client"));

Status Status::MemLimitExceeded() {
  return Status(TErrorCode::MEM_LIMIT_EXCEEDED, "Memory limit exceeded");
}

Status::Status(TErrorCode::type code)
    : msg_(new ErrorMsg(code)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0)
    : msg_(new ErrorMsg(code, arg0)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1)
    : msg_(new ErrorMsg(code, arg0, arg1)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}
Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6,
    arg7)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6,
     arg7, arg8)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(TErrorCode::type code, const ArgType& arg0, const ArgType& arg1,
    const ArgType& arg2, const ArgType& arg3, const ArgType& arg4,
    const ArgType& arg5, const ArgType& arg6, const ArgType& arg7,
    const ArgType& arg8, const ArgType& arg9)
    : msg_(new ErrorMsg(code, arg0, arg1, arg2, arg3, arg4, arg5, arg6,
    arg7, arg8, arg9)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(const string& error_msg)
  : msg_(new ErrorMsg(TErrorCode::GENERAL, error_msg)) {
  VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
}

Status::Status(const string& error_msg, bool silent)
  : msg_(new ErrorMsg(TErrorCode::GENERAL, error_msg)) {
  if (!silent) {
    VLOG(1) << msg_->msg() << "\n" << GetStackTrace();
  }
}

Status::Status(const ErrorMsg& message)
  : msg_(new ErrorMsg(message)) { }

Status::Status(const TStatus& status)
  : msg_(status.status_code == TErrorCode::OK
      ? NULL : new ErrorMsg(status.status_code, status.error_msgs)) { }

Status& Status::operator=(const TStatus& status) {
  delete msg_;
  if (status.status_code == TErrorCode::OK) {
    msg_ = NULL;
  } else {
    msg_ = new ErrorMsg(status.status_code, status.error_msgs);
  }
  return *this;
}

Status::Status(const apache::hive::service::cli::thrift::TStatus& hs2_status)
  : msg_(
      hs2_status.statusCode
        == apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS ? NULL
          : new ErrorMsg(
              static_cast<TErrorCode::type>(hs2_status.statusCode),
              hs2_status.errorMessage)) {
}

Status& Status::operator=(
    const apache::hive::service::cli::thrift::TStatus& hs2_status) {
  delete msg_;
  if (hs2_status.statusCode
        == apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS) {
    msg_ = NULL;
  } else {
    msg_ = new ErrorMsg(
        static_cast<TErrorCode::type>(hs2_status.statusCode), hs2_status.errorMessage);
  }
  return *this;
}

Status Status::Expected(const std::string& error_msg) {
  return Status(error_msg, true);
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
    BOOST_FOREACH(const string& s, status.msg_->details()) {
      msg_->AddDetail(s);
    }
  }
}

const string Status::GetDetail() const {
  return msg_ != NULL ? msg_->GetFullMessageDetails() : "";
}

void Status::ToThrift(TStatus* status) const {
  status->error_msgs.clear();
  if (msg_ == NULL) {
    status->status_code = TErrorCode::OK;
  } else {
    status->status_code = msg_->error();
    status->error_msgs.push_back(msg_->msg());
    BOOST_FOREACH(const string& s, msg_->details()) {
      status->error_msgs.push_back(s);
    }
    status->__isset.error_msgs = true;
  }
}

}
