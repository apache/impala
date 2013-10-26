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

#include <boost/algorithm/string/join.hpp>

#include "common/status.h"
#include "util/debug-util.h"

using namespace std;
using namespace boost::algorithm;

namespace impala {

// NOTE: this is statically initialized and we must be very careful what
// functions these constructors call.  In particular, we cannot call
// glog functions which also rely on static initializations.
// TODO: is there a more controlled way to do this.
const Status Status::OK;
const Status Status::CANCELLED(TStatusCode::CANCELLED, "Cancelled", true);
const Status Status::MEM_LIMIT_EXCEEDED(
    TStatusCode::MEM_LIMIT_EXCEEDED, "Memory limit exceeded", true);
const Status Status::DEPRECATED_RPC(TStatusCode::NOT_IMPLEMENTED_ERROR,
    "Deprecated RPC; please update your client", true);

Status::ErrorDetail::ErrorDetail(const TStatus& status)
  : error_code(status.status_code),
    error_msgs(status.error_msgs) {
  DCHECK_NE(error_code, TStatusCode::OK);
}

Status::Status(const string& error_msg, bool quiet)
  : error_detail_(new ErrorDetail(TStatusCode::INTERNAL_ERROR, error_msg)) {
  if (!quiet) VLOG(1) << error_msg << endl << GetStackTrace();
}

Status::Status(const TStatus& status)
  : error_detail_(
      status.status_code == TStatusCode::OK
        ? NULL
        : new ErrorDetail(status)) {
}

Status& Status::operator=(const TStatus& status) {
  delete error_detail_;
  if (status.status_code == TStatusCode::OK) {
    error_detail_ = NULL;
  } else {
    error_detail_ = new ErrorDetail(status);
  }
  return *this;
}

Status::Status(const apache::hive::service::cli::thrift::TStatus& hs2_status)
  : error_detail_(
      hs2_status.statusCode
        == apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS ? NULL
          : new ErrorDetail(
              static_cast<TStatusCode::type>(hs2_status.statusCode),
              hs2_status.errorMessage)) {
}

Status& Status::operator=(
    const apache::hive::service::cli::thrift::TStatus& hs2_status) {
  delete error_detail_;
  if (hs2_status.statusCode
        == apache::hive::service::cli::thrift::TStatusCode::SUCCESS_STATUS) {
    error_detail_ = NULL;
  } else {
    error_detail_ = new ErrorDetail(
        static_cast<TStatusCode::type>(hs2_status.statusCode), hs2_status.errorMessage);
  }
  return *this;
}

void Status::AddErrorMsg(TStatusCode::type code, const std::string& msg) {
  if (error_detail_ == NULL) {
    error_detail_ = new ErrorDetail(code, msg);
  } else {
    error_detail_->error_msgs.push_back(msg);
  }
  VLOG(2) << msg;
}

void Status::AddErrorMsg(const std::string& msg) {
  AddErrorMsg(TStatusCode::INTERNAL_ERROR, msg);
}

void Status::AddError(const Status& status) {
  if (status.ok()) return;
  AddErrorMsg(status.code(), status.GetErrorMsg());
}

void Status::GetErrorMsgs(vector<string>* msgs) const {
  msgs->clear();
  if (error_detail_ != NULL) {
    *msgs = error_detail_->error_msgs;
  }
}

void Status::GetErrorMsg(string* msg) const {
  msg->clear();
  if (error_detail_ != NULL) {
    *msg = join(error_detail_->error_msgs, "\n");
  }
}

string Status::GetErrorMsg() const {
  string msg;
  GetErrorMsg(&msg);
  return msg;
}

void Status::ToThrift(TStatus* status) const {
  status->error_msgs.clear();
  if (error_detail_ == NULL) {
    status->status_code = TStatusCode::OK;
  } else {
    status->status_code = error_detail_->error_code;
    for (int i = 0; i < error_detail_->error_msgs.size(); ++i) {
      status->error_msgs.push_back(error_detail_->error_msgs[i]);
    }
    status->__isset.error_msgs = !error_detail_->error_msgs.empty();
  }
}

}
