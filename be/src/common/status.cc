// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <boost/algorithm/string/join.hpp>

#include "common/status.h"

using namespace std;
using namespace boost::algorithm;

namespace impala {

const Status Status::OK;

Status::ErrorDetail::ErrorDetail(const TStatus& status)
  : error_code(status.status_code),
    error_msgs(status.error_msgs) {
  DCHECK_NE(error_code, TStatusCode::OK);
}

Status& Status::operator=(const Status& status) {
  delete error_detail_;
  if (status.error_detail_ == NULL) {
    error_detail_ = NULL;
  } else {
    error_detail_ = new ErrorDetail(*status.error_detail_);
  }
  return *this;
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

void Status::AddErrorMsg(TStatusCode::type code, const std::string& msg) {
  if (error_detail_ == NULL) {
    error_detail_ = new ErrorDetail(code, msg);
  } else {
    error_detail_->error_msgs.push_back(msg);
  }
  VLOG(1) << msg;
}

void Status::AddErrorMsg(const std::string& msg) {
  AddErrorMsg(TStatusCode::INTERNAL_ERROR, msg);
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
