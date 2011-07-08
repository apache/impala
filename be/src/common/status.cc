// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include "common/status.h"

using namespace std;

namespace impala {

struct Status::ErrorDetail {
  vector<string> error_msgs;

  ErrorDetail(const string& msg): error_msgs(1, msg) {}
};

const Status Status::OK;

Status::Status(const string& error_msg)
  : error_detail_(new ErrorDetail(error_msg)) {
}

Status::Status(const Status& status)
  : error_detail_(status.error_detail_) {
}

Status::~Status() {
  if (error_detail_ != NULL) delete error_detail_;
}

void Status::GetErrorMsgs(vector<string>* msgs) const {
  msgs->clear();
  if (error_detail_ != NULL) {
    *msgs = error_detail_->error_msgs;
  }
}

void Status::GetErrorMsg(string* msg) const {
}

}
