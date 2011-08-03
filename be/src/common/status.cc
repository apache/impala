// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <boost/algorithm/string/join.hpp>

#include "common/status.h"

using namespace std;
using namespace boost::algorithm;

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
  : error_detail_(
      status.error_detail_ != NULL
        ? new ErrorDetail(*status.error_detail_)
        : NULL) {
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

}
