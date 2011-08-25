// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <sstream>

#include <boost/algorithm/string/join.hpp>
#include "common/object-pool.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"

#include <iostream>

using namespace std;
using namespace boost::algorithm;

namespace impala {

RuntimeState::RuntimeState(const DescriptorTbl& descs, bool abort_on_error, int max_errors)
  : descs_(descs),
    obj_pool_(new ObjectPool()),
    batch_size_(DEFAULT_BATCH_SIZE),
    file_buf_size_(DEFAULT_FILE_BUF_SIZE),
    abort_on_error_(abort_on_error),
    max_errors_(max_errors) {
}

string RuntimeState::ErrorLog() const {
  return join(error_log_, "\n");
}

string RuntimeState::FileErrors() const {
  stringstream out;
  for (int i = 0; i < file_errors_.size(); ++i) {
    out << file_errors_[i].second << " errors in " << file_errors_[i].first << endl;
  }
  return out.str();
}

void RuntimeState::ReportFileErrors(const std::string& file_name, int num_errors) {
  file_errors_.push_back(make_pair(file_name, num_errors));
}

void RuntimeState::LogErrorStream() {
  error_log_.push_back(error_stream_.str());
  // Clear content of stream.
  error_stream_.str("");
  // Clear the ios error flags, if any.
  error_stream_.clear();
}

}
