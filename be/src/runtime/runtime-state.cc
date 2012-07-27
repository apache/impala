// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <string>
#include <sstream>

#include <boost/algorithm/string/join.hpp>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "util/jni-util.h"

#include <jni.h>
#include <iostream>

using namespace llvm;
using namespace std;
using namespace boost::algorithm;

namespace impala {

RuntimeState::RuntimeState(
    const TUniqueId& fragment_id, bool abort_on_error, int max_errors, int batch_size,
    bool llvm_enabled, ExecEnv* exec_env)
  : obj_pool_(new ObjectPool()),
    batch_size_(batch_size > 0 ? batch_size : DEFAULT_BATCH_SIZE),
    file_buffer_size_(DEFAULT_FILE_BUFFER_SIZE),
    abort_on_error_(abort_on_error),
    max_errors_(max_errors),
    fragment_id_(fragment_id),
    exec_env_(exec_env),
    profile_(obj_pool_.get(), "RuntimeState"),
    is_cancelled_(false) {
  if (llvm_enabled) {
    Status status = CreateCodegen();
    DCHECK(status.ok()); // TODO better error handling
  }
}

RuntimeState::RuntimeState()
  : obj_pool_(new ObjectPool()),
    batch_size_(DEFAULT_BATCH_SIZE),
    file_buffer_size_(DEFAULT_FILE_BUFFER_SIZE),
    profile_(obj_pool_.get(), "RuntimeState") {
}

Status RuntimeState::Init(
    const TUniqueId& fragment_id, bool abort_on_error, int max_errors, bool llvm_enabled,
    ExecEnv* exec_env) {
  fragment_id_ = fragment_id;
  abort_on_error_ = abort_on_error;
  max_errors_ = max_errors_;
  exec_env_ = exec_env;
  if (llvm_enabled) {
    RETURN_IF_ERROR(CreateCodegen());
  } else {
    codegen_.reset(NULL);
  }
  return Status::OK;
}

Status RuntimeState::CreateCodegen() {
  RETURN_IF_ERROR(LlvmCodeGen::LoadImpalaIR(obj_pool_.get(), &codegen_));
  codegen_->EnableOptimizations(true);
  profile_.AddChild(codegen_->runtime_profile());
  return Status::OK;
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
