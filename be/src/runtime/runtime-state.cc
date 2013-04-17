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

#include <string>
#include <sstream>

#include "common/logging.h"
#include <boost/algorithm/string/join.hpp>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/timestamp-value.h"
#include "runtime/data-stream-recvr.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/jni-util.h"

#include <jni.h>
#include <iostream>

DECLARE_int32(max_errors);

using namespace boost;
using namespace llvm;
using namespace std;
using namespace boost::algorithm;

namespace impala {

RuntimeState::RuntimeState(
    const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
    const string& now, ExecEnv* exec_env)
  : obj_pool_(new ObjectPool()),
    data_stream_recvrs_pool_(new ObjectPool()),
    unreported_error_idx_(0),
    profile_(obj_pool_.get(), "Fragment " + PrintId(fragment_instance_id)),
    fragment_mem_limit_(NULL),
    is_cancelled_(false) {
  Status status = Init(fragment_instance_id, query_options, now, exec_env);
  DCHECK(status.ok());
}

RuntimeState::RuntimeState(const std::string& now)
  : obj_pool_(new ObjectPool()),
    data_stream_recvrs_pool_(new ObjectPool()),
    unreported_error_idx_(0),
    profile_(obj_pool_.get(), "<unnamed>"),
    fragment_mem_limit_(NULL) {
  query_options_.batch_size = DEFAULT_BATCH_SIZE;
  now_.reset(new TimestampValue(now.c_str(), now.size()));
}

RuntimeState::~RuntimeState() {
}

Status RuntimeState::Init(
    const TUniqueId& fragment_instance_id, const TQueryOptions& query_options,
    const string& now, ExecEnv* exec_env) {
  fragment_instance_id_ = fragment_instance_id;
  query_options_ = query_options;
  now_.reset(new TimestampValue(now.c_str(), now.size()));
  exec_env_ = exec_env;
  if (!query_options.disable_codegen) {
    RETURN_IF_ERROR(CreateCodegen());
  } else {
    codegen_.reset(NULL);
  }
  if (query_options_.max_errors <= 0) {
    // TODO: fix linker error and uncomment this
    //query_options_.max_errors = FLAGS_max_errors;
    query_options_.max_errors = 100;
  }
  if (query_options_.batch_size <= 0) {
    query_options_.batch_size = DEFAULT_BATCH_SIZE;
  }
  if (query_options_.max_io_buffers <= 0) {
    // TODO: how to tune this?
    query_options_.max_io_buffers = 5 * DiskInfo::num_disks();
  }
  
  DCHECK_GT(query_options_.max_io_buffers, 0);
  DCHECK_GE(query_options_.num_scanner_threads, 0);
  return Status::OK;
}

DataStreamRecvr* RuntimeState::CreateRecvr(
    const RowDescriptor& row_desc, PlanNodeId dest_node_id, int num_senders,
    int buffer_size, RuntimeProfile* profile) {
  DataStreamRecvr* recvr = exec_env_->stream_mgr()->CreateRecvr(row_desc,
      fragment_instance_id_, dest_node_id, num_senders, buffer_size, profile);
  lock_guard<mutex> l(data_stream_recvrs_lock_);
  data_stream_recvrs_pool_->Add(recvr);
  return recvr;
}

void RuntimeState::set_now(const TimestampValue* now) {
  now_.reset(new TimestampValue(*now));
}

Status RuntimeState::CreateCodegen() {
  RETURN_IF_ERROR(LlvmCodeGen::LoadImpalaIR(obj_pool_.get(), &codegen_));
  codegen_->EnableOptimizations(true);
  profile_.AddChild(codegen_->runtime_profile());
  return Status::OK;
}

bool RuntimeState::ErrorLogIsEmpty() {
  lock_guard<mutex> l(error_log_lock_);
  return (error_log_.size() > 0);
}

string RuntimeState::ErrorLog() {
  lock_guard<mutex> l(error_log_lock_);
  return join(error_log_, "\n");
}

string RuntimeState::FileErrors() const {
  lock_guard<mutex> l(file_errors_lock_);
  stringstream out;
  for (int i = 0; i < file_errors_.size(); ++i) {
    out << file_errors_[i].second << " errors in " << file_errors_[i].first << endl;
  }
  return out.str();
}

void RuntimeState::ReportFileErrors(const std::string& file_name, int num_errors) {
  lock_guard<mutex> l(file_errors_lock_);
  file_errors_.push_back(make_pair(file_name, num_errors));
}

void RuntimeState::LogError(const string& error) {
  lock_guard<mutex> l(error_log_lock_);
  if (error_log_.size() < query_options_.max_errors) {
    error_log_.push_back(error);
  }
}

void RuntimeState::GetUnreportedErrors(vector<string>* new_errors) {
  lock_guard<mutex> l(error_log_lock_);
  if (unreported_error_idx_ < error_log_.size()) {
    new_errors->assign(error_log_.begin() + unreported_error_idx_, error_log_.end());
    unreported_error_idx_ = error_log_.size();
  }
}

}
