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

#include "runtime/runtime-state.h"

#include <jni.h>
#include <iostream>
#include <sstream>
#include <string>

#include <boost/algorithm/string/join.hpp>
#include <gutil/strings/substitute.h>
#include "common/logging.h"

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-fn-call.h"
#include "exprs/timezone_db.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/krpc-data-stream-mgr.h"
#include "runtime/krpc-data-stream-recvr.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter-bank.h"
#include "runtime/thread-resource-mgr.h"
#include "runtime/timestamp-value.h"
#include "util/auth-util.h" // for GetEffectiveUser()
#include "util/bitmap.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/error-util.h"
#include "util/jni-util.h"
#include "util/mem-info.h"
#include "util/pretty-printer.h"
#include "util/test-info.h"

#include "common/names.h"

using strings::Substitute;

DECLARE_int32(max_errors);

namespace impala {

const char* RuntimeState::LLVM_CLASS_NAME = "class.impala::RuntimeState";

RuntimeState::RuntimeState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& instance_ctx, ExecEnv* exec_env)
  : query_state_(query_state),
    fragment_ctx_(&fragment_ctx),
    instance_ctx_(&instance_ctx),
    now_(new TimestampValue(TimestampValue::Parse(query_state->query_ctx().now_string))),
    utc_timestamp_(new TimestampValue(
        TimestampValue::Parse(query_state->query_ctx().utc_timestamp_string))),
    local_time_zone_(&TimezoneDatabase::GetUtcTimezone()),
    profile_(RuntimeProfile::Create(
        obj_pool(), "Fragment " + PrintId(instance_ctx.fragment_instance_id))),
    instance_buffer_reservation_(obj_pool()->Add(new ReservationTracker)) {
  Init();
}

// Constructor for standalone RuntimeState for test execution and fe-support.cc.
// Sets up a dummy local QueryState (with mem_limit picked up from the query options)
// to allow evaluating exprs, etc.
RuntimeState::RuntimeState(
    const TQueryCtx& qctx, ExecEnv* exec_env, DescriptorTbl* desc_tbl)
  : query_state_(new QueryState(qctx, qctx.client_request.query_options.__isset.mem_limit
                && qctx.client_request.query_options.mem_limit > 0 ?
            qctx.client_request.query_options.mem_limit :
            -1,
        "test-pool")),
    fragment_ctx_(nullptr),
    instance_ctx_(nullptr),
    local_query_state_(query_state_),
    now_(new TimestampValue(TimestampValue::Parse(qctx.now_string))),
    utc_timestamp_(new TimestampValue(TimestampValue::Parse(qctx.utc_timestamp_string))),
    local_time_zone_(&TimezoneDatabase::GetUtcTimezone()),
    profile_(RuntimeProfile::Create(obj_pool(), "<unnamed>")),
    instance_buffer_reservation_(nullptr) {
  // We may use execution resources while evaluating exprs, etc. Decremented in
  // ReleaseResources() to release resources.
  local_query_state_->AcquireBackendResourceRefcount();
  if (query_ctx().request_pool.empty()) {
    const_cast<TQueryCtx&>(query_ctx()).request_pool = "test-pool";
  }
  if (desc_tbl != nullptr) query_state_->desc_tbl_ = desc_tbl;
  Init();
}

RuntimeState::~RuntimeState() {
  DCHECK(released_resources_) << "Must call ReleaseResources()";
  // IMPALA-8270: run local_query_state_ destructor *before* other destructors so that
  // teardown order for the TestEnv/fe-support RuntimeState matches the teardown order
  // for the "real" RuntimeState. The previous divergence lead to hard-to-find bugs.
  if (local_query_state_ != nullptr) local_query_state_.reset();
}

void RuntimeState::Init() {
  SCOPED_TIMER(profile_->total_time_counter());

  // Register with the thread mgr
  resource_pool_ = ExecEnv::GetInstance()->thread_mgr()->CreatePool();
  DCHECK(resource_pool_ != nullptr);
  if (fragment_ctx_ != nullptr) {
    // Ensure that the planner correctly determined the required threads.
    resource_pool_->set_max_required_threads(fragment_ctx_->fragment.thread_reservation);
  }

  total_thread_statistics_ = ADD_THREAD_COUNTERS(runtime_profile(), "TotalThreads");
  total_storage_wait_timer_ = ADD_TIMER(runtime_profile(), "TotalStorageWaitTime");
  total_network_send_timer_ = ADD_TIMER(runtime_profile(), "TotalNetworkSendTime");
  total_network_receive_timer_ = ADD_TIMER(runtime_profile(), "TotalNetworkReceiveTime");

  instance_mem_tracker_ = obj_pool()->Add(new MemTracker(
      runtime_profile(), -1, runtime_profile()->name(), query_mem_tracker()));

  if (instance_buffer_reservation_ != nullptr) {
    instance_buffer_reservation_->InitChildTracker(profile_,
        query_state_->buffer_reservation(), instance_mem_tracker_,
        numeric_limits<int64_t>::max());
  }

  // Find local timezone.
  // (For FE tests leave 'local_time_zone_' as default. FE tests don't load the timezone
  // db since they don't need any timezone information.)
  if (!TestInfo::is_fe_test()) {
    const Timezone* tz = TimezoneDatabase::FindTimezone(query_ctx().local_time_zone);
    if (tz != nullptr) {
      local_time_zone_ = tz;
    } else {
      const string msg = Substitute(
          "Failed to find local timezone '$0'.Falling back to UTC.",
          query_ctx().local_time_zone);
      LOG(WARNING) << msg;
      LogError(ErrorMsg(TErrorCode::GENERAL, msg));
      local_time_zone_ = &TimezoneDatabase::GetUtcTimezone();
    }
  }
}

Status RuntimeState::InitFilterBank(long runtime_filters_reservation_bytes) {
  filter_bank_.reset(
      new RuntimeFilterBank(query_ctx(), this, runtime_filters_reservation_bytes));
  return filter_bank_->ClaimBufferReservation();
}

Status RuntimeState::CreateCodegen() {
  if (codegen_.get() != NULL) return Status::OK();
  // TODO: add the fragment ID to the codegen ID as well
  RETURN_IF_ERROR(LlvmCodeGen::CreateImpalaCodegen(this,
      instance_mem_tracker_, PrintId(fragment_instance_id()), &codegen_));
  codegen_->EnableOptimizations(true);
  profile_->AddChild(codegen_->runtime_profile());
  return Status::OK();
}

Status RuntimeState::CodegenScalarExprs() {
  for (auto& item : scalar_exprs_to_codegen_) {
    llvm::Function* fn;
    RETURN_IF_ERROR(item.first->GetCodegendComputeFn(codegen_.get(), item.second, &fn));
  }
  return Status::OK();
}

Status RuntimeState::StartSpilling(MemTracker* mem_tracker) {
  return query_state_->StartSpilling(this, mem_tracker);
}

string RuntimeState::ErrorLog() {
  lock_guard<SpinLock> l(error_log_lock_);
  return PrintErrorMapToString(error_log_);
}

bool RuntimeState::LogError(const ErrorMsg& message, int vlog_level) {
  lock_guard<SpinLock> l(error_log_lock_);
  // All errors go to the log, unreported_error_count_ is counted independently of the
  // size of the error_log to account for errors that were already reported to the
  // coordinator
  VLOG(vlog_level) << "Error from query " << PrintId(query_id()) << ": " << message.msg();
  if (ErrorCount(error_log_) < query_options().max_errors) {
    AppendError(&error_log_, message);
    return true;
  }
  return false;
}

void RuntimeState::GetUnreportedErrors(ErrorLogMapPB* new_errors) {
  new_errors->clear();
  lock_guard<SpinLock> l(error_log_lock_);
  for (const ErrorLogMap::value_type& v : error_log_) {
    (*new_errors)[v.first] = v.second;
  }
  // Reset all messages, but keep all already reported keys so that we do not report the
  // same errors multiple times.
  ClearErrorMap(error_log_);
}

Status RuntimeState::LogOrReturnError(const ErrorMsg& message) {
  DCHECK_NE(message.error(), TErrorCode::OK);
  // If either abort_on_error=true or the error necessitates execution stops
  // immediately, return an error status.
  if (abort_on_error()
      || message.error() == TErrorCode::CANCELLED
      || message.error() == TErrorCode::CANCELLED_INTERNALLY
      || message.error() == TErrorCode::MEM_LIMIT_EXCEEDED
      || message.error() == TErrorCode::INTERNAL_ERROR
      || message.error() == TErrorCode::DISK_IO_ERROR
      || message.error() == TErrorCode::THREAD_POOL_SUBMIT_FAILED
      || message.error() == TErrorCode::THREAD_POOL_TASK_TIMED_OUT) {
    return Status(message);
  }
  // Otherwise, add the error to the error log and continue.
  LogError(message);
  return Status::OK();
}

double RuntimeState::ComputeExchangeScanRatio() const {
  int64_t bytes_read = 0;
  for (const auto& c : bytes_read_counters_) bytes_read += c->value();
  if (bytes_read == 0) return 0;
  int64_t bytes_sent = 0;
  for (const auto& c : bytes_sent_counters_) bytes_sent += c->value();
  return (double)bytes_sent / bytes_read;
}

void RuntimeState::SetMemLimitExceeded(MemTracker* tracker,
    int64_t failed_allocation_size, const ErrorMsg* msg) {
  // Constructing the MemLimitExceeded and logging it is not cheap, so
  // avoid the cost if the query has already hit an error.
  // This is particularly important on the UDF codepath, because the UDF codepath
  // cannot abort the fragment immediately. It relies on callers checking status
  // periodically. This means that this function could be called a large number of times
  // (e.g. once per row) before the fragment aborts. See IMPALA-6997.
  {
    lock_guard<SpinLock> l(query_status_lock_);
    if (!query_status_.ok()) return;
  }
  Status status = tracker->MemLimitExceeded(this, msg == nullptr ? "" : msg->msg(),
      failed_allocation_size);
  {
    lock_guard<SpinLock> l(query_status_lock_);
    if (query_status_.ok()) query_status_ = status;
  }
  LogError(status.msg());
  // Add warning about missing stats except for compute stats child queries.
  if (!query_ctx().__isset.parent_query_id &&
      query_ctx().__isset.tables_missing_stats &&
      !query_ctx().tables_missing_stats.empty()) {
    LogError(ErrorMsg(TErrorCode::GENERAL,
        GetTablesMissingStatsWarning(query_ctx().tables_missing_stats)));
  }
}

Status RuntimeState::CheckQueryState() {
  DCHECK(instance_mem_tracker_ != nullptr);
  if (UNLIKELY(instance_mem_tracker_->AnyLimitExceeded(MemLimit::HARD))) {
    SetMemLimitExceeded(instance_mem_tracker_);
  }
  return GetQueryStatus();
}

void RuntimeState::ReleaseResources() {
  DCHECK(!released_resources_);
  if (filter_bank_ != nullptr) filter_bank_->Close();
  if (resource_pool_ != nullptr) {
    ExecEnv::GetInstance()->thread_mgr()->DestroyPool(move(resource_pool_));
  }
  // Release any memory associated with codegen.
  if (codegen_ != nullptr) codegen_->Close();

  // Release the reservation, which should be unused at the point.
  if (instance_buffer_reservation_ != nullptr) instance_buffer_reservation_->Close();

  // No more memory should be tracked for this instance at this point.
  if (instance_mem_tracker_->consumption() != 0) {
    LOG(WARNING) << "Query " << PrintId(query_id()) << " may have leaked memory." << endl
                 << instance_mem_tracker_->LogUsage(MemTracker::UNLIMITED_DEPTH);
  }
  instance_mem_tracker_->Close();

  if (local_query_state_.get() != nullptr) {
    local_query_state_->ReleaseBackendResourceRefcount();
  }
  released_resources_ = true;
}

const std::string& RuntimeState::GetEffectiveUser() const {
  return impala::GetEffectiveUser(query_ctx().session);
}

ObjectPool* RuntimeState::obj_pool() const {
  DCHECK(query_state_ != nullptr);
  return query_state_->obj_pool();
}

const TQueryCtx& RuntimeState::query_ctx() const {
  DCHECK(query_state_ != nullptr);
  return query_state_->query_ctx();
}

const DescriptorTbl& RuntimeState::desc_tbl() const {
  DCHECK(query_state_ != nullptr);
  return query_state_->desc_tbl();
}

const TQueryOptions& RuntimeState::query_options() const {
  return query_ctx().client_request.query_options;
}

MemTracker* RuntimeState::query_mem_tracker() {
  DCHECK(query_state_ != nullptr);
  return query_state_->query_mem_tracker();
}

}
