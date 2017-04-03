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

#include <iostream>
#include <jni.h>
#include <sstream>
#include <string>

#include "common/logging.h"
#include <boost/algorithm/string/join.hpp>
#include <gutil/strings/substitute.h>

#include "codegen/llvm-codegen.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "exprs/scalar-fn-call.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-recvr.h"
#include "runtime/descriptors.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-state.h"
#include "runtime/runtime-filter-bank.h"
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

#include "common/names.h"

using namespace llvm;

DECLARE_int32(max_errors);

// The fraction of the query mem limit that is used for the block mgr. Operators
// that accumulate memory all use the block mgr so the majority of the memory should
// be allocated to the block mgr. The remaining memory is used by the non-spilling
// operators and should be independent of data size.
static const float BLOCK_MGR_MEM_FRACTION = 0.8f;

// The minimum amount of memory that must be left after the block mgr reserves the
// BLOCK_MGR_MEM_FRACTION. The block limit is:
// min(query_limit * BLOCK_MGR_MEM_FRACTION, query_limit - BLOCK_MGR_MEM_MIN_REMAINING)
// TODO: this value was picked arbitrarily and the tests are written to rely on this
// for the minimum memory required to run the query. Revisit.
static const int64_t BLOCK_MGR_MEM_MIN_REMAINING = 100 * 1024 * 1024;

namespace impala {

RuntimeState::RuntimeState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
    const TPlanFragmentInstanceCtx& instance_ctx, ExecEnv* exec_env)
  : desc_tbl_(nullptr),
    obj_pool_(new ObjectPool()),
    query_state_(query_state),
    fragment_ctx_(&fragment_ctx),
    instance_ctx_(&instance_ctx),
    now_(new TimestampValue(query_state->query_ctx().now_string.c_str(),
        query_state->query_ctx().now_string.size())),
    exec_env_(exec_env),
    profile_(obj_pool_.get(), "Fragment " + PrintId(instance_ctx.fragment_instance_id)),
    query_mem_tracker_(query_state_->query_mem_tracker()),
    instance_buffer_reservation_(nullptr),
    is_cancelled_(false),
    root_node_id_(-1) {
  Init();
}

RuntimeState::RuntimeState(
    const TQueryCtx& query_ctx, ExecEnv* exec_env, const std::string& request_pool)
  : obj_pool_(new ObjectPool()),
    query_state_(nullptr),
    fragment_ctx_(nullptr),
    instance_ctx_(nullptr),
    local_query_ctx_(query_ctx),
    now_(new TimestampValue(query_ctx.now_string.c_str(), query_ctx.now_string.size())),
    exec_env_(exec_env),
    profile_(obj_pool_.get(), "<unnamed>"),
    query_mem_tracker_(MemTracker::CreateQueryMemTracker(
        query_id(), query_options(), request_pool, obj_pool_.get())),
    instance_buffer_reservation_(nullptr),
    is_cancelled_(false),
    root_node_id_(-1) {
  Init();
}

RuntimeState::~RuntimeState() {
  DCHECK(instance_mem_tracker_ == nullptr) << "Must call ReleaseResources()";
}

void RuntimeState::Init() {
  SCOPED_TIMER(profile_.total_time_counter());

  // Register with the thread mgr
  resource_pool_ = exec_env_->thread_mgr()->RegisterPool();
  DCHECK(resource_pool_ != NULL);

  total_thread_statistics_ = ADD_THREAD_COUNTERS(runtime_profile(), "TotalThreads");
  total_storage_wait_timer_ = ADD_TIMER(runtime_profile(), "TotalStorageWaitTime");
  total_network_send_timer_ = ADD_TIMER(runtime_profile(), "TotalNetworkSendTime");
  total_network_receive_timer_ = ADD_TIMER(runtime_profile(), "TotalNetworkReceiveTime");

  instance_mem_tracker_.reset(new MemTracker(
      runtime_profile(), -1, runtime_profile()->name(), query_mem_tracker_));

  if (query_state_ != nullptr && exec_env_->buffer_pool() != nullptr) {
    instance_buffer_reservation_ = obj_pool_->Add(new ReservationTracker);
    instance_buffer_reservation_->InitChildTracker(&profile_,
        query_state_->buffer_reservation(), instance_mem_tracker_.get(),
        numeric_limits<int64_t>::max());
  }
}

void RuntimeState::InitFilterBank() {
  filter_bank_.reset(new RuntimeFilterBank(query_ctx(), this));
}

Status RuntimeState::CreateBlockMgr() {
  DCHECK(block_mgr_.get() == NULL);

  // Compute the max memory the block mgr will use.
  int64_t block_mgr_limit = query_mem_tracker_->lowest_limit();
  if (block_mgr_limit < 0) block_mgr_limit = numeric_limits<int64_t>::max();
  block_mgr_limit = min(static_cast<int64_t>(block_mgr_limit * BLOCK_MGR_MEM_FRACTION),
      block_mgr_limit - BLOCK_MGR_MEM_MIN_REMAINING);
  if (block_mgr_limit < 0) block_mgr_limit = 0;
  if (query_options().__isset.max_block_mgr_memory &&
      query_options().max_block_mgr_memory > 0) {
    block_mgr_limit = query_options().max_block_mgr_memory;
    LOG(WARNING) << "Block mgr mem limit: "
                 << PrettyPrinter::Print(block_mgr_limit, TUnit::BYTES);
  }

  RETURN_IF_ERROR(BufferedBlockMgr::Create(this, query_mem_tracker(),
      runtime_profile(), exec_env()->tmp_file_mgr(), block_mgr_limit,
      io_mgr()->max_read_buffer_size(), &block_mgr_));
  return Status::OK();
}

Status RuntimeState::CreateCodegen() {
  if (codegen_.get() != NULL) return Status::OK();
  // TODO: add the fragment ID to the codegen ID as well
  RETURN_IF_ERROR(LlvmCodeGen::CreateImpalaCodegen(this,
      instance_mem_tracker_.get(), PrintId(fragment_instance_id()), &codegen_));
  codegen_->EnableOptimizations(true);
  profile_.AddChild(codegen_->runtime_profile());
  return Status::OK();
}

Status RuntimeState::CodegenScalarFns() {
  for (ScalarFnCall* scalar_fn : scalar_fns_to_codegen_) {
    Function* fn;
    RETURN_IF_ERROR(scalar_fn->GetCodegendComputeFn(codegen_.get(), &fn));
  }
  return Status::OK();
}

string RuntimeState::ErrorLog() {
  lock_guard<SpinLock> l(error_log_lock_);
  return PrintErrorMapToString(error_log_);
}

void RuntimeState::GetErrors(ErrorLogMap* errors) {
  lock_guard<SpinLock> l(error_log_lock_);
  *errors = error_log_;
}

bool RuntimeState::LogError(const ErrorMsg& message, int vlog_level) {
  lock_guard<SpinLock> l(error_log_lock_);
  // All errors go to the log, unreported_error_count_ is counted independently of the
  // size of the error_log to account for errors that were already reported to the
  // coordinator
  VLOG(vlog_level) << "Error from query " << query_id() << ": " << message.msg();
  if (ErrorCount(error_log_) < query_options().max_errors) {
    AppendError(&error_log_, message);
    return true;
  }
  return false;
}

void RuntimeState::GetUnreportedErrors(ErrorLogMap* new_errors) {
  lock_guard<SpinLock> l(error_log_lock_);
  *new_errors = error_log_;
  // Reset all messages, but keep all already reported keys so that we do not report the
  // same errors multiple times.
  ClearErrorMap(error_log_);
}

Status RuntimeState::LogOrReturnError(const ErrorMsg& message) {
  DCHECK_NE(message.error(), TErrorCode::OK);
  // If either abort_on_error=true or the error necessitates execution stops
  // immediately, return an error status.
  if (abort_on_error() ||
      message.error() == TErrorCode::MEM_LIMIT_EXCEEDED ||
      message.error() == TErrorCode::CANCELLED) {
    return Status(message);
  }
  // Otherwise, add the error to the error log and continue.
  LogError(message);
  return Status::OK();
}

void RuntimeState::SetMemLimitExceeded(MemTracker* tracker,
    int64_t failed_allocation_size, const ErrorMsg* msg) {
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
  DCHECK(query_status_.IsMemLimitExceeded());
}

Status RuntimeState::CheckQueryState() {
  if (instance_mem_tracker_ != nullptr
      && UNLIKELY(instance_mem_tracker_->AnyLimitExceeded())) {
    SetMemLimitExceeded(instance_mem_tracker_.get());
  }
  return GetQueryStatus();
}

void RuntimeState::AcquireReaderContext(DiskIoRequestContext* reader_context) {
  boost::lock_guard<SpinLock> l(reader_contexts_lock_);
  reader_contexts_.push_back(reader_context);
}

void RuntimeState::UnregisterReaderContexts() {
  boost::lock_guard<SpinLock> l(reader_contexts_lock_);
  for (DiskIoRequestContext* context : reader_contexts_) {
    io_mgr()->UnregisterContext(context);
  }
  reader_contexts_.clear();
}

void RuntimeState::ReleaseResources() {
  UnregisterReaderContexts();
  if (desc_tbl_ != nullptr) desc_tbl_->ClosePartitionExprs(this);
  if (filter_bank_ != nullptr) filter_bank_->Close();
  if (resource_pool_ != nullptr) {
    exec_env_->thread_mgr()->UnregisterPool(resource_pool_);
  }
  block_mgr_.reset(); // Release any block mgr memory, if this is the last reference.
  codegen_.reset(); // Release any memory associated with codegen.

  // Release the reservation, which should be unused at the point.
  if (instance_buffer_reservation_ != nullptr) instance_buffer_reservation_->Close();

  // 'query_mem_tracker_' must be valid as long as 'instance_mem_tracker_' is so
  // delete 'instance_mem_tracker_' first.
  // LogUsage() walks the MemTracker tree top-down when the memory limit is exceeded, so
  // break the link between 'instance_mem_tracker_' and its parent before
  // 'instance_mem_tracker_' and its children are destroyed.
  instance_mem_tracker_->UnregisterFromParent();
  instance_mem_tracker_.reset();

  // If this RuntimeState owns 'query_mem_tracker_' it must deregister it.
  if (query_state_ == nullptr) query_mem_tracker_->UnregisterFromParent();
  query_mem_tracker_ = nullptr;
}

const std::string& RuntimeState::GetEffectiveUser() const {
  return impala::GetEffectiveUser(query_ctx().session);
}

ImpalaBackendClientCache* RuntimeState::impalad_client_cache() {
  return exec_env_->impalad_client_cache();
}

CatalogServiceClientCache* RuntimeState::catalogd_client_cache() {
  return exec_env_->catalogd_client_cache();
}

DiskIoMgr* RuntimeState::io_mgr() {
  return exec_env_->disk_io_mgr();
}

DataStreamMgr* RuntimeState::stream_mgr() {
  return exec_env_->stream_mgr();
}

HBaseTableFactory* RuntimeState::htable_factory() {
  return exec_env_->htable_factory();
}

const TQueryCtx& RuntimeState::query_ctx() const {
  return query_state_ != nullptr ? query_state_->query_ctx() : local_query_ctx_;
}

const TQueryOptions& RuntimeState::query_options() const {
  const TQueryCtx& query_ctx =
      query_state_ != nullptr ? query_state_->query_ctx() : local_query_ctx_;
  return query_ctx.client_request.query_options;
}

}
