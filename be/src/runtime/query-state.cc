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

#include "runtime/query-state.h"

#include <boost/thread/lock_guard.hpp>
#include <boost/thread/locks.hpp>

#include "common/thread-debug-info.h"
#include "exprs/expr.h"
#include "runtime/backend-client.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/bufferpool/reservation-util.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/initial-reservations.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/runtime-state.h"
#include "runtime/scanner-mem-limiter.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "common/names.h"

DEFINE_int32(report_status_retry_interval_ms, 100,
    "The interval in milliseconds to wait before retrying a failed status report RPC to "
    "the coordinator.");

using namespace impala;

QueryState::ScopedRef::ScopedRef(const TUniqueId& query_id) {
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr() != nullptr);
  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
}

QueryState::ScopedRef::~ScopedRef() {
  if (query_state_ == nullptr) return;
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
}

QueryState::QueryState(const TQueryCtx& query_ctx, const string& request_pool)
  : query_ctx_(query_ctx),
    exec_resource_refcnt_(0),
    refcnt_(0),
    is_cancelled_(0),
    query_spilled_(0) {
  if (query_ctx_.request_pool.empty()) {
    // fix up pool name for tests
    DCHECK(!request_pool.empty());
    const_cast<TQueryCtx&>(query_ctx_).request_pool = request_pool;
  }
  TQueryOptions& query_options =
      const_cast<TQueryOptions&>(query_ctx_.client_request.query_options);
  // max_errors does not indicate how many errors in total have been recorded, but rather
  // how many are distinct. It is defined as the sum of the number of generic errors and
  // the number of distinct other errors.
  if (query_options.max_errors <= 0) {
    query_options.max_errors = 100;
  }
  if (query_options.batch_size <= 0) {
    query_options.__set_batch_size(DEFAULT_BATCH_SIZE);
  }
  InitMemTrackers();
}

void QueryState::ReleaseExecResources() {
  DCHECK(!released_exec_resources_);
  // Clean up temporary files.
  if (file_group_ != nullptr) file_group_->Close();
  // Release any remaining reservation.
  if (initial_reservations_ != nullptr) initial_reservations_->ReleaseResources();
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  if (desc_tbl_ != nullptr) desc_tbl_->ReleaseResources();
  // Mark the query as finished on the query MemTracker so that admission control will
  // not consider the whole query memory limit to be "reserved".
  query_mem_tracker_->set_query_exec_finished();
  // At this point query execution should not be consuming any resources but some tracked
  // memory may still be used by the ClientRequestState for result caching. The query
  // MemTracker will be closed later when this QueryState is torn down.
  released_exec_resources_ = true;
}

QueryState::~QueryState() {
  DCHECK_EQ(refcnt_.Load(), 0);
  DCHECK_EQ(exec_resource_refcnt_.Load(), 0);
  DCHECK(released_exec_resources_);
  if (query_mem_tracker_ != nullptr) {
    // Disconnect the query MemTracker hierarchy from the global hierarchy. After this
    // point nothing must touch this query's MemTracker and all tracked memory associated
    // with the query must be released. The whole query subtree of MemTrackers can
    // therefore be safely destroyed.
    query_mem_tracker_->CloseAndUnregisterFromParent();
  }
}

Status QueryState::Init(const TExecQueryFInstancesParams& rpc_params) {
  // Decremented in QueryExecMgr::StartQueryHelper() on success or by the caller of
  // Init() on failure. We need to do this before any returns because Init() always
  // returns a resource refcount to its caller.
  AcquireExecResourceRefcount();

  // Starting a new query creates threads and consumes a non-trivial amount of memory.
  // If we are already starved for memory, fail as early as possible to avoid consuming
  // more resources.
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_mem_tracker = exec_env->process_mem_tracker();
  if (process_mem_tracker->LimitExceeded(MemLimit::HARD)) {
    string msg = Substitute(
        "Query $0 could not start because the backend Impala daemon "
        "is over its memory limit", PrintId(query_id()));
    RETURN_IF_ERROR(process_mem_tracker->MemLimitExceeded(NULL, msg, 0));
  }

  RETURN_IF_ERROR(InitBufferPoolState());

  // don't copy query_ctx, it's large and we already did that in the c'tor
  rpc_params_.__set_coord_state_idx(rpc_params.coord_state_idx);
  TExecQueryFInstancesParams& non_const_params =
      const_cast<TExecQueryFInstancesParams&>(rpc_params);
  rpc_params_.fragment_ctxs.swap(non_const_params.fragment_ctxs);
  rpc_params_.__isset.fragment_ctxs = true;
  rpc_params_.fragment_instance_ctxs.swap(non_const_params.fragment_instance_ctxs);
  rpc_params_.__isset.fragment_instance_ctxs = true;

  instances_prepared_barrier_.reset(
      new CountingBarrier(rpc_params_.fragment_instance_ctxs.size()));
  instances_finished_barrier_.reset(
      new CountingBarrier(rpc_params_.fragment_instance_ctxs.size()));

  // Claim the query-wide minimum reservation. Do this last so that we don't need
  // to handle releasing it if a later step fails.
  initial_reservations_ = obj_pool_.Add(new InitialReservations(&obj_pool_,
      buffer_reservation_, query_mem_tracker_,
      rpc_params.initial_mem_reservation_total_claims));
  RETURN_IF_ERROR(
      initial_reservations_->Init(query_id(), rpc_params.min_mem_reservation_bytes));
  scanner_mem_limiter_ = obj_pool_.Add(new ScannerMemLimiter);
  return Status::OK();
}

void QueryState::InitMemTrackers() {
  const string& pool = query_ctx_.request_pool;
  int64_t bytes_limit = -1;
  if (query_options().__isset.mem_limit && query_options().mem_limit > 0) {
    bytes_limit = query_options().mem_limit;
    VLOG_QUERY << "Using query memory limit from query options: "
               << PrettyPrinter::Print(bytes_limit, TUnit::BYTES);
  }
  query_mem_tracker_ =
      MemTracker::CreateQueryMemTracker(query_id(), query_options(), pool, &obj_pool_);
}

Status QueryState::InitBufferPoolState() {
  ExecEnv* exec_env = ExecEnv::GetInstance();
  int64_t mem_limit = query_mem_tracker_->GetLowestLimit(MemLimit::HARD);
  int64_t max_reservation;
  if (query_options().__isset.buffer_pool_limit
      && query_options().buffer_pool_limit > 0) {
    max_reservation = query_options().buffer_pool_limit;
  } else if (mem_limit == -1) {
    // No query mem limit. The process-wide reservation limit is the only limit on
    // reservations.
    max_reservation = numeric_limits<int64_t>::max();
  } else {
    DCHECK_GE(mem_limit, 0);
    max_reservation = ReservationUtil::GetReservationLimitFromMemLimit(mem_limit);
  }
  VLOG_QUERY << "Buffer pool limit for " << PrintId(query_id()) << ": " << max_reservation;

  buffer_reservation_ = obj_pool_.Add(new ReservationTracker);
  buffer_reservation_->InitChildTracker(
      NULL, exec_env->buffer_reservation(), query_mem_tracker_, max_reservation);

  // TODO: once there's a mechanism for reporting non-fragment-local profiles,
  // should make sure to report this profile so it's not going into a black hole.
  RuntimeProfile* dummy_profile = RuntimeProfile::Create(&obj_pool_, "dummy");
  // Only create file group if spilling is enabled.
  if (query_options().scratch_limit != 0 && !query_ctx_.disable_spilling) {
    file_group_ = obj_pool_.Add(
        new TmpFileMgr::FileGroup(exec_env->tmp_file_mgr(), exec_env->disk_io_mgr(),
            dummy_profile, query_id(), query_options().scratch_limit));
  }
  return Status::OK();
}

const char* QueryState::BackendExecStateToString(const BackendExecState& state) {
  static const unordered_map<BackendExecState, const char*> exec_state_to_str{
      {BackendExecState::PREPARING, "PREPARING"},
      {BackendExecState::EXECUTING, "EXECUTING"},
      {BackendExecState::FINISHED, "FINISHED"},
      {BackendExecState::CANCELLED, "CANCELLED"},
      {BackendExecState::ERROR, "ERROR"}};

  return exec_state_to_str.at(state);
}

inline bool QueryState::IsTerminalState(const BackendExecState& state) {
  return state == BackendExecState::FINISHED
      || state == BackendExecState::CANCELLED
      || state == BackendExecState::ERROR;
}

Status QueryState::UpdateBackendExecState() {
  BackendExecState old_state = backend_exec_state_;

  unique_lock<SpinLock> l(status_lock_);
  // We shouldn't call this function if we're already in a terminal state.
  DCHECK(!IsTerminalState(backend_exec_state_))
      << " Current State: " << BackendExecStateToString(backend_exec_state_)
      << " | Current Status: " << query_status_.GetDetail();

  if (query_status_.IsCancelled()) {
    // Received cancellation - go to CANCELLED state.
    backend_exec_state_ = BackendExecState::CANCELLED;
  } else if (!query_status_.ok()) {
    // Error while executing - go to ERROR state.
    backend_exec_state_ = BackendExecState::ERROR;
  } else {
    // Transition to the next state in the lifecycle.
    backend_exec_state_ = old_state == BackendExecState::PREPARING ?
        BackendExecState::EXECUTING : BackendExecState::FINISHED;
  }
  return query_status_;
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  if (!WaitForPrepare().ok()) return nullptr;
  auto it = fis_map_.find(instance_id);
  return it != fis_map_.end() ? it->second : nullptr;
}

void QueryState::ReportExecStatus(bool done, const Status& status,
    FragmentInstanceState* fis) {
  ReportExecStatusAux(done, status, fis, true);
}

void QueryState::ReportExecStatusAux(bool done, const Status& status,
    FragmentInstanceState* fis, bool instances_started) {
  // if we're reporting an error, we're done
  DCHECK(status.ok() || done);
  // if this is not for a specific fragment instance, we're reporting an error
  DCHECK(fis != nullptr || !status.ok());

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_ctx().query_id);
  DCHECK(rpc_params().__isset.coord_state_idx);
  params.__set_coord_state_idx(rpc_params().coord_state_idx);
  status.SetTStatus(&params);

  if (fis != nullptr) {
    // create status for 'fis'
    params.instance_exec_status.emplace_back();
    params.__isset.instance_exec_status = true;
    TFragmentInstanceExecStatus& instance_status = params.instance_exec_status.back();
    instance_status.__set_fragment_instance_id(fis->instance_id());
    status.SetTStatus(&instance_status);
    instance_status.__set_done(done);
    instance_status.__set_current_state(fis->current_state());

    DCHECK(fis->profile() != nullptr);
    fis->profile()->ToThrift(&instance_status.profile);
    instance_status.__isset.profile = true;

    // Only send updates to insert status if fragment is finished, the coordinator waits
    // until query execution is done to use them anyhow.
    RuntimeState* state = fis->runtime_state();
    if (done && state->dml_exec_state()->ToThrift(&params.insert_exec_status)) {
      params.__isset.insert_exec_status = true;
    }
    // Send new errors to coordinator
    state->GetUnreportedErrors(&params.error_log);
    params.__isset.error_log = (params.error_log.size() > 0);
  }

  Status rpc_status;
  TReportExecStatusResult res;
  DCHECK_EQ(res.status.status_code, TErrorCode::OK);
  // Try to send the RPC 3 times before failing. Sleep for 100ms between retries.
  // It's safe to retry the RPC as the coordinator handles duplicate RPC messages.
  Status client_status;
  for (int i = 0; i < 3; ++i) {
    ImpalaBackendConnection client(ExecEnv::GetInstance()->impalad_client_cache(),
        query_ctx().coord_address, &client_status);
    if (client_status.ok()) {
      rpc_status = client.DoRpc(&ImpalaBackendClient::ReportExecStatus, params, &res);
      if (rpc_status.ok()) break;
    }
    if (i < 2) SleepForMs(FLAGS_report_status_retry_interval_ms);
  }
  Status result_status(res.status);
  if ((!client_status.ok() || !rpc_status.ok() || !result_status.ok()) &&
      instances_started) {
    // TODO: should we try to keep rpc_status for the final report? (but the final
    // report, following this Cancel(), may not succeed anyway.)
    // TODO: not keeping an error status here means that all instances might
    // abort with CANCELLED status, despite there being an error
    // TODO: Fix IMPALA-2990. Cancelling fragment instances without sending the
    // ReporExecStatus RPC may cause query to hang as the coordinator may not be aware
    // of the cancellation. Remove the log statements once IMPALA-2990 is fixed.
    if (!client_status.ok()) {
      LOG(ERROR) << "Cancelling fragment instances due to failure to obtain a connection "
                 << "to the coordinator. (" << client_status.GetDetail()
                 << "). Query " << PrintId(query_id()) << " may hang. See IMPALA-2990.";
    } else if (!rpc_status.ok()) {
      LOG(ERROR) << "Cancelling fragment instances due to failure to reach the "
                 << "coordinator. (" << rpc_status.GetDetail()
                 << "). Query " << PrintId(query_id()) << " may hang. See IMPALA-2990.";
    } else if (!result_status.ok()) {
      // If the ReportExecStatus RPC succeeded in reaching the coordinator and we get
      // back a non-OK status, it means that the coordinator expects us to cancel the
      // fragment instances for this query.
      LOG(INFO) << "Cancelling fragment instances as directed by the coordinator. "
                << "Returned status: " << result_status.GetDetail();
    }
    Cancel();
  }
}

Status QueryState::WaitForPrepare() {
  instances_prepared_barrier_->Wait();

  unique_lock<SpinLock> l(status_lock_);
  return query_status_;
}

Status QueryState::WaitForFinish() {
  instances_finished_barrier_->Wait();

  unique_lock<SpinLock> l(status_lock_);
  return query_status_;
}

void QueryState::StartFInstances() {
  VLOG_QUERY << "StartFInstances(): query_id=" << PrintId(query_id())
      << " #instances=" << rpc_params_.fragment_instance_ctxs.size();
  DCHECK_GT(refcnt_.Load(), 0);
  DCHECK_GT(exec_resource_refcnt_.Load(), 0) << "Should have been taken in Init()";

  // set up desc tbl
  DCHECK(query_ctx().__isset.desc_tbl);
  Status status = DescriptorTbl::Create(&obj_pool_, query_ctx().desc_tbl, &desc_tbl_);
  if (!status.ok()) {
    ErrorDuringPrepare(status, TUniqueId());
    Status updated_query_status = UpdateBackendExecState();
    instances_prepared_barrier_->NotifyRemaining();
    DCHECK(!updated_query_status.ok());
    // TODO (IMPALA-4063): This call to ReportExecStatusAux() should internally be handled
    // by UpdateBackendExecState().
    ReportExecStatusAux(true, status, nullptr, false);
    return;
  }
  VLOG_QUERY << "descriptor table for query=" << PrintId(query_id())
             << "\n" << desc_tbl_->DebugString();

  Status thread_create_status;
  DCHECK_GT(rpc_params_.fragment_ctxs.size(), 0);
  TPlanFragmentCtx* fragment_ctx = &rpc_params_.fragment_ctxs[0];
  int fragment_ctx_idx = 0;
  int num_unstarted_instances = rpc_params_.fragment_instance_ctxs.size();
  fragment_events_start_time_ = MonotonicStopWatch::Now();
  for (const TPlanFragmentInstanceCtx& instance_ctx: rpc_params_.fragment_instance_ctxs) {
    // determine corresponding TPlanFragmentCtx
    if (fragment_ctx->fragment.idx != instance_ctx.fragment_idx) {
      ++fragment_ctx_idx;
      DCHECK_LT(fragment_ctx_idx, rpc_params_.fragment_ctxs.size());
      fragment_ctx = &rpc_params_.fragment_ctxs[fragment_ctx_idx];
      // we expect fragment and instance contexts to follow the same order
      DCHECK_EQ(fragment_ctx->fragment.idx, instance_ctx.fragment_idx);
    }
    FragmentInstanceState* fis = obj_pool_.Add(
        new FragmentInstanceState(this, *fragment_ctx, instance_ctx));

    // start new thread to execute instance
    refcnt_.Add(1); // decremented in ExecFInstance()
    AcquireExecResourceRefcount(); // decremented in ExecFInstance()

    // Add the fragment instance ID to the 'fis_map_'.
    fis_map_.emplace(fis->instance_id(), fis);

    string thread_name = Substitute("$0 (finst:$1)",
        FragmentInstanceState::FINST_THREAD_NAME_PREFIX,
        PrintId(instance_ctx.fragment_instance_id));
    unique_ptr<Thread> t;

    // Inject thread creation failures through debug actions if enabled.
    Status debug_action_status = DebugAction(query_options(), "FIS_FAIL_THREAD_CREATION");
    thread_create_status = debug_action_status.ok() ?
        Thread::Create(FragmentInstanceState::FINST_THREAD_GROUP_NAME, thread_name,
            [this, fis]() { this->ExecFInstance(fis); }, &t, true) :
        debug_action_status;
    if (!thread_create_status.ok()) {
      fis_map_.erase(fis->instance_id());
      // Undo refcnt increments done immediately prior to Thread::Create(). The
      // reference counts were both greater than zero before the increments, so
      // neither of these decrements will free any structures.
      ReleaseExecResourceRefcount();
      ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
      break;
    }
    // update fragment_map_
    vector<FragmentInstanceState*>& fis_list = fragment_map_[instance_ctx.fragment_idx];
    fis_list.push_back(fis);
    t->Detach();
    --num_unstarted_instances;
  }

  if (!thread_create_status.ok()) {
    // We failed to start 'num_unstarted_instances', so make sure to notify
    // 'instances_prepared_barrier_' 'num_unstarted_instances - 1' times, to unblock
    // WaitForPrepare(). The last remaining notification will be set by the call to
    // ErrorDuringPrepare() below.
    while (num_unstarted_instances > 1) {
      DonePreparing();
      --num_unstarted_instances;
    }

    // We prioritize thread creation failure as a query killing error, even over an error
    // during Prepare() for a FIS.
    // We have to notify anyone waiting on WaitForPrepare() that this query has failed.
    ErrorDuringPrepare(thread_create_status, TUniqueId());
    Status updated_query_status = UpdateBackendExecState();
    DCHECK(!updated_query_status.ok());
    // Block until all the already started fragment instances finish Prepare()-ing to
    // to report an error.
    discard_result(WaitForPrepare());
    ReportExecStatusAux(true, thread_create_status, nullptr, true);
    return;
  }

  discard_result(WaitForPrepare());
  if (!UpdateBackendExecState().ok()) return;
  DCHECK(backend_exec_state_ == BackendExecState::EXECUTING)
      << BackendExecStateToString(backend_exec_state_);

  discard_result(WaitForFinish());
  if (!UpdateBackendExecState().ok()) return;
  DCHECK(backend_exec_state_ == BackendExecState::FINISHED)
      << BackendExecStateToString(backend_exec_state_);
}

void QueryState::AcquireExecResourceRefcount() {
  DCHECK(!released_exec_resources_);
  exec_resource_refcnt_.Add(1);
}

void QueryState::ReleaseExecResourceRefcount() {
  int32_t new_val = exec_resource_refcnt_.Add(-1);
  DCHECK_GE(new_val, 0);
  if (new_val == 0) ReleaseExecResources();
}

void QueryState::ExecFInstance(FragmentInstanceState* fis) {
  GetThreadDebugInfo()->SetInstanceId(fis->instance_id());

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  VLOG_QUERY << "Executing instance. instance_id=" << PrintId(fis->instance_id())
      << " fragment_idx=" << fis->instance_ctx().fragment_idx
      << " per_fragment_instance_idx=" << fis->instance_ctx().per_fragment_instance_idx
      << " coord_state_idx=" << rpc_params().coord_state_idx
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue();
  Status status = fis->Exec();
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);
  VLOG_QUERY << "Instance completed. instance_id=" << PrintId(fis->instance_id())
      << " #in-flight="
      << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->GetValue()
      << " status=" << status;
  // initiate cancellation if nobody has done so yet
  if (!status.ok()) Cancel();
  // decrement refcount taken in StartFInstances()
  ReleaseExecResourceRefcount();
  // decrement refcount taken in StartFInstances()
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
}

void QueryState::Cancel() {
  VLOG_QUERY << "Cancel: query_id=" << PrintId(query_id());
  discard_result(WaitForPrepare());
  if (!is_cancelled_.CompareAndSwap(0, 1)) return;
  for (auto entry: fis_map_) entry.second->Cancel();
}

void QueryState::PublishFilter(const TPublishFilterParams& params) {
  if (!WaitForPrepare().ok()) return;
  DCHECK_EQ(fragment_map_.count(params.dst_fragment_idx), 1);
  for (FragmentInstanceState* fis : fragment_map_[params.dst_fragment_idx]) {
    fis->PublishFilter(params);
  }
}

Status QueryState::StartSpilling(RuntimeState* runtime_state, MemTracker* mem_tracker) {
  // Return an error message with the root cause of why spilling is disabled.
  if (query_options().scratch_limit == 0) {
    return mem_tracker->MemLimitExceeded(
        runtime_state, "Could not free memory by spilling to disk: scratch_limit is 0");
  } else if (query_ctx_.disable_spilling) {
    return mem_tracker->MemLimitExceeded(runtime_state,
        "Could not free memory by spilling to disk: spilling was disabled by planner. "
        "Re-enable spilling by setting the query option DISABLE_UNSAFE_SPILLS=false");
  }
  // 'file_group_' must be non-NULL for spilling to be enabled.
  DCHECK(file_group_ != nullptr);
  if (query_spilled_.CompareAndSwap(0, 1)) {
    ImpaladMetrics::NUM_QUERIES_SPILLED->Increment(1);
  }
  return Status::OK();
}
