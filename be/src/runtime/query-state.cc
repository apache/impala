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

#include "exprs/expr.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/backend-client.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/thread.h"

#include "common/names.h"

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
    refcnt_(0),
    is_cancelled_(0) {
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

void QueryState::ReleaseResources() {
  DCHECK(!released_resources_);
  // Clean up temporary files.
  if (file_group_ != nullptr) file_group_->Close();
  // Release any remaining reservation.
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  // Avoid dangling reference from the parent of 'query_mem_tracker_'.
  if (query_mem_tracker_ != nullptr) query_mem_tracker_->UnregisterFromParent();
  if (desc_tbl_ != nullptr) desc_tbl_->ReleaseResources();
  released_resources_ = true;
}

QueryState::~QueryState() {
  DCHECK(released_resources_);
  DCHECK_EQ(refcnt_.Load(), 0);
}

Status QueryState::Init(const TExecQueryFInstancesParams& rpc_params) {
  // Starting a new query creates threads and consumes a non-trivial amount of memory.
  // If we are already starved for memory, fail as early as possible to avoid consuming
  // more resources.
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_mem_tracker = exec_env->process_mem_tracker();
  if (process_mem_tracker->LimitExceeded()) {
    string msg = Substitute(
        "Query $0 could not start because the backend Impala daemon "
        "is over its memory limit", PrintId(query_id()));
    RETURN_IF_ERROR(process_mem_tracker->MemLimitExceeded(NULL, msg, 0));
  }
  // Do buffer-pool-related setup if running in a backend test that explicitly created
  // the pool.
  if (exec_env->buffer_pool() != nullptr) RETURN_IF_ERROR(InitBufferPoolState());

  // don't copy query_ctx, it's large and we already did that in the c'tor
  rpc_params_.__set_coord_state_idx(rpc_params.coord_state_idx);
  TExecQueryFInstancesParams& non_const_params =
      const_cast<TExecQueryFInstancesParams&>(rpc_params);
  rpc_params_.fragment_ctxs.swap(non_const_params.fragment_ctxs);
  rpc_params_.__isset.fragment_ctxs = true;
  rpc_params_.fragment_instance_ctxs.swap(non_const_params.fragment_instance_ctxs);
  rpc_params_.__isset.fragment_instance_ctxs = true;

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
  int64_t query_mem_limit = query_mem_tracker_->limit();
  if (query_mem_limit == -1) query_mem_limit = numeric_limits<int64_t>::max();

  // TODO: IMPALA-3200: add a default upper bound to buffer pool memory derived from
  // query_mem_limit.
  int64_t max_reservation = numeric_limits<int64_t>::max();
  if (query_options().__isset.max_block_mgr_memory
      && query_options().max_block_mgr_memory > 0) {
    max_reservation = query_options().max_block_mgr_memory;
  }

  // TODO: IMPALA-3748: claim the query-wide minimum reservation.
  // For now, rely on exec nodes to grab their minimum reservation during Prepare().
  buffer_reservation_ = obj_pool_.Add(new ReservationTracker);
  buffer_reservation_->InitChildTracker(
      NULL, exec_env->buffer_reservation(), query_mem_tracker_, max_reservation);

  // TODO: once there's a mechanism for reporting non-fragment-local profiles,
  // should make sure to report this profile so it's not going into a black hole.
  RuntimeProfile* dummy_profile = obj_pool_.Add(new RuntimeProfile(&obj_pool_, "dummy"));
  // Only create file group if spilling is enabled.
  if (query_options().scratch_limit != 0 && !query_ctx_.disable_spilling) {
    file_group_ = obj_pool_.Add(
        new TmpFileMgr::FileGroup(exec_env->tmp_file_mgr(), exec_env->disk_io_mgr(),
            dummy_profile, query_id(), query_options().scratch_limit));
  }
  return Status::OK();
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  if (!instances_prepared_promise_.Get().ok()) return nullptr;
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
  DCHECK(fis == nullptr || fis->IsPrepared());

  // This will send a report even if we are cancelled.  If the query completed correctly
  // but fragments still need to be cancelled (e.g. limit reached), the coordinator will
  // be waiting for a final report and profile.
  TReportExecStatusParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_ctx().query_id);
  DCHECK(rpc_params().__isset.coord_state_idx);
  params.__set_coord_state_idx(rpc_params().coord_state_idx);

  if (fis != nullptr) {
    // create status for 'fis'
    params.instance_exec_status.emplace_back();
    params.__isset.instance_exec_status = true;
    TFragmentInstanceExecStatus& instance_status = params.instance_exec_status.back();
    instance_status.__set_fragment_instance_id(fis->instance_id());
    status.SetTStatus(&instance_status);
    instance_status.__set_done(done);

    if (fis->profile() != nullptr) {
      fis->profile()->ToThrift(&instance_status.profile);
      instance_status.__isset.profile = true;
    }

    // Only send updates to insert status if fragment is finished, the coordinator waits
    // until query execution is done to use them anyhow.
    RuntimeState* state = fis->runtime_state();
    if (done && (state->hdfs_files_to_move()->size() > 0
        || state->per_partition_status()->size() > 0)) {
      TInsertExecStatus insert_status;
      if (state->hdfs_files_to_move()->size() > 0) {
        insert_status.__set_files_to_move(*state->hdfs_files_to_move());
      }
      if (state->per_partition_status()->size() > 0) {
        insert_status.__set_per_partition_status(*state->per_partition_status());
      }
      params.__set_insert_exec_status(insert_status);
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
  for (int i = 0; i < 3; ++i) {
    Status client_status;
    ImpalaBackendConnection client(ExecEnv::GetInstance()->impalad_client_cache(),
        query_ctx().coord_address, &client_status);
    if (client_status.ok()) {
      rpc_status = client.DoRpc(&ImpalaBackendClient::ReportExecStatus, params, &res);
      if (rpc_status.ok()) break;
    }
    if (i < 2) SleepForMs(100);
  }
  Status result_status(res.status);
  if ((!rpc_status.ok() || !result_status.ok()) && instances_started) {
    // TODO: should we try to keep rpc_status for the final report? (but the final
    // report, following this Cancel(), may not succeed anyway.)
    // TODO: not keeping an error status here means that all instances might
    // abort with CANCELLED status, despite there being an error
    // TODO: Fix IMPALA-2990. Cancelling fragment instances here may cause query to
    // hang as the coordinator may not be aware of the cancellation.
    Cancel();
  }
}

Status QueryState::WaitForPrepare() {
  return instances_prepared_promise_.Get();
}

void QueryState::StartFInstances() {
  VLOG_QUERY << "StartFInstances(): query_id=" << PrintId(query_id())
      << " #instances=" << rpc_params_.fragment_instance_ctxs.size();
  DCHECK_GT(refcnt_.Load(), 0);

  // set up desc tbl
  DCHECK(query_ctx().__isset.desc_tbl);
  Status status = DescriptorTbl::Create(&obj_pool_, query_ctx().desc_tbl, &desc_tbl_);
  if (!status.ok()) {
    instances_prepared_promise_.Set(status);
    ReportExecStatusAux(true, status, nullptr, false);
    return;
  }
  VLOG_QUERY << "descriptor table for query=" << PrintId(query_id())
             << "\n" << desc_tbl_->DebugString();

  DCHECK_GT(rpc_params_.fragment_ctxs.size(), 0);
  TPlanFragmentCtx* fragment_ctx = &rpc_params_.fragment_ctxs[0];
  int fragment_ctx_idx = 0;
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
    fis_map_.emplace(fis->instance_id(), fis);

    // update fragment_map_
    vector<FragmentInstanceState*>& fis_list = fragment_map_[instance_ctx.fragment_idx];
    fis_list.push_back(fis);

    // start new thread to execute instance
    refcnt_.Add(1);  // decremented in ExecFInstance()
    string thread_name = Substitute(
        "exec-finstance (finst:$0)", PrintId(instance_ctx.fragment_instance_id));
    Thread t(FragmentInstanceState::FINST_THREAD_GROUP_NAME, thread_name,
        [this, fis]() { this->ExecFInstance(fis); });
    t.Detach();
  }

  // don't return until every instance is prepared and record the first non-OK
  // (non-CANCELLED if available) status
  Status prepare_status;
  for (auto entry: fis_map_) {
    Status instance_status = entry.second->WaitForPrepare();
    // don't wipe out an error in one instance with the resulting CANCELLED from
    // the remaining instances
    if (!instance_status.ok() && (prepare_status.ok() || prepare_status.IsCancelled())) {
      prepare_status = instance_status;
    }
  }
  instances_prepared_promise_.Set(prepare_status);
}

void QueryState::ExecFInstance(FragmentInstanceState* fis) {
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  VLOG_QUERY << "Executing instance. instance_id=" << PrintId(fis->instance_id())
      << " fragment_idx=" << fis->instance_ctx().fragment_idx
      << " per_fragment_instance_idx=" << fis->instance_ctx().per_fragment_instance_idx
      << " coord_state_idx=" << rpc_params().coord_state_idx
      << " #in-flight=" << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->value();
  Status status = fis->Exec();
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);
  VLOG_QUERY << "Instance completed. instance_id=" << PrintId(fis->instance_id())
      << " #in-flight=" << ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->value()
      << " status=" << status;
  // initiate cancellation if nobody has done so yet
  if (!status.ok()) Cancel();
  // decrement refcount taken in StartFInstances()
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(this);
}

void QueryState::Cancel() {
  VLOG_QUERY << "Cancel: query_id=" << query_id();
  (void) instances_prepared_promise_.Get();
  if (!is_cancelled_.CompareAndSwap(0, 1)) return;
  for (auto entry: fis_map_) entry.second->Cancel();
}

void QueryState::PublishFilter(int32_t filter_id, int fragment_idx,
    const TBloomFilter& thrift_bloom_filter) {
  if (!instances_prepared_promise_.Get().ok()) return;
  DCHECK_EQ(fragment_map_.count(fragment_idx), 1);
  for (FragmentInstanceState* fis: fragment_map_[fragment_idx]) {
    fis->PublishFilter(filter_id, thrift_bloom_filter);
  }
}
