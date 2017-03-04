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
#include <kudu/client/client.h>

#include "exec/kudu-util.h"
#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/bufferpool/reservation-tracker.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/mem-tracker.h"
#include "runtime/query-exec-mgr.h"
#include "util/debug-util.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace impala;

struct QueryState::KuduClientPtr {
  kudu::client::sp::shared_ptr<kudu::client::KuduClient> kudu_client;
};

QueryState::ScopedRef::ScopedRef(const TUniqueId& query_id) {
  DCHECK(ExecEnv::GetInstance()->query_exec_mgr() != nullptr);
  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->GetQueryState(query_id);
}

QueryState::ScopedRef::~ScopedRef() {
  if (query_state_ == nullptr) return;
  ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
}

QueryState::QueryState(const TQueryCtx& query_ctx, const std::string& pool)
  : query_ctx_(query_ctx),
    refcnt_(0),
    prepared_(false),
    released_resources_(false),
    buffer_reservation_(nullptr),
    file_group_(nullptr) {
  TQueryOptions& query_options = query_ctx_.client_request.query_options;
  // max_errors does not indicate how many errors in total have been recorded, but rather
  // how many are distinct. It is defined as the sum of the number of generic errors and
  // the number of distinct other errors.
  if (query_options.max_errors <= 0) {
    query_options.max_errors = 100;
  }
  if (query_options.batch_size <= 0) {
    query_options.__set_batch_size(DEFAULT_BATCH_SIZE);
  }
  InitMemTrackers(pool);
}

void QueryState::ReleaseResources() {
  // Clean up temporary files.
  if (file_group_ != nullptr) file_group_->Close();
  // Release any remaining reservation.
  if (buffer_reservation_ != nullptr) buffer_reservation_->Close();
  // Avoid dangling reference from the parent of 'query_mem_tracker_'.
  if (query_mem_tracker_ != nullptr) query_mem_tracker_->UnregisterFromParent();
  released_resources_ = true;
}

QueryState::~QueryState() {
  DCHECK(released_resources_);
}

Status QueryState::Prepare() {
  lock_guard<SpinLock> l(prepare_lock_);
  if (prepared_) {
    DCHECK(prepare_status_.ok());
    return Status::OK();
  }
  RETURN_IF_ERROR(prepare_status_);

  Status status;
  // Starting a new query creates threads and consumes a non-trivial amount of memory.
  // If we are already starved for memory, fail as early as possible to avoid consuming
  // more resources.
  ExecEnv* exec_env = ExecEnv::GetInstance();
  MemTracker* process_mem_tracker = exec_env->process_mem_tracker();
  if (process_mem_tracker->LimitExceeded()) {
    string msg = Substitute("Query $0 could not start because the backend Impala daemon "
                            "is over its memory limit",
        PrintId(query_id()));
    status = process_mem_tracker->MemLimitExceeded(NULL, msg, 0);
    goto error;
  }
  // Do buffer-pool-related setup if running in a backend test that explicitly created
  // the pool.
  if (exec_env->buffer_pool() != nullptr) {
    status = InitBufferPoolState();
    if (!status.ok()) goto error;
  }
  prepared_ = true;
  return Status::OK();

error:
  prepare_status_ = status;
  return status;
}

void QueryState::InitMemTrackers(const std::string& pool) {
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

void QueryState::RegisterFInstance(FragmentInstanceState* fis) {
  VLOG_QUERY << "RegisterFInstance(): instance_id=" << PrintId(fis->instance_id());
  lock_guard<SpinLock> l(fis_map_lock_);
  DCHECK_EQ(fis_map_.count(fis->instance_id()), 0);
  fis_map_.insert(make_pair(fis->instance_id(), fis));
}

FragmentInstanceState* QueryState::GetFInstanceState(const TUniqueId& instance_id) {
  VLOG_FILE << "GetFInstanceState(): instance_id=" << PrintId(instance_id);
  lock_guard<SpinLock> l(fis_map_lock_);
  auto it = fis_map_.find(instance_id);
  return it != fis_map_.end() ? it->second : nullptr;
}

Status QueryState::GetKuduClient(const std::vector<std::string>& master_addresses,
                                 kudu::client::KuduClient** client) {
  std::string master_addr_concat = join(master_addresses, ",");
  lock_guard<SpinLock> l(kudu_client_map_lock_);
  auto kudu_client_map_it = kudu_client_map_.find(master_addr_concat);
  if (kudu_client_map_it == kudu_client_map_.end()) {
    // KuduClient doesn't exist, create it
    KuduClientPtr* kudu_client_ptr = new KuduClientPtr;
    RETURN_IF_ERROR(CreateKuduClient(master_addresses, &kudu_client_ptr->kudu_client));
    kudu_client_map_[master_addr_concat].reset(kudu_client_ptr);
    *client = kudu_client_ptr->kudu_client.get();
  } else {
    // Return existing KuduClient
    *client = kudu_client_map_it->second->kudu_client.get();
  }
  return Status::OK();
}
