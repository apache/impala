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


#include "runtime/query-exec-mgr.h"

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>

#include "common/compiler-util.h"
#include "common/logging.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/control_service.pb.h"
#include "runtime/exec-env.h"
#include "runtime/query-state.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/impalad-metrics.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/thread-pool.h"

#include "common/names.h"

using namespace impala;

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

DEFINE_int32(query_exec_mgr_cancellation_thread_pool_size, 1,
    "(Advanced) Size of the QueryExecMgr thread-pool processing cancellations due to "
    "coordinator failure");

const uint32_t QUERY_EXEC_MGR_MAX_CANCELLATION_QUEUE_SIZE = 65536;

QueryExecMgr::QueryExecMgr() {
  // Initialise the cancellation thread pool with 1 thread (by default). The max queue
  // size is deliberately set so high that it should never fill; if it does we fill the
  // queue up to the maximum limit and ignore the rest. The ignored queries will get
  // cancelled when they time out trying to send status reports.
  cancellation_thread_pool_.reset(new ThreadPool<QueryCancellationTask>("query-exec-mgr",
      "cancellation-worker", FLAGS_query_exec_mgr_cancellation_thread_pool_size,
      QUERY_EXEC_MGR_MAX_CANCELLATION_QUEUE_SIZE,
      bind<void>(&QueryExecMgr::CancelFromThreadPool, this, _2)));
  ABORT_IF_ERROR(cancellation_thread_pool_->Init());
}

QueryExecMgr::~QueryExecMgr() {}

Status QueryExecMgr::StartQuery(const ExecQueryFInstancesRequestPB* request,
    const TQueryCtx& query_ctx, const TExecPlanFragmentInfo& fragment_info) {
  TUniqueId query_id = query_ctx.query_id;
  VLOG(2) << "StartQueryFInstances() query_id=" << PrintId(query_id)
          << " coord=" << TNetworkAddressToString(query_ctx.coord_address);
  bool dummy;
  QueryState* qs =
      GetOrCreateQueryState(query_ctx, request->per_backend_mem_limit(), &dummy);
  Status status = qs->Init(request, fragment_info);
  if (!status.ok()) {
    qs->ReleaseBackendResourceRefcount(); // Release refcnt acquired in Init().
    ReleaseQueryState(qs);
    return status;
  }
  // avoid blocking the rpc handler thread for too long by starting a new thread for
  // query startup (which takes ownership of the QueryState reference)
  unique_ptr<Thread> t;
  status = Thread::Create("query-exec-mgr",
      Substitute("query-state-$0", PrintId(query_id)),
          &QueryExecMgr::ExecuteQueryHelper, this, qs, &t, true);
  if (!status.ok()) {
    // decrement refcount taken in QueryState::Init()
    qs->ReleaseBackendResourceRefcount();
    // decrement refcount taken in GetOrCreateQueryState()
    ReleaseQueryState(qs);
    return status;
  }
  t->Detach();
  return Status::OK();
}

QueryState* QueryExecMgr::CreateQueryState(
    const TQueryCtx& query_ctx, int64_t mem_limit) {
  bool created;
  QueryState* qs = GetOrCreateQueryState(query_ctx, mem_limit, &created);
  DCHECK(created);
  return qs;
}

QueryState* QueryExecMgr::GetQueryState(const TUniqueId& query_id) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_id);
    if (it == map_ref->end()) return nullptr;
    qs = it->second;
    refcnt = qs->refcnt_.Add(1);
  }
  DCHECK(qs != nullptr && refcnt > 0);
  VLOG_QUERY << "QueryState: query_id=" << PrintId(query_id) << " refcnt=" << refcnt;
  return qs;
}

QueryState* QueryExecMgr::GetOrCreateQueryState(
    const TQueryCtx& query_ctx, int64_t mem_limit, bool* created) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_ctx.query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_ctx.query_id);
    if (it == map_ref->end()) {
      // Register new QueryState. This marks when the query first starts executing on
      // this backend.
      ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTED->Increment(1);
      ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTING->Increment(1);
      qs = new QueryState(query_ctx, mem_limit);
      map_ref->insert(make_pair(query_ctx.query_id, qs));
      *created = true;
    } else {
      qs = it->second;
      *created = false;
    }
    // decremented by ReleaseQueryState()
    refcnt = qs->refcnt_.Add(1);
  }
  DCHECK(qs != nullptr && refcnt > 0);
  return qs;
}


void QueryExecMgr::ExecuteQueryHelper(QueryState* qs) {
  // Start the query fragment instances and wait for completion or errors.
  if (LIKELY(qs->StartFInstances())) qs->MonitorFInstances();

#if !defined(ADDRESS_SANITIZER) && !defined(THREAD_SANITIZER)
  // tcmalloc and address or thread sanitizer cannot be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->GetValue();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif

  // decrement refcount taken in QueryState::Init();
  qs->ReleaseBackendResourceRefcount();
  // decrement refcount taken in StartQuery()
  ReleaseQueryState(qs);
}

void QueryExecMgr::ReleaseQueryState(QueryState* qs) {
  DCHECK(qs != nullptr);
  TUniqueId query_id = qs->query_id();
  int32_t cnt = qs->refcnt_.Add(-1);
  // don't reference anything from 'qs' beyond this point, 'qs' might get
  // gc'd out from under us
  qs = nullptr;
  VLOG(2) << "ReleaseQueryState(): query_id=" << PrintId(query_id)
          << " refcnt=" << cnt + 1;
  DCHECK_GE(cnt, 0);
  if (cnt > 0) return;

  QueryState* qs_from_map = nullptr;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_id);
    // someone else might have gc'd the entry
    if (it == map_ref->end()) return;
    qs_from_map = it->second;
    DCHECK(qs_from_map->query_ctx().query_id == query_id);
    int32_t cnt = qs_from_map->refcnt_.Load();
    DCHECK_GE(cnt, 0);
    // someone else might have increased the refcnt in the meantime
    if (cnt > 0) return;
    map_ref->erase(it);
  }
  delete qs_from_map;
  VLOG(1) << "ReleaseQueryState(): deleted query_id=" << PrintId(query_id);
  // BACKEND_NUM_QUERIES_EXECUTING is used to detect the backend being quiesced, so we
  // decrement it after we're completely done with the query.
  ImpaladMetrics::BACKEND_NUM_QUERIES_EXECUTING->Increment(-1);
}

void QueryExecMgr::AcquireQueryStateLocked(QueryState* qs) {
  if (qs == nullptr) return;
  int refcnt = qs->refcnt_.Add(1);
  DCHECK(refcnt > 0);
}

void QueryExecMgr::CancelQueriesForFailedCoordinators(
    const std::unordered_set<BackendIdPB>& current_membership) {
  // Build a list of queries that are scheduled by failed coordinators (as
  // evidenced by their absence from the cluster membership list).
  std::vector<QueryCancellationTask> to_cancel;
  ExecEnv::GetInstance()->query_exec_mgr()->qs_map_.DoFuncForAllEntries(
      [&](QueryState* qs) {
        if (qs != nullptr && !qs->IsCancelled()) {
          if (current_membership.find(qs->GetCoordinatorBackendId())
              == current_membership.end()) {
            // decremented by ReleaseQueryState()
            AcquireQueryStateLocked(qs);
            to_cancel.push_back(QueryCancellationTask(qs));
          }
        }
      });

  // Since we are the only producer for the cancellation thread pool, we can find the
  // remaining capacity of the pool and submit the new cancellation requests without
  // blocking.
  int query_num_to_cancel = to_cancel.size();
  int remaining_queue_size = QUERY_EXEC_MGR_MAX_CANCELLATION_QUEUE_SIZE
      - cancellation_thread_pool_->GetQueueSize();
  if (query_num_to_cancel > remaining_queue_size) {
    // Fill the queue up to maximum limit, and ignore the rest which will get cancelled
    // eventually anyways when QueryState::ReportExecStatus() hits the timeout.
    LOG_EVERY_N(WARNING, 60) << "QueryExecMgr cancellation queue is full";
    query_num_to_cancel = remaining_queue_size;
    for (int i = query_num_to_cancel; i < to_cancel.size(); ++i) {
      ReleaseQueryState(to_cancel[i].GetQueryState());
    }
  }
  for (int i = 0; i < query_num_to_cancel; ++i) {
    cancellation_thread_pool_->Offer(to_cancel[i]);
  }
}

void QueryExecMgr::CancelFromThreadPool(const QueryCancellationTask& cancellation_task) {
  QueryState* qs = cancellation_task.GetQueryState();
  VLOG(1) << "CancelFromThreadPool(): cancel query " << qs->query_id();
  qs->Cancel();
  qs->is_coord_active_.Store(false);
  ReleaseQueryState(qs);
}
