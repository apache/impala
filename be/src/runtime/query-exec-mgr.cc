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

#include <gperftools/malloc_extension.h>
#include <gutil/strings/substitute.h>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>

#include "common/logging.h"
#include "runtime/query-state.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/exec-env.h"
#include "runtime/mem-tracker.h"
#include "util/uid-util.h"
#include "util/thread.h"
#include "util/impalad-metrics.h"
#include "util/debug-util.h"

#include "common/names.h"

using namespace impala;

// TODO: this logging should go into a per query log.
DEFINE_int32(log_mem_usage_interval, 0, "If non-zero, impalad will output memory usage "
    "every log_mem_usage_interval'th fragment completion.");

Status QueryExecMgr::StartQuery(const TExecQueryFInstancesParams& params) {
  TUniqueId query_id = params.query_ctx.query_id;
  VLOG_QUERY << "StartQueryFInstances() query_id=" << PrintId(query_id)
             << " coord=" << params.query_ctx.coord_address;

  bool dummy;
  QueryState* qs = GetOrCreateQueryState(params.query_ctx, &dummy);
  Status status = qs->Init(params);
  if (!status.ok()) {
    qs->ReleaseExecResourceRefcount(); // Release refcnt acquired in Init().
    ReleaseQueryState(qs);
    return status;
  }
  // avoid blocking the rpc handler thread for too long by starting a new thread for
  // query startup (which takes ownership of the QueryState reference)
  unique_ptr<Thread> t;
  status = Thread::Create("query-exec-mgr",
      Substitute("start-query-finstances-$0", PrintId(query_id)),
          &QueryExecMgr::StartQueryHelper, this, qs, &t, true);
  if (!status.ok()) {
    // decrement refcount taken in QueryState::Init()
    qs->ReleaseExecResourceRefcount();
    // decrement refcount taken in GetOrCreateQueryState()
    ReleaseQueryState(qs);
    return status;
  }
  t->Detach();
  return Status::OK();
}

QueryState* QueryExecMgr::CreateQueryState(const TQueryCtx& query_ctx) {
  bool created;
  QueryState* qs = GetOrCreateQueryState(query_ctx, &created);
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
  VLOG_QUERY << "QueryState: query_id=" << query_id << " refcnt=" << refcnt;
  return qs;
}

QueryState* QueryExecMgr::GetOrCreateQueryState(
    const TQueryCtx& query_ctx, bool* created) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    ScopedShardedMapRef<QueryState*> map_ref(query_ctx.query_id,
        &ExecEnv::GetInstance()->query_exec_mgr()->qs_map_);
    DCHECK(map_ref.get() != nullptr);

    auto it = map_ref->find(query_ctx.query_id);
    if (it == map_ref->end()) {
      // register new QueryState
      qs = new QueryState(query_ctx);
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


void QueryExecMgr::StartQueryHelper(QueryState* qs) {
  qs->StartFInstances();

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
  qs->ReleaseExecResourceRefcount();
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
  VLOG_QUERY << "ReleaseQueryState(): query_id=" << PrintId(query_id)
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
    DCHECK_EQ(qs_from_map->query_ctx().query_id, query_id);
    int32_t cnt = qs_from_map->refcnt_.Load();
    DCHECK_GE(cnt, 0);
    // someone else might have increased the refcnt in the meantime
    if (cnt > 0) return;
    map_ref->erase(it);
  }
  // TODO: send final status report during gc, but do this from a different thread
  delete qs_from_map;
}
