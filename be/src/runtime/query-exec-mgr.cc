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

Status QueryExecMgr::StartFInstance(const TExecPlanFragmentParams& params) {
  TUniqueId instance_id = params.fragment_instance_ctx.fragment_instance_id;
  VLOG_QUERY << "StartFInstance() instance_id=" << PrintId(instance_id)
             << " coord=" << params.query_ctx.coord_address;

  bool dummy;
  QueryState* qs = GetOrCreateQueryState(
      params.query_ctx, params.fragment_instance_ctx.request_pool, &dummy);
  DCHECK(params.__isset.fragment_ctx);
  DCHECK(params.__isset.fragment_instance_ctx);
  Status status = qs->Prepare();
  if (!status.ok()) {
    ReleaseQueryState(qs);
    return status;
  }

  FragmentInstanceState* fis = qs->obj_pool()->Add(new FragmentInstanceState(
      qs, params.fragment_ctx, params.fragment_instance_ctx, params.query_ctx.desc_tbl));
  // register instance before returning so that async Cancel() calls can
  // find the instance
  qs->RegisterFInstance(fis);
  // start new thread to execute instance
  Thread t("query-exec-mgr",
      Substitute("exec-fragment-instance-$0", PrintId(instance_id)),
      &QueryExecMgr::ExecFInstance, this, fis);
  t.Detach();

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(1L);
  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->Increment(1L);
  return Status::OK();
}

QueryState* QueryExecMgr::CreateQueryState(
    const TQueryCtx& query_ctx, const string& request_pool) {
  bool created;
  QueryState* qs = GetOrCreateQueryState(query_ctx, request_pool, &created);
  DCHECK(created);
  return qs;
}

QueryState* QueryExecMgr::GetOrCreateQueryState(
    const TQueryCtx& query_ctx, const string& request_pool, bool* created) {
  QueryState* qs = nullptr;
  int refcnt;
  {
    lock_guard<mutex> l(qs_map_lock_);
    auto it = qs_map_.find(query_ctx.query_id);
    if (it == qs_map_.end()) {
      // register new QueryState
      qs = new QueryState(query_ctx, request_pool);
      qs_map_.insert(make_pair(query_ctx.query_id, qs));
      VLOG_QUERY << "new QueryState: query_id=" << query_ctx.query_id;
      *created = true;
    } else {
      qs = it->second;
      *created = false;
    }
    // decremented at the end of ExecFInstance()
    refcnt = qs->refcnt_.Add(1);
  }
  DCHECK(qs != nullptr && qs->refcnt_.Load() > 0);
  VLOG_QUERY << "QueryState: query_id=" << query_ctx.query_id << " refcnt=" << refcnt;
  return qs;
}

void QueryExecMgr::ExecFInstance(FragmentInstanceState* fis) {
  fis->Exec();

  ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS_IN_FLIGHT->Increment(-1L);
  VLOG_QUERY << "Instance completed. instance_id=" << PrintId(fis->instance_id());

#ifndef ADDRESS_SANITIZER
  // tcmalloc and address sanitizer can not be used together
  if (FLAGS_log_mem_usage_interval > 0) {
    uint64_t num_complete = ImpaladMetrics::IMPALA_SERVER_NUM_FRAGMENTS->value();
    if (num_complete % FLAGS_log_mem_usage_interval == 0) {
      char buf[2048];
      // This outputs how much memory is currently being used by this impalad
      MallocExtension::instance()->GetStats(buf, 2048);
      LOG(INFO) << buf;
    }
  }
#endif

  // decrement refcount taken in StartFInstance()
  ReleaseQueryState(fis->query_state());
}

QueryState* QueryExecMgr::GetQueryState(const TUniqueId& query_id) {
  VLOG_QUERY << "GetQueryState(): query_id=" << PrintId(query_id);
  lock_guard<mutex> l(qs_map_lock_);
  auto it = qs_map_.find(query_id);
  if (it == qs_map_.end()) return nullptr;
  QueryState* qs = it->second;
  int32_t cnt = qs->refcnt_.Add(1);
  DCHECK_GT(cnt, 0);
  return qs;
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
    // for now, gc right away
    lock_guard<mutex> l(qs_map_lock_);
    auto it = qs_map_.find(query_id);
    // someone else might have gc'd the entry
    if (it == qs_map_.end()) return;
    qs_from_map = it->second;
    DCHECK_EQ(qs_from_map->query_ctx().query_id, query_id);
    int32_t cnt = qs_from_map->refcnt_.Load();
    DCHECK_GE(cnt, 0);
    // someone else might have increased the refcnt in the meantime
    if (cnt > 0) return;
    qs_map_.erase(it);
  }
  // TODO: send final status report during gc, but do this from a different thread
  qs_from_map->ReleaseResources();
  delete qs_from_map;
}
