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

#pragma once

#include "common/global-types.h"
#include "common/status.h"
#include "util/aligned-new.h"
#include "util/sharded-query-map-util.h"
#include "util/thread-pool.h"

namespace impala {

class ExecQueryFInstancesRequestPB;
class QueryState;
class TExecPlanFragmentInfo;
class TQueryCtx;
class TUniqueId;

/// A daemon-wide registry and manager of QueryStates. This is the central entry
/// point for gaining refcounted access to a QueryState. It initiates query execution.
/// It also registers a callback function for updating of cluster membership. When
/// coordinators are absent from the active cluster membership list, it cancels all
/// the running fragments of the queries scheduled by the inactive coordinators.
/// Note that we have to hold the shard lock in order to increment the refcnt for a
/// QueryState safely.
/// Thread-safe.
class QueryExecMgr : public CacheLineAligned {
 public:
  QueryExecMgr();
  ~QueryExecMgr();

  /// Creates QueryState if it doesn't exist and initiates execution of all fragment
  /// instance for this query. All fragment instances hold a reference to their
  /// QueryState for the duration of their execution.
  ///
  /// Returns an error if there was some unrecoverable problem before any instance
  /// was started (like low memory). In that case, no QueryState is created.
  /// After this function returns, it is legal to call QueryState::Cancel(), regardless of
  /// the return value of this function.
  Status StartQuery(const ExecQueryFInstancesRequestPB* request,
      const TQueryCtx& query_ctx, const TExecPlanFragmentInfo& fragment_info);

  /// Creates a QueryState for the given query with the provided parameters. Only valid
  /// to call if the QueryState does not already exist. The caller must call
  /// ReleaseQueryState() with the returned QueryState to decrement the refcount.
  QueryState* CreateQueryState(const TQueryCtx& query_ctx, int64_t mem_limit);

  /// If a QueryState for the given query exists, increments that refcount and returns
  /// the QueryState, otherwise returns nullptr.
  QueryState* GetQueryState(const TUniqueId& query_id);

  /// Decrements the refcount for the given QueryState.
  void ReleaseQueryState(QueryState* qs);

  /// Takes a set of backend ids of active backends and cancels all the running
  /// fragments of the queries which are scheduled by failed coordinators (that
  /// is, ids not in the active set).
  void CancelQueriesForFailedCoordinators(
      const std::unordered_set<BackendIdPB>& current_membership);

  /// Work item for QueryExecMgr::cancellation_thread_pool_.
  /// This class needs to support move construction and assignment for use in ThreadPool.
  class QueryCancellationTask {
   public:
    // Empty constructor needed to make ThreadPool happy.
    QueryCancellationTask() : qs_(nullptr) {}
    QueryCancellationTask(QueryState* qs) : qs_(qs) {}

    QueryState* GetQueryState() const { return qs_; }

   private:
    // QueryState to be cancelled.
    QueryState* qs_;
  };

 private:

  typedef ShardedQueryMap<QueryState*> QueryStateMap;
  QueryStateMap qs_map_;

  /// Thread pool to process cancellation tasks for queries scheduled by failed
  /// coordinators to avoid blocking the statestore callback.
  /// Set thread pool size as 1 by default since the tasks are local function calls.
  std::unique_ptr<ThreadPool<QueryCancellationTask>> cancellation_thread_pool_;

  /// Gets the existing QueryState or creates a new one if not present.
  /// 'created' is set to true if it was created, false otherwise.
  /// Increments the refcount.
  QueryState* GetOrCreateQueryState(
      const TQueryCtx& query_ctx, int64_t mem_limit, bool* created);

  /// Execute instances and decrement refcount (acquire ownership of qs).
  /// Return only after all fragments complete unless an instances hit
  /// an error or the query is cancelled.
  void ExecuteQueryHelper(QueryState* qs);

  /// Increments the refcount for the given QueryState with caller holding the lock
  /// of the sharded QueryState map.
  void AcquireQueryStateLocked(QueryState* qs);

  /// Helper method to process cancellations that result from failed coordinators,
  /// called from the cancellation thread pool. The cancellation_task contains the
  /// QueryState to be cancelled.
  void CancelFromThreadPool(const QueryCancellationTask& cancellation_task);
};
}
