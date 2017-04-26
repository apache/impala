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

#ifndef IMPALA_RUNTIME_QUERY_STATE_H
#define IMPALA_RUNTIME_QUERY_STATE_H

#include <memory>
#include <unordered_map>
#include <boost/scoped_ptr.hpp>

#include "common/atomic.h"
#include "common/object-pool.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/tmp-file-mgr.h"
#include "util/spinlock.h"
#include "util/uid-util.h"
#include "util/promise.h"

namespace impala {

class FragmentInstanceState;
class MemTracker;
class ReservationTracker;

/// Central class for all backend execution state (example: the FragmentInstanceStates
/// of the individual fragment instances) created for a particular query.
/// This class contains or makes accessible state that is shared across fragment
/// instances; in contrast, fragment instance-specific state is collected in
/// FragmentInstanceState.
///
/// The lifetime of a QueryState is dictated by a reference count. Any thread that
/// executes on behalf of a query, and accesses any of its state, must obtain a
/// reference to the corresponding QueryState and hold it for at least the
/// duration of that access. The reference is obtained and released via
/// QueryExecMgr::Get-/ReleaseQueryState() or via QueryState::ScopedRef (the latter
/// for references limited to the scope of a single function or block).
/// As long as the reference count is greater than 0, all of a query's control
/// structures (contained either in this class or accessible through this class, such
/// as the FragmentInstanceStates) are guaranteed to be alive.
///
/// When any fragment instance execution returns with an error status, all
/// fragment instances are automatically cancelled.
///
/// Status reporting: all instances currently report their status independently.
/// Each instance sends at least one final status report with its overall execution
/// status, so if any of the instances encountered an error, that error will be reported.
///
/// Thread-safe, unless noted otherwise.
///
/// TODO:
/// - set up kudu clients in Init(), remove related locking
/// - release resources (those referenced directly or indirectly by the query result
///   set) automatically when all instances have finished execution
///   (either by returning all rows or by being cancelled), rather than waiting for an
///   explicit call to ReleaseResources()
/// - when ReportExecStatus() encounters an error, query execution at this node
///   gets aborted, but it's possible for the coordinator not to find out about that;
///   fix the coordinator to periodically ping the backends (should the coordinator
///   simply poll for the status reports?)
class QueryState {
 public:
  /// Use this class to obtain a QueryState for the duration of a function/block,
  /// rather than manually via QueryExecMgr::Get-/ReleaseQueryState().
  /// Pattern:
  /// {
  ///   QueryState::ScopedRef qs(qid);
  ///   if (qs->query_state() == nullptr) <do something, such as return>
  ///   ...
  /// }
  class ScopedRef {
   public:
    /// Looks up the query state with GetQueryState(). The query state is non-NULL if
    /// the query was already registered.
    ScopedRef(const TUniqueId& query_id);
    ~ScopedRef();

    /// may return nullptr
    QueryState* get() const { return query_state_; }
    QueryState* operator->() const { return query_state_; }

   private:
    QueryState* query_state_;
    DISALLOW_COPY_AND_ASSIGN(ScopedRef);
  };

  /// a shared pool for all objects that have query lifetime
  ObjectPool* obj_pool() { return &obj_pool_; }

  const TQueryCtx& query_ctx() const { return query_ctx_; }
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TQueryOptions& query_options() const {
    return query_ctx_.client_request.query_options;
  }
  MemTracker* query_mem_tracker() const { return query_mem_tracker_; }

  // the following getters are only valid after Prepare()
  ReservationTracker* buffer_reservation() const { return buffer_reservation_; }
  TmpFileMgr::FileGroup* file_group() const { return file_group_; }
  const TExecQueryFInstancesParams& rpc_params() const { return rpc_params_; }

  // the following getters are only valid after StartFInstances()
  const DescriptorTbl& desc_tbl() const { return *desc_tbl_; }

  /// Sets up state required for fragment execution: memory reservations, etc. Fails
  /// if resources could not be acquired. Uses few cycles and never blocks.
  /// Not idempotent, not thread-safe.
  /// The remaining public functions must be called only after Init().
  Status Init(const TExecQueryFInstancesParams& rpc_params) WARN_UNUSED_RESULT;

  /// Performs the runtime-intensive parts of initial setup and starts all fragment
  /// instances belonging to this query. Each instance receives its own execution
  /// thread. Blocks until all fragment instances have finished their Prepare phase.
  /// Not idempotent, not thread-safe.
  void StartFInstances();

  /// Return overall status of Prepare phases of fragment instances. A failure
  /// in any instance's Prepare will cause this function to return an error status.
  /// Blocks until all fragment instances have finished their Prepare phase.
  Status WaitForPrepare();

  /// Blocks until all fragment instances have finished their Prepare phase.
  FragmentInstanceState* GetFInstanceState(const TUniqueId& instance_id);

  /// Blocks until all fragment instances have finished their Prepare phase.
  void PublishFilter(int32_t filter_id, int fragment_idx,
      const TBloomFilter& thrift_bloom_filter);

  /// Cancels all actively executing fragment instances. Blocks until all fragment
  /// instances have finished their Prepare phase. Idempotent.
  void Cancel();

  /// Called once the query is complete to release any resources.
  /// Must be called only once and before destroying the QueryState.
  /// Not idempotent, not thread-safe.
  void ReleaseResources();

  /// Sends a ReportExecStatus rpc to the coordinator. If fis == nullptr, the
  /// status must be an error. If fis is given, expects that fis finished its Prepare
  /// phase; it then sends a report for that instance, including its profile.
  /// If there is an error during the rpc, initiates cancellation.
  void ReportExecStatus(bool done, const Status& status, FragmentInstanceState* fis);

  ~QueryState();

 private:
  friend class QueryExecMgr;

  /// test execution
  friend class RuntimeState;

  static const int DEFAULT_BATCH_SIZE = 1024;

  /// set in c'tor
  const TQueryCtx query_ctx_;

  /// the top-level MemTracker for this query (owned by obj_pool_), created in c'tor
  MemTracker* query_mem_tracker_ = nullptr;

  /// set in Prepare(); rpc_params_.query_ctx is *not* set to avoid duplication
  /// with query_ctx_
  /// TODO: find a way not to have to copy this
  TExecQueryFInstancesParams rpc_params_;

  /// Buffer reservation for this query (owned by obj_pool_)
  /// Only non-null in backend tests that explicitly enabled the new buffer pool
  /// Set in Prepare().
  /// TODO: this will always be non-null once IMPALA-3200 is done
  ReservationTracker* buffer_reservation_ = nullptr;

  /// Temporary files for this query (owned by obj_pool_)
  /// Only non-null in backend tests the explicitly enabled the new buffer pool
  /// Set in Prepare().
  /// TODO: this will always be non-null once IMPALA-3200 is done
  TmpFileMgr::FileGroup* file_group_ = nullptr;

  /// created in StartFInstances(), owned by obj_pool_
  DescriptorTbl* desc_tbl_ = nullptr;

  /// Barrier for the completion of the Prepare phases of all fragment instances,
  /// set in StartFInstances().
  Promise<Status> instances_prepared_promise_;

  /// map from instance id to its state (owned by obj_pool_), populated in
  /// StartFInstances(); not valid to read from until instances_prepare_promise_
  /// is set
  std::unordered_map<TUniqueId, FragmentInstanceState*> fis_map_;

  /// map from fragment index to its instances (owned by obj_pool_), populated in
  /// StartFInstances()
  std::unordered_map<int, std::vector<FragmentInstanceState*>> fragment_map_;

  ObjectPool obj_pool_;
  AtomicInt32 refcnt_;

  /// set to 1 when any fragment instance fails or when Cancel() is called; used to
  /// initiate cancellation exactly once
  AtomicInt32 is_cancelled_;

  /// True if and only if ReleaseResources() has been called.
  bool released_resources_ = false;

  /// Create QueryState w/ refcnt of 0.
  /// The query is associated with the resource pool query_ctx.request_pool or
  /// 'request_pool', if the former is not set (needed for tests).
  QueryState(const TQueryCtx& query_ctx, const std::string& request_pool = "");

  /// Execute the fragment instance and decrement the refcnt when done.
  void ExecFInstance(FragmentInstanceState* fis);

  /// Called from Prepare() to initialize MemTrackers.
  void InitMemTrackers();

  /// Called from Prepare() to setup buffer reservations and the
  /// file group. Fails if required resources are not available.
  Status InitBufferPoolState() WARN_UNUSED_RESULT;

  /// Same behavior as ReportExecStatus().
  /// Cancel on error only if instances_started is true.
  void ReportExecStatusAux(bool done, const Status& status, FragmentInstanceState* fis,
      bool instances_started);
};
}

#endif
