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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/atomic.h"
#include "common/compiler-util.h"
#include "common/logging.h"
#include "common/object-pool.h"
#include "common/status.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/control_service.pb.h"
#include "gutil/macros.h"
#include "gutil/threading/thread_collision_warner.h" // for DFAKE_*
#include "util/counting-barrier.h"
#include "util/spinlock.h"
#include "util/unique-id-hash.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class ControlServiceProxy;
class DataSinkConfig;
class DescriptorTbl;
class FragmentState;
class FragmentInstanceState;
class InitialReservations;
class LlvmCodeGen;
class MemTracker;
class PlanNode;
class PublishFilterParamsPB;
class ReservationTracker;
class RuntimeFilterBank;
class RuntimeProfile;
class RuntimeState;
class ScalarExpr;
class ScannerMemLimiter;
class TmpFileGroup;
class TRuntimeProfileForest;

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
/// Query execution resources (non-control-structure memory, scratch files, threads, etc)
/// are also managed via a separate resource reference count, which should be released as
/// soon as the resources are not needed to free resources promptly.
///
/// We maintain a state denoted by BackendExecState. The initial state is PREPARING.
/// Once all query fragment instances have finished FIS::Prepare(), the BackendExecState
/// will transition to:
/// - EXECUTING if all fragment instances succeeded in Prepare()
/// - ERROR if any fragment instances failed during or after Prepare()
/// - CANCELLED if the query is cancelled
///
/// Please note that even if some fragment instances hit an error during or after
/// Prepare(), the state transition from PREPARING won't happen until all fragment
/// instances have finished Prepare(). This makes sure the query state is initialized
/// to handle either a Cancel() RPC or a PublishFilter() RPC after PREPARING state.
///
/// Once BackendExecState() enters EXECUTING state, any error will trigger the
/// BackendExecState to go into ERROR state and the query execution is considered over
/// on this backend.
///
/// When any fragment instance execution returns with an error status, all fragment
/// instances are automatically cancelled. The query state thread (started by
/// QueryExecMgr) periodically reports the overall status, the current state of execution
/// and the profile of each fragment instance to the coordinator. The frequency of those
/// reports is controlled by the flag status_report_interval_ms; Setting it to 0 disables
/// periodic reporting altogether. Regardless of the value of that flag, a report is sent
/// at least once at the end of execution with an overall status and profile (and 'done'
/// indicator). If execution ended with an error, that error status will be part of
/// the final report (it will not be overridden by the resulting cancellation).
///
/// Thread-safe Notes:
/// - Init() must be called first. After Init() returns successfully, all other public
///   functions are thread-safe to be called unless noted otherwise.
/// - Cancel() is safe to be called at any time (before or during Init()).
///
/// TODO:
/// - set up kudu clients in Init(), remove related locking
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
  RuntimeProfile* host_profile() const { return host_profile_; }
  UniqueIdPB GetCoordinatorBackendId() const;

  /// The following getters are only valid after Init().
  ScannerMemLimiter* scanner_mem_limiter() const { return scanner_mem_limiter_; }

  /// The following getters are only valid after Init() and should be called only from
  /// the backend execution (ie. not the coordinator side, since they require holding
  /// an backend resource refcnt).
  ReservationTracker* buffer_reservation() const {
    DCHECK_GT(backend_resource_refcnt_.Load(), 0);
    return buffer_reservation_;
  }
  InitialReservations* initial_reservations() const { return initial_reservations_; }
  TmpFileGroup* file_group() const {
    DCHECK_GT(backend_resource_refcnt_.Load(), 0);
    return file_group_;
  }
  RuntimeFilterBank* filter_bank() const { return filter_bank_.get(); }

  /// The following getters are only valid after StartFInstances().
  int64_t fragment_events_start_time() const { return fragment_events_start_time_; }

  /// The following getters are only valid after StartFInstances() and should be called
  /// only from the backend execution (ie. not the coordinator side, since they require
  /// holding an backend resource refcnt).
  const DescriptorTbl& desc_tbl() const {
    DCHECK_GT(backend_resource_refcnt_.Load(), 0);
    return *desc_tbl_;
  }

  /// Sets up state required for fragment execution: memory reservations, etc. Fails if
  /// resources could not be acquired. Acquires a backend resource refcount and returns
  /// it to the caller on both success and failure. The caller must release it by
  /// calling ReleaseBackendResourceRefcount().
  ///
  /// Uses few cycles and blocks Cancel() to execute. Not idempotent.
  /// The remaining public functions must be called only after Init().
  Status Init(const ExecQueryFInstancesRequestPB* exec_rpc_params,
      const TExecPlanFragmentInfo& fragment_info) WARN_UNUSED_RESULT;

  /// Performs the runtime-intensive parts of initial setup and starts all fragment
  /// instances belonging to this query. Each instance receives its own execution
  /// thread. Not idempotent, not thread-safe. Must only be called by the query state
  /// thread. Returns true iff all fragment instance threads were started successfully.
  /// Returns false otherwise.
  bool StartFInstances();

  /// Monitors the execution of all underlying fragment instances and updates the query
  /// state accordingly. This is also responsible for sending status reports periodically
  /// to the coordinator. Not idempotent, not thread-safe. Must only be called by the
  /// query state thread.
  void MonitorFInstances();

  /// Blocks until all fragment instances have finished their Prepare phase.
  /// Returns the fragment instance state for 'instance_id' in *fi_state,
  /// or nullptr if it is not present.
  /// Returns an error if fragment preparation failed.
  Status GetFInstanceState(
      const TUniqueId& instance_id, FragmentInstanceState** fi_state);

  /// Blocks until all fragment instances have finished their Prepare phase.
  void PublishFilter(const PublishFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Cancels all actively executing fragment instances. Blocks until all fragment
  /// instances have finished their Prepare phase. Idempotent.
  /// For uninitialized QueryState, just set is_cancelled_ and don't need to cancel
  /// fragment instances.
  void Cancel();

  /// Return true if the executing fragment instances have been cancelled.
  bool IsCancelled() const { return (is_cancelled_.Load() == 1); }

  /// Increment the resource refcount. Must be decremented before the query state
  /// reference is released. A refcount should be held by a fragment or other entity
  /// for as long as it is consuming query backend execution resources (e.g. memory).
  void AcquireBackendResourceRefcount();

  /// Decrement the execution resource refcount and release resources if it goes to zero.
  /// All resource refcounts must be released before query state references are released.
  /// Should be called by the owner of the refcount after it is done consuming query
  /// execution resources.
  void ReleaseBackendResourceRefcount();

  /// Checks whether spilling is enabled for this query. Must be called before the first
  /// call to BufferPool::Unpin() for the query. Returns OK if spilling is enabled. If
  /// spilling is not enabled, logs a MEM_LIMIT_EXCEEDED error from
  /// tracker->MemLimitExceeded() to 'runtime_state'.
  Status StartSpilling(RuntimeState* runtime_state, MemTracker* mem_tracker);

  ~QueryState();

  /// Return overall status of Prepare() phases of fragment instances. A failure
  /// in any instance's Prepare() will cause this function to return an error status.
  /// Blocks until all fragment instances have finished their Prepare() phase.
  Status WaitForPrepare();

  /// Called by a FragmentInstanceState thread to notify that it's done preparing.
  void DonePreparing() { discard_result(instances_prepared_barrier_->Notify()); }

  /// Called by a FragmentInstanceState thread to notify that it's done executing.
  void DoneExecuting() { discard_result(instances_finished_barrier_->Notify()); }

  /// Called to notify that an error was encountered during codegen. This is called by the
  /// first fragment instance thread that invoked its corresponding FragmentState's
  /// Codegen() and encountered the error.
  void ErrorDuringFragmentCodegen(const Status& status);

  /// Called by a fragment instance thread to notify that it hit an error during Prepare()
  /// Updates the query status and the failed instance ID if it's not set already.
  /// Also notifies anyone waiting on WaitForPrepare() if this is called by the last
  /// fragment instance to complete Prepare().
  void ErrorDuringPrepare(const Status& status, const TUniqueId& finst_id);

  /// Called by a fragment instance thread to notify that it hit an error during Execute()
  /// Updates the query status and records the failed instance ID if they're not set
  /// already. Also notifies anyone waiting on WaitForFinishOrTimeout().
  void ErrorDuringExecute(const Status& status, const TUniqueId& finst_id);

  /// Get maximum reservation allowed for this query. MAX_INT64 means effectively
  /// unlimited.
  int64_t GetMaxReservation();

  /// The default BATCH_SIZE.
  static const int DEFAULT_BATCH_SIZE = 1024;

 private:
  friend class QueryExecMgr;

  /// test execution
  friend class RuntimeState;
  friend class TestEnv;

  /// Blocks until all fragment instances have finished executing or until one of them
  /// hits an error, or until 'timeout_ms' milliseconds has elapsed. Returns 'true' if
  /// all fragment instances finished or one of them hits an error. Return 'false' on
  /// time out.
  bool WaitForFinishOrTimeout(int32_t timeout_ms);

  /// Blocks until all fragment instances have finished executing or until one of them
  /// hits an error.
  void WaitForFinish();

  /// States that a query goes through during its lifecycle.
  enum class BackendExecState {
    /// PREPARING: The inital state on receiving an ExecQueryFInstances() RPC from the
    /// coordinator. Implies that the fragment instances are being started.
    PREPARING,
    /// EXECUTING: All fragment instances managed by this QueryState have successfully
    /// completed Prepare(). Implies that the query is executing.
    EXECUTING,
    /// FINISHED: All fragment instances managed by this QueryState have successfully
    /// completed executing.
    FINISHED,
    /// CANCELLED: This query received a CancelQueryFInstances() RPC or was directed by
    /// the coordinator to cancel itself from a response to a ReportExecStatus() RPC.
    /// Does not imply that all the fragment instances have realized cancellation however.
    CANCELLED,
    /// ERROR: received an error from a fragment instance.
    ERROR
  };

  /// Pseudo-lock to verify only query state thread is updating 'backend_exec_state_'.
  DFAKE_MUTEX(backend_exec_state_lock_);

  /// Current state of this query in this executor.
  /// Thread-safety: Only updated by the query state thread.
  BackendExecState backend_exec_state_ = BackendExecState::PREPARING;

  /// Protects 'overall_status_' and 'failed_finstance_id_'.
  SpinLock status_lock_;

  /// The overall status of this QueryState.
  /// A backend can have an error from a specific fragment instance, or it can have a
  /// general error that is independent of any individual fragment. If reporting a
  /// single error, this status is always set to the error being reported. If reporting
  /// multiple errors, the status is set by the following rules:
  /// 1. A general error takes precedence over any fragment instance error.
  /// 2. Any fragment instance error takes precedence over any cancelled status.
  /// 3. If multiple fragments have errors, the first fragment to hit an error is given
  ///    preference.
  /// Status::OK if all the fragment instances managed by this QS are also Status::OK;
  /// Protected by 'status_lock_'.
  Status overall_status_;

  /// ID of first fragment instance to hit an error.
  /// Protected by 'status_lock_'.
  TUniqueId failed_finstance_id_;

  /// set in c'tor
  const TQueryCtx query_ctx_;

  /// the top-level MemTracker for this query (owned by obj_pool_), created in c'tor
  MemTracker* query_mem_tracker_ = nullptr;

  /// The RPC proxy used when reporting status of fragment instances to coordinator.
  /// Set in Init().
  std::unique_ptr<ControlServiceProxy> proxy_;

  /// Set in Init(). TODO: find a way not to have to copy this
  ExecQueryFInstancesRequestPB exec_rpc_params_;
  TExecPlanFragmentInfo fragment_info_;

  /// Buffer reservation for this query (owned by obj_pool_). Set in Init().
  ReservationTracker* buffer_reservation_ = nullptr;

  /// Pool of buffer reservations used to distribute initial reservations to operators
  /// in the query. Contains a ReservationTracker that is a child of
  /// 'buffer_reservation_'. Owned by 'obj_pool_'. Set in Init().
  InitialReservations* initial_reservations_ = nullptr;

  /// Tracks expected memory consumption of all multithreaded scans for this query on
  /// this daemon. Owned by 'obj_pool_'. Set in Init().
  ScannerMemLimiter* scanner_mem_limiter_ = nullptr;

  /// Number of active fragment instances for this query that may consume resources for
  /// query backend execution (i.e. threads, memory) on the Impala daemon.  Query-wide
  /// backend execution resources for this query are released once this goes to zero.
  AtomicInt32 backend_resource_refcnt_;

  /// Temporary files for this query (owned by obj_pool_). Non-null if spilling is
  /// enabled. Set in Prepare().
  TmpFileGroup* file_group_ = nullptr;

  /// Manages runtime filters that are either produced or consumed (or both!) by plan
  /// nodes on this backend.
  std::unique_ptr<RuntimeFilterBank> filter_bank_;

  /// created in StartFInstances(), owned by obj_pool_
  DescriptorTbl* desc_tbl_ = nullptr;

  /// Barrier for the completion of the Prepare() phases of all fragment instances. This
  /// just blocks until ALL fragment instances have finished preparing, regardless of
  /// whether they hit an error or not.
  std::unique_ptr<CountingBarrier> instances_prepared_barrier_;

  /// Barrier for the completion of all the fragment instances.
  /// If the 'Status' is not OK due to an error during fragment instance execution, this
  /// barrier is unblocked immediately. 'overall_status_' is set once this is unblocked
  /// and so is 'failed_instance_id_' if an error is hit.
  std::unique_ptr<CountingBarrier> instances_finished_barrier_;

  /// Map from instance id to its state (owned by obj_pool_), populated in
  /// StartFInstances(); Not valid to read from until 'instances_prepared_barrier_'
  /// is set (i.e. readers should always call WaitForPrepare()).
  std::unordered_map<TUniqueId, FragmentInstanceState*> fis_map_;

  /// Map from fragment index to its fragment state (owned by obj_pool_), populated in
  /// StartFInstances();
  std::unordered_map<TFragmentIdx, FragmentState*> fragment_state_map_;

  ObjectPool obj_pool_;
  AtomicInt32 refcnt_;

  /// Protects 'is_initialized_'.
  std::mutex init_lock_;

  /// Set as true on successful initialization.
  /// Protected by 'init_lock_'.
  bool is_initialized_ = false;

  /// set to 1 when any fragment instance fails or when Cancel() is called; used to
  /// initiate cancellation exactly once
  AtomicInt32 is_cancelled_;

  /// set to false when the coordinator has been detected as inactive in the cluster;
  /// used to avoid sending the last execution report to the inactive/failed coordinator.
  AtomicBool is_coord_active_{true};

  /// True if and only if ReleaseExecResources() has been called.
  bool released_backend_resources_ = false;

  /// Whether the query has spilled. 0 if the query has not spilled. Atomically set to 1
  /// when the query first starts to spill. Required to correctly maintain the
  /// "num-queries-spilled" metric.
  AtomicInt32 query_spilled_;

  /// Records the point in time when fragment instances are started up. Set in
  /// StartFInstances().
  int64_t fragment_events_start_time_ = 0;

  /// Tracks host resource usage of this backend. Owned by 'obj_pool_', created in c'tor.
  RuntimeProfile* const host_profile_;

  /// The number of failed intermediate reports since the last successfully sent report.
  int64_t num_failed_reports_ = 0;

  /// If a status report fails, set to the current time using MonotonicMillis(). Reset to
  /// 0 on a successful report. Used to track how long we've been trying unsuccessfully to
  /// send a status report so that we can cancel after a configurable timeout.
  int64_t failed_report_time_ms_ = 0;

  /// Create QueryState w/ a refcnt of 0 and a memory limit of 'mem_limit' bytes applied
  /// to the query mem tracker. The query is associated with the resource pool set in
  /// 'query_ctx.request_pool' or from 'request_pool', if the former is not set (needed
  /// for tests).
  QueryState(const TQueryCtx& query_ctx, int64_t mem_limit,
      const std::string& request_pool = "");

  /// Execute the fragment instance and decrement the refcnt when done.
  void ExecFInstance(FragmentInstanceState* fis);

  /// Called from Init() to set up buffer reservations and the file group.
  Status InitBufferPoolState() WARN_UNUSED_RESULT;

  /// Initializes the runtime filter bank and claims the initial buffer reservation
  /// for it. The initial reservation must be claimed by 'init_reservations_' before
  /// calling this.
  Status InitFilterBank();

  /// Releases resources used for query backend execution. Guaranteed to be called only
  /// once. Must be called before destroying the QueryState. Not idempotent and not
  /// thread-safe.
  void ReleaseBackendResources();

  /// Functions for calculating user or system time spent on async codegen threads.
  int64_t AsyncCodegenThreadHelper(const std::string& suffix) const;
  int64_t AsyncCodegenThreadUserTime() const;
  int64_t AsyncCodegenThreadSysTime() const;

  /// Helper for ReportExecStatus() to construct a status report to be sent to the
  /// coordinator. The execution statuses (e.g. 'done' indicator) of all fragment
  /// instances belonging to this query state are stored in 'report'. The Thrift
  /// serialized runtime profiles of fragment instances are stored in 'profiles_forest'.
  void ConstructReport(bool instances_started, ReportExecStatusRequestPB* report,
      TRuntimeProfileForest* profiles_forest);

  /// Gather statuses and profiles of all fragment instances belonging to this query state
  /// and send it to the coordinator via ReportExecStatus() RPC. Returns true if the
  /// report rpc was successful or if it was unsuccessful and we've reached the maximum
  /// number of allowed failures and cancelled.
  bool ReportExecStatus();

  /// Returns the amount of time in ms to wait before sending the next status report,
  /// calculated as a function of the status report interval with backoff based on the
  /// number of consecutive failed reports.
  int64_t GetReportWaitTimeMs() const;

  /// Returns true if the overall backend status is already set with an error.
  bool HasErrorStatus() const {
    return !overall_status_.ok() && !overall_status_.IsCancelled();
  }

  /// Returns true if the query has reached a terminal state.
  bool IsTerminalState() const {
    return backend_exec_state_ == BackendExecState::FINISHED
        || backend_exec_state_ == BackendExecState::CANCELLED
        || backend_exec_state_ == BackendExecState::ERROR;
  }

  /// Updates the BackendExecState based on 'overall_status_'. Should only be called when
  /// the current state is a non-terminal state. The transition can either be to the next
  /// legal state or ERROR if 'overall_status_' is an error. Called by the query state
  /// thread only. It acquires the 'status_lock_' to synchronize with the fragment
  /// instance threads' updates to 'overall_status_'.
  ///
  /// Upon reaching a terminal state, it will call ReportExecStatus() to send the final
  /// report to the coordinator and not expect to be called afterwards.
  void UpdateBackendExecState();

  /// A string representation of 'state'.
  const char* BackendExecStateToString(const BackendExecState& state);
};
}
