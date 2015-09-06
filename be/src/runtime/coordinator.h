// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_RUNTIME_COORDINATOR_H
#define IMPALA_RUNTIME_COORDINATOR_H

#include <vector>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "common/status.h"
#include "common/global-types.h"
#include "util/progress-updater.h"
#include "util/runtime-profile.h"
#include "runtime/runtime-state.h"
#include "statestore/simple-scheduler.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Frontend_types.h"

namespace impala {

class DataStreamMgr;
class DataSink;
class RowBatch;
class RowDescriptor;
class PlanFragmentExecutor;
class ObjectPool;
class RuntimeState;
class ImpalaInternalServiceClient;
class Expr;
class ExprContext;
class ExecEnv;
class TUpdateCatalogRequest;
class TQueryExecRequest;
class TReportExecStatusParams;
class TRowBatch;
class TPlanExecRequest;
class TRuntimeProfileTree;
class RuntimeProfile;
class TablePrinter;

/// Query coordinator: handles execution of plan fragments on remote nodes, given
/// a TQueryExecRequest. As part of that, it handles all interactions with the
/// executing backends; it is also responsible for implementing all client requests
/// regarding the query, including cancellation.
/// The coordinator fragment is executed locally in the calling thread, all other
/// fragments are sent to remote nodes. The coordinator also monitors
/// the execution status of the remote fragments and aborts the entire query if an error
/// occurs, either in any of the remote fragments or in the local fragment.
/// Once a query has finished executing and all results have been returned either to the
/// caller of GetNext() or a data sink, execution_completed() will return true. If the
/// query is aborted, execution_completed should also be set to true.
/// Coordinator is thread-safe, with the exception of GetNext().
//
/// A typical sequence of calls for a single query (calls under the same numbered
/// item can happen concurrently):
/// 1. client: Exec()
/// 2. client: Wait()/client: Cancel()/backend: UpdateFragmentExecStatus()
/// 3. client: GetNext()*/client: Cancel()/backend: UpdateFragmentExecStatus()
//
/// The implementation ensures that setting an overall error status and initiating
/// cancellation of local and all remote fragments is atomic.
class Coordinator {
 public:
  Coordinator(ExecEnv* exec_env, RuntimeProfile::EventSequence* events);
  ~Coordinator();

  /// Initiate asynchronous execution of a query with the given schedule. Returns as soon
  /// as all plan fragments have started executing at their respective backends.
  /// 'schedule' must contain at least a coordinator plan fragment (ie, can't
  /// be for a query like 'SELECT 1').
  /// Populates and prepares output_expr_ctxs from the coordinator's fragment if there is
  /// one, and LLVM optimizes them together with the fragment's other exprs.
  /// A call to Exec() must precede all other member function calls.
  Status Exec(QuerySchedule& schedule, std::vector<ExprContext*>* output_expr_ctxs);

  /// Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  /// query doesn't return rows, until the query finishes or is cancelled.
  /// A call to Wait() must precede all calls to GetNext().
  /// Multiple calls to Wait() are idempotent and it is okay to issue multiple
  /// Wait() calls concurrently.
  Status Wait();

  /// Returns tuples from the coordinator fragment. Any returned tuples are valid until
  /// the next GetNext() call. If *batch is NULL, execution has completed and GetNext()
  /// must not be called again.
  /// GetNext() will not set *batch=NULL until all backends have
  /// either completed or have failed.
  /// It is safe to call GetNext() even in the case where there is no coordinator fragment
  /// (distributed INSERT).
  /// '*batch' is owned by the underlying PlanFragmentExecutor and must not be deleted.
  /// *state is owned by the caller, and must not be deleted.
  /// Returns an error status if an error was encountered either locally or by
  /// any of the remote fragments or if the query was cancelled.
  /// GetNext() is not thread-safe: multiple threads must not make concurrent
  /// GetNext() calls (but may call any of the other member functions concurrently
  /// with GetNext()).
  Status GetNext(RowBatch** batch, RuntimeState* state);

  /// Cancel execution of query. This includes the execution of the local plan fragment,
  /// if any, as well as all plan fragments on remote nodes. Sets query_status_ to
  /// the given cause if non-NULL. Otherwise, sets query_status_ to Status::CANCELLED.
  /// Idempotent.
  void Cancel(const Status* cause = NULL);

  /// Updates status and query execution metadata of a particular
  /// fragment; if 'status' is an error status or if 'done' is true,
  /// considers the plan fragment to have finished execution. Assumes
  /// that calls to UpdateFragmentExecStatus() won't happen
  /// concurrently for the same backend.
  /// If 'status' is an error status, also cancel execution of the query via a call
  /// to CancelInternal().
  Status UpdateFragmentExecStatus(const TReportExecStatusParams& params);

  /// only valid *after* calling Exec(), and may return NULL if there is no executor
  RuntimeState* runtime_state();
  const RowDescriptor& row_desc() const;

  /// Only valid after Exec(). Returns runtime_state()->query_mem_tracker() if there
  /// is a coordinator fragment, or query_mem_tracker_ (initialized in Exec()) otherwise.
  MemTracker* query_mem_tracker();

  /// Get cumulative profile aggregated over all fragments of the query.
  /// This is a snapshot of the current state of execution and will change in
  /// the future if not all fragments have finished execution.
  RuntimeProfile* query_profile() const { return query_profile_.get(); }

  const TUniqueId& query_id() const { return query_id_; }

  /// This is safe to call only after Wait()
  const PartitionStatusMap& per_partition_status() { return per_partition_status_; }

  /// Gathers all updates to the catalog required once this query has completed execution.
  /// Returns true if a catalog update is required, false otherwise.
  /// Must only be called after Wait()
  bool PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update);

  /// Return error log for coord and all the fragments. The error messages from the
  /// individual backends are merged into a single output to retain readability.
  std::string GetErrorLog();

  const ProgressUpdater& progress() { return progress_; }

  /// Returns query_status_.
  Status GetStatus();

  /// Returns the exec summary. The exec summary lock must already have been taken.
  /// The caller must not block while holding the lock.
  const TExecSummary& exec_summary() const {
    exec_summary_lock_.DCheckLocked();
    return exec_summary_;
  }

  SpinLock& GetExecSummaryLock() const { return exec_summary_lock_; }

 private:
  class BackendExecState;

  /// Typedef for boost utility to compute averaged stats
  /// TODO: including the median doesn't compile, looks like some includes are missing
  typedef boost::accumulators::accumulator_set<int64_t,
      boost::accumulators::features<
      boost::accumulators::tag::min,
      boost::accumulators::tag::max,
      boost::accumulators::tag::mean,
      boost::accumulators::tag::variance>
  > SummaryStats;

  ExecEnv* exec_env_;
  TUniqueId query_id_;

  /// copied from TQueryExecRequest; constant across all fragments
  TDescriptorTable desc_tbl_;
  TQueryCtx query_ctx_;

  /// copied from TQueryExecRequest, governs when to call ReportQuerySummary
  TStmtType::type stmt_type_;

  /// map from id of a scan node to a specific counter in the node's profile
  typedef std::map<PlanNodeId, RuntimeProfile::Counter*> CounterMap;

  /// Struct for per fragment instance counters that will be aggregated by the coordinator.
  struct FragmentInstanceCounters {
    /// Throughput counters per node
    CounterMap throughput_counters;

    /// Total finished scan ranges per node
    CounterMap scan_ranges_complete_counters;
  };

  /// BackendExecStates owned by obj_pool()
  std::vector<BackendExecState*> backend_exec_states_;

  /// True if the query needs a post-execution step to tidy up
  bool needs_finalization_;

  /// Only valid if needs_finalization is true
  TFinalizeParams finalize_params_;

  /// ensures single-threaded execution of Wait(); must not hold lock_ when acquiring this
  boost::mutex wait_lock_;

  bool has_called_wait_;  // if true, Wait() was called; protected by wait_lock_

  /// Keeps track of number of completed ranges and total scan ranges.
  ProgressUpdater progress_;

  /// protects all fields below
  boost::mutex lock_;

  /// Overall status of the entire query; set to the first reported fragment error
  /// status or to CANCELLED, if Cancel() is called.
  Status query_status_;

  /// If true, the query is done returning all results.  It is possible that the
  /// coordinator still needs to wait for cleanup on remote fragments (e.g. queries
  /// with limit)
  /// Once this is set to true, errors from remote fragments are ignored.
  bool returned_all_results_;

  /// execution state of coordinator fragment
  boost::scoped_ptr<PlanFragmentExecutor> executor_;

  /// Query mem tracker for this coordinator initialized in Exec(). Only valid if there
  /// is no coordinator fragment (i.e. executor_ == NULL). If executor_ is not NULL,
  /// this->runtime_state()->query_mem_tracker() returns the query mem tracker.
  /// (See this->query_mem_tracker())
  boost::shared_ptr<MemTracker> query_mem_tracker_;

  /// owned by plan root, which resides in runtime_state_'s pool
  const RowDescriptor* row_desc_;

  /// map from fragment instance id to corresponding exec state stored in
  /// backend_exec_states_
  typedef boost::unordered_map<TUniqueId, BackendExecState*> BackendExecStateMap;
  BackendExecStateMap backend_exec_state_map_;

  /// Returns a local object pool.
  ObjectPool* obj_pool() { return obj_pool_.get(); }

  /// True if execution has completed, false otherwise.
  bool execution_completed_;

  /// Number of remote fragments that have completed
  int num_remote_fragements_complete_;

  /// If there is no coordinator fragment, Wait simply waits until all
  /// backends report completion by notifying on backend_completion_cv_.
  /// Tied to lock_.
  boost::condition_variable backend_completion_cv_;

  /// Count of the number of backends for which done != true. When this
  /// hits 0, any Wait()'ing thread is notified
  int num_remaining_backends_;

  /// The following two structures, partition_row_counts_ and files_to_move_ are filled in
  /// as the query completes, and track the results of INSERT queries that alter the
  /// structure of tables. They are either the union of the reports from all backends, or
  /// taken from the coordinator fragment: only one of the two can legitimately produce
  /// updates.

  /// The set of partitions that have been written to or updated by all backends, along
  /// with statistics such as the number of rows written (may be 0). For unpartitioned
  /// tables, the empty string denotes the entire table.
  PartitionStatusMap per_partition_status_;

  /// The set of files to move after an INSERT query has run, in (src, dest) form. An empty
  /// string for the destination means that a file is to be deleted.
  FileMoveMap files_to_move_;

  /// Object pool owned by the coordinator. Any executor will have its own pool.
  boost::scoped_ptr<ObjectPool> obj_pool_;

  /// Execution summary for this query.
  mutable SpinLock exec_summary_lock_;
  TExecSummary exec_summary_;

  /// A mapping of plan node ids to index into exec_summary_.nodes
  boost::unordered_map<TPlanNodeId, int> plan_node_id_to_summary_map_;

  /// Aggregate counters for the entire query.
  boost::scoped_ptr<RuntimeProfile> query_profile_;

  /// Event timeline for this query. Unowned.
  RuntimeProfile::EventSequence* query_events_;

  /// Per fragment profile information
  struct PerFragmentProfileData {
    /// Averaged profile for this fragment.  Stored in obj_pool.
    /// The counters in this profile are averages (type AveragedCounter) of the
    /// counters in the fragment instance profiles.
    /// Note that the individual fragment instance profiles themselves are stored and
    /// displayed as children of the root_profile below.
    RuntimeProfile* averaged_profile;

    /// Number of instances running this fragment.
    int num_instances;

    /// Root profile for all fragment instances for this fragment
    RuntimeProfile* root_profile;

    /// Bytes assigned for instances of this fragment
    SummaryStats bytes_assigned;

    /// Completion times for instances of this fragment
    SummaryStats completion_times;

    /// Execution rates for instances of this fragment
    SummaryStats rates;
  };

  /// This is indexed by fragment_idx.
  /// This array is only modified at coordinator startup and query completion and
  /// does not need locks.
  std::vector<PerFragmentProfileData> fragment_profiles_;

  /// Throughput counters for the coordinator fragment
  FragmentInstanceCounters coordinator_counters_;

  /// The set of hosts that the query will run on. Populated in Exec.
  boost::unordered_set<TNetworkAddress> unique_hosts_;

  /// Total time spent in finalization (typically 0 except for INSERT into hdfs tables)
  RuntimeProfile::Counter* finalization_timer_;

  /// Fill in rpc_params based on parameters.
  void SetExecPlanFragmentParams(QuerySchedule& schedule,
      int backend_num, const TPlanFragment& fragment,
      int fragment_idx, const FragmentExecParams& params, int instance_idx,
      const TNetworkAddress& coord, TExecPlanFragmentParams* rpc_params);

  /// Wrapper for ExecPlanFragment() rpc.  This function will be called in parallel
  /// from multiple threads.
  /// Obtains exec_state->lock prior to making rpc, so that it serializes
  /// correctly with UpdateFragmentExecStatus().
  /// exec_state contains all information needed to issue the rpc.
  /// 'coordinator' will always be an instance to this class and 'exec_state' will
  /// always be an instance of BackendExecState.
  Status ExecRemoteFragment(void* exec_state);

  /// Determine fragment number, given fragment id.
  int GetFragmentNum(const TUniqueId& fragment_id);

  /// Print hdfs split size stats to VLOG_QUERY and details to VLOG_FILE
  /// Attaches split size summary to the appropriate runtime profile
  void PrintBackendInfo();

  /// Create aggregate counters for all scan nodes in any of the fragments
  void CreateAggregateCounters(const std::vector<TPlanFragment>& fragments);

  /// Collect scan node counters from the profile.
  /// Assumes lock protecting profile and result is held.
  void CollectScanNodeCounters(RuntimeProfile*, FragmentInstanceCounters* result);

  /// Derived counter function: aggregates throughput for node_id across all backends
  /// (id needs to be for a ScanNode)
  int64_t ComputeTotalThroughput(int node_id);

  /// Derived counter function: aggregates total completed scan ranges for node_id
  /// across all backends(id needs to be for a ScanNode)
  int64_t ComputeTotalScanRangesComplete(int node_id);

  /// Runs cancel logic. Assumes that lock_ is held.
  void CancelInternal();

  /// Cancels remote fragments. Assumes that lock_ is held.  This can be called when
  /// the query is not being cancelled in the case where the query limit is
  /// reached.
  void CancelRemoteFragments();

  /// Acquires lock_ and updates query_status_ with 'status' if it's not already
  /// an error status, and returns the current query_status_.
  /// Calls CancelInternal() when switching to an error status.
  /// If failed_fragment is non-null, it is the fragment_id that has failed, used
  /// for error reporting.
  Status UpdateStatus(const Status& status, const TUniqueId* failed_fragment);

  /// Returns only when either all backends have reported success or the query is in
  /// error. Returns the status of the query.
  /// It is safe to call this concurrently, but any calls must be made only after Exec().
  /// WaitForAllBackends may be called before Wait(), but note that Wait() guarantees
  /// that any coordinator fragment has finished, which this method does not.
  Status WaitForAllBackends();

  /// Perform any post-query cleanup required. Called by Wait() only after all backends
  /// have returned, or if the query has failed, in which case it only cleans up temporary
  /// data rather than finishing the INSERT in flight.
  Status FinalizeQuery();

  /// Moves all temporary staging files to their final destinations.
  Status FinalizeSuccessfulInsert();

  /// Initializes the structures in runtime profile and exec_summary_. Must be
  /// called before RPCs to start remote fragments.
  void InitExecProfile(const TQueryExecRequest& request);

  /// Update fragment profile information from a backend exec state.
  /// This is called repeatedly from UpdateFragmentExecStatus(),
  /// and also at the end of the query from ReportQuerySummary().
  /// This method calls UpdateAverage() and AddChild(), which obtain their own locks
  /// on the backend state.
  void UpdateAverageProfile(BackendExecState* backend_exec_state);

  /// Compute the summary stats (completion_time and rates)
  /// for an individual fragment_profile_ based on the specified backed_exec_state.
  /// Called only from ReportQuerySummary() below.
  void ComputeFragmentSummaryStats(BackendExecState* backend_exec_state);

  /// Outputs aggregate query profile summary.  This is assumed to be called at the end of
  /// a query -- remote fragments' profiles must not be updated while this is running.
  void ReportQuerySummary();

  /// Populates the summary execution stats from the profile. Can only be called when the
  /// query is done.
  void UpdateExecSummary(int fragment_idx, int instance_idx, RuntimeProfile* profile);

  /// Determines what the permissions of directories created by INSERT statements should be
  /// if permission inheritance is enabled. Populates a map from all prefixes of path_str
  /// (including the full path itself) which is a path in Hdfs, to pairs (does_not_exist,
  /// permissions), where does_not_exist is true if the path does not exist in Hdfs. If
  /// does_not_exist is true, permissions is set to the permissions of the most immediate
  /// ancestor of the path that does exist, i.e. the permissions that the path should
  /// inherit when created. Otherwise permissions is set to the actual permissions of the
  /// path. The PermissionCache argument is also used to cache the output across repeated
  /// calls, to avoid repeatedly calling hdfsGetPathInfo() on the same path.
  typedef boost::unordered_map<std::string, std::pair<bool, short> > PermissionCache;
  void PopulatePathPermissionCache(hdfsFS fs, const std::string& path_str,
      PermissionCache* permissions_cache);

  /// Validates that all collection-typed slots in the given batch are set to NULL.
  /// See SubplanNode for details on when collection-typed slots are set to NULL.
  /// TODO: This validation will become obsolete when we can return collection values.
  /// We will then need a different mechanism to assert the correct behavior of the
  /// SubplanNode with respect to setting collection-slots to NULL.
  void ValidateCollectionSlots(RowBatch* batch);
};

}

#endif
