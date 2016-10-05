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
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "common/global-types.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/runtime-state.h"
#include "scheduling/simple-scheduler.h"
#include "service/fragment-exec-state.h"
#include "service/fragment-mgr.h"
#include "util/histogram-metric.h"
#include "util/progress-updater.h"
#include "util/runtime-profile.h"

namespace impala {

class CountingBarrier;
class DataStreamMgr;
class DataSink;
class RowBatch;
class RowDescriptor;
class PlanFragmentExecutor;
class ObjectPool;
class RuntimeState;
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
class TPlanFragment;
class QueryResultSet;

struct DebugOptions;

/// Query coordinator: handles execution of fragment instances on remote nodes, given a
/// TQueryExecRequest. As part of that, it handles all interactions with the executing
/// backends; it is also responsible for implementing all client requests regarding the
/// query, including cancellation.
///
/// The coordinator monitors the execution status of fragment instances and aborts the
/// entire query if an error is reported by any of them.
///
/// Queries that have results have those results fetched by calling GetNext(). Results
/// rows are produced by a fragment instance that always executes on the same machine as
/// the coordinator.
///
/// Once a query has finished executing and all results have been returned either to the
/// caller of GetNext() or a data sink, execution_completed() will return true. If the
/// query is aborted, execution_completed should also be set to true. Coordinator is
/// thread-safe, with the exception of GetNext().
//
/// A typical sequence of calls for a single query (calls under the same numbered
/// item can happen concurrently):
/// 1. client: Exec()
/// 2. client: Wait()/client: Cancel()/backend: UpdateFragmentExecStatus()
/// 3. client: GetNext()*/client: Cancel()/backend: UpdateFragmentExecStatus()
///
/// The implementation ensures that setting an overall error status and initiating
/// cancellation of local and all remote fragments is atomic.
///
/// TODO: move into separate subdirectory and move nested classes into separate files
/// and unnest them
///
/// TODO: remove all data structures and functions that are superceded by their
/// multi-threaded counterpart and remove the "Mt" prefix with which the latter
/// is currently marked
class Coordinator {
 public:
  Coordinator(const QuerySchedule& schedule, ExecEnv* exec_env,
      RuntimeProfile::EventSequence* events);
  ~Coordinator();

  /// Initiate asynchronous execution of a query with the given schedule. When it returns,
  /// all fragment instances have started executing at their respective backends.
  /// A call to Exec() must precede all other member function calls.
  Status Exec();

  /// Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  /// query doesn't return rows, until the query finishes or is cancelled.
  /// A call to Wait() must precede all calls to GetNext().
  /// Multiple calls to Wait() are idempotent and it is okay to issue multiple
  /// Wait() calls concurrently.
  Status Wait();

  /// Fills 'results' with up to 'max_rows' rows. May return fewer than 'max_rows'
  /// rows, but will not return more.
  ///
  /// If *eos is true, execution has completed and GetNext() must not be called
  /// again.
  ///
  /// GetNext() will not set *eos=true until all fragment instances have either completed
  /// or have failed.
  ///
  /// Returns an error status if an error was encountered either locally or by any of the
  /// remote fragments or if the query was cancelled.
  ///
  /// GetNext() is not thread-safe: multiple threads must not make concurrent GetNext()
  /// calls (but may call any of the other member functions concurrently with GetNext()).
  Status GetNext(QueryResultSet* results, int max_rows, bool* eos);

  /// Cancel execution of query. This includes the execution of the local plan fragment,
  /// if any, as well as all plan fragments on remote nodes. Sets query_status_ to the
  /// given cause if non-NULL. Otherwise, sets query_status_ to Status::CANCELLED.
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

  /// Only valid *after* calling Exec(). Return nullptr if the running query does not
  /// produce any rows.
  ///
  /// TODO: The only dependency on this is QueryExecState, used to track memory for the
  /// result cache. Remove this dependency, possibly by moving result caching inside this
  /// class.
  RuntimeState* runtime_state();

  /// Only valid after Exec(). Returns runtime_state()->query_mem_tracker() if there
  /// is a coordinator fragment, or query_mem_tracker_ (initialized in Exec()) otherwise.
  ///
  /// TODO: Remove, see runtime_state().
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
  /// individual fragment instances are merged into a single output to retain readability.
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

  /// Receive a local filter update from a fragment instance. Aggregate that filter update
  /// with others for the same filter ID into a global filter. If all updates for that
  /// filter ID have been received (may be 1 or more per filter), broadcast the global
  /// filter to fragment instances.
  void UpdateFilter(const TUpdateFilterParams& params);

  /// Called once the query is complete to tear down any remaining state.
  void TearDown();

 private:
  class FragmentInstanceState;
  struct FilterState;

  /// Typedef for boost utility to compute averaged stats
  /// TODO: including the median doesn't compile, looks like some includes are missing
  typedef boost::accumulators::accumulator_set<int64_t,
      boost::accumulators::features<
      boost::accumulators::tag::min,
      boost::accumulators::tag::max,
      boost::accumulators::tag::mean,
      boost::accumulators::tag::variance>
  > SummaryStats;

  const QuerySchedule schedule_;
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

  /// FragmentInstanceStates for all fragment instances, including that of the coordinator
  /// fragment. All elements are non-nullptr. Owned by obj_pool(). Filled in
  /// PrepareCoordFragment() and StartRemoteFragments()/MtStartRemoteFInstances().
  std::vector<FragmentInstanceState*> fragment_instance_states_;

  /// True if the query needs a post-execution step to tidy up
  bool needs_finalization_;

  /// Only valid if needs_finalization is true
  TFinalizeParams finalize_params_;

  /// ensures single-threaded execution of Wait(); must not hold lock_ when acquiring this
  boost::mutex wait_lock_;

  bool has_called_wait_;  // if true, Wait() was called; protected by wait_lock_

  /// Keeps track of number of completed ranges and total scan ranges.
  ProgressUpdater progress_;

  /// Protects all fields below. This is held while making RPCs, so this lock should
  /// only be acquired if the acquiring thread is prepared to wait for a significant
  /// time.
  /// Lock ordering is
  /// 1. lock_
  /// 2. FragmentInstanceState::lock_
  boost::mutex lock_;

  /// Overall status of the entire query; set to the first reported fragment error
  /// status or to CANCELLED, if Cancel() is called.
  Status query_status_;

  /// If true, the query is done returning all results.  It is possible that the
  /// coordinator still needs to wait for cleanup on remote fragments (e.g. queries
  /// with limit)
  /// Once this is set to true, errors from remote fragments are ignored.
  bool returned_all_results_;

  /// Non-null if and only if the query produces results for the client; i.e. is of
  /// TStmtType::QUERY. Coordinator uses these to pull results from plan tree and return
  /// them to the client in GetNext(), and also to access the fragment instance's runtime
  /// state.
  ///
  /// Result rows are materialized by this fragment instance in its own thread. They are
  /// materialized into a QueryResultSet provided to the coordinator during GetNext().
  ///
  /// Not owned by this class, created during fragment instance start-up by
  /// FragmentExecState and set here in Exec().
  PlanFragmentExecutor* executor_ = nullptr;
  PlanRootSink* root_sink_ = nullptr;

  /// Query mem tracker for this coordinator initialized in Exec(). Only valid if there
  /// is no coordinator fragment (i.e. executor_ == NULL). If executor_ is not NULL,
  /// this->runtime_state()->query_mem_tracker() returns the query mem tracker.
  /// (See this->query_mem_tracker())
  std::shared_ptr<MemTracker> query_mem_tracker_;

  /// owned by plan root, which resides in runtime_state_'s pool
  const RowDescriptor* row_desc_;

  /// Returns a local object pool.
  ObjectPool* obj_pool() { return obj_pool_.get(); }

  // Sets the TDescriptorTable(s) for the current fragment.
  void SetExecPlanDescriptorTable(const TPlanFragment& fragment,
      TExecPlanFragmentParams* rpc_params);

  /// True if execution has completed, false otherwise.
  bool execution_completed_;

  /// Number of remote fragments that have completed
  int num_remote_fragements_complete_;

  /// If there is no coordinator fragment, Wait() simply waits until all
  /// backends report completion by notifying on instance_completion_cv_.
  /// Tied to lock_.
  boost::condition_variable instance_completion_cv_;

  /// Count of the number of backends for which done != true. When this
  /// hits 0, any Wait()'ing thread is notified
  int num_remaining_fragment_instances_;

  /// The following two structures, partition_row_counts_ and files_to_move_ are filled in
  /// as the query completes, and track the results of INSERT queries that alter the
  /// structure of tables. They are either the union of the reports from all fragment
  /// instances, or taken from the coordinator fragment: only one of the two can
  /// legitimately produce updates.

  /// The set of partitions that have been written to or updated by all fragment
  /// instances, along with statistics such as the number of rows written (may be 0). For
  /// unpartitioned tables, the empty string denotes the entire table.
  PartitionStatusMap per_partition_status_;

  /// The set of files to move after an INSERT query has run, in (src, dest) form. An
  /// empty string for the destination means that a file is to be deleted.
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

    PerFragmentProfileData()
      : averaged_profile(nullptr), num_instances(-1), root_profile(nullptr) {}
  };

  /// This is indexed by fragment idx (TPlanFragment.idx).
  /// This array is only modified at coordinator startup and query completion and
  /// does not need locks.
  std::vector<PerFragmentProfileData> fragment_profiles_;

  /// Throughput counters for the coordinator fragment
  FragmentInstanceCounters coordinator_counters_;

  /// The set of hosts that the query will run on. Populated in Exec.
  boost::unordered_set<TNetworkAddress> unique_hosts_;

  /// Total time spent in finalization (typically 0 except for INSERT into hdfs tables)
  RuntimeProfile::Counter* finalization_timer_;

  /// Barrier that is released when all calls to ExecRemoteFragment() have
  /// returned, successfully or not. Initialised during Exec().
  boost::scoped_ptr<CountingBarrier> exec_complete_barrier_;

  /// Represents a runtime filter target.
  struct FilterTarget {
    TPlanNodeId node_id;
    bool is_local;
    bool is_bound_by_partition_columns;

    // indices into fragment_instance_states_
    boost::unordered_set<int> fragment_instance_state_idxs;

    FilterTarget(const TRuntimeFilterTargetDesc& tFilterTarget) {
      node_id = tFilterTarget.node_id;
      is_bound_by_partition_columns = tFilterTarget.is_bound_by_partition_columns;
      is_local = tFilterTarget.is_local_target;
    }
  };

  /// Protects filter_routing_table_.
  SpinLock filter_lock_;

  /// Map from filter ID to filter.
  typedef boost::unordered_map<int32_t, FilterState> FilterRoutingTable;
  FilterRoutingTable filter_routing_table_;

  /// Set to true when all calls to UpdateFilterRoutingTable() have finished, and it's
  /// safe to concurrently read from filter_routing_table_.
  bool filter_routing_table_complete_;

  /// Total number of filter updates received (always 0 if filter mode is not
  /// GLOBAL). Excludes repeated broadcast filter updates.
  RuntimeProfile::Counter* filter_updates_received_;

  /// The filtering mode for this query. Set in constructor.
  const TRuntimeFilterMode::type filter_mode_;

  /// Tracks the memory consumed by runtime filters during aggregation. Child of
  /// query_mem_tracker_.
  std::unique_ptr<MemTracker> filter_mem_tracker_;

  /// True if and only if TearDown() has been called.
  bool torn_down_;

  /// Returns a pretty-printed table of the current filter state.
  std::string FilterDebugString();

  /// Sets 'filter_routing_table_complete_' and prints the table to the profile and log.
  void MarkFilterRoutingTableComplete();

  /// Fill in rpc_params based on parameters.
  /// 'per_fragment_instance_idx' is the 0-based ordinal of this particular fragment
  /// instance within its fragment.
  void SetExecPlanFragmentParams(const TPlanFragment& fragment,
      const FragmentExecParams& params, int per_fragment_instance_idx,
      TExecPlanFragmentParams* rpc_params);
  void MtSetExecPlanFragmentParams(
      const FInstanceExecParams& params, TExecPlanFragmentParams* rpc_params);

  /// Wrapper for ExecPlanFragment() RPC. This function will be called in parallel from
  /// multiple threads. Creates a new FragmentInstanceState and registers it in
  /// fragment_instance_states_, then calls RPC to issue fragment on remote impalad.
  void ExecRemoteFragment(const FragmentExecParams& fragment_exec_params,
      const TPlanFragment& plan_fragment, DebugOptions* debug_options,
      int fragment_instance_idx);
  void MtExecRemoteFInstance(
      const FInstanceExecParams& exec_params, const DebugOptions* debug_options);

  /// Determine fragment number, given fragment id.
  int GetFragmentNum(const TUniqueId& fragment_id);

  /// Print hdfs split size stats to VLOG_QUERY and details to VLOG_FILE
  /// Attaches split size summary to the appropriate runtime profile
  void PrintFragmentInstanceInfo();

  /// Collect scan node counters from the profile.
  /// Assumes lock protecting profile and result is held.
  void CollectScanNodeCounters(RuntimeProfile*, FragmentInstanceCounters* result);

  /// Runs cancel logic. Assumes that lock_ is held.
  void CancelInternal();

  /// Cancels all fragment instances. Assumes that lock_ is held. This may be called when
  /// the query is not being cancelled in the case where the query limit is reached.
  void CancelFragmentInstances();

  /// Acquires lock_ and updates query_status_ with 'status' if it's not already
  /// an error status, and returns the current query_status_.
  /// Calls CancelInternal() when switching to an error status.
  /// failed_fragment is the fragment_id that has failed, used for error reporting along
  /// with instance_hostname.
  Status UpdateStatus(const Status& status, const TUniqueId& failed_fragment,
      const std::string& instance_hostname);

  /// Returns only when either all fragment instances have reported success or the query
  /// is in error. Returns the status of the query.
  /// It is safe to call this concurrently, but any calls must be made only after Exec().
  /// WaitForAllInstances may be called before Wait(), but note that Wait() guarantees
  /// that any coordinator fragment has finished, which this method does not.
  Status WaitForAllInstances();

  /// Perform any post-query cleanup required. Called by Wait() only after all fragment
  /// instances have returned, or if the query has failed, in which case it only cleans up
  /// temporary data rather than finishing the INSERT in flight.
  Status FinalizeQuery();

  /// Moves all temporary staging files to their final destinations.
  Status FinalizeSuccessfulInsert();

  /// Initializes the structures in runtime profile and exec_summary_. Must be
  /// called before RPCs to start remote fragments.
  void InitExecProfile(const TQueryExecRequest& request);
  void MtInitExecProfiles();

  /// Initialize the structures to collect execution summary of every plan node
  /// (exec_summary_ and plan_node_id_to_summary_map_)
  void MtInitExecSummary();

  /// Update fragment profile information from a fragment instance state.
  void UpdateAverageProfile(FragmentInstanceState* fragment_instance_state);

  /// Compute the summary stats (completion_time and rates)
  /// for an individual fragment_profile_ based on the specified instance state.
  void ComputeFragmentSummaryStats(FragmentInstanceState* fragment_instance_state);

  /// Outputs aggregate query profile summary.  This is assumed to be called at the end of
  /// a query -- remote fragments' profiles must not be updated while this is running.
  void ReportQuerySummary();

  /// Populates the summary execution stats from the profile. Can only be called when the
  /// query is done.
  void UpdateExecSummary(const FragmentInstanceState& instance_state);

  /// Determines what the permissions of directories created by INSERT statements should
  /// be if permission inheritance is enabled. Populates a map from all prefixes of
  /// path_str (including the full path itself) which is a path in Hdfs, to pairs
  /// (does_not_exist, permissions), where does_not_exist is true if the path does not
  /// exist in Hdfs. If does_not_exist is true, permissions is set to the permissions of
  /// the most immediate ancestor of the path that does exist, i.e. the permissions that
  /// the path should inherit when created. Otherwise permissions is set to the actual
  /// permissions of the path. The PermissionCache argument is also used to cache the
  /// output across repeated calls, to avoid repeatedly calling hdfsGetPathInfo() on the
  /// same path.
  typedef boost::unordered_map<std::string, std::pair<bool, short>> PermissionCache;
  void PopulatePathPermissionCache(hdfsFS fs, const std::string& path_str,
      PermissionCache* permissions_cache);

  /// Starts all fragment instances contained in the schedule by issuing RPCs in parallel,
  /// and then waiting for all of the RPCs to complete.
  void StartFragments();

  /// Starts all fragment instances contained in the schedule by issuing RPCs in parallel
  /// and then waiting for all of the RPCs to complete.
  void MtStartFInstances();

  /// Calls CancelInternal() and returns an error if there was any error starting the
  /// fragments.
  /// Also updates query_profile_ with the startup latency histogram.
  Status FinishInstanceStartup();

  /// Build the filter routing table by iterating over all plan nodes and collecting the
  /// filters that they either produce or consume. The source and target fragment
  /// instance indexes for filters are numbered in the range
  /// [start_fragment_instance_idx .. start_fragment_instance_idx + num_hosts]
  void UpdateFilterRoutingTable(const std::vector<TPlanNode>& plan_nodes, int num_hosts,
      int start_fragment_instance_idx);
};

}

#endif
