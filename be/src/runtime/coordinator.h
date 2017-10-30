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

#include <string>
#include <vector>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <rapidjson/document.h>

#include "common/global-types.h"
#include "common/hdfs.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "runtime/runtime-state.h" // for PartitionStatusMap; TODO: disentangle
#include "scheduling/query-schedule.h"
#include "util/progress-updater.h"

namespace impala {

class CountingBarrier;
class ObjectPool;
class RuntimeState;
class TUpdateCatalogRequest;
class TReportExecStatusParams;
class TPlanExecRequest;
class TRuntimeProfileTree;
class RuntimeProfile;
class QueryResultSet;
class MemTracker;
class PlanRootSink;
class FragmentInstanceState;
class QueryState;


/// Query coordinator: handles execution of fragment instances on remote nodes, given a
/// TQueryExecRequest. As part of that, it handles all interactions with the executing
/// backends; it is also responsible for implementing all client requests regarding the
/// query, including cancellation. Once a query ends, either through cancellation or
/// by returning eos, the coordinator releases resources. (Note that DML requests
/// always end with cancellation, via ImpalaServer::UnregisterQuery()/
/// ImpalaServer::CancelInternal()/ClientRequestState::Cancel().)
///
/// The coordinator monitors the execution status of fragment instances and aborts the
/// entire query if an error is reported by any of them.
///
/// Queries that have results have those results fetched by calling GetNext(). Results
/// rows are produced by a fragment instance that always executes on the same machine as
/// the coordinator.
///
/// Thread-safe, with the exception of GetNext().
//
/// A typical sequence of calls for a single query (calls under the same numbered
/// item can happen concurrently):
/// 1. client: Exec()
/// 2. client: Wait()/client: Cancel()/backend: UpdateBackendExecStatus()
/// 3. client: GetNext()*/client: Cancel()/backend: UpdateBackendExecStatus()
///
/// The implementation ensures that setting an overall error status and initiating
/// cancellation of all fragment instances is atomic.
///
/// TODO: move into separate subdirectory and move nested classes into separate files
/// and unnest them
/// TODO: clean up locking behavior; in particular, clarify dependency on lock_
/// TODO: clarify cancellation path; in particular, cancel as soon as we return
/// all results
class Coordinator { // NOLINT: The member variables could be re-ordered to save space
 public:
  Coordinator(const QuerySchedule& schedule, RuntimeProfile::EventSequence* events);
  ~Coordinator();

  /// Initiate asynchronous execution of a query with the given schedule. When it returns,
  /// all fragment instances have started executing at their respective backends.
  /// A call to Exec() must precede all other member function calls.
  Status Exec() WARN_UNUSED_RESULT;

  /// Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  /// query doesn't return rows, until the query finishes or is cancelled.
  /// A call to Wait() must precede all calls to GetNext().
  /// Multiple calls to Wait() are idempotent and it is okay to issue multiple
  /// Wait() calls concurrently.
  Status Wait() WARN_UNUSED_RESULT;

  /// Fills 'results' with up to 'max_rows' rows. May return fewer than 'max_rows'
  /// rows, but will not return more.
  ///
  /// If *eos is true, execution has completed. Subsequent calls to GetNext() will be a
  /// no-op.
  ///
  /// GetNext() will not set *eos=true until all fragment instances have either completed
  /// or have failed.
  ///
  /// Returns an error status if an error was encountered either locally or by any of the
  /// remote fragments or if the query was cancelled.
  ///
  /// GetNext() is not thread-safe: multiple threads must not make concurrent GetNext()
  /// calls (but may call any of the other member functions concurrently with GetNext()).
  Status GetNext(QueryResultSet* results, int max_rows, bool* eos) WARN_UNUSED_RESULT;

  /// Cancel execution of query. This includes the execution of the local plan fragment,
  /// if any, as well as all plan fragments on remote nodes. Sets query_status_ to the
  /// given cause if non-NULL. Otherwise, sets query_status_ to Status::CANCELLED.
  /// Idempotent.
  void Cancel(const Status* cause = nullptr);

  /// Updates execution status of a particular backend as well as Insert-related
  /// status (per_partition_status_ and files_to_move_). Also updates
  /// num_remaining_backends_ and cancels execution if the backend has an error status.
  Status UpdateBackendExecStatus(const TReportExecStatusParams& params)
      WARN_UNUSED_RESULT;

  /// Only valid to call after Exec().
  QueryState* query_state() const { return query_state_; }

  /// Get cumulative profile aggregated over all fragments of the query.
  /// This is a snapshot of the current state of execution and will change in
  /// the future if not all fragments have finished execution.
  RuntimeProfile* query_profile() const { return query_profile_; }

  const TUniqueId& query_id() const;

  MemTracker* query_mem_tracker() const;

  /// This is safe to call only after Wait()
  const PartitionStatusMap& per_partition_status() { return per_partition_status_; }

  /// Returns the latest Kudu timestamp observed across any backends where DML into Kudu
  /// was executed, or 0 if there were no Kudu timestamps reported.
  /// This should only be called after Wait().
  uint64_t GetLatestKuduInsertTimestamp() const;

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

  /// Get a copy of the current exec summary. Thread-safe.
  void GetTExecSummary(TExecSummary* exec_summary);

  /// Receive a local filter update from a fragment instance. Aggregate that filter update
  /// with others for the same filter ID into a global filter. If all updates for that
  /// filter ID have been received (may be 1 or more per filter), broadcast the global
  /// filter to fragment instances.
  void UpdateFilter(const TUpdateFilterParams& params);

  /// Adds to 'document' a serialized array of all backends in a member named
  /// 'backend_states'.
  void BackendsToJson(rapidjson::Document* document);

 private:
  class BackendState;
  struct FilterTarget;
  class FilterState;
  class FragmentStats;

  const QuerySchedule schedule_;

  /// copied from TQueryExecRequest; constant across all fragments
  TQueryCtx query_ctx_;

  /// copied from TQueryExecRequest, governs when to call ReportQuerySummary
  TStmtType::type stmt_type_;

  /// BackendStates for all execution backends, including the coordinator.
  /// All elements are non-nullptr. Owned by obj_pool(). Populated by
  /// InitBackendExec().
  std::vector<BackendState*> backend_states_;

  // index into backend_states_ for coordinator fragment; -1 if no coordinator fragment
  int coord_backend_idx_ = -1;

  /// The QueryState for this coordinator. Set in Exec(). Released in TearDown().
  QueryState* query_state_ = nullptr;

  /// Non-null if and only if the query produces results for the client; i.e. is of
  /// TStmtType::QUERY. Coordinator uses these to pull results from plan tree and return
  /// them to the client in GetNext(), and also to access the fragment instance's runtime
  /// state.
  ///
  /// Result rows are materialized by this fragment instance in its own thread. They are
  /// materialized into a QueryResultSet provided to the coordinator during GetNext().
  ///
  /// Not owned by this class. Set in Exec(). Reset to nullptr (and the implied
  /// reference of QueryState released) in TearDown().
  FragmentInstanceState* coord_instance_ = nullptr;

  /// Not owned by this class. Set in Exec(). Reset to nullptr in TearDown() or when
  /// GetNext() hits eos.
  PlanRootSink* coord_sink_ = nullptr;

  /// True if the query needs a post-execution step to tidy up
  bool needs_finalization_ = false;

  /// Only valid if needs_finalization is true
  TFinalizeParams finalize_params_;

  /// ensures single-threaded execution of Wait(); must not hold lock_ when acquiring this
  boost::mutex wait_lock_;

  bool has_called_wait_ = false;  // if true, Wait() was called; protected by wait_lock_

  /// Keeps track of number of completed ranges and total scan ranges.
  ProgressUpdater progress_;

  /// Total number of filter updates received (always 0 if filter mode is not
  /// GLOBAL). Excludes repeated broadcast filter updates. Set in Exec().
  RuntimeProfile::Counter* filter_updates_received_ = nullptr;

  /// The filtering mode for this query. Set in constructor.
  TRuntimeFilterMode::type filter_mode_;

  /// Tracks the memory consumed by runtime filters during aggregation. Child of
  /// the query mem tracker in 'query_state_' and set in Exec(). Stored in
  /// query_state_->obj_pool() so it has same lifetime as other MemTrackers.
  MemTracker* filter_mem_tracker_ = nullptr;

  /// Object pool owned by the coordinator.
  boost::scoped_ptr<ObjectPool> obj_pool_;

  /// Execution summary for a single query.
  /// A wrapper around TExecSummary, with supporting structures.
  struct ExecSummary {
    TExecSummary thrift_exec_summary;

    /// See the ImpalaServer class comment for the required lock acquisition order.
    /// The caller must not block while holding the lock.
    SpinLock lock;

    /// A mapping of plan node ids to index into thrift_exec_summary.nodes
    boost::unordered_map<TPlanNodeId, int> node_id_to_idx_map;

    void Init(const QuerySchedule& query_schedule);
  };

  ExecSummary exec_summary_;

  /// Aggregate counters for the entire query. Lives in 'obj_pool_'.
  RuntimeProfile* query_profile_ = nullptr;

  /// Protects all fields below. This is held while making RPCs, so this lock should
  /// only be acquired if the acquiring thread is prepared to wait for a significant
  /// time.
  /// TODO: clarify to what extent the fields below need to be protected by lock_
  /// Lock ordering is
  /// 1. wait_lock_
  /// 2. lock_
  /// 3. BackendState::lock_
  /// 4. filter_lock_
  boost::mutex lock_;

  /// Overall status of the entire query; set to the first reported fragment error
  /// status or to CANCELLED, if Cancel() is called.
  Status query_status_;

  /// If true, the query is done returning all results.  It is possible that the
  /// coordinator still needs to wait for cleanup on remote fragments (e.g. queries
  /// with limit)
  /// Once this is set to true, errors from execution backends are ignored.
  bool returned_all_results_ = false;

  /// If there is no coordinator fragment, Wait() simply waits until all
  /// backends report completion by notifying on backend_completion_cv_.
  /// Tied to lock_.
  boost::condition_variable backend_completion_cv_;

  /// Count of the number of backends for which done != true. When this
  /// hits 0, any Wait()'ing thread is notified
  int num_remaining_backends_ = 0;

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

  /// Event timeline for this query. Not owned.
  RuntimeProfile::EventSequence* query_events_ = nullptr;

  /// Indexed by fragment idx (TPlanFragment.idx). Filled in InitFragmentStats(),
  /// elements live in obj_pool().
  std::vector<FragmentStats*> fragment_stats_;

  /// total time spent in finalization (typically 0 except for INSERT into hdfs tables)
  RuntimeProfile::Counter* finalization_timer_ = nullptr;

  /// Barrier that is released when all calls to ExecRemoteFragment() have
  /// returned, successfully or not. Initialised during Exec().
  boost::scoped_ptr<CountingBarrier> exec_complete_barrier_;

  /// Protects filter_routing_table_.
  SpinLock filter_lock_;

  /// Map from filter ID to filter.
  typedef boost::unordered_map<int32_t, FilterState> FilterRoutingTable;
  FilterRoutingTable filter_routing_table_;

  /// Set to true when all calls to UpdateFilterRoutingTable() have finished, and it's
  /// safe to concurrently read from filter_routing_table_.
  bool filter_routing_table_complete_ = false;

  /// True if and only if ReleaseExecResources() has been called.
  bool released_exec_resources_ = false;

  /// Returns a local object pool.
  ObjectPool* obj_pool() { return obj_pool_.get(); }

  /// Only valid *after* calling Exec(). Return nullptr if the running query does not
  /// produce any rows.
  RuntimeState* runtime_state();

  /// Returns a pretty-printed table of the current filter state.
  std::string FilterDebugString();

  /// Cancels all fragment instances. Assumes that lock_ is held. This may be called when
  /// the query is not being cancelled in the case where the query limit is reached.
  void CancelInternal();

  /// Acquires lock_ and updates query_status_ with 'status' if it's not already
  /// an error status, and returns the current query_status_. The status may be
  /// due to an error in a specific fragment instance, or it can be a general error
  /// not tied to a specific fragment instance.
  /// Calls CancelInternal() when switching to an error status.
  /// When an error is due to a specific fragment instance, 'is_fragment_failure' must
  /// be true and 'failed_fragment' is the fragment_id that has failed, used for error
  /// reporting. For a general error not tied to a specific instance,
  /// 'is_fragment_failure' must be false and 'failed_fragment' will be ignored.
  /// 'backend_hostname' is used for error reporting in either case.
  Status UpdateStatus(const Status& status, const std::string& backend_hostname,
      bool is_fragment_failure, const TUniqueId& failed_fragment) WARN_UNUSED_RESULT;

  /// Update per_partition_status_ and files_to_move_.
  void UpdateInsertExecStatus(const TInsertExecStatus& insert_exec_status);

  /// Returns only when either all execution backends have reported success or the query
  /// is in error. Returns the status of the query.
  /// It is safe to call this concurrently, but any calls must be made only after Exec().
  Status WaitForBackendCompletion() WARN_UNUSED_RESULT;

  /// Initializes fragment_stats_ and query_profile_. Must be called before
  /// InitBackendStates().
  void InitFragmentStats();

  /// Populates backend_states_ based on schedule_.fragment_exec_params().
  /// BackendState depends on fragment_stats_, which is why InitFragmentStats()
  /// must be called before this function.
  void InitBackendStates();

  /// Computes execution summary info strings for fragment_stats_ and query_profile_.
  /// This is assumed to be called at the end of a query -- remote fragments'
  /// profiles must not be updated while this is running.
  void ComputeQuerySummary();

  /// TODO: move the next 3 functions into a separate class

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

  /// Moves all temporary staging files to their final destinations.
  Status FinalizeSuccessfulInsert() WARN_UNUSED_RESULT;

  /// Perform any post-query cleanup required. Called by Wait() only after all fragment
  /// instances have returned, or if the query has failed, in which case it only cleans up
  /// temporary data rather than finishing the INSERT in flight.
  Status FinalizeQuery() WARN_UNUSED_RESULT;

  /// Populates backend_states_, starts query execution at all backends in parallel, and
  /// blocks until startup completes.
  void StartBackendExec();

  /// Calls CancelInternal() and returns an error if there was any error starting
  /// backend execution.
  /// Also updates query_profile_ with the startup latency histogram.
  Status FinishBackendStartup() WARN_UNUSED_RESULT;

  /// Build the filter routing table by iterating over all plan nodes and collecting the
  /// filters that they either produce or consume.
  void InitFilterRoutingTable();

  /// Releases all resources associated with query execution. Acquires lock_. Idempotent.
  void ReleaseExecResources();

  /// Same as ReleaseExecResources() except the lock must be held by the caller.
  void ReleaseExecResourcesLocked();
};

}

#endif
