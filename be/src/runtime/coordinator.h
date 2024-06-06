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

#include <list>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <boost/unordered_map.hpp>
#include <rapidjson/document.h>

#include <gtest/gtest_prod.h> // for FRIEND_TEST

#include "common/global-types.h"
#include "common/status.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/data_stream_service.pb.h"
#include "runtime/dml-exec-state.h"
#include "runtime/query-exec-params.h"
#include "util/counting-barrier.h"
#include "util/progress-updater.h"
#include "util/promise.h"
#include "util/runtime-profile-counters.h"
#include "util/spinlock.h"
#include "util/container-util.h"

namespace kudu {
namespace rpc {
class RpcContext;
} // namespace rpc
} // namespace kudu

namespace impala {

class AuxErrorInfoPB;
class ClientRequestState;
class FragmentExecParamsPB;
class FragmentInstanceState;
class MemTracker;
class ObjectPool;
class PlanRootSink;
class QueryDriver;
class QueryResultSet;
class QueryState;
class ReportExecStatusRequestPB;
class RuntimeProfile;
class RuntimeState;
class TPlanExecRequest;
class TRuntimeProfileTree;
class TUpdateCatalogRequest;

/// Query coordinator: handles execution of fragment instances on remote nodes, given a
/// TQueryExecRequest. As part of that, it handles all interactions with the executing
/// backends; it is also responsible for implementing all client requests regarding the
/// query, including cancellation. Once a query ends, either by returning EOS, through
/// client cancellation, returning an error, or by finalizing a DML request, the
/// coordinator releases resources.
///
/// The coordinator monitors the execution status of fragment instances and aborts the
/// entire query if an error is reported by any of them.
///
/// Queries that have results have those results fetched by calling GetNext(). Results
/// rows are produced by a fragment instance that always executes on the same machine as
/// the coordinator.
///
/// Thread-safe except where noted.
///
/// A typical sequence of calls for a single query (calls under the same numbered
/// item can happen concurrently):
/// 1. client: Exec()
/// 2. client: Wait()/client: Cancel()/backend: UpdateBackendExecStatus()
/// 3. client: GetNext()*/client: Cancel()/backend: UpdateBackendExecStatus()
///
/// A query is considered to be executing until one of three things occurs:
/// 1. An error is encountered. Backend cancellation is automatically initiated for all
///    backends that haven't yet completed and the overall query status is set to the
///    first (non-cancelled) encountered error status.
/// 2. The query is cancelled via an explicit Cancel() call. The overall query status
///    is set to CANCELLED and cancellation is initiated for all backends still
///    executing (without an error status).
/// 3. The query has returned all rows. The overall query status is OK (and remains
///    OK). Client cancellation is no longer possible and subsequent backend errors are
///    ignored. The coordinator initiates cancellation for all backends and waits for the
///    final exec status reports.
///
/// Lifecycle: this object must not be destroyed until after one of the three states
/// above is reached (error, cancelled, or EOS) to ensure resources are released.
/// There is work that needs to be done as part of the transition into each of the
/// above states. That work is done by the thread that triggered the state transition
/// *after* the atomic state transition by calling HandleExecStateTransition(). If
/// another thread depends on that work being complete, they can wait on the 'finalized_'
/// promise.
///
/// Lock ordering: (lower-numbered acquired before higher-numbered)
/// 1. wait_lock_
/// 2. Coordinator::FilterRoutingTable::lock
/// 3. exec_state_lock_, backend_states_init_lock_, FilterState::lock_, ExecSummary::lock
/// 4. Coordinator::BackendState::lock_ (leafs)
///
/// TODO: move into separate subdirectory and move nested classes into separate files
/// and unnest them
class Coordinator { // NOLINT: The member variables could be re-ordered to save space
 public:
  static const std::string PROFILE_EVENT_LABEL_FIRST_ROW_FETCHED;

  Coordinator(ClientRequestState* parent, const TExecRequest& exec_request,
      const QuerySchedulePB& query_schedule, RuntimeProfile::EventSequence* events);
  ~Coordinator();

  /// Initiate asynchronous execution of a query with the given schedule. When it
  /// returns, all fragment instances have started executing at their respective
  /// backends. Exec() must be called exactly once and a call to Exec() must precede
  /// all other member function calls.
  Status Exec() WARN_UNUSED_RESULT;

  /// Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  /// query doesn't return rows, until the query finishes or is cancelled. A call to
  /// Wait() must precede all calls to GetNext().  Multiple calls to Wait() are
  /// idempotent and it is okay to issue multiple Wait() calls concurrently. Only valid to
  /// call after Exec() returns successfully.
  Status Wait() WARN_UNUSED_RESULT;

  /// Fills 'results' with up to 'max_rows' rows. May return fewer than 'max_rows'
  /// rows, but will not return more. If *eos is true, all rows have been returned.
  /// Returns a non-OK status if an error was encountered either locally or by any of
  /// the executing backends, or if the query was cancelled via Cancel().  After *eos
  /// is true, subsequent calls to GetNext() will be a no-op. 'block_on_wait_time_us' is
  /// the amount of time the client spent (in microseconds) waiting in BlockOnWait(). It
  /// is used to set the correct timeout value for
  /// PlanRootSink::GetNext(..., int64_t timeout_us).
  ///
  /// GetNext() is not thread-safe: multiple threads must not make concurrent GetNext()
  /// calls.
  Status GetNext(QueryResultSet* results, int max_rows, bool* eos,
      int64_t block_on_wait_time_us) WARN_UNUSED_RESULT;

  /// Cancel execution of query and sets the overall query status to CANCELLED if the
  /// query is still executing. Idempotent. If 'wait_until_finalized' is true and another
  /// thread is cancelling 'coord_', block until the query profile is completed, e.g.
  /// if ComputeQuerySummary() is running concurrently. This is only used by the thread
  /// unregistering the query - other threads should return immediately if the query
  /// is in the process of being cancelled.
  void Cancel(bool wait_until_finalized=false);

  /// Called by the report status RPC handler to update execution status of a particular
  /// backend as well as dml_exec_state_ and the profile. This may block if exec RPCs are
  /// pending. 'request' contains details of the status update. 'thrift_profiles' contains
  /// Thrift runtime profiles of all fragment instances from the backend.
  Status UpdateBackendExecStatus(const ReportExecStatusRequestPB& request,
      const TRuntimeProfileForest& thrift_profiles) WARN_UNUSED_RESULT;

  /// Returns the time in ms since the latest report was received for the backend which
  /// has gone the longest without a report being received, and sets 'address' to the host
  /// for that backend. Only valid to call if Exec() has returned successfully. May return
  /// 0, for example if all of the backends have already completed, in which case
  /// 'address' will not be set.
  int64_t GetMaxBackendStateLagMs(NetworkAddressPB* address);

  /// Get cumulative profile aggregated over all fragments of the query.
  /// This is a snapshot of the current state of execution and will change in
  /// the future if not all fragments have finished execution.
  RuntimeProfile* query_profile() const { return query_profile_; }

  /// Safe to call only after Exec().
  MemTracker* query_mem_tracker() const;

  /// Safe to call only after Wait().
  DmlExecState* dml_exec_state() { return &dml_exec_state_; }

  /// Return error log for coord and all the fragments. The error messages from the
  /// individual fragment instances are merged into a single output to retain readability.
  std::string GetErrorLog();

  const ProgressUpdater& scan_progress() const { return scan_progress_; }

  const ProgressUpdater& query_progress() const { return query_progress_; }

  /// Get a copy of the current exec summary. Thread-safe.
  void GetTExecSummary(TExecSummary* exec_summary);

  /// Receive a local filter update from a fragment instance. Aggregate that filter update
  /// with others for the same filter ID into a global filter. If all updates for that
  /// filter ID have been received (may be 1 or more per filter), broadcast the global
  /// filter to fragment instances.
  void UpdateFilter(const UpdateFilterParamsPB& params, kudu::rpc::RpcContext* context);

  /// Adds to 'document' a serialized array of all backends in a member named
  /// 'backend_states'.
  void BackendsToJson(rapidjson::Document* document);

  /// Adds to 'document' a serialized array of all backend names and stats of all fragment
  /// instances running on each backend in a member named 'backend_instances'.
  void FInstanceStatsToJson(rapidjson::Document* document);

  /// Struct to aggregate resource usage information at the finstance, backend and
  /// query level.
  struct ResourceUtilization {
    /// Peak memory used for this query (value of the query memtracker's
    /// peak_consumption()). At the finstance or backend level, this is the
    /// peak value for that backend or if at the query level, this is the max
    /// peak value from any backend.
    int64_t peak_per_host_mem_consumption = 0;

    /// Total peak memory usage for this query at all backend.
    /// Note that it is the sum of peaks, not the peak of the sum.
    int64_t total_peak_mem_usage = 0;

    /// Total bytes read across all scan nodes.
    int64_t bytes_read = 0;

    /// Total bytes sent by instances that did not contain a scan node.
    int64_t exchange_bytes_sent = 0;

    /// Total bytes sent by instances that contained a scan node.
    int64_t scan_bytes_sent = 0;

    /// Total user cpu consumed.
    int64_t cpu_user_ns = 0;

    /// Total system cpu consumed.
    int64_t cpu_sys_ns = 0;

    /// Total num rows produced by each join node. The key is join node id.
    std::map<int32_t, int64_t> per_join_rows_produced;

    /// Merge utilization from 'other' into this.
    void Merge(const ResourceUtilization& other) {
      peak_per_host_mem_consumption =
          std::max(peak_per_host_mem_consumption, other.peak_per_host_mem_consumption);
      total_peak_mem_usage += other.peak_per_host_mem_consumption;
      bytes_read += other.bytes_read;
      exchange_bytes_sent += other.exchange_bytes_sent;
      scan_bytes_sent += other.scan_bytes_sent;
      cpu_user_ns += other.cpu_user_ns;
      cpu_sys_ns += other.cpu_sys_ns;
      MergeMapValues(other.per_join_rows_produced, &per_join_rows_produced);
    }

    /// Max join rows produced across join nodes
    const std::pair<int32_t, int64_t> MaxJoinNodeRowsProduced() const {
      std::pair<int32_t, int64_t> entryWithMaxValue = std::make_pair(0, 0);
      for (const auto& entry : per_join_rows_produced) {
        if (entry.second > entryWithMaxValue.second)
          entryWithMaxValue = std::make_pair(entry.first, entry.second);
      }
      return entryWithMaxValue;
    }
  };

  /// Aggregate resource utilization for the query (i.e. across all backends based on the
  /// latest status reports received from those backends).
  ResourceUtilization ComputeQueryResourceUtilization();

  /// Return the backends and their associated resource utilization.
  std::list<std::pair<NetworkAddressPB, ResourceUtilization>>
      BackendResourceUtilization();

  /// Return the backends in 'candidates' that still have at least one fragment instance
  /// executing on them. The returned backends may not be in the same order as the input.
  std::vector<NetworkAddressPB> GetActiveBackends(
      const std::vector<NetworkAddressPB>& candidates);

 private:
  class BackendState;
  class BackendResourceState;
  struct FilterTarget;
  struct FilterRoutingTable;
  class FilterState;
  class FragmentStats;

  /// The parent QueryDriver object for this coordinator. The reference is set in the
  /// constructor. It always outlives the coordinator.
  QueryDriver* parent_query_driver_;

  /// The parent ClientRequestState object for this coordinator. The reference is set in
  /// the constructor. It always outlives the coordinator.
  ClientRequestState* parent_request_state_;

  /// Contains references to Thrift and Protobuf structs constituting the execution
  /// parameters. These structs are owned by the QueryDriver and ClientRequestState.
  QueryExecParams exec_params_;

  /// Copied from TQueryExecRequest, governs when finalization occurs. Set in Exec().
  TStmtType::type stmt_type_;

  /// BackendStates for all execution backends, including the coordinator. All elements
  /// are non-nullptr and owned by obj_pool(). Populated by Exec()/InitBackendStates().
  std::vector<BackendState*> backend_states_;

  /// A map from the NetworkAddressPB of a backend to the BackendState running on the
  /// NetworkAddressPB. All values are non-nullptr and owned by obj_pool(). The address
  /// is the kRPC address (Coordinator::BackendState::krpc_impalad_address) of the
  /// Backend. This map is distinct from QuerySchedulePB::per_backend_exec_params(),
  /// which uses the Thrift address as the key rather than the kRPC address.
  boost::unordered_map<NetworkAddressPB, BackendState*> addr_to_backend_state_;

  /// Protects the population of backend_states_ vector (not the BackendState objects).
  /// Used when accessing backend_states_ if it's not guaranteed that
  /// InitBackendStates() has completed.
  SpinLock backend_states_init_lock_;

  /// The QueryState for this coordinator. Reference taken in Exec(). Reference
  /// released in destructor.
  QueryState* query_state_ = nullptr;

  /// Non-null if and only if the query produces results for the client; i.e. is of
  /// TStmtType::QUERY. Coordinator uses these to pull results from plan tree and return
  /// them to the client in GetNext(), and also to access the fragment instance's runtime
  /// state.
  ///
  /// Result rows are materialized by this fragment instance in its own thread. They are
  /// materialized into a QueryResultSet provided to the coordinator during GetNext().
  ///
  /// Owned by the QueryState. Set in Exec().
  FragmentInstanceState* coord_instance_ = nullptr;

  /// Owned by the QueryState. Set in Exec().
  PlanRootSink* coord_sink_ = nullptr;

  /// ensures single-threaded execution of Wait(). See lock ordering class comment.
  SpinLock wait_lock_;

  /// If true, Wait() was called. When read / written in Wait() the wait_lock_ should
  /// still be acquired to ensure Wait() is only executed once.
  AtomicBool has_called_wait_;

  BackendResourceState* backend_resource_state_ = nullptr;

  /// Keeps track of number of completed ranges and total scan ranges. Initialized by
  /// Exec().
  ProgressUpdater scan_progress_;

  /// Keeps track of number of completed fragment instances and total fragment instances.
  /// Initialized by Exec().
  ProgressUpdater query_progress_;

  /// Aggregate counters for the entire query. Lives in 'obj_pool_'. Set in Exec().
  RuntimeProfile* query_profile_ = nullptr;

  /// Aggregate counters for backend host resource usage and other per-host information.
  /// Will contain a child profile for each backend host that participates in the query
  /// execution. Lives in 'obj_pool_'. Set in Exec().
  RuntimeProfile* host_profiles_ = nullptr;

  /// Total time spent in finalization (typically 0 except for INSERT into hdfs
  /// tables). Set in Exec().
  RuntimeProfile::Counter* finalization_timer_ = nullptr;

  /// Total number of filter updates received (always 0 if filter mode is not
  /// GLOBAL). Excludes repeated broadcast filter updates. Set in Exec().
  RuntimeProfile::Counter* filter_updates_received_ = nullptr;

  /// A RuntimeProfile Counter of the number of completed backends. Updated for each
  /// Backend in 'UpdateBackendExecStatus' when 'ApplyExecStatusReport' returns true.
  /// Only valid after InitBackendStates() is called. Does not count the number of
  /// CANCELLED Backends.
  RuntimeProfile::Counter* num_completed_backends_ = nullptr;

  /// The filtering mode for this query. Set in constructor.
  TRuntimeFilterMode::type filter_mode_;

  /// Tracks the memory consumed by runtime filters during aggregation. Child of
  /// the query mem tracker in 'query_state_' and set in Exec(). Stored in
  /// query_state_->obj_pool() so it has same lifetime as other MemTrackers.
  MemTracker* filter_mem_tracker_ = nullptr;

  /// Object pool owned by the coordinator.
  std::unique_ptr<ObjectPool> obj_pool_;

  /// Execution summary for a single query.
  /// A wrapper around TExecSummary, with supporting structures.
  struct ExecSummary {
    TExecSummary thrift_exec_summary;

    /// See the ImpalaServer class comment for the required lock acquisition order.
    /// The caller must not block while holding the lock.
    SpinLock lock;

    /// A mapping of plan node ids to index into thrift_exec_summary.nodes
    boost::unordered_map<TPlanNodeId, int> node_id_to_idx_map;

    /// A mapping of fragment data sink ids to index into thrift_exec_summary.nodes
    boost::unordered_map<TPlanNodeId, int> data_sink_id_to_idx_map;

    void Init(const QueryExecParams& exec_params);
  };

  // Initialized by Exec().
  ExecSummary exec_summary_;

  /// Filled in as the query completes and tracks the results of DML queries.  This is
  /// either the union of the reports from all fragment instances, or taken from the
  /// coordinator fragment: only one of the two can legitimately produce updates.
  DmlExecState dml_exec_state_;

  /// Event timeline for this query. Not owned.
  RuntimeProfile::EventSequence* query_events_ = nullptr;

  /// Indexed by fragment idx (TPlanFragment.idx). Filled in
  /// Exec()/InitFragmentStats(), elements live in obj_pool(). Updated by BackendState
  /// sequentially, without synchronization.
  std::vector<FragmentStats*> fragment_stats_;

  /// Barrier that is released when all backend Exec() rpcs have completed successfully,
  /// or when one fails, in which case the returned value is the error from that rpc.
  TypedCountingBarrier<Status> exec_rpcs_status_barrier_;

  /// True if all Exec() rpcs have completed.
  AtomicBool exec_rpcs_complete_{false};

  /// Barrier that is released when all backends have indicated execution completion,
  /// or when all backends are cancelled due to an execution error or client requested
  /// cancellation. Initialized in StartBackendExec().
  std::unique_ptr<CountingBarrier> backend_exec_complete_barrier_;

  /// Barrier that is released when all Backends have released their admission control
  /// resources.
  CountingBarrier backend_released_barrier_;

  // Protects exec_state_ and exec_status_. exec_state_ can be read independently via
  // the atomic, but the lock is held when writing either field and when reading both
  // fields together.
  SpinLock exec_state_lock_;

  /// EXECUTING: in-flight; the only non-terminal state
  /// RETURNED_RESULTS: GetNext() set eos to true, or for DML, the request is complete
  /// CANCELLED: Cancel() was called (not: someone called CancelBackends())
  /// ERROR: received an error from a backend
  enum class ExecState {
    EXECUTING, RETURNED_RESULTS, CANCELLED, ERROR
  };
  AtomicEnum<ExecState> exec_state_{ExecState::EXECUTING};

  /// Overall execution status; only set on exec_state_ transitions:
  /// - EXECUTING: OK
  /// - RETURNED_RESULTS: OK
  /// - CANCELLED: CANCELLED
  /// - ERROR: error status
  Status exec_status_;

  /// Set when all of the work to transition into a terminal state is complete,
  /// including ComputeQuerySummary().
  Promise<bool> finalized_;

  /// Contains all the state about filters being handled by this coordinator.
  std::unique_ptr<FilterRoutingTable> filter_routing_table_;

  /// True if the first row has been fetched, false otherwise.
  bool first_row_fetched_ = false;

  /// Returns a local object pool.
  ObjectPool* obj_pool() { return obj_pool_.get(); }

  /// Returns request's finalize params, or nullptr if not present. If not present, then
  /// HDFS INSERT finalization is not required.
  const TFinalizeParams* finalize_params() const;

  const TQueryCtx& query_ctx() const;

  const TUniqueId& query_id() const;

  /// Returns a pretty-printed table of the current filter state.
  /// Caller must have exclusive access to filter_lock_.
  std::string FilterDebugString();

  /// Called when the query is done executing due to reaching EOS or client
  /// cancellation. If 'exec_state_' != EXECUTING, does nothing. Otherwise sets
  /// 'exec_state_' to 'state' (must be either CANCELLED or RETURNED_RESULTS), and
  /// finalizes execution (cancels remaining backends if transitioning to CANCELLED;
  /// either way, calls ComputeQuerySummary() and releases resources). Returns the
  /// resulting overall execution status.
  Status SetNonErrorTerminalState(const ExecState state) WARN_UNUSED_RESULT;

  /// Transitions 'exec_state_' given an execution status and returns the resulting
  /// overall status:
  ///
  /// - if the 'status' parameter is ok, no state transition
  /// - if 'exec_state_' is EXECUTING and 'status' is not ok, transitions to ERROR
  /// - if 'exec_state_' is already RETURNED_RESULTS, CANCELLED, or ERROR: does not
  ///   transition state (those are terminal states) however in the case of ERROR,
  ///   status may be updated to a more interesting status.
  ///
  /// Should not be called for (client initiated) cancellation. Call
  /// SetNonErrorTerminalState(CANCELLED) instead.
  ///
  /// 'failed_finstance' is the fragment instance id that has failed (or nullptr if the
  /// failure is not specific to a fragment instance), used for error reporting along
  /// with 'instance_hostname'.
  Status UpdateExecState(const Status& status, const TUniqueId* failed_finstance,
      const string& instance_hostname) WARN_UNUSED_RESULT;

  /// Helper for SetNonErrorTerminalState() and UpdateExecStateIfError(). If the caller
  /// transitioned to a terminal state (which happens exactly once for the lifetime of
  /// the Coordinator object), then finalizes execution (cancels remaining backends if
  /// transitioning to CANCELLED; in all cases releases resources and calls
  /// ComputeQuerySummary()). Must not be called if exec RPCs are pending.
  /// Will block waiting for backends to completed if transitioning to the
  /// RETURNED_RESULTS terminal state. Does not block if already in terminal state or
  /// transitioning to ERROR or CANCELLED.
  void HandleExecStateTransition(const ExecState old_state, const ExecState new_state);

  /// Return true if 'exec_state_' is RETURNED_RESULTS.
  bool ReturnedAllResults() WARN_UNUSED_RESULT {
    return exec_state_.Load() == ExecState::RETURNED_RESULTS;
  }

  /// Return the string representation of 'state'.
  static const char* ExecStateToString(const ExecState state);

  // For DCHECK_EQ, etc of ExecState values.
  friend std::ostream& operator<<(std::ostream& o, const ExecState s) {
    return o << ExecStateToString(s);
  }

  /// Helper for HandleExecStateTransition(). Sends cancellation request to all
  /// executing backends. If 'fire_and_forget' is true, does not wait for the
  /// final exec status reports from the backends. The ExecState state-machine
  /// ensures this is called at most once.
  void CancelBackends(bool fire_and_forget);

  /// Returns only when either all execution backends have reported success or a request
  /// to cancel the backends has already been sent. It is safe to call this concurrently,
  /// but any calls must be made only after Exec().
  void WaitForBackends();

  /// Returns when all Exec() rpcs have completed. Thread-safe.
  void WaitOnExecRpcs();

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

  /// Perform any post-query cleanup required for HDFS (or other Hadoop FileSystem)
  /// INSERT. Called by Wait() only after all fragment instances have returned, or if
  /// the query has failed, in which case it only cleans up temporary data rather than
  /// finishing the INSERT in flight.
  Status FinalizeHdfsDml() WARN_UNUSED_RESULT;

  /// Perform any post-query cleanup required for Result Sink writing. Called by Wait()
  /// only after all fragment instances have returned. It is the responsibility of an
  /// external frontend to clean up the Result Sink location on success or error of the
  /// running query.
  Status FinalizeResultSink() WARN_UNUSED_RESULT;

  /// Helper for Exec(). Populates backend_states_, starts query execution at all
  /// backends in parallel, and blocks until startup completes.
  Status StartBackendExec();

  /// Helper for Exec(). Checks for errors encountered when starting backend execution,
  /// using any non-OK status, if any, as the overall status. Returns the overall
  /// status. Also updates query_profile_ with the startup latency histogram and the
  /// backend_exec_complete_barrier_ if there is any backend which is already done (only
  /// possible at this point if no fragment instances were assigned to it).
  Status FinishBackendStartup() WARN_UNUSED_RESULT;

  /// Build the filter routing table by iterating over all plan nodes and data sinks and
  /// collecting the filters that they either produce or consume.
  void InitFilterRoutingTable();

  /// Helper for InitFilterRoutingTable() that adds a source join node or join build sink
  /// for 'filter' to the routing table. 'src_fragment_params' is the parameters for the
  /// fragment containing the join node (if build is integrated) or build sink (if the
  /// build is separate). 'num_instances' and 'num_backends' are the number of instances
  /// and backends that the fragment runs on.
  void AddFilterSource(const FragmentExecParamsPB& src_fragment_params, int num_instances,
      int num_backends, const TRuntimeFilterDesc& filter, int join_node_id);

  /// Helper for HandleExecStateTransition(). Releases all resources associated with
  /// query execution. The ExecState state-machine ensures this is called exactly once.
  void ReleaseExecResources();

  /// Helper for HandleExecStateTransition(). Releases admission control resources for
  /// use by other queries. This should only be called if one of following
  /// preconditions is satisfied for each backend on which the query is executing:
  ///
  /// * The backend finished execution. Rationale: the backend isn't consuming
  ///   resources.
  /// * A cancellation RPC was delivered to the backend.
  ///   Rationale: the backend will be cancelled and release resources soon. By the
  ///   time a newly admitted query fragment starts up on the backend and starts consuming
  ///   resources, the resources from this query will probably have been released.
  /// * Sending the cancellation RPC to the backend failed
  ///   Rationale: the backend is either down or will tear itself down when it next tries
  ///   to send a status RPC to the coordinator. It's possible that the fragment will be
  ///   slow to tear down and we could overadmit and cause query failures. However, given
  ///   the communication errors, we need to proceed based on incomplete information about
  ///   the state of the cluster. We choose to optimistically assume that the backend will
  ///   tear itself down in a timely manner and admit more queries instead of
  ///   pessimistically queueing queries while we wait for a response from a backend that
  ///   may never come.
  ///
  /// Calling WaitForBackends() or CancelBackends() before this function is sufficient
  /// to satisfy the above preconditions. If the query has an expensive finalization
  /// step post query execution (e.g. a DML statement), then this should be called
  /// after that completes to avoid over-admitting queries.
  ///
  /// The ExecState state-machine ensures this is called exactly once.
  void ReleaseQueryAdmissionControlResources();

  /// Helper method to release admission control resource for the given vector of
  /// BackendStates. Resources are released using
  /// AdmissionController::ReleaseQueryBackends which releases the admitted memory used
  /// by each BackendState and decrements the number of running queries on the host
  /// running the BackendState.
  void ReleaseBackendAdmissionControlResources(
      const std::vector<BackendState*>& backend_states);

  /// Checks the exec_state_ of the query and returns true if the query is executing.
  bool IsExecuting();

  /// Helper function for UpdateBackendExecStatus that iterates through the given vector
  /// of AuxErrorInfoPB objects and uses each one to check if any nodes should be
  /// blacklisted. AuxErrorInfoPB contains additional error information about why the
  /// fragment failed, beyond what is available in the
  /// ReportExecStatusRequestPB::overall_status field. This method uses information in
  /// AuxErrorInfoPB to classify specific nodes as "faulty" and then blacklists them. A
  /// node might be considered "faulty" if, for example, a RPC to that node failed, or a
  /// fragment on that node failed due to a disk IO error.
  /// 'status' is the Status of the given BackendState. 'backend_state' is the
  /// BackendState that reported an error.
  /// Returns the Status object used when blacklisting a backend, or Status::OK if no
  /// backends were blacklisted.
  Status UpdateBlacklistWithAuxErrorInfo(std::vector<AuxErrorInfoPB>* aux_error_info,
      const Status& status, BackendState* backend_state) WARN_UNUSED_RESULT;

  /// Helper function for UpdateBackendExecStatus that check if the backend node should
  /// be blacklisted based on the reported backend error.
  /// 'status' is the Status of the given BackendState. 'backend_state' is the
  /// BackendState that reported an error.
  /// Returns the Status object used when blacklisting a backend, or Status::OK if no
  /// backends were blacklisted.
  Status UpdateBlacklistWithBackendState(
      const Status& status, BackendState* backend_state) WARN_UNUSED_RESULT;

  /// Called if the Exec RPC to the given vector of BackendStates failed. Currently, just
  /// triggers a retry of the query.
  void HandleFailedExecRpcs(std::vector<BackendState*> failed_backend_states);

  /// Deletes the query-level staging directory.
  Status DeleteQueryLevelStagingDir();

  /// BackendState and BackendResourceState are private to the Coordinator class, so mark
  /// all tests in CoordinatorBackendStateTest as friends.
  friend class CoordinatorBackendStateTest;
  FRIEND_TEST(CoordinatorBackendStateTest, StateMachine);
  FRIEND_TEST(CoordinatorBackendStateTest, CoordinatorOnly);
  FRIEND_TEST(CoordinatorBackendStateTest, TimedRelease);
  FRIEND_TEST(CoordinatorBackendStateTest, BatchedRelease);
};

}
