// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_RUNTIME_COORDINATOR_H
#define IMPALA_RUNTIME_COORDINATOR_H

#include <vector>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "common/status.h"
#include "common/global-types.h"
#include "util/runtime-profile.h"
#include "runtime/runtime-state.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/Frontend_types.h"

namespace impala {

class DataStreamMgr;
class DataSink;
class ExecStats;
class RowBatch;
class RowDescriptor;
class PlanFragmentExecutor;
class ObjectPool;
class RuntimeState;
class ImpalaInternalServiceClient;
class Expr;
class ExecEnv;
class TCatalogUpdate;
class TQueryExecRequest;
class TReportExecStatusParams;
class TRowBatch;
class TPlanExecRequest;
class TPlanExecParams;
class TRuntimeProfileTree;
class RuntimeProfile;

// Query coordinator: handles execution of plan fragments on remote nodes, given
// a TQueryExecRequest. As part of that, it handles all interactions with the
// executing backends; it is also responsible for implementing all client requests
// regarding the query, including cancellation.
// The coordinator fragment is executed locally in the calling thread, all other
// fragments are sent to remote nodes. The coordinator also monitors
// the execution status of the remote fragments and aborts the entire query if an error
// occurs, either in any of the remote fragments or in the local fragment.
// Once a query has finished executing and all results have been returned either to the
// caller of GetNext() or a data sink, execution_completed() will return true. If the
// query is aborted, execution_completed should also be set to true.
// Coordinator is thread-safe, with the exception of GetNext().
//
// A typical sequence of calls for a single query (calls under the same numbered
// item can happen concurrently):
// 1. client: Exec()
// 2. client: Wait()/client: Cancel()/backend: UpdateFragmentExecStatus()
// 3. client: GetNext()*/client: Cancel()/backend: UpdateFragmentExecStatus()
//
// The implementation ensures that setting an overall error status and initiating
// cancellation of local and all remote fragments is atomic.
//
// TODO:
// - add profile counters for coordinator (how much time do we spend in startup?)
class Coordinator {
 public:
  Coordinator(ExecEnv* exec_env, ExecStats* exec_stats);
  ~Coordinator();

  // Initiate asynchronous execution of query. Returns as soon as all plan fragments
  // have started executing at their respective backends.
  // 'Request' must contain at least a coordinator plan fragment (ie, can't
  // be for a query like 'SELECT 1').
  // The destination host/port of the 2nd fragment (the one sending to the coordinator)
  // is set to FLAGS_coord_host/FLAGS_backend_port.
  // A call to Exec() must precede all other member function calls.
  Status Exec(TQueryExecRequest* request);

  // Blocks until result rows are ready to be retrieved via GetNext(), or, if the
  // query doesn't return rows, until the query finishes or is cancelled.
  // A call to Wait() must precede all calls to GetNext().
  // Multiple calls to Wait() are idempotent and it is okay to issue multiple
  // Wait() calls concurrently.
  Status Wait();

  // Returns tuples from the coordinator fragment. Any returned tuples are valid until
  // the next GetNext() call. If *batch is NULL, execution has completed and GetNext()
  // must not be called again.
  // GetNext() will not set *batch=NULL until all backends have
  // either completed or have failed. 
  // It is safe to call GetNext() even in the case where there is no coordinator fragment
  // (distributed INSERT).
  // '*batch' is owned by the underlying PlanFragmentExecutor and must not be deleted.
  // *state is owned by the caller, and must not be deleted.
  // Returns an error status if an error was encountered either locally or by
  // any of the remote fragments or if the query was cancelled.
  // GetNext() is not thread-safe: multiple threads must not make concurrent
  // GetNext() calls (but may call any of the other member functions concurrently
  // with GetNext()).
  Status GetNext(RowBatch** batch, RuntimeState* state);

  // Cancel execution of query. This includes the execution of the local plan fragment,
  // if any, as well as all plan fragments on remote nodes.
  void Cancel();

  // Updates status and query execution metadata of a particular
  // fragment; if 'status' is an error status or if 'done' is true,
  // considers the plan fragment to have finished execution. Assumes
  // that calls to UpdateFragmentExecStatus() won't happen
  // concurrently for the same backend.
  // If 'status' is an error status, also cancel execution of the query via a call
  // to CancelInternal().
  Status UpdateFragmentExecStatus(const TReportExecStatusParams& params);

  // only valid *after* calling Exec(), and may return NULL if there is no executor
  RuntimeState* runtime_state();
  const RowDescriptor& row_desc() const;

  // Get cumulative profile aggregated over all fragments of the query.
  // This is a snapshot of the current state of execution and will change in
  // the future if not all fragments have finished execution.
  RuntimeProfile* query_profile() const { return query_profile_.get(); }

  ExecStats* exec_stats() { return exec_stats_; }
  const TUniqueId& query_id() const { return query_id_; }

  // This is safe to call only after Wait()
  const PartitionRowCount& partition_row_counts() { return partition_row_counts_; }

  // Gathers all updates to the catalog required once this query has completed execution.
  // Returns true if a catalog update is required, false otherwise.
  // Must only be called after Wait()
  bool PrepareCatalogUpdate(TCatalogUpdate* catalog_update);

  // Return error log for coord and all the fragments
  std::string GetErrorLog();

 private:
  ExecEnv* exec_env_;
  TUniqueId query_id_;

  // map from id of a scan node to the throughput counter in this->profile
  typedef std::map<PlanNodeId, RuntimeProfile::Counter*> ThroughputCounterMap;
  
  // Execution state of a particular fragment.
  // Concurrent accesses:
  // - GetNodeThroughput() called when coordinator's profile is printed
  // - updates through UpdateFragmentExecStatus()
  struct BackendExecState {
    TUniqueId fragment_id;
    WallClockStopWatch stopwatch;  // wall clock timer for this fragment
    int backend_num;  // backend_exec_states_[backend_num] points to us
    const std::pair<std::string, int> hostport;  // of ImpalaInternalService
    int64_t total_split_size;  // summed up across all splits; in bytes

    // needed for ParallelExecutor::Exec()
    const TPlanExecRequest* exec_request;  
    const TPlanExecParams* exec_params;  

    // protects fields below
    // lock ordering: Coordinator::lock_ can only get obtained *prior*
    // to lock
    boost::mutex lock;

    // if the status indicates an error status, execution of this fragment
    // has either been aborted by the remote backend (which then reported the error)
    // or cancellation has been initiated; either way, execution must not be cancelled
    Status status;

    bool initiated; // if true, TPlanExecRequest rpc has been sent
    bool done;  // if true, execution terminated; do not cancel in that case
    bool profile_created;  // true after the first call to profile->Update()
    RuntimeProfile* profile;  // owned by obj_pool()
    std::vector<std::string> error_log; // errors reported by this backend
    
    // Throughput counters for this fragment
    ThroughputCounterMap throughput_counters;

    BackendExecState(const TUniqueId& fragment_id, int backend_num,
                     const std::pair<std::string, int>& hostport,
                     const TPlanExecRequest* exec_request,
                     const TPlanExecParams* exec_params,
                     ObjectPool* obj_pool);

    // Computes sum of split sizes of leftmost scan
    void ComputeTotalSplitSize(const TPlanExecParams& params);

    // Return value of throughput counter for given plan_node_id, or 0 if that node
    // doesn't exist.
    // Thread-safe.
    int64_t GetNodeThroughput(int plan_node_id);
  };
  // BackendExecStates owned by obj_pool()
  std::vector<BackendExecState*> backend_exec_states_;

  // True if the query needs a post-execution step to tidy up
  bool needs_finalization_;

  // Only valid if needs_finalization is true
  TFinalizeParams finalize_params_;

  // ensures single-threaded execution of Wait(); must not hold lock_ when acquiring this
  boost::mutex wait_lock_;

  bool has_called_wait_;  // if true, Wait() was called; protected by wait_lock_

  // protects all fields below
  boost::mutex lock_;

  // Overall status of the entire query; set to the first reported fragment error
  // status or to CANCELLED, if Cancel() is called.
  Status query_status_;

  // execution state of coordinator fragment
  boost::scoped_ptr<PlanFragmentExecutor> executor_;

  // owned by plan root, which resides in runtime_state_'s pool
  const RowDescriptor* row_desc_;

  // map from fragment id to corresponding exec state stored in backend_exec_states_
  typedef boost::unordered_map<TUniqueId, BackendExecState*> BackendExecStateMap;
  BackendExecStateMap backend_exec_state_map_;

  // Return executor_'s runtime state's object pool, if executor_ is set,
  // otherwise return a local object pool.
  ObjectPool* obj_pool();

  // True if execution has completed, false otherwise.
  bool execution_completed_;
  
  // Number of remote fragments that have completed
  int num_remote_fragements_complete_;

  // Repository for statistics gathered during the execution of a
  // single query. Not owned by us.
  ExecStats* exec_stats_;

  // If there is no coordinator fragment, Wait simply waits until all
  // backends report completion by notifying on backend_completion_cv_.
  // Tied to lock_.
  boost::condition_variable backend_completion_cv_;

  // Count of the number of backends for which done != true. When this
  // hits 0, any Wait()'ing thread is notified
  int num_remaining_backends_;

  // The following two structures, partition_row_counts_ and files_to_move_ are filled in
  // as the query completes, and track the results of INSERT queries that alter the
  // structure of tables. They are either the union of the reports from all backends, or
  // taken from the coordinator fragment: only one of the two can legitimately produce
  // updates.

  // The set of partitions that have been written to or updated, along with the number of
  // rows written (may be 0). For unpartitioned tables, the empty string denotes the
  // entire table.
  PartitionRowCount partition_row_counts_;

  // The set of files to move after an INSERT query has run, in (src, dest) form. An empty
  // string for the destination means that a file is to be deleted.
  FileMoveMap files_to_move_;

  // Object pool used used only if no fragment is executing (otherwise
  // we use the executor's object pool), use obj_pool() to access
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Aggregate counters for the entire query.
  boost::scoped_ptr<RuntimeProfile> query_profile_;

  // Profile for aggregate throughput counters; allocated in obj_pool().
  RuntimeProfile* agg_throughput_profile_;
    
  // Throughput counters for the coordinator fragment
  ThroughputCounterMap coordinator_throughput_counters_;

  // Wrapper for ExecPlanFragment() rpc.  This function will be called in parallel
  // from multiple threads.
  // Obtains exec_state->lock prior to making rpc, so that it serializes
  // correctly with UpdateFragmentExecStatus().
  // exec_state contains all information needed to issue the rpc.
  // 'coordinator' will always be an instance to this class and 'exec_state' will
  // always be an instance of BackendExecState.
  Status ExecRemoteFragment(void* exec_state);

  // Determine fragment number, given fragment id.
  int GetFragmentNum(const TUniqueId& fragment_id);

  // Print hdfs split size stats to VLOG_QUERY and details to VLOG_FILE
  void PrintBackendInfo();

  // Create aggregate throughput counters for all scan nodes in any of the fragments
  void CreateThroughputCounters(const std::vector<TPlanExecRequest>& fragment_requests);

  // Build throughput_counters from profile.  Assumes lock protecting profile and result
  // is held.
  void CreateThroughputCounters(RuntimeProfile* profile, ThroughputCounterMap* result);

  // Derived counter function: aggregates throughput for node_id across all backends
  // (id needs to be for a ScanNode)
  int64_t ComputeTotalThroughput(int node_id);

  // Runs cancel logic. Assumes that lock_ is held.
  void CancelInternal();

  // Returns query_status_.
  Status GetStatus();

  // Acquires lock_ and updates query_status_ with 'status' if it's not already
  // an error status, and returns the current query_status_.
  // Calls CancelInternal() when switching to an error status.
  // If failed_fragment is non-null, it is the fragment_id that has failed, used
  // for error reporting.
  Status UpdateStatus(const Status& status, const TUniqueId* failed_fragment);

  // Returns only when either all backends have reported success or the query is in error.
  // Returns the status of the query.
  // It is safe to call this concurrently, but any calls must be made only after Exec().
  // WaitForAllBackends may be called before Wait(), but note that Wait() guarantees
  // that any coordinator fragment has finished, which this method does not.
  Status WaitForAllBackends();

  // Perform any post-query cleanup required. Called by Wait() only after all
  // backends are returned.
  Status FinalizeQuery();

  // Outputs aggregate query profile summary.  This is assumed to be called at the
  // end of a succesfully executed query.
  void ReportQuerySummary();
};

}

#endif
