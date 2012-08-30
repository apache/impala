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
#include "gen-cpp/Types_types.h"

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
// occurs.
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
// TODO:
// - add profile counters for coordinator (how much time do we spend in startup?)
// - start all backends for a particular TPlanExecRequest in parallel
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
  // the next GetNext() call. If the coordinator has a sink, then no tuples will be
  // returned via *batch, but will instead be sent to the sink. In that case,
  // GetNext() will block until all result rows have been sent to the sink.
  // (TODO: this is not how the code behaves; we need to fix the code)
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
  // to Cancel().
  Status UpdateFragmentExecStatus(const TReportExecStatusParams& params);

  // only valid *after* calling Exec(), and may return NULL if there is no executor
  RuntimeState* runtime_state();
  const RowDescriptor& row_desc() const;

  // Get cumulative profile aggregated over all fragments of the query.
  // This is a snapshot of the current state of execution and will change in
  // the future if not all fragments have finished execution.
  // RuntimeProfile is not thread-safe, but non-updating functions (PrettyPrint(),
  // ToThrift(), etc.) are safe to call while counters are being updated concurrently.
  RuntimeProfile* query_profile() const { return query_profile_.get(); }

  // True iff either a) all rows have been returned or sent to a table sink or b) the
  // query has been aborted
  bool execution_completed() { return execution_completed_; }

  ExecStats* exec_stats() { return exec_stats_; }
  const TUniqueId& query_id() const { return query_id_; }

  // Gathers all updates to the catalog required once this query has completed execution.
  // Returns true if a catalog update is required, false otherwise.
  // Must only be called after Wait()
  bool PrepareCatalogUpdate(TCatalogUpdate* catalog_update);

 private:
  ExecEnv* exec_env_;
  TUniqueId query_id_;

  bool has_called_wait_;  // if true, Wait() was called

  // execution state of a particular fragment
  struct BackendExecState {
    TUniqueId fragment_id;
    int backend_num;  // backend_exec_states_[backend_num] points to us
    const std::pair<std::string, int> hostport;  // of ImpalaInternalService
    int64_t total_split_size;  // summed up across all splits
    const TPlanExecRequest* exec_request;  
    const TPlanExecParams* exec_params;  

    // protects fields below
    // lock ordering: Coordinator::lock_ can only get obtained *prior*
    // to lock
    boost::mutex lock;

    Status status;
    bool initiated; // if true, TPlanExecRequest rpc has been sent
    bool done;  // if true, execution terminated
    RuntimeProfile* profile;  // owned by obj_pool()

    // Lists the Hdfs partitions affected by this query
    std::set<std::string> updated_hdfs_partitions;
    
    BackendExecState(const TUniqueId& fragment_id, int backend_num,
                     const std::pair<std::string, int>& hostport,
                     const TPlanExecRequest* exec_request,
                     const TPlanExecParams* exec_params) 
      : fragment_id(fragment_id),
        backend_num(backend_num),
        hostport(hostport),
        total_split_size(0),
        exec_request(exec_request),
        exec_params(exec_params),
        initiated(false),
        done(false),
        profile(NULL) {
      ComputeTotalSplitSize(*exec_params);
    }

    void ComputeTotalSplitSize(const TPlanExecParams& params);
  };

  // BackendExecStates owned by obj_pool()
  std::vector<BackendExecState*> backend_exec_states_;

  // protects all fields below
  boost::mutex lock_;

  // ensures single-threaded execution of Wait()
  boost::mutex wait_lock_;

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
  // backends report completion. The following variables manage
  // signalling from UpdateFragmentExecStatus to Wait once all
  // backends are done.

  // Protects access to num_remaining_backends_, and coordinates
  // signal / wait on backend_completion_cv_. Must be the last lock
  // acquired - do not acquire any other locks while holding
  // backend_completion_lock_. wait_lock_ or lock_ may be held when
  // acquiring this lock.
  boost::mutex backend_completion_lock_;
  boost::condition_variable backend_completion_cv_;

  // Count of the number of backends for which done != true. When this
  // hits 0, any Wait()'ing thread is notified
  int num_remaining_backends_;

  // True if we Cancel was called one or more times
  bool is_cancelled_;

  // Object pool used used only if no fragment is executing (otherwise
  // we use the executor's object pool), use obj_pool() to access
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Aggregate counters for the entire query.
  boost::scoped_ptr<RuntimeProfile> query_profile_;

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

  // Executes the GetNext() logic, but doesn't call Close() when execution
  // is completed.
  Status GetNextInternal(RowBatch** batch, RuntimeState* state);

  // Runs cancel logic. If 'get_lock' is true, obtains lock_; otherwise doesn't.
  void Cancel(bool get_lock);
};

}

#endif
