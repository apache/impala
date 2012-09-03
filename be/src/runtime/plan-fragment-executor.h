// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/function.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/runtime-state.h"

namespace impala {

class HdfsFsCache;
class ExecNode;
class RowDescriptor;
class RowBatch;
class DataSink;
class DataStreamMgr;
class RuntimeProfile;
class RuntimeState;
class TRowBatch;
class TPlanExecRequest;
class TPlanExecParams;

// PlanFragmentExecutor handles all aspects of the execution of a single plan fragment,
// including setup and tear-down, both in the success and error case.
// Tear-down frees all memory allocated for this plan fragment and closes all data
// streams; it happens automatically in the d'tor.
//
// The executor makes an aggregated profile for the entire fragment available,
// which includes profile information for the plan itself as well as the output
// sink, if any.
// The ReportStatusCallback passed into the c'tor is invoked periodically (controlled
// by the flag status_report_interval) to report the execution status. It is guaranteed
// to be called at least once at the end of execution with an overall status and profile
// (and 'done' indicator).
//
// Aside from Cancel(), which may be called asynchronously, this class is not
// thread-safe.
class PlanFragmentExecutor {
 public:
  // Callback to report execution status of plan fragment.
  // 'profile' is the cumulative profile, 'done' indicates whether the execution
  // is done or still continuing.
  // Note: this does not take a const RuntimeProfile&, because it might need to call
  // functions like PrettyPrint() or ToThrift(), neither of which is const
  // because they take locks.
  typedef boost::function<
      void (const Status& status, RuntimeProfile* profile, bool done)>
      ReportStatusCallback;

  // report_status_cb, if !empty(), is used to report the accumulated profile
  // information periodically during execution (Open() or GetNext()).
  PlanFragmentExecutor(ExecEnv* exec_env, const ReportStatusCallback& report_status_cb);

  // Closes the underlying plan fragment and frees up all resources allocated
  // in Open()/GetNext(). Tells report_thread_ to stop and blocks until it has finished.
  ~PlanFragmentExecutor();

  // Prepare for execution. Call this prior to Open().
  // This call won't block.
  // runtime_state() and row_desc() will not be valid until Prepare() is called.
  Status Prepare(const TPlanExecRequest& request, const TPlanExecParams& params);

  // Start execution. Call this prior to GetNext().
  // If this fragment has a sink, Open() will send all rows produced
  // by the fragment to that sink. Therefore, Open() may block until
  // all rows are produced.
  // This also starts the status-reporting thread, if the interval flag
  // is > 0 and a callback was specified in the c'tor.
  // If this fragment has a sink, the report thread will have invoked
  // report_status_cb for the final time when Open() returns.
  Status Open();

  // Return results through 'batch'. Sets '*batch' to NULL if no more results.
  // '*batch' is owned by PlanFragmentExecutor and must not be deleted.
  // When *batch == NULL, GetNext() should not be called anymore.
  Status GetNext(RowBatch** batch);

  // Initiate cancellation. Must not be called until after Prepare() returned.
  void Cancel();

  // call these only after Prepare()
  RuntimeState* runtime_state() { return runtime_state_.get(); }
  const RowDescriptor& row_desc();

  // Profile information for plan and output sink.
  RuntimeProfile* profile();

 private:
  ExecEnv* exec_env_;  // not owned
  ExecNode* plan_;  // lives in runtime_state_->obj_pool()
  TUniqueId query_id_;

  // profile reporting-related
  ReportStatusCallback report_status_cb_;
  boost::thread report_thread_;
  boost::mutex report_thread_lock_;

  // Indicates that profile reporting thread should stop.
  // Tied to report_thread_lock_.
  boost::condition_variable stop_report_thread_cv_;

  // Indicates that profile reporting thread started.
  // Tied to report_thread_lock_.
  boost::condition_variable report_thread_started_cv_;
  bool report_thread_active_;  // true if we started the thread

  // true if plan_->GetNext() indicated that it's done
  bool done_; 

  // true if Prepare() returned OK
  bool prepared_;

  // Overall execution status. Either ok() or set to the first error status that
  // was encountered.
  Status status_;

  // Protects status_
  // lock ordering:
  // 1. report_thread_lock_
  // 2. status_lock_
  boost::mutex status_lock_;

  // Output sink for rows sent to this fragment. May not be set, in which case rows are
  // returned via GetNext's row batch
  // Created in Prepare (if required), owned by this object.
  boost::scoped_ptr<DataSink> sink_;
  boost::scoped_ptr<RuntimeState> runtime_state_;
  boost::scoped_ptr<RowBatch> row_batch_;
  boost::scoped_ptr<TRowBatch> thrift_batch_;

  RuntimeProfile::Counter* rows_produced_counter_;

  ObjectPool* obj_pool() { return runtime_state_->obj_pool(); }

  // Main loop of profile reporting thread.
  // Exits if:
  // - notified on done_cv_
  // - !status_.ok() at profile reporting time
  void ReportProfile();

  void Close();

  // If status_.ok(), sets status_ to status.
  void UpdateStatus(const Status& status);

  // Executes Open() logic and returns resulting status. Does not set status_.
  // If this plan fragment has no sink, OpenInternal() does nothing. 
  // If this plan fragment has a sink and OpenInternal() returns without an
  // error condition all rows will have been sent to the sink, the sink will
  // have been closed and the report thread will have been stopped, ensuring a
  // last status report will have been sent to the coordinator. sink_ will be
  // set to NULL after successful execution.
  // In the error case, the destructor will clean up the report thread and the
  // sink.
  Status OpenInternal();

  // Executes GetNext() logic and returns resulting status.
  Status GetNextInternal(RowBatch** batch);

  // Stops report thread, if one is running. Blocks until report thread terminates.
  // Idempotent.
  void StopReportThread();

  // Print stats about scan ranges for each volumeId in params to info log.
  void PrintVolumeIds(const TPlanExecParams& params);

  const DescriptorTbl& desc_tbl() { return runtime_state_->desc_tbl(); }
};

}

#endif
