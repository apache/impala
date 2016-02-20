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


#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/function.hpp>

#include "common/status.h"
#include "common/object-pool.h"
#include "runtime/runtime-state.h"
#include "util/thread.h"

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
class TPlanFragment;
class TPlanFragmentExecParams;
class TPlanExecParams;

/// PlanFragmentExecutor handles all aspects of the execution of a single plan fragment,
/// including setup and tear-down, both in the success and error case.
/// Tear-down frees all memory allocated for this plan fragment and closes all data
/// streams; it happens automatically in the d'tor.
//
/// The executor makes an aggregated profile for the entire fragment available,
/// which includes profile information for the plan itself as well as the output
/// sink, if any.
/// The ReportStatusCallback passed into the c'tor is invoked periodically to report the
/// execution status. The frequency of those reports is controlled by the flag
/// status_report_interval; setting that flag to 0 disables periodic reporting altogether
/// Regardless of the value of that flag, if a report callback is specified, it is
/// invoked at least once at the end of execution with an overall status and profile
/// (and 'done' indicator). The only exception is when execution is cancelled, in which
/// case the callback is *not* invoked (the coordinator already knows that execution
/// stopped, because it initiated the cancellation).
//
/// Aside from Cancel(), which may be called asynchronously, this class is not
/// thread-safe.
class PlanFragmentExecutor {
 public:
  /// Callback to report execution status of plan fragment.
  /// 'profile' is the cumulative profile, 'done' indicates whether the execution
  /// is done or still continuing.
  /// Note: this does not take a const RuntimeProfile&, because it might need to call
  /// functions like PrettyPrint() or ToThrift(), neither of which is const
  /// because they take locks.
  typedef boost::function<
      void (const Status& status, RuntimeProfile* profile, bool done)>
      ReportStatusCallback;

  /// report_status_cb, if !empty(), is used to report the accumulated profile
  /// information periodically during execution (Open() or GetNext()).
  PlanFragmentExecutor(ExecEnv* exec_env, const ReportStatusCallback& report_status_cb);

  /// Closes the underlying plan fragment and frees up all resources allocated
  /// in Open()/GetNext().
  /// It is an error to delete a PlanFragmentExecutor with a report callback
  /// before Open()/GetNext() (depending on whether the fragment has a sink)
  /// indicated that execution is finished.
  ~PlanFragmentExecutor();

  /// Prepare for execution. Call this prior to Open().
  ///
  /// runtime_state() and row_desc() will not be valid until Prepare() is
  /// called. runtime_state() will always be valid after Prepare() returns, unless the
  /// query was cancelled before Prepare() was called.  If request.query_options.mem_limit
  /// > 0, it is used as an approximate limit on the number of bytes this query can
  /// consume at runtime.  The query will be aborted (MEM_LIMIT_EXCEEDED) if it goes over
  /// that limit.
  ///
  /// If Cancel() is called before Prepare(), Prepare() is a no-op and returns
  /// Status::CANCELLED;
  Status Prepare(const TExecPlanFragmentParams& request);

  /// Start execution. Call this prior to GetNext().
  /// If this fragment has a sink, Open() will send all rows produced
  /// by the fragment to that sink. Therefore, Open() may block until
  /// all rows are produced (and a subsequent call to GetNext() will not return
  /// any rows).
  /// This also starts the status-reporting thread, if the interval flag
  /// is > 0 and a callback was specified in the c'tor.
  /// If this fragment has a sink, report_status_cb will have been called for the final
  /// time when Open() returns, and the status-reporting thread will have been stopped.
  Status Open();

  /// Return results through 'batch'. Sets '*batch' to NULL if no more results.
  /// '*batch' is owned by PlanFragmentExecutor and must not be deleted.
  /// When *batch == NULL, GetNext() should not be called anymore. Also, report_status_cb
  /// will have been called for the final time and the status-reporting thread
  /// will have been stopped.
  Status GetNext(RowBatch** batch);

  /// Closes the underlying plan fragment and frees up all resources allocated
  /// in Open()/GetNext().
  void Close();

  /// Initiate cancellation. If called concurrently with Prepare(), will wait for
  /// Prepare() to finish in order to properly tear down Prepare()'d state.
  ///
  /// Cancel() may be called more than once. Calls after the first will have no
  /// effect. Duplicate calls to Cancel() are not serialised, and may safely execute
  /// concurrently.
  ///
  /// It is legal to call Cancel() if Prepare() returned an error.
  void Cancel();

  /// Returns true if this query has a limit and it has been reached.
  bool ReachedLimit();

  /// Releases the thread token for this fragment executor.
  void ReleaseThreadToken();

  /// call these only after Prepare()
  RuntimeState* runtime_state() { return runtime_state_.get(); }
  const RowDescriptor& row_desc();

  /// Profile information for plan and output sink.
  RuntimeProfile* profile();

  /// Name of the counter that is tracking per query, per host peak mem usage.
  static const std::string PER_HOST_PEAK_MEM_COUNTER;

 private:
  ExecEnv* exec_env_;  // not owned
  ExecNode* plan_;  // lives in runtime_state_->obj_pool()
  TUniqueId query_id_;

  /// profile reporting-related
  ReportStatusCallback report_status_cb_;
  boost::scoped_ptr<Thread> report_thread_;
  boost::mutex report_thread_lock_;

  /// Indicates that profile reporting thread should stop.
  /// Tied to report_thread_lock_.
  boost::condition_variable stop_report_thread_cv_;

  /// Indicates that profile reporting thread started.
  /// Tied to report_thread_lock_.
  boost::condition_variable report_thread_started_cv_;
  bool report_thread_active_;  // true if we started the thread

  /// true if plan_->GetNext() indicated that it's done
  bool done_;

  /// true if Close() has been called
  bool closed_;

  /// true if this fragment has not returned the thread token to the thread resource mgr
  bool has_thread_token_;

  /// Overall execution status. Either ok() or set to the first error status that
  /// was encountered.
  Status status_;

  /// Protects status_
  /// lock ordering:
  /// 1. report_thread_lock_
  /// 2. status_lock_
  boost::mutex status_lock_;

  /// 'runtime_state_' has to be before 'sink_' as 'sink_' relies on the object pool
  /// of 'runtime_state_'.
  boost::scoped_ptr<RuntimeState> runtime_state_;
  /// Output sink for rows sent to this fragment. May not be set, in which case rows are
  /// returned via GetNext's row batch
  /// Created in Prepare (if required), owned by this object.
  boost::scoped_ptr<DataSink> sink_;
  boost::scoped_ptr<RowBatch> row_batch_;
  boost::scoped_ptr<TRowBatch> thrift_batch_;

  /// Protects is_prepared_ and is_cancelled_, and is also used to coordinate between
  /// Prepare() and Cancel() to ensure mutual exclusion.
  boost::mutex prepare_lock_;

  /// True if Prepare() has been called and done some work - even if it returned an
  /// error. If Cancel() was called before Prepare(), is_prepared_ will not be set.
  bool is_prepared_;

  /// True if and only if Cancel() has been called.
  bool is_cancelled_;

  /// A counter for the per query, per host peak mem usage. Note that this is not the
  /// max of the peak memory of all fragments running on a host since it needs to take
  /// into account when they are running concurrently. All fragments for a single query
  /// on a single host will have the same value for this counter.
  RuntimeProfile::Counter* per_host_mem_usage_;

  /// Number of rows returned by this fragment
  RuntimeProfile::Counter* rows_produced_counter_;

  /// Average number of thread tokens for the duration of the plan fragment execution.
  /// Fragments that do a lot of cpu work (non-coordinator fragment) will have at
  /// least 1 token.  Fragments that contain a hdfs scan node will have 1+ tokens
  /// depending on system load.  Other nodes (e.g. hash join node) can also reserve
  /// additional tokens.
  /// This is a measure of how much CPU resources this fragment used during the course
  /// of the execution.
  RuntimeProfile::Counter* average_thread_tokens_;

  /// Stopwatch for this entire fragment. Started in Prepare(), stopped in Close().
  MonotonicStopWatch fragment_sw_;

  /// (Atomic) Flag that indicates whether a completed fragment report has been or will
  /// be fired. It is initialized to 0 and atomically swapped to 1 when a completed
  /// fragment report is about to be fired. Used for reducing the probability that a
  /// report is sent twice at the end of the fragment.
  AtomicInt<int> completed_report_sent_;

  /// Sampled memory usage at even time intervals.
  RuntimeProfile::TimeSeriesCounter* mem_usage_sampled_counter_;

  /// Sampled thread usage (tokens) at even time intervals.
  RuntimeProfile::TimeSeriesCounter* thread_usage_sampled_counter_;

  ObjectPool* obj_pool() { return runtime_state_->obj_pool(); }

  /// typedef for TPlanFragmentExecParams.per_node_scan_ranges
  typedef std::map<TPlanNodeId, std::vector<TScanRangeParams> > PerNodeScanRanges;

  /// Main loop of profile reporting thread.
  /// Exits when notified on done_cv_.
  /// On exit, *no report is sent*, ie, this will not send the final report.
  void ReportProfile();

  /// Invoked the report callback if there is a report callback and the current
  /// status isn't CANCELLED. Sets 'done' to true in the callback invocation if
  /// done == true or we have an error status.
  void SendReport(bool done);

  /// If status_.ok(), sets status_ to status.
  /// If we're transitioning to an error status, stops report thread and
  /// sends a final report.
  void UpdateStatus(const Status& status);

  /// Called when the fragment execution is complete to finalize counters.
  void FragmentComplete();

  /// Optimizes the code-generated functions in runtime_state_->llvm_codegen().
  /// Must be called between plan_->Prepare() and plan_->Open().
  /// This is somewhat time consuming so we don't want it to do it in
  /// PlanFragmentExecutor()::Prepare() to allow starting plan fragments more
  /// quickly and in parallel (in a deep plan tree, the fragments are started
  /// in level order).
  void OptimizeLlvmModule();

  /// Executes Open() logic and returns resulting status. Does not set status_.
  /// If this plan fragment has no sink, OpenInternal() does nothing.
  /// If this plan fragment has a sink and OpenInternal() returns without an
  /// error condition, all rows will have been sent to the sink, the sink will
  /// have been closed, a final report will have been sent and the report thread will
  /// have been stopped. sink_ will be set to NULL after successful execution.
  Status OpenInternal();

  /// Executes GetNext() logic and returns resulting status.
  /// sets done_ to true if the last row batch was returned.
  Status GetNextInternal(RowBatch** batch);

  /// Stops report thread, if one is running. Blocks until report thread terminates.
  /// Idempotent.
  void StopReportThread();

  /// Print stats about scan ranges for each volumeId in params to info log.
  void PrintVolumeIds(const TPlanExecParams& params);
  void PrintVolumeIds(const PerNodeScanRanges& per_node_scan_ranges);

  const DescriptorTbl& desc_tbl() { return runtime_state_->desc_tbl(); }
};

}

#endif
