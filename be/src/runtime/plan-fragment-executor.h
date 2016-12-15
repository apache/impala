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


#ifndef IMPALA_SERVICE_PLAN_EXECUTOR_H
#define IMPALA_SERVICE_PLAN_EXECUTOR_H

#include <vector>
#include <boost/scoped_ptr.hpp>
#include <boost/function.hpp>

#include "common/object-pool.h"
#include "common/status.h"
#include "runtime/runtime-state.h"
#include "util/promise.h"
#include "util/runtime-profile-counters.h"
#include "util/thread.h"

namespace impala {

class HdfsFsCache;
class ExecNode;
class PlanRootSink;
class RowDescriptor;
class RowBatch;
class DataSink;
class DataStreamMgr;
class RuntimeProfile;
class RuntimeState;
class TRowBatch;
class TPlanExecRequest;
class TPlanFragment;
class TPlanExecParams;

/// PlanFragmentExecutor handles all aspects of the execution of a single plan fragment,
/// including setup and tear-down, both in the success and error case. Tear-down, which
/// happens in Close(), frees all memory allocated for this plan fragment and closes all
/// data streams.
///
/// The lifecycle of a PlanFragmentExecutor is as follows:
///     if (Prepare().ok()) {
///       Open()
///       Exec()
///     }
///     Close()
///
/// The executor makes an aggregated profile for the entire fragment available, which
/// includes profile information for the plan itself as well as the output sink.
///
/// The ReportStatusCallback passed into the c'tor is invoked periodically to report the
/// execution profile. The frequency of those reports is controlled by the flag
/// status_report_interval; setting that flag to 0 disables periodic reporting altogether
/// Regardless of the value of that flag, if a report callback is specified, it is invoked
/// at least once at the end of execution with an overall status and profile (and 'done'
/// indicator).
///
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
  /// information periodically during execution.
  PlanFragmentExecutor(const ReportStatusCallback& report_status_cb);

  /// It is an error to delete a PlanFragmentExecutor with a report callback before Exec()
  /// indicated that execution is finished, or to delete one that has not been Close()'d
  /// if Prepare() has been called.
  ~PlanFragmentExecutor();

  /// Prepare for execution. Call this prior to Open().
  ///
  /// runtime_state() will not be valid until Prepare() is called. runtime_state() will
  /// always be valid after Prepare() returns, unless the query was cancelled before
  /// Prepare() was called.  If request.query_options.mem_limit > 0, it is used as an
  /// approximate limit on the number of bytes this query can consume at runtime.  The
  /// query will be aborted (MEM_LIMIT_EXCEEDED) if it goes over that limit.
  ///
  /// If Cancel() is called before Prepare(), Prepare() is a no-op and returns
  /// Status::CANCELLED;
  ///
  /// If Prepare() fails, it will invoke final status callback with the error status.
  /// TODO: remove desc_tbl parameter once we do a per-query exec rpc (and we
  /// have a single descriptor table to cover all fragment instances); at the moment
  /// we need to pass the TDescriptorTable explicitly
  Status Prepare(QueryState* query_state, const TDescriptorTable& desc_tbl,
      const TPlanFragmentCtx& fragment_ctx, const TPlanFragmentInstanceCtx& instance_ctx);

  /// Opens the fragment plan and sink. Starts the profile reporting thread, if
  /// required.  Can be called only if Prepare() succeeded. If Open() fails it will
  /// invoke the final status callback with the error status.
  /// TODO: is this needed? It's only ever called in conjunction with Exec() and Close()
  Status Open();

  /// Executes the fragment by repeatedly driving the sink with batches produced by the
  /// exec node tree. report_status_cb will have been called for the final time when
  /// Exec() returns, and the status-reporting thread will have been stopped. Can be
  /// called only if Open() succeeded.
  Status Exec();

  /// Closes the underlying plan fragment and frees up all resources allocated in
  /// Prepare() and Open(). Must be called if Prepare() has been called - no matter
  /// whether or not Prepare() succeeded.
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

  /// call these only after Prepare()
  RuntimeState* runtime_state() { return runtime_state_.get(); }

  /// Profile information for plan and output sink.
  RuntimeProfile* profile();

  /// Blocks until Prepare() is completed.
  Status WaitForPrepare() { return prepared_promise_.Get(); }

  /// Blocks until exec tree and sink are both opened. It is an error to call this before
  /// Prepare() has completed. If Prepare() returned an error, WaitForOpen() will
  /// return that error without blocking.
  Status WaitForOpen();

  /// Returns fragment instance's sink if this is the root fragment instance. Valid after
  /// Prepare() returns; if Prepare() fails may be nullptr.
  PlanRootSink* root_sink() { return root_sink_; }

  /// Name of the counter that is tracking per query, per host peak mem usage.
  static const std::string PER_HOST_PEAK_MEM_COUNTER;

 private:
  ExecNode* exec_tree_; // lives in runtime_state_->obj_pool()
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

  /// When the report thread starts, it sets 'report_thread_active_' to true and signals
  /// 'report_thread_started_cv_'. The report thread is shut down by setting
  /// 'report_thread_active_' to false and signalling 'stop_report_thread_cv_'. Protected
  /// by 'report_thread_lock_'.
  bool report_thread_active_;

  /// true if Close() has been called
  bool closed_;

  /// true if this fragment has not returned the thread token to the thread resource mgr
  bool has_thread_token_;

  /// 'runtime_state_' has to be before 'sink_' as 'sink_' relies on the object pool of
  /// 'runtime_state_'. This means 'sink_' is destroyed first so any implicit connections
  /// (e.g. mem_trackers_) from 'runtime_state_' to 'sink_' need to be severed prior to
  /// the dtor of 'runtime_state_'.
  boost::scoped_ptr<RuntimeState> runtime_state_;

  /// Profile for timings for each stage of the plan fragment instance's lifecycle.
  RuntimeProfile* timings_profile_;

  /// Output sink for rows sent to this fragment. Created in Prepare(), owned by this
  /// object.
  boost::scoped_ptr<DataSink> sink_;

  /// Set if this fragment instance is the root of the entire plan, so that a consumer can
  /// pull results by calling root_sink_->GetNext(). Same object as sink_.
  PlanRootSink* root_sink_ = nullptr;

  boost::scoped_ptr<RowBatch> row_batch_;

  /// Protects is_prepared_ and is_cancelled_, and is also used to coordinate between
  /// Prepare() and Cancel() to ensure mutual exclusion.
  boost::mutex prepare_lock_;

  /// True if Prepare() has been called and done some work - even if it returned an
  /// error. If Cancel() was called before Prepare(), is_prepared_ will not be set.
  bool is_prepared_;

  /// Set when Prepare() returns.
  Promise<Status> prepared_promise_;

  /// Set when OpenInternal() returns.
  Promise<Status> opened_promise_;

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

  /// Sampled memory usage at even time intervals.
  RuntimeProfile::TimeSeriesCounter* mem_usage_sampled_counter_;

  /// Sampled thread usage (tokens) at even time intervals.
  RuntimeProfile::TimeSeriesCounter* thread_usage_sampled_counter_;

  ObjectPool* obj_pool() { return runtime_state_->obj_pool(); }

  /// typedef for TPlanFragmentInstanceCtx.per_node_scan_ranges
  typedef std::map<TPlanNodeId, std::vector<TScanRangeParams>> PerNodeScanRanges;

  /// Main loop of profile reporting thread.
  /// Exits when notified on stop_report_thread_cv_ and report_thread_active_ is set to
  /// false. This will not send the final report.
  void ReportProfileThread();

  /// Invoked the report callback. If 'done' is true, sends the final report with
  /// 'status' and the profile. This type of report is sent once and only by the
  /// instance execution thread.  Otherwise, a profile-only report is sent, which the
  /// ReportProfileThread() thread will do periodically.
  void SendReport(bool done, const Status& status);

  /// Called when the fragment execution is complete to finalize counters and send
  /// the final status report.  Must be called only once.
  void FragmentComplete(const Status& status);

  /// Optimizes the code-generated functions in runtime_state_->llvm_codegen().
  /// Must be called after exec_tree_->Prepare() and before exec_tree_->Open().
  /// Returns error if LLVM optimization or compilation fails.
  Status OptimizeLlvmModule();

  /// Executes Open() logic and returns resulting status. Does not set status_.
  Status OpenInternal();

  /// Pulls row batches from fragment instance and pushes them to sink_ in a loop. Returns
  /// OK if the input was exhausted and sent to the sink successfully, an error otherwise.
  /// If ExecInternal() returns without an error condition, all rows will have been sent
  /// to the sink, the sink will have been closed, a final report will have been sent and
  /// the report thread will have been stopped.
  Status ExecInternal();

  /// Performs all the logic of Prepare() and returns resulting status.
  /// TODO: remove desc_tbl parameter as part of per-query exec rpc
  Status PrepareInternal(QueryState* qs, const TDescriptorTable& desc_tbl,
      const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& instance_ctx);

  /// Releases the thread token for this fragment executor.
  void ReleaseThreadToken();

  /// Stops report thread, if one is running. Blocks until report thread terminates.
  /// Idempotent.
  void StopReportThread();

  /// Print stats about scan ranges for each volumeId in params to info log.
  void PrintVolumeIds(const PerNodeScanRanges& per_node_scan_ranges);

  const DescriptorTbl& desc_tbl() { return runtime_state_->desc_tbl(); }
};

}

#endif
