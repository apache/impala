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


#ifndef IMPALA_RUNTIME_FRAGMENT_INSTANCE_STATE_H
#define IMPALA_RUNTIME_FRAGMENT_INSTANCE_STATE_H

#include <string>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"
#include "util/promise.h"

#include "gen-cpp/ImpalaInternalService_types.h"
#include "runtime/row-batch.h"
#include "util/promise.h"
#include "util/runtime-profile.h"

namespace impala {

class TPlanFragmentCtx;
class TPlanFragmentInstanceCtx;
class TBloomFilter;
class TUniqueId;
class TNetworkAddress;
class TQueryCtx;
class QueryState;
class RuntimeProfile;
class ExecNode;
class PlanRootSink;
class Thread;
class DataSink;
class RuntimeState;

/// FragmentInstanceState handles all aspects of the execution of a single plan fragment
/// instance, including setup and finalization, both in the success and error case.
/// Close() happens automatically at the end of Exec() and frees all memory allocated
/// for this fragment instance and closes all data streams.
///
/// The FIS makes an aggregated profile for the entire fragment available, which
/// includes profile information for the plan itself as well as the output sink.
/// The FIS periodically makes a ReportExecStatus RPC to the coordinator to report the
/// execution status and profile. The frequency of those reports is controlled by the flag
/// status_report_interval; setting that flag to 0 disables periodic reporting altogether
/// Regardless of the value of that flag, a report is sent at least once at the end of
/// execution with an overall status and profile (and 'done' indicator).
/// The FIS will send at least one final status report. If execution ended with an error,
/// that error status will be part of the final report (it will not be overridden by
/// the resulting cancellation).
///
/// This class is thread-safe.
/// All non-getter public functions other than Exec() block until the Prepare phase
/// finishes.
/// No member variables, other than the ones passed to the c'tor, are valid before
/// the Prepare phase finishes.
///
/// TODO:
/// - absorb RuntimeState?
/// - should WaitForPrepare/Open() return the overall execution status, if there
///   was a failure?
class FragmentInstanceState {
 public:
  FragmentInstanceState(QueryState* query_state, const TPlanFragmentCtx& fragment_ctx,
      const TPlanFragmentInstanceCtx& instance_ctx);

  /// Main loop of fragment instance execution. Blocks until execution finishes and
  /// automatically releases resources. Returns execution status.
  /// Must only be called once.
  Status Exec() WARN_UNUSED_RESULT;

  /// Cancels execution and sends a final status report. Idempotent.
  void Cancel();

  /// Blocks until the Prepare phase of Exec() is finished and returns the status.
  Status WaitForPrepare();

  /// Returns true if the Prepare phase of Exec() is finished.
  bool IsPrepared();

  /// Blocks until the Prepare phase of Exec() is finished and the exec tree is
  /// opened, and returns that status. If the preparation phase encountered an error,
  /// GetOpenStatus() will return that error without blocking.
  Status WaitForOpen();

  /// Publishes filter with ID 'filter_id' to this fragment instance's filter bank.
  void PublishFilter(int32_t filter_id, const TBloomFilter& thrift_bloom_filter);

  /// Returns fragment instance's sink if this is the root fragment instance. Valid after
  /// the Prepare phase. May be nullptr.
  PlanRootSink* root_sink() { return root_sink_; }

  /// Name of the counter that is tracking per query, per host peak mem usage.
  /// TODO: this doesn't look like it belongs here
  static const std::string PER_HOST_PEAK_MEM_COUNTER;

  QueryState* query_state() { return query_state_; }
  RuntimeState* runtime_state() { return runtime_state_; }
  RuntimeProfile* profile() const;
  const TQueryCtx& query_ctx() const;
  const TPlanFragmentCtx& fragment_ctx() const { return fragment_ctx_; }
  const TPlanFragmentInstanceCtx& instance_ctx() const { return instance_ctx_; }
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& instance_id() const { return instance_ctx_.fragment_instance_id; }
  const TNetworkAddress& coord_address() const { return query_ctx().coord_address; }
  ObjectPool* obj_pool();

  static const std::string FINST_THREAD_GROUP_NAME;

 private:
  QueryState* query_state_;
  const TPlanFragmentCtx& fragment_ctx_;
  const TPlanFragmentInstanceCtx& instance_ctx_;

  /// All following member variables that are initialized to nullptr are set
  /// in Prepare().

  ExecNode* exec_tree_ = nullptr; // lives in obj_pool()
  RuntimeState* runtime_state_ = nullptr;  // lives in obj_pool()

  /// profile reporting-related
  std::unique_ptr<Thread> report_thread_;
  boost::mutex report_thread_lock_;

  /// Indicates that profile reporting thread should stop.
  /// Tied to report_thread_lock_.
  boost::condition_variable stop_report_thread_cv_;

  /// Indicates that profile reporting thread started.
  /// Tied to report_thread_lock_.
  boost::condition_variable report_thread_started_cv_;

  /// When the report thread starts, it sets report_thread_active_ to true and signals
  /// report_thread_started_cv_. The report thread is shut down by setting
  /// report_thread_active_ to false and signalling stop_report_thread_cv_. Protected
  /// by report_thread_lock_.
  bool report_thread_active_ = false;

  /// Profile for timings for each stage of the plan fragment instance's lifecycle.
  /// Lives in obj_pool().
  RuntimeProfile* timings_profile_ = nullptr;

  /// Output sink for rows sent to this fragment. Created in Prepare(), lives in
  /// obj_pool().
  DataSink* sink_ = nullptr;

  /// Set if this fragment instance is the root of the entire plan, so that a consumer can
  /// pull results by calling root_sink_->GetNext(). Same object as sink_.
  PlanRootSink* root_sink_ = nullptr;

  /// should live in obj_pool(), but managed separately so we can delete it in Close()
  boost::scoped_ptr<RowBatch> row_batch_;

  /// Set when Prepare() returns.
  Promise<Status> prepared_promise_;

  /// Set when OpenInternal() returns.
  Promise<Status> opened_promise_;

  /// A counter for the per query, per host peak mem usage. Note that this is not the
  /// max of the peak memory of all fragments running on a host since it needs to take
  /// into account when they are running concurrently. All fragments for a single query
  /// on a single host will have the same value for this counter.
  RuntimeProfile::Counter* per_host_mem_usage_ = nullptr;

  /// Number of rows returned by this fragment
  /// TODO: by this instance?
  RuntimeProfile::Counter* rows_produced_counter_ = nullptr;

  /// Average number of thread tokens for the duration of the fragment instance execution.
  /// Instances that do a lot of cpu work (non-coordinator fragment) will have at
  /// least 1 token.  Instances that contain a hdfs scan node will have 1+ tokens
  /// depending on system load.  Other nodes (e.g. hash join node) can also reserve
  /// additional tokens.
  /// This is a measure of how much CPU resources this instance used during the course
  /// of the execution.
  /// TODO-MT: remove
  RuntimeProfile::Counter* avg_thread_tokens_ = nullptr;

  /// Sampled memory usage at even time intervals.
  RuntimeProfile::TimeSeriesCounter* mem_usage_sampled_counter_ = nullptr;

  /// Sampled thread usage (tokens) at even time intervals.
  RuntimeProfile::TimeSeriesCounter* thread_usage_sampled_counter_ = nullptr;

  /// Prepare for execution. runtime_state() will not be valid until Prepare() is called.
  /// runtime_state() will always be valid after Prepare() returns.
  /// If request.query_options.mem_limit > 0, it is used as an
  /// approximate limit on the number of bytes this query can consume at runtime.  The
  /// query will be aborted (MEM_LIMIT_EXCEEDED) if it goes over that limit.
  ///
  /// A failure in Prepare() will result in partially-initialized state.
  Status Prepare() WARN_UNUSED_RESULT;

  /// Executes Open() logic and returns resulting status.
  Status Open() WARN_UNUSED_RESULT;

  /// Pulls row batches from exec_tree_ and pushes them to sink_ in a loop. Returns
  /// OK if the input was exhausted and sent to the sink successfully, an error otherwise.
  /// If ExecInternal() returns without an error condition, all rows will have been sent
  /// to the sink and the sink will have been flushed.
  Status ExecInternal() WARN_UNUSED_RESULT;

  /// Closes the underlying fragment instance and frees up all resources allocated in
  /// Prepare() and Open(). Assumes the report thread is stopped. Can handle
  /// partially-finished Prepare().
  void Close();

  /// Main loop of profile reporting thread.
  /// Exits when notified on stop_report_thread_cv_ and report_thread_active_ is set to
  /// false. This will not send the final report.
  void ReportProfileThread();

  /// Invoked the report callback. If 'done' is true, sends the final report with
  /// 'status' and the profile. This type of report is sent once and only by the
  /// instance execution thread.  Otherwise, a profile-only report is sent, which the
  /// ReportProfileThread() thread will do periodically.
  void SendReport(bool done, const Status& status);

  /// Called when execution is complete to finalize counters and send the final status
  /// report.  Must be called only once. Can handle partially-finished Prepare().
  void Finalize(const Status& status);

  /// Releases the thread token for this fragment executor. Can handle
  /// partially-finished Prepare().
  void ReleaseThreadToken();

  /// Stops report thread, if one is running. Blocks until report thread terminates.
  /// Idempotent.
  void StopReportThread();

  /// Print stats about scan ranges for each volumeId in params to info log.
  void PrintVolumeIds();
};

}

#endif
