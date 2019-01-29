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

#ifndef IMPALA_RUNTIME_COORDINATOR_BACKEND_STATE_H
#define IMPALA_RUNTIME_COORDINATOR_BACKEND_STATE_H

#include <vector>
#include <unordered_set>

#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/thread/mutex.hpp>

#include "gen-cpp/control_service.proxy.h"
#include "kudu/rpc/rpc_controller.h"
#include "runtime/coordinator.h"
#include "scheduling/query-schedule.h"
#include "util/error-util-internal.h"
#include "util/progress-updater.h"
#include "util/runtime-profile.h"
#include "util/stopwatch.h"

namespace impala {

class ProgressUpdater;
class ObjectPool;
class DebugOptions;
class CountingBarrier;
class ReportExecStatusRequestPB;
class TUniqueId;
class TQueryCtx;
class ExecSummary;
struct FInstanceExecParams;

/// This class manages all aspects of the execution of all fragment instances of a
/// single query on a particular backend.
/// Thread-safe unless pointed out otherwise.
class Coordinator::BackendState {
 public:
  BackendState(const Coordinator& coord, int state_idx,
      TRuntimeFilterMode::type filter_mode);

  /// Creates InstanceStats for all instance in backend_exec_params in obj_pool
  /// and installs the instance profiles as children of the corresponding FragmentStats'
  /// root profile. Also creates a child profile below 'host_profile_parent' that contains
  /// counters for the backend.
  /// Separated from c'tor to simplify future handling of out-of-mem errors.
  void Init(const BackendExecParams& backend_exec_params,
      const std::vector<FragmentStats*>& fragment_stats,
      RuntimeProfile* host_profile_parent, ObjectPool* obj_pool);

  /// Starts query execution at this backend by issuing an ExecQueryFInstances rpc and
  /// notifies on rpc_complete_barrier when the rpc completes. Success/failure is
  /// communicated through GetStatus(). Uses filter_routing_table to remove filters
  /// that weren't selected during its construction.
  /// The debug_options are applied to the appropriate TPlanFragmentInstanceCtxs, based
  /// on their node_id/instance_idx.
  void Exec(const DebugOptions& debug_options,
      const FilterRoutingTable& filter_routing_table,
      CountingBarrier* rpc_complete_barrier);

  /// Update overall execution status, including the instances' exec status/profiles
  /// and the error log, if this backend is not already done. Updates the fragment
  /// instances' TExecStats in exec_summary (exec_summary->nodes.exec_stats) and updates
  /// progress_update. If any instance reports an error, the overall execution status
  /// becomes the first reported error status. Returns true iff this update changed
  /// IsDone() from false to true, either because it was the last fragment to complete or
  /// because it was the first error received.
  bool ApplyExecStatusReport(const ReportExecStatusRequestPB& backend_exec_status,
      const TRuntimeProfileForest& thrift_profiles, ExecSummary* exec_summary,
      ProgressUpdater* scan_range_progress, DmlExecState* dml_exec_state);

  /// Merges the incoming 'thrift_profile' into this backend state's host profile.
  void UpdateHostProfile(const TRuntimeProfileTree& thrift_profile);

  /// Update completion_times, rates, and avg_profile for all fragment_stats.
  void UpdateExecStats(const std::vector<FragmentStats*>& fragment_stats);

  /// Make a PublishFilter rpc with given params if this backend has instances of the
  /// fragment with idx == rpc_params->dst_fragment_idx, otherwise do nothing.
  void PublishFilter(const TPublishFilterParams& rpc_params);

  /// Cancel execution at this backend if anything is running. Returns true
  /// if cancellation was attempted, false otherwise.
  bool Cancel();

  /// Return the overall execution status. For an error status, the error could come
  /// from the fragment instance level or it can be a general error from the backend
  /// (with no specific fragment responsible). For a caller to distinguish between
  /// these errors and to determine the specific fragment instance (if applicable),
  /// both 'is_fragment_failure' and 'failed_instance_id' must be non-null.
  /// A general error will set *is_fragment_failure to false and leave
  /// failed_instance_id untouched.
  /// A fragment-specific error will set *is_fragment_failure to true and set
  /// *failed_instance_id to the id of the fragment instance that failed.
  /// If the caller does not need this information, both 'is_fragment_failure' and
  /// 'failed_instance_id' must be omitted (using the default value of nullptr).
  Status GetStatus(bool* is_fragment_failure = nullptr,
      TUniqueId* failed_instance_id = nullptr) WARN_UNUSED_RESULT;

  /// Return true if execution at this backend is done. Thread-safe. Caller must not hold
  /// lock_.
  bool IsDone();

  /// Return peak memory consumption and aggregated resource usage across all fragment
  /// instances for this backend.
  ResourceUtilization ComputeResourceUtilization();

  /// Merge the accumulated error log into 'merged'.
  void MergeErrorLog(ErrorLogMap* merged);

  const TNetworkAddress& impalad_address() const { return host_; }
  const TNetworkAddress& krpc_impalad_address() const { return krpc_host_; }
  int state_idx() const { return state_idx_; }

  /// Valid after Init().
  const BackendExecParams* exec_params() const { return backend_exec_params_; }

  /// Only valid after Exec().
  int64_t rpc_latency() const { return rpc_latency_; }

  int64_t last_report_time_ms() {
    boost::lock_guard<boost::mutex> l(lock_);
    return last_report_time_ms_;
  }

  /// Print host/port info for the first backend that's still in progress as a
  /// debugging aid for backend deadlocks.
  static void LogFirstInProgress(std::vector<BackendState*> backend_states);

  /// Serializes backend state to JSON by adding members to 'value', including total
  /// number of instances, peak memory consumption, host and status amongst others.
  void ToJson(rapidjson::Value* value, rapidjson::Document* doc);

  /// Serializes the InstanceStats of all instances of this backend state to JSON by
  /// adding members to 'value', including the remote host name.
  void InstanceStatsToJson(rapidjson::Value* value, rapidjson::Document* doc);

  /// Returns a timestamp using monotonic time for tracking arrival of status reports.
  static int64_t GenerateReportTimestamp() { return MonotonicMillis(); }

 private:
  /// Execution stats for a single fragment instance.
  /// Not thread-safe.
  class InstanceStats {
   public:
    InstanceStats(const FInstanceExecParams& exec_params, FragmentStats* fragment_stats,
        ObjectPool* obj_pool);

    /// Updates 'this' with exec_status and the fragment intance's thrift profile. Also
    /// updates the fragment instance's TExecStats in exec_summary and 'progress_updater'
    /// with the number of newly completed scan ranges. Also updates the instance's avg
    /// profile. Caller must hold BackendState::lock_.
    void Update(const FragmentInstanceExecStatusPB& exec_status,
        const TRuntimeProfileTree& thrift_profile, ExecSummary* exec_summary,
        ProgressUpdater* scan_range_progress);

    int per_fragment_instance_idx() const {
      return exec_params_.per_fragment_instance_idx;
    }

    /// Serializes instance stats to JSON by adding members to 'value', including its
    /// instance id, plan fragment name, and the last event that was recorded during
    /// execution of the instance.
    void ToJson(rapidjson::Value* value, rapidjson::Document* doc);

   private:
    friend class BackendState;

    /// query lifetime
    const FInstanceExecParams& exec_params_;

    /// Unix time in milliseconds of the last status report update for this fragment
    /// instance. Set in Update(). Uses UnixMillis() instead of MonotonicMillis() as
    /// the last update time in the profile is wall clock time.
    ///
    /// This is also used for computing the elapsed time (presented in the debug webpage)
    /// since the last status report update. While UnixMillis() may be prone to time
    /// change due to clock adjustment (e.g. NTP), it's assumed that time change is not
    /// frequent enough to seriously affect the output. Moreover, the inconsistency only
    /// persists for the duration of one status report update (5 seconds by default).
    int64_t last_report_time_ms_ = 0;

    /// The sequence number of the last report.
    int64_t last_report_seq_no_ = 0;

    /// owned by coordinator object pool provided in the c'tor, created in Update()
    RuntimeProfile* profile_ = nullptr;

    /// true if the final report has been received for the fragment instance.
    /// Used to handle duplicate done ReportExecStatus RPC messages. Used only
    /// in ApplyExecStatusReport()
    bool done_ = false;

    /// true after the first call to profile->Update()
    bool profile_created_ = false;

    /// cumulative size of all splits; set in c'tor
    int64_t total_split_size_ = 0;

    /// wall clock timer for this instance
    MonotonicStopWatch stopwatch_;

    /// total scan ranges complete across all scan nodes
    int64_t total_ranges_complete_ = 0;

    /// SCAN_RANGES_COMPLETE_COUNTERs in profile_
    std::vector<RuntimeProfile::Counter*> scan_ranges_complete_counters_;

    /// Collection of BYTES_READ_COUNTERs of all scan nodes in this fragment instance.
    std::vector<RuntimeProfile::Counter*> bytes_read_counters_;

    /// Collection of TotalBytesSent of all data stream senders in this fragment instance.
    std::vector<RuntimeProfile::Counter*> bytes_sent_counters_;

    /// Descriptor string for the last query status report time in the profile.
    static const char* LAST_REPORT_TIME_DESC;

    /// The current state of this fragment instance's execution. This gets serialized in
    /// ToJson() and is displayed in the debug webpages.
    FInstanceExecStatePB current_state_ = FInstanceExecStatePB::WAITING_FOR_EXEC;

    /// Extracts scan_ranges_complete_counters_ and  bytes_read_counters_ from profile_.
    void InitCounters();
  };

  const Coordinator& coord_; /// Coordinator object that owns this BackendState

  const int state_idx_;  /// index of 'this' in Coordinator::backend_states_
  const TRuntimeFilterMode::type filter_mode_;

  /// Backend exec params, owned by the QuerySchedule and has query lifetime.
  const BackendExecParams* backend_exec_params_;

  /// map from instance idx to InstanceStats, the latter live in the obj_pool parameter
  /// of Init()
  std::unordered_map<int, InstanceStats*> instance_stats_map_;

  /// indices of fragments executing on this backend, populated in Init()
  std::unordered_set<int> fragments_;

  /// Contains counters for the backend host that are not specific to a particular
  /// fragment instance, e.g. global CPU utilization and scratch space usage.
  /// Owned by coordinator object pool provided in the c'tor, created in Update().
  RuntimeProfile* host_profile_ = nullptr;

  /// Thrift address of execution backend.
  TNetworkAddress host_;
  /// Krpc address of execution backend.
  TNetworkAddress krpc_host_;

  /// protects fields below
  /// lock ordering: Coordinator::lock_ must only be obtained *prior* to lock_
  boost::mutex lock_;

  // number of in-flight instances
  int num_remaining_instances_ = 0;

  /// If the status indicates an error status, execution has either been aborted by the
  /// executing impalad (which then reported the error) or cancellation has been
  /// initiated; either way, execution must not be cancelled.
  Status status_;

  /// Used to distinguish between errors reported by a specific fragment instance,
  /// which would set failed_instance_id_, rather than an error independent of any
  /// specific fragment.
  bool is_fragment_failure_ = false;

  /// Id of the first fragment instance that reports an error status.
  /// Invalid if no fragment instance has reported an error status.
  TUniqueId failed_instance_id_;

  /// Errors reported by this fragment instance.
  ErrorLogMap error_log_;

  /// Time, in ms, that it took to execute the ExecRemoteFragment() RPC.
  int64_t rpc_latency_ = 0;

  /// If true, ExecPlanFragment() rpc has been sent - even if it was not determined to be
  /// successful.
  bool rpc_sent_ = false;

  /// Initialized in Init(), then set in each call to ApplyExecStatusReport().
  /// Uses GenerateReportTimeout().
  int64_t last_report_time_ms_ = 0;

  const TQueryCtx& query_ctx() const { return coord_.query_ctx(); }
  const TUniqueId& query_id() const { return coord_.query_id(); }

  /// Fill in rpc_params based on state. Uses filter_routing_table to remove filters
  /// that weren't selected during its construction.
  void SetRpcParams(const DebugOptions& debug_options,
      const FilterRoutingTable& filter_routing_table,
      TExecQueryFInstancesParams* rpc_params);

  /// Version of IsDone() where caller must hold lock_ via lock;
  bool IsDoneLocked(const boost::unique_lock<boost::mutex>& lock) const;

  /// Same as ComputeResourceUtilization() but caller must hold lock.
  ResourceUtilization ComputeResourceUtilizationLocked();
};

/// Per fragment execution statistics.
class Coordinator::FragmentStats {
 public:
  /// typedef for boost utility to compute averaged stats
  typedef boost::accumulators::accumulator_set<int64_t,
      boost::accumulators::features<
      boost::accumulators::tag::min,
      boost::accumulators::tag::max,
      boost::accumulators::tag::mean,
      boost::accumulators::tag::variance>
  > SummaryStats;

  /// Create avg and root profiles in obj_pool.
  FragmentStats(const std::string& avg_profile_name,
      const std::string& root_profile_name,
      int num_instances, ObjectPool* obj_pool);

  RuntimeProfile* avg_profile() { return avg_profile_; }
  RuntimeProfile* root_profile() { return root_profile_; }
  SummaryStats* bytes_assigned() { return &bytes_assigned_; }

  /// Compute stats for 'bytes_assigned' and add as info string to avg_profile.
  void AddSplitStats();

  /// Add summary string with execution stats to avg profile.
  void AddExecStats();

 private:
  friend class BackendState;

  /// Averaged profile for this fragment.  Stored in obj_pool.
  /// The counters in this profile are averages (type AveragedCounter) of the
  /// counters in the fragment instance profiles.
  /// Note that the individual fragment instance profiles themselves are stored and
  /// displayed as children of the root_profile below.
  RuntimeProfile* avg_profile_;

  /// root profile for all fragment instances for this fragment; resides in obj_pool
  RuntimeProfile* root_profile_;

  /// Number of instances running this fragment.
  int num_instances_;

  /// Bytes assigned for instances of this fragment
  SummaryStats bytes_assigned_;

  /// Completion times for instances of this fragment
  SummaryStats completion_times_;

  /// Execution rates for instances of this fragment
  SummaryStats rates_;
};

}

#endif
