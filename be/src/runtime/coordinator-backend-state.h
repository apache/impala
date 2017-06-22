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

#include <boost/thread/mutex.hpp>

#include "runtime/coordinator.h"
#include "util/progress-updater.h"
#include "util/stopwatch.h"
#include "util/runtime-profile.h"
#include "gen-cpp/Types_types.h"

namespace impala {

class ProgressUpdater;
class FInstanceExecParams;
class ObjectPool;
class DebugOptions;
class CountingBarrier;
class TUniqueId;
class TQueryCtx;
class TReportExecStatusParams;
class ExecSummary;

/// This class manages all aspects of the execution of all fragment instances of a
/// single query on a particular backend.
/// Thread-safe unless pointed out otherwise.
class Coordinator::BackendState {
 public:
  BackendState(const TUniqueId& query_id, int state_idx,
      TRuntimeFilterMode::type filter_mode);

  /// Creates InstanceStats for all entries in instance_params_list in obj_pool
  /// and installs the instance profiles as children of the corresponding FragmentStats'
  /// root profile.
  /// Separated from c'tor to simplify future handling of out-of-mem errors.
  void Init(const vector<const FInstanceExecParams*>& instance_params_list,
      const std::vector<FragmentStats*>& fragment_stats, ObjectPool* obj_pool);

  /// Starts query execution at this backend by issuing an ExecQueryFInstances rpc and
  /// notifies on rpc_complete_barrier when the rpc completes. Success/failure is
  /// communicated through GetStatus(). Uses filter_routing_table to remove filters
  /// that weren't selected during its construction.
  /// The debug_options are applied to the appropriate TPlanFragmentInstanceCtxs, based
  /// on their node_id/instance_idx.
  void Exec(const TQueryCtx& query_ctx, const DebugOptions& debug_options,
      const FilterRoutingTable& filter_routing_table,
      CountingBarrier* rpc_complete_barrier);

  /// Update overall execution status, including the instances' exec status/profiles
  /// and the error log. Updates the fragment instances' TExecStats in exec_summary
  /// (exec_summary->nodes.exec_stats) and updates progress_update, and sets
  /// done to true if all fragment instances completed, regardless of status.
  /// If any instance reports an error, the overall execution status becomes the first
  /// reported error status and 'done' is set to true.
  void ApplyExecStatusReport(const TReportExecStatusParams& backend_exec_status,
      ExecSummary* exec_summary, ProgressUpdater* scan_range_progress, bool* done);

  /// Update completion_times, rates, and avg_profile for all fragment_stats.
  void UpdateExecStats(const std::vector<FragmentStats*>& fragment_stats);

  /// Make a PublishFilter rpc with given params if this backend has instances of the
  /// fragment with idx == rpc_params->dst_fragment_idx, otherwise do nothing.
  /// This takes by-value parameters because we cannot guarantee that the originating
  /// coordinator won't be destroyed while this executes.
  /// TODO: switch to references when we fix the lifecycle problems of coordinators.
  void PublishFilter(std::shared_ptr<TPublishFilterParams> rpc_params);

  /// Cancel execution at this backend if anything is running. Returns true
  /// if cancellation was attempted, false otherwise.
  bool Cancel();

  /// Return the overall execution status. For an error status, also return the id
  /// of the instance that caused that status, if failed_instance_id != nullptr.
  Status GetStatus(TUniqueId* failed_instance_id = nullptr) WARN_UNUSED_RESULT;

  /// Return peak memory consumption.
  int64_t GetPeakConsumption();

  /// Merge the accumulated error log into 'merged'.
  void MergeErrorLog(ErrorLogMap* merged);

  const TNetworkAddress& impalad_address() const { return host_; }
  int state_idx() const { return state_idx_; }

  /// only valid after Exec()
  int64_t rpc_latency() const { return rpc_latency_; }

  /// Return true if execution at this backend is done.
  bool IsDone();

 private:
  /// Execution stats for a single fragment instance.
  /// Not thread-safe.
  class InstanceStats {
   public:
    InstanceStats(const FInstanceExecParams& exec_params, FragmentStats* fragment_stats,
        ObjectPool* obj_pool);

    /// Update 'this' with exec_status, the fragment instances' TExecStats in
    /// exec_summary, and 'progress_updater' with the number of
    /// newly completed scan ranges. Also updates the instance's avg profile.
    void Update(const TFragmentInstanceExecStatus& exec_status,
        ExecSummary* exec_summary, ProgressUpdater* scan_range_progress);

    int per_fragment_instance_idx() const {
      return exec_params_.per_fragment_instance_idx;
    }

   private:
    friend class BackendState;

    /// query lifetime
    const FInstanceExecParams& exec_params_;

    /// owned by coordinator object pool provided in the c'tor, created in Update()
    RuntimeProfile* profile_;

    /// true if the final report has been received for the fragment instance.
    /// Used to handle duplicate done ReportExecStatus RPC messages. Used only
    /// in ApplyExecStatusReport()
    bool done_;

    /// true after the first call to profile->Update()
    bool profile_created_;

    /// cumulative size of all splits; set in c'tor
    int64_t total_split_size_;

    /// wall clock timer for this instance
    MonotonicStopWatch stopwatch_;

    /// total scan ranges complete across all scan nodes
    int64_t total_ranges_complete_;

    /// SCAN_RANGES_COMPLETE_COUNTERs in profile_
    std::vector<RuntimeProfile::Counter*> scan_ranges_complete_counters_;

    /// PER_HOST_PEAK_MEM_COUNTER
    RuntimeProfile::Counter* peak_mem_counter_;

    /// Extract scan_ranges_complete_counters_ and peak_mem_counter_ from profile_.
    void InitCounters();
  };

  const TUniqueId query_id_;
  const int state_idx_;  /// index of 'this' in Coordinator::backend_states_
  const TRuntimeFilterMode::type filter_mode_;

  /// all instances of a particular fragment are contiguous in this vector;
  /// query lifetime
  std::vector<const FInstanceExecParams*> instance_params_list_;

  /// map from instance idx to InstanceStats, the latter live in the obj_pool parameter
  /// of Init()
  std::unordered_map<int, InstanceStats*> instance_stats_map_;

  /// indices of fragments executing on this backend, populated in Init()
  std::unordered_set<int> fragments_;

  TNetworkAddress host_;

  /// protects fields below
  /// lock ordering: Coordinator::lock_ must only be obtained *prior* to lock_
  boost::mutex lock_;

  // number of in-flight instances
  int num_remaining_instances_;

  /// If the status indicates an error status, execution has either been aborted by the
  /// executing impalad (which then reported the error) or cancellation has been
  /// initiated; either way, execution must not be cancelled.
  Status status_;

  /// Id of the first fragment instance that reports an error status.
  /// Invalid if no fragment instance has reported an error status.
  TUniqueId failed_instance_id_;

  /// Errors reported by this fragment instance.
  ErrorLogMap error_log_;

  /// Time, in ms, that it took to execute the ExecRemoteFragment() RPC.
  int64_t rpc_latency_;

  /// If true, ExecPlanFragment() rpc has been sent - even if it was not determined to be
  /// successful.
  bool rpc_sent_;

  /// peak memory used for this query (value of that node's query memtracker's
  /// peak_consumption()
  int64_t peak_consumption_;

  /// Fill in rpc_params based on state. Uses filter_routing_table to remove filters
  /// that weren't selected during its construction.
  void SetRpcParams(const DebugOptions& debug_options,
      const FilterRoutingTable& filter_routing_table,
      TExecQueryFInstancesParams* rpc_params);

  /// Return true if execution at this backend is done. Doesn't acquire lock.
  bool IsDoneInternal() const;
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
