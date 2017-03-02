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

#include "runtime/coordinator.h"

#include <map>
#include <memory>
#include <thrift/protocol/TDebugProtocol.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/min.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/median.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/unordered_set.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>
#include <errno.h>

#include "common/logging.h"
#include "exec/data-sink.h"
#include "exec/plan-root-sink.h"
#include "exec/scan-node.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Partitions_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "runtime/backend-client.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/data-stream-sender.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/mem-tracker.h"
#include "runtime/parallel-executor.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "scheduling/scheduler.h"
#include "util/bloom-filter.h"
#include "util/container-util.h"
#include "util/counting-barrier.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/summary-util.h"
#include "util/table-printer.h"
#include "util/uid-util.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace strings;
namespace accumulators = boost::accumulators;
using boost::algorithm::iequals;
using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;
using boost::filesystem::path;
using std::unique_ptr;

DECLARE_int32(be_port);
DECLARE_string(hostname);

DEFINE_bool(insert_inherit_permissions, false, "If true, new directories created by "
    "INSERTs will inherit the permissions of their parent directories");

namespace impala {

// Maximum number of fragment instances that can publish each broadcast filter.
static const int MAX_BROADCAST_FILTER_PRODUCERS = 3;

// container for debug options in TPlanFragmentInstanceCtx (debug_node, debug_action,
// debug_phase)
struct DebugOptions {
  int instance_state_idx;
  int node_id;
  TDebugAction::type action;
  TExecNodePhase::type phase;  // INVALID: debug options invalid

  DebugOptions()
    : instance_state_idx(-1), node_id(-1), action(TDebugAction::WAIT),
      phase(TExecNodePhase::INVALID) {}

  // If these debug options apply to the candidate fragment instance, returns true
  // otherwise returns false.
  bool IsApplicable(int candidate_instance_state_idx) {
    if (phase == TExecNodePhase::INVALID) return false;
    return (instance_state_idx == -1 ||
        instance_state_idx == candidate_instance_state_idx);
  }
};

/// Execution state of a particular fragment instance.
///
/// Concurrent accesses:
/// - updates through UpdateFragmentExecStatus()
class Coordinator::InstanceState {
 public:
  InstanceState(const FInstanceExecParams& params, ObjectPool* obj_pool)
    : exec_params_(params),
      total_split_size_(0),
      profile_(nullptr),
      total_ranges_complete_(0),
      rpc_latency_(0),
      rpc_sent_(false),
      done_(false),
      profile_created_(false) {
    const string& profile_name = Substitute("Instance $0 (host=$1)",
        PrintId(params.instance_id), lexical_cast<string>(params.host));
    profile_ = obj_pool->Add(new RuntimeProfile(obj_pool, profile_name));
  }

  /// Called to set the initial status of the fragment instance after the
  /// ExecRemoteFragment() RPC has returned. If 'rpc_sent' is true,
  /// CancelFragmentInstances() will include this instance in the set of potential
  /// fragment instances to cancel.
  void SetInitialStatus(const Status& status, bool rpc_sent) {
    DCHECK(!rpc_sent_);
    rpc_sent_ = rpc_sent;
    status_ = status;
    if (!status_.ok()) return;
    stopwatch_.Start();
  }

  /// Computes sum of split sizes of leftmost scan.
  void ComputeTotalSplitSize(const PerNodeScanRanges& per_node_scan_ranges);

  /// Updates the total number of scan ranges complete for this fragment. Returns the
  /// delta since the last time this was called. Not thread-safe without lock() being
  /// acquired by the caller.
  int64_t UpdateNumScanRangesCompleted();

  // The following getters do not require lock() to be held.
  const TUniqueId& fragment_instance_id() const { return exec_params_.instance_id; }
  FragmentIdx fragment_idx() const { return exec_params_.fragment().idx; }
  MonotonicStopWatch* stopwatch() { return &stopwatch_; }
  const TNetworkAddress& impalad_address() const { return exec_params_.host; }
  int64_t total_split_size() const { return total_split_size_; }
  bool done() const { return done_; }
  int per_fragment_instance_idx() const { return exec_params_.per_fragment_instance_idx; }
  bool rpc_sent() const { return rpc_sent_; }
  int64_t rpc_latency() const { return rpc_latency_; }

  mutex* lock() { return &lock_; }

  void set_status(const Status& status) { status_ = status; }
  void set_done(bool done) { done_ = done; }
  void set_rpc_latency(int64_t millis) {
    DCHECK_EQ(rpc_latency_, 0);
    rpc_latency_ = millis;
  }

  // Return values of the following functions must be accessed with lock() held
  RuntimeProfile* profile() const { return profile_; }
  void set_profile(RuntimeProfile* profile) { profile_ = profile; }
  FragmentInstanceCounters* aggregate_counters() { return &aggregate_counters_; }
  ErrorLogMap* error_log() { return &error_log_; }
  Status* status() { return &status_; }

  /// Registers that the fragment instance's profile has been created and initially
  /// populated. Returns whether the profile had already been initialised so that callers
  /// can tell if they are the first to do so. Not thread-safe.
  bool SetProfileCreated() {
    bool cur = profile_created_;
    profile_created_ = true;
    return cur;
  }

 private:
  const FInstanceExecParams& exec_params_;

  /// Wall clock timer for this fragment.
  MonotonicStopWatch stopwatch_;

  /// Summed across all splits; in bytes.
  int64_t total_split_size_;

  /// Protects fields below. Can be held while doing an RPC, so SpinLock is a bad idea.
  /// lock ordering: Coordinator::lock_ must only be obtained *prior* to lock_
  mutex lock_;

  /// If the status indicates an error status, execution of this fragment has either been
  /// aborted by the executing impalad (which then reported the error) or cancellation has
  /// been initiated; either way, execution must not be cancelled.
  Status status_;

  /// Owned by coordinator object pool provided in the c'tor
  RuntimeProfile* profile_;

  /// Errors reported by this fragment instance.
  ErrorLogMap error_log_;

  /// Total scan ranges complete across all scan nodes.
  int64_t total_ranges_complete_;

  /// Summary counters aggregated across the duration of execution.
  FragmentInstanceCounters aggregate_counters_;

  /// Time, in ms, that it took to execute the ExecRemoteFragment() RPC.
  int64_t rpc_latency_;

  /// If true, ExecPlanFragment() rpc has been sent - even if it was not determined to be
  /// successful.
  bool rpc_sent_;

  /// If true, execution terminated; do not cancel in that case.
  bool done_;

  /// True after the first call to profile->Update()
  bool profile_created_;
};

/// Represents a runtime filter target.
struct Coordinator::FilterTarget {
  TPlanNodeId node_id;
  bool is_local;
  bool is_bound_by_partition_columns;

  // indices into fragment_instance_states_
  unordered_set<int> fragment_instance_state_idxs;

  FilterTarget(const TRuntimeFilterTargetDesc& tFilterTarget) {
    node_id = tFilterTarget.node_id;
    is_bound_by_partition_columns = tFilterTarget.is_bound_by_partition_columns;
    is_local = tFilterTarget.is_local_target;
  }
};


/// State of filters that are received for aggregation.
///
/// A broadcast join filter is published as soon as the first update is received for it
/// and subsequent updates are ignored (as they will be the same).
/// Updates for a partitioned join filter are aggregated in 'bloom_filter' and this is
/// published once 'pending_count' reaches 0 and if the filter was not disabled before
/// that.
///
/// A filter is disabled if an always_true filter update is received, an OOM is hit,
/// filter aggregation is complete or if the query is complete.
/// Once a filter is disabled, subsequent updates for that filter are ignored.
class Coordinator::FilterState {
 public:
  FilterState(const TRuntimeFilterDesc& desc, const TPlanNodeId& src) : desc_(desc),
      src_(src), pending_count_(0), first_arrival_time_(0L), completion_time_(0L),
      disabled_(false) { }

  TBloomFilter* bloom_filter() { return bloom_filter_.get(); }
  boost::unordered_set<int>* src_fragment_instance_state_idxs() {
    return &src_fragment_instance_state_idxs_;
  }
  const boost::unordered_set<int>& src_fragment_instance_state_idxs() const {
    return src_fragment_instance_state_idxs_;
  }
  std::vector<FilterTarget>* targets() { return &targets_; }
  const std::vector<FilterTarget>& targets() const { return targets_; }
  int64_t first_arrival_time() const { return first_arrival_time_; }
  int64_t completion_time() const { return completion_time_; }
  const TPlanNodeId& src() const { return src_; }
  const TRuntimeFilterDesc& desc() const { return desc_; }
  int pending_count() const { return pending_count_; }
  void set_pending_count(int pending_count) { pending_count_ = pending_count; }
  bool disabled() const { return disabled_; }

  /// Aggregates partitioned join filters and updates memory consumption.
  /// Disables filter if always_true filter is received or OOM is hit.
  void ApplyUpdate(const TUpdateFilterParams& params, Coordinator* coord);

  /// Disables a filter. A disabled filter consumes no memory.
  void Disable(MemTracker* tracker);

 private:
  /// Contains the specification of the runtime filter.
  TRuntimeFilterDesc desc_;

  TPlanNodeId src_;
  std::vector<FilterTarget> targets_;

  // Index into fragment_instance_states_ for source fragment instances.
  boost::unordered_set<int> src_fragment_instance_state_idxs_;

  /// Number of remaining backends to hear from before filter is complete.
  int pending_count_;

  /// BloomFilter aggregated from all source plan nodes, to be broadcast to all
  /// destination plan fragment instances. Owned by this object so that it can be
  /// deallocated once finished with. Only set for partitioned joins (broadcast joins
  /// need no aggregation).
  /// In order to avoid memory spikes, an incoming filter is moved (vs. copied) to the
  /// output structure in the case of a broadcast join. Similarly, for partitioned joins,
  /// the filter is moved from the following member to the output structure.
  std::unique_ptr<TBloomFilter> bloom_filter_;

  /// Time at which first local filter arrived.
  int64_t first_arrival_time_;

  /// Time at which all local filters arrived.
  int64_t completion_time_;

  /// True if the filter is permanently disabled for this query.
  bool disabled_;

  /// TODO: Add a per-object lock so that we can avoid holding the global filter_lock_
  /// for every filter update.

};

void Coordinator::InstanceState::ComputeTotalSplitSize(
    const PerNodeScanRanges& per_node_scan_ranges) {
  total_split_size_ = 0;

  for (const PerNodeScanRanges::value_type& entry: per_node_scan_ranges) {
    for (const TScanRangeParams& scan_range_params: entry.second) {
      if (!scan_range_params.scan_range.__isset.hdfs_file_split) continue;
      total_split_size_ += scan_range_params.scan_range.hdfs_file_split.length;
    }
  }
}

int64_t Coordinator::InstanceState::UpdateNumScanRangesCompleted() {
  int64_t total = 0;
  CounterMap& complete = aggregate_counters_.scan_ranges_complete_counters;
  for (CounterMap::iterator i = complete.begin(); i != complete.end(); ++i) {
    total += i->second->value();
  }
  int64_t delta = total - total_ranges_complete_;
  total_ranges_complete_ = total;
  DCHECK_GE(delta, 0);
  return delta;
}

Coordinator::Coordinator(const QuerySchedule& schedule, ExecEnv* exec_env,
    RuntimeProfile::EventSequence* events)
  : schedule_(schedule),
    exec_env_(exec_env),
    has_called_wait_(false),
    returned_all_results_(false),
    query_state_(nullptr),
    num_remaining_fragment_instances_(0),
    obj_pool_(new ObjectPool()),
    query_events_(events),
    filter_routing_table_complete_(false),
    filter_mode_(schedule.query_options().runtime_filter_mode),
    torn_down_(false) {}

Coordinator::~Coordinator() {
  DCHECK(torn_down_) << "TearDown() must be called before Coordinator is destroyed";
}

PlanFragmentExecutor* Coordinator::executor() {
  return coord_instance_->executor();
}

TExecNodePhase::type GetExecNodePhase(const string& key) {
  map<int, const char*>::const_iterator entry =
      _TExecNodePhase_VALUES_TO_NAMES.begin();
  for (; entry != _TExecNodePhase_VALUES_TO_NAMES.end(); ++entry) {
    if (iequals(key, (*entry).second)) {
      return static_cast<TExecNodePhase::type>(entry->first);
    }
  }
  return TExecNodePhase::INVALID;
}

TDebugAction::type GetDebugAction(const string& key) {
  map<int, const char*>::const_iterator entry =
      _TDebugAction_VALUES_TO_NAMES.begin();
  for (; entry != _TDebugAction_VALUES_TO_NAMES.end(); ++entry) {
    if (iequals(key, (*entry).second)) {
      return static_cast<TDebugAction::type>(entry->first);
    }
  }
  return TDebugAction::WAIT;
}

static void ProcessQueryOptions(
    const TQueryOptions& query_options, DebugOptions* debug_options) {
  DCHECK(debug_options != NULL);
  if (!query_options.__isset.debug_action || query_options.debug_action.empty()) {
    debug_options->phase = TExecNodePhase::INVALID;  // signal not set
    return;
  }
  vector<string> components;
  split(components, query_options.debug_action, is_any_of(":"), token_compress_on);
  if (components.size() < 3 || components.size() > 4) return;
  if (components.size() == 3) {
    debug_options->instance_state_idx = -1;
    debug_options->node_id = atoi(components[0].c_str());
    debug_options->phase = GetExecNodePhase(components[1]);
    debug_options->action = GetDebugAction(components[2]);
  } else {
    debug_options->instance_state_idx = atoi(components[0].c_str());
    debug_options->node_id = atoi(components[1].c_str());
    debug_options->phase = GetExecNodePhase(components[2]);
    debug_options->action = GetDebugAction(components[3]);
  }
  DCHECK(!(debug_options->phase == TExecNodePhase::CLOSE &&
           debug_options->action == TDebugAction::WAIT))
      << "Do not use CLOSE:WAIT debug actions "
      << "because nodes cannot be cancelled in Close()";
}

Status Coordinator::Exec() {
  const TQueryExecRequest& request = schedule_.request();
  DCHECK(request.plan_exec_info.size() > 0);

  needs_finalization_ = request.__isset.finalize_params;
  if (needs_finalization_) finalize_params_ = request.finalize_params;

  VLOG_QUERY << "Exec() query_id=" << schedule_.query_id()
             << " stmt=" << request.query_ctx.client_request.stmt;
  stmt_type_ = request.stmt_type;
  query_id_ = schedule_.query_id();
  desc_tbl_ = request.desc_tbl;
  query_ctx_ = request.query_ctx;

  query_profile_.reset(
      new RuntimeProfile(obj_pool(), "Execution Profile " + PrintId(query_id_)));
  finalization_timer_ = ADD_TIMER(query_profile_, "FinalizationTimer");
  filter_updates_received_ = ADD_COUNTER(query_profile_, "FiltersReceived", TUnit::UNIT);

  SCOPED_TIMER(query_profile_->total_time_counter());

  // initialize progress updater
  const string& str = Substitute("Query $0", PrintId(query_id_));
  progress_.Init(str, schedule_.num_scan_ranges());

  // runtime filters not yet supported for mt execution
  bool is_mt_execution = request.query_ctx.client_request.query_options.mt_dop > 0;
  if (is_mt_execution) filter_mode_ = TRuntimeFilterMode::OFF;

  // to keep things simple, make async Cancel() calls wait until plan fragment
  // execution has been initiated, otherwise we might try to cancel fragment
  // execution at Impala daemons where it hasn't even started
  lock_guard<mutex> l(lock_);

  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->CreateQueryState(
      query_ctx_, schedule_.request_pool());
  filter_mem_tracker_.reset(new MemTracker(
      -1, "Runtime Filter (Coordinator)", query_state_->query_mem_tracker(), false));

  InitExecProfiles();
  InitExecSummary();
  StartFInstances();

  // In the error case, it's safe to return and not to get coord_sink_ here to close - if
  // there was an error, but the coordinator fragment was successfully started, it should
  // cancel itself when it receives an error status after reporting its profile.
  RETURN_IF_ERROR(FinishInstanceStartup());

  // Grab executor and wait until Prepare() has finished so that runtime state etc. will
  // be set up. Must do this here in order to get a reference to coord_instance_
  // so that coord_sink_ remains valid throughout query lifetime.
  if (schedule_.GetCoordFragment() != nullptr) {
    coord_instance_ = query_state_->GetFInstanceState(query_id_);
    if (coord_instance_ == nullptr) {
      // Coordinator instance might have failed and unregistered itself even
      // though it was successfully started (e.g. Prepare() might have failed).
      InstanceState* coord_state = fragment_instance_states_[0];
      DCHECK(coord_state != nullptr);
      lock_guard<mutex> instance_state_lock(*coord_state->lock());
      // Try and return the fragment instance status if it was already set.
      // TODO: Consider waiting for coord_state->done() here.
      RETURN_IF_ERROR(*coord_state->status());
      return Status(
          Substitute("Coordinator fragment instance ($0) failed", PrintId(query_id_)));
    }

    // When WaitForPrepare() returns OK(), the executor's root sink will be set up. At
    // that point, the coordinator must be sure to call root_sink()->CloseConsumer(); the
    // fragment instance's executor will not complete until that point.
    // TODO: Consider moving this to Wait().
    Status prepare_status = executor()->WaitForPrepare();
    coord_sink_ = executor()->root_sink();
    RETURN_IF_ERROR(prepare_status);
    DCHECK(coord_sink_ != nullptr);
  }

  PrintFragmentInstanceInfo();
  return Status::OK();
}

void Coordinator::UpdateFilterRoutingTable(const FragmentExecParams& fragment_params) {
  DCHECK(schedule_.request().query_ctx.client_request.query_options.mt_dop == 0);
  int num_hosts = fragment_params.instance_exec_params.size();
  DCHECK_GT(num_hosts, 0);
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "UpdateFilterRoutingTable() called although runtime filters are disabled";
  DCHECK(!filter_routing_table_complete_)
      << "UpdateFilterRoutingTable() called after setting filter_routing_table_complete_";

  for (const TPlanNode& plan_node: fragment_params.fragment.plan.nodes) {
    if (!plan_node.__isset.runtime_filters) continue;
    for (const TRuntimeFilterDesc& filter: plan_node.runtime_filters) {
      if (filter_mode_ == TRuntimeFilterMode::LOCAL && !filter.has_local_targets) {
        continue;
      }
      FilterRoutingTable::iterator i = filter_routing_table_.emplace(
          filter.filter_id, FilterState(filter, plan_node.node_id)).first;
      FilterState* f = &(i->second);
      if (plan_node.__isset.hash_join_node) {
        // Set the 'pending_count_' to zero to indicate that for a filter with local-only
        // targets the coordinator does not expect to receive any filter updates.
        int pending_count = filter.is_broadcast_join ?
            (filter.has_remote_targets ? 1 : 0) : num_hosts;
        f->set_pending_count(pending_count);
        vector<int> src_idxs = fragment_params.GetInstanceIdxs();

        // If this is a broadcast join with only non-local targets, build and publish it
        // on MAX_BROADCAST_FILTER_PRODUCERS instances. If this is not a broadcast join
        // or it is a broadcast join with local targets, it should be generated
        // everywhere the join is executed.
        if (filter.is_broadcast_join && !filter.has_local_targets
            && num_hosts > MAX_BROADCAST_FILTER_PRODUCERS) {
          random_shuffle(src_idxs.begin(), src_idxs.end());
          src_idxs.resize(MAX_BROADCAST_FILTER_PRODUCERS);
        }
        f->src_fragment_instance_state_idxs()->insert(src_idxs.begin(), src_idxs.end());
      } else if (plan_node.__isset.hdfs_scan_node) {
        auto it = filter.planid_to_target_ndx.find(plan_node.node_id);
        DCHECK(it != filter.planid_to_target_ndx.end());
        const TRuntimeFilterTargetDesc& tFilterTarget = filter.targets[it->second];
        if (filter_mode_ == TRuntimeFilterMode::LOCAL && !tFilterTarget.is_local_target) {
          continue;
        }
        vector<int> idxs = fragment_params.GetInstanceIdxs();
        FilterTarget target(tFilterTarget);
        target.fragment_instance_state_idxs.insert(idxs.begin(), idxs.end());
        f->targets()->push_back(target);
      } else {
        DCHECK(false) << "Unexpected plan node with runtime filters: "
            << ThriftDebugString(plan_node);
      }
    }
  }
}

void Coordinator::StartFInstances() {
  int num_fragment_instances = schedule_.GetNumFragmentInstances();
  DCHECK_GT(num_fragment_instances, 0);

  fragment_instance_states_.resize(num_fragment_instances);
  exec_complete_barrier_.reset(new CountingBarrier(num_fragment_instances));
  num_remaining_fragment_instances_ = num_fragment_instances;

  DebugOptions debug_options;
  ProcessQueryOptions(schedule_.query_options(), &debug_options);
  const TQueryExecRequest& request = schedule_.request();

  VLOG_QUERY << "starting " << num_fragment_instances << " fragment instances for query "
             << query_id_;
  query_events_->MarkEvent(
      Substitute("Ready to start $0 fragment instances", num_fragment_instances));

  // TODO-MT: populate the runtime filter routing table
  // This requires local aggregation of filters prior to sending
  // for broadcast joins in order to avoid more complicated merge logic here.

  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    DCHECK_EQ(request.plan_exec_info.size(), 1);
    // Populate the runtime filter routing table. This should happen before starting the
    // fragment instances. This code anticipates the indices of the instance states
    // created later on in ExecRemoteFragment()
    for (const FragmentExecParams& fragment_params: schedule_.fragment_exec_params()) {
      UpdateFilterRoutingTable(fragment_params);
    }
    MarkFilterRoutingTableComplete();
  }

  int num_instances = 0;
  for (const FragmentExecParams& fragment_params: schedule_.fragment_exec_params()) {
    num_instances += fragment_params.instance_exec_params.size();
    for (const FInstanceExecParams& instance_params:
        fragment_params.instance_exec_params) {
      InstanceState* exec_state = obj_pool()->Add(
          new InstanceState(instance_params, obj_pool()));
      int instance_state_idx = GetInstanceIdx(instance_params.instance_id);
      fragment_instance_states_[instance_state_idx] = exec_state;

      DebugOptions* instance_debug_options =
          debug_options.IsApplicable(instance_state_idx) ? &debug_options : NULL;
      exec_env_->fragment_exec_thread_pool()->Offer(
          std::bind(&Coordinator::ExecRemoteFInstance,
            this, std::cref(instance_params), instance_debug_options));
    }
  }
  exec_complete_barrier_->Wait();
  VLOG_QUERY << "started " << num_fragment_instances << " fragment instances for query "
      << query_id_;
  query_events_->MarkEvent(
      Substitute("All $0 fragment instances started", num_instances));
}

Status Coordinator::FinishInstanceStartup() {
  Status status = Status::OK();
  const TMetricDef& def =
      MakeTMetricDef("fragment-latencies", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  HistogramMetric latencies(def, 20000, 3);
  for (InstanceState* exec_state: fragment_instance_states_) {
    lock_guard<mutex> l(*exec_state->lock());
    // Preserve the first non-OK status, if there is one
    if (status.ok()) status = *exec_state->status();
    latencies.Update(exec_state->rpc_latency());
  }

  query_profile_->AddInfoString(
      "Fragment instance start latencies", latencies.ToHumanReadable());

  if (!status.ok()) {
    DCHECK(query_status_.ok()); // nobody should have been able to cancel
    query_status_ = status;
    CancelInternal();
  }
  return status;
}

string Coordinator::FilterDebugString() {
  TablePrinter table_printer;
  table_printer.AddColumn("ID", false);
  table_printer.AddColumn("Src. Node", false);
  table_printer.AddColumn("Tgt. Node(s)", false);
  table_printer.AddColumn("Targets", false);
  table_printer.AddColumn("Target type", false);
  table_printer.AddColumn("Partition filter", false);

  // Distribution metrics are only meaningful if the coordinator is routing the filter.
  if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
    table_printer.AddColumn("Pending (Expected)", false);
    table_printer.AddColumn("First arrived", false);
    table_printer.AddColumn("Completed", false);
  }
  table_printer.AddColumn("Enabled", false);
  lock_guard<SpinLock> l(filter_lock_);
  for (FilterRoutingTable::value_type& v: filter_routing_table_) {
    vector<string> row;
    const FilterState& state = v.second;
    row.push_back(lexical_cast<string>(v.first));
    row.push_back(lexical_cast<string>(state.src()));
    vector<string> target_ids;
    vector<string> num_target_instances;
    vector<string> target_types;
    vector<string> partition_filter;
    for (const FilterTarget& target: state.targets()) {
      target_ids.push_back(lexical_cast<string>(target.node_id));
      num_target_instances.push_back(
          lexical_cast<string>(target.fragment_instance_state_idxs.size()));
      target_types.push_back(target.is_local ? "LOCAL" : "REMOTE");
      partition_filter.push_back(target.is_bound_by_partition_columns ? "true" : "false");
    }
    row.push_back(join(target_ids, ", "));
    row.push_back(join(num_target_instances, ", "));
    row.push_back(join(target_types, ", "));
    row.push_back(join(partition_filter, ", "));

    if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
      int pending_count = state.completion_time() != 0L ? 0 : state.pending_count();
      row.push_back(Substitute("$0 ($1)", pending_count,
          state.src_fragment_instance_state_idxs().size()));
      if (state.first_arrival_time() == 0L) {
        row.push_back("N/A");
      } else {
        row.push_back(PrettyPrinter::Print(state.first_arrival_time(), TUnit::TIME_NS));
      }
      if (state.completion_time() == 0L) {
        row.push_back("N/A");
      } else {
        row.push_back(PrettyPrinter::Print(state.completion_time(), TUnit::TIME_NS));
      }
    }

    row.push_back(!state.disabled() ? "true" : "false");
    table_printer.AddRow(row);
  }
  // Add a line break, as in all contexts this is called we need to start a new line to
  // print it correctly.
  return Substitute("\n$0", table_printer.ToString());
}

void Coordinator::MarkFilterRoutingTableComplete() {
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "MarkFilterRoutingTableComplete() called although runtime filters are disabled";
  query_profile_->AddInfoString(
      "Number of filters", Substitute("$0", filter_routing_table_.size()));
  query_profile_->AddInfoString("Filter routing table", FilterDebugString());
  if (VLOG_IS_ON(2)) VLOG_QUERY << FilterDebugString();
  filter_routing_table_complete_ = true;
}

Status Coordinator::GetStatus() {
  lock_guard<mutex> l(lock_);
  return query_status_;
}

Status Coordinator::UpdateStatus(const Status& status, const TUniqueId& instance_id,
    const string& instance_hostname) {
  {
    lock_guard<mutex> l(lock_);

    // The query is done and we are just waiting for fragment instances to clean up.
    // Ignore their cancelled updates.
    if (returned_all_results_ && status.IsCancelled()) return query_status_;

    // nothing to update
    if (status.ok()) return query_status_;

    // don't override an error status; also, cancellation has already started
    if (!query_status_.ok()) return query_status_;

    query_status_ = status;
    CancelInternal();
  }

  // Log the id of the fragment that first failed so we can track it down easier.
  VLOG_QUERY << "Query id=" << query_id_ << " failed because fragment id="
             << instance_id << " on host=" << instance_hostname << " failed.";

  return query_status_;
}

void Coordinator::PopulatePathPermissionCache(hdfsFS fs, const string& path_str,
    PermissionCache* permissions_cache) {
  // Find out if the path begins with a hdfs:// -style prefix, and remove it and the
  // location (e.g. host:port) if so.
  int scheme_end = path_str.find("://");
  string stripped_str;
  if (scheme_end != string::npos) {
    // Skip past the subsequent location:port/ prefix.
    stripped_str = path_str.substr(path_str.find("/", scheme_end + 3));
  } else {
    stripped_str = path_str;
  }

  // Get the list of path components, used to build all path prefixes.
  vector<string> components;
  split(components, stripped_str, is_any_of("/"));

  // Build a set of all prefixes (including the complete string) of stripped_path. So
  // /a/b/c/d leads to a vector of: /a, /a/b, /a/b/c, /a/b/c/d
  vector<string> prefixes;
  // Stores the current prefix
  stringstream accumulator;
  for (const string& component: components) {
    if (component.empty()) continue;
    accumulator << "/" << component;
    prefixes.push_back(accumulator.str());
  }

  // Now for each prefix, stat() it to see if a) it exists and b) if so what its
  // permissions are. When we meet a directory that doesn't exist, we record the fact that
  // we need to create it, and the permissions of its parent dir to inherit.
  //
  // Every prefix is recorded in the PermissionCache so we don't do more than one stat()
  // for each path. If we need to create the directory, we record it as the pair (true,
  // perms) so that the caller can identify which directories need their permissions
  // explicitly set.

  // Set to the permission of the immediate parent (i.e. the permissions to inherit if the
  // current dir doesn't exist).
  short permissions = 0;
  for (const string& path: prefixes) {
    PermissionCache::const_iterator it = permissions_cache->find(path);
    if (it == permissions_cache->end()) {
      hdfsFileInfo* info = hdfsGetPathInfo(fs, path.c_str());
      if (info != NULL) {
        // File exists, so fill the cache with its current permissions.
        permissions_cache->insert(
            make_pair(path, make_pair(false, info->mPermissions)));
        permissions = info->mPermissions;
        hdfsFreeFileInfo(info, 1);
      } else {
        // File doesn't exist, so we need to set its permissions to its immediate parent
        // once it's been created.
        permissions_cache->insert(make_pair(path, make_pair(true, permissions)));
      }
    } else {
      permissions = it->second.second;
    }
  }
}

Status Coordinator::FinalizeSuccessfulInsert() {
  PermissionCache permissions_cache;
  HdfsFsCache::HdfsFsMap filesystem_connection_cache;
  HdfsOperationSet partition_create_ops(&filesystem_connection_cache);

  // INSERT finalization happens in the five following steps
  // 1. If OVERWRITE, remove all the files in the target directory
  // 2. Create all the necessary partition directories.
  DescriptorTbl* descriptor_table;
  DescriptorTbl::Create(obj_pool(), desc_tbl_, &descriptor_table);
  HdfsTableDescriptor* hdfs_table = static_cast<HdfsTableDescriptor*>(
      descriptor_table->GetTableDescriptor(finalize_params_.table_id));
  DCHECK(hdfs_table != NULL) << "INSERT target table not known in descriptor table: "
                             << finalize_params_.table_id;

  // Loop over all partitions that were updated by this insert, and create the set of
  // filesystem operations required to create the correct partition structure on disk.
  for (const PartitionStatusMap::value_type& partition: per_partition_status_) {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "Overwrite/PartitionCreationTimer",
          "FinalizationTimer"));
    // INSERT allows writes to tables that have partitions on multiple filesystems.
    // So we need to open connections to different filesystems as necessary. We use a
    // local connection cache and populate it with one connection per filesystem that the
    // partitions are on.
    hdfsFS partition_fs_connection;
    RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      partition.second.partition_base_dir, &partition_fs_connection,
          &filesystem_connection_cache));

    // Look up the partition in the descriptor table.
    stringstream part_path_ss;
    if (partition.second.id == -1) {
      // If this is a non-existant partition, use the default partition location of
      // <base_dir>/part_key_1=val/part_key_2=val/...
      part_path_ss << finalize_params_.hdfs_base_dir << "/" << partition.first;
    } else {
      HdfsPartitionDescriptor* part = hdfs_table->GetPartition(partition.second.id);
      DCHECK(part != NULL) << "table_id=" << hdfs_table->id()
                           << " partition_id=" << partition.second.id
                           << "\n" <<  PrintThrift(runtime_state()->instance_ctx());
      part_path_ss << part->location();
    }
    const string& part_path = part_path_ss.str();
    bool is_s3_path = IsS3APath(part_path.c_str());

    // If this is an overwrite insert, we will need to delete any updated partitions
    if (finalize_params_.is_overwrite) {
      if (partition.first.empty()) {
        // If the root directory is written to, then the table must not be partitioned
        DCHECK(per_partition_status_.size() == 1);
        // We need to be a little more careful, and only delete data files in the root
        // because the tmp directories the sink(s) wrote are there also.
        // So only delete files in the table directory - all files are treated as data
        // files by Hive and Impala, but directories are ignored (and may legitimately
        // be used to store permanent non-table data by other applications).
        int num_files = 0;
        // hfdsListDirectory() only sets errno if there is an error, but it doesn't set
        // it to 0 if the call succeed. When there is no error, errno could be any
        // value. So need to clear errno before calling it.
        // Once HDFS-8407 is fixed, the errno reset won't be needed.
        errno = 0;
        hdfsFileInfo* existing_files =
            hdfsListDirectory(partition_fs_connection, part_path.c_str(), &num_files);
        if (existing_files == NULL && errno == EAGAIN) {
          errno = 0;
          existing_files =
              hdfsListDirectory(partition_fs_connection, part_path.c_str(), &num_files);
        }
        // hdfsListDirectory() returns NULL not only when there is an error but also
        // when the directory is empty(HDFS-8407). Need to check errno to make sure
        // the call fails.
        if (existing_files == NULL && errno != 0) {
          return GetHdfsErrorMsg("Could not list directory: ", part_path);
        }
        for (int i = 0; i < num_files; ++i) {
          const string filename = path(existing_files[i].mName).filename().string();
          if (existing_files[i].mKind == kObjectKindFile && !IsHiddenFile(filename)) {
            partition_create_ops.Add(DELETE, existing_files[i].mName);
          }
        }
        hdfsFreeFileInfo(existing_files, num_files);
      } else {
        // This is a partition directory, not the root directory; we can delete
        // recursively with abandon, after checking that it ever existed.
        // TODO: There's a potential race here between checking for the directory
        // and a third-party deleting it.
        if (FLAGS_insert_inherit_permissions && !is_s3_path) {
          // There is no directory structure in S3, so "inheriting" permissions is not
          // possible.
          // TODO: Try to mimic inheriting permissions for S3.
          PopulatePathPermissionCache(
              partition_fs_connection, part_path, &permissions_cache);
        }
        // S3 doesn't have a directory structure, so we technically wouldn't need to
        // CREATE_DIR on S3. However, libhdfs always checks if a path exists before
        // carrying out an operation on that path. So we still need to call CREATE_DIR
        // before we access that path due to this limitation.
        if (hdfsExists(partition_fs_connection, part_path.c_str()) != -1) {
          partition_create_ops.Add(DELETE_THEN_CREATE, part_path);
        } else {
          // Otherwise just create the directory.
          partition_create_ops.Add(CREATE_DIR, part_path);
        }
      }
    } else if (!is_s3_path
        || !query_ctx_.client_request.query_options.s3_skip_insert_staging) {
      // If the S3_SKIP_INSERT_STAGING query option is set, then the partition directories
      // would have already been created by the table sinks.
      if (FLAGS_insert_inherit_permissions && !is_s3_path) {
        PopulatePathPermissionCache(
            partition_fs_connection, part_path, &permissions_cache);
      }
      if (hdfsExists(partition_fs_connection, part_path.c_str()) == -1) {
        partition_create_ops.Add(CREATE_DIR, part_path);
      }
    }
  }

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "Overwrite/PartitionCreationTimer",
          "FinalizationTimer"));
    if (!partition_create_ops.Execute(exec_env_->hdfs_op_thread_pool(), false)) {
      for (const HdfsOperationSet::Error& err: partition_create_ops.errors()) {
        // It's ok to ignore errors creating the directories, since they may already
        // exist. If there are permission errors, we'll run into them later.
        if (err.first->op() != CREATE_DIR) {
          return Status(Substitute(
              "Error(s) deleting partition directories. First error (of $0) was: $1",
              partition_create_ops.errors().size(), err.second));
        }
      }
    }
  }

  // 3. Move all tmp files
  HdfsOperationSet move_ops(&filesystem_connection_cache);
  HdfsOperationSet dir_deletion_ops(&filesystem_connection_cache);

  for (FileMoveMap::value_type& move: files_to_move_) {
    // Empty destination means delete, so this is a directory. These get deleted in a
    // separate pass to ensure that we have moved all the contents of the directory first.
    if (move.second.empty()) {
      VLOG_ROW << "Deleting file: " << move.first;
      dir_deletion_ops.Add(DELETE, move.first);
    } else {
      VLOG_ROW << "Moving tmp file: " << move.first << " to " << move.second;
      if (FilesystemsMatch(move.first.c_str(), move.second.c_str())) {
        move_ops.Add(RENAME, move.first, move.second);
      } else {
        move_ops.Add(MOVE, move.first, move.second);
      }
    }
  }

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "FileMoveTimer", "FinalizationTimer"));
    if (!move_ops.Execute(exec_env_->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) moving partition files. First error (of "
         << move_ops.errors().size() << ") was: " << move_ops.errors()[0].second;
      return Status(ss.str());
    }
  }

  // 4. Delete temp directories
  {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "FileDeletionTimer",
         "FinalizationTimer"));
    if (!dir_deletion_ops.Execute(exec_env_->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) deleting staging directories. First error (of "
         << dir_deletion_ops.errors().size() << ") was: "
         << dir_deletion_ops.errors()[0].second;
      return Status(ss.str());
    }
  }

  // 5. Optionally update the permissions of the created partition directories
  // Do this last so that we don't make a dir unwritable before we write to it.
  if (FLAGS_insert_inherit_permissions) {
    HdfsOperationSet chmod_ops(&filesystem_connection_cache);
    for (const PermissionCache::value_type& perm: permissions_cache) {
      bool new_dir = perm.second.first;
      if (new_dir) {
        short permissions = perm.second.second;
        VLOG_QUERY << "INSERT created new directory: " << perm.first
                   << ", inherited permissions are: " << oct << permissions;
        chmod_ops.Add(CHMOD, perm.first, permissions);
      }
    }
    if (!chmod_ops.Execute(exec_env_->hdfs_op_thread_pool(), false)) {
      stringstream ss;
      ss << "Error(s) setting permissions on newly created partition directories. First"
         << " error (of " << chmod_ops.errors().size() << ") was: "
         << chmod_ops.errors()[0].second;
      return Status(ss.str());
    }
  }

  return Status::OK();
}

Status Coordinator::FinalizeQuery() {
  // All instances must have reported their final statuses before finalization, which is a
  // post-condition of Wait. If the query was not successful, still try to clean up the
  // staging directory.
  DCHECK(has_called_wait_);
  DCHECK(needs_finalization_);

  VLOG_QUERY << "Finalizing query: " << query_id_;
  SCOPED_TIMER(finalization_timer_);
  Status return_status = GetStatus();
  if (return_status.ok()) {
    return_status = FinalizeSuccessfulInsert();
  }

  stringstream staging_dir;
  DCHECK(finalize_params_.__isset.staging_dir);
  staging_dir << finalize_params_.staging_dir << "/" << PrintId(query_id_,"_") << "/";

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(staging_dir.str(), &hdfs_conn));
  VLOG_QUERY << "Removing staging directory: " << staging_dir.str();
  hdfsDelete(hdfs_conn, staging_dir.str().c_str(), 1);

  return return_status;
}

Status Coordinator::WaitForAllInstances() {
  unique_lock<mutex> l(lock_);
  while (num_remaining_fragment_instances_ > 0 && query_status_.ok()) {
    VLOG_QUERY << "Coordinator waiting for fragment instances to finish, "
               << num_remaining_fragment_instances_ << " remaining";
    instance_completion_cv_.wait(l);
  }
  if (query_status_.ok()) {
    VLOG_QUERY << "All fragment instances finished successfully.";
  } else {
    VLOG_QUERY << "All fragment instances finished due to one or more errors. "
               << query_status_.GetDetail();
  }

  return query_status_;
}

Status Coordinator::Wait() {
  lock_guard<mutex> l(wait_lock_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (has_called_wait_) return Status::OK();
  has_called_wait_ = true;

  if (stmt_type_ == TStmtType::QUERY) {
    DCHECK(executor() != nullptr);
    return UpdateStatus(executor()->WaitForOpen(), runtime_state()->fragment_instance_id(),
        FLAGS_hostname);
  }

  DCHECK_EQ(stmt_type_, TStmtType::DML);
  // Query finalization can only happen when all backends have reported
  // relevant state. They only have relevant state to report in the parallel
  // INSERT case, otherwise all the relevant state is from the coordinator
  // fragment which will be available after Open() returns.
  // Ignore the returned status if finalization is required., since FinalizeQuery() will
  // pick it up and needs to execute regardless.
  Status status = WaitForAllInstances();
  if (!needs_finalization_ && !status.ok()) return status;

  // Query finalization is required only for HDFS table sinks
  if (needs_finalization_) RETURN_IF_ERROR(FinalizeQuery());

  query_profile_->AddInfoString(
      "DML Stats", DataSink::OutputDmlStats(per_partition_status_, "\n"));
  // For DML queries, when Wait is done, the query is complete.  Report aggregate
  // query profiles at this point.
  // TODO: make sure ReportQuerySummary gets called on error
  ReportQuerySummary();

  return status;
}

Status Coordinator::GetNext(QueryResultSet* results, int max_rows, bool* eos) {
  VLOG_ROW << "GetNext() query_id=" << query_id_;
  DCHECK(has_called_wait_);
  SCOPED_TIMER(query_profile_->total_time_counter());

  if (returned_all_results_) {
    // May be called after the first time we set *eos. Re-set *eos and return here;
    // already torn-down coord_sink_ so no more work to do.
    *eos = true;
    return Status::OK();
  }

  DCHECK(coord_sink_ != nullptr)
      << "GetNext() called without result sink. Perhaps Prepare() failed and was not "
      << "checked?";
  Status status = coord_sink_->GetNext(runtime_state(), results, max_rows, eos);

  // if there was an error, we need to return the query's error status rather than
  // the status we just got back from the local executor (which may well be CANCELLED
  // in that case).  Coordinator fragment failed in this case so we log the query_id.
  RETURN_IF_ERROR(
      UpdateStatus(status, runtime_state()->fragment_instance_id(), FLAGS_hostname));

  if (*eos) {
    returned_all_results_ = true;
    // Trigger tear-down of coordinator fragment by closing the consumer. Must do before
    // WaitForAllInstances().
    coord_sink_->CloseConsumer();
    coord_sink_ = nullptr;

    // Don't return final NULL until all instances have completed.  GetNext must wait for
    // all instances to complete before ultimately signalling the end of execution via a
    // NULL batch. After NULL is returned, the coordinator may tear down query state, and
    // perform post-query finalization which might depend on the reports from all
    // instances.
    //
    // TODO: Waiting should happen in TearDown() (and then we wouldn't need to call
    // CloseConsumer() here). See IMPALA-4275 for details.
    RETURN_IF_ERROR(WaitForAllInstances());
    if (query_status_.ok()) {
      // If the query completed successfully, report aggregate query profiles.
      ReportQuerySummary();
    }
  }

  return Status::OK();
}

void Coordinator::PrintFragmentInstanceInfo() {
  for (InstanceState* state: fragment_instance_states_) {
    SummaryStats& acc = fragment_profiles_[state->fragment_idx()].bytes_assigned;
    acc(state->total_split_size());
  }

  for (int id = (executor() == NULL ? 0 : 1); id < fragment_profiles_.size(); ++id) {
    SummaryStats& acc = fragment_profiles_[id].bytes_assigned;
    double min = accumulators::min(acc);
    double max = accumulators::max(acc);
    double mean = accumulators::mean(acc);
    double stddev = sqrt(accumulators::variance(acc));
    stringstream ss;
    ss << " min: " << PrettyPrinter::Print(min, TUnit::BYTES)
      << ", max: " << PrettyPrinter::Print(max, TUnit::BYTES)
      << ", avg: " << PrettyPrinter::Print(mean, TUnit::BYTES)
      << ", stddev: " << PrettyPrinter::Print(stddev, TUnit::BYTES);
    fragment_profiles_[id].averaged_profile->AddInfoString("split sizes", ss.str());

    if (VLOG_FILE_IS_ON) {
      VLOG_FILE << "Byte split for fragment " << id << " " << ss.str();
      for (InstanceState* exec_state: fragment_instance_states_) {
        if (exec_state->fragment_idx() != id) continue;
        VLOG_FILE << "data volume for ipaddress " << exec_state << ": "
                  << PrettyPrinter::Print(exec_state->total_split_size(), TUnit::BYTES);
      }
    }
  }
}

void Coordinator::InitExecSummary() {
  const TQueryExecRequest& request = schedule_.request();
  // init exec_summary_.{nodes, exch_to_sender_map}
  exec_summary_.__isset.nodes = true;
  DCHECK(exec_summary_.nodes.empty());
  for (const TPlanExecInfo& plan_exec_info: request.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      if (!fragment.__isset.plan) continue;

      // eventual index of fragment's root node in exec_summary_.nodes
      int root_node_idx = exec_summary_.nodes.size();

      const TPlan& plan = fragment.plan;
      int num_instances =
          schedule_.GetFragmentExecParams(fragment.idx).instance_exec_params.size();
      for (const TPlanNode& node: plan.nodes) {
        plan_node_id_to_summary_map_[node.node_id] = exec_summary_.nodes.size();
        exec_summary_.nodes.emplace_back();
        TPlanNodeExecSummary& node_summary = exec_summary_.nodes.back();
        node_summary.__set_node_id(node.node_id);
        node_summary.__set_fragment_idx(fragment.idx);
        node_summary.__set_label(node.label);
        node_summary.__set_label_detail(node.label_detail);
        node_summary.__set_num_children(node.num_children);
        if (node.__isset.estimated_stats) {
          node_summary.__set_estimated_stats(node.estimated_stats);
        }
        node_summary.exec_stats.resize(num_instances);
      }

      if (fragment.__isset.output_sink
          && fragment.output_sink.type == TDataSinkType::DATA_STREAM_SINK) {
        const TDataStreamSink& sink = fragment.output_sink.stream_sink;
        int exch_idx = plan_node_id_to_summary_map_[sink.dest_node_id];
        if (sink.output_partition.type == TPartitionType::UNPARTITIONED) {
          exec_summary_.nodes[exch_idx].__set_is_broadcast(true);
        }
        exec_summary_.__isset.exch_to_sender_map = true;
        exec_summary_.exch_to_sender_map[exch_idx] = root_node_idx;
      }
    }
  }
}

void Coordinator::InitExecProfiles() {
  vector<const TPlanFragment*> fragments;
  schedule_.GetTPlanFragments(&fragments);
  fragment_profiles_.resize(fragments.size());

  const TPlanFragment* coord_fragment = schedule_.GetCoordFragment();

  // Initialize the runtime profile structure. This adds the per fragment average
  // profiles followed by the per fragment instance profiles.
  for (const TPlanFragment* fragment: fragments) {
    string profile_name =
        (fragment == coord_fragment) ? "Coordinator Fragment $0" : "Fragment $0";
    PerFragmentProfileData* data = &fragment_profiles_[fragment->idx];
    data->num_instances =
        schedule_.GetFragmentExecParams(fragment->idx).instance_exec_params.size();
    // TODO-MT: stop special-casing the coordinator fragment
    if (fragment != coord_fragment) {
      data->averaged_profile = obj_pool()->Add(new RuntimeProfile(
          obj_pool(), Substitute("Averaged Fragment $0", fragment->display_name), true));
      query_profile_->AddChild(data->averaged_profile, true);
    }
    data->root_profile = obj_pool()->Add(
        new RuntimeProfile(obj_pool(), Substitute(profile_name, fragment->display_name)));
    // Note: we don't start the wall timer here for the fragment profile;
    // it's uninteresting and misleading.
    query_profile_->AddChild(data->root_profile);
  }
}

void Coordinator::CollectScanNodeCounters(RuntimeProfile* profile,
    FragmentInstanceCounters* counters) {
  vector<RuntimeProfile*> children;
  profile->GetAllChildren(&children);
  for (RuntimeProfile* p: children) {
    PlanNodeId id = ExecNode::GetNodeIdFromProfile(p);

    // This profile is not for an exec node.
    if (id == g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID) continue;

    RuntimeProfile::Counter* throughput_counter =
        p->GetCounter(ScanNode::TOTAL_THROUGHPUT_COUNTER);
    if (throughput_counter != NULL) {
      counters->throughput_counters[id] = throughput_counter;
    }
    RuntimeProfile::Counter* scan_ranges_counter =
        p->GetCounter(ScanNode::SCAN_RANGES_COMPLETE_COUNTER);
    if (scan_ranges_counter != NULL) {
      counters->scan_ranges_complete_counters[id] = scan_ranges_counter;
    }
  }
}

void Coordinator::ExecRemoteFInstance(
    const FInstanceExecParams& exec_params, const DebugOptions* debug_options) {
  NotifyBarrierOnExit notifier(exec_complete_barrier_.get());
  TExecPlanFragmentParams rpc_params;
  SetExecPlanFragmentParams(exec_params, &rpc_params);
  if (debug_options != NULL) {
    rpc_params.fragment_instance_ctx.__set_debug_node_id(debug_options->node_id);
    rpc_params.fragment_instance_ctx.__set_debug_action(debug_options->action);
    rpc_params.fragment_instance_ctx.__set_debug_phase(debug_options->phase);
  }
  int instance_state_idx = GetInstanceIdx(exec_params.instance_id);
  InstanceState* exec_state = fragment_instance_states_[instance_state_idx];
  exec_state->ComputeTotalSplitSize(
      rpc_params.fragment_instance_ctx.per_node_scan_ranges);
  VLOG_FILE << "making rpc: ExecPlanFragment"
      << " host=" << exec_state->impalad_address()
      << " instance_id=" << PrintId(exec_state->fragment_instance_id());

  // Guard against concurrent UpdateExecStatus() that may arrive after RPC returns.
  lock_guard<mutex> l(*exec_state->lock());
  int64_t start = MonotonicMillis();

  Status client_connect_status;
  ImpalaBackendConnection backend_client(exec_env_->impalad_client_cache(),
      exec_state->impalad_address(), &client_connect_status);
  if (!client_connect_status.ok()) {
    exec_state->SetInitialStatus(client_connect_status, false);
    return;
  }

  TExecPlanFragmentResult thrift_result;
  Status rpc_status = backend_client.DoRpc(&ImpalaBackendClient::ExecPlanFragment,
      rpc_params, &thrift_result);
  exec_state->set_rpc_latency(MonotonicMillis() - start);

  const string ERR_TEMPLATE = "ExecPlanRequest rpc query_id=$0 instance_id=$1 failed: $2";

  if (!rpc_status.ok()) {
    const string& err_msg = Substitute(ERR_TEMPLATE, PrintId(query_id()),
        PrintId(exec_state->fragment_instance_id()), rpc_status.msg().msg());
    VLOG_QUERY << err_msg;
    exec_state->SetInitialStatus(Status(err_msg), true);
    return;
  }

  Status exec_status = Status(thrift_result.status);
  if (!exec_status.ok()) {
    const string& err_msg = Substitute(ERR_TEMPLATE, PrintId(query_id()),
        PrintId(exec_state->fragment_instance_id()),
        exec_status.msg().GetFullMessageDetails());
    VLOG_QUERY << err_msg;
    exec_state->SetInitialStatus(Status(err_msg), true);
    return;
  }

  exec_state->SetInitialStatus(Status::OK(), true);
  VLOG_FILE << "rpc succeeded: ExecPlanFragment"
      << " instance_id=" << PrintId(exec_state->fragment_instance_id());
}

void Coordinator::Cancel(const Status* cause) {
  lock_guard<mutex> l(lock_);
  // if the query status indicates an error, cancellation has already been initiated
  if (!query_status_.ok()) return;
  // prevent others from cancelling a second time

  // TODO: This should default to OK(), not CANCELLED if there is no cause (or callers
  // should explicitly pass Status::OK()). Fragment instances may be cancelled at the end
  // of a successful query. Need to clean up relationship between query_status_ here and
  // in QueryExecState. See IMPALA-4279.
  query_status_ = (cause != NULL && !cause->ok()) ? *cause : Status::CANCELLED;
  CancelInternal();
}

void Coordinator::CancelInternal() {
  VLOG_QUERY << "Cancel() query_id=" << query_id_;
  CancelFragmentInstances();

  // Report the summary with whatever progress the query made before being cancelled.
  ReportQuerySummary();
}

void Coordinator::CancelFragmentInstances() {
  int num_cancelled = 0;
  for (InstanceState* exec_state: fragment_instance_states_) {
    DCHECK(exec_state != nullptr);

    // lock each exec_state individually to synchronize correctly with
    // UpdateFragmentExecStatus() (which doesn't get the global lock_
    // to set its status)
    lock_guard<mutex> l(*exec_state->lock());

    // Nothing to cancel if the exec rpc was not sent
    if (!exec_state->rpc_sent()) continue;

    // don't cancel if it already finished
    if (exec_state->done()) continue;

    /// If the status is not OK, we still try to cancel - !OK status might mean
    /// communication failure between fragment instance and coordinator, but fragment
    /// instance might still be running.

    // set an error status to make sure we only cancel this once
    exec_state->set_status(Status::CANCELLED);

    // if we get an error while trying to get a connection to the backend,
    // keep going
    Status status;
    ImpalaBackendConnection backend_client(
        exec_env_->impalad_client_cache(), exec_state->impalad_address(), &status);
    if (!status.ok()) continue;
    ++num_cancelled;
    TCancelPlanFragmentParams params;
    params.protocol_version = ImpalaInternalServiceVersion::V1;
    params.__set_fragment_instance_id(exec_state->fragment_instance_id());
    TCancelPlanFragmentResult res;
    VLOG_QUERY << "sending CancelPlanFragment rpc for instance_id="
               << exec_state->fragment_instance_id() << " backend="
               << exec_state->impalad_address();
    Status rpc_status;
    // Try to send the RPC 3 times before failing.
    bool retry_is_safe;
    for (int i = 0; i < 3; ++i) {
      rpc_status = backend_client.DoRpc(&ImpalaBackendClient::CancelPlanFragment,
          params, &res, &retry_is_safe);
      if (rpc_status.ok() || !retry_is_safe) break;
    }
    if (!rpc_status.ok()) {
      exec_state->status()->MergeStatus(rpc_status);
      stringstream msg;
      msg << "CancelPlanFragment rpc query_id=" << query_id_
          << " instance_id=" << exec_state->fragment_instance_id()
          << " failed: " << rpc_status.msg().msg();
      // make a note of the error status, but keep on cancelling the other fragments
      exec_state->status()->AddDetail(msg.str());
      continue;
    }
    if (res.status.status_code != TErrorCode::OK) {
      exec_state->status()->AddDetail(join(res.status.error_msgs, "; "));
    }
  }
  VLOG_QUERY << Substitute(
      "CancelFragmentInstances() query_id=$0, tried to cancel $1 fragment instances",
      PrintId(query_id_), num_cancelled);

  // Notify that we completed with an error.
  instance_completion_cv_.notify_all();
}

Status Coordinator::UpdateFragmentExecStatus(const TReportExecStatusParams& params) {
  VLOG_FILE << "UpdateFragmentExecStatus() "
            << " instance=" << PrintId(params.fragment_instance_id)
            << " status=" << params.status.status_code
            << " done=" << (params.done ? "true" : "false");
  int instance_state_idx = GetInstanceIdx(params.fragment_instance_id);
  if (instance_state_idx >= fragment_instance_states_.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Unknown fragment instance index $0 (max known: $1)",
            instance_state_idx, fragment_instance_states_.size() - 1));
  }
  InstanceState* exec_state = fragment_instance_states_[instance_state_idx];

  const TRuntimeProfileTree& cumulative_profile = params.profile;
  Status status(params.status);
  {
    lock_guard<mutex> l(*exec_state->lock());
    if (!status.ok()) {
      // During query cancellation, exec_state is set to CANCELLED. However, we might
      // process a non-error message from a fragment executor that is sent
      // before query cancellation is invoked. Make sure we don't go from error status to
      // OK.
      exec_state->set_status(status);
    }
    exec_state->set_done(params.done);
    if (exec_state->status()->ok()) {
      // We can't update this backend's profile if ReportQuerySummary() is running,
      // because it depends on all profiles not changing during its execution (when it
      // calls SortChildren()). ReportQuerySummary() only gets called after
      // WaitForAllInstances() returns or at the end of CancelFragmentInstances().
      // WaitForAllInstances() only returns after all backends have completed (in which
      // case we wouldn't be in this function), or when there's an error, in which case
      // CancelFragmentInstances() is called. CancelFragmentInstances sets all
      // exec_state's statuses to cancelled.
      // TODO: We're losing this profile information. Call ReportQuerySummary only after
      // all backends have completed.
      exec_state->profile()->Update(cumulative_profile);

      // Update the average profile for the fragment corresponding to this instance.
      exec_state->profile()->ComputeTimeInProfile();
      UpdateAverageProfile(exec_state);
      UpdateExecSummary(*exec_state);
    }
    if (!exec_state->SetProfileCreated()) {
      CollectScanNodeCounters(exec_state->profile(), exec_state->aggregate_counters());
    }

    // Log messages aggregated by type
    if (params.__isset.error_log && params.error_log.size() > 0) {
      // Append the log messages from each update with the global state of the query
      // execution
      MergeErrorMaps(exec_state->error_log(), params.error_log);
      VLOG_FILE << "instance_id=" << exec_state->fragment_instance_id()
                << " error log: " << PrintErrorMapToString(*exec_state->error_log());
    }
    progress_.Update(exec_state->UpdateNumScanRangesCompleted());
  }

  if (params.done && params.__isset.insert_exec_status) {
    lock_guard<mutex> l(lock_);
    // Merge in table update data (partitions written to, files to be moved as part of
    // finalization)
    for (const PartitionStatusMap::value_type& partition:
         params.insert_exec_status.per_partition_status) {
      TInsertPartitionStatus* status = &(per_partition_status_[partition.first]);
      status->__set_num_modified_rows(
          status->num_modified_rows + partition.second.num_modified_rows);
      status->__set_kudu_latest_observed_ts(std::max(
          partition.second.kudu_latest_observed_ts, status->kudu_latest_observed_ts));
      status->__set_id(partition.second.id);
      status->__set_partition_base_dir(partition.second.partition_base_dir);

      if (partition.second.__isset.stats) {
        if (!status->__isset.stats) status->__set_stats(TInsertStats());
        DataSink::MergeDmlStats(partition.second.stats, &status->stats);
      }
    }
    files_to_move_.insert(
        params.insert_exec_status.files_to_move.begin(),
        params.insert_exec_status.files_to_move.end());
  }

  if (VLOG_FILE_IS_ON) {
    stringstream s;
    exec_state->profile()->PrettyPrint(&s);
    VLOG_FILE << "profile for instance_id=" << exec_state->fragment_instance_id()
              << "\n" << s.str();
  }
  // also print the cumulative profile
  // TODO: fix the coordinator/PlanFragmentExecutor, so this isn't needed
  if (VLOG_FILE_IS_ON) {
    stringstream s;
    query_profile_->PrettyPrint(&s);
    VLOG_FILE << "cumulative profile for query_id=" << query_id_
              << "\n" << s.str();
  }

  // for now, abort the query if we see any error except if the error is cancelled
  // and returned_all_results_ is true.
  // (UpdateStatus() initiates cancellation, if it hasn't already been)
  if (!(returned_all_results_ && status.IsCancelled()) && !status.ok()) {
    UpdateStatus(status, exec_state->fragment_instance_id(),
        TNetworkAddressToString(exec_state->impalad_address()));
    return Status::OK();
  }

  if (params.done) {
    lock_guard<mutex> l(lock_);
    exec_state->stopwatch()->Stop();
    DCHECK_GT(num_remaining_fragment_instances_, 0);
    VLOG_QUERY << "Fragment instance completed:"
        << " id=" << PrintId(exec_state->fragment_instance_id())
        << " host=" << exec_state->impalad_address()
        << " remaining=" << num_remaining_fragment_instances_ - 1;
    if (VLOG_QUERY_IS_ON && num_remaining_fragment_instances_ > 1) {
      // print host/port info for the first backend that's still in progress as a
      // debugging aid for backend deadlocks
      for (InstanceState* exec_state: fragment_instance_states_) {
        lock_guard<mutex> l2(*exec_state->lock());
        if (!exec_state->done()) {
          VLOG_QUERY << "query_id=" << query_id_ << ": first in-progress backend: "
                     << exec_state->impalad_address();
          break;
        }
      }
    }
    if (--num_remaining_fragment_instances_ == 0) {
      instance_completion_cv_.notify_all();
    }
  }

  return Status::OK();
}

uint64_t Coordinator::GetLatestKuduInsertTimestamp() const {
  uint64_t max_ts = 0;
  for (const auto& entry : per_partition_status_) {
    max_ts = std::max(max_ts,
        static_cast<uint64_t>(entry.second.kudu_latest_observed_ts));
  }
  return max_ts;
}

RuntimeState* Coordinator::runtime_state() {
  return executor() == NULL ? NULL : executor()->runtime_state();
}

bool Coordinator::PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update) {
  // Assume we are called only after all fragments have completed
  DCHECK(has_called_wait_);

  for (const PartitionStatusMap::value_type& partition: per_partition_status_) {
    catalog_update->created_partitions.insert(partition.first);
  }

  return catalog_update->created_partitions.size() != 0;
}

// Comparator to order RuntimeProfiles by descending total time
typedef struct {
  typedef pair<RuntimeProfile*, bool> Profile;
  bool operator()(const Profile& a, const Profile& b) const {
    // Reverse ordering: we want the longest first
    return
        a.first->total_time_counter()->value() > b.first->total_time_counter()->value();
  }
} InstanceComparator;

void Coordinator::UpdateAverageProfile(InstanceState* instance_state) {
  FragmentIdx fragment_idx = instance_state->fragment_idx();
  DCHECK_GE(fragment_idx, 0);
  DCHECK_LT(fragment_idx, fragment_profiles_.size());
  PerFragmentProfileData* data = &fragment_profiles_[fragment_idx];

  // No locks are taken since UpdateAverage() and AddChild() take their own locks
  if (data->averaged_profile != nullptr) {
    data->averaged_profile->UpdateAverage(instance_state->profile());
  }
  data->root_profile->AddChild(instance_state->profile());
}

void Coordinator::ComputeFragmentSummaryStats(InstanceState* instance_state) {
  FragmentIdx fragment_idx = instance_state->fragment_idx();
  DCHECK_GE(fragment_idx, 0);
  DCHECK_LT(fragment_idx, fragment_profiles_.size());
  PerFragmentProfileData* data = &fragment_profiles_[fragment_idx];

  int64_t completion_time = instance_state->stopwatch()->ElapsedTime();
  data->completion_times(completion_time);
  data->rates(instance_state->total_split_size()
      / (completion_time / 1000.0 / 1000.0 / 1000.0));

  // Add the child in case it has not been added previously
  // via UpdateAverageProfile(). AddChild() will do nothing if the child
  // already exists.
  data->root_profile->AddChild(instance_state->profile());
}

void Coordinator::UpdateExecSummary(const InstanceState& instance_state) {
  vector<RuntimeProfile*> children;
  instance_state.profile()->GetAllChildren(&children);

  lock_guard<SpinLock> l(exec_summary_lock_);
  for (int i = 0; i < children.size(); ++i) {
    int node_id = ExecNode::GetNodeIdFromProfile(children[i]);
    if (node_id == -1) continue;

    TPlanNodeExecSummary& exec_summary =
        exec_summary_.nodes[plan_node_id_to_summary_map_[node_id]];
    DCHECK_LT(instance_state.per_fragment_instance_idx(), exec_summary.exec_stats.size());
    DCHECK_EQ(fragment_profiles_[instance_state.fragment_idx()].num_instances,
        exec_summary.exec_stats.size());
    TExecStats& stats =
        exec_summary.exec_stats[instance_state.per_fragment_instance_idx()];

    RuntimeProfile::Counter* rows_counter = children[i]->GetCounter("RowsReturned");
    RuntimeProfile::Counter* mem_counter = children[i]->GetCounter("PeakMemoryUsage");
    if (rows_counter != NULL) stats.__set_cardinality(rows_counter->value());
    if (mem_counter != NULL) stats.__set_memory_used(mem_counter->value());
    stats.__set_latency_ns(children[i]->local_time());
    // TODO: we don't track cpu time per node now. Do that.
    exec_summary.__isset.exec_stats = true;
  }
  VLOG(2) << PrintExecSummary(exec_summary_);
}

// This function appends summary information to the query_profile_ before
// outputting it to VLOG.  It adds:
//   1. Averaged fragment instance profiles (TODO: add outliers)
//   2. Summary of fragment instance durations (min, max, mean, stddev)
//   3. Summary of fragment instance rates (min, max, mean, stddev)
// TODO: add histogram/percentile
void Coordinator::ReportQuerySummary() {
  // In this case, the query did not even get to start all fragment instances.
  // Some of the state that is used below might be uninitialized.  In this case,
  // the query has made so little progress, reporting a summary is not very useful.
  if (!has_called_wait_) return;

  if (!fragment_instance_states_.empty()) {
    // Average all fragment instances for each fragment.
    for (InstanceState* state: fragment_instance_states_) {
      state->profile()->ComputeTimeInProfile();
      UpdateAverageProfile(state);
      // Skip coordinator fragment, if one exists.
      // TODO: Can we remove the special casing here?
      if (coord_instance_ == nullptr || state->fragment_idx() != 0) {
        ComputeFragmentSummaryStats(state);
      }
      UpdateExecSummary(*state);
    }

    InstanceComparator comparator;
    // Per fragment instances have been collected, output summaries
    for (int i = (executor() != NULL ? 1 : 0); i < fragment_profiles_.size(); ++i) {
      fragment_profiles_[i].root_profile->SortChildren(comparator);
      SummaryStats& completion_times = fragment_profiles_[i].completion_times;
      SummaryStats& rates = fragment_profiles_[i].rates;

      stringstream times_label;
      times_label
        << "min:" << PrettyPrinter::Print(
            accumulators::min(completion_times), TUnit::TIME_NS)
        << "  max:" << PrettyPrinter::Print(
            accumulators::max(completion_times), TUnit::TIME_NS)
        << "  mean: " << PrettyPrinter::Print(
            accumulators::mean(completion_times), TUnit::TIME_NS)
        << "  stddev:" << PrettyPrinter::Print(
            sqrt(accumulators::variance(completion_times)), TUnit::TIME_NS);

      stringstream rates_label;
      rates_label
        << "min:" << PrettyPrinter::Print(
            accumulators::min(rates), TUnit::BYTES_PER_SECOND)
        << "  max:" << PrettyPrinter::Print(
            accumulators::max(rates), TUnit::BYTES_PER_SECOND)
        << "  mean:" << PrettyPrinter::Print(
            accumulators::mean(rates), TUnit::BYTES_PER_SECOND)
        << "  stddev:" << PrettyPrinter::Print(
            sqrt(accumulators::variance(rates)), TUnit::BYTES_PER_SECOND);

      fragment_profiles_[i].averaged_profile->AddInfoString(
          "completion times", times_label.str());
      fragment_profiles_[i].averaged_profile->AddInfoString(
          "execution rates", rates_label.str());
      fragment_profiles_[i].averaged_profile->AddInfoString(
          "num instances", lexical_cast<string>(fragment_profiles_[i].num_instances));
    }

    // Add per node peak memory usage as InfoString
    // Map from Impalad address to peak memory usage of this query
    typedef unordered_map<TNetworkAddress, int64_t> PerNodePeakMemoryUsage;
    PerNodePeakMemoryUsage per_node_peak_mem_usage;
    for (InstanceState* state: fragment_instance_states_) {
      int64_t initial_usage = 0;
      int64_t* mem_usage = FindOrInsert(&per_node_peak_mem_usage,
          state->impalad_address(), initial_usage);
      RuntimeProfile::Counter* mem_usage_counter =
          state->profile()->GetCounter(PlanFragmentExecutor::PER_HOST_PEAK_MEM_COUNTER);
      if (mem_usage_counter != NULL && mem_usage_counter->value() > *mem_usage) {
        per_node_peak_mem_usage[state->impalad_address()] = mem_usage_counter->value();
      }
    }
    stringstream info;
    for (PerNodePeakMemoryUsage::value_type entry: per_node_peak_mem_usage) {
      info << entry.first << "("
           << PrettyPrinter::Print(entry.second, TUnit::BYTES) << ") ";
    }
    query_profile_->AddInfoString("Per Node Peak Memory Usage", info.str());
  }
}

string Coordinator::GetErrorLog() {
  ErrorLogMap merged;
  for (InstanceState* state: fragment_instance_states_) {
    lock_guard<mutex> l(*state->lock());
    if (state->error_log()->size() > 0)  MergeErrorMaps(&merged, *state->error_log());
  }
  return PrintErrorMapToString(merged);
}

void Coordinator::SetExecPlanFragmentParams(
    const FInstanceExecParams& params, TExecPlanFragmentParams* rpc_params) {
  rpc_params->__set_protocol_version(ImpalaInternalServiceVersion::V1);
  rpc_params->__set_query_ctx(query_ctx_);

  TPlanFragmentCtx fragment_ctx;
  TPlanFragmentInstanceCtx fragment_instance_ctx;

  fragment_ctx.__set_fragment(params.fragment());
  SetExecPlanDescriptorTable(params.fragment(), rpc_params);

  // Remove filters that weren't selected during filter routing table construction.
  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    DCHECK(schedule_.request().query_ctx.client_request.query_options.mt_dop == 0);
    int instance_idx = GetInstanceIdx(params.instance_id);
    for (TPlanNode& plan_node: rpc_params->fragment_ctx.fragment.plan.nodes) {
      if (plan_node.__isset.runtime_filters) {
        vector<TRuntimeFilterDesc> required_filters;
        for (const TRuntimeFilterDesc& desc: plan_node.runtime_filters) {
          FilterRoutingTable::iterator filter_it =
              filter_routing_table_.find(desc.filter_id);
          if (filter_it == filter_routing_table_.end()) continue;
          const FilterState& f = filter_it->second;
          if (plan_node.__isset.hash_join_node) {
            if (f.src_fragment_instance_state_idxs().find(instance_idx) ==
                f.src_fragment_instance_state_idxs().end()) {
              DCHECK(desc.is_broadcast_join);
              continue;
            }
          }
          // We don't need a target-side check here, because a filter is either sent to
          // all its targets or none, and the none case is handled by checking if the
          // filter is in the routing table.
          required_filters.push_back(desc);
        }
        plan_node.__set_runtime_filters(required_filters);
      }
    }
  }

  fragment_instance_ctx.__set_request_pool(schedule_.request_pool());
  fragment_instance_ctx.__set_per_node_scan_ranges(params.per_node_scan_ranges);
  fragment_instance_ctx.__set_per_exch_num_senders(
      params.fragment_exec_params.per_exch_num_senders);
  fragment_instance_ctx.__set_destinations(
      params.fragment_exec_params.destinations);
  fragment_instance_ctx.__set_sender_id(params.sender_id);
  fragment_instance_ctx.fragment_instance_id = params.instance_id;
  fragment_instance_ctx.per_fragment_instance_idx = params.per_fragment_instance_idx;
  rpc_params->__set_fragment_ctx(fragment_ctx);
  rpc_params->__set_fragment_instance_ctx(fragment_instance_ctx);
}

void Coordinator::SetExecPlanDescriptorTable(const TPlanFragment& fragment,
    TExecPlanFragmentParams* rpc_params) {
  DCHECK(rpc_params->__isset.query_ctx);
  TDescriptorTable thrift_desc_tbl;

  // Always add the Tuple and Slot descriptors.
  thrift_desc_tbl.__set_tupleDescriptors(desc_tbl_.tupleDescriptors);
  thrift_desc_tbl.__set_slotDescriptors(desc_tbl_.slotDescriptors);

  // Collect the TTupleId(s) for ScanNode(s).
  unordered_set<TTupleId> tuple_ids;
  for (const TPlanNode& plan_node: fragment.plan.nodes) {
    switch (plan_node.node_type) {
      case TPlanNodeType::HDFS_SCAN_NODE:
        tuple_ids.insert(plan_node.hdfs_scan_node.tuple_id);
        break;
      case TPlanNodeType::KUDU_SCAN_NODE:
        tuple_ids.insert(plan_node.kudu_scan_node.tuple_id);
        break;
      case TPlanNodeType::HBASE_SCAN_NODE:
        tuple_ids.insert(plan_node.hbase_scan_node.tuple_id);
        break;
      case TPlanNodeType::DATA_SOURCE_NODE:
        tuple_ids.insert(plan_node.data_source_node.tuple_id);
        break;
      case TPlanNodeType::HASH_JOIN_NODE:
      case TPlanNodeType::AGGREGATION_NODE:
      case TPlanNodeType::SORT_NODE:
      case TPlanNodeType::EMPTY_SET_NODE:
      case TPlanNodeType::EXCHANGE_NODE:
      case TPlanNodeType::UNION_NODE:
      case TPlanNodeType::SELECT_NODE:
      case TPlanNodeType::NESTED_LOOP_JOIN_NODE:
      case TPlanNodeType::ANALYTIC_EVAL_NODE:
      case TPlanNodeType::SINGULAR_ROW_SRC_NODE:
      case TPlanNodeType::UNNEST_NODE:
      case TPlanNodeType::SUBPLAN_NODE:
        // Do nothing
        break;
      default:
        DCHECK(false) << "Invalid node type: " << plan_node.node_type;
    }
  }

  // Collect TTableId(s) matching the TTupleId(s).
  unordered_set<TTableId> table_ids;
  for (const TTupleId& tuple_id: tuple_ids) {
    for (const TTupleDescriptor& tuple_desc: desc_tbl_.tupleDescriptors) {
      if (tuple_desc.__isset.tableId &&  tuple_id == tuple_desc.id) {
        table_ids.insert(tuple_desc.tableId);
      }
    }
  }

  // Collect the tableId for the table sink.
  if (fragment.__isset.output_sink && fragment.output_sink.__isset.table_sink
      && fragment.output_sink.type == TDataSinkType::TABLE_SINK) {
    table_ids.insert(fragment.output_sink.table_sink.target_table_id);
  }

  // Iterate over all TTableDescriptor(s) and add the ones that are needed.
  for (const TTableDescriptor& table_desc: desc_tbl_.tableDescriptors) {
    if (table_ids.find(table_desc.id) == table_ids.end()) continue;
    thrift_desc_tbl.tableDescriptors.push_back(table_desc);
    thrift_desc_tbl.__isset.tableDescriptors = true;
  }

  rpc_params->query_ctx.__set_desc_tbl(thrift_desc_tbl);
}

namespace {

// Make a PublishFilter rpc to 'impalad' for given fragment_instance_id
// and params.
// This takes by-value parameters because we cannot guarantee that the originating
// coordinator won't be destroyed while this executes.
// TODO: switch to references when we fix the lifecycle problems of coordinators.
void DistributeFilters(shared_ptr<TPublishFilterParams> params,
    TNetworkAddress impalad, TUniqueId fragment_instance_id) {
  Status status;
  ImpalaBackendConnection backend_client(
      ExecEnv::GetInstance()->impalad_client_cache(), impalad, &status);
  if (!status.ok()) return;
  // Make a local copy of the shared 'master' set of parameters
  TPublishFilterParams local_params(*params);
  local_params.dst_instance_id = fragment_instance_id;
  local_params.__set_bloom_filter(params->bloom_filter);
  TPublishFilterResult res;
  backend_client.DoRpc(&ImpalaBackendClient::PublishFilter, local_params, &res);
};

}

// TODO: call this as soon as it's clear that we won't reference the state
// anymore, ie, in CancelInternal() and when GetNext() hits eos
void Coordinator::TearDown() {
  DCHECK(!torn_down_) << "Coordinator::TearDown() may not be called twice";
  torn_down_ = true;
  if (filter_routing_table_.size() > 0) {
    query_profile_->AddInfoString("Final filter table", FilterDebugString());
  }

  {
    lock_guard<SpinLock> l(filter_lock_);
    for (auto& filter : filter_routing_table_) {
      FilterState* state = &filter.second;
      state->Disable(filter_mem_tracker_.get());
    }
  }
  // This may be NULL while executing UDFs.
  if (filter_mem_tracker_.get() != nullptr) {
    filter_mem_tracker_->UnregisterFromParent();
    filter_mem_tracker_.reset();
  }
  // Need to protect against failed Prepare(), where root_sink() would not be set.
  if (coord_sink_ != nullptr) {
    coord_sink_->CloseConsumer();
    coord_sink_ = nullptr;
  }
  coord_instance_ = nullptr;
  if (query_state_ != nullptr) {
    // Tear down the query state last - other members like 'filter_mem_tracker_'
    // may reference objects with query lifetime, like the query MemTracker.
    ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
    query_state_ = nullptr;
  }
}

void Coordinator::UpdateFilter(const TUpdateFilterParams& params) {
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "UpdateFilter() called although runtime filters are disabled";
  DCHECK(exec_complete_barrier_.get() != NULL)
      << "Filters received before fragments started!";
  exec_complete_barrier_->Wait();
  DCHECK(filter_routing_table_complete_)
      << "Filter received before routing table complete";

  // Make a 'master' copy that will be shared by all concurrent delivery RPC attempts.
  shared_ptr<TPublishFilterParams> rpc_params(new TPublishFilterParams());
  unordered_set<int> target_fragment_instance_state_idxs;
  {
    lock_guard<SpinLock> l(filter_lock_);
    FilterRoutingTable::iterator it = filter_routing_table_.find(params.filter_id);
    if (it == filter_routing_table_.end()) {
      LOG(INFO) << "Could not find filter with id: " << params.filter_id;
      return;
    }
    FilterState* state = &it->second;

    DCHECK(state->desc().has_remote_targets)
          << "Coordinator received filter that has only local targets";

    // Check if the filter has already been sent, which could happen in three cases:
    //   * if one local filter had always_true set - no point waiting for other local
    //     filters that can't affect the aggregated global filter
    //   * if this is a broadcast join, and another local filter was already received
    //   * if the filter could not be allocated and so an always_true filter was sent
    //     immediately.
    if (state->disabled()) return;

    if (filter_updates_received_->value() == 0) {
      query_events_->MarkEvent("First dynamic filter received");
    }
    filter_updates_received_->Add(1);

    state->ApplyUpdate(params, this);

    if (state->pending_count() > 0 && !state->disabled()) return;
    // At this point, we either disabled this filter or aggregation is complete.
    DCHECK(state->disabled() || state->pending_count() == 0);

    // No more updates are pending on this filter ID. Create a distribution payload and
    // offer it to the queue.
    for (const FilterTarget& target: *state->targets()) {
      // Don't publish the filter to targets that are in the same fragment as the join
      // that produced it.
      if (target.is_local) continue;
      target_fragment_instance_state_idxs.insert(
          target.fragment_instance_state_idxs.begin(),
          target.fragment_instance_state_idxs.end());
    }

    // Assign outgoing bloom filter.
    if (state->bloom_filter() != NULL) {
      // Complete filter case.
      // TODO: Replace with move() in Thrift 0.9.3.
      TBloomFilter* aggregated_filter = state->bloom_filter();
      filter_mem_tracker_->Release(aggregated_filter->directory.size());
      swap(rpc_params->bloom_filter, *aggregated_filter);
      DCHECK_EQ(aggregated_filter->directory.size(), 0);
    } else {
      // Disabled filter case (due to OOM or due to receiving an always_true filter).
      rpc_params->bloom_filter.always_true = true;
    }

    // Filter is complete, and can be released.
    state->Disable(filter_mem_tracker_.get());
    DCHECK_EQ(state->bloom_filter(), reinterpret_cast<TBloomFilter*>(NULL));
  }

  rpc_params->filter_id = params.filter_id;

  for (int target_idx: target_fragment_instance_state_idxs) {
    InstanceState* fragment_inst = fragment_instance_states_[target_idx];
    DCHECK(fragment_inst != NULL) << "Missing fragment instance: " << target_idx;
    exec_env_->rpc_pool()->Offer(bind<void>(DistributeFilters, rpc_params,
        fragment_inst->impalad_address(), fragment_inst->fragment_instance_id()));
        // TODO: switch back to the following once we fixed the lifecycle
        // problems of Coordinator
        //std::cref(fragment_inst->impalad_address()),
        //std::cref(fragment_inst->fragment_instance_id())));
  }
}


void Coordinator::FilterState::ApplyUpdate(const TUpdateFilterParams& params,
    Coordinator* coord) {
  DCHECK_GT(pending_count_, 0);
  DCHECK_EQ(completion_time_, 0L);
  if (first_arrival_time_ == 0L) {
    first_arrival_time_ = coord->query_events_->ElapsedTime();
  }

  --pending_count_;
  if (params.bloom_filter.always_true) {
    Disable(coord->filter_mem_tracker_.get());
  } else if (bloom_filter_.get() == NULL) {
    int64_t heap_space = params.bloom_filter.directory.size();
    if (!coord->filter_mem_tracker_.get()->TryConsume(heap_space)) {
      VLOG_QUERY << "Not enough memory to allocate filter: "
                 << PrettyPrinter::Print(heap_space, TUnit::BYTES)
                 << " (query: " << PrintId(coord->query_id()) << ")";
      // Disable, as one missing update means a correct filter cannot be produced.
      Disable(coord->filter_mem_tracker_.get());
    } else {
      bloom_filter_.reset(new TBloomFilter());
      // Workaround for fact that parameters are const& for Thrift RPCs - yet we want to
      // move the payload from the request rather than copy it and take double the memory
      // cost. After this point, params.bloom_filter is an empty filter and should not be
      // read.
      TBloomFilter* non_const_filter =
          &const_cast<TBloomFilter&>(params.bloom_filter);
      swap(*bloom_filter_.get(), *non_const_filter);
      DCHECK_EQ(non_const_filter->directory.size(), 0);
    }
  } else {
    BloomFilter::Or(params.bloom_filter, bloom_filter_.get());
  }

  if (pending_count_ == 0 || disabled_) {
    completion_time_ = coord->query_events_->ElapsedTime();
  }
}

void Coordinator::FilterState::Disable(MemTracker* tracker) {
  disabled_ = true;
  if (bloom_filter_.get() == NULL) return;
  int64_t heap_space = bloom_filter_.get()->directory.size();
  tracker->Release(heap_space);
  bloom_filter_.reset();
}

}
