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

#include "runtime/coordinator.h"

#include <limits>
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
#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/unordered_set.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>
#include <errno.h>

#include "common/logging.h"
#include "exprs/expr.h"
#include "exec/data-sink.h"
#include "runtime/client-cache.h"
#include "runtime/data-stream-sender.h"
#include "runtime/data-stream-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/plan-fragment-executor.h"
#include "runtime/row-batch.h"
#include "runtime/parallel-executor.h"
#include "scheduling/scheduler.h"
#include "exec/data-sink.h"
#include "exec/scan-node.h"
#include "util/bloom-filter.h"
#include "util/container-util.h"
#include "util/counting-barrier.h"
#include "util/debug-util.h"
#include "util/error-util.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/llama-util.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/summary-util.h"
#include "util/table-printer.h"
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Frontend_types.h"
#include "gen-cpp/PlanNodes_types.h"
#include "gen-cpp/Partitions_types.h"
#include "gen-cpp/ImpalaInternalService_constants.h"

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

// container for debug options in TPlanFragmentExecParams (debug_node, debug_action,
// debug_phase)
struct DebugOptions {
  int fragment_instance_idx;
  int node_id;
  TDebugAction::type action;
  TExecNodePhase::type phase;  // INVALID: debug options invalid

  DebugOptions()
    : fragment_instance_idx(-1), node_id(-1), action(TDebugAction::WAIT),
      phase(TExecNodePhase::INVALID) {}

  // If these debug options apply to the candidate fragment instance, returns true
  // otherwise returns false.
  bool IsApplicable(int candidate_fragment_instance_idx) {
    if (phase == TExecNodePhase::INVALID) return false;
    return (fragment_instance_idx == -1 ||
        fragment_instance_idx == candidate_fragment_instance_idx);
  }
};

/// Execution state of a particular fragment instance.
///
/// Concurrent accesses:
/// - GetNodeThroughput() called when coordinator's profile is printed
/// - updates through UpdateFragmentExecStatus()
class Coordinator::FragmentInstanceState {
 public:
  FragmentInstanceState(int fragment_idx, const FragmentExecParams* params,
      int instance_idx, ObjectPool* obj_pool)
    : fragment_instance_id_(params->instance_ids[instance_idx]),
      impalad_address_(params->hosts[instance_idx]),
      total_split_size_(0),
      fragment_idx_(fragment_idx),
      instance_idx_(instance_idx),
      rpc_sent_(false),
      done_(false),
      profile_created_(false),
      total_ranges_complete_(0),
      rpc_latency_(0) {
    const string& profile_name = Substitute("Instance $0 (host=$1)",
        PrintId(fragment_instance_id_), lexical_cast<string>(impalad_address_));
    profile_ = obj_pool->Add(new RuntimeProfile(obj_pool, profile_name));
  }

  /// Called to set the initial status of the fragment instance after the
  /// ExecRemoteFragment() RPC has returned.
  void SetInitialStatus(const Status& status) {
    DCHECK(!rpc_sent_);
    status_ = status;
    if (!status_.ok()) return;
    rpc_sent_ = true;
    stopwatch_.Start();
  }

  /// Computes sum of split sizes of leftmost scan.
  void ComputeTotalSplitSize(const PerNodeScanRanges& per_node_scan_ranges);

  /// Return value of throughput counter for given plan_node_id, or 0 if that node doesn't
  /// exist. Thread-safe, and takes lock() internally.
  int64_t GetNodeThroughput(int plan_node_id);

  /// Return number of completed scan ranges for plan_node_id, or 0 if that node doesn't
  /// exist. Thread-safe, and takes lock() internally.
  int64_t GetNumScanRangesCompleted(int plan_node_id);

  /// Updates the total number of scan ranges complete for this fragment. Returns the
  /// delta since the last time this was called. Not thread-safe without lock() being
  /// acquired by the caller.
  int64_t UpdateNumScanRangesCompleted();

  // The following getters do not require lock() to be held.
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  MonotonicStopWatch* stopwatch() { return &stopwatch_; }
  const TNetworkAddress& impalad_address() const { return impalad_address_; }
  int64_t total_split_size() const { return total_split_size_; }
  bool done() const { return done_; }
  int fragment_idx() const { return fragment_idx_; }
  int instance_idx() const { return instance_idx_; }
  bool rpc_sent() const { return rpc_sent_; }
  int64_t rpc_latency() const { return rpc_latency_; }

  // Getters below must be accessed with lock() held
  RuntimeProfile* profile() { return profile_; }
  FragmentInstanceCounters* aggregate_counters() { return &aggregate_counters_; }
  ErrorLogMap* error_log() { return &error_log_; }
  Status* status() { return &status_; }

  mutex* lock() { return &lock_; }

  void SetStatus(const Status& status) { status_ = status; }
  void SetDone(bool done) { done_ = done; }

  /// Registers that the fragment instance's profile has been created and initially
  /// populated. Returns whether the profile had already been initialised so that callers
  /// can tell if they are the first to do so. Not thread-safe.
  bool SetProfileCreated() {
    bool cur = profile_created_;
    profile_created_ = true;
    return cur;
  }

  void SetRpcLatency(int64_t millis) {
    DCHECK_EQ(rpc_latency_, 0);
    rpc_latency_ = millis;
  }

 private:
  /// The unique ID of this instance of this fragment (there may be many instance of the
  /// same fragment, but this ID uniquely identifies this FragmentInstanceState).
  TUniqueId fragment_instance_id_;

  /// Wall clock timer for this fragment.
  MonotonicStopWatch stopwatch_;

  /// Address of ImpalaInternalService this fragment is running on.
  const TNetworkAddress impalad_address_;

  /// Summed across all splits; in bytes.
  int64_t total_split_size_;

  /// Fragment idx for this ExecState
  int fragment_idx_;

  /// The 0-based instance idx.
  int instance_idx_;

  /// Protects fields below. Can be held while doing an RPC, so SpinLock is a bad idea.
  /// lock ordering: Coordinator::lock_ must only be obtained *prior* to lock_
  mutex lock_;

  /// If the status indicates an error status, execution of this fragment has either been
  /// aborted by the remote impalad (which then reported the error) or cancellation has
  /// been initiated; either way, execution must not be cancelled
  Status status_;

  /// If true, ExecPlanFragment() rpc has been sent.
  bool rpc_sent_;

  /// If true, execution terminated; do not cancel in that case.
  bool done_;

  /// True after the first call to profile->Update()
  bool profile_created_;

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
};

void Coordinator::FragmentInstanceState::ComputeTotalSplitSize(
    const PerNodeScanRanges& per_node_scan_ranges) {
  total_split_size_ = 0;

  BOOST_FOREACH(const PerNodeScanRanges::value_type& entry, per_node_scan_ranges) {
    BOOST_FOREACH(const TScanRangeParams& scan_range_params, entry.second) {
      if (!scan_range_params.scan_range.__isset.hdfs_file_split) continue;
      total_split_size_ += scan_range_params.scan_range.hdfs_file_split.length;
    }
  }
}

int64_t Coordinator::FragmentInstanceState::GetNodeThroughput(int plan_node_id) {
  RuntimeProfile::Counter* counter = NULL;
  {
    lock_guard<mutex> l(lock_);
    CounterMap& throughput_counters = aggregate_counters_.throughput_counters;
    CounterMap::iterator i = throughput_counters.find(plan_node_id);
    if (i == throughput_counters.end()) return 0;
    counter = i->second;
  }
  DCHECK(counter != NULL);
  // make sure not to hold lock when calling value() to avoid potential deadlocks
  return counter->value();
}

int64_t Coordinator::FragmentInstanceState::GetNumScanRangesCompleted(int plan_node_id) {
  RuntimeProfile::Counter* counter = NULL;
  {
    lock_guard<mutex> l(lock_);
    CounterMap& ranges_complete = aggregate_counters_.scan_ranges_complete_counters;
    CounterMap::iterator i = ranges_complete.find(plan_node_id);
    if (i == ranges_complete.end()) return 0;
    counter = i->second;
  }
  DCHECK(counter != NULL);
  // make sure not to hold lock when calling value() to avoid potential deadlocks
  return counter->value();
}

int64_t Coordinator::FragmentInstanceState::UpdateNumScanRangesCompleted() {
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

Coordinator::Coordinator(ExecEnv* exec_env, RuntimeProfile::EventSequence* events)
  : exec_env_(exec_env),
    has_called_wait_(false),
    returned_all_results_(false),
    executor_(NULL), // Set in Prepare()
    query_mem_tracker_(), // Set in Exec()
    num_remaining_fragment_instances_(0),
    obj_pool_(new ObjectPool()),
    query_events_(events),
    filter_routing_table_complete_(false) {
}

Coordinator::~Coordinator() {
  query_mem_tracker_.reset();
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

// TODO: templatize this
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
    debug_options->fragment_instance_idx = -1;
    debug_options->node_id = atoi(components[0].c_str());
    debug_options->phase = GetExecNodePhase(components[1]);
    debug_options->action = GetDebugAction(components[2]);
  } else {
    debug_options->fragment_instance_idx = atoi(components[0].c_str());
    debug_options->node_id = atoi(components[1].c_str());
    debug_options->phase = GetExecNodePhase(components[2]);
    debug_options->action = GetDebugAction(components[3]);
  }
  DCHECK(!(debug_options->phase == TExecNodePhase::CLOSE &&
           debug_options->action == TDebugAction::WAIT))
      << "Do not use CLOSE:WAIT debug actions "
      << "because nodes cannot be cancelled in Close()";
}

Status Coordinator::Exec(QuerySchedule& schedule,
    vector<ExprContext*>* output_expr_ctxs) {
  const TQueryExecRequest& request = schedule.request();
  DCHECK_GT(request.fragments.size(), 0);
  needs_finalization_ = request.__isset.finalize_params;
  if (needs_finalization_) finalize_params_ = request.finalize_params;

  VLOG_QUERY << "Exec() query_id=" << schedule.query_id();
  stmt_type_ = request.stmt_type;
  query_id_ = schedule.query_id();
  desc_tbl_ = request.desc_tbl;
  query_ctx_ = request.query_ctx;

  query_profile_.reset(
      new RuntimeProfile(obj_pool(), "Execution Profile " + PrintId(query_id_)));
  finalization_timer_ = ADD_TIMER(query_profile_, "FinalizationTimer");
  filter_updates_received_ = ADD_COUNTER(query_profile_, "FiltersReceived", TUnit::UNIT);

  SCOPED_TIMER(query_profile_->total_time_counter());

  const TNetworkAddress& coord = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);

  // After the coordinator fragment is started, it may call UpdateFilter() asynchronously,
  // which waits on this barrier for completion.
  if (schedule.num_fragment_instances() > 0) {
    exec_complete_barrier_.reset(new CountingBarrier(schedule.num_fragment_instances()));
  }

  // to keep things simple, make async Cancel() calls wait until plan fragment
  // execution has been initiated, otherwise we might try to cancel fragment
  // execution at Impala daemons where it hasn't even started
  lock_guard<mutex> l(lock_);

  // we run the root fragment ourselves if it is unpartitioned
  bool has_coordinator_fragment =
      request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;

  if (has_coordinator_fragment) {
    executor_.reset(new PlanFragmentExecutor(
            exec_env_, PlanFragmentExecutor::ReportStatusCallback()));
    // If a coordinator fragment is requested (for most queries this will be the case, the
    // exception is parallel INSERT queries), start this before starting any more plan
    // fragments, otherwise they start sending data before the local exchange node had a
    // chance to register with the stream mgr.
    // TODO: This is no longer necessary (see IMPALA-1599). Consider starting all
    // fragments in the same way with no coordinator special case.
    UpdateFilterRoutingTable(request.fragments[0].plan.nodes, 1, 0);
    TExecPlanFragmentParams rpc_params;
    SetExecPlanFragmentParams(schedule, request.fragments[0],
        (*schedule.exec_params())[0], 0, 0, 0, coord, &rpc_params);
    RETURN_IF_ERROR(executor_->Prepare(rpc_params));

    // Prepare output_expr_ctxs before optimizing the LLVM module. The other exprs of this
    // coordinator fragment have been prepared in executor_->Prepare().
    DCHECK(output_expr_ctxs != NULL);
    RETURN_IF_ERROR(Expr::CreateExprTrees(
        runtime_state()->obj_pool(), request.fragments[0].output_exprs,
        output_expr_ctxs));
    MemTracker* output_expr_tracker = runtime_state()->obj_pool()->Add(new MemTracker(
        -1, -1, "Output exprs", runtime_state()->instance_mem_tracker(), false));
    RETURN_IF_ERROR(Expr::Prepare(
        *output_expr_ctxs, runtime_state(), row_desc(), output_expr_tracker));
  } else {
    // The coordinator instance may require a query mem tracker even if there is no
    // coordinator fragment. For example, result-caching tracks memory via the query mem
    // tracker.
    // If there is a fragment, the fragment executor created above initializes the query
    // mem tracker. If not, the query mem tracker is created here.
    int64_t query_limit = -1;
    if (query_ctx_.request.query_options.__isset.mem_limit &&
        query_ctx_.request.query_options.mem_limit > 0) {
      query_limit = query_ctx_.request.query_options.mem_limit;
    }
    MemTracker* pool_tracker = MemTracker::GetRequestPoolMemTracker(
        schedule.request_pool(), exec_env_->process_mem_tracker());
    query_mem_tracker_ =
        MemTracker::GetQueryMemTracker(query_id_, query_limit, -1, pool_tracker, NULL);

    executor_.reset(NULL);
  }

  // Initialize the execution profile structures.
  InitExecProfile(request);

  if (schedule.num_fragment_instances() > 0) {
    RETURN_IF_ERROR(StartRemoteFragments(&schedule));
    // If we have a coordinator fragment and remote fragments (the common case), release
    // the thread token on the coordinator fragment. This fragment spends most of the time
    // waiting and doing very little work. Holding on to the token causes underutilization
    // of the machine. If there are 12 queries on this node, that's 12 tokens reserved for
    // no reason.
    if (has_coordinator_fragment) executor_->ReleaseThreadToken();
  }

  PrintFragmentInstanceInfo();

  const string& str = Substitute("Query $0", PrintId(query_id_));
  progress_ = ProgressUpdater(str, schedule.num_scan_ranges());

  return Status::OK();
}

void Coordinator::UpdateFilterRoutingTable(const vector<TPlanNode>& plan_nodes,
    int num_hosts, int start_fragment_instance_idx) {
  DCHECK(!filter_routing_table_complete_)
      << "UpdateFilterRoutingTable() called after setting filter_routing_table_complete_";
  for (const TPlanNode& plan_node: plan_nodes) {
    if (!plan_node.__isset.runtime_filters) continue;
    BOOST_FOREACH(const TRuntimeFilterDesc& filter, plan_node.runtime_filters) {
      if (filter_mode_ == TRuntimeFilterMode::LOCAL && !filter.has_local_target) {
        continue;
      }
      FilterState* f = &(filter_routing_table_[filter.filter_id]);
      if (plan_node.__isset.hash_join_node) {
        f->desc = filter;
        f->src = plan_node.node_id;
        f->pending_count = filter.is_broadcast_join ? 1 : num_hosts;
        vector<int> src_idxs;
        for (int i = 0; i < num_hosts; ++i) {
          src_idxs.push_back(start_fragment_instance_idx + i);
        }

        // If this is a broadcast join with a non-local target, only build and publish it
        // on MAX_BROADCAST_FILTER_PRODUCERS instances. If this is an intra-fragment
        // filter for a broadcast join, it is short-circuited in every fragment instance
        // and therefore will not be published globally, and should be generated
        // everywhere it can be used.
        if (filter.is_broadcast_join && !filter.has_local_target
            && num_hosts > MAX_BROADCAST_FILTER_PRODUCERS) {
          random_shuffle(src_idxs.begin(), src_idxs.end());
          src_idxs.resize(MAX_BROADCAST_FILTER_PRODUCERS);
        }
        f->src_fragment_instance_idxs.insert(src_idxs.begin(), src_idxs.end());
      } else if (plan_node.__isset.hdfs_scan_node) {
        f->target = plan_node.node_id;
        for (int i = 0; i < num_hosts; ++i) {
          f->target_fragment_instance_idxs.insert(start_fragment_instance_idx + i);
        }
      } else {
        // TODO: Frontend should not target filters at HBase scan nodes.
        DCHECK(plan_node.__isset.hbase_scan_node)
            << "Unexpected plan node with runtime filters: "
            << ThriftDebugString(plan_node);
      }
    }
  }
}

Status Coordinator::StartRemoteFragments(QuerySchedule* schedule) {
  int32_t num_fragment_instances = schedule->num_fragment_instances();
  DCHECK_GT(num_fragment_instances , 0);
  DebugOptions debug_options;
  ProcessQueryOptions(schedule->query_options(), &debug_options);
  const TQueryExecRequest& request = schedule->request();

  fragment_instance_states_.resize(num_fragment_instances);
  num_remaining_fragment_instances_ = num_fragment_instances;
  VLOG_QUERY << "starting " << num_fragment_instances << " fragment instances for query "
             << query_id_;

  query_events_->MarkEvent(
      Substitute("Ready to start $0 remote fragments", num_fragment_instances));

  int fragment_instance_idx = 0;
  bool has_coordinator_fragment =
      request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;
  filter_mode_ = schedule->query_options().runtime_filter_mode;
  // Start one fragment instance per fragment per host (number of hosts running each
  // fragment may not be constant).
  for (int fragment_idx = (has_coordinator_fragment ? 1 : 0);
       fragment_idx < request.fragments.size(); ++fragment_idx) {
    const FragmentExecParams* params = &(*schedule->exec_params())[fragment_idx];
    int num_hosts = params->hosts.size();
    DCHECK_GT(num_hosts, 0);

    if (filter_mode_ != TRuntimeFilterMode::OFF) {
      UpdateFilterRoutingTable(request.fragments[fragment_idx].plan.nodes, num_hosts,
          fragment_instance_idx);
    }

    fragment_profiles_[fragment_idx].num_instances = num_hosts;
    // Start one fragment instance for every fragment_instance required by the
    // schedule. Each fragment instance is assigned a unique ID, numbered from 0, with
    // instances for fragment ID 0 being assigned IDs [0 .. num_hosts(fragment_id_0)] and
    // so on. This enumeration scheme is relied upon by UpdateFilterRoutingTable().
    for (int per_fragment_instance_idx = 0; per_fragment_instance_idx < num_hosts;
         ++per_fragment_instance_idx) {
      DebugOptions* fragment_instance_debug_options =
          debug_options.IsApplicable(fragment_instance_idx) ? &debug_options : NULL;
      exec_env_->fragment_exec_thread_pool()->Offer(
          bind<void>(mem_fn(&Coordinator::ExecRemoteFragment), this,
              params, // fragment_exec_params
              &request.fragments[fragment_idx], // plan_fragment,
              fragment_instance_debug_options,
              schedule,
              fragment_instance_idx++,
              fragment_idx,
              per_fragment_instance_idx));
    }
  }
  filter_routing_table_complete_ = true;
  exec_complete_barrier_->Wait();
  query_events_->MarkEvent(
      Substitute("All $0 remote fragments started", fragment_instance_idx));

  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    query_profile_->AddInfoString(
        "Number of filters", Substitute("$0", filter_routing_table_.size()));
    query_profile_->AddInfoString("Filter routing table", FilterDebugString());
    if (VLOG_IS_ON(2)) VLOG_QUERY << FilterDebugString();
  }

  Status status = Status::OK();
  const TMetricDef& def =
      MakeTMetricDef("fragment-latencies", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  HistogramMetric latencies(def, 20000, 3);
  BOOST_FOREACH(FragmentInstanceState* exec_state, fragment_instance_states_) {
    lock_guard<mutex> l(*exec_state->lock());
    // Preserve the first non-OK status, if there is one
    if (status.ok()) status = *exec_state->status();
    latencies.Update(exec_state->rpc_latency());
  }

  query_profile_->AddInfoString("Fragment start latencies", latencies.ToHumanReadable());

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
  table_printer.AddColumn("Tgt. Node", false);
  table_printer.AddColumn("Targets", false);
  table_printer.AddColumn("Type", false);
  table_printer.AddColumn("Partition filter", false);

  // Distribution metrics are only meaningful if the coordinator is routing the filter.
  if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
    table_printer.AddColumn("Pending", false);
    table_printer.AddColumn("First arrived", false);
    table_printer.AddColumn("Completed", false);
  }
  lock_guard<SpinLock> l(filter_lock_);
  BOOST_FOREACH(const FilterRoutingTable::value_type& v, filter_routing_table_) {
    vector<string> row;
    const FilterState& state = v.second;
    row.push_back(lexical_cast<string>(v.first));
    row.push_back(lexical_cast<string>(state.src));
    row.push_back(lexical_cast<string>(state.target));
    row.push_back(lexical_cast<string>(state.target_fragment_instance_idxs.size()));
    if (state.desc.is_broadcast_join) {
      row.push_back(state.desc.has_local_target ? "LOCAL" : "GLOBAL (Broadcast)");
    } else {
      row.push_back("GLOBAL (Partition)");
    }
    row.push_back(state.desc.is_bound_by_partition_columns ? "true" : "false");

    if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
      row.push_back(Substitute("$0 ($1)", state.pending_count,
              state.src_fragment_instance_idxs.size()));
      if (state.first_arrival_time == 0L) {
        row.push_back("N/A");
      } else {
        row.push_back(PrettyPrinter::Print(state.first_arrival_time, TUnit::TIME_NS));
      }
      if (state.completion_time == 0L) {
        row.push_back("N/A");
      } else {
        row.push_back(PrettyPrinter::Print(state.completion_time, TUnit::TIME_NS));
      }
    }
    table_printer.AddRow(row);
  }
  // Add a line break, as in all contexts this is called we need to start a new line to
  // print it correctly.
  return Substitute("\n$0", table_printer.ToString());
}

Status Coordinator::GetStatus() {
  lock_guard<mutex> l(lock_);
  return query_status_;
}

Status Coordinator::UpdateStatus(const Status& status, const TUniqueId& instance_id,
    const string& instance_hostname) {
  {
    lock_guard<mutex> l(lock_);

    // The query is done and we are just waiting for remote fragments to clean up.
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
  BOOST_FOREACH(const string& component, components) {
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
  BOOST_FOREACH(const string& path, prefixes) {
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
  hdfsFS hdfs_connection;
  // InsertStmt ensures that all partitions are on the same filesystem as the table's
  // base directory, so opening a single connection is okay.
  // TODO: modify this code so that restriction can be lifted.
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      finalize_params_.hdfs_base_dir, &hdfs_connection));

  // INSERT finalization happens in the five following steps
  // 1. If OVERWRITE, remove all the files in the target directory
  // 2. Create all the necessary partition directories.
  HdfsOperationSet partition_create_ops(&hdfs_connection);
  DescriptorTbl* descriptor_table;
  DescriptorTbl::Create(obj_pool(), desc_tbl_, &descriptor_table);
  HdfsTableDescriptor* hdfs_table = static_cast<HdfsTableDescriptor*>(
      descriptor_table->GetTableDescriptor(finalize_params_.table_id));
  DCHECK(hdfs_table != NULL) << "INSERT target table not known in descriptor table: "
                             << finalize_params_.table_id;

  // Loop over all partitions that were updated by this insert, and create the set of
  // filesystem operations required to create the correct partition structure on disk.
  BOOST_FOREACH(const PartitionStatusMap::value_type& partition, per_partition_status_) {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "Overwrite/PartitionCreationTimer",
          "FinalizationTimer"));

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
                           << "\n" <<  PrintThrift(runtime_state()->fragment_params());
      part_path_ss << part->location();
    }
    const string& part_path = part_path_ss.str();

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
            hdfsListDirectory(hdfs_connection, part_path.c_str(), &num_files);
        if (existing_files == NULL && errno == EAGAIN) {
          errno = 0;
          existing_files =
              hdfsListDirectory(hdfs_connection, part_path.c_str(), &num_files);
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
        if (FLAGS_insert_inherit_permissions) {
          PopulatePathPermissionCache(hdfs_connection, part_path, &permissions_cache);
        }
        if (hdfsExists(hdfs_connection, part_path.c_str()) != -1) {
          partition_create_ops.Add(DELETE_THEN_CREATE, part_path);
        } else {
          // Otherwise just create the directory.
          partition_create_ops.Add(CREATE_DIR, part_path);
        }
      }
    } else {
      if (FLAGS_insert_inherit_permissions) {
        PopulatePathPermissionCache(hdfs_connection, part_path, &permissions_cache);
      }
      if (hdfsExists(hdfs_connection, part_path.c_str()) == -1) {
        partition_create_ops.Add(CREATE_DIR, part_path);
      }
    }
  }

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "Overwrite/PartitionCreationTimer",
          "FinalizationTimer"));
    if (!partition_create_ops.Execute(exec_env_->hdfs_op_thread_pool(), false)) {
      BOOST_FOREACH(const HdfsOperationSet::Error& err, partition_create_ops.errors()) {
        // It's ok to ignore errors creating the directories, since they may already
        // exist. If there are permission errors, we'll run into them later.
        if (err.first->op() != CREATE_DIR) {
          stringstream ss;
          ss << "Error(s) deleting partition directories. First error (of "
             << partition_create_ops.errors().size() << ") was: " << err.second;
          return Status(ss.str());
        }
      }
    }
  }

  // 3. Move all tmp files
  HdfsOperationSet move_ops(&hdfs_connection);
  HdfsOperationSet dir_deletion_ops(&hdfs_connection);

  BOOST_FOREACH(FileMoveMap::value_type& move, files_to_move_) {
    // Empty destination means delete, so this is a directory. These get deleted in a
    // separate pass to ensure that we have moved all the contents of the directory first.
    if (move.second.empty()) {
      VLOG_ROW << "Deleting file: " << move.first;
      dir_deletion_ops.Add(DELETE, move.first);
    } else {
      VLOG_ROW << "Moving tmp file: " << move.first << " to " << move.second;
      move_ops.Add(RENAME, move.first, move.second);
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
  // Do this last in case we make the dirs unwriteable.
  if (FLAGS_insert_inherit_permissions) {
    HdfsOperationSet chmod_ops(&hdfs_connection);
    BOOST_FOREACH(const PermissionCache::value_type& perm, permissions_cache) {
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
  // All backends must have reported their final statuses before finalization, which is a
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

Status Coordinator::WaitForAllBackends() {
  unique_lock<mutex> l(lock_);
  while (num_remaining_fragment_instances_ > 0 && query_status_.ok()) {
    VLOG_QUERY << "Coordinator waiting for backends to finish, "
               << num_remaining_fragment_instances_ << " remaining";
    backend_completion_cv_.wait(l);
  }
  if (query_status_.ok()) {
    VLOG_QUERY << "All backends finished successfully.";
  } else {
    VLOG_QUERY << "All backends finished due to one or more errors.";
  }

  return query_status_;
}

Status Coordinator::Wait() {
  lock_guard<mutex> l(wait_lock_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (has_called_wait_) return Status::OK();
  has_called_wait_ = true;
  Status return_status = Status::OK();
  if (executor_.get() != NULL) {
    // Open() may block
    return_status = UpdateStatus(executor_->Open(),
        runtime_state()->fragment_instance_id(), FLAGS_hostname);

    if (return_status.ok()) {
      // If the coordinator fragment has a sink, it will have finished executing at this
      // point.  It's safe therefore to copy the set of files to move and updated
      // partitions into the query-wide set.
      RuntimeState* state = runtime_state();
      DCHECK(state != NULL);

      // No other backends should have updated these structures if the coordinator has a
      // fragment.  (Backends have a sink only if the coordinator does not)
      DCHECK_EQ(files_to_move_.size(), 0);
      DCHECK_EQ(per_partition_status_.size(), 0);

      // Because there are no other updates, safe to copy the maps rather than merge them.
      files_to_move_ = *state->hdfs_files_to_move();
      per_partition_status_ = *state->per_partition_status();
    }
  } else {
    // Query finalization can only happen when all backends have reported
    // relevant state. They only have relevant state to report in the parallel
    // INSERT case, otherwise all the relevant state is from the coordinator
    // fragment which will be available after Open() returns.
    // Ignore the returned status if finalization is required., since FinalizeQuery() will
    // pick it up and needs to execute regardless.
    Status status = WaitForAllBackends();
    if (!needs_finalization_ && !status.ok()) return status;
  }

  // Query finalization is required only for HDFS table sinks
  if (needs_finalization_) {
    RETURN_IF_ERROR(FinalizeQuery());
  }

  if (stmt_type_ == TStmtType::DML) {
    query_profile_->AddInfoString("Insert Stats",
        DataSink::OutputInsertStats(per_partition_status_, "\n"));
    // For DML queries, when Wait is done, the query is complete.  Report aggregate
    // query profiles at this point.
    // TODO: make sure ReportQuerySummary gets called on error
    ReportQuerySummary();
  }

  if (filter_routing_table_.size() > 0) {
    query_profile_->AddInfoString("Final filter table", FilterDebugString());
  }

  return return_status;
}

Status Coordinator::GetNext(RowBatch** batch, RuntimeState* state) {
  VLOG_ROW << "GetNext() query_id=" << query_id_;
  DCHECK(has_called_wait_);
  SCOPED_TIMER(query_profile_->total_time_counter());

  if (executor_.get() == NULL) {
    // If there is no local fragment, we produce no output, and execution will
    // have finished after Wait.
    *batch = NULL;
    return GetStatus();
  }

  // do not acquire lock_ here, otherwise we could block and prevent an async
  // Cancel() from proceeding
  Status status = executor_->GetNext(batch);

  // if there was an error, we need to return the query's error status rather than
  // the status we just got back from the local executor (which may well be CANCELLED
  // in that case).  Coordinator fragment failed in this case so we log the query_id.
  RETURN_IF_ERROR(UpdateStatus(status, runtime_state()->fragment_instance_id(),
      FLAGS_hostname));

  if (*batch == NULL) {
    returned_all_results_ = true;
    if (executor_->ReachedLimit()) {
      // We've reached the query limit, cancel the remote fragments.  The
      // Exchange node on our fragment is no longer receiving rows so the
      // remote fragments must be explicitly cancelled.
      CancelRemoteFragments();
      RuntimeState* state = runtime_state();
      if (state != NULL) {
        // Cancel the streams receiving batches.  The exchange nodes that would
        // normally read from the streams are done.
        state->stream_mgr()->Cancel(state->fragment_instance_id());
      }
    }

    // Don't return final NULL until all backends have completed.
    // GetNext must wait for all backends to complete before
    // ultimately signalling the end of execution via a NULL
    // batch. After NULL is returned, the coordinator may tear down
    // query state, and perform post-query finalization which might
    // depend on the reports from all backends.
    RETURN_IF_ERROR(WaitForAllBackends());
    if (query_status_.ok()) {
      // If the query completed successfully, report aggregate query profiles.
      ReportQuerySummary();
    }
  } else {
#ifndef NDEBUG
    ValidateCollectionSlots(*batch);
#endif
  }

  return Status::OK();
}

void Coordinator::ValidateCollectionSlots(RowBatch* batch) {
  const RowDescriptor& row_desc = executor_->row_desc();
  if (!row_desc.HasVarlenSlots()) return;
  for (int i = 0; i < batch->num_rows(); ++i) {
    TupleRow* row = batch->GetRow(i);
    for (int j = 0; j < row_desc.tuple_descriptors().size(); ++j) {
      const TupleDescriptor* tuple_desc = row_desc.tuple_descriptors()[j];
      if (tuple_desc->collection_slots().empty()) continue;
      for (int k = 0; k < tuple_desc->collection_slots().size(); ++k) {
        const SlotDescriptor* slot_desc = tuple_desc->collection_slots()[k];
        int tuple_idx = row_desc.GetTupleIdx(slot_desc->parent()->id());
        const Tuple* tuple = row->GetTuple(tuple_idx);
        if (tuple == NULL) continue;
        DCHECK(tuple->IsNull(slot_desc->null_indicator_offset()));
      }
    }
  }
}

void Coordinator::PrintFragmentInstanceInfo() {
  for (int i = 0; i < fragment_instance_states_.size(); ++i) {
    SummaryStats& acc =
        fragment_profiles_[fragment_instance_states_[i]->fragment_idx()].bytes_assigned;
    acc(fragment_instance_states_[i]->total_split_size());
  }

  for (int i = (executor_.get() == NULL ? 0 : 1); i < fragment_profiles_.size(); ++i) {
    SummaryStats& acc = fragment_profiles_[i].bytes_assigned;
    double min = accumulators::min(acc);
    double max = accumulators::max(acc);
    double mean = accumulators::mean(acc);
    double stddev = sqrt(accumulators::variance(acc));
    stringstream ss;
    ss << " min: " << PrettyPrinter::Print(min, TUnit::BYTES)
      << ", max: " << PrettyPrinter::Print(max, TUnit::BYTES)
      << ", avg: " << PrettyPrinter::Print(mean, TUnit::BYTES)
      << ", stddev: " << PrettyPrinter::Print(stddev, TUnit::BYTES);
    fragment_profiles_[i].averaged_profile->AddInfoString("split sizes", ss.str());

    if (VLOG_FILE_IS_ON) {
      VLOG_FILE << "Byte split for fragment " << i << " " << ss.str();
      for (int j = 0; j < fragment_instance_states_.size(); ++j) {
        FragmentInstanceState* exec_state = fragment_instance_states_[j];
        if (exec_state->fragment_idx() != i) continue;
        VLOG_FILE << "data volume for ipaddress " << exec_state << ": "
                  << PrettyPrinter::Print(exec_state->total_split_size(), TUnit::BYTES);
      }
    }
  }
}

void Coordinator::InitExecProfile(const TQueryExecRequest& request) {
  // Initialize the structure to collect execution summary of every plan node.
  exec_summary_.__isset.nodes = true;
  for (int i = 0; i < request.fragments.size(); ++i) {
    if (!request.fragments[i].__isset.plan) continue;
    const TPlan& plan = request.fragments[i].plan;
    int fragment_first_node_idx = exec_summary_.nodes.size();

    for (int j = 0; j < plan.nodes.size(); ++j) {
      TPlanNodeExecSummary node;
      node.node_id = plan.nodes[j].node_id;
      node.fragment_id = i;
      node.label = plan.nodes[j].label;
      node.__set_label_detail(plan.nodes[j].label_detail);
      node.num_children = plan.nodes[j].num_children;

      if (plan.nodes[j].__isset.estimated_stats) {
        node.__set_estimated_stats(plan.nodes[j].estimated_stats);
      }

      plan_node_id_to_summary_map_[plan.nodes[j].node_id] = exec_summary_.nodes.size();
      exec_summary_.nodes.push_back(node);
    }

    if (request.fragments[i].__isset.output_sink &&
        request.fragments[i].output_sink.type == TDataSinkType::DATA_STREAM_SINK) {
      const TDataStreamSink& sink = request.fragments[i].output_sink.stream_sink;
      int exch_idx = plan_node_id_to_summary_map_[sink.dest_node_id];
      if (sink.output_partition.type == TPartitionType::UNPARTITIONED) {
        exec_summary_.nodes[exch_idx].__set_is_broadcast(true);
      }
      exec_summary_.__isset.exch_to_sender_map = true;
      exec_summary_.exch_to_sender_map[exch_idx] = fragment_first_node_idx;
    }
  }

  if (executor_.get() != NULL) {
    // register coordinator's fragment profile now, before those of the backends,
    // so it shows up at the top
    query_profile_->AddChild(executor_->profile());
    executor_->profile()->set_name(Substitute("Coordinator Fragment $0",
        request.fragments[0].display_name));
    CollectScanNodeCounters(executor_->profile(), &coordinator_counters_);
  }

  // Initialize the runtime profile structure. This adds the per fragment average
  // profiles followed by the per fragment instance profiles.
  bool has_coordinator_fragment =
      request.fragments[0].partition.type == TPartitionType::UNPARTITIONED;
  fragment_profiles_.resize(request.fragments.size());
  for (int i = 0; i < request.fragments.size(); ++i) {
    fragment_profiles_[i].num_instances = 0;

    // Special case fragment idx 0 if there is a coordinator. There is only one
    // instance of this profile so the average is just the coordinator profile.
    if (i == 0 && has_coordinator_fragment) {
      fragment_profiles_[i].averaged_profile = executor_->profile();
      fragment_profiles_[i].num_instances = 1;
      continue;
    }
    fragment_profiles_[i].averaged_profile =
        obj_pool()->Add(new RuntimeProfile(obj_pool(),
            Substitute("Averaged Fragment $0", request.fragments[i].display_name), true));
    // Insert the avg profiles in ascending fragment number order. If
    // there is a coordinator fragment, it's been placed in
    // fragment_profiles_[0].averaged_profile, ensuring that this code
    // will put the first averaged profile immediately after it. If
    // there is no coordinator fragment, the first averaged profile
    // will be inserted as the first child of query_profile_, and then
    // all other averaged fragments will follow.
    query_profile_->AddChild(fragment_profiles_[i].averaged_profile, true,
        (i > 0) ? fragment_profiles_[i-1].averaged_profile : NULL);

    fragment_profiles_[i].root_profile =
        obj_pool()->Add(new RuntimeProfile(obj_pool(),
            Substitute("Fragment $0", request.fragments[i].display_name)));
    // Note: we don't start the wall timer here for the fragment
    // profile; it's uninteresting and misleading.
    query_profile_->AddChild(fragment_profiles_[i].root_profile);
  }
}

void Coordinator::CollectScanNodeCounters(RuntimeProfile* profile,
    FragmentInstanceCounters* counters) {
  vector<RuntimeProfile*> children;
  profile->GetAllChildren(&children);
  for (int i = 0; i < children.size(); ++i) {
    RuntimeProfile* p = children[i];
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

void Coordinator::CreateAggregateCounters(
    const vector<TPlanFragment>& fragments) {
  BOOST_FOREACH(const TPlanFragment& fragment, fragments) {
    if (!fragment.__isset.plan) continue;
    const vector<TPlanNode>& nodes = fragment.plan.nodes;
    BOOST_FOREACH(const TPlanNode& node, nodes) {
      if (node.node_type != TPlanNodeType::HDFS_SCAN_NODE
          && node.node_type != TPlanNodeType::HBASE_SCAN_NODE) {
        continue;
      }

      stringstream s;
      s << PrintPlanNodeType(node.node_type) << " (id="
        << node.node_id << ") Throughput";
      query_profile_->AddDerivedCounter(s.str(), TUnit::BYTES_PER_SECOND,
          bind<int64_t>(mem_fn(&Coordinator::ComputeTotalThroughput),
                        this, node.node_id));
      s.str("");
      s << PrintPlanNodeType(node.node_type) << " (id="
        << node.node_id << ") Completed scan ranges";
      query_profile_->AddDerivedCounter(s.str(), TUnit::UNIT,
          bind<int64_t>(mem_fn(&Coordinator::ComputeTotalScanRangesComplete),
                        this, node.node_id));
    }
  }
}

int64_t Coordinator::ComputeTotalThroughput(int node_id) {
  int64_t value = 0;
  for (int i = 0; i < fragment_instance_states_.size(); ++i) {
    FragmentInstanceState* exec_state = fragment_instance_states_[i];
    value += exec_state->GetNodeThroughput(node_id);
  }
  // Add up the local fragment throughput counter
  CounterMap& throughput_counters = coordinator_counters_.throughput_counters;
  CounterMap::iterator it = throughput_counters.find(node_id);
  if (it != throughput_counters.end()) {
    value += it->second->value();
  }
  return value;
}

int64_t Coordinator::ComputeTotalScanRangesComplete(int node_id) {
  int64_t value = 0;
  for (int i = 0; i < fragment_instance_states_.size(); ++i) {
    FragmentInstanceState* exec_state = fragment_instance_states_[i];
    value += exec_state->GetNumScanRangesCompleted(node_id);
  }
  // Add up the local fragment throughput counter
  CounterMap& scan_ranges_complete = coordinator_counters_.scan_ranges_complete_counters;
  CounterMap::iterator it = scan_ranges_complete.find(node_id);
  if (it != scan_ranges_complete.end()) {
    value += it->second->value();
  }
  return value;
}

void Coordinator::ExecRemoteFragment(const FragmentExecParams* fragment_exec_params,
    const TPlanFragment* plan_fragment, DebugOptions* debug_options,
    QuerySchedule* schedule, int fragment_instance_idx, int fragment_idx,
    int per_fragment_instance_idx) {
  NotifyBarrierOnExit notifier(exec_complete_barrier_.get());
  TExecPlanFragmentParams rpc_params;
  SetExecPlanFragmentParams(*schedule, *plan_fragment, *fragment_exec_params,
      fragment_instance_idx, fragment_idx, per_fragment_instance_idx,
      MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port), &rpc_params);
  if (debug_options != NULL) {
    rpc_params.params.__set_debug_node_id(debug_options->node_id);
    rpc_params.params.__set_debug_action(debug_options->action);
    rpc_params.params.__set_debug_phase(debug_options->phase);
  }
  FragmentInstanceState* exec_state = obj_pool()->Add(
      new FragmentInstanceState(fragment_idx, fragment_exec_params,
          per_fragment_instance_idx, obj_pool()));
  exec_state->ComputeTotalSplitSize(rpc_params.params.per_node_scan_ranges);
  fragment_instance_states_[fragment_instance_idx] = exec_state;
  VLOG_FILE << "making rpc: ExecPlanFragment query_id=" << query_id_
            << " instance_id=" << exec_state->fragment_instance_id()
            << " host=" << exec_state->impalad_address();

  // Guard against concurrent UpdateExecStatus() that may arrive after RPC returns.
  lock_guard<mutex> l(*exec_state->lock());
  int64_t start = MonotonicMillis();

  Status client_connect_status;
  ImpalaInternalServiceConnection backend_client(exec_env_->impalad_client_cache(),
      exec_state->impalad_address(), &client_connect_status);
  if (!client_connect_status.ok()) {
    exec_state->SetInitialStatus(client_connect_status);
    return;
  }

  TExecPlanFragmentResult thrift_result;
  Status rpc_status = backend_client.DoRpc(&ImpalaInternalServiceClient::ExecPlanFragment,
      rpc_params, &thrift_result);

  exec_state->SetRpcLatency(MonotonicMillis() - start);

  const string ERR_TEMPLATE = "ExecPlanRequest rpc query_id=$0 instance_id=$1 failed: $2";

  if (!rpc_status.ok()) {
    const string& err_msg = Substitute(ERR_TEMPLATE, PrintId(query_id()),
        PrintId(exec_state->fragment_instance_id()), rpc_status.msg().msg());
    VLOG_QUERY << err_msg;
    exec_state->SetInitialStatus(Status(err_msg));
    return;
  }

  Status exec_plan_status = Status(thrift_result.status);
  if (!exec_plan_status.ok()) {
    const string& err_msg = Substitute(ERR_TEMPLATE, PrintId(query_id()),
        PrintId(exec_state->fragment_instance_id()),
        exec_plan_status.msg().GetFullMessageDetails());
    VLOG_QUERY << err_msg;
    exec_state->SetInitialStatus(Status(err_msg));
    return;
  }

  exec_state->SetInitialStatus(Status::OK());
  return;
}

void Coordinator::Cancel(const Status* cause) {
  lock_guard<mutex> l(lock_);
  // if the query status indicates an error, cancellation has already been initiated
  if (!query_status_.ok()) return;
  // prevent others from cancelling a second time
  query_status_ = (cause != NULL && !cause->ok()) ? *cause : Status::CANCELLED;
  CancelInternal();
}

void Coordinator::CancelInternal() {
  VLOG_QUERY << "Cancel() query_id=" << query_id_;
  DCHECK(!query_status_.ok());

  // cancel local fragment
  if (executor_.get() != NULL) executor_->Cancel();

  CancelRemoteFragments();

  // Report the summary with whatever progress the query made before being cancelled.
  ReportQuerySummary();
}

void Coordinator::CancelRemoteFragments() {
  for (int i = 0; i < fragment_instance_states_.size(); ++i) {
    FragmentInstanceState* exec_state = fragment_instance_states_[i];

    // If a fragment failed before we finished issuing all remote fragments,
    // this function will have been called before we finished populating
    // fragment_instance_states_. Skip any such uninitialized exec states.
    if (exec_state == NULL) continue;

    // lock each exec_state individually to synchronize correctly with
    // UpdateFragmentExecStatus() (which doesn't get the global lock_
    // to set its status)
    lock_guard<mutex> l(*exec_state->lock());

    // no need to cancel if we already know it terminated w/ an error status
    if (!exec_state->status()->ok()) continue;

    // Nothing to cancel if the exec rpc was not sent
    if (!exec_state->rpc_sent()) continue;

    // don't cancel if it already finished
    if (exec_state->done()) continue;

    // set an error status to make sure we only cancel this once
    exec_state->SetStatus(Status::CANCELLED);

    // if we get an error while trying to get a connection to the backend,
    // keep going
    Status status;
    ImpalaInternalServiceConnection backend_client(
        exec_env_->impalad_client_cache(), exec_state->impalad_address(), &status);
    if (!status.ok()) continue;

    TCancelPlanFragmentParams params;
    params.protocol_version = ImpalaInternalServiceVersion::V1;
    params.__set_fragment_instance_id(exec_state->fragment_instance_id());
    TCancelPlanFragmentResult res;
    VLOG_QUERY << "sending CancelPlanFragment rpc for instance_id="
               << exec_state->fragment_instance_id() << " backend="
               << exec_state->impalad_address();
    Status rpc_status = backend_client.DoRpc(
        &ImpalaInternalServiceClient::CancelPlanFragment, params, &res);
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

  // notify that we completed with an error
  backend_completion_cv_.notify_all();
}

Status Coordinator::UpdateFragmentExecStatus(const TReportExecStatusParams& params) {
  VLOG_FILE << "UpdateFragmentExecStatus() query_id=" << query_id_
            << " status=" << params.status.status_code
            << " done=" << (params.done ? "true" : "false");
  uint32_t fragment_instance_idx = params.fragment_instance_idx;
  if (fragment_instance_idx >= fragment_instance_states_.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Unknown fragment instance index $0 (max known: $1)",
            fragment_instance_idx, fragment_instance_states_.size() - 1));
  }
  FragmentInstanceState* exec_state = fragment_instance_states_[fragment_instance_idx];

  const TRuntimeProfileTree& cumulative_profile = params.profile;
  Status status(params.status);
  {
    lock_guard<mutex> l(*exec_state->lock());
    if (!status.ok()) {
      // During query cancellation, exec_state is set to CANCELLED. However, we might
      // process a non-error message from a fragment executor that is sent
      // before query cancellation is invoked. Make sure we don't go from error status to
      // OK.
      exec_state->SetStatus(status);
    }
    exec_state->SetDone(params.done);
    if (exec_state->status()->ok()) {
      // We can't update this backend's profile if ReportQuerySummary() is running,
      // because it depends on all profiles not changing during its execution (when it
      // calls SortChildren()). ReportQuerySummary() only gets called after
      // WaitForAllBackends() returns or at the end of CancelRemoteFragments().
      // WaitForAllBackends() only returns after all backends have completed (in which
      // case we wouldn't be in this function), or when there's an error, in which case
      // CancelRemoteFragments() is called. CancelRemoteFragments sets all exec_state's
      // statuses to cancelled.
      // TODO: We're losing this profile information. Call ReportQuerySummary only after
      // all backends have completed.
      exec_state->profile()->Update(cumulative_profile);

      // Update the average profile for the fragment corresponding to this instance.
      exec_state->profile()->ComputeTimeInProfile();
      UpdateAverageProfile(exec_state);
      UpdateExecSummary(exec_state->fragment_idx(), exec_state->instance_idx(),
          exec_state->profile());
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
    BOOST_FOREACH(const PartitionStatusMap::value_type& partition,
        params.insert_exec_status.per_partition_status) {
      TInsertPartitionStatus* status = &(per_partition_status_[partition.first]);
      status->num_appended_rows += partition.second.num_appended_rows;
      status->id = partition.second.id;
      if (!status->__isset.stats) status->__set_stats(TInsertStats());
      DataSink::MergeInsertStats(partition.second.stats, &status->stats);
    }
    files_to_move_.insert(
        params.insert_exec_status.files_to_move.begin(),
        params.insert_exec_status.files_to_move.end());
  }

  if (VLOG_FILE_IS_ON) {
    stringstream s;
    exec_state->profile()->PrettyPrint(&s);
    VLOG_FILE << "profile for query_id=" << query_id_
              << " instance_id=" << exec_state->fragment_instance_id()
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
    VLOG_QUERY << "Fragment instance " << params.fragment_instance_idx << "("
               << exec_state->fragment_instance_id() << ") on host "
               << exec_state->impalad_address() << " completed, "
               << num_remaining_fragment_instances_ - 1 << " remaining: query_id="
               << query_id_;
    if (VLOG_QUERY_IS_ON && num_remaining_fragment_instances_ > 1) {
      // print host/port info for the first backend that's still in progress as a
      // debugging aid for backend deadlocks
      for (int i = 0; i < fragment_instance_states_.size(); ++i) {
        FragmentInstanceState* exec_state = fragment_instance_states_[i];
        lock_guard<mutex> l2(*exec_state->lock());
        if (!exec_state->done()) {
          VLOG_QUERY << "query_id=" << query_id_ << ": first in-progress backend: "
                     << exec_state->impalad_address();
          break;
        }
      }
    }
    if (--num_remaining_fragment_instances_ == 0) {
      backend_completion_cv_.notify_all();
    }
  }

  return Status::OK();
}

const RowDescriptor& Coordinator::row_desc() const {
  DCHECK(executor_.get() != NULL);
  return executor_->row_desc();
}

RuntimeState* Coordinator::runtime_state() {
  return executor_.get() == NULL ? NULL : executor_->runtime_state();
}

MemTracker* Coordinator::query_mem_tracker() {
  return executor_.get() == NULL ? query_mem_tracker_.get() :
      executor_->runtime_state()->query_mem_tracker();
}

bool Coordinator::PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update) {
  // Assume we are called only after all fragments have completed
  DCHECK(has_called_wait_);

  BOOST_FOREACH(const PartitionStatusMap::value_type& partition, per_partition_status_) {
    catalog_update->created_partitions.insert(partition.first);
  }

  return catalog_update->created_partitions.size() != 0;
}

// Comparator to order fragments by descending total time
typedef struct {
  typedef pair<RuntimeProfile*, bool> Profile;
  bool operator()(const Profile& a, const Profile& b) const {
    // Reverse ordering: we want the longest first
    return
        a.first->total_time_counter()->value() > b.first->total_time_counter()->value();
  }
} InstanceComparator;

// Update fragment average profile information from a backend execution state.
void Coordinator::UpdateAverageProfile(FragmentInstanceState* fragment_instance_state) {
  int fragment_idx = fragment_instance_state->fragment_idx();
  DCHECK_GE(fragment_idx, 0);
  DCHECK_LT(fragment_idx, fragment_profiles_.size());
  PerFragmentProfileData& data = fragment_profiles_[fragment_idx];

  // No locks are taken since UpdateAverage() and AddChild() take their own locks
  data.averaged_profile->UpdateAverage(fragment_instance_state->profile());
  data.root_profile->AddChild(fragment_instance_state->profile());
}

// Compute fragment summary information from a backend execution state.
void Coordinator::ComputeFragmentSummaryStats(
    FragmentInstanceState* fragment_instance_state) {
  int fragment_idx = fragment_instance_state->fragment_idx();
  DCHECK_GE(fragment_idx, 0);
  DCHECK_LT(fragment_idx, fragment_profiles_.size());
  PerFragmentProfileData& data = fragment_profiles_[fragment_idx];

  int64_t completion_time = fragment_instance_state->stopwatch()->ElapsedTime();
  data.completion_times(completion_time);
  data.rates(fragment_instance_state->total_split_size() / (completion_time / 1000.0
    / 1000.0 / 1000.0));

  // Add the child in case it has not been added previously
  // via UpdateAverageProfile(). AddChild() will do nothing if the child
  // already exists.
  data.root_profile->AddChild(fragment_instance_state->profile());
}

void Coordinator::UpdateExecSummary(int fragment_idx, int instance_idx,
    RuntimeProfile* profile) {
  vector<RuntimeProfile*> children;
  profile->GetAllChildren(&children);

  lock_guard<SpinLock> l(exec_summary_lock_);
  for (int i = 0; i < children.size(); ++i) {
    int id = ExecNode::GetNodeIdFromProfile(children[i]);
    if (id == -1) continue;

    TPlanNodeExecSummary& exec_summary =
        exec_summary_.nodes[plan_node_id_to_summary_map_[id]];
    if (exec_summary.exec_stats.empty()) {
      // First time, make an exec_stats for each instance this plan node is running on.
      DCHECK_LT(fragment_idx, fragment_profiles_.size());
      exec_summary.exec_stats.resize(fragment_profiles_[fragment_idx].num_instances);
    }
    DCHECK_LT(instance_idx, exec_summary.exec_stats.size());
    TExecStats& stats = exec_summary.exec_stats[instance_idx];

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
//   1. Averaged remote fragment profiles (TODO: add outliers)
//   2. Summary of remote fragment durations (min, max, mean, stddev)
//   3. Summary of remote fragment rates (min, max, mean, stddev)
// TODO: add histogram/percentile
void Coordinator::ReportQuerySummary() {
  // In this case, the query did not even get to start on all the remote nodes,
  // some of the state that is used below might be uninitialized.  In this case,
  // the query has made so little progress, reporting a summary is not very useful.
  if (!has_called_wait_) return;

  // The fragment has finished executing.  Update the profile to compute the
  // fraction of time spent in each node.
  if (executor_.get() != NULL) {
    executor_->profile()->ComputeTimeInProfile();
    UpdateExecSummary(0, 0, executor_->profile());
  }

  if (!fragment_instance_states_.empty()) {
    // Average all remote fragments for each fragment.
    for (int i = 0; i < fragment_instance_states_.size(); ++i) {
      fragment_instance_states_[i]->profile()->ComputeTimeInProfile();
      UpdateAverageProfile(fragment_instance_states_[i]);
      ComputeFragmentSummaryStats(fragment_instance_states_[i]);
      UpdateExecSummary(fragment_instance_states_[i]->fragment_idx(),
          fragment_instance_states_[i]->instance_idx(),
          fragment_instance_states_[i]->profile());
    }

    InstanceComparator comparator;
    // Per fragment instances have been collected, output summaries
    for (int i = (executor_.get() != NULL ? 1 : 0); i < fragment_profiles_.size(); ++i) {
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
    if (executor_.get() != NULL) {
      // Coordinator fragment is not included in fragment_instance_states_.
      RuntimeProfile::Counter* mem_usage_counter =
          executor_->profile()->GetCounter(
              PlanFragmentExecutor::PER_HOST_PEAK_MEM_COUNTER);
      if (mem_usage_counter != NULL) {
        TNetworkAddress coord = MakeNetworkAddress(FLAGS_hostname, FLAGS_be_port);
        per_node_peak_mem_usage[coord] = mem_usage_counter->value();
      }
    }
    for (int i = 0; i < fragment_instance_states_.size(); ++i) {
      int64_t initial_usage = 0;
      int64_t* mem_usage = FindOrInsert(&per_node_peak_mem_usage,
          fragment_instance_states_[i]->impalad_address(), initial_usage);
      RuntimeProfile::Counter* mem_usage_counter =
          fragment_instance_states_[i]->profile()->GetCounter(
              PlanFragmentExecutor::PER_HOST_PEAK_MEM_COUNTER);
      if (mem_usage_counter != NULL && mem_usage_counter->value() > *mem_usage) {
        per_node_peak_mem_usage[fragment_instance_states_[i]->impalad_address()] =
            mem_usage_counter->value();
      }
    }
    stringstream info;
    BOOST_FOREACH(PerNodePeakMemoryUsage::value_type entry, per_node_peak_mem_usage) {
      info << entry.first << "("
           << PrettyPrinter::Print(entry.second, TUnit::BYTES) << ") ";
    }
    query_profile_->AddInfoString("Per Node Peak Memory Usage", info.str());
  }
}

string Coordinator::GetErrorLog() {
  ErrorLogMap merged;
  {
    lock_guard<mutex> l(lock_);
    if (executor_.get() != NULL && executor_->runtime_state() != NULL &&
        !executor_->runtime_state()->ErrorLogIsEmpty()) {
      MergeErrorMaps(&merged, executor_->runtime_state()->error_log());
    }
  }
  for (int i = 0; i < fragment_instance_states_.size(); ++i) {
    lock_guard<mutex> l(*fragment_instance_states_[i]->lock());
    if (fragment_instance_states_[i]->error_log()->size() > 0) {
      MergeErrorMaps(&merged, *fragment_instance_states_[i]->error_log());
    }
  }
  return PrintErrorMapToString(merged);
}

void Coordinator::SetExecPlanFragmentParams(QuerySchedule& schedule,
    const TPlanFragment& fragment, const FragmentExecParams& params,
    int fragment_instance_idx, int fragment_idx, int per_fragment_instance_idx,
    const TNetworkAddress& coord, TExecPlanFragmentParams* rpc_params) {
  rpc_params->__set_protocol_version(ImpalaInternalServiceVersion::V1);
  rpc_params->__set_fragment(fragment);
  // Remove filters that weren't selected during filter routing table construction.
  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    for (TPlanNode& plan_node: rpc_params->fragment.plan.nodes) {
      if (plan_node.__isset.runtime_filters) {
        vector<TRuntimeFilterDesc> required_filters;
        for (const TRuntimeFilterDesc& desc: plan_node.runtime_filters) {
          FilterRoutingTable::iterator filter_it =
              filter_routing_table_.find(desc.filter_id);
          if (filter_it == filter_routing_table_.end()) continue;
          FilterState* f = &filter_it->second;
          if (plan_node.__isset.hash_join_node) {
            if (f->src_fragment_instance_idxs.find(fragment_instance_idx) ==
                f->src_fragment_instance_idxs.end()) {
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
  SetExecPlanDescriptorTable(fragment, rpc_params);

  TNetworkAddress exec_host = params.hosts[per_fragment_instance_idx];
  if (schedule.HasReservation()) {
    // The reservation has already have been validated at this point.
    TNetworkAddress resource_hostport;
    schedule.GetResourceHostport(exec_host, &resource_hostport);
    map<TNetworkAddress, llama::TAllocatedResource>::const_iterator it =
        schedule.reservation()->allocated_resources.find(resource_hostport);
    // Only set reserved resource if we actually have one for this plan
    // fragment. Otherwise, don't set it (usually this the coordinator fragment), and it
    // won't participate in dynamic RM controls.
    if (it != schedule.reservation()->allocated_resources.end()) {
      rpc_params->__set_reserved_resource(it->second);
      rpc_params->__set_local_resource_address(resource_hostport);
    }
  }
  rpc_params->params.__set_request_pool(schedule.request_pool());
  FragmentScanRangeAssignment::const_iterator it =
      params.scan_range_assignment.find(exec_host);
  // Scan ranges may not always be set, so use an empty structure if so.
  const PerNodeScanRanges& scan_ranges =
      (it != params.scan_range_assignment.end()) ? it->second : PerNodeScanRanges();

  rpc_params->params.__set_per_node_scan_ranges(scan_ranges);
  rpc_params->params.__set_per_exch_num_senders(params.per_exch_num_senders);
  rpc_params->params.__set_destinations(params.destinations);
  rpc_params->params.__set_sender_id(params.sender_id_base + per_fragment_instance_idx);
  rpc_params->__isset.params = true;
  rpc_params->fragment_instance_ctx.__set_query_ctx(query_ctx_);
  rpc_params->fragment_instance_ctx.fragment_instance_id =
      params.instance_ids[per_fragment_instance_idx];
  rpc_params->fragment_instance_ctx.per_fragment_instance_idx = per_fragment_instance_idx;
  rpc_params->fragment_instance_ctx.num_fragment_instances = params.instance_ids.size();
  rpc_params->fragment_instance_ctx.fragment_instance_idx = fragment_instance_idx;
  rpc_params->__isset.fragment_instance_ctx = true;
}

void Coordinator::SetExecPlanDescriptorTable(const TPlanFragment& fragment,
    TExecPlanFragmentParams* rpc_params) {
  TDescriptorTable thrift_desc_tbl;

  // Always add the Tuple and Slot descriptors.
  thrift_desc_tbl.__set_tupleDescriptors(desc_tbl_.tupleDescriptors);
  thrift_desc_tbl.__set_slotDescriptors(desc_tbl_.slotDescriptors);

  // Collect the TTupleId(s) for ScanNode(s).
  unordered_set<TTupleId> tuple_ids;
  BOOST_FOREACH(const TPlanNode& plan_node, fragment.plan.nodes) {
    // TODO This should change if there is a ScanNode for Kudu.
    switch (plan_node.node_type) {
      case TPlanNodeType::HDFS_SCAN_NODE:
        tuple_ids.insert(plan_node.hdfs_scan_node.tuple_id);
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
  BOOST_FOREACH(const TTupleId& tuple_id, tuple_ids) {
    BOOST_FOREACH(const TTupleDescriptor& tuple_desc, desc_tbl_.tupleDescriptors) {
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
  BOOST_FOREACH(const TTableDescriptor& table_desc, desc_tbl_.tableDescriptors) {
    if (table_ids.find(table_desc.id) == table_ids.end()) continue;
    thrift_desc_tbl.tableDescriptors.push_back(table_desc);
    thrift_desc_tbl.__isset.tableDescriptors = true;
  }

  rpc_params->__set_desc_tbl(thrift_desc_tbl);
}
namespace {

void DistributeFilters(shared_ptr<TPublishFilterParams> params, TNetworkAddress impalad,
    TUniqueId fragment_instance_id) {
  Status status;
  ImpalaInternalServiceConnection backend_client(
      ExecEnv::GetInstance()->impalad_client_cache(), impalad, &status);
  if (!status.ok()) return;
  // Make a local copy of the shared 'master' set of parameters
  TPublishFilterParams local_params(*params);
  local_params.dst_instance_id = fragment_instance_id;
  local_params.__set_bloom_filter(params->bloom_filter);
  TPublishFilterResult res;
  backend_client.DoRpc(&ImpalaInternalServiceClient::PublishFilter, local_params, &res);
};

}

void Coordinator::UpdateFilter(const TUpdateFilterParams& params) {
  DCHECK(exec_complete_barrier_.get() != NULL)
      << "Filters received before fragments started!";
  exec_complete_barrier_->Wait();
  DCHECK(filter_routing_table_complete_)
      << "Filter received before routing table complete";

  // Make a 'master' copy that will be shared by all concurrent delivery RPC attempts.
  shared_ptr<TPublishFilterParams> rpc_params(new TPublishFilterParams());
  unordered_set<int32_t> target_fragment_instance_idxs;
  unique_ptr<BloomFilter> bloom_filter(new BloomFilter(params.bloom_filter, NULL, NULL));
  {
    lock_guard<SpinLock> l(filter_lock_);
    FilterRoutingTable::iterator it = filter_routing_table_.find(params.filter_id);
    if (it == filter_routing_table_.end()) {
      LOG(INFO) << "Could not find filter with id: " << params.filter_id;
      return;
    }
    FilterState* state = &it->second;
    DCHECK(!state->desc.has_local_target)
        << "Coordinator received filter that has local target";

    // Receiving unnecessary updates for a broadcast.
    if (state->pending_count == 0) {
      DCHECK(state->desc.is_broadcast_join)
          << "Received more updates than expected for partition filter: "
          << params.filter_id;
      return;
    }
    if (state->first_arrival_time == 0L) {
      state->first_arrival_time = query_events_->ElapsedTime();
    }
    --state->pending_count;

    if (filter_updates_received_->value() == 0) {
      query_events_->MarkEvent("First dynamic filter received");
    }
    filter_updates_received_->Add(1);
    if (state->bloom_filter == NULL) {
      state->bloom_filter =
          obj_pool()->Add(bloom_filter.release());
    } else {
      // TODO: Implement BloomFilter::Or(const ThriftBloomFilter&)
      state->bloom_filter->Or(*bloom_filter);
    }

    if (state->pending_count > 0) return;
    // No more filters are pending on this filter ID. Create a distribution payload and
    // offer it to the queue.
    state->completion_time = query_events_->ElapsedTime();
    target_fragment_instance_idxs = state->target_fragment_instance_idxs;
    state->bloom_filter->ToThrift(&rpc_params->bloom_filter);
  }

  rpc_params->filter_id = params.filter_id;

  for (int idx: target_fragment_instance_idxs) {
    FragmentInstanceState* fragment_inst = fragment_instance_states_[idx];
    DCHECK(fragment_inst != NULL) << "Missing fragment instance: " << idx;

    exec_env_->rpc_pool()->Offer(bind<void>(DistributeFilters, rpc_params,
        fragment_inst->impalad_address(), fragment_inst->fragment_instance_id()));
  }
}

}
