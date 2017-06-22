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

#include "runtime/coordinator-backend-state.h"

#include <sstream>
#include <string>
#include <boost/lexical_cast.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/lock_guard.hpp>
#include <boost/accumulators/accumulators.hpp>

#include "common/object-pool.h"
#include "exec/exec-node.h"
#include "exec/scan-node.h"
#include "scheduling/query-schedule.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/debug-options.h"
#include "runtime/client-cache.h"
#include "runtime/client-cache-types.h"
#include "runtime/backend-client.h"
#include "runtime/coordinator-filter-state.h"
#include "util/error-util.h"
#include "util/uid-util.h"
#include "util/network-util.h"
#include "util/counting-barrier.h"
#include "util/progress-updater.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/ImpalaInternalService_constants.h"

#include "common/names.h"

using namespace impala;
namespace accumulators = boost::accumulators;

Coordinator::BackendState::BackendState(
    const TUniqueId& query_id, int state_idx, TRuntimeFilterMode::type filter_mode)
  : query_id_(query_id),
    state_idx_(state_idx),
    filter_mode_(filter_mode),
    rpc_latency_(0),
    rpc_sent_(false),
    peak_consumption_(0L) {
}

void Coordinator::BackendState::Init(
    const vector<const FInstanceExecParams*>& instance_params_list,
    const vector<FragmentStats*>& fragment_stats, ObjectPool* obj_pool) {
  instance_params_list_ = instance_params_list;
  host_ = instance_params_list_[0]->host;
  num_remaining_instances_ = instance_params_list.size();

  // populate instance_stats_map_ and install instance
  // profiles as child profiles in fragment_stats' profile
  int prev_fragment_idx = -1;
  for (const FInstanceExecParams* instance_params: instance_params_list) {
    DCHECK_EQ(host_, instance_params->host);  // all hosts must be the same
    int fragment_idx = instance_params->fragment().idx;
    DCHECK_LT(fragment_idx, fragment_stats.size());
    if (prev_fragment_idx != -1 && fragment_idx != prev_fragment_idx) {
      // all instances of a fragment are contiguous
      DCHECK_EQ(fragments_.count(fragment_idx), 0);
      prev_fragment_idx = fragment_idx;
    }
    fragments_.insert(fragment_idx);

    instance_stats_map_.emplace(
        GetInstanceIdx(instance_params->instance_id),
        obj_pool->Add(
          new InstanceStats(*instance_params, fragment_stats[fragment_idx], obj_pool)));
  }
}

void Coordinator::BackendState::SetRpcParams(
    const DebugOptions& debug_options, const FilterRoutingTable& filter_routing_table,
    TExecQueryFInstancesParams* rpc_params) {
  rpc_params->__set_protocol_version(ImpalaInternalServiceVersion::V1);
  rpc_params->__set_coord_state_idx(state_idx_);

  // set fragment_ctxs and fragment_instance_ctxs
  rpc_params->fragment_instance_ctxs.resize(instance_params_list_.size());
  for (int i = 0; i < instance_params_list_.size(); ++i) {
    TPlanFragmentInstanceCtx& instance_ctx = rpc_params->fragment_instance_ctxs[i];
    const FInstanceExecParams& params = *instance_params_list_[i];
    int fragment_idx = params.fragment_exec_params.fragment.idx;

    // add a TPlanFragmentCtx, if we don't already have it
    if (rpc_params->fragment_ctxs.empty()
        || rpc_params->fragment_ctxs.back().fragment.idx != fragment_idx) {
      rpc_params->fragment_ctxs.emplace_back();
      TPlanFragmentCtx& fragment_ctx = rpc_params->fragment_ctxs.back();
      fragment_ctx.__set_fragment(params.fragment_exec_params.fragment);
      fragment_ctx.__set_destinations(params.fragment_exec_params.destinations);
    }

    instance_ctx.fragment_idx = fragment_idx;
    instance_ctx.fragment_instance_id = params.instance_id;
    instance_ctx.per_fragment_instance_idx = params.per_fragment_instance_idx;
    instance_ctx.__set_per_node_scan_ranges(params.per_node_scan_ranges);
    instance_ctx.__set_per_exch_num_senders(
        params.fragment_exec_params.per_exch_num_senders);
    instance_ctx.__set_sender_id(params.sender_id);
    if (debug_options.node_id() != -1
        && (debug_options.instance_idx() == -1
            || debug_options.instance_idx() == GetInstanceIdx(params.instance_id))) {
      instance_ctx.__set_debug_options(debug_options.ToThrift());
    }

    if (filter_mode_ == TRuntimeFilterMode::OFF) continue;

    // Remove filters that weren't selected during filter routing table construction.
    // TODO: do this more efficiently, we're looping over the entire plan for each
    // instance separately
    DCHECK_EQ(rpc_params->query_ctx.client_request.query_options.mt_dop, 0);
    int instance_idx = GetInstanceIdx(params.instance_id);
    for (TPlanNode& plan_node: rpc_params->fragment_ctxs.back().fragment.plan.nodes) {
      if (!plan_node.__isset.hash_join_node) continue;
      if (!plan_node.__isset.runtime_filters) continue;

      vector<TRuntimeFilterDesc> required_filters;
      for (const TRuntimeFilterDesc& desc: plan_node.runtime_filters) {
        FilterRoutingTable::const_iterator filter_it =
            filter_routing_table.find(desc.filter_id);
        // filter was dropped in Coordinator::InitFilterRoutingTable()
        if (filter_it == filter_routing_table.end()) continue;
        const FilterState& f = filter_it->second;
        if (f.src_fragment_instance_idxs().find(instance_idx)
            == f.src_fragment_instance_idxs().end()) {
          DCHECK(desc.is_broadcast_join);
          continue;
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

void Coordinator::BackendState::Exec(
    const TQueryCtx& query_ctx, const DebugOptions& debug_options,
    const FilterRoutingTable& filter_routing_table,
    CountingBarrier* exec_complete_barrier) {
  NotifyBarrierOnExit notifier(exec_complete_barrier);
  TExecQueryFInstancesParams rpc_params;
  rpc_params.__set_query_ctx(query_ctx);
  SetRpcParams(debug_options, filter_routing_table, &rpc_params);
  VLOG_FILE << "making rpc: ExecQueryFInstances"
      << " host=" << impalad_address() << " query_id=" << PrintId(query_id_);

  // guard against concurrent UpdateBackendExecStatus() that may arrive after RPC returns
  lock_guard<mutex> l(lock_);
  int64_t start = MonotonicMillis();

  ImpalaBackendConnection backend_client(
      ExecEnv::GetInstance()->impalad_client_cache(), impalad_address(), &status_);
  if (!status_.ok()) return;

  TExecQueryFInstancesResult thrift_result;
  Status rpc_status = backend_client.DoRpc(
      &ImpalaBackendClient::ExecQueryFInstances, rpc_params, &thrift_result);
  rpc_sent_ = true;
  rpc_latency_ = MonotonicMillis() - start;

  const string ERR_TEMPLATE =
      "ExecQueryFInstances rpc query_id=$0 failed: $1";

  if (!rpc_status.ok()) {
    const string& err_msg =
        Substitute(ERR_TEMPLATE, PrintId(query_id_), rpc_status.msg().msg());
    VLOG_QUERY << err_msg;
    status_ = Status(err_msg);
    return;
  }

  Status exec_status = Status(thrift_result.status);
  if (!exec_status.ok()) {
    const string& err_msg = Substitute(ERR_TEMPLATE, PrintId(query_id_),
        exec_status.msg().GetFullMessageDetails());
    VLOG_QUERY << err_msg;
    status_ = Status(err_msg);
    return;
  }

  for (const auto& entry: instance_stats_map_) entry.second->stopwatch_.Start();
  VLOG_FILE << "rpc succeeded: ExecQueryFInstances query_id=" << PrintId(query_id_);
}

Status Coordinator::BackendState::GetStatus(TUniqueId* failed_instance_id) {
  lock_guard<mutex> l(lock_);
  if (!status_.ok() && failed_instance_id != nullptr) {
    *failed_instance_id = failed_instance_id_;
  }
  return status_;
}

int64_t Coordinator::BackendState::GetPeakConsumption() {
  lock_guard<mutex> l(lock_);
  return peak_consumption_;
}

void Coordinator::BackendState::MergeErrorLog(ErrorLogMap* merged) {
  lock_guard<mutex> l(lock_);
  if (error_log_.size() > 0)  MergeErrorMaps(error_log_, merged);
}

bool Coordinator::BackendState::IsDone() {
  lock_guard<mutex> l(lock_);
  return IsDoneInternal();
}

inline bool Coordinator::BackendState::IsDoneInternal() const {
  return num_remaining_instances_ == 0 || !status_.ok();
}

void Coordinator::BackendState::ApplyExecStatusReport(
    const TReportExecStatusParams& backend_exec_status, ExecSummary* exec_summary,
    ProgressUpdater* scan_range_progress, bool* done) {
  lock_guard<SpinLock> l1(exec_summary->lock);
  lock_guard<mutex> l2(lock_);
  for (const TFragmentInstanceExecStatus& instance_exec_status:
      backend_exec_status.instance_exec_status) {
    Status instance_status(instance_exec_status.status);
    int instance_idx = GetInstanceIdx(instance_exec_status.fragment_instance_id);
    DCHECK_EQ(instance_stats_map_.count(instance_idx), 1);
    InstanceStats* instance_stats = instance_stats_map_[instance_idx];
    DCHECK_EQ(instance_stats->exec_params_.instance_id,
        instance_exec_status.fragment_instance_id);
    // Ignore duplicate or out-of-order messages.
    if (instance_stats->done_) continue;
    if (instance_status.ok()) {
      instance_stats->Update(instance_exec_status, exec_summary, scan_range_progress);
      if (instance_stats->peak_mem_counter_ != nullptr) {
        // protect against out-of-order status updates
        peak_consumption_ =
            max(peak_consumption_, instance_stats->peak_mem_counter_->value());
      }
    } else {
      // if a query is aborted due to an error encountered by a single fragment instance,
      // all other fragment instances will report a cancelled status; make sure not
      // to mask the original error status
      if (status_.ok() || status_.IsCancelled()) {
        status_ = instance_status;
        failed_instance_id_ = instance_exec_status.fragment_instance_id;
      }
    }
    DCHECK_GT(num_remaining_instances_, 0);
    if (instance_exec_status.done) {
      DCHECK(!instance_stats->done_);
      instance_stats->done_ = true;
      --num_remaining_instances_;
    }

    // TODO: clean up the ReportQuerySummary() mess
    if (status_.ok()) {
      // We can't update this backend's profile if ReportQuerySummary() is running,
      // because it depends on all profiles not changing during its execution (when it
      // calls SortChildren()). ReportQuerySummary() only gets called after
      // WaitForBackendCompletion() returns or at the end of CancelFragmentInstances().
      // WaitForBackendCompletion() only returns after all backends have completed (in
      // which case we wouldn't be in this function), or when there's an error, in which
      // case CancelFragmentInstances() is called. CancelFragmentInstances sets all
      // exec_state's statuses to cancelled.
      // TODO: We're losing this profile information. Call ReportQuerySummary only after
      // all backends have completed.
    }
  }

  // Log messages aggregated by type
  if (backend_exec_status.__isset.error_log && backend_exec_status.error_log.size() > 0) {
    // Append the log messages from each update with the global state of the query
    // execution
    MergeErrorMaps(backend_exec_status.error_log, &error_log_);
    VLOG_FILE << "host=" << host_ << " error log: " << PrintErrorMapToString(error_log_);
  }

  *done = IsDoneInternal();
  // TODO: keep backend-wide stopwatch?
}

void Coordinator::BackendState::UpdateExecStats(
    const vector<FragmentStats*>& fragment_stats) {
  lock_guard<mutex> l(lock_);
  for (const auto& entry: instance_stats_map_) {
    const InstanceStats& instance_stats = *entry.second;
    int fragment_idx = instance_stats.exec_params_.fragment().idx;
    DCHECK_LT(fragment_idx, fragment_stats.size());
    FragmentStats* f = fragment_stats[fragment_idx];
    int64_t completion_time = instance_stats.stopwatch_.ElapsedTime();
    f->completion_times_(completion_time);
    if (completion_time > 0) {
      f->rates_(instance_stats.total_split_size_
        / (completion_time / 1000.0 / 1000.0 / 1000.0));
    }
    f->avg_profile_->UpdateAverage(instance_stats.profile_);
  }
}

bool Coordinator::BackendState::Cancel() {
  lock_guard<mutex> l(lock_);

  // Nothing to cancel if the exec rpc was not sent
  if (!rpc_sent_) return false;

  // don't cancel if it already finished (for any reason)
  if (IsDoneInternal()) return false;

  /// If the status is not OK, we still try to cancel - !OK status might mean
  /// communication failure between backend and coordinator, but fragment
  /// instances might still be running.

  // set an error status to make sure we only cancel this once
  if (status_.ok()) status_ = Status::CANCELLED;

  TCancelQueryFInstancesParams params;
  params.protocol_version = ImpalaInternalServiceVersion::V1;
  params.__set_query_id(query_id_);
  TCancelQueryFInstancesResult dummy;
  VLOG_QUERY << "sending CancelQueryFInstances rpc for query_id="
             << query_id_ << " backend=" << TNetworkAddressToString(impalad_address());

  Status rpc_status;
  Status client_status;
  // Try to send the RPC 3 times before failing.
  for (int i = 0; i < 3; ++i) {
    ImpalaBackendConnection backend_client(ExecEnv::GetInstance()->impalad_client_cache(),
        impalad_address(), &client_status);
    if (client_status.ok()) {
      // The return value 'dummy' is ignored as it's only set if the fragment instance
      // cannot be found in the backend. The fragment instances of a query can all be
      // cancelled locally in a backend due to RPC failure to coordinator. In which case,
      // the query state can be gone already.
      rpc_status = backend_client.DoRpc(
          &ImpalaBackendClient::CancelQueryFInstances, params, &dummy);
      if (rpc_status.ok()) break;
    }
  }
  if (!client_status.ok()) {
    status_.MergeStatus(client_status);
    VLOG_QUERY << "CancelQueryFInstances query_id= " << query_id_
               << " failed to connect to " << TNetworkAddressToString(impalad_address())
               << " :" << client_status.msg().msg();
    return true;
  }
  if (!rpc_status.ok()) {
    status_.MergeStatus(rpc_status);
    VLOG_QUERY << "CancelQueryFInstances query_id= " << query_id_
               << " rpc to " << TNetworkAddressToString(impalad_address())
               << " failed: " << rpc_status.msg().msg();
    return true;
  }
  return true;
}

void Coordinator::BackendState::PublishFilter(
    shared_ptr<TPublishFilterParams> rpc_params) {
  DCHECK_EQ(rpc_params->dst_query_id, query_id_);
  if (fragments_.count(rpc_params->dst_fragment_idx) == 0) return;
  Status status;
  ImpalaBackendConnection backend_client(
      ExecEnv::GetInstance()->impalad_client_cache(), host_, &status);
  if (!status.ok()) return;
  // Make a local copy of the shared 'master' set of parameters
  TPublishFilterParams local_params(*rpc_params);
  local_params.__set_bloom_filter(rpc_params->bloom_filter);
  TPublishFilterResult res;
  backend_client.DoRpc(&ImpalaBackendClient::PublishFilter, local_params, &res);
  // TODO: switch back to the following once we fix the lifecycle
  // problems of Coordinator
  //std::cref(fragment_inst->impalad_address()),
  //std::cref(fragment_inst->fragment_instance_id())));
}

Coordinator::BackendState::InstanceStats::InstanceStats(
    const FInstanceExecParams& exec_params, FragmentStats* fragment_stats,
    ObjectPool* obj_pool)
  : exec_params_(exec_params),
    profile_(nullptr),
    done_(false),
    profile_created_(false),
    total_split_size_(0),
    total_ranges_complete_(0) {
  const string& profile_name = Substitute("Instance $0 (host=$1)",
      PrintId(exec_params.instance_id), lexical_cast<string>(exec_params.host));
  profile_ = obj_pool->Add(new RuntimeProfile(obj_pool, profile_name));
  fragment_stats->root_profile()->AddChild(profile_);

  // add total split size to fragment_stats->bytes_assigned()
  for (const PerNodeScanRanges::value_type& entry: exec_params_.per_node_scan_ranges) {
    for (const TScanRangeParams& scan_range_params: entry.second) {
      if (!scan_range_params.scan_range.__isset.hdfs_file_split) continue;
      total_split_size_ += scan_range_params.scan_range.hdfs_file_split.length;
    }
  }
  (*fragment_stats->bytes_assigned())(total_split_size_);
}

void Coordinator::BackendState::InstanceStats::InitCounters() {
  vector<RuntimeProfile*> children;
  profile_->GetAllChildren(&children);
  for (RuntimeProfile* p: children) {
    PlanNodeId id = ExecNode::GetNodeIdFromProfile(p);
    // This profile is not for an exec node.
    if (id == g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID) continue;

    RuntimeProfile::Counter* c =
        p->GetCounter(ScanNode::SCAN_RANGES_COMPLETE_COUNTER);
    if (c != nullptr) scan_ranges_complete_counters_.push_back(c);
  }

  peak_mem_counter_ =
      profile_->GetCounter(FragmentInstanceState::PER_HOST_PEAK_MEM_COUNTER);
}

void Coordinator::BackendState::InstanceStats::Update(
    const TFragmentInstanceExecStatus& exec_status,
    ExecSummary* exec_summary, ProgressUpdater* scan_range_progress) {
  DCHECK(Status(exec_status.status).ok());
  if (exec_status.done) stopwatch_.Stop();
  profile_->Update(exec_status.profile);
  if (!profile_created_) {
    profile_created_ = true;
    InitCounters();
  }
  profile_->ComputeTimeInProfile();

  // update exec_summary
  // TODO: why do this every time we get an updated instance profile?
  vector<RuntimeProfile*> children;
  profile_->GetAllChildren(&children);

  TExecSummary& thrift_exec_summary = exec_summary->thrift_exec_summary;
  for (RuntimeProfile* child: children) {
    int node_id = ExecNode::GetNodeIdFromProfile(child);
    if (node_id == -1) continue;

    // TODO: create plan_node_id_to_summary_map_
    TPlanNodeExecSummary& node_exec_summary =
        thrift_exec_summary.nodes[exec_summary->node_id_to_idx_map[node_id]];
    int per_fragment_instance_idx = exec_params_.per_fragment_instance_idx;
    DCHECK_LT(per_fragment_instance_idx, node_exec_summary.exec_stats.size())
        << " node_id=" << node_id << " instance_id=" << PrintId(exec_params_.instance_id)
        << " fragment_idx=" << exec_params_.fragment().idx;
    TExecStats& instance_stats =
        node_exec_summary.exec_stats[per_fragment_instance_idx];

    RuntimeProfile::Counter* rows_counter = child->GetCounter("RowsReturned");
    RuntimeProfile::Counter* mem_counter = child->GetCounter("PeakMemoryUsage");
    if (rows_counter != nullptr) instance_stats.__set_cardinality(rows_counter->value());
    if (mem_counter != nullptr) instance_stats.__set_memory_used(mem_counter->value());
    instance_stats.__set_latency_ns(child->local_time());
    // TODO: track interesting per-node metrics
    node_exec_summary.__isset.exec_stats = true;
  }

  // determine newly-completed scan ranges and update scan_range_progress
  int64_t total = 0;
  for (RuntimeProfile::Counter* c: scan_ranges_complete_counters_) total += c->value();
  int64_t delta = total - total_ranges_complete_;
  total_ranges_complete_ = total;
  scan_range_progress->Update(delta);
}

Coordinator::FragmentStats::FragmentStats(const string& avg_profile_name,
    const string& root_profile_name, int num_instances, ObjectPool* obj_pool)
  : avg_profile_(
      obj_pool->Add(new RuntimeProfile(obj_pool, avg_profile_name, true))),
    root_profile_(
      obj_pool->Add(new RuntimeProfile(obj_pool, root_profile_name))),
    num_instances_(num_instances) {
}

void Coordinator::FragmentStats::AddSplitStats() {
  double min = accumulators::min(bytes_assigned_);
  double max = accumulators::max(bytes_assigned_);
  double mean = accumulators::mean(bytes_assigned_);
  double stddev = sqrt(accumulators::variance(bytes_assigned_));
  stringstream ss;
  ss << " min: " << PrettyPrinter::Print(min, TUnit::BYTES)
    << ", max: " << PrettyPrinter::Print(max, TUnit::BYTES)
    << ", avg: " << PrettyPrinter::Print(mean, TUnit::BYTES)
    << ", stddev: " << PrettyPrinter::Print(stddev, TUnit::BYTES);
  avg_profile_->AddInfoString("split sizes", ss.str());
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

void Coordinator::FragmentStats::AddExecStats() {
  InstanceComparator comparator;
  root_profile_->SortChildren(comparator);

  stringstream times_label;
  times_label
    << "min:" << PrettyPrinter::Print(
        accumulators::min(completion_times_), TUnit::TIME_NS)
    << "  max:" << PrettyPrinter::Print(
        accumulators::max(completion_times_), TUnit::TIME_NS)
    << "  mean: " << PrettyPrinter::Print(
        accumulators::mean(completion_times_), TUnit::TIME_NS)
    << "  stddev:" << PrettyPrinter::Print(
        sqrt(accumulators::variance(completion_times_)), TUnit::TIME_NS);

  stringstream rates_label;
  rates_label
    << "min:" << PrettyPrinter::Print(
        accumulators::min(rates_), TUnit::BYTES_PER_SECOND)
    << "  max:" << PrettyPrinter::Print(
        accumulators::max(rates_), TUnit::BYTES_PER_SECOND)
    << "  mean:" << PrettyPrinter::Print(
        accumulators::mean(rates_), TUnit::BYTES_PER_SECOND)
    << "  stddev:" << PrettyPrinter::Print(
        sqrt(accumulators::variance(rates_)), TUnit::BYTES_PER_SECOND);

  // why plural?
  avg_profile_->AddInfoString("completion times", times_label.str());
  // why plural?
  avg_profile_->AddInfoString("execution rates", rates_label.str());
  avg_profile_->AddInfoString("num instances", lexical_cast<string>(num_instances_));
}
