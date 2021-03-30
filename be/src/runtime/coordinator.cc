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

#include <cerrno>
#include <unordered_set>

#include <thrift/protocol/TDebugProtocol.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

#include "common/hdfs.h"
#include "exec/data-sink.h"
#include "exec/plan-root-sink.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "kudu/rpc/rpc_context.h"
#include "kudu/rpc/rpc_sidecar.h"
#include "runtime/coordinator-backend-state.h"
#include "runtime/coordinator-filter-state.h"
#include "runtime/debug-options.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/query-driver.h"
#include "runtime/query-state.h"
#include "scheduling/admission-controller.h"
#include "scheduling/query-schedule.h"
#include "scheduling/scheduler.h"
#include "service/client-request-state.h"
#include "util/bloom-filter.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/kudu-status-util.h"
#include "util/min-max-filter.h"
#include "util/pretty-printer.h"
#include "util/table-printer.h"
#include "util/uid-util.h"

#include "common/names.h"

using kudu::rpc::RpcContext;
using kudu::rpc::RpcController;
using kudu::rpc::RpcSidecar;
using namespace apache::thrift;
using namespace rapidjson;
using boost::algorithm::iequals;
using boost::algorithm::is_any_of;
using boost::algorithm::join;
using boost::algorithm::token_compress_on;
using boost::algorithm::split;
using boost::filesystem::path;

DECLARE_string(hostname);

using namespace impala;

PROFILE_DEFINE_COUNTER(NumBackends, STABLE_HIGH, TUnit::UNIT,
    "Number of backends running this query.");
PROFILE_DEFINE_COUNTER(TotalBytesRead, STABLE_HIGH, TUnit::BYTES,
    "Total number of bytes read by a query.");
PROFILE_DEFINE_COUNTER(TotalCpuTime, STABLE_HIGH, TUnit::TIME_NS,
    "Total CPU time (user + system) consumed by a query.");
PROFILE_DEFINE_COUNTER(FiltersReceived,STABLE_LOW, TUnit::UNIT,
    "Total number of filter updates received (always 0 if filter mode is not "
    "GLOBAL). Excludes repeated broadcast filter updates.");
PROFILE_DEFINE_COUNTER(NumFragments, STABLE_HIGH, TUnit::UNIT,
    "Number of fragments in the plan of a query.");
PROFILE_DEFINE_COUNTER(NumFragmentInstances, STABLE_HIGH, TUnit::UNIT,
     "Number of fragment instances executed by a query.");
PROFILE_DEFINE_COUNTER(TotalBytesSent, STABLE_LOW, TUnit::BYTES,"The total number"
    " of bytes sent (across the network) by this query in exchange nodes. Does not "
    "include remote reads, data written to disk, or data sent to the client.");
PROFILE_DEFINE_COUNTER(TotalScanBytesSent, STABLE_LOW, TUnit::BYTES,
    "The total number of bytes sent (across the network) by fragment instances that "
    "had a scan node in their plan.");
PROFILE_DEFINE_COUNTER(TotalInnerBytesSent, STABLE_LOW, TUnit::BYTES, "The total "
    "number of bytes sent (across the network) by fragment instances that did not have a"
    " scan node in their plan i.e. that received their input data from other instances"
    " through exchange node.");
PROFILE_DEFINE_COUNTER(ExchangeScanRatio, STABLE_LOW, TUnit::DOUBLE_VALUE,
    "The ratio between TotalScanByteSent and TotalBytesRead, i.e. the selectivity over "
    "all fragment instances that had a scan node in their plan.");
PROFILE_DEFINE_COUNTER(InnerNodeSelectivityRatio, STABLE_LOW, TUnit::DOUBLE_VALUE,
    "The ratio between bytes sent by instances with a scan node in their plan and "
    "instances without a scan node in their plan. This indicates how well the inner "
    "nodes of the execution plan reduced the data volume.");
PROFILE_DEFINE_COUNTER(NumCompletedBackends, STABLE_HIGH, TUnit::UNIT,"The number of "
    "completed backends. Only valid after all backends have started executing. "
    "Does not count the number of CANCELLED Backends.");
PROFILE_DEFINE_TIMER(FinalizationTimer, STABLE_LOW,
    "Total time spent in finalization (typically 0 except for INSERT into hdfs tables).");

// Maximum number of fragment instances that can publish each broadcast filter.
static const int MAX_BROADCAST_FILTER_PRODUCERS = 3;

Coordinator::Coordinator(ClientRequestState* parent, const QuerySchedule& schedule,
    RuntimeProfile::EventSequence* events)
  : parent_query_driver_(parent->parent_driver()),
    parent_request_state_(parent),
    schedule_(schedule),
    filter_mode_(schedule.query_options().runtime_filter_mode),
    obj_pool_(new ObjectPool()),
    query_events_(events),
    exec_rpcs_status_barrier_(schedule_.per_backend_exec_params().size()),
    backend_released_barrier_(schedule_.per_backend_exec_params().size()),
    filter_routing_table_(new FilterRoutingTable) {}

Coordinator::~Coordinator() {
  // Must have entered a terminal exec state guaranteeing resources were released.
  DCHECK(!IsExecuting());
  DCHECK_LE(backend_exec_complete_barrier_->pending(), 0);
  // Release the coordinator's reference to the query control structures.
  if (query_state_ != nullptr) {
    ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
  }
}

Status Coordinator::Exec() {
  const TQueryExecRequest& request = schedule_.request();
  DCHECK(request.plan_exec_info.size() > 0);

  VLOG_QUERY << "Exec() query_id=" << PrintId(query_id())
             << " stmt=" << request.query_ctx.client_request.stmt;
  stmt_type_ = request.stmt_type;

  query_profile_ =
      RuntimeProfile::Create(obj_pool(), "Execution Profile " + PrintId(query_id()));
  finalization_timer_ = PROFILE_FinalizationTimer.Instantiate(query_profile_);
  filter_updates_received_ = PROFILE_FiltersReceived.Instantiate(query_profile_);

  host_profiles_ = RuntimeProfile::Create(obj_pool(), "Per Node Profiles");
  query_profile_->AddChild(host_profiles_);

  SCOPED_TIMER(query_profile_->total_time_counter());

  // initialize progress updater
  const string& str = Substitute("Query $0", PrintId(query_id()));
  progress_.Init(str, schedule_.num_scan_ranges());

  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->CreateQueryState(
      query_ctx(), schedule_.coord_backend_mem_limit());
  filter_mem_tracker_ = query_state_->obj_pool()->Add(new MemTracker(
      -1, "Runtime Filter (Coordinator)", query_state_->query_mem_tracker(), false));

  InitFragmentStats();
  // create BackendStates and per-instance state, including profiles, and install
  // the latter in the FragmentStats' root profile
  InitBackendStates();
  exec_summary_.Init(schedule_);

  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    // Populate the runtime filter routing table. This should happen before starting the
    // fragment instances. This code anticipates the indices of the instance states
    // created later on in ExecRemoteFragment()
    InitFilterRoutingTable();
  }

  // At this point, all static setup is done and all structures are initialized. Only
  // runtime-related state changes past this point (examples: fragment instance
  // profiles, etc.)

  RETURN_IF_ERROR(StartBackendExec());
  RETURN_IF_ERROR(FinishBackendStartup());

  // set coord_instance_ and coord_sink_
  if (schedule_.GetCoordFragment() != nullptr) {
    // this blocks until all fragment instances have finished their Prepare phase
    Status query_status = query_state_->GetFInstanceState(query_id(), &coord_instance_);
    if (!query_status.ok()) return UpdateExecState(query_status, nullptr, FLAGS_hostname);
    // We expected this query to have a coordinator instance.
    DCHECK(coord_instance_ != nullptr);
    // When GetFInstanceState() returns the coordinator instance, the Prepare phase is
    // done and the FragmentInstanceState's root sink will be set up.
    coord_sink_ = coord_instance_->GetRootSink();
    DCHECK(coord_sink_ != nullptr);
  }
  return Status::OK();
}

void Coordinator::InitFragmentStats() {
  vector<const TPlanFragment*> fragments;
  schedule_.GetTPlanFragments(&fragments);
  const TPlanFragment* coord_fragment = schedule_.GetCoordFragment();
  int64_t total_num_finstances = 0;

  for (const TPlanFragment* fragment: fragments) {
    string root_profile_name =
        Substitute(
          fragment == coord_fragment ? "Coordinator Fragment $0" : "Fragment $0",
          fragment->display_name);
    string avg_profile_name =
        Substitute("Averaged Fragment $0", fragment->display_name);
    int num_instances =
        schedule_.GetFragmentExecParams(fragment->idx).instance_exec_params.size();
    total_num_finstances += num_instances;
    // TODO: special-case the coordinator fragment?
    FragmentStats* fragment_stats = obj_pool()->Add(
        new FragmentStats(
          avg_profile_name, root_profile_name, num_instances, obj_pool()));
    fragment_stats_.push_back(fragment_stats);
    query_profile_->AddChild(fragment_stats->avg_profile(), true);
    query_profile_->AddChild(fragment_stats->root_profile());
  }
  COUNTER_SET(PROFILE_NumFragments.Instantiate(query_profile_),
      static_cast<int64_t>(fragments.size()));
  COUNTER_SET(PROFILE_NumFragmentInstances.Instantiate(query_profile_),
      total_num_finstances);
}

void Coordinator::InitBackendStates() {
  int num_backends = schedule_.per_backend_exec_params().size();
  DCHECK_GT(num_backends, 0);

  lock_guard<SpinLock> l(backend_states_init_lock_);
  backend_states_.resize(num_backends);

  COUNTER_SET(PROFILE_NumBackends.Instantiate(query_profile_), num_backends);

  // create BackendStates
  int backend_idx = 0;
  for (const auto& entry : schedule_.per_backend_exec_params()) {
    BackendState* backend_state = obj_pool()->Add(new BackendState(
        schedule_, query_ctx(), backend_idx, filter_mode_, entry.second));
    backend_state->Init(fragment_stats_, host_profiles_, obj_pool());
    backend_states_[backend_idx++] = backend_state;
    // was_inserted is true if the pair was successfully inserted into the map, false
    // otherwise.
    bool was_inserted = addr_to_backend_state_
                            .emplace(backend_state->krpc_impalad_address(), backend_state)
                            .second;
    if (UNLIKELY(!was_inserted)) {
      DCHECK(false) << "Network address " << backend_state->krpc_impalad_address()
                    << " associated with multiple BackendStates";
    }
  }
  backend_resource_state_ =
      obj_pool()->Add(new BackendResourceState(backend_states_, schedule_));
  num_completed_backends_ = PROFILE_NumCompletedBackends.Instantiate(query_profile_);
}

void Coordinator::ExecSummary::Init(const QuerySchedule& schedule) {
  const TQueryExecRequest& request = schedule.request();
  // init exec_summary_.{nodes, exch_to_sender_map}
  thrift_exec_summary.__isset.nodes = true;
  DCHECK(thrift_exec_summary.nodes.empty());
  for (const TPlanExecInfo& plan_exec_info: request.plan_exec_info) {
    for (const TPlanFragment& fragment: plan_exec_info.fragments) {
      if (!fragment.__isset.plan) continue;

      // eventual index of fragment's root node in exec_summary_.nodes
      int root_node_idx = thrift_exec_summary.nodes.size();

      const TPlan& plan = fragment.plan;
      const TDataSink& output_sink = fragment.output_sink;
      // Count the number of hosts and instances.
      const vector<FInstanceExecParams>& instance_params =
          schedule.GetFragmentExecParams(fragment.idx).instance_exec_params;
      unordered_set<TNetworkAddress> host_set;
      for (const FInstanceExecParams& instance: instance_params) {
        host_set.insert(instance.host);
      }
      int num_hosts = host_set.size();
      int num_instances = instance_params.size();

      // Add the data sink at the root of the fragment.
      data_sink_id_to_idx_map[fragment.idx] = thrift_exec_summary.nodes.size();
      thrift_exec_summary.nodes.emplace_back();
      // Note that some clients like impala-shell depend on many of these fields being
      // set, even if they are optional in the thrift.
      TPlanNodeExecSummary& node_summary = thrift_exec_summary.nodes.back();
      node_summary.__set_node_id(-1);
      node_summary.__set_fragment_idx(fragment.idx);
      node_summary.__set_label(output_sink.label);
      node_summary.__set_label_detail("");
      node_summary.__set_num_children(1);
      DCHECK(output_sink.__isset.estimated_stats);
      node_summary.__set_estimated_stats(output_sink.estimated_stats);
      node_summary.__set_num_hosts(num_hosts);
      node_summary.exec_stats.resize(num_instances);

      // We don't track rows returned from sinks, but some clients like impala-shell
      // expect it to be set in the thrift struct. Set it to -1 for compatibility
      // with those tools.
      node_summary.estimated_stats.__set_cardinality(-1);
      for (TExecStats& instance_stats : node_summary.exec_stats) {
        instance_stats.__set_cardinality(-1);
      }

      for (const TPlanNode& node : plan.nodes) {
        node_id_to_idx_map[node.node_id] = thrift_exec_summary.nodes.size();
        thrift_exec_summary.nodes.emplace_back();
        TPlanNodeExecSummary& node_summary = thrift_exec_summary.nodes.back();
        node_summary.__set_node_id(node.node_id);
        node_summary.__set_fragment_idx(fragment.idx);
        node_summary.__set_label(node.label);
        node_summary.__set_label_detail(node.label_detail);
        node_summary.__set_num_children(node.num_children);
        DCHECK(node.__isset.estimated_stats);
        node_summary.__set_estimated_stats(node.estimated_stats);
        node_summary.__set_num_hosts(num_hosts);
        node_summary.exec_stats.resize(num_instances);
      }

      if (fragment.__isset.output_sink &&
          (fragment.output_sink.type == TDataSinkType::DATA_STREAM_SINK
           || IsJoinBuildSink(fragment.output_sink.type))) {
        int dst_node_idx;
        if (fragment.output_sink.type == TDataSinkType::DATA_STREAM_SINK) {
          const TDataStreamSink& sink = fragment.output_sink.stream_sink;
          dst_node_idx = node_id_to_idx_map[sink.dest_node_id];
          if (sink.output_partition.type == TPartitionType::UNPARTITIONED) {
            thrift_exec_summary.nodes[dst_node_idx].__set_is_broadcast(true);
          }
        } else {
          DCHECK(IsJoinBuildSink(fragment.output_sink.type));
          const TJoinBuildSink& sink = fragment.output_sink.join_build_sink;
          dst_node_idx = node_id_to_idx_map[sink.dest_node_id];
        }
        thrift_exec_summary.__isset.exch_to_sender_map = true;
        thrift_exec_summary.exch_to_sender_map[dst_node_idx] = root_node_idx;
      }
    }
  }
}

void Coordinator::InitFilterRoutingTable() {
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "InitFilterRoutingTable() called although runtime filters are disabled";
  DCHECK(!filter_routing_table_->is_complete)
      << "InitFilterRoutingTable() called after table marked as complete";

  lock_guard<shared_mutex> lock(filter_routing_table_->lock); // Exclusive lock.
  for (const FragmentExecParams& fragment_params: schedule_.fragment_exec_params()) {
    int num_instances = fragment_params.instance_exec_params.size();
    DCHECK_GT(num_instances, 0);
    int num_backends = fragment_params.GetNumBackends();
    DCHECK_GT(num_backends, 0);

    // Hash join build sinks can produce filters in mt_dop > 0 plans.
    if (fragment_params.fragment.output_sink.__isset.join_build_sink) {
      const TJoinBuildSink& join_sink =
          fragment_params.fragment.output_sink.join_build_sink;
      for (const TRuntimeFilterDesc& filter: join_sink.runtime_filters) {
        // The join node ID is used to identify the join that produces the filter, even
        // though the builder is separate from the actual node.
        DCHECK_EQ(filter.src_node_id, join_sink.dest_node_id);
        AddFilterSource(
            fragment_params, num_instances, num_backends, filter, filter.src_node_id);
      }
    }
    for (const TPlanNode& plan_node: fragment_params.fragment.plan.nodes) {
      if (!plan_node.__isset.runtime_filters) continue;
      for (const TRuntimeFilterDesc& filter: plan_node.runtime_filters) {
        DCHECK(filter_mode_ == TRuntimeFilterMode::GLOBAL || filter.has_local_targets);
        // Currently hash joins are the only filter sources. Otherwise it must be
        // a filter consumer.
        if (plan_node.__isset.join_node &&
            plan_node.join_node.__isset.hash_join_node) {
          AddFilterSource(
              fragment_params, num_instances, num_backends, filter, plan_node.node_id);
        } else if (plan_node.__isset.hdfs_scan_node || plan_node.__isset.kudu_scan_node) {
          FilterState* f = filter_routing_table_->GetOrCreateFilterState(filter);
          auto it = filter.planid_to_target_ndx.find(plan_node.node_id);
          DCHECK(it != filter.planid_to_target_ndx.end());
          const TRuntimeFilterTargetDesc& t_target = filter.targets[it->second];
          DCHECK(filter_mode_ == TRuntimeFilterMode::GLOBAL || t_target.is_local_target);
          f->targets()->emplace_back(t_target, fragment_params.fragment.idx);
        } else {
          DCHECK(false) << "Unexpected plan node with runtime filters: "
              << ThriftDebugString(plan_node);
        }
      }
    }
  }

  query_profile_->AddInfoString(
      "Number of filters", Substitute("$0", filter_routing_table_->num_filters()));
  query_profile_->AddInfoString("Filter routing table", FilterDebugString());
  if (VLOG_IS_ON(2)) VLOG_QUERY << FilterDebugString();
  filter_routing_table_->is_complete = true;
}

void Coordinator::AddFilterSource(const FragmentExecParams& src_fragment_params,
    int num_instances, int num_backends, const TRuntimeFilterDesc& filter,
    int join_node_id) {
  FilterState* f = filter_routing_table_->GetOrCreateFilterState(filter);
  // Set the 'pending_count_' to zero to indicate that for a filter with
  // local-only targets the coordinator does not expect to receive any filter
  // updates. We expect to receive a single aggregated filter from each backend
  // for partitioned joins.
  int pending_count = filter.is_broadcast_join
      ? (filter.has_remote_targets ? 1 : 0) : num_backends;
  f->set_pending_count(pending_count);

  // Determine which instances will produce the filters.
  // TODO: IMPALA-9333: having a shared RuntimeFilterBank between all fragments on
  // a backend allows further optimizations to reduce the number of broadcast join
  // filters sent over the network, by considering cross-fragment filters on
  // the same backend as local filters:
  // 1. Produce a local filter on any backend with a destination fragment.
  // 2. Only produce one local filter per backend (although, this would be made
  //    redundant by IMPALA-4224 - sharing broadcast join hash tables).
  // 3. Don't produce a global filter if all targets can be satisfied with
  //    local producers.
  // This work was deferred from the IMPALA-4400 change because it provides only
  // incremental performance benefits.
  vector<int> src_idxs = src_fragment_params.GetInstanceIdxs();

  // If this is a broadcast join with only non-local targets, build and publish it
  // on MAX_BROADCAST_FILTER_PRODUCERS instances. If this is not a broadcast join
  // or it is a broadcast join with local targets, it should be generated
  // everywhere the join is executed.
  if (filter.is_broadcast_join && !filter.has_local_targets
      && num_instances > MAX_BROADCAST_FILTER_PRODUCERS) {
    random_shuffle(src_idxs.begin(), src_idxs.end());
    src_idxs.resize(MAX_BROADCAST_FILTER_PRODUCERS);
  }
  for (int src_idx : src_idxs) {
    TRuntimeFilterSource filter_src;
    filter_src.src_node_id = join_node_id;
    filter_src.filter_id = filter.filter_id;
    filter_routing_table_->finstance_filters_produced[src_idx].emplace_back(
        filter_src);
  }
  f->set_num_producers(src_idxs.size());
}

void Coordinator::WaitOnExecRpcs() {
  if (exec_rpcs_complete_.Load()) return;
  for (BackendState* backend_state : backend_states_) {
    backend_state->WaitOnExecRpc();
  }
  exec_rpcs_complete_.Store(true);
}

Status Coordinator::StartBackendExec() {
  int num_backends = backend_states_.size();
  backend_exec_complete_barrier_.reset(new CountingBarrier(num_backends));

  DebugOptions debug_options(schedule_.query_options());

  VLOG_QUERY << "starting execution on " << num_backends << " backends for query_id="
             << PrintId(query_id());
  query_events_->MarkEvent(Substitute("Ready to start on $0 backends", num_backends));

  // Serialize the TQueryCtx once and pass it to each backend. The serialized buffer must
  // stay valid until WaitOnExecRpcs() has returned.
  ThriftSerializer serializer(true);
  uint8_t* serialized_buf = nullptr;
  uint32_t serialized_len = 0;
  Status serialize_status =
      serializer.SerializeToBuffer(&query_ctx(), &serialized_len, &serialized_buf);
  if (UNLIKELY(!serialize_status.ok())) {
    return UpdateExecState(serialize_status, nullptr, FLAGS_hostname);
  }
  kudu::Slice query_ctx_slice(serialized_buf, serialized_len);

  for (BackendState* backend_state: backend_states_) {
    if (exec_rpcs_status_barrier_.pending() <= 0) {
      // One of the backends has already indicated an error with Exec().
      break;
    }
    DebugActionNoFail(schedule_.query_options(), "COORD_BEFORE_EXEC_RPC");
    // Safe for ExecAsync() to read 'filter_routing_table_' because it is complete
    // at this point and won't be destroyed while this function is executing,
    // because it won't be torn down until WaitOnExecRpcs() has returned.
    DCHECK(filter_mode_ == TRuntimeFilterMode::OFF || filter_routing_table_->is_complete);
    backend_state->ExecAsync(debug_options, *filter_routing_table_, query_ctx_slice,
        &exec_rpcs_status_barrier_);
  }
  Status exec_rpc_status = exec_rpcs_status_barrier_.Wait();
  if (!exec_rpc_status.ok()) {
    // One of the backends failed to startup, so we cancel the other ones.
    CancelBackends(/*fire_and_forget=*/ true);
    WaitOnExecRpcs();
    vector<BackendState*> failed_backend_states;
    for (BackendState* backend_state : backend_states_) {
      // If Exec() rpc failed for a reason besides being aborted, blacklist the executor
      // and retry the query.
      if (!backend_state->exec_rpc_status().ok()
          && !backend_state->exec_rpc_status().IsAborted()) {
        failed_backend_states.push_back(backend_state);
        LOG(INFO) << "Blacklisting " << backend_state->impalad_address()
                  << " because an Exec() rpc to it failed.";
        const BackendDescriptorPB& be_desc = backend_state->exec_params()->be_desc;
        ExecEnv::GetInstance()->cluster_membership_mgr()->BlacklistExecutor(be_desc,
            FromKuduStatus(backend_state->exec_rpc_status(), "Exec() rpc failed"));
      }
    }
    if (!failed_backend_states.empty()) {
      HandleFailedExecRpcs(failed_backend_states);
    }
    VLOG_QUERY << "query startup cancelled due to a failed Exec() rpc: "
               << exec_rpc_status;
    return UpdateExecState(exec_rpc_status, nullptr, FLAGS_hostname);
  }

  WaitOnExecRpcs();
  VLOG_QUERY << "started execution on " << num_backends << " backends for query_id="
             << PrintId(query_id());
  query_events_->MarkEvent(
      Substitute("All $0 execution backends ($1 fragment instances) started",
        num_backends, schedule_.GetNumFragmentInstances()));
  return Status::OK();
}

Status Coordinator::FinishBackendStartup() {
  DCHECK(exec_rpcs_complete_.Load());
  const TMetricDef& def =
      MakeTMetricDef("backend-startup-latencies", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  // Capture up to 30 minutes of start-up times, in ms, with 4 s.f. accuracy.
  HistogramMetric latencies(def, 30 * 60 * 1000, 4);
  Status status = Status::OK();
  string error_hostname;
  string max_latency_host;
  int max_latency = 0;
  for (BackendState* backend_state: backend_states_) {
    // All of the Exec() rpcs must have completed successfully.
    DCHECK(backend_state->exec_rpc_status().ok());
    // preserve the first non-OK, if there is one
    Status backend_status = backend_state->GetStatus();
    if (!backend_status.ok() && status.ok()) {
      status = backend_status;
      error_hostname = backend_state->impalad_address().hostname();
    }
    if (backend_state->rpc_latency() > max_latency) {
      // Find the backend that takes the most time to acknowledge to
      // the ExecQueryFInstances() RPC.
      max_latency = backend_state->rpc_latency();
      max_latency_host = NetworkAddressPBToString(backend_state->impalad_address());
    }
    latencies.Update(backend_state->rpc_latency());
    // Mark backend complete if no fragment instances were assigned to it.
    if (backend_state->IsEmptyBackend()) {
      backend_exec_complete_barrier_->Notify();
      num_completed_backends_->Add(1);
    }
  }
  query_profile_->AddInfoString(
      "Backend startup latencies", latencies.ToHumanReadable());
  query_profile_->AddInfoString("Slowest backend to start up", max_latency_host);
  return UpdateExecState(status, nullptr, error_hostname);
}

string Coordinator::FilterDebugString() {
  TablePrinter table_printer;
  table_printer.AddColumn("ID", false);
  table_printer.AddColumn("Src. Node", false);
  table_printer.AddColumn("Tgt. Node(s)", false);
  table_printer.AddColumn("Target type", false);
  table_printer.AddColumn("Partition filter", false);
  // Distribution metrics are only meaningful if the coordinator is routing the filter.
  if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
    table_printer.AddColumn("Pending (Expected)", false);
    table_printer.AddColumn("First arrived", false);
    table_printer.AddColumn("Completed", false);
  }
  table_printer.AddColumn("Enabled", false);
  for (auto& v: filter_routing_table_->id_to_filter) {
    vector<string> row;
    const FilterState& state = v.second;
    row.push_back(lexical_cast<string>(v.first));
    row.push_back(lexical_cast<string>(state.desc().src_node_id));
    vector<string> target_ids;
    vector<string> target_types;
    vector<string> partition_filter;
    for (const FilterTarget& target: state.targets()) {
      target_ids.push_back(lexical_cast<string>(target.node_id));
      target_types.push_back(target.is_local ? "LOCAL" : "REMOTE");
      partition_filter.push_back(target.is_bound_by_partition_columns ? "true" : "false");
    }
    row.push_back(join(target_ids, ", "));
    row.push_back(join(target_types, ", "));
    row.push_back(join(partition_filter, ", "));

    if (filter_mode_ == TRuntimeFilterMode::GLOBAL) {
      int pending_count = state.completion_time() != 0L ? 0 : state.pending_count();
      row.push_back(Substitute("$0 ($1)", pending_count, state.num_producers()));
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

    // In case of remote filter, we might intentionally disable the filter upon
    // completion to prevent further update. In such case, we should check if all filter
    // updates have been successfully received.
    row.push_back(state.enabled() || state.received_all_updates() ? "true" : "false");
    table_printer.AddRow(row);
  }
  // Add a line break, as in all contexts this is called we need to start a new line to
  // print it correctly.
  return Substitute("\n$0", table_printer.ToString());
}

const char* Coordinator::ExecStateToString(const ExecState state) {
  static const unordered_map<ExecState, const char *> exec_state_to_str{
    {ExecState::EXECUTING,        "EXECUTING"},
    {ExecState::RETURNED_RESULTS, "RETURNED_RESULTS"},
    {ExecState::CANCELLED,        "CANCELLED"},
    {ExecState::ERROR,            "ERROR"}};
  return exec_state_to_str.at(state);
}

Status Coordinator::SetNonErrorTerminalState(const ExecState state) {
  DCHECK(state == ExecState::RETURNED_RESULTS || state == ExecState::CANCELLED);
  Status ret_status;
  {
    lock_guard<SpinLock> l(exec_state_lock_);
    // May have already entered a terminal state, in which case nothing to do.
    if (!IsExecuting()) return exec_status_;
    DCHECK(exec_status_.ok()) << exec_status_;
    exec_state_.Store(state);
    if (state == ExecState::CANCELLED) exec_status_ = Status::CANCELLED;
    ret_status = exec_status_;
  }
  VLOG_QUERY << Substitute("ExecState: query id=$0 execution $1", PrintId(query_id()),
      state == ExecState::CANCELLED ? "cancelled" : "completed");
  HandleExecStateTransition(ExecState::EXECUTING, state);
  return ret_status;
}

Status Coordinator::UpdateExecState(const Status& status,
    const TUniqueId* failed_finst, const string& instance_hostname) {
  Status ret_status;
  ExecState old_state, new_state;
  {
    lock_guard<SpinLock> l(exec_state_lock_);
    old_state = exec_state_.Load();
    if (old_state == ExecState::EXECUTING) {
      DCHECK(exec_status_.ok()) << exec_status_;
      if (!status.ok()) {
        // Error while executing - go to ERROR state.
        exec_status_ = status;
        exec_state_.Store(ExecState::ERROR);
      }
    } else if (old_state == ExecState::RETURNED_RESULTS) {
      // Already returned all results. Leave exec status as ok, stay in this state.
      DCHECK(exec_status_.ok()) << exec_status_;
    } else if (old_state == ExecState::CANCELLED) {
      // Client requested cancellation already, stay in this state.  Ignores errors
      // after requested cancellations.
      DCHECK(exec_status_.IsCancelled()) << exec_status_;
    } else {
      // Already in the ERROR state, stay in this state but update status to be the
      // first non-cancelled status.
      DCHECK_EQ(old_state, ExecState::ERROR);
      DCHECK(!exec_status_.ok());
      if (!status.ok() && !status.IsCancelled() && exec_status_.IsCancelled()) {
        exec_status_ = status;
      }
    }
    new_state = exec_state_.Load();
    ret_status = exec_status_;
  }
  // Log interesting status: a non-cancelled error or a cancellation if was executing.
  if (!status.ok() && (!status.IsCancelled() || old_state == ExecState::EXECUTING)) {
    VLOG_QUERY << Substitute(
        "ExecState: query id=$0 finstance=$1 on host=$2 ($3 -> $4) status=$5",
        PrintId(query_id()), failed_finst != nullptr ? PrintId(*failed_finst) : "N/A",
        instance_hostname, ExecStateToString(old_state), ExecStateToString(new_state),
        status.GetDetail());
  }
  // After dropping the lock, apply the state transition (if any) side-effects.
  HandleExecStateTransition(old_state, new_state);
  return ret_status;
}

void Coordinator::HandleExecStateTransition(
    const ExecState old_state, const ExecState new_state) {
  static const unordered_map<ExecState, const char *> exec_state_to_event{
    {ExecState::EXECUTING,        "Executing"},
    {ExecState::RETURNED_RESULTS, "Last row fetched"},
    {ExecState::CANCELLED,        "Execution cancelled"},
    {ExecState::ERROR,            "Execution error"}};
  if (old_state == new_state) return;
  // Once we enter a terminal state, we stay there, guaranteeing this code runs only once.
  DCHECK_EQ(old_state, ExecState::EXECUTING);
  // Should never transition to the initial state.
  DCHECK_NE(new_state, ExecState::EXECUTING);
  // Can't transition until the exec RPCs are no longer in progress. Otherwise, a
  // cancel RPC could be missed, and resources freed before a backend has had a chance
  // to take a resource reference.
  DCHECK(exec_rpcs_complete_.Load()) << "exec rpcs not completed";

  query_events_->MarkEvent(exec_state_to_event.at(new_state));
  // This thread won the race to transitioning into a terminal state - terminate
  // execution and release resources.
  ReleaseExecResources();
  if (new_state == ExecState::RETURNED_RESULTS) {
    // Cancel all backends, but wait for the final status reports to be received so that
    // we have a complete profile for this successful query.
    CancelBackends(/*fire_and_forget=*/ false);
    WaitForBackends();
  } else {
    CancelBackends(/*fire_and_forget=*/ true);
  }
  ReleaseQueryAdmissionControlResources();
  // Once the query has released its admission control resources, update its end time.
  parent_request_state_->UpdateEndTime();
  // Can compute summary only after we stop accepting reports from the backends. Both
  // WaitForBackends() and CancelBackends() ensures that.
  // TODO: should move this off of the query execution path?
  ComputeQuerySummary();
  finalized_.Set(true);
}

Status Coordinator::FinalizeHdfsDml() {
  // All instances must have reported their final statuses before finalization, which is a
  // post-condition of Wait. If the query was not successful, still try to clean up the
  // staging directory.
  DCHECK(has_called_wait_.Load());
  DCHECK(finalize_params() != nullptr);
  bool is_transactional = finalize_params()->__isset.write_id;

  VLOG_QUERY << "Finalizing query: " << PrintId(query_id());
  SCOPED_TIMER(finalization_timer_);
  Status return_status = UpdateExecState(Status::OK(), nullptr, FLAGS_hostname);
  if (return_status.ok()) {
    HdfsTableDescriptor* hdfs_table;
    DCHECK(query_ctx().__isset.desc_tbl_serialized);
    RETURN_IF_ERROR(DescriptorTbl::CreateHdfsTblDescriptor(
            query_ctx().desc_tbl_serialized, finalize_params()->table_id, obj_pool(),
            &hdfs_table));
    DCHECK(hdfs_table != nullptr)
        << "INSERT target table not known in descriptor table: "
        << finalize_params()->table_id;
    // There is no need for finalization for transactional inserts.
    if (!is_transactional) {
      // 'write_id' is NOT set, therefore we need to do some finalization, e.g. moving
      // files or delete old files in case of INSERT OVERWRITE.
      return_status = dml_exec_state_.FinalizeHdfsInsert(*finalize_params(),
          query_ctx().client_request.query_options.s3_skip_insert_staging,
          hdfs_table, query_profile_);
    }
    hdfs_table->ReleaseResources();
  } else if (is_transactional) {
    parent_request_state_->AbortTransaction();
  }
  if (is_transactional) {
    DCHECK(!finalize_params()->__isset.staging_dir);
  } else {
    RETURN_IF_ERROR(DeleteQueryLevelStagingDir());
  }
  return return_status;
}

Status Coordinator::DeleteQueryLevelStagingDir() {
  stringstream staging_dir;
  DCHECK(finalize_params()->__isset.staging_dir);
  staging_dir << finalize_params()->staging_dir << "/" << PrintId(query_id(),"_") << "/";

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(staging_dir.str(), &hdfs_conn));
  VLOG_QUERY << "Removing staging directory: " << staging_dir.str();
  hdfsDelete(hdfs_conn, staging_dir.str().c_str(), 1);
  return Status::OK();
}

void Coordinator::WaitForBackends() {
  int32_t num_remaining = backend_exec_complete_barrier_->pending();
  if (num_remaining > 0) {
    VLOG_QUERY << "Coordinator waiting for backends to finish, " << num_remaining
               << " remaining. query_id=" << PrintId(query_id());
    backend_exec_complete_barrier_->Wait();
  }
}

Status Coordinator::Wait() {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  lock_guard<SpinLock> l(wait_lock_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (has_called_wait_.Load()) return Status::OK();
  has_called_wait_.Store(true);

  if (stmt_type_ == TStmtType::QUERY) {
    DCHECK(coord_instance_ != nullptr);
    return UpdateExecState(coord_instance_->WaitForOpen(),
        &coord_instance_->runtime_state()->fragment_instance_id(), FLAGS_hostname);
  }
  DCHECK_EQ(stmt_type_, TStmtType::DML);
  // DML finalization can only happen when all backends have completed all side-effects
  // and reported relevant state.
  WaitForBackends();
  if (finalize_params() != nullptr) {
    RETURN_IF_ERROR(UpdateExecState(
            FinalizeHdfsDml(), nullptr, FLAGS_hostname));
  }
  // DML requests are finished at this point.
  RETURN_IF_ERROR(SetNonErrorTerminalState(ExecState::RETURNED_RESULTS));
  query_profile_->AddInfoString(
      "DML Stats", dml_exec_state_.OutputPartitionStats("\n"));
  return Status::OK();
}

Status Coordinator::GetNext(QueryResultSet* results, int max_rows, bool* eos,
    int64_t block_on_wait_time_us) {
  VLOG_ROW << "GetNext() query_id=" << PrintId(query_id());
  DCHECK(has_called_wait_.Load());
  SCOPED_TIMER(query_profile_->total_time_counter());

  if (ReturnedAllResults()) {
    // Nothing left to do: already in a terminal state and no more results.
    *eos = true;
    return Status::OK();
  }
  DCHECK(coord_instance_ != nullptr) << "Exec() should be called first";
  DCHECK(coord_sink_ != nullptr)     << "Exec() should be called first";
  RuntimeState* runtime_state = coord_instance_->runtime_state();

  // If FETCH_ROWS_TIMEOUT_MS is 0, then the timeout passed to PlanRootSink::GetNext()
  // should be 0 as well so that the method waits for rows indefinitely.
  // If the first row has been fetched, then set the timeout to FETCH_ROWS_TIMEOUT_MS. If
  // the first row has not been fetched, then it is possible the client spent time
  // waiting for the query to 'finish' before issuing a GetNext() request.
  int64_t timeout_us;
  if (parent_request_state_->fetch_rows_timeout_us() == 0) {
    timeout_us = 0;
  } else {
    timeout_us = !first_row_fetched_ ?
        max(static_cast<int64_t>(1),
            parent_request_state_->fetch_rows_timeout_us() - block_on_wait_time_us) :
        parent_request_state_->fetch_rows_timeout_us();
  }

  Status status = coord_sink_->GetNext(runtime_state, results, max_rows, eos, timeout_us);
  if (!first_row_fetched_ && results->size() > 0) {
    query_events_->MarkEvent("First row fetched");
    first_row_fetched_ = true;
  }
  RETURN_IF_ERROR(UpdateExecState(
          status, &runtime_state->fragment_instance_id(), FLAGS_hostname));
  if (*eos) RETURN_IF_ERROR(SetNonErrorTerminalState(ExecState::RETURNED_RESULTS));
  return Status::OK();
}

void Coordinator::Cancel(bool wait_until_finalized) {
  // Illegal to call Cancel() before Exec() returns, so there's no danger of the cancel
  // RPC passing the exec RPC.
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  discard_result(SetNonErrorTerminalState(ExecState::CANCELLED));
  // CancelBackends() is called for all transitions into a terminal state.
  // RETURNED_RESULTS, however, calls it with fire_and_forget=false and may be blocked
  // waiting for cancellation. In that case, we want explicit cancellation to unblock
  // backend_exec_complete_barrier_, which we do by forcing cancellation.
  if (ReturnedAllResults()) CancelBackends(/*fire_and_forget=*/ true);

  // IMPALA-5756: Wait until finalized, in case a different thread was handling the
  // transition to the terminal state.
  if (wait_until_finalized) finalized_.Get();
}

void Coordinator::CancelBackends(bool fire_and_forget) {
  int num_cancelled = 0;
  for (BackendState* backend_state: backend_states_) {
    DCHECK(backend_state != nullptr);
    BackendState::CancelResult cr = backend_state->Cancel(fire_and_forget);
    if (cr.cancel_attempted) ++num_cancelled;
    if (!fire_and_forget && cr.became_done) backend_exec_complete_barrier_->Notify();
  }
  if (fire_and_forget) backend_exec_complete_barrier_->NotifyRemaining();
  VLOG_QUERY << Substitute(
      "CancelBackends() query_id=$0, tried to cancel $1 backends",
      PrintId(query_id()), num_cancelled);
}

Status Coordinator::UpdateBackendExecStatus(const ReportExecStatusRequestPB& request,
    const TRuntimeProfileForest& thrift_profiles) {
  const int32_t coord_state_idx = request.coord_state_idx();
  VLOG_FILE << "UpdateBackendExecStatus() query_id=" << PrintId(query_id())
            << " backend_idx=" << coord_state_idx;

  if (coord_state_idx >= backend_states_.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Unknown backend index $0 (max known: $1)",
            coord_state_idx, backend_states_.size() - 1));
  }
  BackendState* backend_state = backend_states_[coord_state_idx];

  if (thrift_profiles.__isset.host_profile) {
    backend_state->UpdateHostProfile(thrift_profiles.host_profile);
  }

  // Set by ApplyExecStatusReport, contains all the AuxErrorInfoPB objects in
  // ReportExecStatusRequestPB.
  vector<AuxErrorInfoPB> aux_error_info;

  if (backend_state->ApplyExecStatusReport(request, thrift_profiles, &exec_summary_,
          &progress_, &dml_exec_state_, &aux_error_info)) {
    // This backend execution has completed.
    if (VLOG_QUERY_IS_ON) {
      // Don't log backend completion if the query has already been cancelled.
      int pending_backends = backend_exec_complete_barrier_->pending();
      if (pending_backends >= 1) {
        VLOG_QUERY << "Backend completed:"
                   << " host=" << backend_state->impalad_address()
                   << " remaining=" << pending_backends
                   << " query_id=" << PrintId(query_id());
        BackendState::LogFirstInProgress(backend_states_);
      }
    }
    bool is_fragment_failure;
    TUniqueId failed_instance_id;
    Status status = backend_state->GetStatus(&is_fragment_failure, &failed_instance_id);

    // Iterate through all AuxErrorInfoPB objects, and use each one to possibly blacklist
    // any "faulty" nodes.
    Status retryable_status = UpdateBlacklistWithAuxErrorInfo(
        &aux_error_info, status, backend_state);

    // If any nodes were blacklisted, retry the query. This needs to be done before
    // UpdateExecState is called with the error status to avoid exposing the error to any
    // clients. If a retry is attempted, the ClientRequestState::query_status_ will be
    // set by TryQueryRetry, which prevents the error status from being exposed to any
    // clients.
    if (!retryable_status.ok()) {
      parent_query_driver_->TryQueryRetry(parent_request_state_, &retryable_status);
    }

    if (!status.ok()) {
      // We may start receiving status reports before all exec rpcs are complete.
      // Can't apply state transition until no more exec rpcs will be sent.
      // TODO(IMPALA-6788): we should stop issuing ExecQueryFInstance rpcs and cancel any
      // inflight when this happens.
      WaitOnExecRpcs();

      // Transition the status if we're not already in a terminal state. This won't block
      // because either this transitions to an ERROR state or the query is already in
      // a terminal state.
      // If both 'retryable_status' and 'status' are errors, prefer 'retryable_status' as
      // it includes 'status' as well as additional error log information from
      // UpdateBlacklistWithAuxErrorInfo.
      const Status& update_exec_state_status =
          !retryable_status.ok() ? retryable_status : status;
      discard_result(UpdateExecState(update_exec_state_status,
          is_fragment_failure ? &failed_instance_id : nullptr,
          NetworkAddressPBToString(backend_state->impalad_address())));
    }
    // We've applied all changes from the final status report - notify waiting threads.
    discard_result(backend_exec_complete_barrier_->Notify());

    // Mark backend_state as closed and release the backend_state's resources if
    // necessary.
    vector<BackendState*> releasable_backends;
    backend_resource_state_->MarkBackendFinished(backend_state, &releasable_backends);
    if (!releasable_backends.empty()) {
      ReleaseBackendAdmissionControlResources(releasable_backends);
      backend_resource_state_->BackendsReleased(releasable_backends);
      for (int i = 0; i < releasable_backends.size(); ++i) {
        backend_released_barrier_.Notify();
      }
    }
    num_completed_backends_->Add(1);
  } else {
    // Iterate through all AuxErrorInfoPB objects, and use each one to possibly blacklist
    // any "faulty" nodes.
    Status retryable_status = UpdateBlacklistWithAuxErrorInfo(
        &aux_error_info, Status::OK(), backend_state);

    // If any nodes were blacklisted, retry the query.
    if (!retryable_status.ok()) {
      parent_query_driver_->TryQueryRetry(parent_request_state_, &retryable_status);
    }
  }

  // If query execution has terminated, return a cancelled status to force the fragment
  // instance to stop executing.
  return IsExecuting() ? Status::OK() : Status::CANCELLED;
}

Status Coordinator::UpdateBlacklistWithAuxErrorInfo(
    vector<AuxErrorInfoPB>* aux_error_info, const Status& status,
    BackendState* backend_state) {
  // If the Backend failed due to a RPC failure, blacklist the destination node of
  // the failed RPC. Only blacklist one node per ReportExecStatusRequestPB to avoid
  // blacklisting nodes too aggressively. Currently, only blacklist the first node
  // that contains a valid RPCErrorInfoPB object.
  for (auto aux_error : *aux_error_info) {
    if (aux_error.has_rpc_error_info()) {
      RPCErrorInfoPB rpc_error_info = aux_error.rpc_error_info();
      DCHECK(rpc_error_info.has_dest_node());
      DCHECK(rpc_error_info.has_posix_error_code());
      const NetworkAddressPB& dest_node = rpc_error_info.dest_node();

      auto dest_node_and_be_state = addr_to_backend_state_.find(dest_node);

      // If the target address of the RPC is not known to the Coordinator, it cannot
      // be blacklisted.
      if (dest_node_and_be_state == addr_to_backend_state_.end()) {
        string err_msg = "Query failed due to a failed RPC to an unknown target address "
            + NetworkAddressPBToString(dest_node);
        DCHECK(false) << err_msg;
        LOG(ERROR) << err_msg;
        continue;
      }

      // The execution parameters of the destination node for the failed RPC.
      const BackendExecParams* dest_node_exec_params =
          dest_node_and_be_state->second->exec_params();

      // The Coordinator for the query should never be blacklisted.
      if (dest_node_exec_params->is_coord_backend) {
        VLOG_QUERY << "Query failed due to a failed RPC to the Coordinator";
        continue;
      }

      // A set of RPC related posix error codes that should cause the target node
      // of the failed RPC to be blacklisted.
      static const set<int32_t> blacklistable_rpc_error_codes = {
          ECONNRESET, // 104: Connection reset by peer
          ENOTCONN, // 107: Transport endpoint is not connected
          ESHUTDOWN, // 108: Cannot send after transport endpoint shutdown
          ECONNREFUSED // 111: Connection refused
      };

      // If the RPC error code matches any of the 'blacklistable' errors codes, blacklist
      // the target executor of the RPC and return.
      if (blacklistable_rpc_error_codes.find(rpc_error_info.posix_error_code())
          != blacklistable_rpc_error_codes.end()) {
        string src_node_addr =
            NetworkAddressPBToString(backend_state->krpc_impalad_address());
        string dest_node_addr = NetworkAddressPBToString(dest_node);
        VLOG_QUERY << Substitute(
            "Blacklisting $0 because a RPC to it failed, query_id=$1", dest_node_addr,
            PrintId(query_id()));

        Status retryable_status = Status::Expected(
            Substitute("RPC from $0 to $1 failed", src_node_addr, dest_node_addr));
        retryable_status.MergeStatus(status);

        ExecEnv::GetInstance()->cluster_membership_mgr()->BlacklistExecutor(
            dest_node_exec_params->be_desc, retryable_status);

        // Only blacklist one node per report.
        return retryable_status;
      }
    }
  }
  return Status::OK();
}

void Coordinator::HandleFailedExecRpcs(vector<BackendState*> failed_backend_states) {
  DCHECK(!failed_backend_states.empty());

  // Create an error based on the Exec RPC failure Status
  vector<string> backend_addresses;
  for (BackendState* backend_state : failed_backend_states) {
    backend_addresses.push_back(
        NetworkAddressPBToString(backend_state->krpc_impalad_address()));
  }
  Status retryable_status = Status::Expected(
      Substitute("ExecFInstances RPC to $0 failed", join(backend_addresses, ",")));
  for (BackendState* backend_state : failed_backend_states) {
    retryable_status.MergeStatus(
        FromKuduStatus(backend_state->exec_rpc_status(), "Exec() rpc failed"));
  }

  // Retry the query
  parent_query_driver_->TryQueryRetry(parent_request_state_, &retryable_status);
}

int64_t Coordinator::GetMaxBackendStateLagMs(NetworkAddressPB* address) {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first.";
  DCHECK_GT(backend_states_.size(), 0);
  int64_t current_time = BackendState::GenerateReportTimestamp();
  int64_t min_last_report_time_ms = current_time;
  BackendState* min_state = nullptr;
  for (BackendState* backend_state : backend_states_) {
    if (backend_state->IsDone()) continue;
    int64_t last_report_time_ms = backend_state->last_report_time_ms();
    DCHECK_GT(last_report_time_ms, 0);
    if (last_report_time_ms < min_last_report_time_ms) {
      min_last_report_time_ms = last_report_time_ms;
      min_state = backend_state;
    }
  }
  if (min_state == nullptr) return 0;
  *address = min_state->krpc_impalad_address();
  return current_time - min_last_report_time_ms;
}

// TODO: add histogram/percentile
void Coordinator::ComputeQuerySummary() {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  // In this case, the query did not even get to start all fragment instances.
  // Some of the state that is used below might be uninitialized.  In this case,
  // the query has made so little progress, reporting a summary is not very useful.
  if (!has_called_wait_.Load()) return;

  if (backend_states_.empty()) return;
  // make sure fragment_stats_ are up-to-date
  for (BackendState* backend_state: backend_states_) {
    backend_state->UpdateExecStats(fragment_stats_);
  }

  for (FragmentStats* fragment_stats: fragment_stats_) {
    fragment_stats->AddSplitStats();
    // TODO: output the split info string and detailed stats to VLOG_FILE again?
    fragment_stats->AddExecStats();
  }

  stringstream mem_info, cpu_user_info, cpu_system_info, bytes_read_info;
  ResourceUtilization total_utilization;
  for (BackendState* backend_state: backend_states_) {
    ResourceUtilization utilization = backend_state->ComputeResourceUtilization();
    total_utilization.Merge(utilization);
    string network_address = NetworkAddressPBToString(backend_state->impalad_address());
    mem_info << network_address << "("
             << PrettyPrinter::Print(utilization.peak_per_host_mem_consumption,
                 TUnit::BYTES) << ") ";
    bytes_read_info << network_address << "("
                    << PrettyPrinter::Print(utilization.bytes_read, TUnit::BYTES) << ") ";
    cpu_user_info << network_address << "("
                  << PrettyPrinter::Print(utilization.cpu_user_ns, TUnit::TIME_NS)
                  << ") ";
    cpu_system_info << network_address << "("
                    << PrettyPrinter::Print(utilization.cpu_sys_ns, TUnit::TIME_NS)
                    << ") ";
  }

  // The definitions of these counters are in the top of this file.
  COUNTER_SET(PROFILE_TotalBytesRead.Instantiate(query_profile_),
      total_utilization.bytes_read);
  COUNTER_SET(PROFILE_TotalCpuTime.Instantiate(query_profile_),
      total_utilization.cpu_user_ns + total_utilization.cpu_sys_ns);
  COUNTER_SET(PROFILE_TotalBytesSent.Instantiate(query_profile_),
      total_utilization.scan_bytes_sent + total_utilization.exchange_bytes_sent);
  COUNTER_SET(PROFILE_TotalScanBytesSent.Instantiate(query_profile_),
      total_utilization.scan_bytes_sent);
  COUNTER_SET(PROFILE_TotalInnerBytesSent.Instantiate(query_profile_),
      total_utilization.exchange_bytes_sent);

  double xchg_scan_ratio = 0;
  if (total_utilization.bytes_read > 0) {
    xchg_scan_ratio =
        (double)total_utilization.scan_bytes_sent / total_utilization.bytes_read;
  }
  COUNTER_SET(PROFILE_ExchangeScanRatio.Instantiate(query_profile_), xchg_scan_ratio);

  double inner_node_ratio = 0;
  if (total_utilization.scan_bytes_sent > 0) {
    inner_node_ratio =
        (double)total_utilization.exchange_bytes_sent / total_utilization.scan_bytes_sent;
  }
  COUNTER_SET(PROFILE_InnerNodeSelectivityRatio.Instantiate(query_profile_),
      inner_node_ratio);

  // TODO(IMPALA-8126): Move to host profiles
  query_profile_->AddInfoString("Per Node Peak Memory Usage", mem_info.str());
  query_profile_->AddInfoString("Per Node Bytes Read", bytes_read_info.str());
  query_profile_->AddInfoString("Per Node User Time", cpu_user_info.str());
  query_profile_->AddInfoString("Per Node System Time", cpu_system_info.str());
}

string Coordinator::GetErrorLog() {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  ErrorLogMap merged;
  {
    lock_guard<SpinLock> l(backend_states_init_lock_);
    for (BackendState* state: backend_states_) state->MergeErrorLog(&merged);
  }
  return PrintErrorMapToString(merged);
}

void Coordinator::ReleaseExecResources() {
  lock_guard<shared_mutex> lock(filter_routing_table_->lock); // Exclusive lock.
  if (filter_routing_table_->num_filters() > 0) {
    query_profile_->AddInfoString("Final filter table", FilterDebugString());
  }

  for (auto& filter : filter_routing_table_->id_to_filter) {
    unique_lock<SpinLock> l(filter.second.lock());
    filter.second.WaitForPublishFilter();
    filter.second.DisableAndRelease(
        filter_mem_tracker_, filter.second.received_all_updates());
  }

  // This may be NULL while executing UDFs.
  if (filter_mem_tracker_ != nullptr) filter_mem_tracker_->Close();
  // At this point some tracked memory may still be used in the coordinator for result
  // caching. The query MemTracker will be cleaned up later.
}

void Coordinator::ReleaseQueryAdmissionControlResources() {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  vector<BackendState*> unreleased_backends =
      backend_resource_state_->CloseAndGetUnreleasedBackends();
  if (!unreleased_backends.empty()) {
    ReleaseBackendAdmissionControlResources(unreleased_backends);
    backend_resource_state_->BackendsReleased(unreleased_backends);
    for (int i = 0; i < unreleased_backends.size(); ++i) {
      backend_released_barrier_.Notify();
    }
  }
  // Wait for all backends to be released before calling
  // AdmissionController::ReleaseQuery.
  backend_released_barrier_.Wait();
  LOG(INFO) << "Release admission control resources for query_id=" << PrintId(query_id());
  AdmissionController* admission_controller =
      ExecEnv::GetInstance()->admission_controller();
  DCHECK(admission_controller != nullptr);
  admission_controller->ReleaseQuery(
      schedule_, ComputeQueryResourceUtilization().peak_per_host_mem_consumption);
  query_events_->MarkEvent("Released admission control resources");
}

void Coordinator::ReleaseBackendAdmissionControlResources(
    const vector<BackendState*>& backend_states) {
  AdmissionController* admission_controller =
      ExecEnv::GetInstance()->admission_controller();
  DCHECK(admission_controller != nullptr);
  vector<TNetworkAddress> host_addrs;
  for (auto backend_state : backend_states) {
    host_addrs.push_back(FromNetworkAddressPB(backend_state->impalad_address()));
  }
  admission_controller->ReleaseQueryBackends(schedule_, host_addrs);
}

Coordinator::ResourceUtilization Coordinator::ComputeQueryResourceUtilization() {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  ResourceUtilization query_resource_utilization;
  for (BackendState* backend_state: backend_states_) {
    query_resource_utilization.Merge(backend_state->ComputeResourceUtilization());
  }
  return query_resource_utilization;
}

vector<NetworkAddressPB> Coordinator::GetActiveBackends(
    const vector<NetworkAddressPB>& candidates) {
  // Build set from vector so that runtime of this function is O(backend_states.size()).
  std::unordered_set<NetworkAddressPB> candidate_set(
      candidates.begin(), candidates.end());
  vector<NetworkAddressPB> result;
  lock_guard<SpinLock> l(backend_states_init_lock_);
  for (BackendState* backend_state : backend_states_) {
    if (candidate_set.find(backend_state->impalad_address()) != candidate_set.end()
        && !backend_state->IsDone()) {
      result.push_back(backend_state->impalad_address());
    }
  }
  return result;
}

void Coordinator::UpdateFilter(const UpdateFilterParamsPB& params, RpcContext* context) {
  VLOG(2) << "UpdateFilter(filter_id=" << params.filter_id() << ")";
  shared_lock<shared_mutex> lock(filter_routing_table_->lock);
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "UpdateFilter() called although runtime filters are disabled";
  DCHECK(backend_exec_complete_barrier_.get() != nullptr)
      << "Filters received before fragments started!";

  WaitOnExecRpcs();
  DCHECK(filter_routing_table_->is_complete)
      << "Filter received before routing table complete";

  PublishFilterParamsPB rpc_params;
  std::unordered_set<int> target_fragment_idxs;
  if (!IsExecuting()) {
    LOG(INFO) << "Filter update received for non-executing query with id: "
        << query_id();
    return;
  }
  auto it = filter_routing_table_->id_to_filter.find(params.filter_id());
  if (it == filter_routing_table_.get()->id_to_filter.end()) {
    // This should not be possible since 'id_to_filter' is never changed after
    // InitFilterRoutingTable().
    DCHECK(false);
    LOG(INFO) << "Could not find filter with id: " << rpc_params.filter_id();
    return;
  }
  FilterState* state = &it->second;
  {
    lock_guard<SpinLock> l(state->lock());
    DCHECK(state->desc().has_remote_targets)
        << "Coordinator received filter that has only local targets";

    // Check if the filter has already been sent, which could happen in five cases:
    //   * if one local filter had always_true set - no point waiting for other local
    //     filters that can't affect the aggregated global filter
    //   * if this is a broadcast join, and another local filter was already received
    //   * if the filter could not be allocated and so an always_true filter was sent
    //     immediately.
    //   * query execution finished and resources were released: filters do not need
    //     to be processed.
    //   * if the inbound sidecar for Bloom filter cannot be successfully retrieved.
    if (state->disabled()) return;

    if (filter_updates_received_->value() == 0) {
      query_events_->MarkEvent("First dynamic filter received");
    }
    filter_updates_received_->Add(1);

    state->ApplyUpdate(params, this, context);

    if (state->pending_count() > 0 && state->enabled()) return;
    // At this point, we either disabled this filter or aggregation is complete.

    // No more updates are pending on this filter ID. Create a distribution payload and
    // offer it to the queue.
    for (const FilterTarget& target : *state->targets()) {
      // Don't publish the filter to targets that are in the same fragment as the join
      // that produced it.
      if (target.is_local) continue;
      target_fragment_idxs.insert(target.fragment_idx);
    }

    if (state->is_bloom_filter()) {
      // Assign an outgoing bloom filter.
      *rpc_params.mutable_bloom_filter() = state->bloom_filter();

      DCHECK(rpc_params.bloom_filter().always_false()
          || rpc_params.bloom_filter().always_true()
          || !state->bloom_filter_directory().empty());

    } else {
      DCHECK(state->is_min_max_filter());
      MinMaxFilter::Copy(state->min_max_filter(), rpc_params.mutable_min_max_filter());
    }

    // Filter is complete. We disable it so future UpdateFilter rpcs will be ignored,
    // e.g., if it was a broadcast join. If filter is still enabled at this point, it
    // means all filter updates have been successfully received and applied.
    state->Disable(state->enabled());

    TUniqueIdToUniqueIdPB(query_id(), rpc_params.mutable_dst_query_id());
    rpc_params.set_filter_id(params.filter_id());

    // Called WaitForExecRpcs() so backend_states_ is valid.
    for (BackendState* bs : backend_states_) {
      if (!IsExecuting()) {
        if (rpc_params.has_bloom_filter()) {
          filter_mem_tracker_->Release(state->bloom_filter_directory().size());
          state->bloom_filter_directory().clear();
          state->bloom_filter_directory().shrink_to_fit();
          return;
        }
      }

      if (bs->HasFragmentIdx(target_fragment_idxs)) {
        rpc_params.set_filter_id(params.filter_id());
        RpcController* controller = obj_pool()->Add(new RpcController);
        PublishFilterResultPB* res = obj_pool()->Add(new PublishFilterResultPB);
        if (rpc_params.has_bloom_filter() && !rpc_params.bloom_filter().always_false()
            && !rpc_params.bloom_filter().always_true()) {
          BloomFilter::AddDirectorySidecar(rpc_params.mutable_bloom_filter(), controller,
              state->bloom_filter_directory());
        }
        bs->PublishFilter(state, filter_mem_tracker_, rpc_params, *controller, *res);
      }
    }
  }
}

void Coordinator::FilterState::ApplyUpdate(
    const UpdateFilterParamsPB& params, Coordinator* coord, RpcContext* context) {
  DCHECK(enabled());
  DCHECK_GT(pending_count_, 0);
  DCHECK_EQ(completion_time_, 0L);
  if (first_arrival_time_ == 0L) {
    first_arrival_time_ = coord->query_events_->ElapsedTime();
  }

  --pending_count_;
  if (is_bloom_filter()) {
    DCHECK(params.has_bloom_filter());
    if (params.bloom_filter().always_true()) {
      // An always_true filter is received. We don't need to wait for other pending
      // backends.
      DisableAndRelease(coord->filter_mem_tracker_, true);
    } else if (params.bloom_filter().always_false()) {
      if (!bloom_filter_.has_log_bufferpool_space()) {
        bloom_filter_ = BloomFilterPB(params.bloom_filter());
      }
    } else {
      // If the incoming Bloom filter is neither an always true filter nor an
      // always false filter, then it must be the case that a non-empty sidecar slice
      // has been received. Refer to BloomFilter::ToProtobuf() for further details.
      DCHECK(params.bloom_filter().has_directory_sidecar_idx());
      kudu::Slice sidecar_slice;
      kudu::Status status = context->GetInboundSidecar(
          params.bloom_filter().directory_sidecar_idx(), &sidecar_slice);
      if (!status.ok()) {
        LOG(ERROR) << "Cannot get inbound sidecar: " << status.message().ToString();
        DisableAndRelease(coord->filter_mem_tracker_, false);
      } else if (bloom_filter_.always_false()) {
        int64_t heap_space = sidecar_slice.size();
        if (!coord->filter_mem_tracker_->TryConsume(heap_space)) {
          VLOG_QUERY << "Not enough memory to allocate filter: "
                     << PrettyPrinter::Print(heap_space, TUnit::BYTES)
                     << " (query_id=" << PrintId(coord->query_id()) << ")";
          // Disable, as one missing update means a correct filter cannot be produced.
          DisableAndRelease(coord->filter_mem_tracker_, false);
        } else {
          bloom_filter_ = params.bloom_filter();
          bloom_filter_directory_ = sidecar_slice.ToString();
        }
      } else {
        DCHECK_EQ(bloom_filter_directory_.size(), sidecar_slice.size());
        BloomFilter::Or(params.bloom_filter(), sidecar_slice.data(), &bloom_filter_,
            reinterpret_cast<uint8_t*>(const_cast<char*>(bloom_filter_directory_.data())),
            sidecar_slice.size());
      }
    }
  } else {
    DCHECK(is_min_max_filter());
    DCHECK(params.has_min_max_filter());
    if (params.min_max_filter().always_true()) {
      // An always_true filter is received. We don't need to wait for other pending
      // backends.
      DisableAndRelease(coord->filter_mem_tracker_, true);
    } else if (min_max_filter_.always_false()) {
      MinMaxFilter::Copy(params.min_max_filter(), &min_max_filter_);
    } else {
      MinMaxFilter::Or(params.min_max_filter(), &min_max_filter_,
          ColumnType::FromThrift(desc_.src_expr.nodes[0].type));
    }
  }

  if (pending_count_ == 0 || disabled()) {
    completion_time_ = coord->query_events_->ElapsedTime();
  }
}

void Coordinator::FilterState::DisableAndRelease(
    MemTracker* tracker, const bool all_updates_received) {
  Disable(all_updates_received);
  if (is_bloom_filter()) {
    tracker->Release(bloom_filter_directory_.size());
    bloom_filter_directory_.clear();
    bloom_filter_directory_.shrink_to_fit();
  }
}

void Coordinator::FilterState::Disable(const bool all_updates_received) {
  all_updates_received_ = all_updates_received;
  if (is_bloom_filter()) {
    bloom_filter_.set_always_true(true);
    bloom_filter_.set_always_false(false);
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.set_always_true(true);
    min_max_filter_.set_always_false(false);
  }
}

void Coordinator::FilterState::WaitForPublishFilter() {
  while (num_inflight_publish_filter_rpcs_ > 0) {
    publish_filter_done_cv_.wait(lock_);
  }
}

void Coordinator::GetTExecSummary(TExecSummary* exec_summary) {
  lock_guard<SpinLock> l(exec_summary_.lock);
  *exec_summary = exec_summary_.thrift_exec_summary;
}

MemTracker* Coordinator::query_mem_tracker() const {
  return query_state_->query_mem_tracker();
}

void Coordinator::BackendsToJson(Document* doc) {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  Value states(kArrayType);
  {
    lock_guard<SpinLock> l(backend_states_init_lock_);
    for (BackendState* state : backend_states_) {
      Value val(kObjectType);
      state->ToJson(&val, doc);
      states.PushBack(val, doc->GetAllocator());
    }
  }
  doc->AddMember("backend_states", states, doc->GetAllocator());
}

void Coordinator::FInstanceStatsToJson(Document* doc) {
  DCHECK(exec_rpcs_complete_.Load()) << "Exec() must be called first";
  Value states(kArrayType);
  {
    lock_guard<SpinLock> l(backend_states_init_lock_);
    for (BackendState* state : backend_states_) {
      Value val(kObjectType);
      state->InstanceStatsToJson(&val, doc);
      states.PushBack(val, doc->GetAllocator());
    }
  }
  doc->AddMember("backend_instances", states, doc->GetAllocator());
}

const TQueryCtx& Coordinator::query_ctx() const {
  return schedule_.request().query_ctx;
}

const TUniqueId& Coordinator::query_id() const {
  return query_ctx().query_id;
}

const TFinalizeParams* Coordinator::finalize_params() const {
  return schedule_.request().__isset.finalize_params
      ? &schedule_.request().finalize_params : nullptr;
}

bool Coordinator::IsExecuting() {
  ExecState current_state = exec_state_.Load();
  return current_state == ExecState::EXECUTING;
}
