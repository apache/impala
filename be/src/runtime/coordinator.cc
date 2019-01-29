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
#include "gen-cpp/ImpalaInternalService.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "runtime/exec-env.h"
#include "runtime/fragment-instance-state.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/query-exec-mgr.h"
#include "runtime/coordinator-filter-state.h"
#include "runtime/coordinator-backend-state.h"
#include "runtime/debug-options.h"
#include "runtime/query-state.h"
#include "scheduling/admission-controller.h"
#include "scheduling/scheduler.h"
#include "scheduling/query-schedule.h"
#include "service/client-request-state.h"
#include "util/bloom-filter.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/min-max-filter.h"
#include "util/table-printer.h"

#include "common/names.h"

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

// Maximum number of fragment instances that can publish each broadcast filter.
static const int MAX_BROADCAST_FILTER_PRODUCERS = 3;

Coordinator::Coordinator(ClientRequestState* parent, const QuerySchedule& schedule,
    RuntimeProfile::EventSequence* events)
  : parent_request_state_(parent),
    schedule_(schedule),
    filter_mode_(schedule.query_options().runtime_filter_mode),
    obj_pool_(new ObjectPool()),
    query_events_(events),
    exec_rpcs_complete_barrier_(schedule_.per_backend_exec_params().size()) {}

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
  finalization_timer_ = ADD_TIMER(query_profile_, "FinalizationTimer");
  filter_updates_received_ = ADD_COUNTER(query_profile_, "FiltersReceived", TUnit::UNIT);

  host_profiles_ = RuntimeProfile::Create(obj_pool(), "Per Node Profiles");
  query_profile_->AddChild(host_profiles_);

  SCOPED_TIMER(query_profile_->total_time_counter());

  // initialize progress updater
  const string& str = Substitute("Query $0", PrintId(query_id()));
  progress_.Init(str, schedule_.num_scan_ranges());

  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->CreateQueryState(
      query_ctx(), schedule_.per_backend_mem_limit());
  filter_mem_tracker_ = query_state_->obj_pool()->Add(new MemTracker(
      -1, "Runtime Filter (Coordinator)", query_state_->query_mem_tracker(), false));

  InitFragmentStats();
  // create BackendStates and per-instance state, including profiles, and install
  // the latter in the FragmentStats' root profile
  InitBackendStates();
  exec_summary_.Init(schedule_);

  // TODO-MT: populate the runtime filter routing table
  // This requires local aggregation of filters prior to sending
  // for broadcast joins in order to avoid more complicated merge logic here.

  if (filter_mode_ != TRuntimeFilterMode::OFF) {
    DCHECK_EQ(request.plan_exec_info.size(), 1);
    // Populate the runtime filter routing table. This should happen before starting the
    // fragment instances. This code anticipates the indices of the instance states
    // created later on in ExecRemoteFragment()
    InitFilterRoutingTable();
  }

  // At this point, all static setup is done and all structures are initialized. Only
  // runtime-related state changes past this point (examples: fragment instance
  // profiles, etc.)

  StartBackendExec();
  RETURN_IF_ERROR(FinishBackendStartup());

  // set coord_instance_ and coord_sink_
  if (schedule_.GetCoordFragment() != nullptr) {
    // this blocks until all fragment instances have finished their Prepare phase
    coord_instance_ = query_state_->GetFInstanceState(query_id());
    if (coord_instance_ == nullptr) {
      // at this point, the query is done with the Prepare phase, and we expect
      // to have a coordinator instance, but coord_instance_ == nullptr,
      // which means we failed before or during Prepare().
      Status query_status = query_state_->WaitForPrepare();
      DCHECK(!query_status.ok());
      return UpdateExecState(query_status, nullptr, FLAGS_hostname);
    }
    // When GetFInstanceState() returns the coordinator instance, the Prepare phase is
    // done and the FragmentInstanceState's root sink will be set up.
    coord_sink_ = coord_instance_->root_sink();
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
  RuntimeProfile::Counter* num_fragments =
      ADD_COUNTER(query_profile_, "NumFragments", TUnit::UNIT);
  num_fragments->Set(static_cast<int64_t>(fragments.size()));
  RuntimeProfile::Counter* num_finstances =
      ADD_COUNTER(query_profile_, "NumFragmentInstances", TUnit::UNIT);
  num_finstances->Set(total_num_finstances);
}

void Coordinator::InitBackendStates() {
  int num_backends = schedule_.per_backend_exec_params().size();
  DCHECK_GT(num_backends, 0);

  lock_guard<SpinLock> l(backend_states_init_lock_);
  backend_states_.resize(num_backends);

  RuntimeProfile::Counter* num_backends_counter =
      ADD_COUNTER(query_profile_, "NumBackends", TUnit::UNIT);
  num_backends_counter->Set(num_backends);

  // create BackendStates
  int backend_idx = 0;
  for (const auto& entry: schedule_.per_backend_exec_params()) {
    BackendState* backend_state = obj_pool()->Add(
        new BackendState(*this, backend_idx, filter_mode_));
    backend_state->Init(entry.second, fragment_stats_, host_profiles_, obj_pool());
    backend_states_[backend_idx++] = backend_state;
  }
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
      int num_instances =
          schedule.GetFragmentExecParams(fragment.idx).instance_exec_params.size();

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
        node_summary.exec_stats.resize(num_instances);
      }

      if (fragment.__isset.output_sink
          && fragment.output_sink.type == TDataSinkType::DATA_STREAM_SINK) {
        const TDataStreamSink& sink = fragment.output_sink.stream_sink;
        int exch_idx = node_id_to_idx_map[sink.dest_node_id];
        if (sink.output_partition.type == TPartitionType::UNPARTITIONED) {
          thrift_exec_summary.nodes[exch_idx].__set_is_broadcast(true);
        }
        thrift_exec_summary.__isset.exch_to_sender_map = true;
        thrift_exec_summary.exch_to_sender_map[exch_idx] = root_node_idx;
      }
    }
  }
}

void Coordinator::InitFilterRoutingTable() {
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "InitFilterRoutingTable() called although runtime filters are disabled";
  DCHECK(!filter_routing_table_complete_)
      << "InitFilterRoutingTable() called after setting filter_routing_table_complete_";

  lock_guard<shared_mutex> lock(filter_lock_);
  for (const FragmentExecParams& fragment_params: schedule_.fragment_exec_params()) {
    int num_instances = fragment_params.instance_exec_params.size();
    DCHECK_GT(num_instances, 0);

    for (const TPlanNode& plan_node: fragment_params.fragment.plan.nodes) {
      if (!plan_node.__isset.runtime_filters) continue;
      for (const TRuntimeFilterDesc& filter: plan_node.runtime_filters) {
        DCHECK(filter_mode_ == TRuntimeFilterMode::GLOBAL || filter.has_local_targets);
        FilterRoutingTable::iterator i = filter_routing_table_.emplace(
            filter.filter_id, FilterState(filter, plan_node.node_id)).first;
        FilterState* f = &(i->second);

        // source plan node of filter
        if (plan_node.__isset.hash_join_node) {
          // Set the 'pending_count_' to zero to indicate that for a filter with
          // local-only targets the coordinator does not expect to receive any filter
          // updates.
          int pending_count = filter.is_broadcast_join
              ? (filter.has_remote_targets ? 1 : 0) : num_instances;
          f->set_pending_count(pending_count);

          // determine source instances
          // TODO: store this in FInstanceExecParams, not in FilterState
          vector<int> src_idxs = fragment_params.GetInstanceIdxs();

          // If this is a broadcast join with only non-local targets, build and publish it
          // on MAX_BROADCAST_FILTER_PRODUCERS instances. If this is not a broadcast join
          // or it is a broadcast join with local targets, it should be generated
          // everywhere the join is executed.
          if (filter.is_broadcast_join && !filter.has_local_targets
              && num_instances > MAX_BROADCAST_FILTER_PRODUCERS) {
            random_shuffle(src_idxs.begin(), src_idxs.end());
            src_idxs.resize(MAX_BROADCAST_FILTER_PRODUCERS);
          }
          f->src_fragment_instance_idxs()->insert(src_idxs.begin(), src_idxs.end());

        // target plan node of filter
        } else if (plan_node.__isset.hdfs_scan_node || plan_node.__isset.kudu_scan_node) {
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
      "Number of filters", Substitute("$0", filter_routing_table_.size()));
  query_profile_->AddInfoString("Filter routing table", FilterDebugString());
  if (VLOG_IS_ON(2)) VLOG_QUERY << FilterDebugString();
  filter_routing_table_complete_ = true;
}

void Coordinator::StartBackendExec() {
  int num_backends = backend_states_.size();
  backend_exec_complete_barrier_.reset(new CountingBarrier(num_backends));

  DebugOptions debug_options(schedule_.query_options());

  VLOG_QUERY << "starting execution on " << num_backends << " backends for query_id="
             << PrintId(query_id());
  query_events_->MarkEvent(Substitute("Ready to start on $0 backends", num_backends));

  for (BackendState* backend_state: backend_states_) {
    ExecEnv::GetInstance()->exec_rpc_thread_pool()->Offer(
        [backend_state, this, &debug_options]() {
          DebugActionNoFail(schedule_.query_options(), "COORD_BEFORE_EXEC_RPC");
          backend_state->Exec(
              debug_options, filter_routing_table_, &exec_rpcs_complete_barrier_);
        });
  }
  exec_rpcs_complete_barrier_.Wait();

  VLOG_QUERY << "started execution on " << num_backends << " backends for query_id="
             << PrintId(query_id());
  query_events_->MarkEvent(
      Substitute("All $0 execution backends ($1 fragment instances) started",
        num_backends, schedule_.GetNumFragmentInstances()));
}

Status Coordinator::FinishBackendStartup() {
  const TMetricDef& def =
      MakeTMetricDef("backend-startup-latencies", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  // Capture up to 30 minutes of start-up times, in ms, with 4 s.f. accuracy.
  HistogramMetric latencies(def, 30 * 60 * 1000, 4);
  Status status = Status::OK();
  string error_hostname;
  string max_latency_host;
  int max_latency = 0;
  for (BackendState* backend_state: backend_states_) {
    // preserve the first non-OK, if there is one
    Status backend_status = backend_state->GetStatus();
    if (!backend_status.ok() && status.ok()) {
      status = backend_status;
      error_hostname = backend_state->impalad_address().hostname;
    }
    if (backend_state->rpc_latency() > max_latency) {
      // Find the backend that takes the most time to acknowledge to
      // the ExecQueryFinstances() RPC.
      max_latency = backend_state->rpc_latency();
      max_latency_host = TNetworkAddressToString(backend_state->impalad_address());
    }
    latencies.Update(backend_state->rpc_latency());
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
  for (FilterRoutingTable::value_type& v: filter_routing_table_) {
    vector<string> row;
    const FilterState& state = v.second;
    row.push_back(lexical_cast<string>(v.first));
    row.push_back(lexical_cast<string>(state.src()));
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
      row.push_back(Substitute("$0 ($1)", pending_count,
          state.src_fragment_instance_idxs().size()));
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
  DCHECK_LE(exec_rpcs_complete_barrier_.pending(), 0) << "exec rpcs not completed";

  query_events_->MarkEvent(exec_state_to_event.at(new_state));
  // This thread won the race to transitioning into a terminal state - terminate
  // execution and release resources.
  ReleaseExecResources();
  if (new_state == ExecState::RETURNED_RESULTS) {
    // TODO: IMPALA-6984: cancel all backends in this case too.
    WaitForBackends();
  } else {
    CancelBackends();
  }
  ReleaseAdmissionControlResources();
  // Once the query has released its admission control resources, update its end time.
  parent_request_state_->UpdateEndTime();
  // Can compute summary only after we stop accepting reports from the backends. Both
  // WaitForBackends() and CancelBackends() ensures that.
  // TODO: should move this off of the query execution path?
  ComputeQuerySummary();
}

Status Coordinator::FinalizeHdfsInsert() {
  // All instances must have reported their final statuses before finalization, which is a
  // post-condition of Wait. If the query was not successful, still try to clean up the
  // staging directory.
  DCHECK(has_called_wait_);
  DCHECK(finalize_params() != nullptr);

  VLOG_QUERY << "Finalizing query: " << PrintId(query_id());
  SCOPED_TIMER(finalization_timer_);
  Status return_status = UpdateExecState(Status::OK(), nullptr, FLAGS_hostname);
  if (return_status.ok()) {
    HdfsTableDescriptor* hdfs_table;
    RETURN_IF_ERROR(DescriptorTbl::CreateHdfsTblDescriptor(query_ctx().desc_tbl,
            finalize_params()->table_id, obj_pool(), &hdfs_table));
    DCHECK(hdfs_table != nullptr)
        << "INSERT target table not known in descriptor table: "
        << finalize_params()->table_id;
    return_status = dml_exec_state_.FinalizeHdfsInsert(*finalize_params(),
        query_ctx().client_request.query_options.s3_skip_insert_staging,
        hdfs_table, query_profile_);
    hdfs_table->ReleaseResources();
  }

  stringstream staging_dir;
  DCHECK(finalize_params()->__isset.staging_dir);
  staging_dir << finalize_params()->staging_dir << "/" << PrintId(query_id(),"_") << "/";

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(staging_dir.str(), &hdfs_conn));
  VLOG_QUERY << "Removing staging directory: " << staging_dir.str();
  hdfsDelete(hdfs_conn, staging_dir.str().c_str(), 1);

  return return_status;
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
  lock_guard<SpinLock> l(wait_lock_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (has_called_wait_) return Status::OK();
  has_called_wait_ = true;

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
            FinalizeHdfsInsert(), nullptr, FLAGS_hostname));
  }
  // DML requests are finished at this point.
  RETURN_IF_ERROR(SetNonErrorTerminalState(ExecState::RETURNED_RESULTS));
  query_profile_->AddInfoString(
      "DML Stats", dml_exec_state_.OutputPartitionStats("\n"));
  return Status::OK();
}

Status Coordinator::GetNext(QueryResultSet* results, int max_rows, bool* eos) {
  VLOG_ROW << "GetNext() query_id=" << PrintId(query_id());
  DCHECK(has_called_wait_);
  SCOPED_TIMER(query_profile_->total_time_counter());

  if (ReturnedAllResults()) {
    // Nothing left to do: already in a terminal state and no more results.
    *eos = true;
    return Status::OK();
  }
  DCHECK(coord_instance_ != nullptr) << "Exec() should be called first";
  DCHECK(coord_sink_ != nullptr)     << "Exec() should be called first";
  RuntimeState* runtime_state = coord_instance_->runtime_state();

  Status status = coord_sink_->GetNext(runtime_state, results, max_rows, eos);
  RETURN_IF_ERROR(UpdateExecState(
          status, &runtime_state->fragment_instance_id(), FLAGS_hostname));
  if (*eos) RETURN_IF_ERROR(SetNonErrorTerminalState(ExecState::RETURNED_RESULTS));
  return Status::OK();
}

void Coordinator::Cancel() {
  // Illegal to call Cancel() before Exec() returns, so there's no danger of the cancel
  // RPC passing the exec RPC.
  DCHECK_LE(exec_rpcs_complete_barrier_.pending(), 0) << "Exec() must be called first";
  discard_result(SetNonErrorTerminalState(ExecState::CANCELLED));
  // CancelBackends() is called for all transitions into a terminal state except
  // for RETURNED_RESULTS. We need to call it now because after Cancel() is called
  // the coordinator is not guaranteed to get UpdateBackendExecStatus() calls, and
  // so we need to unblock the backend_exec_complete_barrier_.
  // TODO: Remove this once IMPALA-6984 is fixed. It won't be necessary since
  // CancelBackends() will be called when transitioning to RETURNED_RESULTS.
  if (ReturnedAllResults()) CancelBackends();
}

void Coordinator::CancelBackends() {
  int num_cancelled = 0;
  for (BackendState* backend_state: backend_states_) {
    DCHECK(backend_state != nullptr);
    if (backend_state->Cancel()) ++num_cancelled;
  }
  backend_exec_complete_barrier_->NotifyRemaining();

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

  if (backend_state->ApplyExecStatusReport(request, thrift_profiles, &exec_summary_,
          &progress_, &dml_exec_state_)) {
    // This backend execution has completed.
    if (VLOG_QUERY_IS_ON) {
      // Don't log backend completion if the query has already been cancelled.
      int pending_backends = backend_exec_complete_barrier_->pending();
      if (pending_backends >= 1) {
        VLOG_QUERY << "Backend completed:"
                   << " host=" << TNetworkAddressToString(backend_state->impalad_address())
                   << " remaining=" << pending_backends
                   << " query_id=" << PrintId(query_id());
        BackendState::LogFirstInProgress(backend_states_);
      }
    }
    bool is_fragment_failure;
    TUniqueId failed_instance_id;
    Status status = backend_state->GetStatus(&is_fragment_failure, &failed_instance_id);
    if (!status.ok()) {
      // We may start receiving status reports before all exec rpcs are complete.
      // Can't apply state transition until no more exec rpcs will be sent.
      exec_rpcs_complete_barrier_.Wait();
      // Transition the status if we're not already in a terminal state. This won't block
      // because either this transitions to an ERROR state or the query is already in
      // a terminal state.
      discard_result(UpdateExecState(status,
              is_fragment_failure ? &failed_instance_id : nullptr,
              TNetworkAddressToString(backend_state->impalad_address())));
    }
    // We've applied all changes from the final status report - notify waiting threads.
    discard_result(backend_exec_complete_barrier_->Notify());
  }
  // If query execution has terminated, return a cancelled status to force the fragment
  // instance to stop executing.
  return IsExecuting() ? Status::OK() : Status::CANCELLED;
}

int64_t Coordinator::GetMaxBackendStateLagMs(TNetworkAddress* address) {
  if (exec_rpcs_complete_barrier_.pending() > 0) {
    // Exec() hadn't completed for all the backends, so we can't rely on
    // 'last_report_time_ms_' being set yet.
    return 0;
  }
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
  // In this case, the query did not even get to start all fragment instances.
  // Some of the state that is used below might be uninitialized.  In this case,
  // the query has made so little progress, reporting a summary is not very useful.
  if (!has_called_wait_) return;

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
    string network_address = TNetworkAddressToString(
        backend_state->impalad_address());
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

  // The total number of bytes read by this query.
  COUNTER_SET(ADD_COUNTER(query_profile_, "TotalBytesRead", TUnit::BYTES),
      total_utilization.bytes_read);
  // The total number of bytes sent by this query in exchange nodes. Does not include
  // remote reads, data written to disk, or data sent to the client.
  COUNTER_SET(ADD_COUNTER(query_profile_, "TotalBytesSent", TUnit::BYTES),
      total_utilization.scan_bytes_sent + total_utilization.exchange_bytes_sent);
  // The total number of bytes sent by fragment instances that had a scan node in their
  // plan.
  COUNTER_SET(ADD_COUNTER(query_profile_, "TotalScanBytesSent", TUnit::BYTES),
      total_utilization.scan_bytes_sent);
  // The total number of bytes sent by fragment instances that did not have a scan node in
  // their plan, i.e. that received their input data from other instances through exchange
  // node.
  COUNTER_SET(ADD_COUNTER(query_profile_, "TotalInnerBytesSent", TUnit::BYTES),
      total_utilization.exchange_bytes_sent);

  double xchg_scan_ratio = 0;
  if (total_utilization.bytes_read > 0) {
    xchg_scan_ratio =
        (double)total_utilization.scan_bytes_sent / total_utilization.bytes_read;
  }
  // The ratio between TotalScanBytesSent and TotalBytesRead, i.e. the selectivity over
  // all fragment instances that had a scan node in their plan.
  COUNTER_SET(ADD_COUNTER(query_profile_, "ExchangeScanRatio", TUnit::DOUBLE_VALUE),
      xchg_scan_ratio);

  double inner_node_ratio = 0;
  if (total_utilization.scan_bytes_sent > 0) {
    inner_node_ratio =
        (double)total_utilization.exchange_bytes_sent / total_utilization.scan_bytes_sent;
  }
  // The ratio between bytes sent by instances with a scan node in their plan and
  // instances without a scan node in their plan. This indicates how well the inner nodes
  // of the execution plan reduced the data volume.
  COUNTER_SET(
      ADD_COUNTER(query_profile_, "InnerNodeSelectivityRatio", TUnit::DOUBLE_VALUE),
      inner_node_ratio);

  COUNTER_SET(ADD_COUNTER(query_profile_, "TotalCpuTime", TUnit::TIME_NS),
      total_utilization.cpu_user_ns + total_utilization.cpu_sys_ns);

  // TODO(IMPALA-8126): Move to host profiles
  query_profile_->AddInfoString("Per Node Peak Memory Usage", mem_info.str());
  query_profile_->AddInfoString("Per Node Bytes Read", bytes_read_info.str());
  query_profile_->AddInfoString("Per Node User Time", cpu_user_info.str());
  query_profile_->AddInfoString("Per Node System Time", cpu_system_info.str());
}

string Coordinator::GetErrorLog() {
  ErrorLogMap merged;
  {
    lock_guard<SpinLock> l(backend_states_init_lock_);
    for (BackendState* state: backend_states_) state->MergeErrorLog(&merged);
  }
  return PrintErrorMapToString(merged);
}

void Coordinator::ReleaseExecResources() {
  lock_guard<shared_mutex> lock(filter_lock_);
  if (filter_routing_table_.size() > 0) {
    query_profile_->AddInfoString("Final filter table", FilterDebugString());
  }

  for (auto& filter : filter_routing_table_) {
    FilterState* state = &filter.second;
    state->Disable(filter_mem_tracker_);
  }
  // This may be NULL while executing UDFs.
  if (filter_mem_tracker_ != nullptr) filter_mem_tracker_->Close();
  // At this point some tracked memory may still be used in the coordinator for result
  // caching. The query MemTracker will be cleaned up later.
}

void Coordinator::ReleaseAdmissionControlResources() {
  LOG(INFO) << "Release admission control resources for query_id=" << PrintId(query_id());
  AdmissionController* admission_controller =
      ExecEnv::GetInstance()->admission_controller();
  DCHECK(admission_controller != nullptr);
  admission_controller->ReleaseQuery(
      schedule_, ComputeQueryResourceUtilization().peak_per_host_mem_consumption);
  query_events_->MarkEvent("Released admission control resources");
}

Coordinator::ResourceUtilization Coordinator::ComputeQueryResourceUtilization() {
  ResourceUtilization query_resource_utilization;
  for (BackendState* backend_state: backend_states_) {
    query_resource_utilization.Merge(backend_state->ComputeResourceUtilization());
  }
  return query_resource_utilization;
}

vector<TNetworkAddress> Coordinator::GetActiveBackends(
    const vector<TNetworkAddress>& candidates) {
  // Build set from vector so that runtime of this function is O(backend_states.size()).
  unordered_set<TNetworkAddress> candidate_set(candidates.begin(), candidates.end());
  vector<TNetworkAddress> result;
  lock_guard<SpinLock> l(backend_states_init_lock_);
  for (BackendState* backend_state : backend_states_) {
    if (candidate_set.find(backend_state->impalad_address()) != candidate_set.end()
        && !backend_state->IsDone()) {
      result.push_back(backend_state->impalad_address());
    }
  }
  return result;
}

void Coordinator::UpdateFilter(const TUpdateFilterParams& params) {
  shared_lock<shared_mutex> lock(filter_lock_);
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "UpdateFilter() called although runtime filters are disabled";
  DCHECK(backend_exec_complete_barrier_.get() != nullptr)
      << "Filters received before fragments started!";

  exec_rpcs_complete_barrier_.Wait();
  DCHECK(filter_routing_table_complete_)
      << "Filter received before routing table complete";

  TPublishFilterParams rpc_params;
  unordered_set<int> target_fragment_idxs;
  {
    lock_guard<SpinLock> l(filter_update_lock_);
    if (!IsExecuting()) {
      LOG(INFO) << "Filter update received for non-executing query with id: "
                << query_id();
      return;
    }
    FilterRoutingTable::iterator it = filter_routing_table_.find(params.filter_id);
    if (it == filter_routing_table_.end()) {
      LOG(INFO) << "Could not find filter with id: " << params.filter_id;
      return;
    }
    FilterState* state = &it->second;

    DCHECK(state->desc().has_remote_targets)
          << "Coordinator received filter that has only local targets";

    // Check if the filter has already been sent, which could happen in four cases:
    //   * if one local filter had always_true set - no point waiting for other local
    //     filters that can't affect the aggregated global filter
    //   * if this is a broadcast join, and another local filter was already received
    //   * if the filter could not be allocated and so an always_true filter was sent
    //     immediately.
    //   * query execution finished and resources were released: filters do not need
    //     to be processed.
    if (state->disabled()) return;

    if (filter_updates_received_->value() == 0) {
      query_events_->MarkEvent("First dynamic filter received");
    }
    filter_updates_received_->Add(1);

    state->ApplyUpdate(params, this);

    if (state->pending_count() > 0 && !state->disabled()) return;
    // At this point, we either disabled this filter or aggregation is complete.

    // No more updates are pending on this filter ID. Create a distribution payload and
    // offer it to the queue.
    for (const FilterTarget& target: *state->targets()) {
      // Don't publish the filter to targets that are in the same fragment as the join
      // that produced it.
      if (target.is_local) continue;
      target_fragment_idxs.insert(target.fragment_idx);
    }

    if (state->is_bloom_filter()) {
      // Assign outgoing bloom filter.
      TBloomFilter& aggregated_filter = state->bloom_filter();

      swap(rpc_params.bloom_filter, aggregated_filter);
      DCHECK(rpc_params.bloom_filter.always_false || rpc_params.bloom_filter.always_true
          || !rpc_params.bloom_filter.directory.empty());
      DCHECK(aggregated_filter.directory.empty());
      rpc_params.__isset.bloom_filter = true;
    } else {
      DCHECK(state->is_min_max_filter());
      MinMaxFilter::Copy(state->min_max_filter(), &rpc_params.min_max_filter);
      rpc_params.__isset.min_max_filter = true;
    }

    // Filter is complete, and can be released.
    state->Disable(filter_mem_tracker_);
  }

  rpc_params.__set_dst_query_id(query_id());
  rpc_params.__set_filter_id(params.filter_id);

  // Waited for exec_rpcs_complete_barrier_ so backend_states_ is valid.
  for (BackendState* bs: backend_states_) {
    for (int fragment_idx: target_fragment_idxs) {
      if (!IsExecuting()) goto cleanup;
      rpc_params.__set_dst_fragment_idx(fragment_idx);
      bs->PublishFilter(rpc_params);
    }
  }

cleanup:
  // For bloom filters, the memory used in the filter_routing_table_ is transfered to
  // rpc_params. Hence the Release() function on the filter_mem_tracker_ is called
  // here to ensure that the MemTracker is updated after the memory is actually freed.
  if (rpc_params.__isset.bloom_filter) {
    filter_mem_tracker_->Release(rpc_params.bloom_filter.directory.size());
  }
}

void Coordinator::FilterState::ApplyUpdate(const TUpdateFilterParams& params,
    Coordinator* coord) {
  DCHECK(!disabled());
  DCHECK_GT(pending_count_, 0);
  DCHECK_EQ(completion_time_, 0L);
  if (first_arrival_time_ == 0L) {
    first_arrival_time_ = coord->query_events_->ElapsedTime();
  }

  --pending_count_;
  if (is_bloom_filter()) {
    DCHECK(params.__isset.bloom_filter);
    if (params.bloom_filter.always_true) {
      Disable(coord->filter_mem_tracker_);
    } else if (bloom_filter_.always_false) {
      int64_t heap_space = params.bloom_filter.directory.size();
      if (!coord->filter_mem_tracker_->TryConsume(heap_space)) {
        VLOG_QUERY << "Not enough memory to allocate filter: "
                   << PrettyPrinter::Print(heap_space, TUnit::BYTES)
                   << " (query_id=" << PrintId(coord->query_id()) << ")";
        // Disable, as one missing update means a correct filter cannot be produced.
        Disable(coord->filter_mem_tracker_);
      } else {
        // Workaround for fact that parameters are const& for Thrift RPCs - yet we want to
        // move the payload from the request rather than copy it and take double the
        // memory cost. After this point, params.bloom_filter is an empty filter and
        // should not be read.
        TBloomFilter* non_const_filter = &const_cast<TBloomFilter&>(params.bloom_filter);
        swap(bloom_filter_, *non_const_filter);
        DCHECK_EQ(non_const_filter->directory.size(), 0);
      }
    } else {
      BloomFilter::Or(params.bloom_filter, &bloom_filter_);
    }
  } else {
    DCHECK(is_min_max_filter());
    DCHECK(params.__isset.min_max_filter);
    if (params.min_max_filter.always_true) {
      Disable(coord->filter_mem_tracker_);
    } else if (min_max_filter_.always_false) {
      MinMaxFilter::Copy(params.min_max_filter, &min_max_filter_);
    } else {
      MinMaxFilter::Or(params.min_max_filter, &min_max_filter_,
          ColumnType::FromThrift(desc_.src_expr.nodes[0].type));
    }
  }

  if (pending_count_ == 0 || disabled()) {
    completion_time_ = coord->query_events_->ElapsedTime();
  }
}

void Coordinator::FilterState::Disable(MemTracker* tracker) {
  if (is_bloom_filter()) {
    bloom_filter_.always_true = true;
    bloom_filter_.always_false = false;
    tracker->Release(bloom_filter_.directory.size());
    bloom_filter_.directory.clear();
    bloom_filter_.directory.shrink_to_fit();
  } else {
    DCHECK(is_min_max_filter());
    min_max_filter_.always_true = true;
    min_max_filter_.always_false = false;
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
