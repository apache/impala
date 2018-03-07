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

#include <thrift/protocol/TDebugProtocol.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/filesystem.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include <gutil/strings/substitute.h>

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
#include "util/bloom-filter.h"
#include "util/counting-barrier.h"
#include "util/hdfs-bulk-ops.h"
#include "util/hdfs-util.h"
#include "util/histogram-metric.h"
#include "util/min-max-filter.h"
#include "util/table-printer.h"

#include "common/names.h"

using namespace apache::thrift;
using namespace rapidjson;
using namespace strings;
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

Coordinator::Coordinator(
    const QuerySchedule& schedule, RuntimeProfile::EventSequence* events)
  : schedule_(schedule),
    filter_mode_(schedule.query_options().runtime_filter_mode),
    obj_pool_(new ObjectPool()),
    query_events_(events) {}

Coordinator::~Coordinator() {
  DCHECK(released_exec_resources_)
      << "ReleaseExecResources() must be called before Coordinator is destroyed";
  DCHECK(released_admission_control_resources_)
      << "ReleaseAdmissionControlResources() must be called before Coordinator is "
      << "destroyed";
  if (query_state_ != nullptr) {
    ExecEnv::GetInstance()->query_exec_mgr()->ReleaseQueryState(query_state_);
  }
}

Status Coordinator::Exec() {
  const TQueryExecRequest& request = schedule_.request();
  DCHECK(request.plan_exec_info.size() > 0);

  needs_finalization_ = request.__isset.finalize_params;
  if (needs_finalization_) finalize_params_ = request.finalize_params;

  VLOG_QUERY << "Exec() query_id=" << schedule_.query_id()
             << " stmt=" << request.query_ctx.client_request.stmt;
  stmt_type_ = request.stmt_type;
  query_ctx_ = request.query_ctx;
  query_ctx_.__set_request_pool(schedule_.request_pool());

  query_profile_ =
      RuntimeProfile::Create(obj_pool(), "Execution Profile " + PrintId(query_id()));
  finalization_timer_ = ADD_TIMER(query_profile_, "FinalizationTimer");
  filter_updates_received_ = ADD_COUNTER(query_profile_, "FiltersReceived", TUnit::UNIT);

  SCOPED_TIMER(query_profile_->total_time_counter());

  // initialize progress updater
  const string& str = Substitute("Query $0", PrintId(query_id()));
  progress_.Init(str, schedule_.num_scan_ranges());

  // runtime filters not yet supported for mt execution
  bool is_mt_execution = request.query_ctx.client_request.query_options.mt_dop > 0;
  if (is_mt_execution) filter_mode_ = TRuntimeFilterMode::OFF;

  // to keep things simple, make async Cancel() calls wait until plan fragment
  // execution has been initiated, otherwise we might try to cancel fragment
  // execution at Impala daemons where it hasn't even started
  // TODO: revisit this, it may not be true anymore
  lock_guard<mutex> l(lock_);

  query_state_ = ExecEnv::GetInstance()->query_exec_mgr()->CreateQueryState(query_ctx_);
  query_state_->AcquireExecResourceRefcount(); // Decremented in ReleaseExecResources().
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

  // At this point, all static setup is done and all structures are initialized.
  // Only runtime-related state changes past this point (examples:
  // num_remaining_backends_, fragment instance profiles, etc.)

  StartBackendExec();
  RETURN_IF_ERROR(FinishBackendStartup());

  // set coord_instance_ and coord_sink_
  if (schedule_.GetCoordFragment() != nullptr) {
    // this blocks until all fragment instances have finished their Prepare phase
    coord_instance_ = query_state_->GetFInstanceState(query_id());
    if (coord_instance_ == nullptr) {
      // at this point, the query is done with the Prepare phase, and we expect
      // to have a coordinator instance, but coord_instance_ == nullptr,
      // which means we failed Prepare
      Status prepare_status = query_state_->WaitForPrepare();
      DCHECK(!prepare_status.ok());
      return prepare_status;
    }

    // When GetFInstanceState() returns the coordinator instance, the Prepare phase
    // is done and the FragmentInstanceState's root sink will be set up. At that point,
    // the coordinator must be sure to call root_sink()->CloseConsumer(); the
    // fragment instance's executor will not complete until that point.
    // TODO: what does this mean?
    // TODO: Consider moving this to Wait().
    // TODO: clarify need for synchronization on this event
    DCHECK(coord_instance_->IsPrepared() && coord_instance_->WaitForPrepare().ok());
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
  backend_states_.resize(num_backends);

  RuntimeProfile::Counter* num_backends_counter =
      ADD_COUNTER(query_profile_, "NumBackends", TUnit::UNIT);
  num_backends_counter->Set(num_backends);

  // create BackendStates
  bool has_coord_fragment = schedule_.GetCoordFragment() != nullptr;
  const TNetworkAddress& coord_address = ExecEnv::GetInstance()->backend_address();
  int backend_idx = 0;
  for (const auto& entry: schedule_.per_backend_exec_params()) {
    if (has_coord_fragment && coord_address == entry.first) {
      coord_backend_idx_ = backend_idx;
    }
    BackendState* backend_state = obj_pool()->Add(
        new BackendState(query_id(), backend_idx, filter_mode_));
    backend_state->Init(entry.second, fragment_stats_, obj_pool());
    backend_states_[backend_idx++] = backend_state;
  }
  DCHECK(!has_coord_fragment || coord_backend_idx_ != -1);
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
      int num_instances =
          schedule.GetFragmentExecParams(fragment.idx).instance_exec_params.size();
      for (const TPlanNode& node: plan.nodes) {
        node_id_to_idx_map[node.node_id] = thrift_exec_summary.nodes.size();
        thrift_exec_summary.nodes.emplace_back();
        TPlanNodeExecSummary& node_summary = thrift_exec_summary.nodes.back();
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
  DCHECK(schedule_.request().query_ctx.client_request.query_options.mt_dop == 0);
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "InitFilterRoutingTable() called although runtime filters are disabled";
  DCHECK(!filter_routing_table_complete_)
      << "InitFilterRoutingTable() called after setting filter_routing_table_complete_";

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
  exec_complete_barrier_.reset(new CountingBarrier(num_backends));
  num_remaining_backends_ = num_backends;

  DebugOptions debug_options(schedule_.query_options());

  VLOG_QUERY << "starting execution on " << num_backends << " backends for query_id="
             << query_id();
  query_events_->MarkEvent(Substitute("Ready to start on $0 backends", num_backends));

  for (BackendState* backend_state: backend_states_) {
    ExecEnv::GetInstance()->exec_rpc_thread_pool()->Offer(
        [backend_state, this, &debug_options]() {
          backend_state->Exec(query_ctx_, debug_options, filter_routing_table_,
            exec_complete_barrier_.get());
        });
  }

  exec_complete_barrier_->Wait();
  VLOG_QUERY << "started execution on " << num_backends << " backends for query_id="
             << query_id();
  query_events_->MarkEvent(
      Substitute("All $0 execution backends ($1 fragment instances) started",
        num_backends, schedule_.GetNumFragmentInstances()));
}

Status Coordinator::FinishBackendStartup() {
  Status status = Status::OK();
  const TMetricDef& def =
      MakeTMetricDef("backend-startup-latencies", TMetricKind::HISTOGRAM, TUnit::TIME_MS);
  // Capture up to 30 minutes of start-up times, in ms, with 4 s.f. accuracy.
  HistogramMetric latencies(def, 30 * 60 * 1000, 4);
  for (BackendState* backend_state: backend_states_) {
    // preserve the first non-OK, if there is one
    Status backend_status = backend_state->GetStatus();
    if (!backend_status.ok() && status.ok()) status = backend_status;
    latencies.Update(backend_state->rpc_latency());
  }

  query_profile_->AddInfoString(
      "Backend startup latencies", latencies.ToHumanReadable());

  if (!status.ok()) {
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

Status Coordinator::GetStatus() {
  lock_guard<mutex> l(lock_);
  return query_status_;
}

Status Coordinator::UpdateStatus(const Status& status, const string& backend_hostname,
    bool is_fragment_failure, const TUniqueId& instance_id) {
  {
    lock_guard<mutex> l(lock_);

    // The query is done and we are just waiting for backends to clean up.
    // Ignore their cancelled updates.
    if (returned_all_results_ && status.IsCancelled()) return query_status_;

    // nothing to update
    if (status.ok()) return query_status_;

    // don't override an error status; also, cancellation has already started
    if (!query_status_.ok()) return query_status_;

    query_status_ = status;
    CancelInternal();
  }

  if (is_fragment_failure) {
    // Log the id of the fragment that first failed so we can track it down more easily.
    VLOG_QUERY << "query_id=" << query_id() << " failed because fragment_instance_id="
               << instance_id << " on host=" << backend_hostname << " failed.";
  } else {
    VLOG_QUERY << "query_id=" << query_id() << " failed due to error on host="
               << backend_hostname;
  }
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
      if (info != nullptr) {
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
  HdfsTableDescriptor* hdfs_table;
  RETURN_IF_ERROR(DescriptorTbl::CreateHdfsTblDescriptor(query_ctx_.desc_tbl,
      finalize_params_.table_id, obj_pool(), &hdfs_table));
  DCHECK(hdfs_table != nullptr)
      << "INSERT target table not known in descriptor table: "
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
      DCHECK(part != nullptr)
          << "table_id=" << hdfs_table->id() << " partition_id=" << partition.second.id
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
        if (existing_files == nullptr && errno == EAGAIN) {
          errno = 0;
          existing_files =
              hdfsListDirectory(partition_fs_connection, part_path.c_str(), &num_files);
        }
        // hdfsListDirectory() returns nullptr not only when there is an error but also
        // when the directory is empty(HDFS-8407). Need to check errno to make sure
        // the call fails.
        if (existing_files == nullptr && errno != 0) {
          return Status(GetHdfsErrorMsg("Could not list directory: ", part_path));
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

  // We're done with the HDFS descriptor - free up its resources.
  hdfs_table->ReleaseResources();
  hdfs_table = nullptr;

  {
    SCOPED_TIMER(ADD_CHILD_TIMER(query_profile_, "Overwrite/PartitionCreationTimer",
          "FinalizationTimer"));
    if (!partition_create_ops.Execute(
        ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
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
    if (!move_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
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
    if (!dir_deletion_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
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
    if (!chmod_ops.Execute(ExecEnv::GetInstance()->hdfs_op_thread_pool(), false)) {
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

  VLOG_QUERY << "Finalizing query: " << query_id();
  SCOPED_TIMER(finalization_timer_);
  Status return_status = GetStatus();
  if (return_status.ok()) {
    return_status = FinalizeSuccessfulInsert();
  }

  stringstream staging_dir;
  DCHECK(finalize_params_.__isset.staging_dir);
  staging_dir << finalize_params_.staging_dir << "/" << PrintId(query_id(),"_") << "/";

  hdfsFS hdfs_conn;
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(staging_dir.str(), &hdfs_conn));
  VLOG_QUERY << "Removing staging directory: " << staging_dir.str();
  hdfsDelete(hdfs_conn, staging_dir.str().c_str(), 1);

  return return_status;
}

Status Coordinator::WaitForBackendCompletion() {
  unique_lock<mutex> l(lock_);
  while (num_remaining_backends_ > 0 && query_status_.ok()) {
    VLOG_QUERY << "Coordinator waiting for backends to finish, "
               << num_remaining_backends_ << " remaining. query_id=" << query_id();
    backend_completion_cv_.Wait(l);
  }
  if (query_status_.ok()) {
    VLOG_QUERY << "All backends finished successfully. query_id=" << query_id();
  } else {
    VLOG_QUERY << "All backends finished due to one or more errors. query_id="
               << query_id() << ". " << query_status_.GetDetail();
  }

  return query_status_;
}

Status Coordinator::Wait() {
  lock_guard<mutex> l(wait_lock_);
  SCOPED_TIMER(query_profile_->total_time_counter());
  if (has_called_wait_) return Status::OK();
  has_called_wait_ = true;

  if (stmt_type_ == TStmtType::QUERY) {
    DCHECK(coord_instance_ != nullptr);
    return UpdateStatus(coord_instance_->WaitForOpen(), FLAGS_hostname, true,
        runtime_state()->fragment_instance_id());
  }

  DCHECK_EQ(stmt_type_, TStmtType::DML);
  // Query finalization can only happen when all backends have reported
  // relevant state. They only have relevant state to report in the parallel
  // INSERT case, otherwise all the relevant state is from the coordinator
  // fragment which will be available after Open() returns.
  // Ignore the returned status if finalization is required., since FinalizeQuery() will
  // pick it up and needs to execute regardless.
  Status status = WaitForBackendCompletion();
  if (!needs_finalization_ && !status.ok()) return status;

  // Execution of query fragments has finished. We don't need to hold onto query execution
  // resources while we finalize the query.
  ReleaseExecResources();
  // Query finalization is required only for HDFS table sinks
  if (needs_finalization_) RETURN_IF_ERROR(FinalizeQuery());
  // Release admission control resources after we'd done the potentially heavyweight
  // finalization.
  ReleaseAdmissionControlResources();

  query_profile_->AddInfoString(
      "DML Stats", DataSink::OutputDmlStats(per_partition_status_, "\n"));
  // For DML queries, when Wait is done, the query is complete.
  ComputeQuerySummary();
  return status;
}

Status Coordinator::GetNext(QueryResultSet* results, int max_rows, bool* eos) {
  VLOG_ROW << "GetNext() query_id=" << query_id();
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
  RETURN_IF_ERROR(UpdateStatus(status, FLAGS_hostname, true,
      runtime_state()->fragment_instance_id()));

  if (*eos) {
    returned_all_results_ = true;
    query_events_->MarkEvent("Last row fetched");
    // release query execution resources here, since we won't be fetching more result rows
    ReleaseExecResources();
    // wait for all backends to complete before computing the summary
    // TODO: relocate this so GetNext() won't have to wait for backends to complete?
    RETURN_IF_ERROR(WaitForBackendCompletion());
    // Release admission control resources after backends are finished.
    ReleaseAdmissionControlResources();
    // if the query completed successfully, compute the summary
    if (query_status_.ok()) ComputeQuerySummary();
  }

  return Status::OK();
}

void Coordinator::Cancel(const Status* cause) {
  lock_guard<mutex> l(lock_);
  // if the query status indicates an error, cancellation has already been initiated;
  // prevent others from cancelling a second time
  if (!query_status_.ok()) return;

  // TODO: This should default to OK(), not CANCELLED if there is no cause (or callers
  // should explicitly pass Status::OK()). Fragment instances may be cancelled at the end
  // of a successful query. Need to clean up relationship between query_status_ here and
  // in QueryExecState. See IMPALA-4279.
  query_status_ = (cause != nullptr && !cause->ok()) ? *cause : Status::CANCELLED;
  CancelInternal();
}

void Coordinator::CancelInternal() {
  VLOG_QUERY << "Cancel() query_id=" << query_id();
  // TODO: remove when restructuring cancellation, which should happen automatically
  // as soon as the coordinator knows that the query is finished
  DCHECK(!query_status_.ok());

  int num_cancelled = 0;
  for (BackendState* backend_state: backend_states_) {
    DCHECK(backend_state != nullptr);
    if (backend_state->Cancel()) ++num_cancelled;
  }
  VLOG_QUERY << Substitute(
      "CancelBackends() query_id=$0, tried to cancel $1 backends",
      PrintId(query_id()), num_cancelled);
  backend_completion_cv_.NotifyAll();

  ReleaseExecResourcesLocked();
  ReleaseAdmissionControlResourcesLocked();
  // Report the summary with whatever progress the query made before being cancelled.
  ComputeQuerySummary();
}

Status Coordinator::UpdateBackendExecStatus(const TReportExecStatusParams& params) {
  VLOG_FILE << "UpdateBackendExecStatus()  backend_idx=" << params.coord_state_idx;
  if (params.coord_state_idx >= backend_states_.size()) {
    return Status(TErrorCode::INTERNAL_ERROR,
        Substitute("Unknown backend index $0 (max known: $1)",
            params.coord_state_idx, backend_states_.size() - 1));
  }
  BackendState* backend_state = backend_states_[params.coord_state_idx];
  // TODO: return here if returned_all_results_?
  // TODO: return CANCELLED in that case? Although that makes the cancellation propagation
  // path more irregular.

  // TODO: only do this when the sink is done; probably missing a done field
  // in TReportExecStatus for that
  if (params.__isset.insert_exec_status) {
    UpdateInsertExecStatus(params.insert_exec_status);
  }

  if (backend_state->ApplyExecStatusReport(params, &exec_summary_, &progress_)) {
    // This report made this backend done, so update the status and
    // num_remaining_backends_.

    // for now, abort the query if we see any error except if returned_all_results_ is
    // true (UpdateStatus() initiates cancellation, if it hasn't already been)
    // TODO: clarify control flow here, it's unclear we should even process this status
    // report if returned_all_results_ is true
    bool is_fragment_failure;
    TUniqueId failed_instance_id;
    Status status = backend_state->GetStatus(&is_fragment_failure, &failed_instance_id);
    if (!status.ok() && !returned_all_results_) {
      Status ignored =
          UpdateStatus(status, TNetworkAddressToString(backend_state->impalad_address()),
              is_fragment_failure, failed_instance_id);
      return Status::OK();
    }

    lock_guard<mutex> l(lock_);
    DCHECK_GT(num_remaining_backends_, 0);
    if (VLOG_QUERY_IS_ON && num_remaining_backends_ > 1) {
      VLOG_QUERY << "Backend completed: "
          << " host=" << backend_state->impalad_address()
          << " remaining=" << num_remaining_backends_ - 1
          << " query_id=" << query_id();
      BackendState::LogFirstInProgress(backend_states_);
    }
    if (--num_remaining_backends_ == 0 || !status.ok()) {
      backend_completion_cv_.NotifyAll();
    }
    return Status::OK();
  }
  // If all results have been returned, return a cancelled status to force the fragment
  // instance to stop executing.
  if (returned_all_results_) return Status::CANCELLED;

  return Status::OK();
}

void Coordinator::UpdateInsertExecStatus(const TInsertExecStatus& insert_exec_status) {
  lock_guard<mutex> l(lock_);
  for (const PartitionStatusMap::value_type& partition:
       insert_exec_status.per_partition_status) {
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
      insert_exec_status.files_to_move.begin(), insert_exec_status.files_to_move.end());
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
  return coord_instance_ == nullptr ? nullptr : coord_instance_->runtime_state();
}

bool Coordinator::PrepareCatalogUpdate(TUpdateCatalogRequest* catalog_update) {
  // Assume we are called only after all fragments have completed
  DCHECK(has_called_wait_);

  for (const PartitionStatusMap::value_type& partition: per_partition_status_) {
    catalog_update->created_partitions.insert(partition.first);
  }

  return catalog_update->created_partitions.size() != 0;
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

  stringstream info;
  for (BackendState* backend_state: backend_states_) {
    info << backend_state->impalad_address() << "("
         << PrettyPrinter::Print(backend_state->GetPeakConsumption(), TUnit::BYTES)
         << ") ";
  }
  query_profile_->AddInfoString("Per Node Peak Memory Usage", info.str());
}

string Coordinator::GetErrorLog() {
  ErrorLogMap merged;
  for (BackendState* state: backend_states_) {
    state->MergeErrorLog(&merged);
  }
  return PrintErrorMapToString(merged);
}

void Coordinator::ReleaseExecResources() {
  lock_guard<mutex> l(lock_);
  ReleaseExecResourcesLocked();
}

void Coordinator::ReleaseExecResourcesLocked() {
  if (released_exec_resources_) return;
  released_exec_resources_ = true;
  if (filter_routing_table_.size() > 0) {
    query_profile_->AddInfoString("Final filter table", FilterDebugString());
  }

  {
    lock_guard<SpinLock> l(filter_lock_);
    for (auto& filter : filter_routing_table_) {
      FilterState* state = &filter.second;
      state->Disable(filter_mem_tracker_);
    }
  }
  // This may be NULL while executing UDFs.
  if (filter_mem_tracker_ != nullptr) filter_mem_tracker_->Close();
  // Need to protect against failed Prepare(), where root_sink() would not be set.
  if (coord_sink_ != nullptr) coord_sink_->CloseConsumer();
  // Now that we've released our own resources, can release query-wide resources.
  if (query_state_ != nullptr) query_state_->ReleaseExecResourceRefcount();
  // At this point some tracked memory may still be used in the coordinator for result
  // caching. The query MemTracker will be cleaned up later.
}

void Coordinator::ReleaseAdmissionControlResources() {
  lock_guard<mutex> l(lock_);
  ReleaseAdmissionControlResourcesLocked();
}

void Coordinator::ReleaseAdmissionControlResourcesLocked() {
  if (released_admission_control_resources_) return;
  LOG(INFO) << "Release admission control resources for query_id="
            << PrintId(query_ctx_.query_id);
  AdmissionController* admission_controller =
      ExecEnv::GetInstance()->admission_controller();
  if (admission_controller != nullptr) admission_controller->ReleaseQuery(schedule_);
  released_admission_control_resources_ = true;
  query_events_->MarkEvent("Released admission control resources");
}

void Coordinator::UpdateFilter(const TUpdateFilterParams& params) {
  DCHECK_NE(filter_mode_, TRuntimeFilterMode::OFF)
      << "UpdateFilter() called although runtime filters are disabled";
  DCHECK(exec_complete_barrier_.get() != nullptr)
      << "Filters received before fragments started!";
  exec_complete_barrier_->Wait();
  DCHECK(filter_routing_table_complete_)
      << "Filter received before routing table complete";

  TPublishFilterParams rpc_params;
  unordered_set<int> target_fragment_idxs;
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
      filter_mem_tracker_->Release(aggregated_filter.directory.size());

      // TODO: Track memory used by 'rpc_params'.
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

  for (BackendState* bs: backend_states_) {
    for (int fragment_idx: target_fragment_idxs) {
      rpc_params.__set_dst_fragment_idx(fragment_idx);
      bs->PublishFilter(rpc_params);
    }
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
                   << " (query_id=" << coord->query_id() << ")";
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
      MinMaxFilter::Or(params.min_max_filter, &min_max_filter_);
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

const TUniqueId& Coordinator::query_id() const {
  return query_ctx_.query_id;
}

void Coordinator::GetTExecSummary(TExecSummary* exec_summary) {
  lock_guard<SpinLock> l(exec_summary_.lock);
  *exec_summary = exec_summary_.thrift_exec_summary;
}

MemTracker* Coordinator::query_mem_tracker() const {
  return query_state()->query_mem_tracker();
}

void Coordinator::BackendsToJson(Document* doc) {
  Value states(kArrayType);
  {
    lock_guard<mutex> l(lock_);
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
    lock_guard<mutex> l(lock_);
    for (BackendState* state : backend_states_) {
      Value val(kObjectType);
      state->InstanceStatsToJson(&val, doc);
      states.PushBack(val, doc->GetAllocator());
    }
  }
  doc->AddMember("backend_instances", states, doc->GetAllocator());
}
}
