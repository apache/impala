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

#include "scheduling/scheduler.h"

#include <algorithm>
#include <random>
#include <vector>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
#include <boost/unordered_set.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "flatbuffers/flatbuffers.h"
#include "gen-cpp/ImpalaInternalService_constants.h"
#include "gen-cpp/Types_types.h"
#include "runtime/exec-env.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/hash-ring.h"
#include "statestore/statestore-subscriber.h"
#include "thirdparty/pcg-cpp-0.98/include/pcg_random.hpp"
#include "util/container-util.h"
#include "util/flat_buffer.h"
#include "util/hash-util.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"

#include "common/names.h"

using boost::algorithm::join;
using namespace apache::thrift;
using namespace org::apache::impala::fb;
using namespace strings;

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");

Scheduler::Scheduler(ClusterMembershipMgr* cluster_membership_mgr,
    MetricGroup* metrics, RequestPoolService* request_pool_service)
  : metrics_(metrics->GetOrCreateChildGroup("scheduler")),
    cluster_membership_mgr_(cluster_membership_mgr),
    request_pool_service_(request_pool_service) {
  LOG(INFO) << "Starting scheduler";
  if (metrics_ != nullptr) {
    total_assignments_ = metrics_->AddCounter(ASSIGNMENTS_KEY, 0);
    total_local_assignments_ = metrics_->AddCounter(LOCAL_ASSIGNMENTS_KEY, 0);
    initialized_ = metrics_->AddProperty(SCHEDULER_INIT_KEY, true);
  }
}

const TBackendDescriptor& Scheduler::LookUpBackendDesc(
    const ExecutorConfig& executor_config, const TNetworkAddress& host) {
  const TBackendDescriptor* desc = executor_config.group.LookUpBackendDesc(host);
  if (desc == nullptr) {
    // Local host may not be in executor_config's executor group if it's a dedicated
    // coordinator.
    const TBackendDescriptor& local_be_desc = executor_config.local_be_desc;
    DCHECK(host == local_be_desc.address);
    DCHECK(!local_be_desc.is_executor);
    desc = &local_be_desc;
  }
  return *desc;
}

Status Scheduler::GenerateScanRanges(const vector<TFileSplitGeneratorSpec>& specs,
    vector<TScanRangeLocationList>* generated_scan_ranges) {
  for (const auto& spec : specs) {
    // Converts the spec to one or more scan ranges.
    const FbFileDesc* fb_desc =
        flatbuffers::GetRoot<FbFileDesc>(spec.file_desc.file_desc_data.c_str());
    DCHECK(fb_desc->file_blocks() == nullptr || fb_desc->file_blocks()->size() == 0);

    long scan_range_offset = 0;
    long remaining = fb_desc->length();
    long scan_range_length = std::min(spec.max_block_size, fb_desc->length());
    if (!spec.is_splittable) scan_range_length = fb_desc->length();

    while (remaining > 0) {
      THdfsFileSplit hdfs_scan_range;
      THdfsCompression::type compression;
      RETURN_IF_ERROR(FromFbCompression(fb_desc->compression(), &compression));
      hdfs_scan_range.__set_file_compression(compression);
      hdfs_scan_range.__set_file_length(fb_desc->length());
      hdfs_scan_range.__set_relative_path(fb_desc->relative_path()->str());
      hdfs_scan_range.__set_length(scan_range_length);
      hdfs_scan_range.__set_mtime(fb_desc->last_modification_time());
      hdfs_scan_range.__set_offset(scan_range_offset);
      hdfs_scan_range.__set_partition_id(spec.partition_id);
      hdfs_scan_range.__set_is_erasure_coded(fb_desc->is_ec());
      hdfs_scan_range.__set_partition_path_hash(spec.partition_path_hash);
      TScanRange scan_range;
      scan_range.__set_hdfs_file_split(hdfs_scan_range);
      TScanRangeLocationList scan_range_list;
      scan_range_list.__set_scan_range(scan_range);

      generated_scan_ranges->push_back(scan_range_list);
      scan_range_offset += scan_range_length;
      remaining -= scan_range_length;
      scan_range_length = (scan_range_length > remaining ? remaining : scan_range_length);
    }
  }

  return Status::OK();
}

Status Scheduler::ComputeScanRangeAssignment(
    const ExecutorConfig& executor_config, QuerySchedule* schedule) {
  RuntimeProfile::Counter* total_assignment_timer =
      ADD_TIMER(schedule->summary_profile(), "ComputeScanRangeAssignmentTimer");
  const TQueryExecRequest& exec_request = schedule->request();
  for (const TPlanExecInfo& plan_exec_info : exec_request.plan_exec_info) {
    for (const auto& entry : plan_exec_info.per_node_scan_ranges) {
      const TPlanNodeId node_id = entry.first;
      const TPlanFragment& fragment = schedule->GetContainingFragment(node_id);
      bool exec_at_coord = (fragment.partition.type == TPartitionType::UNPARTITIONED);

      const TPlanNode& node = schedule->GetNode(node_id);
      DCHECK_EQ(node.node_id, node_id);

      bool has_preference =
          node.__isset.hdfs_scan_node && node.hdfs_scan_node.__isset.replica_preference;
      const TReplicaPreference::type* node_replica_preference = has_preference ?
          &node.hdfs_scan_node.replica_preference :
          nullptr;
      bool node_random_replica = node.__isset.hdfs_scan_node
          && node.hdfs_scan_node.__isset.random_replica
          && node.hdfs_scan_node.random_replica;

      FragmentScanRangeAssignment* assignment =
          &schedule->GetFragmentExecParams(fragment.idx)->scan_range_assignment;

      const vector<TScanRangeLocationList>* locations = nullptr;
      vector<TScanRangeLocationList> expanded_locations;
      if (entry.second.split_specs.empty()) {
        // directly use the concrete ranges.
        locations = &entry.second.concrete_ranges;
      } else {
        // union concrete ranges and expanded specs.
        expanded_locations.insert(expanded_locations.end(),
            entry.second.concrete_ranges.begin(), entry.second.concrete_ranges.end());
        RETURN_IF_ERROR(
            GenerateScanRanges(entry.second.split_specs, &expanded_locations));
        locations = &expanded_locations;
      }
      DCHECK(locations != nullptr);
      RETURN_IF_ERROR(
          ComputeScanRangeAssignment(executor_config, node_id, node_replica_preference,
              node_random_replica, *locations, exec_request.host_list, exec_at_coord,
              schedule->query_options(), total_assignment_timer, assignment));
      schedule->IncNumScanRanges(locations->size());
    }
  }
  return Status::OK();
}

void Scheduler::ComputeFragmentExecParams(
    const ExecutorConfig& executor_config, QuerySchedule* schedule) {
  const TQueryExecRequest& exec_request = schedule->request();

  // for each plan, compute the FInstanceExecParams for the tree of fragments
  for (const TPlanExecInfo& plan_exec_info : exec_request.plan_exec_info) {
    // set instance_id, host, per_node_scan_ranges
    ComputeFragmentExecParams(executor_config, plan_exec_info,
        schedule->GetFragmentExecParams(plan_exec_info.fragments[0].idx), schedule);

    // Set destinations, per_exch_num_senders, sender_id.
    for (const TPlanFragment& src_fragment : plan_exec_info.fragments) {
      if (!src_fragment.output_sink.__isset.stream_sink) continue;
      FragmentIdx dest_idx =
          schedule->GetFragmentIdx(src_fragment.output_sink.stream_sink.dest_node_id);
      DCHECK_LT(dest_idx, plan_exec_info.fragments.size());
      const TPlanFragment& dest_fragment = plan_exec_info.fragments[dest_idx];
      FragmentExecParams* dest_params =
          schedule->GetFragmentExecParams(dest_fragment.idx);
      FragmentExecParams* src_params = schedule->GetFragmentExecParams(src_fragment.idx);

      // populate src_params->destinations
      src_params->destinations.resize(dest_params->instance_exec_params.size());
      for (int i = 0; i < dest_params->instance_exec_params.size(); ++i) {
        TPlanFragmentDestination& dest = src_params->destinations[i];
        dest.__set_fragment_instance_id(dest_params->instance_exec_params[i].instance_id);
        const TNetworkAddress& host = dest_params->instance_exec_params[i].host;
        dest.__set_thrift_backend(host);
        const TBackendDescriptor& desc = LookUpBackendDesc(executor_config, host);
        DCHECK(desc.__isset.krpc_address);
        DCHECK(IsResolvedAddress(desc.krpc_address));
        dest.__set_krpc_backend(desc.krpc_address);
      }

      // enumerate senders consecutively;
      // for distributed merge we need to enumerate senders across fragment instances
      const TDataStreamSink& sink = src_fragment.output_sink.stream_sink;
      DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
          || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
          || sink.output_partition.type == TPartitionType::RANDOM
          || sink.output_partition.type == TPartitionType::KUDU);
      PlanNodeId exch_id = sink.dest_node_id;
      int sender_id_base = dest_params->per_exch_num_senders[exch_id];
      dest_params->per_exch_num_senders[exch_id] +=
          src_params->instance_exec_params.size();
      for (int i = 0; i < src_params->instance_exec_params.size(); ++i) {
        FInstanceExecParams& src_instance_params = src_params->instance_exec_params[i];
        src_instance_params.sender_id = sender_id_base + i;
      }
    }
  }
}

void Scheduler::ComputeFragmentExecParams(const ExecutorConfig& executor_config,
    const TPlanExecInfo& plan_exec_info, FragmentExecParams* fragment_params,
    QuerySchedule* schedule) {
  // traverse input fragments
  for (FragmentIdx input_fragment_idx : fragment_params->input_fragments) {
    ComputeFragmentExecParams(executor_config, plan_exec_info,
        schedule->GetFragmentExecParams(input_fragment_idx), schedule);
  }

  const TPlanFragment& fragment = fragment_params->fragment;
  // case 1: single instance executed at coordinator
  if (fragment.partition.type == TPartitionType::UNPARTITIONED) {
    const TBackendDescriptor& local_be_desc = executor_config.local_be_desc;
    const TNetworkAddress& coord = local_be_desc.address;
    DCHECK(local_be_desc.__isset.krpc_address);
    const TNetworkAddress& krpc_coord = local_be_desc.krpc_address;
    DCHECK(IsResolvedAddress(krpc_coord));
    // make sure that the coordinator instance ends up with instance idx 0
    TUniqueId instance_id = fragment_params->is_coord_fragment
        ? schedule->query_id()
        : schedule->GetNextInstanceId();
    fragment_params->instance_exec_params.emplace_back(
        instance_id, coord, krpc_coord, 0, *fragment_params);
    FInstanceExecParams& instance_params = fragment_params->instance_exec_params.back();

    // That instance gets all of the scan ranges, if there are any.
    if (!fragment_params->scan_range_assignment.empty()) {
      DCHECK_EQ(fragment_params->scan_range_assignment.size(), 1);
      auto first_entry = fragment_params->scan_range_assignment.begin();
      instance_params.per_node_scan_ranges = first_entry->second;
    }

    return;
  }

  if (ContainsNode(fragment.plan, TPlanNodeType::UNION_NODE)) {
    CreateUnionInstances(executor_config, fragment_params, schedule);
    return;
  }

  PlanNodeId leftmost_scan_id = FindLeftmostScan(fragment.plan);
  if (leftmost_scan_id != g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID) {
    // case 2: leaf fragment with leftmost scan
    // TODO: check that there's only one scan in this fragment
    CreateScanInstances(executor_config, leftmost_scan_id, fragment_params, schedule);
  } else {
    // case 3: interior fragment without leftmost scan
    // we assign the same hosts as those of our leftmost input fragment (so that a
    // merge aggregation fragment runs on the hosts that provide the input data)
    CreateCollocatedInstances(fragment_params, schedule);
  }
}

void Scheduler::CreateUnionInstances(const ExecutorConfig& executor_config,
    FragmentExecParams* fragment_params, QuerySchedule* schedule) {
  const TPlanFragment& fragment = fragment_params->fragment;
  DCHECK(ContainsNode(fragment.plan, TPlanNodeType::UNION_NODE));

  // Add hosts of scan nodes.
  vector<TPlanNodeType::type> scan_node_types{TPlanNodeType::HDFS_SCAN_NODE,
      TPlanNodeType::HBASE_SCAN_NODE, TPlanNodeType::DATA_SOURCE_NODE,
      TPlanNodeType::KUDU_SCAN_NODE};
  vector<TPlanNodeId> scan_node_ids;
  FindNodes(fragment.plan, scan_node_types, &scan_node_ids);
  vector<TNetworkAddress> scan_hosts;
  for (TPlanNodeId id : scan_node_ids) {
    GetScanHosts(executor_config.local_be_desc, id, *fragment_params, &scan_hosts);
  }

  unordered_set<TNetworkAddress> hosts(scan_hosts.begin(), scan_hosts.end());

  // Add hosts of input fragments.
  for (FragmentIdx idx : fragment_params->input_fragments) {
    const FragmentExecParams& input_params = *schedule->GetFragmentExecParams(idx);
    for (const FInstanceExecParams& instance_params : input_params.instance_exec_params) {
      hosts.insert(instance_params.host);
    }
  }
  DCHECK(!hosts.empty()) << "no hosts for fragment " << fragment.idx
                         << " with a UnionNode";

  // create a single instance per host
  // TODO-MT: figure out how to parallelize Union
  int per_fragment_idx = 0;
  for (const TNetworkAddress& host : hosts) {
    const TBackendDescriptor& backend_descriptor = LookUpBackendDesc(
        executor_config, host);
    DCHECK(backend_descriptor.__isset.krpc_address);
    const TNetworkAddress& krpc_host = backend_descriptor.krpc_address;
    DCHECK(IsResolvedAddress(krpc_host));
    fragment_params->instance_exec_params.emplace_back(schedule->GetNextInstanceId(),
        host, krpc_host, per_fragment_idx++, *fragment_params);
    // assign all scan ranges
    FInstanceExecParams& instance_params = fragment_params->instance_exec_params.back();
    if (fragment_params->scan_range_assignment.count(host) > 0) {
      instance_params.per_node_scan_ranges = fragment_params->scan_range_assignment[host];
    }
  }
}

void Scheduler::CreateScanInstances(const ExecutorConfig& executor_config,
    PlanNodeId leftmost_scan_id, FragmentExecParams* fragment_params,
    QuerySchedule* schedule) {
  int max_num_instances =
      schedule->request().query_ctx.client_request.query_options.mt_dop;
  if (max_num_instances == 0) max_num_instances = 1;

  if (fragment_params->scan_range_assignment.empty()) {
    const TBackendDescriptor& local_be_desc = executor_config.local_be_desc;
    DCHECK(local_be_desc.__isset.krpc_address);
    DCHECK(IsResolvedAddress(local_be_desc.krpc_address));
    // this scan doesn't have any scan ranges: run a single instance on the coordinator
    fragment_params->instance_exec_params.emplace_back(schedule->GetNextInstanceId(),
        local_be_desc.address, local_be_desc.krpc_address, 0,
        *fragment_params);
    return;
  }

  int per_fragment_instance_idx = 0;
  for (const auto& assignment_entry : fragment_params->scan_range_assignment) {
    // evenly divide up the scan ranges of the leftmost scan between at most
    // <dop> instances
    const TNetworkAddress& host = assignment_entry.first;
    const TBackendDescriptor& backend_descriptor = LookUpBackendDesc(
        executor_config, host);
    DCHECK(backend_descriptor.__isset.krpc_address);
    TNetworkAddress krpc_host = backend_descriptor.krpc_address;
    DCHECK(IsResolvedAddress(krpc_host));
    auto scan_ranges_it = assignment_entry.second.find(leftmost_scan_id);
    DCHECK(scan_ranges_it != assignment_entry.second.end());
    const vector<TScanRangeParams>& params_list = scan_ranges_it->second;

    int64 total_size = 0;
    for (const TScanRangeParams& params : params_list) {
      if (params.scan_range.__isset.hdfs_file_split) {
        total_size += params.scan_range.hdfs_file_split.length;
      } else {
        // fake load-balancing for Kudu and Hbase: every split has length 1
        // TODO: implement more accurate logic for Kudu and Hbase
        ++total_size;
      }
    }

    // try to load-balance scan ranges by assigning just beyond the average number of
    // bytes to each instance
    // TODO: fix shortcomings introduced by uneven split sizes,
    // this could end up assigning 0 scan ranges to an instance
    int num_instances = ::min(max_num_instances, static_cast<int>(params_list.size()));
    DCHECK_GT(num_instances, 0);
    float avg_bytes_per_instance = static_cast<float>(total_size) / num_instances;
    int64_t total_assigned_bytes = 0;
    int params_idx = 0; // into params_list
    for (int i = 0; i < num_instances; ++i) {
      fragment_params->instance_exec_params.emplace_back(schedule->GetNextInstanceId(),
          host, krpc_host, per_fragment_instance_idx++, *fragment_params);
      FInstanceExecParams& instance_params = fragment_params->instance_exec_params.back();

      // Threshold beyond which we want to assign to the next instance.
      int64_t threshold_total_bytes = avg_bytes_per_instance * (i + 1);

      // Assign each scan range in params_list. When the per-instance threshold is
      // reached, move on to the next instance.
      while (params_idx < params_list.size()) {
        const TScanRangeParams& scan_range_params = params_list[params_idx];
        instance_params.per_node_scan_ranges[leftmost_scan_id].push_back(
            scan_range_params);
        if (scan_range_params.scan_range.__isset.hdfs_file_split) {
          total_assigned_bytes += scan_range_params.scan_range.hdfs_file_split.length;
        } else {
          // for Kudu and Hbase every split has length 1
          ++total_assigned_bytes;
        }
        ++params_idx;
        // If this assignment pushes this instance past the threshold, move on to the next
        // instance. However, if this is the last instance, assign any remaining scan
        // ranges here since there are no further instances to load-balance across. There
        // may be leftover scan ranges because threshold_total_bytes only approximates the
        // per-node byte threshold.
        if (total_assigned_bytes >= threshold_total_bytes && i != num_instances - 1) {
          break;
        }
      }
      if (params_idx == params_list.size()) break; // nothing left to assign
    }
    DCHECK_EQ(params_idx, params_list.size()); // everything got assigned
    DCHECK_EQ(total_assigned_bytes, total_size);
  }
}

void Scheduler::CreateCollocatedInstances(
    FragmentExecParams* fragment_params, QuerySchedule* schedule) {
  DCHECK_GE(fragment_params->input_fragments.size(), 1);
  const FragmentExecParams* input_fragment_params =
      schedule->GetFragmentExecParams(fragment_params->input_fragments[0]);
  int per_fragment_instance_idx = 0;
  for (const FInstanceExecParams& input_instance_params :
      input_fragment_params->instance_exec_params) {
    fragment_params->instance_exec_params.emplace_back(schedule->GetNextInstanceId(),
        input_instance_params.host, input_instance_params.krpc_host,
        per_fragment_instance_idx++, *fragment_params);
  }
}

Status Scheduler::ComputeScanRangeAssignment(const ExecutorConfig& executor_config,
    PlanNodeId node_id, const TReplicaPreference::type* node_replica_preference,
    bool node_random_replica, const vector<TScanRangeLocationList>& locations,
    const vector<TNetworkAddress>& host_list, bool exec_at_coord,
    const TQueryOptions& query_options, RuntimeProfile::Counter* timer,
    FragmentScanRangeAssignment* assignment) {
  const ExecutorGroup& executor_group = executor_config.group;
  if (executor_group.NumExecutors() == 0 && !exec_at_coord) {
    return Status(TErrorCode::NO_REGISTERED_BACKENDS);
  }

  SCOPED_TIMER(timer);
  // We adjust all replicas with memory distance less than base_distance to base_distance
  // and collect all replicas with equal or better distance as candidates. For a full list
  // of memory distance classes see TReplicaPreference in PlanNodes.thrift.
  TReplicaPreference::type base_distance = query_options.replica_preference;

  // A preference attached to the plan node takes precedence.
  if (node_replica_preference) base_distance = *node_replica_preference;

  // Between otherwise equivalent executors we optionally break ties by comparing their
  // random rank.
  bool random_replica = query_options.schedule_random_replica || node_random_replica;

  // TODO: Build this one from executor_group
  ExecutorGroup coord_only_executor_group;
  const TBackendDescriptor& local_be_desc = executor_config.local_be_desc;
  coord_only_executor_group.AddExecutor(local_be_desc);
  VLOG_QUERY << "Exec at coord is " << (exec_at_coord ? "true" : "false");
  AssignmentCtx assignment_ctx(
      exec_at_coord ? coord_only_executor_group : executor_group, total_assignments_,
      total_local_assignments_);

  // Holds scan ranges that must be assigned for remote reads.
  vector<const TScanRangeLocationList*> remote_scan_range_locations;

  // Loop over all scan ranges, select an executor for those with local impalads and
  // collect all others for later processing.
  for (const TScanRangeLocationList& scan_range_locations : locations) {
    TReplicaPreference::type min_distance = TReplicaPreference::REMOTE;

    // Select executor for the current scan range.
    if (exec_at_coord) {
      DCHECK(assignment_ctx.executor_group().LookUpExecutorIp(
          local_be_desc.address.hostname, nullptr));
      assignment_ctx.RecordScanRangeAssignment(local_be_desc, node_id, host_list,
          scan_range_locations, assignment);
    } else {
      // Collect executor candidates with smallest memory distance.
      vector<IpAddr> executor_candidates;
      if (base_distance < TReplicaPreference::REMOTE) {
        for (const TScanRangeLocation& location : scan_range_locations.locations) {
          const TNetworkAddress& replica_host = host_list[location.host_idx];
          // Determine the adjusted memory distance to the closest executor for the
          // replica host.
          TReplicaPreference::type memory_distance = TReplicaPreference::REMOTE;
          IpAddr executor_ip;
          bool has_local_executor = assignment_ctx.executor_group().LookUpExecutorIp(
              replica_host.hostname, &executor_ip);
          if (has_local_executor) {
            if (location.is_cached) {
              memory_distance = TReplicaPreference::CACHE_LOCAL;
            } else {
              memory_distance = TReplicaPreference::DISK_LOCAL;
            }
          } else {
            memory_distance = TReplicaPreference::REMOTE;
          }
          memory_distance = max(memory_distance, base_distance);

          // We only need to collect executor candidates for non-remote reads, as it is
          // the nature of remote reads that there is no executor available.
          if (memory_distance < TReplicaPreference::REMOTE) {
            DCHECK(has_local_executor);
            // Check if we found a closer replica than the previous ones.
            if (memory_distance < min_distance) {
              min_distance = memory_distance;
              executor_candidates.clear();
              executor_candidates.push_back(executor_ip);
            } else if (memory_distance == min_distance) {
              executor_candidates.push_back(executor_ip);
            }
          }
        }
      } // End of candidate selection.
      DCHECK(!executor_candidates.empty() || min_distance == TReplicaPreference::REMOTE);

      // Check the effective memory distance of the candidates to decide whether to treat
      // the scan range as cached.
      bool cached_replica = min_distance == TReplicaPreference::CACHE_LOCAL;

      // Pick executor based on data location.
      bool local_executor = min_distance != TReplicaPreference::REMOTE;

      if (!local_executor) {
        remote_scan_range_locations.push_back(&scan_range_locations);
        continue;
      }
      // For local reads we want to break ties by executor rank in these cases:
      // - if it is enforced via a query option.
      // - when selecting between cached replicas. In this case there is no OS buffer
      //   cache to worry about.
      // Remote reads will always break ties by executor rank.
      bool decide_local_assignment_by_rank = random_replica || cached_replica;
      const IpAddr* executor_ip = nullptr;
      executor_ip = assignment_ctx.SelectExecutorFromCandidates(
          executor_candidates, decide_local_assignment_by_rank);
      TBackendDescriptor executor;
      assignment_ctx.SelectExecutorOnHost(*executor_ip, &executor);
      assignment_ctx.RecordScanRangeAssignment(
          executor, node_id, host_list, scan_range_locations, assignment);
    } // End of executor selection.
  } // End of for loop over scan ranges.

  // Assign remote scans to executors.
  int num_remote_executor_candidates = query_options.num_remote_executor_candidates;
  for (const TScanRangeLocationList* scan_range_locations : remote_scan_range_locations) {
    DCHECK(!exec_at_coord);
    const IpAddr* executor_ip;
    vector<IpAddr> remote_executor_candidates;
    // Limit the number of remote executor candidates:
    // 1. When enabled by setting 'num_remote_executor_candidates' > 0
    // AND
    // 2. This is an HDFS file split
    // AND
    // 3. The number of remote executor candidates is less than the number of backends.
    // Otherwise, fall back to the normal method of selecting executors for remote
    // ranges, which allows for execution on any backend.
    if (scan_range_locations->scan_range.__isset.hdfs_file_split &&
        num_remote_executor_candidates > 0 &&
        num_remote_executor_candidates < executor_group.NumExecutors()) {
      assignment_ctx.GetRemoteExecutorCandidates(
          &scan_range_locations->scan_range.hdfs_file_split,
          num_remote_executor_candidates, &remote_executor_candidates);
      // Like the local case, schedule_random_replica determines how to break ties.
      executor_ip = assignment_ctx.SelectExecutorFromCandidates(
          remote_executor_candidates, random_replica);
    } else {
      executor_ip = assignment_ctx.SelectRemoteExecutor();
    }
    TBackendDescriptor executor;
    assignment_ctx.SelectExecutorOnHost(*executor_ip, &executor);
    assignment_ctx.RecordScanRangeAssignment(
        executor, node_id, host_list, *scan_range_locations, assignment);
  }

  if (VLOG_FILE_IS_ON) assignment_ctx.PrintAssignment(*assignment);

  return Status::OK();
}

PlanNodeId Scheduler::FindLeftmostNode(
    const TPlan& plan, const vector<TPlanNodeType::type>& types) {
  // the first node with num_children == 0 is the leftmost node
  int node_idx = 0;
  while (node_idx < plan.nodes.size() && plan.nodes[node_idx].num_children != 0) {
    ++node_idx;
  }
  if (node_idx == plan.nodes.size()) {
    return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
  }
  const TPlanNode& node = plan.nodes[node_idx];

  for (int i = 0; i < types.size(); ++i) {
    if (node.node_type == types[i]) return node.node_id;
  }
  return g_ImpalaInternalService_constants.INVALID_PLAN_NODE_ID;
}

PlanNodeId Scheduler::FindLeftmostScan(const TPlan& plan) {
  vector<TPlanNodeType::type> scan_node_types{TPlanNodeType::HDFS_SCAN_NODE,
      TPlanNodeType::HBASE_SCAN_NODE, TPlanNodeType::DATA_SOURCE_NODE,
      TPlanNodeType::KUDU_SCAN_NODE};
  return FindLeftmostNode(plan, scan_node_types);
}

bool Scheduler::ContainsNode(const TPlan& plan, TPlanNodeType::type type) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    if (plan.nodes[i].node_type == type) return true;
  }
  return false;
}

void Scheduler::FindNodes(const TPlan& plan, const vector<TPlanNodeType::type>& types,
    vector<TPlanNodeId>* results) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    for (int j = 0; j < types.size(); ++j) {
      if (plan.nodes[i].node_type == types[j]) {
        results->push_back(plan.nodes[i].node_id);
        break;
      }
    }
  }
}

void Scheduler::GetScanHosts(const TBackendDescriptor& local_be_desc, TPlanNodeId scan_id,
    const FragmentExecParams& params, vector<TNetworkAddress>* scan_hosts) {
  // Get the list of impalad host from scan_range_assignment_
  for (const FragmentScanRangeAssignment::value_type& scan_range_assignment :
      params.scan_range_assignment) {
    const PerNodeScanRanges& per_node_scan_ranges = scan_range_assignment.second;
    if (per_node_scan_ranges.find(scan_id) != per_node_scan_ranges.end()) {
      scan_hosts->push_back(scan_range_assignment.first);
    }
  }

  if (scan_hosts->empty()) {
    // this scan node doesn't have any scan ranges; run it on the coordinator
    // TODO: we'll need to revisit this strategy once we can partition joins
    // (in which case this fragment might be executing a right outer join
    // with a large build table)
    scan_hosts->push_back(local_be_desc.address);
    return;
  }
}

Status Scheduler::Schedule(QuerySchedule* schedule) {
  // Use a snapshot of the cluster membership state upfront to avoid using inconsistent
  // views throughout scheduling.
  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr_->GetSnapshot();
  if (membership_snapshot->local_be_desc.get() == nullptr) {
    // This can happen in the short time period after the ImpalaServer has finished
    // starting up (which makes the local backend available) and the next statestore
    // update that pulls the local backend descriptor into the membership snapshot.
    return Status("Local backend has not been registered in the cluster membership");
  }
  const string& group_name = ClusterMembershipMgr::DEFAULT_EXECUTOR_GROUP;
  VLOG_QUERY << "Scheduling query " << PrintId(schedule->query_id())
      << " on executor group: " << group_name;

  auto it = membership_snapshot->executor_groups.find(group_name);
  if (it == membership_snapshot->executor_groups.end()) {
    return Status(Substitute("Unknown executor group: $0", group_name));
  }
  const ExecutorGroup& executor_group = it->second;
  ExecutorConfig executor_config =
      {executor_group, *membership_snapshot->local_be_desc};
  RETURN_IF_ERROR(ComputeScanRangeAssignment(executor_config, schedule));
  ComputeFragmentExecParams(executor_config, schedule);
  ComputeBackendExecParams(executor_config, schedule);
#ifndef NDEBUG
  schedule->Validate();
#endif

  // TODO: Move to admission control, it doesn't need to be in the Scheduler.
  string resolved_pool;
  // Re-resolve the pool name to propagate any resolution errors now that this request
  // is known to require a valid pool.
  RETURN_IF_ERROR(request_pool_service_->ResolveRequestPool(
          schedule->request().query_ctx, &resolved_pool));
  // Resolved pool name should have been set in the TQueryCtx and shouldn't have changed.
  DCHECK_EQ(resolved_pool, schedule->request_pool());
  schedule->summary_profile()->AddInfoString("Request Pool", schedule->request_pool());
  return Status::OK();
}

void Scheduler::ComputeBackendExecParams(
    const ExecutorConfig& executor_config, QuerySchedule* schedule) {
  PerBackendExecParams per_backend_params;
  for (const FragmentExecParams& f : schedule->fragment_exec_params()) {
    for (const FInstanceExecParams& i : f.instance_exec_params) {
      BackendExecParams& be_params = per_backend_params[i.host];
      be_params.instance_params.push_back(&i);
      // Different fragments do not synchronize their Open() and Close(), so the backend
      // does not provide strong guarantees about whether one fragment instance releases
      // resources before another acquires them. Conservatively assume that all fragment
      // instances on this backend can consume their peak resources at the same time,
      // i.e. that this backend's peak resources is the sum of the per-fragment-instance
      // peak resources for the instances executing on this backend.
      be_params.min_mem_reservation_bytes += f.fragment.min_mem_reservation_bytes;
      be_params.initial_mem_reservation_total_claims +=
          f.fragment.initial_mem_reservation_total_claims;
      be_params.thread_reservation += f.fragment.thread_reservation;
    }
  }

  int64_t largest_min_reservation = 0;
  for (auto& backend : per_backend_params) {
    const TNetworkAddress& host = backend.first;
    backend.second.admit_mem_limit =
        LookUpBackendDesc(executor_config, host).admit_mem_limit;
    largest_min_reservation =
        max(largest_min_reservation, backend.second.min_mem_reservation_bytes);
  }
  schedule->set_per_backend_exec_params(per_backend_params);
  schedule->set_largest_min_reservation(largest_min_reservation);

  stringstream min_mem_reservation_ss;
  stringstream num_fragment_instances_ss;
  for (const auto& e: per_backend_params) {
    min_mem_reservation_ss << TNetworkAddressToString(e.first) << "("
         << PrettyPrinter::Print(e.second.min_mem_reservation_bytes, TUnit::BYTES)
         << ") ";
    num_fragment_instances_ss << TNetworkAddressToString(e.first) << "("
         << PrettyPrinter::Print(e.second.instance_params.size(), TUnit::UNIT)
         << ") ";
  }
  schedule->summary_profile()->AddInfoString("Per Host Min Memory Reservation",
      min_mem_reservation_ss.str());
  schedule->summary_profile()->AddInfoString("Per Host Number of Fragment Instances",
      num_fragment_instances_ss.str());
}

Scheduler::AssignmentCtx::AssignmentCtx(const ExecutorGroup& executor_group,
    IntCounter* total_assignments, IntCounter* total_local_assignments)
  : executor_group_(executor_group),
    first_unused_executor_idx_(0),
    total_assignments_(total_assignments),
    total_local_assignments_(total_local_assignments) {
  DCHECK_GT(executor_group.NumExecutors(), 0);
  random_executor_order_ = executor_group.GetAllExecutorIps();
  std::mt19937 g(rand());
  std::shuffle(random_executor_order_.begin(), random_executor_order_.end(), g);
  // Initialize inverted map for executor rank lookups
  int i = 0;
  for (const IpAddr& ip : random_executor_order_) random_executor_rank_[ip] = i++;
}

const IpAddr* Scheduler::AssignmentCtx::SelectExecutorFromCandidates(
    const std::vector<IpAddr>& data_locations, bool break_ties_by_rank) {
  DCHECK(!data_locations.empty());
  // List of candidate indexes into 'data_locations'.
  vector<int> candidates_idxs;
  // Find locations with minimum number of assigned bytes.
  int64_t min_assigned_bytes = numeric_limits<int64_t>::max();
  for (int i = 0; i < data_locations.size(); ++i) {
    const IpAddr& executor_ip = data_locations[i];
    int64_t assigned_bytes = 0;
    auto handle_it = assignment_heap_.find(executor_ip);
    if (handle_it != assignment_heap_.end()) {
      assigned_bytes = (*handle_it->second).assigned_bytes;
    }
    if (assigned_bytes < min_assigned_bytes) {
      candidates_idxs.clear();
      min_assigned_bytes = assigned_bytes;
    }
    if (assigned_bytes == min_assigned_bytes) candidates_idxs.push_back(i);
  }

  DCHECK(!candidates_idxs.empty());
  auto min_rank_idx = candidates_idxs.begin();
  if (break_ties_by_rank) {
    min_rank_idx = min_element(candidates_idxs.begin(), candidates_idxs.end(),
        [&data_locations, this](const int& a, const int& b) {
          return GetExecutorRank(data_locations[a]) < GetExecutorRank(data_locations[b]);
        });
  }
  return &data_locations[*min_rank_idx];
}

void Scheduler::AssignmentCtx::GetRemoteExecutorCandidates(
    const THdfsFileSplit* hdfs_file_split, int num_candidates,
    vector<IpAddr>* remote_executor_candidates) {
  // This should be given an empty vector
  DCHECK_EQ(remote_executor_candidates->size(), 0);
  // This function should not be used when 'num_candidates' exceeds the number
  // of executors.
  DCHECK_LT(num_candidates, executor_group_.NumExecutors());
  // Two different hashes of the filename can result in the same executor.
  // The function should return distinct executors, so it may need to do more hashes
  // than 'num_candidates'.
  set<IpAddr> distinct_backends;
  // Generate multiple hashes of the file split by using the hash as a seed to a PRNG.
  // Note: The hash includes the partition path hash, the filename (relative to the
  // partition directory), and the offset. The offset is used to allow very large files
  // that have multiple splits to be spread across more executors.
  uint32_t hash = static_cast<uint32_t>(hdfs_file_split->partition_path_hash);
  hash = HashUtil::Hash(hdfs_file_split->relative_path.data(),
      hdfs_file_split->relative_path.length(), hash);
  hash = HashUtil::Hash(&hdfs_file_split->offset, sizeof(hdfs_file_split->offset), hash);
  pcg32 prng(hash);
  // To avoid any problem scenarios, limit the total number of iterations
  int max_iterations = num_candidates * 3;
  for (int i = 0; i < max_iterations; ++i) {
    // Look up nearest IpAddr
    const IpAddr* executor_addr = executor_group_.GetHashRing()->GetNode(prng());
    DCHECK(executor_addr != nullptr);
    distinct_backends.insert(*executor_addr);
    // Short-circuit if we reach the appropriate number of replicas
    if (distinct_backends.size() == num_candidates) break;
  }
  for (const IpAddr& addr : distinct_backends) {
    remote_executor_candidates->push_back(addr);
  }
}

const IpAddr* Scheduler::AssignmentCtx::SelectRemoteExecutor() {
  const IpAddr* candidate_ip;
  if (HasUnusedExecutors()) {
    // Pick next unused executor.
    candidate_ip = GetNextUnusedExecutorAndIncrement();
  } else {
    // Pick next executor from assignment_heap. All executors must have been inserted into
    // the heap at this point.
    DCHECK_GT(executor_group_.NumExecutors(), 0);
    DCHECK_EQ(executor_group_.NumExecutors(), assignment_heap_.size());
    candidate_ip = &(assignment_heap_.top().ip);
  }
  DCHECK(candidate_ip != nullptr);
  return candidate_ip;
}

bool Scheduler::AssignmentCtx::HasUnusedExecutors() const {
  return first_unused_executor_idx_ < random_executor_order_.size();
}

const IpAddr* Scheduler::AssignmentCtx::GetNextUnusedExecutorAndIncrement() {
  DCHECK(HasUnusedExecutors());
  const IpAddr* ip = &random_executor_order_[first_unused_executor_idx_++];
  return ip;
}

int Scheduler::AssignmentCtx::GetExecutorRank(const IpAddr& ip) const {
  auto it = random_executor_rank_.find(ip);
  DCHECK(it != random_executor_rank_.end());
  return it->second;
}

void Scheduler::AssignmentCtx::SelectExecutorOnHost(
    const IpAddr& executor_ip, TBackendDescriptor* executor) {
  DCHECK(executor_group_.LookUpExecutorIp(executor_ip, nullptr));
  const ExecutorGroup::Executors& executors_on_host =
      executor_group_.GetExecutorsForHost(executor_ip);
  DCHECK(executors_on_host.size() > 0);
  if (executors_on_host.size() == 1) {
    *executor = *executors_on_host.begin();
  } else {
    ExecutorGroup::Executors::const_iterator* next_executor_on_host;
    next_executor_on_host =
        FindOrInsert(&next_executor_per_host_, executor_ip, executors_on_host.begin());
    auto eq = [next_executor_on_host](auto& elem) {
      const TBackendDescriptor& next_executor = **next_executor_on_host;
      // The IP addresses must already match, so it is sufficient to check the port.
      DCHECK_EQ(next_executor.ip_address, elem.ip_address);
      return next_executor.address.port == elem.address.port;
    };
    DCHECK(find_if(executors_on_host.begin(), executors_on_host.end(), eq)
        != executors_on_host.end());
    *executor = **next_executor_on_host;
    // Rotate
    ++(*next_executor_on_host);
    if (*next_executor_on_host == executors_on_host.end()) {
      *next_executor_on_host = executors_on_host.begin();
    }
  }
}

void Scheduler::AssignmentCtx::RecordScanRangeAssignment(
    const TBackendDescriptor& executor, PlanNodeId node_id,
    const vector<TNetworkAddress>& host_list,
    const TScanRangeLocationList& scan_range_locations,
    FragmentScanRangeAssignment* assignment) {
  int64_t scan_range_length = 0;
  if (scan_range_locations.scan_range.__isset.hdfs_file_split) {
    scan_range_length = scan_range_locations.scan_range.hdfs_file_split.length;
  } else if (scan_range_locations.scan_range.__isset.kudu_scan_token) {
    // Hack so that kudu ranges are well distributed.
    // TODO: KUDU-1133 Use the tablet size instead.
    scan_range_length = 1000;
  }

  IpAddr executor_ip;
  bool ret = executor_group_.LookUpExecutorIp(executor.address.hostname, &executor_ip);
  DCHECK(ret);
  DCHECK(!executor_ip.empty());
  assignment_heap_.InsertOrUpdate(
      executor_ip, scan_range_length, GetExecutorRank(executor_ip));

  // See if the read will be remote. This is not the case if the impalad runs on one of
  // the replica's datanodes.
  bool remote_read = true;
  // For local reads we can set volume_id and is_cached. For remote reads HDFS will
  // decide which replica to use so we keep those at default values.
  int volume_id = -1;
  bool is_cached = false;
  for (const TScanRangeLocation& location : scan_range_locations.locations) {
    const TNetworkAddress& replica_host = host_list[location.host_idx];
    IpAddr replica_ip;
    if (executor_group_.LookUpExecutorIp(replica_host.hostname, &replica_ip)
        && executor_ip == replica_ip) {
      remote_read = false;
      volume_id = location.volume_id;
      is_cached = location.is_cached;
      break;
    }
  }

  if (remote_read) {
    assignment_byte_counters_.remote_bytes += scan_range_length;
  } else {
    assignment_byte_counters_.local_bytes += scan_range_length;
    if (is_cached) assignment_byte_counters_.cached_bytes += scan_range_length;
  }

  if (total_assignments_ != nullptr) {
    DCHECK(total_local_assignments_ != nullptr);
    total_assignments_->Increment(1);
    if (!remote_read) total_local_assignments_->Increment(1);
  }

  PerNodeScanRanges* scan_ranges =
      FindOrInsert(assignment, executor.address, PerNodeScanRanges());
  vector<TScanRangeParams>* scan_range_params_list =
      FindOrInsert(scan_ranges, node_id, vector<TScanRangeParams>());
  // Add scan range.
  TScanRangeParams scan_range_params;
  scan_range_params.scan_range = scan_range_locations.scan_range;
  scan_range_params.__set_volume_id(volume_id);
  scan_range_params.__set_is_cached(is_cached);
  scan_range_params.__set_is_remote(remote_read);
  scan_range_params_list->push_back(scan_range_params);

  if (VLOG_FILE_IS_ON) {
    VLOG_FILE << "Scheduler assignment to executor: "
              << TNetworkAddressToString(executor.address) << "("
              << (remote_read ? "remote" : "local") << " selection)";
  }
}

void Scheduler::AssignmentCtx::PrintAssignment(
    const FragmentScanRangeAssignment& assignment) {
  VLOG_FILE << "Total remote scan volume = "
            << PrettyPrinter::Print(assignment_byte_counters_.remote_bytes, TUnit::BYTES);
  VLOG_FILE << "Total local scan volume = "
            << PrettyPrinter::Print(assignment_byte_counters_.local_bytes, TUnit::BYTES);
  VLOG_FILE << "Total cached scan volume = "
            << PrettyPrinter::Print(assignment_byte_counters_.cached_bytes, TUnit::BYTES);

  for (const FragmentScanRangeAssignment::value_type& entry : assignment) {
    VLOG_FILE << "ScanRangeAssignment: server=" << ThriftDebugString(entry.first);
    for (const PerNodeScanRanges::value_type& per_node_scan_ranges : entry.second) {
      stringstream str;
      for (const TScanRangeParams& params : per_node_scan_ranges.second) {
        str << ThriftDebugString(params) << " ";
      }
      VLOG_FILE << "node_id=" << per_node_scan_ranges.first << " ranges=" << str.str();
    }
  }
}

void Scheduler::AddressableAssignmentHeap::InsertOrUpdate(
    const IpAddr& ip, int64_t assigned_bytes, int rank) {
  auto handle_it = executor_handles_.find(ip);
  if (handle_it == executor_handles_.end()) {
    AssignmentHeap::handle_type handle = executor_heap_.push({assigned_bytes, rank, ip});
    executor_handles_.emplace(ip, handle);
  } else {
    // We need to rebuild the heap after every update operation. Calling decrease once is
    // sufficient as both assignments decrease the key.
    AssignmentHeap::handle_type handle = handle_it->second;
    (*handle).assigned_bytes += assigned_bytes;
    executor_heap_.decrease(handle);
  }
}
}
