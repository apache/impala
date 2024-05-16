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

#include <stdlib.h>
#include <algorithm>
#include <limits>
#include <random>
#include <unordered_map>
#include <vector>
#include <boost/unordered_set.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "gen-cpp/CatalogObjects_generated.h"
#include "gen-cpp/DataSinks_types.h"
#include "gen-cpp/ErrorCodes_types.h"
#include "gen-cpp/ImpalaInternalService_types.h"
#include "gen-cpp/Query_constants.h"
#include "gen-cpp/Types_types.h"
#include "gen-cpp/common.pb.h"
#include "gen-cpp/statestore_service.pb.h"
#include "scheduling/executor-group.h"
#include "scheduling/hash-ring.h"
#include "thirdparty/pcg-cpp-0.98/include/pcg_random.hpp"
#include "util/compression-util.h"
#include "util/debug-util.h"
#include "util/flat_buffer.h"
#include "util/hash-util.h"
#include "util/metrics.h"
#include "util/network-util.h"
#include "util/pretty-printer.h"
#include "util/runtime-profile-counters.h"
#include "util/uid-util.h"

#include "common/names.h"

using std::pop_heap;
using std::push_heap;
using namespace apache::thrift;
using namespace org::apache::impala::fb;
using namespace strings;

DEFINE_bool_hidden(sort_runtime_filter_aggregator_candidates, false,
    "Control whether to sort intermediate runtime filter aggregator candidates based on "
    "their KRPC address. Only used for testing.");

namespace impala {

static const string LOCAL_ASSIGNMENTS_KEY("simple-scheduler.local-assignments.total");
static const string ASSIGNMENTS_KEY("simple-scheduler.assignments.total");
static const string SCHEDULER_INIT_KEY("simple-scheduler.initialized");
static const string SCHEDULER_WARNING_KEY("Scheduler Warning");

static const vector<TPlanNodeType::type> SCAN_NODE_TYPES{TPlanNodeType::HDFS_SCAN_NODE,
    TPlanNodeType::HBASE_SCAN_NODE, TPlanNodeType::DATA_SOURCE_NODE,
    TPlanNodeType::KUDU_SCAN_NODE, TPlanNodeType::ICEBERG_METADATA_SCAN_NODE,
    TPlanNodeType::SYSTEM_TABLE_SCAN_NODE};

// Consistent scheduling requires picking up to k distinct candidates out of n nodes.
// Since each iteration can pick a node that it already picked (i.e. it is sampling with
// replacement), it may need more than k iterations to pick k distinct candidates.
// There is also no guaranteed bound on the number of iterations. To protect against
// bugs and large numbers of iterations, we limit the number of iterations. This constant
// determines the number of iterations per distinct candidate allowed. Eight iterations
// per distinct candidate provides a very high probability of actually getting k distinct
// candidates. See GetRemoteExecutorCandidates() for a deeper description.
static const int MAX_ITERATIONS_PER_EXECUTOR_CANDIDATE = 8;

const string Scheduler::PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION =
    "Per Host Min Memory Reservation";

Scheduler::Scheduler(MetricGroup* metrics, RequestPoolService* request_pool_service)
  : metrics_(metrics->GetOrCreateChildGroup("scheduler")),
    request_pool_service_(request_pool_service) {
  LOG(INFO) << "Starting scheduler";
  if (metrics_ != nullptr) {
    total_assignments_ = metrics_->AddCounter(ASSIGNMENTS_KEY, 0);
    total_local_assignments_ = metrics_->AddCounter(LOCAL_ASSIGNMENTS_KEY, 0);
    initialized_ = metrics_->AddProperty(SCHEDULER_INIT_KEY, true);
  }
}

const BackendDescriptorPB& Scheduler::LookUpBackendDesc(
    const ExecutorConfig& executor_config, const NetworkAddressPB& host) {
  const BackendDescriptorPB* desc = executor_config.group.LookUpBackendDesc(host);
  if (desc == nullptr) {
    if (executor_config.all_coords.NumExecutors() > 0) {
      // Group containing all coordinators was provided, so use that for lookup.
      desc = executor_config.all_coords.LookUpBackendDesc(host);
      DCHECK_NE(nullptr, desc);
    } else {
      // Coordinator host may not be in executor_config's executor group if it's a
      // dedicated coordinator, or if it is configured to be in a different executor
      // group.
      const BackendDescriptorPB& coord_desc = executor_config.coord_desc;
      DCHECK(host == coord_desc.address());
      desc = &coord_desc;
    }
  }
  return *desc;
}

NetworkAddressPB Scheduler::LookUpKrpcHost(
    const ExecutorConfig& executor_config, const NetworkAddressPB& backend_host) {
  const BackendDescriptorPB& backend_descriptor =
      LookUpBackendDesc(executor_config, backend_host);
  DCHECK(backend_descriptor.has_krpc_address());
  const NetworkAddressPB& krpc_host = backend_descriptor.krpc_address();
  DCHECK(IsResolvedAddress(krpc_host));
  return krpc_host;
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
    long scan_range_length = fb_desc->length();

    if (spec.is_splittable) {
      scan_range_length = std::min(spec.max_block_size, fb_desc->length());
      if (spec.is_footer_only) {
        scan_range_offset = fb_desc->length() - scan_range_length;
        remaining = scan_range_length;
      }
    } else {
      DCHECK(!spec.is_footer_only);
    }

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
      hdfs_scan_range.__set_partition_path_hash(spec.partition_path_hash);
      hdfs_scan_range.__set_is_encrypted(fb_desc->is_encrypted());
      hdfs_scan_range.__set_is_erasure_coded(fb_desc->is_ec());
      if (fb_desc->absolute_path() != nullptr) {
        hdfs_scan_range.__set_absolute_path(fb_desc->absolute_path()->str());
      }
      TScanRange scan_range;
      scan_range.__set_hdfs_file_split(hdfs_scan_range);
      if (spec.file_desc.__isset.file_metadata) {
        scan_range.__set_file_metadata(spec.file_desc.file_metadata);
      }
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
    const ExecutorConfig& executor_config, ScheduleState* state) {
  RuntimeProfile::Counter* total_assignment_timer =
      ADD_TIMER(state->summary_profile(), "ComputeScanRangeAssignmentTimer");
  const TQueryExecRequest& exec_request = state->request();
  for (const TPlanExecInfo& plan_exec_info : exec_request.plan_exec_info) {
    for (const auto& entry : plan_exec_info.per_node_scan_ranges) {
      const TPlanNodeId node_id = entry.first;
      const TPlanFragment& fragment = state->GetContainingFragment(node_id);
      bool exec_at_coord = (fragment.partition.type == TPartitionType::UNPARTITIONED);
      DCHECK(executor_config.group.NumExecutors() > 0 || exec_at_coord);

      const TPlanNode& node = state->GetNode(node_id);
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
          &state->GetFragmentScheduleState(fragment.idx)->scan_range_assignment;

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
              state->query_options(), total_assignment_timer, state->rng(),
              state->summary_profile(), assignment));
      state->IncNumScanRanges(locations->size());
    }
  }
  return Status::OK();
}

Status Scheduler::ComputeFragmentExecParams(
    const ExecutorConfig& executor_config, ScheduleState* state) {
  const TQueryExecRequest& exec_request = state->request();

  // for each plan, compute the FInstanceScheduleStates for the tree of fragments.
  // The plans are in dependency order, so we compute parameters for each plan
  // *before* its input join build plans. This allows the join build plans to
  // be easily co-located with the plans consuming their output.
  for (const TPlanExecInfo& plan_exec_info : exec_request.plan_exec_info) {
    // set instance_id, host, per_node_scan_ranges
    RETURN_IF_ERROR(ComputeFragmentExecParams(executor_config, plan_exec_info,
        state->GetFragmentScheduleState(plan_exec_info.fragments[0].idx), state));

    // Set destinations, per_exch_num_senders, sender_id.
    for (const TPlanFragment& src_fragment : plan_exec_info.fragments) {
      VLOG(3) << "Computing exec params for fragment " << src_fragment.display_name;
      if (!src_fragment.output_sink.__isset.stream_sink
          && !src_fragment.output_sink.__isset.join_build_sink) {
        continue;
      }

      FragmentScheduleState* src_state =
          state->GetFragmentScheduleState(src_fragment.idx);
      if (state->query_options().runtime_filter_mode == TRuntimeFilterMode::GLOBAL
          && src_fragment.produced_runtime_filters_reservation_bytes > 0) {
        ComputeRandomKrpcForAggregation(executor_config, state, src_state,
            state->query_options().max_num_filters_aggregated_per_host);
      }

      if (!src_fragment.output_sink.__isset.stream_sink) continue;
      FragmentIdx dest_idx =
          state->GetFragmentIdx(src_fragment.output_sink.stream_sink.dest_node_id);
      FragmentScheduleState* dest_state = state->GetFragmentScheduleState(dest_idx);

      // populate src_state->destinations
      for (int i = 0; i < dest_state->instance_states.size(); ++i) {
        PlanFragmentDestinationPB* dest = src_state->exec_params->add_destinations();
        *dest->mutable_fragment_instance_id() =
            dest_state->instance_states[i].exec_params.instance_id();
        const NetworkAddressPB& host = dest_state->instance_states[i].host;
        *dest->mutable_address() = host;
        const BackendDescriptorPB& desc = LookUpBackendDesc(executor_config, host);
        DCHECK(desc.has_krpc_address());
        DCHECK(IsResolvedAddress(desc.krpc_address()));
        *dest->mutable_krpc_backend() = desc.krpc_address();
      }

      // enumerate senders consecutively;
      // for distributed merge we need to enumerate senders across fragment instances
      const TDataStreamSink& sink = src_fragment.output_sink.stream_sink;
      DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED
          || sink.output_partition.type == TPartitionType::HASH_PARTITIONED
          || sink.output_partition.type == TPartitionType::RANDOM
          || sink.output_partition.type == TPartitionType::KUDU
          || sink.output_partition.type == TPartitionType::DIRECTED);
      PlanNodeId exch_id = sink.dest_node_id;
      google::protobuf::Map<int32_t, int32_t>* per_exch_num_senders =
          dest_state->exec_params->mutable_per_exch_num_senders();
      int sender_id_base = (*per_exch_num_senders)[exch_id];
      (*per_exch_num_senders)[exch_id] += src_state->instance_states.size();
      for (int i = 0; i < src_state->instance_states.size(); ++i) {
        FInstanceScheduleState& src_instance_state = src_state->instance_states[i];
        src_instance_state.exec_params.set_sender_id(sender_id_base + i);
      }
    }
  }
  return Status::OK();
}

// Helper type to map instance indexes to aggregator indexes.
typedef vector<pair<int, int>> InstanceToAggPairs;

void Scheduler::ComputeRandomKrpcForAggregation(const ExecutorConfig& executor_config,
    ScheduleState* state, FragmentScheduleState* src_state, int num_filters_per_host) {
  if (num_filters_per_host <= 1) return;
  // src_state->instance_states organize fragment instances as one dimension vector
  // where instances scheduled in same host is placed in adjacent indices.
  // Group instance indices from common host to 'instance_groups' by comparing
  // krpc address between adjacent element of instance_states.
  const NetworkAddressPB& coord_address = executor_config.coord_desc.krpc_address();
  vector<InstanceToAggPairs> instance_groups;
  InstanceToAggPairs coordinator_instances;
  for (int i = 0; i < src_state->instance_states.size(); ++i) {
    const NetworkAddressPB& krpc_address = src_state->instance_states[i].krpc_host;
    if (KrpcAddressEqual(krpc_address, coord_address)) {
      coordinator_instances.emplace_back(i, 0);
    } else {
      if (i == 0
          || !KrpcAddressEqual(
              krpc_address, src_state->instance_states[i - 1].krpc_host)) {
        // This is the first fragment instance scheduled in a host.
        // Append empty InstanceToAggPairs to 'instance_groups'.
        instance_groups.emplace_back();
      }
      instance_groups.back().emplace_back(i, 0);
    }
  }

  int num_non_coordinator_host = instance_groups.size();
  if (num_non_coordinator_host == 0) return;

  // Select number of intermediate aggregator so that each aggregator will receive
  // runtime filter update from at most 'num_filters_per_host' executors.
  int num_agg = (int)ceil((double)num_non_coordinator_host / num_filters_per_host);
  DCHECK_GT(num_agg, 0);

  if (UNLIKELY(FLAGS_sort_runtime_filter_aggregator_candidates)) {
    sort(instance_groups.begin(), instance_groups.end(),
        [src_state](InstanceToAggPairs a, InstanceToAggPairs b) {
          int idx_a = a[0].first;
          int idx_b = b[0].first;
          return CompareNetworkAddressPB(src_state->instance_states[idx_a].krpc_host,
                     src_state->instance_states[idx_b].krpc_host)
              < 0;
        });
  } else {
    std::shuffle(instance_groups.begin(), instance_groups.end(), *state->rng());
  }
  if (coordinator_instances.size() > 0) {
    // Put coordinator group behind so that coordinator won't be selected as intermediate
    // aggregator.
    instance_groups.push_back(coordinator_instances);
  }

  RuntimeFilterAggregatorInfoPB* agg_info =
      src_state->exec_params->mutable_filter_agg_info();
  agg_info->set_num_aggregators(num_agg);

  int group_idx = 0;
  int agg_idx = -1;
  InstanceToAggPairs instance_to_agg;
  vector<int> num_reporting_hosts(num_agg, 0);
  for (auto group : instance_groups) {
    const NetworkAddressPB& host = src_state->instance_states[group[0].first].host;
    if (group_idx < num_agg) {
      // First 'num_agg' of 'instance_groups' host selected as intermediate aggregator.
      const BackendDescriptorPB& desc = LookUpBackendDesc(executor_config, host);
      NetworkAddressPB* address = agg_info->add_aggregator_krpc_addresses();
      *address = desc.address();
      DCHECK(desc.has_krpc_address());
      DCHECK(IsResolvedAddress(desc.krpc_address()));
      NetworkAddressPB* krpc_address = agg_info->add_aggregator_krpc_backends();
      *krpc_address = desc.krpc_address();
    }
    agg_idx = (agg_idx + 1) % num_agg;
    num_reporting_hosts[agg_idx] += 1;
    for (auto entry : group) {
      entry.second = agg_idx;
      instance_to_agg.push_back(entry);
    }
    ++group_idx;
  }

  // Sort 'instance_to_agg' based on the original instance order to populate
  // 'aggregator_idx_to_report'.
  sort(instance_to_agg.begin(), instance_to_agg.end());
  DCHECK_EQ(src_state->instance_states.size(), instance_to_agg.size());
  for (auto entry : instance_to_agg) {
    agg_info->add_aggregator_idx_to_report(entry.second);
  }

  // Write how many host is expected to report to each intermediate aggregator,
  // including the aggregator host itself.
  for (auto num_reporters : num_reporting_hosts) {
    agg_info->add_num_reporter_per_aggregator(num_reporters);
  }
}

static inline void initializeSchedulerWarning(RuntimeProfile* summary_profile) {
  if (summary_profile->GetInfoString(SCHEDULER_WARNING_KEY) == nullptr) {
    summary_profile->AddInfoString(SCHEDULER_WARNING_KEY,
        "Cluster membership might changed between planning and scheduling");
  }
}

Status Scheduler::CheckEffectiveInstanceCount(
    const FragmentScheduleState* fragment_state, ScheduleState* state) {
  // These checks are only intended if COMPUTE_PROCESSING_COST=true.
  if (!state->query_options().compute_processing_cost) return Status::OK();

  int effective_instance_count = fragment_state->fragment.effective_instance_count;
  DCHECK_GT(effective_instance_count, 0);
  if (effective_instance_count < fragment_state->instance_states.size()) {
    initializeSchedulerWarning(state->summary_profile());
    string warn_message = Substitute(
        "$0 scheduled instance count ($1) is higher than its effective count ($2)",
        fragment_state->fragment.display_name, fragment_state->instance_states.size(),
        effective_instance_count);
    state->summary_profile()->AppendInfoString(SCHEDULER_WARNING_KEY, warn_message);
    LOG(WARNING) << warn_message;
  }

  DCHECK(!fragment_state->instance_states.empty());
  // Find host with the largest instance assignment.
  // Initialize to the first fragment instance.
  int largest_inst_idx = 0;
  int largest_inst_per_host = 1;
  int this_inst_count = 1;
  int num_host = 1;
  for (int i = 1; i < fragment_state->instance_states.size(); i++) {
    if (fragment_state->instance_states[i].host
        == fragment_state->instance_states[i - 1].host) {
      this_inst_count++;
    } else {
      if (largest_inst_per_host < this_inst_count) {
        largest_inst_per_host = this_inst_count;
        largest_inst_idx = i - 1;
      }
      this_inst_count = 1;
      num_host++;
    }
  }
  if (largest_inst_per_host < this_inst_count) {
    largest_inst_per_host = this_inst_count;
    largest_inst_idx = fragment_state->instance_states.size() - 1;
  }

  QueryConstants qc;
  if (largest_inst_per_host > qc.MAX_FRAGMENT_INSTANCES_PER_NODE) {
    return Status(Substitute(
        "$0 scheduled instance count ($1) is higher than maximum instances per node"
        " ($2), indicating a planner bug. Consider running the query with"
        " COMPUTE_PROCESSING_COST=false.",
        fragment_state->fragment.display_name, largest_inst_per_host,
        qc.MAX_FRAGMENT_INSTANCES_PER_NODE));
  }

  int planned_inst_per_host = ceil((float)effective_instance_count / num_host);
  if (largest_inst_per_host > planned_inst_per_host) {
    LOG(WARNING) << fragment_state->fragment.display_name
                 << " has imbalance number of instance to host assignment."
                 << " Consider running the query with COMPUTE_PROCESSING_COST=false."
                 << " Host " << fragment_state->instance_states[largest_inst_idx].host
                 << " has " << largest_inst_per_host << " instances assigned."
                 << " effective_instance_count=" << effective_instance_count
                 << " planned_inst_per_host=" << planned_inst_per_host
                 << " num_host=" << num_host;
  }
  return Status::OK();
}

Status Scheduler::ComputeFragmentExecParams(const ExecutorConfig& executor_config,
    const TPlanExecInfo& plan_exec_info, FragmentScheduleState* fragment_state,
    ScheduleState* state) {
  // Create exec params for child fragments connected by an exchange. Instance creation
  // for this fragment depends on where the input fragment instances are scheduled.
  for (FragmentIdx input_fragment_idx : fragment_state->exchange_input_fragments) {
    RETURN_IF_ERROR(ComputeFragmentExecParams(executor_config, plan_exec_info,
        state->GetFragmentScheduleState(input_fragment_idx), state));
  }

  const TPlanFragment& fragment = fragment_state->fragment;
  if (fragment.output_sink.__isset.join_build_sink) {
    // case 0: join build fragment, co-located with its parent fragment. Join build
    // fragments may be unpartitioned if they are co-located with the root fragment.
    VLOG(3) << "Computing exec params for collocated join build fragment "
            << fragment_state->fragment.display_name;
    CreateCollocatedJoinBuildInstances(fragment_state, state);
  } else if (fragment.partition.type == TPartitionType::UNPARTITIONED) {
    // case 1: unpartitioned fragment - either the coordinator fragment at the root of
    // the plan that must be executed at the coordinator or an unpartitioned fragment
    // that can be executed anywhere.
    VLOG(3) << "Computing exec params for "
            << (fragment_state->is_root_coord_fragment
                ? "root coordinator" : "unpartitioned")
            << " fragment " << fragment_state->fragment.display_name;
    NetworkAddressPB host;
    NetworkAddressPB krpc_host;
    if (fragment_state->is_root_coord_fragment || fragment.is_coordinator_only ||
        executor_config.group.NumExecutors() == 0) {
      // 1. The root coordinator fragment ('fragment_state->is_root_coord_fragment') must
      // be scheduled on the coordinator.
      // 2. Some other fragments ('fragment.is_coordinator_only'), such as
      // Iceberg metadata scanner fragments, must also run on the coordinator.
      // 3. Otherwise if no executors are available, we need to schedule on the
      // coordinator.
      const BackendDescriptorPB& coord_desc = executor_config.coord_desc;
      host = coord_desc.address();
      DCHECK(coord_desc.has_krpc_address());
      krpc_host = coord_desc.krpc_address();
    } else if (fragment_state->exchange_input_fragments.size() > 0) {
      // Interior unpartitioned fragments can be scheduled on an arbitrary executor.
      // Pick a random instance from the first input fragment.
      const FragmentScheduleState& input_fragment_state =
          *state->GetFragmentScheduleState(fragment_state->exchange_input_fragments[0]);
      int num_input_instances = input_fragment_state.instance_states.size();
      int instance_idx =
          std::uniform_int_distribution<int>(0, num_input_instances - 1)(*state->rng());
      host = input_fragment_state.instance_states[instance_idx].host;
      krpc_host = input_fragment_state.instance_states[instance_idx].krpc_host;
    } else {
      // Other fragments, e.g. ones with only a constant union or empty set, are scheduled
      // on a random executor.
      vector<BackendDescriptorPB> all_executors =
          executor_config.group.GetAllExecutorDescriptors();
      int idx = std::uniform_int_distribution<int>(0, all_executors.size() - 1)(
          *state->rng());
      const BackendDescriptorPB& be_desc = all_executors[idx];
      host = be_desc.address();
      DCHECK(be_desc.has_krpc_address());
      krpc_host = be_desc.krpc_address();
    }
    VLOG(3) << "Scheduled unpartitioned fragment on " << krpc_host;
    DCHECK(IsResolvedAddress(krpc_host));
    // make sure that the root coordinator instance ends up with instance idx 0
    UniqueIdPB instance_id = fragment_state->is_root_coord_fragment ?
        state->query_id() :
        state->GetNextInstanceId();
    fragment_state->instance_states.emplace_back(
        instance_id, host, krpc_host, 0, *fragment_state);
    FInstanceScheduleState& instance_state = fragment_state->instance_states.back();
    *fragment_state->exec_params->add_instances() = instance_id;

    // That instance gets all of the scan ranges, if there are any.
    if (!fragment_state->scan_range_assignment.empty()) {
      DCHECK_EQ(fragment_state->scan_range_assignment.size(), 1);
      auto first_entry = fragment_state->scan_range_assignment.begin();
      for (const PerNodeScanRanges::value_type& entry : first_entry->second) {
        instance_state.AddScanRanges(entry.first, entry.second);
      }
    }
  } else if (ContainsUnionNode(fragment.plan) || ContainsScanNode(fragment.plan)) {
    VLOG(3) << "Computing exec params for scan and/or union fragment.";
    // case 2: leaf fragment (i.e. no input fragments) with a single scan node.
    // case 3: union fragment, which may have scan nodes and may have input fragments.
    CreateCollocatedAndScanInstances(executor_config, fragment_state, state);
  } else {
    VLOG(3) << "Computing exec params for interior fragment.";
    // case 4: interior (non-leaf) fragment without a scan or union.
    // We assign the same hosts as those of our leftmost input fragment (so that a
    // merge aggregation fragment runs on the hosts that provide the input data) OR
    // follow the effective_instance_count specified by planner.
    CreateInputCollocatedInstances(fragment_state, state);
  }
  return CheckEffectiveInstanceCount(fragment_state, state);
}

// Maybe the easiest way to understand the objective of this algorithm is as a
// generalization of two simpler instance creation algorithms that decide how many
// instances of a fragment to create on each node, given a set of nodes that were
// already chosen by previous scheduling steps:
// 1. Instance creation for an interior fragment (i.e. a fragment without scans)
//    without a UNION, where we create one finstance for each instance of the leftmost
//    input fragment.
// 2. Instance creation for a fragment with a single scan and no UNION, where we create
//    finstances on each host with a scan range, with one finstance per scan range,
//    up to mt_dop finstances.
//
// This algorithm more-or-less creates the union of all finstances that would be created
// by applying the above algorithms to each input fragment and scan node. I.e. the
// parallelism on each host is the max of the parallelism that would result from each
// of the above algorithms. Note that step 1 is modified to run on fragments with union
// nodes, by considering all input fragments and not just the leftmost because we expect
// unions to be symmetrical for purposes of planning, unlike joins.
//
// The high-level steps are:
// 1. Compute the set of hosts that this fragment should run on and the parallelism on
//    each host.
// 2. Instantiate finstances on each host.
//    a) Map scan ranges to finstances for each scan node with AssignRangesToInstances().
//    b) Create the finstances, based on the computed parallelism and assign the scan
//       ranges to it.
void Scheduler::CreateCollocatedAndScanInstances(const ExecutorConfig& executor_config,
    FragmentScheduleState* fragment_state, ScheduleState* state) {
  const TPlanFragment& fragment = fragment_state->fragment;
  bool has_union = ContainsUnionNode(fragment.plan);
  DCHECK(has_union || ContainsScanNode(fragment.plan));
  // Build a map of hosts to the num instances this fragment should have, before we take
  // into account scan ranges. If this fragment has input fragments, we always run with
  // at least the same num instances as the input fragment.
  std::unordered_map<NetworkAddressPB, int> instances_per_host;

  // Add hosts of input fragments, counting the number of instances of the fragment.
  // Only do this if there's a union - otherwise only consider the parallelism of
  // the input scan, for consistency with the previous behaviour of only using
  // the parallelism of the scan.
  if (has_union) {
    for (FragmentIdx idx : fragment_state->exchange_input_fragments) {
      std::unordered_map<NetworkAddressPB, int> input_fragment_instances_per_host;
      const FragmentScheduleState& input_state = *state->GetFragmentScheduleState(idx);
      for (const FInstanceScheduleState& instance_state : input_state.instance_states) {
        ++input_fragment_instances_per_host[instance_state.host];
      }
      // Merge with the existing hosts by taking the max num instances.
      if (instances_per_host.empty()) {
        // Optimization for the common case of one input fragment.
        instances_per_host = move(input_fragment_instances_per_host);
      } else {
        for (auto& entry : input_fragment_instances_per_host) {
          int& num_instances = instances_per_host[entry.first];
          num_instances = max(num_instances, entry.second);
        }
      }
    }
  }

  // Add hosts of scan nodes.
  vector<TPlanNodeId> scan_node_ids = FindScanNodes(fragment.plan);
  DCHECK(has_union || scan_node_ids.size() == 1) << "This method may need revisiting "
      << "for plans with no union and multiple scans per fragment";
  vector<NetworkAddressPB> scan_hosts;
  GetScanHosts(scan_node_ids, *fragment_state, &scan_hosts);
  if (scan_hosts.empty() && instances_per_host.empty()) {
    // None of the scan nodes have any scan ranges and there is no input fragment feeding
    // into this fragment; run it on a random executor.
    // TODO TODO: the TODO below seems partially stale
    // TODO: we'll need to revisit this strategy once we can partition joins
    // (in which case this fragment might be executing a right outer join
    // with a large build table)
    vector<BackendDescriptorPB> all_executors =
        executor_config.group.GetAllExecutorDescriptors();
    int idx = std::uniform_int_distribution<int>(0, all_executors.size() - 1)(
        *state->rng());
    const BackendDescriptorPB& be_desc = all_executors[idx];
    scan_hosts.push_back(be_desc.address());
  }
  for (const NetworkAddressPB& host_addr : scan_hosts) {
    // Ensure that the num instances is at least as many as input fragments. We don't
    // want to increment if there were already some instances from the input fragment,
    // since that could result in too high a num_instances.
    int& host_num_instances = instances_per_host[host_addr];
    host_num_instances = max(1, host_num_instances);
  }
  DCHECK(!instances_per_host.empty())
      << "no hosts for fragment " << fragment.idx << " (" << fragment.display_name << ")";

  const TQueryOptions& request_query_option =
      state->request().query_ctx.client_request.query_options;
  int max_num_instances = 1;
  if (request_query_option.compute_processing_cost) {
    // Numbers should follow 'effective_instance_count' from planner.
    // 'effective_instance_count' is total instances across all executors.
    DCHECK_GT(fragment.effective_instance_count, 0);
    int inst_per_host =
        ceil((float)fragment.effective_instance_count / instances_per_host.size());
    max_num_instances = max(1, inst_per_host);
  } else {
    // Number of instances should be bounded by mt_dop.
    max_num_instances = max(1, request_query_option.mt_dop);
  }

  // Track the index of the next instance to be created for this fragment.
  int per_fragment_instance_idx = 0;
  for (const auto& entry : instances_per_host) {
    const NetworkAddressPB& host = entry.first;
    NetworkAddressPB krpc_host = LookUpKrpcHost(executor_config, host);
    FragmentScanRangeAssignment& sra = fragment_state->scan_range_assignment;
    auto assignment_it = sra.find(host);
    // One entry in outer vector per scan node in 'scan_node_ids'.
    // The inner vectors are the output of AssignRangesToInstances().
    // The vector may be ragged - i.e. different nodes have different numbers
    // of instances.
    vector<vector<vector<ScanRangeParamsPB>>> per_scan_per_instance_ranges;
    for (TPlanNodeId scan_node_id : scan_node_ids) {
      // Ensure empty list is created if no scan ranges are scheduled on this host.
      per_scan_per_instance_ranges.emplace_back();
      if (assignment_it == sra.end()) continue;
      auto scan_ranges_it = assignment_it->second.find(scan_node_id);
      if (scan_ranges_it == assignment_it->second.end()) continue;
      per_scan_per_instance_ranges.back() =
          AssignRangesToInstances(max_num_instances, scan_ranges_it->second);
      DCHECK_LE(per_scan_per_instance_ranges.back().size(), max_num_instances);
    }

    // The number of instances to create, based on the scan with the most ranges and
    // the input parallelism that we computed for the host above.
    int num_instances = entry.second;
    DCHECK_GE(num_instances, 1);
    DCHECK_LE(num_instances, max_num_instances)
        << "Input parallelism too high. Fragment " << fragment.display_name
        << " num_host=" << instances_per_host.size()
        << " scan_host_count=" << scan_hosts.size()
        << " mt_dop=" << request_query_option.mt_dop
        << " compute_processing_cost=" << request_query_option.compute_processing_cost
        << " effective_instance_count=" << fragment.effective_instance_count;
    for (const auto& per_scan_ranges : per_scan_per_instance_ranges) {
      num_instances = max(num_instances, static_cast<int>(per_scan_ranges.size()));
    }

    // Create the finstances. Must do this even if there are no scan range assignments
    // because we may have other input fragments.
    int host_finstance_start_idx = fragment_state->instance_states.size();
    for (int i = 0; i < num_instances; ++i) {
      UniqueIdPB instance_id = state->GetNextInstanceId();
      fragment_state->instance_states.emplace_back(
          instance_id, host, krpc_host, per_fragment_instance_idx++, *fragment_state);
      *fragment_state->exec_params->add_instances() = instance_id;
    }
    // Allocate scan ranges to the finstances if needed. Currently we simply allocate
    // them to fragment instances in order. This may be suboptimal in cases where the
    // amount of work is somewhat uneven in each scan and we could even out the overall
    // amount of work by shuffling the ranges between finstances.
    for (int scan_idx = 0; scan_idx < per_scan_per_instance_ranges.size(); ++scan_idx) {
      const auto& per_instance_ranges = per_scan_per_instance_ranges[scan_idx];
      for (int inst_idx = 0; inst_idx < per_instance_ranges.size(); ++inst_idx) {
        FInstanceScheduleState& instance_state =
            fragment_state->instance_states[host_finstance_start_idx + inst_idx];
        instance_state.AddScanRanges(
            scan_node_ids[scan_idx], per_instance_ranges[inst_idx]);
      }
    }
  }
  if (IsExceedMaxFsWriters(fragment_state, fragment_state, state)) {
    LOG(WARNING) << "Extra table sink instances scheduled, probably due to mismatch of "
                    "cluster state during planning vs scheduling. Expected: "
                 << state->query_options().max_fs_writers
                 << " Found: " << fragment_state->instance_states.size();
  }
}

vector<vector<ScanRangeParamsPB>> Scheduler::AssignRangesToInstances(
    int max_num_instances, vector<ScanRangeParamsPB>& ranges) {
  DCHECK_GT(max_num_instances, 0);
  int num_instances = min(max_num_instances, static_cast<int>(ranges.size()));
  vector<vector<ScanRangeParamsPB>> per_instance_ranges(num_instances);
  if (num_instances < 2) {
    // Short-circuit the assignment algorithm for the single instance case.
    per_instance_ranges[0] = ranges;
  } else {
    int idx = 0;
    for (auto& range : ranges) {
      per_instance_ranges[idx].push_back(range);
      idx = (idx + 1 == num_instances) ? 0 : idx + 1;
    }
  }
  return per_instance_ranges;
}

void Scheduler::CreateInputCollocatedInstances(
    FragmentScheduleState* fragment_state, ScheduleState* state) {
  DCHECK_GE(fragment_state->exchange_input_fragments.size(), 1);
  const TPlanFragment& fragment = fragment_state->fragment;
  const FragmentScheduleState& input_fragment_state =
      *state->GetFragmentScheduleState(fragment_state->exchange_input_fragments[0]);
  int per_fragment_instance_idx = 0;

  int max_instances = input_fragment_state.instance_states.size();
  if (fragment.effective_instance_count > 0) {
    max_instances = fragment.effective_instance_count;
  } else if (IsExceedMaxFsWriters(fragment_state, &input_fragment_state, state)) {
    max_instances = state->query_options().max_fs_writers;
  }

  if (max_instances != input_fragment_state.instance_states.size()) {
    std::unordered_set<std::pair<NetworkAddressPB, NetworkAddressPB>> all_hosts;
    for (const FInstanceScheduleState& input_instance_state :
        input_fragment_state.instance_states) {
      all_hosts.insert({input_instance_state.host, input_instance_state.krpc_host});
    }
    // This implementation creates the desired number of instances while balancing them
    // across hosts and ensuring that instances on the same host get consecutive instance
    // indexes.
    int num_hosts = all_hosts.size();
    int instances_per_host = max_instances / num_hosts;
    int remainder = max_instances % num_hosts;
    auto host_itr = all_hosts.begin();
    for (int i = 0; i < num_hosts; i++) {
      for (int j = 0; j < instances_per_host + (i < remainder); ++j) {
        UniqueIdPB instance_id = state->GetNextInstanceId();
        fragment_state->instance_states.emplace_back(instance_id, host_itr->first,
            host_itr->second, per_fragment_instance_idx++, *fragment_state);
        *fragment_state->exec_params->add_instances() = instance_id;
      }
      if (host_itr != all_hosts.end()) host_itr++;
    }
  } else {
    for (const FInstanceScheduleState& input_instance_state :
        input_fragment_state.instance_states) {
      UniqueIdPB instance_id = state->GetNextInstanceId();
      fragment_state->instance_states.emplace_back(instance_id, input_instance_state.host,
          input_instance_state.krpc_host, per_fragment_instance_idx++, *fragment_state);
      *fragment_state->exec_params->add_instances() = instance_id;
    }
  }
}

void Scheduler::CreateCollocatedJoinBuildInstances(
    FragmentScheduleState* fragment_state, ScheduleState* state) {
  const TPlanFragment& fragment = fragment_state->fragment;
  DCHECK(fragment.output_sink.__isset.join_build_sink);
  const TJoinBuildSink& sink = fragment.output_sink.join_build_sink;
  int join_fragment_idx = state->GetFragmentIdx(sink.dest_node_id);
  FragmentScheduleState* join_fragment_state =
      state->GetFragmentScheduleState(join_fragment_idx);
  DCHECK(!join_fragment_state->instance_states.empty())
      << "Parent fragment instances must already be created.";
  vector<FInstanceScheduleState>* instance_states = &fragment_state->instance_states;
  bool share_build = fragment.output_sink.join_build_sink.share_build;
  int per_fragment_instance_idx = 0;
  for (FInstanceScheduleState& parent_state : join_fragment_state->instance_states) {
    // Share the build if join build sharing is enabled for this fragment and the previous
    // instance was on the same host (instances for a backend are clustered together).
    if (!share_build || instance_states->empty()
        || instance_states->back().krpc_host != parent_state.krpc_host) {
      UniqueIdPB instance_id = state->GetNextInstanceId();
      instance_states->emplace_back(instance_id, parent_state.host,
          parent_state.krpc_host, per_fragment_instance_idx++, *fragment_state);
      instance_states->back().exec_params.set_num_join_build_outputs(0);
      *fragment_state->exec_params->add_instances() = instance_id;
    }
    JoinBuildInputPB build_input;
    build_input.set_join_node_id(sink.dest_node_id);
    FInstanceExecParamsPB& pb = instance_states->back().exec_params;
    *build_input.mutable_input_finstance_id() = pb.instance_id();
    *parent_state.exec_params.add_join_build_inputs() = build_input;
    VLOG(3) << "Linked join build for node id=" << sink.dest_node_id
            << " build finstance=" << PrintId(pb.instance_id())
            << " dst finstance=" << PrintId(parent_state.exec_params.instance_id());
    pb.set_num_join_build_outputs(pb.num_join_build_outputs() + 1);
  }
}

Status Scheduler::ComputeScanRangeAssignment(const ExecutorConfig& executor_config,
    PlanNodeId node_id, const TReplicaPreference::type* node_replica_preference,
    bool node_random_replica, const vector<TScanRangeLocationList>& locations,
    const vector<TNetworkAddress>& host_list, bool exec_at_coord,
    const TQueryOptions& query_options, RuntimeProfile::Counter* timer, std::mt19937* rng,
    RuntimeProfile* summary_profile, FragmentScanRangeAssignment* assignment) {
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

  // This temp group is necessary because of the AssignmentCtx interface. This group is
  // used to schedule scan ranges for the plan node passed where the caller of this method
  // has determined that it needs to be scheduled on the coordinator only. Note that this
  // also includes queries where the whole query should run on the coordinator, as is
  // determined by Scheduler::IsCoordinatorOnlyQuery(). For those queries, the
  // AdmissionController will pass an empty executor group and rely on this method being
  // called with exec_at_coord = true.
  // TODO: Either get this from the ExecutorConfig or modify the AssignmentCtx interface
  // to handle this case.
  ExecutorGroup coord_only_executor_group("coordinator-only-group");
  const BackendDescriptorPB& coord_desc = executor_config.coord_desc;
  coord_only_executor_group.AddExecutor(coord_desc);
  VLOG_ROW << "Exec at coord is " << (exec_at_coord ? "true" : "false");
  AssignmentCtx assignment_ctx(exec_at_coord ? coord_only_executor_group : executor_group,
      total_assignments_, total_local_assignments_, rng);

  // Holds scan ranges that must be assigned for remote reads.
  vector<const TScanRangeLocationList*> remote_scan_range_locations;

  // Loop over all scan ranges, select an executor for those with local impalads and
  // collect all others for later processing.
  for (const TScanRangeLocationList& scan_range_locations : locations) {
    TReplicaPreference::type min_distance = TReplicaPreference::REMOTE;

    // Select executor for the current scan range.
    if (exec_at_coord) {
      DCHECK(assignment_ctx.executor_group().LookUpExecutorIp(
          coord_desc.address().hostname(), nullptr));
      assignment_ctx.RecordScanRangeAssignment(
          coord_desc, node_id, host_list, scan_range_locations, assignment);
    } else if (scan_range_locations.scan_range.__isset.is_system_scan &&
               scan_range_locations.scan_range.is_system_scan) {
      // Must run on a coordinator, lookup in executor_config.all_coords
      DCHECK_EQ(1, scan_range_locations.locations.size());
      const TNetworkAddress& coordinator =
          host_list[scan_range_locations.locations[0].host_idx];
      const BackendDescriptorPB* coordinatorPB =
          executor_config.all_coords.LookUpBackendDesc(FromTNetworkAddress(coordinator));
      if (coordinatorPB == nullptr) {
        // Coordinator is no longer available, skip this range.
        string warn_message = Substitute(
            "Coordinator $0 is no longer available for system table scan assignment",
            TNetworkAddressToString(coordinator));
        if (LIKELY(summary_profile != nullptr)) {
          initializeSchedulerWarning(summary_profile);
          summary_profile->AppendInfoString(SCHEDULER_WARNING_KEY, warn_message);
        }
        LOG(WARNING) << warn_message;
        continue;
      }
      assignment_ctx.RecordScanRangeAssignment(
        *coordinatorPB, node_id, host_list, scan_range_locations, assignment);
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
      BackendDescriptorPB executor;
      assignment_ctx.SelectExecutorOnHost(*executor_ip, &executor);
      assignment_ctx.RecordScanRangeAssignment(
          executor, node_id, host_list, scan_range_locations, assignment);
    } // End of executor selection.
  } // End of for loop over scan ranges.

  // Assign remote scans to executors.
  int num_remote_executor_candidates =
      min(query_options.num_remote_executor_candidates, executor_group.NumExecutors());
  for (const TScanRangeLocationList* scan_range_locations : remote_scan_range_locations) {
    DCHECK(!exec_at_coord);
    const IpAddr* executor_ip;
    vector<IpAddr> remote_executor_candidates;
    // Limit the number of remote executor candidates:
    // 1. When enabled by setting 'num_remote_executor_candidates' > 0
    // AND
    // 2. This is an HDFS file split
    // Otherwise, fall back to the normal method of selecting executors for remote
    // ranges, which allows for execution on any backend.
    if (scan_range_locations->scan_range.__isset.hdfs_file_split &&
        num_remote_executor_candidates > 0) {
      assignment_ctx.GetRemoteExecutorCandidates(
          &scan_range_locations->scan_range.hdfs_file_split,
          num_remote_executor_candidates, &remote_executor_candidates);
      // Like the local case, schedule_random_replica determines how to break ties.
      executor_ip = assignment_ctx.SelectExecutorFromCandidates(
          remote_executor_candidates, random_replica);
    } else {
      executor_ip = assignment_ctx.SelectRemoteExecutor();
    }
    BackendDescriptorPB executor;
    assignment_ctx.SelectExecutorOnHost(*executor_ip, &executor);
    assignment_ctx.RecordScanRangeAssignment(
        executor, node_id, host_list, *scan_range_locations, assignment);
  }

  if (VLOG_FILE_IS_ON) assignment_ctx.PrintAssignment(*assignment);

  return Status::OK();
}

bool Scheduler::ContainsNode(const TPlan& plan, TPlanNodeType::type type) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    if (plan.nodes[i].node_type == type) return true;
  }
  return false;
}

bool Scheduler::ContainsNode(
    const TPlan& plan, const std::vector<TPlanNodeType::type>& types) {
  for (int i = 0; i < plan.nodes.size(); ++i) {
    for (int j = 0; j < types.size(); ++j) {
      if (plan.nodes[i].node_type == types[j]) return true;
    }
  }
  return false;
}

bool Scheduler::ContainsScanNode(const TPlan& plan) {
  return ContainsNode(plan, SCAN_NODE_TYPES);
}

bool Scheduler::ContainsUnionNode(const TPlan& plan) {
  return ContainsNode(plan, TPlanNodeType::UNION_NODE);
}

std::vector<TPlanNodeId> Scheduler::FindNodes(
    const TPlan& plan, const vector<TPlanNodeType::type>& types) {
  vector<TPlanNodeId> results;
  for (int i = 0; i < plan.nodes.size(); ++i) {
    for (int j = 0; j < types.size(); ++j) {
      if (plan.nodes[i].node_type == types[j]) {
        results.push_back(plan.nodes[i].node_id);
        break;
      }
    }
  }
  return results;
}

std::vector<TPlanNodeId> Scheduler::FindScanNodes(const TPlan& plan) {
  return FindNodes(plan, SCAN_NODE_TYPES);
}

void Scheduler::GetScanHosts(const vector<TPlanNodeId>& scan_ids,
    const FragmentScheduleState& fragment_state, vector<NetworkAddressPB>* scan_hosts) {
  for (const TPlanNodeId& scan_id : scan_ids) {
    // Get the list of impalad host from scan_range_assignment_
    for (const FragmentScanRangeAssignment::value_type& scan_range_assignment :
        fragment_state.scan_range_assignment) {
      const PerNodeScanRanges& per_node_scan_ranges = scan_range_assignment.second;
      if (per_node_scan_ranges.find(scan_id) != per_node_scan_ranges.end()) {
        scan_hosts->push_back(scan_range_assignment.first);
      }
    }
  }
}

Status Scheduler::Schedule(const ExecutorConfig& executor_config, ScheduleState* state) {
  RETURN_IF_ERROR(DebugAction(state->query_options(), "SCHEDULER_SCHEDULE"));
  RETURN_IF_ERROR(ComputeScanRangeAssignment(executor_config, state));
  RETURN_IF_ERROR(ComputeFragmentExecParams(executor_config, state));
  ComputeBackendExecParams(executor_config, state);
#ifndef NDEBUG
  state->Validate();
#endif
  state->set_executor_group(executor_config.group.name());
  return Status::OK();
}

bool Scheduler::IsCoordinatorOnlyQuery(const TQueryExecRequest& exec_request) {
  DCHECK_GT(exec_request.plan_exec_info.size(), 0);
  int64_t num_parallel_plans = exec_request.plan_exec_info.size();
  const TPlanExecInfo& plan_exec_info = exec_request.plan_exec_info[0];
  int64_t num_fragments = plan_exec_info.fragments.size();
  DCHECK_GT(num_fragments, 0);
  auto type = plan_exec_info.fragments[0].partition.type;
  return num_parallel_plans == 1 && num_fragments == 1
      && type == TPartitionType::UNPARTITIONED;
}

void Scheduler::PopulateFilepathToHostsMapping(const FInstanceScheduleState& finst,
    ScheduleState* state, ByNodeFilepathToHosts* duplicate_check) {
  for (const auto& per_node_ranges : finst.exec_params.per_node_scan_ranges()) {
    const TPlanNode& node = state->GetNode(per_node_ranges.first);
    if (node.node_type != TPlanNodeType::HDFS_SCAN_NODE) continue;
    if (!node.hdfs_scan_node.__isset.deleteFileScanNodeId) continue;
    const TPlanNodeId delete_file_node_id = node.hdfs_scan_node.deleteFileScanNodeId;

    for (const auto& scan_ranges : per_node_ranges.second.scan_ranges()) {
      string file_path;
      bool is_relative = false;
      const HdfsFileSplitPB& hdfs_file_split = scan_ranges.scan_range().hdfs_file_split();
      if (hdfs_file_split.has_relative_path() &&
          !hdfs_file_split.relative_path().empty()) {
        file_path = hdfs_file_split.relative_path();
        is_relative = true;
      } else {
        file_path = hdfs_file_split.absolute_path();
      }
      DCHECK(!file_path.empty());

      std::unordered_set<NetworkAddressPB>& current_hosts =
          (*duplicate_check)[delete_file_node_id][file_path];
      if (current_hosts.find(finst.host) != current_hosts.end()) continue;
      current_hosts.insert(finst.host);

      auto* by_node_filepath_to_hosts =
          state->query_schedule_pb()->mutable_by_node_filepath_to_hosts();
      auto* filepath_to_hosts =
          (*by_node_filepath_to_hosts)[delete_file_node_id].mutable_filepath_to_hosts();
      (*filepath_to_hosts)[file_path].set_is_relative(is_relative);
      auto* hosts = (*filepath_to_hosts)[file_path].add_hosts();
      *hosts = finst.host;
    }
  }
}

void Scheduler::ComputeBackendExecParams(
    const ExecutorConfig& executor_config, ScheduleState* state) {
  ByNodeFilepathToHosts duplicate_check;
  for (FragmentScheduleState& f : state->fragment_schedule_states()) {
    const NetworkAddressPB* prev_host = nullptr;
    int num_hosts = 0;
    for (FInstanceScheduleState& i : f.instance_states) {
      PopulateFilepathToHostsMapping(i, state, &duplicate_check);

      BackendScheduleState& be_state = state->GetOrCreateBackendScheduleState(i.host);
      be_state.exec_params->add_instance_params()->Swap(&i.exec_params);
      // Different fragments do not synchronize their Open() and Close(), so the backend
      // does not provide strong guarantees about whether one fragment instance releases
      // resources before another acquires them. Conservatively assume that all fragment
      // instances on this backend can consume their peak resources at the same time,
      // i.e. that this backend's peak resources is the sum of the per-fragment-instance
      // peak resources for the instances executing on this backend.
      be_state.exec_params->set_min_mem_reservation_bytes(
          be_state.exec_params->min_mem_reservation_bytes()
          + f.fragment.instance_min_mem_reservation_bytes);
      be_state.exec_params->set_initial_mem_reservation_total_claims(
          be_state.exec_params->initial_mem_reservation_total_claims()
          + f.fragment.instance_initial_mem_reservation_total_claims);
      be_state.exec_params->set_thread_reservation(
          be_state.exec_params->thread_reservation() + f.fragment.thread_reservation);
      // Some memory is shared between fragments on a host. Only add it for the first
      // instance of this fragment on the host.
      if (prev_host == nullptr || *prev_host != i.host) {
        be_state.exec_params->set_min_mem_reservation_bytes(
            be_state.exec_params->min_mem_reservation_bytes()
            + f.fragment.backend_min_mem_reservation_bytes);
        be_state.exec_params->set_initial_mem_reservation_total_claims(
            be_state.exec_params->initial_mem_reservation_total_claims()
            + f.fragment.backend_initial_mem_reservation_total_claims);
        prev_host = &i.host;
        ++num_hosts;
      }
    }
    f.exec_params->set_num_hosts(num_hosts);
  }

  // Compute 'slots_to_use' for each backend based on the max # of instances of
  // any fragment on that backend. If 'compute_processing_cost' is on, and Planner
  // set 'max_slot_per_executor', pick the min between 'dominant_instance_count' and
  // 'max_slot_per_executor'.
  bool cap_slots = state->query_options().compute_processing_cost
      && state->query_options().slot_count_strategy == TSlotCountStrategy::PLANNER_CPU_ASK
      && state->request().__isset.max_slot_per_executor
      && state->request().max_slot_per_executor > 0;
  for (auto& backend : state->per_backend_schedule_states()) {
    int be_max_instances = 0; // max # of instances of any fragment.
    int dominant_instance_count = 0; // sum of all dominant fragment instances.

    // Instances for a fragment are clustered together because of how the vector is
    // constructed above. For example, 3 fragments with 2 instances each will be in this
    // order inside exec_params->instance_params() vector.
    //   [F00_a, F00_b, F01_c, F01_d, F02_e, F02_f]
    // So we can compute the max # of instances of any fragment with a single pass over
    // the vector.
    int curr_fragment_idx = -1;
    int curr_instance_count = 0; // Number of instances of the current fragment seen.
    bool is_dominant = false;
    for (auto& finstance : backend.second.exec_params->instance_params()) {
      if (curr_fragment_idx == -1 || curr_fragment_idx != finstance.fragment_idx()) {
        // We arrived at new fragment group. Update 'be_max_instances'.
        be_max_instances = max(be_max_instances, curr_instance_count);
        // Reset 'curr_fragment_idx' and other counting related variables.
        curr_fragment_idx = finstance.fragment_idx();
        curr_instance_count = 0;
        is_dominant =
            state->GetFragmentScheduleState(curr_fragment_idx)->fragment.is_dominant;
      }
      ++curr_instance_count;
      if (is_dominant) ++dominant_instance_count;
    }
    // Update 'be_max_instances' one last time.
    be_max_instances = max(be_max_instances, curr_instance_count);

    // Default slots to use number from IMPALA-8998.
    // For fragment with largest num of instances running in this backend, it
    // ensures allocation of 1 slot for each instance of that fragment.
    int slots_to_use = be_max_instances;

    // Done looping exec_params->instance_params(). Derived 'slots_to_use' based on
    // finalized 'be_max_instances' and 'dominant_instance_count'.
    if (cap_slots) {
      if (dominant_instance_count >= be_max_instances) {
        // One case where it is possible to have 'dominant_instance_count' <
        // 'be_max_instances' is with dedicated coordinator setup. The schedule would
        // only assign one coordinator fragment instance to the coordinator node,
        // but 'dominant_instance_count' can be 0 if fragment.is_dominant == false.
        // In that case, ignore 'dominant_instance_count' and continue with
        // 'be_max_instances'.
        // However, if 'dominant_instance_count' >= 'be_max_instances',
        // continue with 'dominant_instance_count'.
        slots_to_use = dominant_instance_count;
      }
      slots_to_use = min(slots_to_use, state->request().max_slot_per_executor);
    }
    backend.second.exec_params->set_slots_to_use(slots_to_use);
  }

  // This also ensures an entry always exists for the coordinator backend.
  int64_t coord_min_reservation = 0;
  const NetworkAddressPB& coord_addr = executor_config.coord_desc.address();
  BackendScheduleState& coord_be_state =
      state->GetOrCreateBackendScheduleState(coord_addr);
  coord_be_state.exec_params->set_is_coord_backend(true);
  coord_min_reservation = coord_be_state.exec_params->min_mem_reservation_bytes();

  int64_t largest_min_reservation = 0;
  for (auto& backend : state->per_backend_schedule_states()) {
    const NetworkAddressPB& host = backend.first;
    const BackendDescriptorPB be_desc = LookUpBackendDesc(executor_config, host);
    backend.second.be_desc = be_desc;
    *backend.second.exec_params->mutable_backend_id() = be_desc.backend_id();
    *backend.second.exec_params->mutable_address() = be_desc.address();
    *backend.second.exec_params->mutable_krpc_address() = be_desc.krpc_address();
    if (!backend.second.exec_params->is_coord_backend()) {
      largest_min_reservation = max(largest_min_reservation,
          backend.second.exec_params->min_mem_reservation_bytes());
    }
  }
  state->set_largest_min_reservation(largest_min_reservation);
  state->set_coord_min_reservation(coord_min_reservation);

  stringstream min_mem_reservation_ss;
  stringstream num_fragment_instances_ss;
  for (const auto& e : state->per_backend_schedule_states()) {
    min_mem_reservation_ss << NetworkAddressPBToString(e.first) << "("
                           << PrettyPrinter::Print(
                                  e.second.exec_params->min_mem_reservation_bytes(),
                                  TUnit::BYTES)
                           << ") ";
    num_fragment_instances_ss << NetworkAddressPBToString(e.first) << "("
                              << PrettyPrinter::Print(
                                     e.second.exec_params->instance_params().size(),
                                     TUnit::UNIT)
                              << ") ";
  }
  state->summary_profile()->AddInfoString(
      PROFILE_INFO_KEY_PER_HOST_MIN_MEMORY_RESERVATION, min_mem_reservation_ss.str());
  state->summary_profile()->AddInfoString(
      "Per Host Number of Fragment Instances", num_fragment_instances_ss.str());
}

Scheduler::AssignmentCtx::AssignmentCtx(const ExecutorGroup& executor_group,
    IntCounter* total_assignments, IntCounter* total_local_assignments, std::mt19937* rng)
  : executor_group_(executor_group),
    first_unused_executor_idx_(0),
    total_assignments_(total_assignments),
    total_local_assignments_(total_local_assignments) {
  DCHECK_GT(executor_group.NumExecutors(), 0);
  random_executor_order_ = executor_group.GetAllExecutorIps();
  std::shuffle(random_executor_order_.begin(), random_executor_order_.end(), *rng);
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
  // This function should not be called with 'num_candidates' exceeding the number of
  // executors.
  DCHECK_LE(num_candidates, executor_group_.NumExecutors());
  // Two different hashes of the filename can result in the same executor.
  // The function should return distinct executors, so it may need to do more hashes
  // than 'num_candidates'.
  unordered_set<IpAddr> distinct_backends;
  distinct_backends.reserve(num_candidates);
  // Generate multiple hashes of the file split by using the hash as a seed to a PRNG.
  // Note: The hash includes the partition path hash, the filename (relative to the
  // partition directory), and the offset. The offset is used to allow very large files
  // that have multiple splits to be spread across more executors.
  uint32_t hash = static_cast<uint32_t>(hdfs_file_split->partition_path_hash);
  hash = HashUtil::Hash(hdfs_file_split->relative_path.data(),
      hdfs_file_split->relative_path.length(), hash);
  hash = HashUtil::Hash(&hdfs_file_split->offset, sizeof(hdfs_file_split->offset), hash);
  pcg32 prng(hash);
  // The function should return distinct executors, so it may need to do more hashes
  // than 'num_candidates'. To avoid any problem scenarios, limit the total number of
  // iterations. The number of iterations is set to a reasonably high level, because
  // on average the loop short circuits considerably earlier. Using a higher number of
  // iterations is useful for smaller clusters where we are using this function to get
  // all the backends in a consistent order rather than picking a consistent subset.
  // Suppose there are three nodes and the number of remote executor candidates is three.
  // One can calculate the probability of picking three distinct executors in at most
  // n iterations. For n=3, the second pick must not overlap the first (probability 2/3),
  // and the third pick must not be either the first or second (probability 1/3). So:
  // P(3) = 1*(2/3)*(1/3)=2/9
  // The probability that it is done in at most n+1 steps is the probability that
  // it completed in n steps combined with the probability that it completes in the n+1st
  // step. In order to complete in the n+1st step, the previous n steps must not have
  // all landed on a single backend (probability (1/3)^(n-1)) and this step must not land
  // on the two backends already chosen (probability 1/3). So, the recursive step is:
  // P(n+1) = P(n) + (1 - P(n))*(1-(1/3)^(n-1))*(1/3)
  // Here are some example probabilities:
  // Probability of completing in at most 5 iterations: 0.6284
  // Probability of completing in at most 10 iterations: 0.9506
  // Probability of completing in at most 15 iterations: 0.9935
  // Probability of completing in at most 20 iterations: 0.9991
  int max_iterations = num_candidates * MAX_ITERATIONS_PER_EXECUTOR_CANDIDATE;
  for (int i = 0; i < max_iterations; ++i) {
    // Look up nearest IpAddr
    const IpAddr* executor_addr = executor_group_.GetHashRing()->GetNode(prng());
    DCHECK(executor_addr != nullptr);
    auto insert_ret = distinct_backends.insert(*executor_addr);
    // The return type of unordered_set.insert() is a pair<iterator, bool> where the
    // second element is whether this was a new element. If this is a new element,
    // add this element to the return vector.
    if (insert_ret.second) {
      remote_executor_candidates->push_back(*executor_addr);
    }
    // Short-circuit if we reach the appropriate number of replicas
    if (remote_executor_candidates->size() == num_candidates) break;
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
    DCHECK_GT(executor_group_.NumHosts(), 0);
    DCHECK_EQ(executor_group_.NumHosts(), assignment_heap_.size());
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
    const IpAddr& executor_ip, BackendDescriptorPB* executor) {
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
      const BackendDescriptorPB& next_executor = **next_executor_on_host;
      // The IP addresses must already match, so it is sufficient to check the port.
      DCHECK_EQ(next_executor.ip_address(), elem.ip_address());
      return next_executor.address().port() == elem.address().port();
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

void TScanRangeToScanRangePB(const TScanRange& tscan_range, ScanRangePB* scan_range_pb) {
  if (tscan_range.__isset.hdfs_file_split) {
    HdfsFileSplitPB* hdfs_file_split = scan_range_pb->mutable_hdfs_file_split();
    hdfs_file_split->set_relative_path(tscan_range.hdfs_file_split.relative_path);
    hdfs_file_split->set_offset(tscan_range.hdfs_file_split.offset);
    hdfs_file_split->set_length(tscan_range.hdfs_file_split.length);
    hdfs_file_split->set_partition_id(tscan_range.hdfs_file_split.partition_id);
    hdfs_file_split->set_file_length(tscan_range.hdfs_file_split.file_length);
    hdfs_file_split->set_file_compression(
        THdfsCompressionToProto(tscan_range.hdfs_file_split.file_compression));
    hdfs_file_split->set_mtime(tscan_range.hdfs_file_split.mtime);
    hdfs_file_split->set_partition_path_hash(
        tscan_range.hdfs_file_split.partition_path_hash);
    hdfs_file_split->set_is_encrypted(tscan_range.hdfs_file_split.is_encrypted);
    hdfs_file_split->set_is_erasure_coded(tscan_range.hdfs_file_split.is_erasure_coded);
    if (tscan_range.hdfs_file_split.__isset.absolute_path) {
      hdfs_file_split->set_absolute_path(
          tscan_range.hdfs_file_split.absolute_path);
    }
  }
  if (tscan_range.__isset.hbase_key_range) {
    HBaseKeyRangePB* hbase_key_range = scan_range_pb->mutable_hbase_key_range();
    hbase_key_range->set_startkey(tscan_range.hbase_key_range.startKey);
    hbase_key_range->set_stopkey(tscan_range.hbase_key_range.stopKey);
  }
  if (tscan_range.__isset.kudu_scan_token) {
    scan_range_pb->set_kudu_scan_token(tscan_range.kudu_scan_token);
  }
  if (tscan_range.__isset.file_metadata) {
    scan_range_pb->set_file_metadata(tscan_range.file_metadata);
  }
  if (tscan_range.__isset.is_system_scan) {
    scan_range_pb->set_is_system_scan(tscan_range.is_system_scan);
  }
}

void Scheduler::AssignmentCtx::RecordScanRangeAssignment(
    const BackendDescriptorPB& executor, PlanNodeId node_id,
    const vector<TNetworkAddress>& host_list,
    const TScanRangeLocationList& scan_range_locations,
    FragmentScanRangeAssignment* assignment) {
  if (scan_range_locations.scan_range.__isset.is_system_scan &&
      scan_range_locations.scan_range.is_system_scan) {
    // Assigned to a coordinator.
    PerNodeScanRanges* scan_ranges =
        FindOrInsert(assignment, executor.address(), PerNodeScanRanges());
    vector<ScanRangeParamsPB>* scan_range_params_list =
        FindOrInsert(scan_ranges, node_id, vector<ScanRangeParamsPB>());
    ScanRangeParamsPB scan_range_params;
    TScanRangeToScanRangePB(
        scan_range_locations.scan_range, scan_range_params.mutable_scan_range());
    scan_range_params_list->push_back(scan_range_params);

    VLOG_FILE << "Scheduler assignment of system table scan to coordinator: "
              << executor.address();
    return;
  }

  int64_t scan_range_length = 0;
  if (scan_range_locations.scan_range.__isset.hdfs_file_split) {
    scan_range_length = scan_range_locations.scan_range.hdfs_file_split.length;
  } else if (scan_range_locations.scan_range.__isset.kudu_scan_token) {
    // Hack so that kudu ranges are well distributed.
    // TODO: KUDU-1133 Use the tablet size instead.
    scan_range_length = 1000;
  }

  IpAddr executor_ip;
  bool ret =
      executor_group_.LookUpExecutorIp(executor.address().hostname(), &executor_ip);
  DCHECK(ret);
  DCHECK(!executor_ip.empty());
  assignment_heap_.InsertOrUpdate(
      executor_ip, scan_range_length, GetExecutorRank(executor_ip));

  // See if the read will be remote. This is not the case if the impalad runs on one of
  // the replica's datanodes.
  bool remote_read = true;
  // For local reads we can set volume_id and try_hdfs_cache. For remote reads HDFS will
  // decide which replica to use so we keep those at default values.
  int volume_id = -1;
  bool try_hdfs_cache = false;
  for (const TScanRangeLocation& location : scan_range_locations.locations) {
    const TNetworkAddress& replica_host = host_list[location.host_idx];
    IpAddr replica_ip;
    if (executor_group_.LookUpExecutorIp(replica_host.hostname, &replica_ip)
        && executor_ip == replica_ip) {
      remote_read = false;
      volume_id = location.volume_id;
      try_hdfs_cache = location.is_cached;
      break;
    }
  }

  if (remote_read) {
    assignment_byte_counters_.remote_bytes += scan_range_length;
  } else {
    assignment_byte_counters_.local_bytes += scan_range_length;
    if (try_hdfs_cache) assignment_byte_counters_.cached_bytes += scan_range_length;
  }

  if (total_assignments_ != nullptr) {
    DCHECK(total_local_assignments_ != nullptr);
    total_assignments_->Increment(1);
    if (!remote_read) total_local_assignments_->Increment(1);
  }

  PerNodeScanRanges* scan_ranges =
      FindOrInsert(assignment, executor.address(), PerNodeScanRanges());
  vector<ScanRangeParamsPB>* scan_range_params_list =
      FindOrInsert(scan_ranges, node_id, vector<ScanRangeParamsPB>());
  // Add scan range.
  ScanRangeParamsPB scan_range_params;
  TScanRangeToScanRangePB(
      scan_range_locations.scan_range, scan_range_params.mutable_scan_range());
  scan_range_params.set_volume_id(volume_id);
  scan_range_params.set_try_hdfs_cache(try_hdfs_cache);
  scan_range_params.set_is_remote(remote_read);
  scan_range_params_list->push_back(scan_range_params);

  if (VLOG_FILE_IS_ON) {
    VLOG_FILE << "Scheduler assignment to executor: " << executor.address() << "("
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
    VLOG_FILE << "ScanRangeAssignment: server=" << entry.first.DebugString();
    for (const PerNodeScanRanges::value_type& per_node_scan_ranges : entry.second) {
      stringstream str;
      for (const ScanRangeParamsPB& params : per_node_scan_ranges.second) {
        str << params.DebugString() << " ";
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
