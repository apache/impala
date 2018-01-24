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

#include "scheduling/scheduler-test-util.h"

#include <boost/unordered_set.hpp>

#include "common/names.h"
#include "scheduling/scheduler.h"

using namespace impala;
using namespace impala::test;

DECLARE_int32(krpc_port);

/// Sample 'n' elements without replacement from the set [0..N-1].
/// This is an implementation of "Algorithm R" by J. Vitter.
void SampleN(int n, int N, vector<int>* out) {
  if (n == 0) return;
  DCHECK(n <= N);
  out->reserve(n);
  out->clear();
  for (int i = 0; i < n; ++i) out->push_back(i);
  for (int i = n; i < N; ++i) {
    // Accept element with probability n/i. Place at random position.
    int r = rand() % i;
    if (r < n) (*out)[r] = i;
  }
}

/// Sample a set of 'n' elements from 'in' without replacement and copy them to
/// 'out'.
template <typename T>
void SampleNElements(int n, const vector<T>& in, vector<T>* out) {
  vector<int> idxs;
  SampleN(n, in.size(), &idxs);
  DCHECK_EQ(n, idxs.size());
  out->reserve(n);
  for (int idx : idxs) out->push_back(in[idx]);
}

/// Define constants from scheduler-test-util.h here.
const int Cluster::BACKEND_PORT = 1000;
const int Cluster::DATANODE_PORT = 2000;
const string Cluster::HOSTNAME_PREFIX = "host_";
const string Cluster::IP_PREFIX = "10";

/// Default size for new blocks is 1MB.
const int64_t Block::DEFAULT_BLOCK_SIZE = 1 << 20;

int Cluster::AddHost(bool has_backend, bool has_datanode, bool is_executor) {
  int host_idx = hosts_.size();
  int be_port = has_backend ? BACKEND_PORT : -1;
  int dn_port = has_datanode ? DATANODE_PORT : -1;
  IpAddr ip = HostIdxToIpAddr(host_idx);
  DCHECK(ip_to_idx_.find(ip) == ip_to_idx_.end());
  ip_to_idx_[ip] = host_idx;
  hosts_.push_back(Host(HostIdxToHostname(host_idx), ip, be_port, dn_port, is_executor));
  // Add host to lists of backend indexes per type.
  if (has_backend) backend_host_idxs_.push_back(host_idx);
  if (has_datanode) {
    datanode_host_idxs_.push_back(host_idx);
    if (has_backend) {
      datanode_with_backend_host_idxs_.push_back(host_idx);
    } else {
      datanode_only_host_idxs_.push_back(host_idx);
    }
  }
  return host_idx;
}

void Cluster::AddHosts(int num_hosts, bool has_backend, bool has_datanode,
    bool is_executor) {
  for (int i = 0; i < num_hosts; ++i) AddHost(has_backend, has_datanode, is_executor);
}

Hostname Cluster::HostIdxToHostname(int host_idx) {
  return HOSTNAME_PREFIX + std::to_string(host_idx);
}

void Cluster::GetBackendAddress(int host_idx, TNetworkAddress* addr) const {
  DCHECK_LT(host_idx, hosts_.size());
  addr->hostname = hosts_[host_idx].ip;
  addr->port = hosts_[host_idx].be_port;
}

const vector<int>& Cluster::datanode_with_backend_host_idxs() const {
  return datanode_with_backend_host_idxs_;
}

const vector<int>& Cluster::datanode_only_host_idxs() const {
  return datanode_only_host_idxs_;
}

IpAddr Cluster::HostIdxToIpAddr(int host_idx) {
  DCHECK_LT(host_idx, (1 << 24));
  string suffix;
  for (int i = 0; i < 3; ++i) {
    suffix = "." + std::to_string(host_idx % 256) + suffix; // prepend
    host_idx /= 256;
  }
  DCHECK_EQ(0, host_idx);
  return IP_PREFIX + suffix;
}

void Schema::AddSingleBlockTable(
    const TableName& table_name, const vector<int>& non_cached_replica_host_idxs) {
  AddSingleBlockTable(table_name, non_cached_replica_host_idxs, {});
}

void Schema::AddSingleBlockTable(const TableName& table_name,
    const vector<int>& non_cached_replica_host_idxs,
    const vector<int>& cached_replica_host_idxs) {
  DCHECK(tables_.find(table_name) == tables_.end());
  Block block;
  int num_replicas =
      non_cached_replica_host_idxs.size() + cached_replica_host_idxs.size();
  block.replica_host_idxs = non_cached_replica_host_idxs;
  block.replica_host_idxs.insert(block.replica_host_idxs.end(),
      cached_replica_host_idxs.begin(), cached_replica_host_idxs.end());
  // Initialize for non-cached replicas first.
  block.replica_host_idx_is_cached.resize(non_cached_replica_host_idxs.size(), false);
  // Fill up to final size for cached replicas.
  block.replica_host_idx_is_cached.insert(
      block.replica_host_idx_is_cached.end(), cached_replica_host_idxs.size(), true);
  DCHECK_EQ(block.replica_host_idxs.size(), block.replica_host_idx_is_cached.size());
  DCHECK_EQ(block.replica_host_idxs.size(), num_replicas);
  // Create table
  Table table;
  table.blocks.push_back(block);
  // Insert table
  tables_.emplace(table_name, table);
}

void Schema::AddMultiBlockTable(const TableName& table_name, int num_blocks,
    ReplicaPlacement replica_placement, int num_replicas) {
  AddMultiBlockTable(table_name, num_blocks, replica_placement, num_replicas, 0);
}

void Schema::AddMultiBlockTable(const TableName& table_name, int num_blocks,
    ReplicaPlacement replica_placement, int num_replicas, int num_cached_replicas) {
  DCHECK_GT(num_replicas, 0);
  DCHECK(num_cached_replicas <= num_replicas);
  Table table;
  for (int i = 0; i < num_blocks; ++i) {
    Block block;
    vector<int>& replica_idxs = block.replica_host_idxs;

    // Determine replica host indexes.
    switch (replica_placement) {
      case ReplicaPlacement::RANDOM:
        SampleNElements(num_replicas, cluster_.datanode_host_idxs(), &replica_idxs);
        break;
      case ReplicaPlacement::LOCAL_ONLY:
        DCHECK(num_replicas <= cluster_.datanode_with_backend_host_idxs().size());
        SampleNElements(
            num_replicas, cluster_.datanode_with_backend_host_idxs(), &replica_idxs);
        break;
      case ReplicaPlacement::REMOTE_ONLY:
        DCHECK(num_replicas <= cluster_.datanode_only_host_idxs().size());
        SampleNElements(num_replicas, cluster_.datanode_only_host_idxs(), &replica_idxs);
        break;
      default:
        DCHECK(false) << "Unsupported replica placement: " << (int)replica_placement;
    }

    // Determine cached replicas.
    vector<int> cached_replicas;
    vector<bool>& is_cached = block.replica_host_idx_is_cached;
    is_cached.resize(num_replicas, false);
    SampleN(num_cached_replicas, num_replicas, &cached_replicas);
    // Flag cached entries.
    for (const int idx : cached_replicas) is_cached[idx] = true;

    DCHECK_EQ(replica_idxs.size(), is_cached.size());
    table.blocks.push_back(block);
  }
  // Insert table
  tables_[table_name] = table;
}

const Table& Schema::GetTable(const TableName& table_name) const {
  auto it = tables_.find(table_name);
  DCHECK(it != tables_.end());
  return it->second;
}

void Plan::SetReplicaPreference(TReplicaPreference::type p) {
  query_options_.replica_preference = p;
}

const vector<TNetworkAddress>& Plan::referenced_datanodes() const {
  return referenced_datanodes_;
}

const vector<TScanRangeLocationList>& Plan::scan_range_locations() const {
  return scan_range_locations_;
}

void Plan::AddTableScan(const TableName& table_name) {
  const Table& table = schema_.GetTable(table_name);
  const vector<Block>& blocks = table.blocks;
  for (int i = 0; i < blocks.size(); ++i) {
    const Block& block = blocks[i];
    TScanRangeLocationList scan_range_locations;
    BuildTScanRangeLocationList(table_name, block, i, &scan_range_locations);
    scan_range_locations_.push_back(scan_range_locations);
  }
}

void Plan::BuildTScanRangeLocationList(const TableName& table_name, const Block& block,
    int block_idx, TScanRangeLocationList* scan_range_locations) {
  const vector<int>& replica_idxs = block.replica_host_idxs;
  const vector<bool>& is_cached = block.replica_host_idx_is_cached;
  DCHECK_EQ(replica_idxs.size(), is_cached.size());
  int num_replicas = replica_idxs.size();
  BuildScanRange(table_name, block, block_idx, &scan_range_locations->scan_range);
  scan_range_locations->locations.resize(num_replicas);
  for (int i = 0; i < num_replicas; ++i) {
    TScanRangeLocation& location = scan_range_locations->locations[i];
    location.host_idx = FindOrInsertDatanodeIndex(replica_idxs[i]);
    location.__set_is_cached(is_cached[i]);
  }
}

void Plan::BuildScanRange(const TableName& table_name, const Block& block, int block_idx,
    TScanRange* scan_range) {
  // Initialize locations.scan_range correctly.
  THdfsFileSplit file_split;
  // 'length' is the only member considered by the scheduler.
  file_split.length = block.length;
  // Encoding the table name and block index in the file helps debugging.
  file_split.file_name = table_name + "_block_" + std::to_string(block_idx);
  file_split.offset = 0;
  file_split.partition_id = 0;
  // For now, we model each file by a single block.
  file_split.file_length = block.length;
  file_split.file_compression = THdfsCompression::NONE;
  file_split.mtime = 1;
  scan_range->__set_hdfs_file_split(file_split);
}

int Plan::FindOrInsertDatanodeIndex(int cluster_datanode_idx) {
  const Host& host = schema_.cluster().hosts()[cluster_datanode_idx];
  auto ret = host_idx_to_datanode_idx_.emplace(
      cluster_datanode_idx, referenced_datanodes_.size());
  bool inserted_new_element = ret.second;
  if (inserted_new_element) {
    TNetworkAddress datanode;
    datanode.hostname = host.ip;
    datanode.port = host.dn_port;
    referenced_datanodes_.push_back(datanode);
  }
  return ret.first->second;
}

int Result::NumTotalAssignments(int host_idx) const {
  return CountAssignmentsIf(IsHost(host_idx));
}

int Result::NumTotalAssignedBytes(int host_idx) const {
  return CountAssignedBytesIf(IsHost(host_idx));
}

int Result::NumCachedAssignments(int host_idx) const {
  return CountAssignmentsIf(IsCached(IsHost(host_idx)));
}

int Result::NumCachedAssignedBytes(int host_idx) const {
  return CountAssignedBytesIf(IsCached(IsHost(host_idx)));
}

int Result::NumDiskAssignments(int host_idx) const {
  return CountAssignmentsIf(IsDisk(IsHost(host_idx)));
}

int Result::NumDiskAssignedBytes(int host_idx) const {
  return CountAssignedBytesIf(IsDisk(IsHost(host_idx)));
}

int Result::NumRemoteAssignments(int host_idx) const {
  return CountAssignmentsIf(IsRemote(IsHost(host_idx)));
}

int Result::NumRemoteAssignedBytes(int host_idx) const {
  return CountAssignedBytesIf(IsRemote(IsHost(host_idx)));
}

int Result::MaxNumAssignmentsPerHost() const {
  NumAssignmentsPerBackend num_assignments_per_backend;
  CountAssignmentsPerBackend(&num_assignments_per_backend);
  int max_count = 0;
  for (const auto& elem : num_assignments_per_backend) {
    max_count = max(max_count, elem.second);
  }
  return max_count;
}

int64_t Result::MaxNumAssignedBytesPerHost() const {
  NumAssignedBytesPerBackend num_assigned_bytes_per_backend;
  CountAssignedBytesPerBackend(&num_assigned_bytes_per_backend);
  int64_t max_assigned_bytes = 0;
  for (const auto& elem : num_assigned_bytes_per_backend) {
    max_assigned_bytes = max(max_assigned_bytes, elem.second);
  }
  return max_assigned_bytes;
}

int Result::MinNumAssignmentsPerHost() const {
  NumAssignmentsPerBackend num_assignments_per_backend;
  CountAssignmentsPerBackend(&num_assignments_per_backend);
  int min_count = numeric_limits<int>::max();
  for (const auto& elem : num_assignments_per_backend) {
    min_count = min(min_count, elem.second);
  }
  DCHECK_GT(min_count, 0);
  return min_count;
}

int64_t Result::MinNumAssignedBytesPerHost() const {
  NumAssignedBytesPerBackend num_assigned_bytes_per_backend;
  CountAssignedBytesPerBackend(&num_assigned_bytes_per_backend);
  int64_t min_assigned_bytes = 0;
  for (const auto& elem : num_assigned_bytes_per_backend) {
    min_assigned_bytes = max(min_assigned_bytes, elem.second);
  }
  DCHECK_GT(min_assigned_bytes, 0);
  return min_assigned_bytes;
}

int Result::NumDistinctBackends() const {
  unordered_set<IpAddr> backends;
  AssignmentCallback cb = [&backends](
      const AssignmentInfo& assignment) { backends.insert(assignment.addr.hostname); };
  ProcessAssignments(cb);
  return backends.size();
}

const FragmentScanRangeAssignment& Result::GetAssignment(int index) const {
  DCHECK_GT(assignments_.size(), index);
  return assignments_[index];
}

FragmentScanRangeAssignment* Result::AddAssignment() {
  assignments_.push_back(FragmentScanRangeAssignment());
  return &assignments_.back();
}

Result::AssignmentFilter Result::Any() const {
  return [](const AssignmentInfo& assignment) { return true; };
}

Result::AssignmentFilter Result::IsHost(int host_idx) const {
  TNetworkAddress expected_addr;
  plan_.cluster().GetBackendAddress(host_idx, &expected_addr);
  return [expected_addr](
      const AssignmentInfo& assignment) { return assignment.addr == expected_addr; };
}

Result::AssignmentFilter Result::IsCached(AssignmentFilter filter) const {
  return [filter](const AssignmentInfo& assignment) {
    return filter(assignment) && assignment.is_cached;
  };
}

Result::AssignmentFilter Result::IsDisk(AssignmentFilter filter) const {
  return [filter](const AssignmentInfo& assignment) {
    return filter(assignment) && !assignment.is_cached && !assignment.is_remote;
  };
}

Result::AssignmentFilter Result::IsRemote(AssignmentFilter filter) const {
  return [filter](const AssignmentInfo& assignment) {
    return filter(assignment) && assignment.is_remote;
  };
}

void Result::ProcessAssignments(const AssignmentCallback& cb) const {
  for (const FragmentScanRangeAssignment& assignment : assignments_) {
    for (const auto& assignment_elem : assignment) {
      const TNetworkAddress& addr = assignment_elem.first;
      const PerNodeScanRanges& per_node_ranges = assignment_elem.second;
      for (const auto& per_node_ranges_elem : per_node_ranges) {
        const vector<TScanRangeParams> scan_range_params_vector =
            per_node_ranges_elem.second;
        for (const TScanRangeParams& scan_range_params : scan_range_params_vector) {
          const TScanRange& scan_range = scan_range_params.scan_range;
          DCHECK(scan_range.__isset.hdfs_file_split);
          const THdfsFileSplit& hdfs_file_split = scan_range.hdfs_file_split;
          bool is_cached =
              scan_range_params.__isset.is_cached ? scan_range_params.is_cached : false;
          bool is_remote =
              scan_range_params.__isset.is_remote ? scan_range_params.is_remote : false;
          cb({addr, hdfs_file_split, is_cached, is_remote});
        }
      }
    }
  }
}

int Result::CountAssignmentsIf(const AssignmentFilter& filter) const {
  int count = 0;
  AssignmentCallback cb = [&count, filter](const AssignmentInfo& assignment) {
    if (filter(assignment)) ++count;
  };
  ProcessAssignments(cb);
  return count;
}

int64_t Result::CountAssignedBytesIf(const AssignmentFilter& filter) const {
  int64_t assigned_bytes = 0;
  AssignmentCallback cb = [&assigned_bytes, filter](const AssignmentInfo& assignment) {
    if (filter(assignment)) assigned_bytes += assignment.hdfs_file_split.length;
  };
  ProcessAssignments(cb);
  return assigned_bytes;
}

void Result::CountAssignmentsPerBackend(
    NumAssignmentsPerBackend* num_assignments_per_backend) const {
  AssignmentCallback cb = [&num_assignments_per_backend](
      const AssignmentInfo& assignment) {
    ++(*num_assignments_per_backend)[assignment.addr.hostname];
  };
  ProcessAssignments(cb);
}

void Result::CountAssignedBytesPerBackend(
    NumAssignedBytesPerBackend* num_assignments_per_backend) const {
  AssignmentCallback cb = [&num_assignments_per_backend](
      const AssignmentInfo& assignment) {
    (*num_assignments_per_backend)[assignment.addr.hostname] +=
        assignment.hdfs_file_split.length;
  };
  ProcessAssignments(cb);
}

SchedulerWrapper::SchedulerWrapper(const Plan& plan)
  : plan_(plan), metrics_("TestMetrics") {
  InitializeScheduler();
}

Status SchedulerWrapper::Compute(bool exec_at_coord, Result* result) {
  DCHECK(scheduler_ != nullptr);

  // Compute Assignment.
  FragmentScanRangeAssignment* assignment = result->AddAssignment();
  return scheduler_->ComputeScanRangeAssignment(*scheduler_->GetExecutorsConfig(), 0,
      nullptr, false, plan_.scan_range_locations(), plan_.referenced_datanodes(),
      exec_at_coord, plan_.query_options(), nullptr, assignment);
}

void SchedulerWrapper::AddBackend(const Host& host) {
  // Add to topic delta
  TTopicDelta delta;
  delta.topic_name = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  delta.is_delta = true;
  AddHostToTopicDelta(host, &delta);
  SendTopicDelta(delta);
}

void SchedulerWrapper::RemoveBackend(const Host& host) {
  // Add deletion to topic delta
  TTopicDelta delta;
  delta.topic_name = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  delta.is_delta = true;
  TTopicItem item;
  item.__set_deleted(true);
  item.__set_key(host.ip);
  delta.topic_entries.push_back(item);
  SendTopicDelta(delta);
}

void SchedulerWrapper::SendFullMembershipMap() {
  TTopicDelta delta;
  delta.topic_name = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  delta.is_delta = false;
  for (const Host& host : plan_.cluster().hosts()) {
    if (host.be_port >= 0) AddHostToTopicDelta(host, &delta);
  }
  SendTopicDelta(delta);
}

void SchedulerWrapper::SendEmptyUpdate() {
  TTopicDelta delta;
  delta.topic_name = Statestore::IMPALA_MEMBERSHIP_TOPIC;
  delta.is_delta = true;
  SendTopicDelta(delta);
}

void SchedulerWrapper::InitializeScheduler() {
  DCHECK(scheduler_ == nullptr);
  DCHECK_GT(plan_.cluster().NumHosts(), 0) << "Cannot initialize scheduler with 0 "
                                           << "hosts.";
  const Host& scheduler_host = plan_.cluster().hosts()[0];
  string scheduler_backend_id = scheduler_host.ip;
  TNetworkAddress scheduler_backend_address =
      MakeNetworkAddress(scheduler_host.ip, scheduler_host.be_port);
  TNetworkAddress scheduler_krpc_address =
      MakeNetworkAddress(scheduler_host.ip, FLAGS_krpc_port);
  scheduler_.reset(new Scheduler(nullptr, scheduler_backend_id,
      &metrics_, nullptr, nullptr));
  const Status status = scheduler_->Init(scheduler_backend_address,
      scheduler_krpc_address, scheduler_host.ip);
  DCHECK(status.ok()) << "Scheduler init failed in test";
  // Initialize the scheduler backend maps.
  SendFullMembershipMap();
}

void SchedulerWrapper::AddHostToTopicDelta(const Host& host, TTopicDelta* delta) const {
  DCHECK_GT(host.be_port, 0) << "Host cannot be added to scheduler without a running "
                             << "backend";
  // Build backend descriptor.
  TBackendDescriptor be_desc;
  be_desc.address.hostname = host.ip;
  be_desc.address.port = host.be_port;
  be_desc.ip_address = host.ip;
  be_desc.__set_is_coordinator(host.is_coordinator);
  be_desc.__set_is_executor(host.is_executor);

  // Build topic item.
  TTopicItem item;
  item.key = host.ip;
  ThriftSerializer serializer(false);
  Status status = serializer.Serialize(&be_desc, &item.value);
  DCHECK(status.ok());

  // Add to topic delta.
  delta->topic_entries.push_back(item);
}

void SchedulerWrapper::SendTopicDelta(const TTopicDelta& delta) {
  DCHECK(scheduler_ != nullptr);
  // Wrap in topic delta map.
  StatestoreSubscriber::TopicDeltaMap delta_map;
  delta_map.emplace(Statestore::IMPALA_MEMBERSHIP_TOPIC, delta);

  // Send to the scheduler.
  vector<TTopicDelta> dummy_result;
  scheduler_->UpdateMembership(delta_map, &dummy_result);
}
