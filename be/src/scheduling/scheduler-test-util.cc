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

#include <random>

#include <boost/unordered_set.hpp>

#include "common/names.h"
#include "flatbuffers/flatbuffers.h"
#include "gen-cpp/CatalogObjects_generated.h"
#include "scheduling/cluster-membership-mgr.h"
#include "scheduling/cluster-membership-test-util.h"
#include "scheduling/scheduler.h"
#include "util/hash-util.h"
#include "service/impala-server.h"

using namespace impala;
using namespace impala::test;
using namespace org::apache::impala::fb;

DECLARE_int32(krpc_port);

/// Make the BlockNamingPolicy enum easy to print. Must be declared in impala::test
namespace impala {
namespace test {
std::ostream& operator<<(std::ostream& os, const BlockNamingPolicy& naming_policy)
{
  switch(naming_policy) {
  case BlockNamingPolicy::UNPARTITIONED:
    os << "UNPARTITIONED"; break;
  case BlockNamingPolicy::PARTITIONED_SINGLE_FILENAME:
    os << "PARTITIONED_SINGLE_FILENAME"; break;
  case BlockNamingPolicy::PARTITIONED_UNIQUE_FILENAMES:
    os << "PARTITIONED_UNIQUE_FILENAMES"; break;
  default: os.setstate(std::ios_base::failbit);
  }
  return os;
}
}
}

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
/// Default size for files is 4MB.
const int64_t FileSplitGeneratorSpec::DEFAULT_FILE_SIZE = 1 << 22;
/// Default size for file splits is 1 MB.
const int64_t FileSplitGeneratorSpec::DEFAULT_BLOCK_SIZE = 1 << 20;

ClusterMembershipMgr::BeDescSharedPtr BuildBackendDescriptor(const Host& host) {
  auto be_desc = make_shared<BackendDescriptorPB>();
  be_desc->mutable_address()->set_hostname(host.ip);
  be_desc->mutable_address()->set_port(host.be_port);
  be_desc->set_ip_address(host.ip);
  be_desc->set_is_coordinator(host.is_coordinator);
  be_desc->set_is_executor(host.is_executor);
  be_desc->set_is_quiescing(false);
  be_desc->set_admit_mem_limit(GIGABYTE);
  ExecutorGroupDescPB* exec_desc = be_desc->add_executor_groups();
  exec_desc->set_name(ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);
  exec_desc->set_min_size(1);
  return be_desc;
}

int Cluster::AddHost(bool has_backend, bool has_datanode, bool is_executor) {
  int host_idx = hosts_.size();
  int be_port = has_backend ? BACKEND_PORT : -1;
  int dn_port = has_datanode ? DATANODE_PORT : -1;
  IpAddr ip = HostIdxToIpAddr(host_idx);
  DCHECK(ip_to_idx_.find(ip) == ip_to_idx_.end());
  ip_to_idx_[ip] = host_idx;
  hosts_.push_back(Host(HostIdxToHostname(host_idx),
      ip, be_port, dn_port, has_backend && is_executor));
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

void Cluster::GetBackendAddress(int host_idx, TNetworkAddress* addr) const {
  DCHECK_LT(host_idx, hosts_.size());
  addr->hostname = hosts_[host_idx].ip;
  addr->port = hosts_[host_idx].be_port;
}

Cluster Cluster::CreateRemoteCluster(int num_impala_nodes, int num_data_nodes) {
  Cluster cluster;
  // Set of Impala hosts
  cluster.AddHosts(num_impala_nodes, true, false);
  // Set of datanodes
  cluster.AddHosts(num_data_nodes, false, true);
  return cluster;
}

const vector<int>& Cluster::datanode_with_backend_host_idxs() const {
  return datanode_with_backend_host_idxs_;
}

const vector<int>& Cluster::datanode_only_host_idxs() const {
  return datanode_only_host_idxs_;
}

void Schema::AddEmptyTable(
    const TableName& table_name, BlockNamingPolicy naming_policy) {
  DCHECK(tables_.find(table_name) == tables_.end());
  // Create table
  Table table;
  table.naming_policy = naming_policy;
  // Insert table
  tables_.emplace(table_name, table);
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
  AddMultiBlockTable(table_name, num_blocks, replica_placement, num_replicas, 0,
      BlockNamingPolicy::UNPARTITIONED);
}

void Schema::AddMultiBlockTable(const TableName& table_name, int num_blocks,
    ReplicaPlacement replica_placement, int num_replicas, int num_cached_replicas,
    BlockNamingPolicy naming_policy) {
  DCHECK_GT(num_replicas, 0);
  DCHECK(num_cached_replicas <= num_replicas);
  Table table;
  table.naming_policy = naming_policy;
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

void Schema::AddFileSplitGeneratorSpecs(
    const TableName& table_name, const std::vector<FileSplitGeneratorSpec>& specs) {
  Table* table = &tables_[table_name];
  table->specs.insert(table->specs.end(), specs.begin(), specs.end());
}

void Schema::AddFileSplitGeneratorDefaultSpecs(const TableName& table_name, int num) {
  Table* table = &tables_[table_name];
  for (int i = 0; i < num; ++i) table->specs.push_back(FileSplitGeneratorSpec());
}

const Table& Schema::GetTable(const TableName& table_name) const {
  auto it = tables_.find(table_name);
  DCHECK(it != tables_.end());
  return it->second;
}

void Plan::SetReplicaPreference(TReplicaPreference::type p) {
  query_options_.replica_preference = p;
}

void Plan::SetNumRemoteExecutorCandidates(int32_t num) {
  query_options_.num_remote_executor_candidates = num;
}

const vector<TNetworkAddress>& Plan::referenced_datanodes() const {
  return referenced_datanodes_;
}

const TScanRangeSpec& Plan::scan_range_specs() const {
  return scan_range_specs_;
}

void Plan::AddSystemTableScan() {
  for (const Host& host : cluster().hosts()) {
    // SystemTable host_list uses coordinator addresses.
    TNetworkAddress node;
    node.hostname = host.ip;
    node.port = host.be_port;
    int32_t host_idx = referenced_datanodes_.size();
    referenced_datanodes_.push_back(node);

    TScanRangeLocationList scan_range_locations;
    scan_range_locations.scan_range.__set_is_system_scan(true);
    scan_range_locations.locations.resize(1);
    scan_range_locations.locations[0].host_idx = host_idx;
    scan_range_specs_.concrete_ranges.push_back(scan_range_locations);
  }
}

void Plan::AddTableScan(const TableName& table_name) {
  const Table& table = schema_.GetTable(table_name);
  const vector<Block>& blocks = table.blocks;
  for (int i = 0; i < blocks.size(); ++i) {
    const Block& block = blocks[i];
    TScanRangeLocationList scan_range_locations;
    BuildTScanRangeLocationList(table_name, block, i, table.naming_policy,
        &scan_range_locations);
    scan_range_specs_.concrete_ranges.push_back(scan_range_locations);
  }
  const vector<FileSplitGeneratorSpec>& specs = table.specs;
  for (int i = 0; i < specs.size(); ++i) {
    const FileSplitGeneratorSpec& file_spec = specs[i];
    TFileSplitGeneratorSpec spec;
    BuildScanRangeSpec(table_name, file_spec, i, table.naming_policy, &spec);
    scan_range_specs_.split_specs.push_back(spec);
  }
}

void Plan::BuildTScanRangeLocationList(const TableName& table_name, const Block& block,
    int block_idx, BlockNamingPolicy naming_policy,
    TScanRangeLocationList* scan_range_locations) {
  const vector<int>& replica_idxs = block.replica_host_idxs;
  const vector<bool>& is_cached = block.replica_host_idx_is_cached;
  DCHECK_EQ(replica_idxs.size(), is_cached.size());
  int num_replicas = replica_idxs.size();
  BuildScanRange(table_name, block, block_idx, naming_policy,
      &scan_range_locations->scan_range);
  scan_range_locations->locations.resize(num_replicas);
  for (int i = 0; i < num_replicas; ++i) {
    TScanRangeLocation& location = scan_range_locations->locations[i];
    location.host_idx = FindOrInsertDatanodeIndex(replica_idxs[i]);
    location.__set_is_cached(is_cached[i]);
  }
}

void Plan::GetBlockPaths(const TableName& table_name, bool is_spec, int index,
    BlockNamingPolicy naming_policy, string* relative_path, int64_t* partition_id,
    string* partition_path) {
  // For debugging, it is useful to differentiate between Blocks and
  // FileSplitGeneratorSpecs.
  string spec_or_block = is_spec ? "_spec_" : "_block_";

  switch (naming_policy) {
  case BlockNamingPolicy::UNPARTITIONED:
    // For unpartitioned tables, use unique file names and set partition_id = 0
    // Encoding the table name and index in the file helps debugging.
    // For example, an unpartitioned table may contain two files with paths like:
    // /warehouse/{table_name}/file1.csv
    // /warehouse/{table_name}/file2.csv
    *relative_path = table_name + spec_or_block + std::to_string(index);
    *partition_id = 0;
    *partition_path = "/warehouse/" + table_name;
    break;
  case BlockNamingPolicy::PARTITIONED_SINGLE_FILENAME:
    // For partitioned tables with simple names, use a simple file name, but vary the
    // partition id and partition_path by using the index as the partition_id and as
    // part of the partition_path.
    // For example, a partitioned table with two partitions might have paths like:
    // /warehouse/{table_name}/year=2010/000001_0
    // /warehouse/{table_name}/year=2009/000001_0
    *relative_path = "000001_0";
    *partition_id = index;
    *partition_path = "/warehouse/" + table_name + "/part=" + std::to_string(index);
    break;
  case BlockNamingPolicy::PARTITIONED_UNIQUE_FILENAMES:
    // For partitioned tables with unique names, the file name, partition_id, and
    // partition_path all incorporate the index.
    // For example, a partitioned table with two partitions might have paths like:
    // /warehouse/{table_name}/year=2010/6541856e3fb0583d_654267678_data.0.parq
    // /warehouse/{table_name}/year=2009/6541856e3fb0583d_627636719_data.0.parq
    *relative_path = table_name + spec_or_block + std::to_string(index);
    *partition_id = index;
    *partition_path = "/warehouse/" + table_name + "/part=" + std::to_string(index);
    break;
  default:
    DCHECK(false) << "Invalid naming_policy";
  }
}

void Plan::BuildScanRange(const TableName& table_name, const Block& block, int block_idx,
    BlockNamingPolicy naming_policy, TScanRange* scan_range) {
  // Initialize locations.scan_range correctly.
  THdfsFileSplit file_split;
  // 'length' is the only member considered by the scheduler for scheduling non-remote
  // blocks.
  file_split.length = block.length;

  // Consistent remote scheduling considers the partition_path_hash, relative_path,
  // and offset when scheduling blocks.
  string partition_path;
  GetBlockPaths(table_name, false, block_idx, naming_policy, &file_split.relative_path,
      &file_split.partition_id, &partition_path);
  file_split.partition_path_hash =
      static_cast<int32_t>(HashUtil::Hash(partition_path.data(),
          partition_path.length(), 0));
  file_split.offset = 0;

  // For now, we model each file by a single block.
  file_split.file_length = block.length;
  file_split.file_compression = THdfsCompression::NONE;
  file_split.mtime = 1;
  scan_range->__set_hdfs_file_split(file_split);
}

void Plan::BuildScanRangeSpec(const TableName& table_name,
    const FileSplitGeneratorSpec& spec, int spec_idx, BlockNamingPolicy naming_policy,
    TFileSplitGeneratorSpec* thrift_spec) {
  THdfsFileDesc thrift_file;

  string relative_path;
  int64_t partition_id;
  string partition_path;
  GetBlockPaths(table_name, true, spec_idx, naming_policy, &relative_path,
      &partition_id, &partition_path);

  flatbuffers::FlatBufferBuilder fb_builder;
  auto rel_path =
      fb_builder.CreateString(relative_path);
  auto fb_file_desc = CreateFbFileDesc(fb_builder, rel_path, spec.length);
  fb_builder.Finish(fb_file_desc);

  string buffer(
      reinterpret_cast<const char*>(fb_builder.GetBufferPointer()), fb_builder.GetSize());
  thrift_file.__set_file_desc_data(buffer);

  thrift_spec->__set_partition_id(partition_id);
  thrift_spec->__set_file_desc(thrift_file);
  thrift_spec->__set_max_block_size(spec.block_size);
  thrift_spec->__set_is_splittable(spec.is_splittable);
  thrift_spec->__set_is_footer_only(spec.is_footer_only);
  int32_t partition_path_hash = static_cast<int32_t>(HashUtil::Hash(partition_path.data(),
      partition_path.length(), 0));
  thrift_spec->__set_partition_path_hash(partition_path_hash);
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
  AssignmentCallback cb = [&backends](const AssignmentInfo& assignment) {
    backends.insert(assignment.addr.hostname());
  };
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
  return [expected_addr](const AssignmentInfo& assignment) {
    return assignment.addr == FromTNetworkAddress(expected_addr);
  };
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
      const NetworkAddressPB& addr = assignment_elem.first;
      const PerNodeScanRanges& per_node_ranges = assignment_elem.second;
      for (const auto& per_node_ranges_elem : per_node_ranges) {
        const vector<ScanRangeParamsPB> scan_range_params_vector =
            per_node_ranges_elem.second;
        for (const ScanRangeParamsPB& scan_range_params : scan_range_params_vector) {
          const ScanRangePB& scan_range = scan_range_params.scan_range();
          HdfsFileSplitPB hdfs_file_split;
          if (scan_range.has_hdfs_file_split()) {
            hdfs_file_split = scan_range.hdfs_file_split();
          } else {
            DCHECK(scan_range.has_is_system_scan() && scan_range.is_system_scan());
            hdfs_file_split.set_length(0);
          }
          bool try_hdfs_cache = scan_range_params.has_try_hdfs_cache() ?
              scan_range_params.try_hdfs_cache() : false;
          bool is_remote =
              scan_range_params.has_is_remote() ? scan_range_params.is_remote() : false;
          cb({addr, hdfs_file_split, try_hdfs_cache, is_remote});
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
    if (filter(assignment)) assigned_bytes += assignment.hdfs_file_split.length();
  };
  ProcessAssignments(cb);
  return assigned_bytes;
}

void Result::CountAssignmentsPerBackend(
    NumAssignmentsPerBackend* num_assignments_per_backend) const {
  AssignmentCallback cb = [&num_assignments_per_backend](
                              const AssignmentInfo& assignment) {
    ++(*num_assignments_per_backend)[assignment.addr.hostname()];
  };
  ProcessAssignments(cb);
}

void Result::CountAssignedBytesPerBackend(
    NumAssignedBytesPerBackend* num_assignments_per_backend) const {
  AssignmentCallback cb = [&num_assignments_per_backend](
                              const AssignmentInfo& assignment) {
    (*num_assignments_per_backend)[assignment.addr.hostname()] +=
        assignment.hdfs_file_split.length();
  };
  ProcessAssignments(cb);
}

SchedulerWrapper::SchedulerWrapper(const Plan& plan)
  : plan_(plan), metrics_("TestMetrics") {
  InitializeScheduler();
}

Status SchedulerWrapper::Compute(bool exec_at_coord, Result* result,
    bool include_all_coordinators) {
  DCHECK(scheduler_ != nullptr);

  // Compute Assignment.
  FragmentScanRangeAssignment* assignment = result->AddAssignment();
  const vector<TScanRangeLocationList>* locations = nullptr;
  vector<TScanRangeLocationList> expanded_locations;
  if (plan_.scan_range_specs().split_specs.empty()) {
    // directly use the concrete ranges.
    locations = &plan_.scan_range_specs().concrete_ranges;
  } else {
    // union concrete ranges and expanded specs.
    for (const TScanRangeLocationList& range : plan_.scan_range_specs().concrete_ranges) {
      expanded_locations.push_back(range);
    }
    RETURN_IF_ERROR(scheduler_->GenerateScanRanges(
        plan_.scan_range_specs().split_specs, &expanded_locations));
    locations = &expanded_locations;
  }
  DCHECK(locations != nullptr);

  ClusterMembershipMgr::SnapshotPtr membership_snapshot =
      cluster_membership_mgr_->GetSnapshot();
  auto it = membership_snapshot->executor_groups.find(
      ImpalaServer::DEFAULT_EXECUTOR_GROUP_NAME);

  ExecutorGroup coords("all-coordinators");
  if (include_all_coordinators) {
    for (const auto& it : membership_snapshot->current_backends) {
      if (it.second.has_is_coordinator() && it.second.is_coordinator()) {
        coords.AddExecutor(it.second);
        LOG(INFO) << "Adding " << it.second.address() << " to all coordinators";
      }
    }
    LOG(INFO) << "Added " << coords.NumExecutors() << " to all coordinators";
  }

  // If a group does not exist (e.g. no executors are registered), we pass an empty group
  // to the scheduler to exercise its error handling logic.
  bool no_executor_group = it == membership_snapshot->executor_groups.end();
  ExecutorGroup empty_group("empty-group");
  DCHECK(membership_snapshot->local_be_desc.get() != nullptr);
  Scheduler::ExecutorConfig executor_config = {no_executor_group ? empty_group :
      it->second, *membership_snapshot->local_be_desc, coords};
  std::mt19937 rng(rand());
  return scheduler_->ComputeScanRangeAssignment(executor_config, 0, nullptr, false,
      *locations, plan_.referenced_datanodes(), exec_at_coord, plan_.query_options(),
      nullptr, &rng, nullptr, assignment);
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
  cluster_membership_mgr_.reset(
      new ClusterMembershipMgr(scheduler_backend_id, nullptr, &metrics_));
  cluster_membership_mgr_->SetLocalBeDescFn(
      [scheduler_host]() { return BuildBackendDescriptor(scheduler_host); });
  Status status = cluster_membership_mgr_->Init();
  DCHECK(status.ok()) << "Cluster membership manager init failed in test";
  scheduler_.reset(new Scheduler(&metrics_, nullptr));
  // Initialize the cluster membership manager
  SendFullMembershipMap();
}

void SchedulerWrapper::AddHostToTopicDelta(const Host& host, TTopicDelta* delta) const {
  DCHECK_GT(host.be_port, 0) << "Host cannot be added to scheduler without a running "
                             << "backend";
  ClusterMembershipMgr::BeDescSharedPtr be_desc = BuildBackendDescriptor(host);

  // Build topic item.
  TTopicItem item;
  item.key = host.ip;
  bool success = be_desc->SerializeToString(&item.value);
  DCHECK(success);

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
  cluster_membership_mgr_->UpdateMembership(delta_map, &dummy_result);
}
